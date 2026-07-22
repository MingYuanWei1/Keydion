import hashlib
import importlib.util
import io
import os
import shutil
import stat
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest import mock

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from PyPDF2 import PdfWriter
from sqlalchemy import create_engine, inspect, text

from services import publishing_migration
from services.paper_identity import normalize_alias_key
from services.paper_storage import PaperStorage
from services.publishing_migration import (
    MigrationBlocked,
    backfill_all,
    backfill_one_paper,
    run_preflight,
    validate_contract_ready,
)


def _contract_migration_module():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "migrations" / "versions" / "0003_publishing_contract.py"
    )
    spec = importlib.util.spec_from_file_location(
        "publishing_contract_migration_for_tests", migration_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load publishing contract migration")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublishingMigrationTests(unittest.TestCase):
    def setUp(self):
        maintenance = mock.patch.dict(
            os.environ, {"PAPERQUERY_PUBLISHING_MAINTENANCE": "1"},
        )
        maintenance.start()
        self.addCleanup(maintenance.stop)
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        root = Path(self.temp_dir.name)
        self.papers = root / "papers"
        self.papers.mkdir()
        self.engine = create_engine(f"sqlite:///{root / 'migration.sqlite'}")
        self.addCleanup(self.engine.dispose)
        self._create_expanded_fixture()

    def _create_expanded_fixture(self):
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE papers_metadata (
                    filename VARCHAR(255) PRIMARY KEY,
                    title VARCHAR(255), journal VARCHAR(255), category VARCHAR(255),
                    language VARCHAR(255), keywords TEXT, abstract TEXT,
                    author_name VARCHAR(255), author_email VARCHAR(255),
                    author_school VARCHAR(255), published_at VARCHAR(255),
                    ib_ee_data TEXT, is_ib_sample VARCHAR(10),
                    is_anonymous VARCHAR(10), cp_data TEXT, ia_data TEXT,
                    id VARCHAR(36) UNIQUE, lifecycle_state VARCHAR(16),
                    current_revision INTEGER, row_version INTEGER,
                    index_status VARCHAR(16), indexed_revision INTEGER,
                    index_error TEXT, direct_idempotency_key VARCHAR(255),
                    direct_payload_hash VARCHAR(64),
                    origin_submission_id VARCHAR(255),
                    reservation_expires_at DATETIME
                )
            """))
            conn.execute(text("""
                CREATE TABLE papers_chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename VARCHAR(255), paper_id VARCHAR(36),
                    revision_number INTEGER, chunk_index INTEGER,
                    content TEXT, embedding_vec BLOB, lang VARCHAR(10)
                )
            """))
            conn.execute(text("""
                CREATE TABLE submissions (
                    id VARCHAR(255) PRIMARY KEY, pdf_filename VARCHAR(255),
                    pending_filename VARCHAR(255), title VARCHAR(255),
                    author_name VARCHAR(255), author_email VARCHAR(255),
                    author_school VARCHAR(255), status VARCHAR(50),
                    submitted_at VARCHAR(255), feedback TEXT, abstract TEXT,
                    keywords TEXT, journal VARCHAR(255), category VARCHAR(255),
                    language VARCHAR(255), submitted_by VARCHAR(255),
                    original_filename VARCHAR(255), ib_ee_data TEXT,
                    is_ib_sample VARCHAR(10), is_anonymous VARCHAR(10),
                    cp_data TEXT, ia_data TEXT, paper_id VARCHAR(36),
                    submitter_name VARCHAR(255), reviewed_at DATETIME,
                    reviewer VARCHAR(255), comment TEXT,
                    decision_idempotency_key VARCHAR(255),
                    decision_payload_hash VARCHAR(64)
                )
            """))
            conn.execute(text("""
                CREATE TABLE paper_revisions (
                    paper_id VARCHAR(36) NOT NULL, revision_number INTEGER NOT NULL,
                    sha256 VARCHAR(64) NOT NULL, size_bytes INTEGER NOT NULL,
                    created_at DATETIME NOT NULL, created_by VARCHAR(255) NOT NULL,
                    restored_from_revision INTEGER,
                    PRIMARY KEY (paper_id, revision_number)
                )
            """))
            conn.execute(text("""
                CREATE TABLE paper_filename_aliases (
                    lookup_key VARCHAR(255) PRIMARY KEY, filename VARCHAR(255) NOT NULL,
                    paper_id VARCHAR(36) NOT NULL, created_at DATETIME NOT NULL
                )
            """))
            conn.execute(text("""
                CREATE TABLE publishing_jobs (
                    id VARCHAR(36) PRIMARY KEY, kind VARCHAR(32) NOT NULL,
                    paper_id VARCHAR(36) NOT NULL, revision_number INTEGER NOT NULL,
                    dedupe_key VARCHAR(255) NOT NULL UNIQUE, state VARCHAR(16) NOT NULL,
                    attempts INTEGER NOT NULL, available_at DATETIME NOT NULL,
                    lease_token VARCHAR(36), lease_expires_at DATETIME,
                    last_error TEXT, created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
            """))
            conn.execute(text("""
                CREATE TABLE publishing_migration_journal (
                    legacy_key VARCHAR(255) PRIMARY KEY, paper_id VARCHAR(36) NOT NULL UNIQUE,
                    revision_number INTEGER NOT NULL,
                    source_sha256 VARCHAR(64), source_size_bytes INTEGER,
                    legacy_chunk_count INTEGER NOT NULL,
                    legacy_chunk_fingerprint VARCHAR(64), checkpoint VARCHAR(32) NOT NULL,
                    created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL
                )
            """))
            conn.execute(text("""
                CREATE TABLE publishing_migration_state (
                    name VARCHAR(32) PRIMARY KEY, paper_count INTEGER NOT NULL,
                    submission_count INTEGER NOT NULL, chunk_count INTEGER NOT NULL,
                    vector_count INTEGER NOT NULL, ddl_phase VARCHAR(32) NOT NULL,
                    captured_at DATETIME NOT NULL
                )
            """))
            conn.execute(text("""
                CREATE TABLE submission_identity_fence (
                    name VARCHAR(32) PRIMARY KEY, generation BIGINT NOT NULL
                )
            """))
            conn.execute(text("""
                INSERT INTO submission_identity_fence (name, generation)
                VALUES ('global', 0)
            """))
            conn.execute(text("""
                CREATE TABLE publishing_migration_issues (
                    id VARCHAR(36) PRIMARY KEY, kind VARCHAR(32) NOT NULL,
                    legacy_key VARCHAR(255), paper_id VARCHAR(36), details TEXT NOT NULL,
                    blocking BOOLEAN NOT NULL, resolved_at DATETIME,
                    created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL
                )
            """))
            conn.execute(text("""
                CREATE TABLE rag_index_meta (
                    name VARCHAR(32) PRIMARY KEY, value INTEGER NOT NULL
                )
            """))

    def insert_legacy_paper(self, filename, **metadata):
        values = {"filename": filename, "title": metadata.get("title", filename)}
        with self.engine.begin() as conn:
            conn.execute(
                text("INSERT INTO papers_metadata (filename, title) VALUES (:filename, :title)"),
                values,
            )

    def write_legacy(self, filename, contents=b"%PDF-1.4\n"):
        (self.papers / filename).write_bytes(contents)
        self.insert_legacy_paper(filename)

    def insert_submission(self, submission_id, status, pdf_filename=None,
                          pending_filename=None, feedback=None, comment=None):
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO submissions (
                    id, status, pdf_filename, pending_filename, feedback, comment
                ) VALUES (
                    :id, :status, :pdf_filename, :pending_filename, :feedback, :comment
                )
            """), {
                "id": submission_id,
                "status": status,
                "pdf_filename": pdf_filename,
                "pending_filename": pending_filename,
                "feedback": feedback,
                "comment": comment,
            })

    def scalar(self, sql, **params):
        with self.engine.connect() as conn:
            return conn.execute(text(sql), params).scalar()

    def _legacy_engine(self, name="legacy.sqlite"):
        engine = create_engine(f"sqlite:///{Path(self.temp_dir.name) / name}")
        self.addCleanup(engine.dispose)
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE papers_metadata (
                    filename VARCHAR(255) PRIMARY KEY,
                    title VARCHAR(255), journal VARCHAR(255), category VARCHAR(255),
                    language VARCHAR(255), keywords TEXT, abstract TEXT,
                    author_name VARCHAR(255), author_email VARCHAR(255),
                    author_school VARCHAR(255), published_at VARCHAR(255),
                    ib_ee_data TEXT, is_ib_sample VARCHAR(10),
                    is_anonymous VARCHAR(10), cp_data TEXT, ia_data TEXT
                )
            """))
            conn.execute(text("""
                CREATE TABLE papers_chunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename VARCHAR(255), chunk_index INTEGER, content TEXT,
                    embedding_vec BLOB, lang VARCHAR(10)
                )
            """))
            conn.execute(text("""
                CREATE TABLE submissions (
                    id VARCHAR(255) PRIMARY KEY, pdf_filename VARCHAR(255),
                    pending_filename VARCHAR(255), title VARCHAR(255),
                    author_name VARCHAR(255), author_email VARCHAR(255),
                    author_school VARCHAR(255), status VARCHAR(50),
                    submitted_at VARCHAR(255), feedback TEXT, abstract TEXT,
                    keywords TEXT, journal VARCHAR(255), category VARCHAR(255),
                    language VARCHAR(255), submitted_by VARCHAR(255),
                    original_filename VARCHAR(255), ib_ee_data TEXT,
                    is_ib_sample VARCHAR(10), is_anonymous VARCHAR(10),
                    cp_data TEXT, ia_data TEXT
                )
            """))
            conn.execute(text("""
                CREATE TABLE rag_index_meta (
                    name VARCHAR(32) PRIMARY KEY, value INTEGER NOT NULL
                )
            """))
        return engine

    def _alembic_config(self, engine):
        config_path = Path(self.temp_dir.name) / f"{id(engine)}-alembic.ini"
        config_path.write_text(
            "[alembic]\n"
            f"script_location = {Path(__file__).resolve().parents[1] / 'migrations'}\n",
            encoding="utf-8",
        )
        config = Config(str(config_path))
        config.attributes["papers_dir"] = self.papers
        config.attributes["connection"] = engine.connect()
        self.addCleanup(config.attributes["connection"].close)
        return config

    def test_file_only_pdf_is_inventoried_as_a_paper(self):
        (self.papers / "orphan.pdf").write_bytes(b"%PDF-1.4\n")
        report = run_preflight(self.engine, self.papers)
        self.assertEqual(report.importable_file_only, ("orphan.pdf",))
        self.assertEqual(report.blockers, ())

    def test_metadata_without_pdf_blocks_cutover(self):
        self.insert_legacy_paper("missing.pdf")
        report = run_preflight(self.engine, self.papers)
        self.assertIn("missing.pdf", report.missing_pdfs)
        self.assertTrue(report.blockers)
        self.assertEqual(report.blockers[0].code, "missing_pdf")

    def test_casefold_alias_collision_blocks_cutover(self):
        self.write_legacy("Ｐａｐｅｒ.pdf")
        self.write_legacy("paper.pdf")
        report = run_preflight(self.engine, self.papers)
        self.assertEqual(len(report.alias_collisions), 1)
        self.assertEqual(report.alias_collisions[0], ("paper.pdf", "Ｐａｐｅｒ.pdf"))
        self.assertEqual(report.blockers[0].code, "alias_collision")

    def test_normalized_alias_over_storage_limit_is_not_truncated(self):
        filename = f"{'㍿' * 63}.pdf"
        self.write_legacy(filename)
        self.assertGreater(len(normalize_alias_key(filename)), 255)
        report = run_preflight(self.engine, self.papers)
        self.assertEqual(
            [(issue.code, issue.legacy_key) for issue in report.blockers],
            [("alias_collision", filename)],
        )

    def test_report_and_issues_are_immutable(self):
        self.insert_legacy_paper("missing.pdf")
        report = run_preflight(self.engine, self.papers)
        with self.assertRaises(FrozenInstanceError):
            report.metadata_count = 0
        with self.assertRaises(FrozenInstanceError):
            report.issues[0].blocking = False

    def test_legacy_rejected_submission_without_filename_is_reported_unavailable(self):
        self.insert_submission("rejected", "rejected")
        report = run_preflight(self.engine, self.papers)
        self.assertEqual(report.unavailable_rejected_pdfs, ("rejected",))

    def test_unsafe_legacy_keys_are_never_hashed_or_backfilled(self):
        outside = Path(self.temp_dir.name) / "outside.pdf"
        outside.write_bytes(b"outside-secret")
        symlink = self.papers / "escape.pdf"
        symlink.symlink_to(outside)
        unsafe = ("../outside.pdf", str(outside), "escape.pdf")
        for filename in unsafe:
            self.insert_legacy_paper(filename)

        with mock.patch(
            "services.publishing_migration._hash_file",
            side_effect=AssertionError("unsafe target was hashed"),
        ) as hash_file:
            report = run_preflight(self.engine, self.papers)
        self.assertEqual(hash_file.call_count, 0)
        self.assertEqual(
            tuple(issue.legacy_key for issue in report.blockers),
            unsafe,
        )
        self.assertTrue(all(issue.code == "unresolved_filename" for issue in report.blockers))

        for filename in unsafe:
            with self.subTest(filename=filename):
                with self.assertRaises(MigrationBlocked):
                    backfill_one_paper(self.engine, self.papers, filename)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_journal"), 0)
        self.assertFalse((self.papers / ".publishing-migration-stage").exists())
        self.assertEqual(
            sorted(path.name for path in self.papers.iterdir()),
            ["escape.pdf"],
        )

    def test_preflight_performs_zero_sql_or_file_mutations(self):
        self.write_legacy("paper.pdf")
        before_tree = tuple(sorted(str(path.relative_to(self.papers)) for path in self.papers.rglob("*")))
        report = run_preflight(self.engine, self.papers)
        after_tree = tuple(sorted(str(path.relative_to(self.papers)) for path in self.papers.rglob("*")))
        self.assertFalse(report.blockers)
        self.assertEqual(before_tree, after_tree)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_journal"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_issues"), 0)

    def test_preflight_blocks_staging_symlink_without_mutation(self):
        self.write_legacy("paper.pdf")
        stage_target = self.papers / "stage-target"
        stage_target.mkdir()
        stage_link = self.papers / ".publishing-migration-stage"
        stage_link.symlink_to(stage_target, target_is_directory=True)
        before = os.readlink(stage_link)

        report = run_preflight(self.engine, self.papers)

        self.assertIn("cross_device_staging", tuple(issue.code for issue in report.blockers))
        self.assertTrue(stage_link.is_symlink())
        self.assertEqual(os.readlink(stage_link), before)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_journal"), 0)

    def test_every_unsafe_flat_pdf_entry_blocks_preflight(self):
        (self.papers / "safe.pdf").write_bytes(b"safe")
        outside = Path(self.temp_dir.name) / "outside.pdf"
        outside.write_bytes(b"outside-secret")
        (self.papers / "inside-link.pdf").symlink_to(self.papers / "safe.pdf")
        (self.papers / "outside-link.pdf").symlink_to(outside)
        (self.papers / "broken.pdf").symlink_to(self.papers / "absent.pdf")
        (self.papers / "directory.pdf").mkdir()
        os.mkfifo(self.papers / "fifo.pdf")

        report = run_preflight(self.engine, self.papers)

        self.assertEqual(report.importable_file_only, ("safe.pdf",))
        self.assertEqual(
            {(issue.code, issue.legacy_key) for issue in report.blockers},
            {
                ("unresolved_filename", "inside-link.pdf"),
                ("unresolved_filename", "outside-link.pdf"),
                ("unresolved_filename", "broken.pdf"),
                ("unresolved_filename", "directory.pdf"),
                ("unresolved_filename", "fifo.pdf"),
            },
        )

    def test_hidden_flat_pdfs_are_inventoried_and_unsafe_hidden_entries_block(self):
        (self.papers / ".hidden.pdf").write_bytes(b"hidden-safe")
        outside = Path(self.temp_dir.name) / "outside-hidden.pdf"
        outside.write_bytes(b"outside")
        (self.papers / ".unsafe.pdf").symlink_to(outside)

        report = run_preflight(self.engine, self.papers)

        self.assertEqual(report.importable_file_only, (".hidden.pdf",))
        self.assertIn(
            ("unresolved_filename", ".unsafe.pdf"),
            {(issue.code, issue.legacy_key) for issue in report.blockers},
        )

    def test_capacity_counts_each_hardlinked_import_filename(self):
        payload = b"hardlinked-source"
        (self.papers / "one.pdf").write_bytes(payload)
        os.link(self.papers / "one.pdf", self.papers / "two.pdf")

        report = run_preflight(self.engine, self.papers)

        self.assertEqual(report.importable_file_only, ("one.pdf", "two.pdf"))
        self.assertEqual(report.total_pdf_bytes, len(payload) * 2)

    def test_source_replacement_after_lstat_never_reads_symlink_target(self):
        self.write_legacy("paper.pdf", b"trusted")
        outside = Path(self.temp_dir.name) / "outside.pdf"
        outside.write_bytes(b"outside-secret")

        def replace_source(_name):
            source = self.papers / "paper.pdf"
            source.unlink()
            source.symlink_to(outside)

        with mock.patch(
            "services.publishing_migration._after_source_lstat",
            side_effect=replace_source,
        ):
            with self.assertRaises(MigrationBlocked):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")

        self.assertEqual(outside.read_bytes(), b"outside-secret")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_journal"), 0)

    def test_target_directory_replacement_after_open_never_writes_outside(self):
        self.write_legacy("paper.pdf", b"trusted")
        outside = Path(self.temp_dir.name) / "outside-target"
        outside.mkdir()

        def replace_target(_paper_id):
            paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
            target = self.papers / paper_id
            moved = self.papers / f"{paper_id}.detached"
            target.rename(moved)
            target.symlink_to(outside, target_is_directory=True)

        with mock.patch(
            "services.publishing_migration._after_target_directory_opened",
            side_effect=replace_target,
        ):
            with self.assertRaises(MigrationBlocked):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")

        self.assertEqual(tuple(outside.iterdir()), ())
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 0)

    def test_backfill_creates_verified_revision_and_preserves_flat_pdf(self):
        contents = b"%PDF-1.4\nlegacy"
        self.write_legacy("paper.pdf", contents)
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_chunks
                    (filename, chunk_index, content, embedding_vec, lang)
                VALUES ('paper.pdf', 0, 'chunk', X'00010203', 'en')
            """))

        result = backfill_one_paper(self.engine, self.papers, "paper.pdf")

        self.assertEqual(result.revision_number, 1)
        self.assertEqual(result.sha256, hashlib.sha256(contents).hexdigest())
        self.assertEqual(Path(result.destination).read_bytes(), contents)
        self.assertEqual((self.papers / "paper.pdf").read_bytes(), contents)
        self.assertEqual(
            self.scalar("SELECT lookup_key FROM paper_filename_aliases"),
            normalize_alias_key("paper.pdf"),
        )
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM papers_metadata WHERE id IS NOT NULL"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM papers_chunks WHERE paper_id IS NOT NULL"), 1)
        self.assertEqual(self.scalar("SELECT index_status FROM papers_metadata"), "ready")
        self.assertEqual(self.scalar("SELECT indexed_revision FROM papers_metadata"), 1)
        self.assertEqual(self.scalar("SELECT value FROM rag_index_meta WHERE name='chunks_version'"), 1)
        # Simulate a crash after the Paper/chunk transaction commits but before
        # the separate journal checkpoint commits.
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE publishing_migration_journal
                SET checkpoint = 'destination_verified'
            """))
        replay = backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertTrue(replay.resumed)
        self.assertEqual(
            self.scalar("SELECT checkpoint FROM publishing_migration_journal"),
            "db_complete",
        )
        completed_replay = backfill_one_paper(
            self.engine, self.papers, "paper.pdf",
        )
        self.assertTrue(completed_replay.resumed)
        self.assertEqual(
            self.scalar("SELECT checkpoint FROM publishing_migration_journal"),
            "db_complete",
        )
        self.assertEqual(self.scalar("SELECT value FROM rag_index_meta WHERE name='chunks_version'"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_jobs"), 0)

    def test_backfill_hands_off_private_single_link_revision_without_owned_stage(self):
        self.papers.chmod(0o755)
        self.write_legacy("paper.pdf", b"private-source")

        result = backfill_one_paper(self.engine, self.papers, "paper.pdf")

        destination = Path(result.destination)
        stage_root = self.papers / ".publishing-migration-stage"
        stage_paper = stage_root / result.paper_id
        for directory in (self.papers, stage_root, stage_paper, destination.parent):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o600)
        self.assertEqual(destination.stat().st_nlink, 1)
        self.assertEqual(list(stage_paper.iterdir()), [])
        self.assertEqual(destination.read_bytes(), b"private-source")

    def test_migrated_revision_supports_complete_paper_storage_lifecycle(self):
        pdf = io.BytesIO()
        writer = PdfWriter()
        writer.add_blank_page(width=72, height=72)
        writer.write(pdf)
        self.write_legacy("paper.pdf", pdf.getvalue())
        result = backfill_one_paper(self.engine, self.papers, "paper.pdf")
        pending = Path(self.temp_dir.name) / "pending"

        storage = PaperStorage(self.papers, pending)
        try:
            self.assertEqual(
                storage.open_revision(result.paper_id, 1),
                Path(result.destination).resolve(),
            )
            copied = storage.copy_revision(
                result.paper_id,
                source_revision=1,
                target_revision=2,
                operation_id="migration-copy",
                title="Migrated Paper",
                author="Migration Author",
            )
            self.assertEqual(copied.path, storage.revision_path(result.paper_id, 2))
            self.assertEqual(
                storage.reconcile_expired(
                    time.time() + 60,
                    {(result.paper_id, 1), (result.paper_id, 2)},
                ),
                0,
            )
            storage.delete_paper(result.paper_id, ["paper.pdf"])
            self.assertFalse(Path(result.destination).parent.exists())
            self.assertFalse((self.papers / "paper.pdf").exists())
            self.assertEqual(storage.reconcile_expired(time.time() + 60, set()), 0)
        finally:
            storage.close()

    def test_backfill_fails_closed_without_explicit_maintenance_guard(self):
        self.write_legacy("paper.pdf")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PAPERQUERY_PUBLISHING_MAINTENANCE", None)
            with self.assertRaisesRegex(MigrationBlocked, "maintenance"):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_journal"), 0)

    def test_concurrent_backfill_invocations_are_globally_serialized(self):
        self.write_legacy("paper.pdf", b"source")
        active = 0
        maximum = 0
        mutex = threading.Lock()

        def observe_lock(_paper_key):
            nonlocal active, maximum
            with mutex:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.05)
            with mutex:
                active -= 1

        first_engine = create_engine(str(self.engine.url))
        second_engine = create_engine(str(self.engine.url))
        self.addCleanup(first_engine.dispose)
        self.addCleanup(second_engine.dispose)
        with mock.patch(
            "services.publishing_migration._after_migration_lock_acquired",
            side_effect=observe_lock,
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = tuple(executor.map(
                    lambda candidate: backfill_one_paper(
                        candidate, self.papers, "paper.pdf",
                    ),
                    (first_engine, second_engine),
                ))

        self.assertEqual(maximum, 1)
        self.assertEqual(results[0].paper_id, results[1].paper_id)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_journal"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 1)

    def test_expanded_journal_persists_fixed_revision_number(self):
        columns = {
            column["name"]: column
            for column in inspect(self.engine).get_columns("publishing_migration_journal")
        }
        self.assertIn("revision_number", columns)
        self.assertFalse(columns["revision_number"]["nullable"])
        self.write_legacy("paper.pdf")
        backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertEqual(
            self.scalar("SELECT revision_number FROM publishing_migration_journal"),
            1,
        )

    def test_interruption_after_verified_copy_resumes_with_stable_uuid(self):
        self.write_legacy("paper.pdf")
        with mock.patch(
            "services.publishing_migration._after_copy_verified",
            side_effect=RuntimeError("injected after verified copy"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected after verified copy"):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")

        paper_id = self.scalar(
            "SELECT paper_id FROM publishing_migration_journal WHERE legacy_key='paper.pdf'"
        )
        self.assertIsNotNone(paper_id)
        self.assertEqual(self.scalar("SELECT checkpoint FROM publishing_migration_journal"), "copy_verified")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 0)

        result = backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertEqual(result.paper_id, paper_id)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_journal"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM papers_metadata"), 1)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 1)
        retained_stage = self.papers / ".publishing-migration-stage" / paper_id / "1.pdf.part"
        self.assertFalse(retained_stage.exists())
        self.assertEqual((self.papers / paper_id / "1.pdf").stat().st_nlink, 1)

    def test_interruption_during_copy_retains_partial_stage_and_resumes_safely(self):
        self.write_legacy("paper.pdf", b"complete-source")

        def interrupted_copy(_source, target, length):
            self.assertEqual(length, 1024 * 1024)
            target.write(b"partial")
            raise RuntimeError("injected during copy")

        with mock.patch(
            "services.publishing_migration.shutil.copyfileobj",
            side_effect=interrupted_copy,
        ):
            with self.assertRaisesRegex(RuntimeError, "injected during copy"):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")
        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        partial = self.papers / ".publishing-migration-stage" / paper_id / "1.pdf.part"
        self.assertEqual(partial.read_bytes(), b"partial")

        result = backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertEqual(result.paper_id, paper_id)
        self.assertEqual((self.papers / paper_id / "1.pdf").read_bytes(), b"complete-source")
        self.assertEqual(partial.read_bytes(), b"partial")

    def test_mismatched_existing_destination_is_never_overwritten(self):
        self.write_legacy("paper.pdf", b"source")
        with mock.patch(
            "services.publishing_migration._after_copy_verified",
            side_effect=RuntimeError("stop"),
        ):
            with self.assertRaises(RuntimeError):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")
        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        destination = self.papers / paper_id / "1.pdf"
        destination.parent.mkdir(exist_ok=True)
        destination.write_bytes(b"different")

        with self.assertRaises(MigrationBlocked):
            backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertEqual(destination.read_bytes(), b"different")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 0)

    def test_dangling_destination_symlink_is_refused_before_database_rows(self):
        self.write_legacy("paper.pdf", b"source")
        with mock.patch(
            "services.publishing_migration._after_copy_verified",
            side_effect=RuntimeError("stop"),
        ):
            with self.assertRaises(RuntimeError):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")
        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        destination = self.papers / paper_id / "1.pdf"
        destination.symlink_to(Path(self.temp_dir.name) / "does-not-exist.pdf")

        with self.assertRaises(MigrationBlocked):
            backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertTrue(destination.is_symlink())
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 0)

    def test_destination_created_in_publication_race_is_never_overwritten(self):
        self.write_legacy("paper.pdf", b"source")
        real_publish = publishing_migration._publish_verified_fd_no_replace

        def racing_publish(source_fd, target_fd, destination):
            racer_fd = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o640,
                dir_fd=target_fd,
            )
            try:
                os.write(racer_fd, b"racer")
            finally:
                os.close(racer_fd)
            return real_publish(source_fd, target_fd, destination)

        with mock.patch(
            "services.publishing_migration._publish_verified_fd_no_replace",
            side_effect=racing_publish,
        ):
            with self.assertRaises(MigrationBlocked):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")
        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        destination = self.papers / paper_id / "1.pdf"
        self.assertEqual(destination.read_bytes(), b"racer")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 0)

    def test_destination_replaced_immediately_before_atomic_publication_survives(self):
        self.write_legacy("paper.pdf", b"source")

        def publish_racer(_stage_name):
            paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
            destination = self.papers / paper_id / "1.pdf"
            destination.write_bytes(b"late-racer")

        with mock.patch(
            "services.publishing_migration._before_atomic_publication",
            side_effect=publish_racer,
        ):
            with self.assertRaises(MigrationBlocked):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")

        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        destination = self.papers / paper_id / "1.pdf"
        self.assertEqual(destination.read_bytes(), b"late-racer")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 0)

    def test_matching_stage_swap_publishes_verified_open_file(self):
        self.write_legacy("paper.pdf", b"verified-source")
        with mock.patch(
            "services.publishing_migration._after_copy_verified",
            side_effect=RuntimeError("retain matching stage"),
        ):
            with self.assertRaisesRegex(RuntimeError, "retain matching stage"):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")

        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        stage = self.papers / ".publishing-migration-stage" / paper_id / "1.pdf.part"
        verified = stage.with_name("verified-stage-owned-by-migration")

        def swap_matching_stage(_stage_name):
            stage.rename(verified)
            stage.write_bytes(b"replacement-owned-by-racer")

        with mock.patch(
            "services.publishing_migration._before_atomic_publication",
            side_effect=swap_matching_stage,
        ):
            result = backfill_one_paper(self.engine, self.papers, "paper.pdf")

        self.assertEqual(Path(result.destination).read_bytes(), b"verified-source")
        self.assertEqual(stage.read_bytes(), b"replacement-owned-by-racer")
        self.assertFalse(verified.exists())
        self.assertEqual(Path(result.destination).stat().st_nlink, 1)

    def test_missing_descriptor_publication_primitive_fails_before_destination(self):
        self.write_legacy("paper.pdf", b"verified-source")

        with mock.patch(
            "services.publishing_migration.sys.platform", "unsupported-os",
        ):
            with self.assertRaisesRegex(
                MigrationBlocked,
                "no descriptor-based no-replace publication primitive",
            ):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")

        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        self.assertFalse((self.papers / paper_id / "1.pdf").exists())
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 0)

    def test_stage_replacement_after_mismatch_hash_is_never_unlinked(self):
        self.write_legacy("paper.pdf", b"complete-source")

        def interrupted_copy(_source, target, length):
            target.write(b"partial")
            raise RuntimeError("stop with partial")

        with mock.patch(
            "services.publishing_migration.shutil.copyfileobj",
            side_effect=interrupted_copy,
        ):
            with self.assertRaises(RuntimeError):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")
        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        partial = self.papers / ".publishing-migration-stage" / paper_id / "1.pdf.part"

        def replace_stage(stage_name, matched):
            self.assertEqual(stage_name, "1.pdf.part")
            self.assertFalse(matched)
            partial.unlink()
            partial.write_bytes(b"replacement-owned-by-racer")

        with mock.patch(
            "services.publishing_migration._after_stage_candidate_hashed",
            side_effect=replace_stage,
        ):
            result = backfill_one_paper(self.engine, self.papers, "paper.pdf")

        self.assertEqual(partial.read_bytes(), b"replacement-owned-by-racer")
        self.assertEqual(Path(result.destination).read_bytes(), b"complete-source")

    def test_interruption_after_atomic_publication_resumes_with_retained_stage(self):
        self.write_legacy("paper.pdf", b"source")
        with mock.patch(
            "services.publishing_migration._after_atomic_publication",
            side_effect=RuntimeError("injected after reservation"),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected after reservation"):
                backfill_one_paper(self.engine, self.papers, "paper.pdf")
        paper_id = self.scalar("SELECT paper_id FROM publishing_migration_journal")
        destination = self.papers / paper_id / "1.pdf"
        part = self.papers / ".publishing-migration-stage" / paper_id / "1.pdf.part"
        self.assertEqual(destination.read_bytes(), b"source")
        self.assertEqual(part.read_bytes(), b"source")

        result = backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertEqual(result.paper_id, paper_id)
        self.assertEqual(part.read_bytes(), b"source")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM paper_revisions"), 1)

    def test_exact_submission_linking_persists_nonblocking_issues(self):
        self.write_legacy("one.pdf")
        self.write_legacy("two.pdf")
        self.insert_submission("linked", "accepted", pdf_filename="one.pdf", feedback="good")
        self.insert_submission("unmatched", "accepted", pdf_filename="missing.pdf")
        self.insert_submission(
            "ambiguous", "accepted", pdf_filename="one.pdf", pending_filename="two.pdf"
        )
        self.insert_submission("pending", "pending", pdf_filename="one.pdf")
        self.insert_submission("rejected", "rejected", pending_filename="gone.pdf", feedback="no")

        backfill_all(self.engine, self.papers)

        with self.engine.connect() as conn:
            rows = {
                row.id: row
                for row in conn.execute(text("""
                    SELECT id, status, paper_id, feedback, comment
                    FROM submissions ORDER BY id
                """)).mappings()
            }
            issues = conn.execute(text("""
                SELECT kind, legacy_key, paper_id, blocking
                FROM publishing_migration_issues ORDER BY kind
            """)).mappings().all()
        self.assertIsNotNone(rows["linked"].paper_id)
        self.assertEqual(rows["linked"].feedback, "good")
        self.assertEqual(rows["linked"].comment, "good")
        self.assertIsNone(rows["unmatched"].paper_id)
        self.assertIsNone(rows["ambiguous"].paper_id)
        self.assertIsNone(rows["pending"].paper_id)
        self.assertIsNone(rows["rejected"].paper_id)
        self.assertEqual(len(rows), 5)
        self.assertEqual(
            [(row.kind, row.legacy_key, row.paper_id, bool(row.blocking)) for row in issues],
            [
                ("submission_ambiguous", "ambiguous", None, False),
                ("submission_unmatched", "unmatched", None, False),
            ],
        )
        report = run_preflight(self.engine, self.papers)
        self.assertEqual(report.unavailable_rejected_pdfs, ("rejected",))
        validate_contract_ready(self.engine, self.papers)

    def test_same_paper_claimed_by_two_accepted_submissions_is_ambiguous(self):
        self.write_legacy("one.pdf")
        self.insert_submission("first", "accepted", pdf_filename="one.pdf")
        self.insert_submission("second", "accepted", pdf_filename="one.pdf")

        report = run_preflight(self.engine, self.papers)
        self.assertEqual(
            [(issue.code, issue.legacy_key) for issue in report.issues],
            [
                ("submission_ambiguous", "first"),
                ("submission_ambiguous", "second"),
            ],
        )

        backfill_all(self.engine, self.papers)

        with self.engine.connect() as conn:
            links = conn.execute(text(
                "SELECT id, paper_id FROM submissions ORDER BY id"
            )).mappings().all()
            issues = conn.execute(text("""
                SELECT kind, legacy_key, blocking
                FROM publishing_migration_issues
                WHERE resolved_at IS NULL
                ORDER BY legacy_key
            """)).mappings().all()
        self.assertEqual(
            [(row.id, row.paper_id) for row in links],
            [("first", None), ("second", None)],
        )
        self.assertEqual(
            [(row.kind, row.legacy_key, bool(row.blocking)) for row in issues],
            [
                ("submission_ambiguous", "first", False),
                ("submission_ambiguous", "second", False),
            ],
        )
        validate_contract_ready(self.engine, self.papers)

    def test_accepted_submission_links_are_recomputed_even_when_prelinked(self):
        self.write_legacy("one.pdf")
        self.write_legacy("two.pdf")
        self.insert_submission("unmatched", "accepted", pdf_filename="missing.pdf")
        self.insert_submission(
            "ambiguous", "accepted", pdf_filename="one.pdf", pending_filename="two.pdf",
        )
        with self.engine.begin() as conn:
            conn.execute(text(
                "UPDATE submissions SET paper_id='stale-paper-id'"
            ))

        backfill_all(self.engine, self.papers)

        self.assertEqual(
            self.scalar("SELECT COUNT(*) FROM submissions WHERE paper_id IS NOT NULL"),
            0,
        )

    def test_contract_recomputes_and_rejects_stale_accepted_submission_link(self):
        self.write_legacy("one.pdf")
        self.write_legacy("two.pdf")
        self.insert_submission(
            "ambiguous", "accepted", pdf_filename="one.pdf", pending_filename="two.pdf",
        )
        backfill_all(self.engine, self.papers)
        paper_id = self.scalar(
            "SELECT id FROM papers_metadata WHERE filename='one.pdf'"
        )
        with self.engine.begin() as conn:
            conn.execute(text(
                "UPDATE submissions SET paper_id=:paper_id WHERE id='ambiguous'"
            ), {"paper_id": paper_id})

        with self.assertRaisesRegex(MigrationBlocked, "accepted Submission"):
            validate_contract_ready(self.engine, self.papers)

    def test_submission_issue_kind_transition_resolves_stale_opposite(self):
        self.write_legacy("one.pdf")
        self.write_legacy("two.pdf")
        self.insert_submission("changing", "accepted", pdf_filename="missing.pdf")
        backfill_all(self.engine, self.papers)

        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE submissions
                SET pdf_filename='one.pdf', pending_filename='two.pdf'
                WHERE id='changing'
            """))
        backfill_all(self.engine, self.papers)

        with self.engine.connect() as conn:
            issues = conn.execute(text("""
                SELECT kind, resolved_at FROM publishing_migration_issues
                WHERE legacy_key='changing' ORDER BY kind
            """)).mappings().all()
        self.assertEqual([row.kind for row in issues], [
            "submission_ambiguous", "submission_unmatched",
        ])
        self.assertIsNone(issues[0].resolved_at)
        self.assertIsNotNone(issues[1].resolved_at)

    def test_contract_rejects_stale_opposite_submission_diagnostic(self):
        self.write_legacy("one.pdf")
        self.write_legacy("two.pdf")
        self.insert_submission(
            "changing", "accepted", pdf_filename="one.pdf", pending_filename="two.pdf",
        )
        backfill_all(self.engine, self.papers)
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO publishing_migration_issues (
                    id, kind, legacy_key, paper_id, details, blocking,
                    resolved_at, created_at, updated_at
                ) VALUES (
                    'stale-opposite', 'submission_unmatched', 'changing', NULL,
                    'stale diagnostic', 0, NULL,
                    '2026-07-20 00:00:00', '2026-07-20 00:00:00'
                )
            """))

        with self.assertRaisesRegex(MigrationBlocked, "stale opposite"):
            validate_contract_ready(self.engine, self.papers)

    def test_papers_without_chunks_get_one_deduplicated_index_job(self):
        self.write_legacy("paper.pdf")
        first = backfill_one_paper(self.engine, self.papers, "paper.pdf")
        second = backfill_one_paper(self.engine, self.papers, "paper.pdf")
        self.assertEqual(first.paper_id, second.paper_id)
        self.assertEqual(self.scalar("SELECT index_status FROM papers_metadata"), "pending")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_jobs"), 1)
        self.assertEqual(
            self.scalar("SELECT kind FROM publishing_jobs"),
            "index_revision",
        )
        self.assertEqual(
            self.scalar("SELECT dedupe_key FROM publishing_jobs"),
            f"index:{first.paper_id}:1",
        )

    def test_backfill_all_imports_file_only_pdf_with_empty_optional_metadata(self):
        (self.papers / "orphan.pdf").write_bytes(b"%PDF-1.4\norphan")
        results = backfill_all(self.engine, self.papers)
        self.assertEqual(tuple(result.legacy_filename for result in results), ("orphan.pdf",))
        with self.engine.connect() as conn:
            paper = conn.execute(text("""
                SELECT filename, title, abstract, author_name, id
                FROM papers_metadata
            """)).mappings().one()
        self.assertEqual(paper.filename, "orphan.pdf")
        self.assertIsNone(paper.title)
        self.assertIsNone(paper.abstract)
        self.assertIsNone(paper.author_name)
        self.assertTrue((self.papers / paper.id / "1.pdf").is_file())

    def test_duplicate_chunks_block_contract_validation(self):
        self.write_legacy("paper.pdf")
        with self.engine.begin() as conn:
            for content in ("first", "second"):
                conn.execute(text("""
                    INSERT INTO papers_chunks
                        (filename, chunk_index, content, embedding_vec, lang)
                    VALUES ('paper.pdf', 0, :content, X'00', 'en')
                """), {"content": content})
        report = run_preflight(self.engine, self.papers)
        self.assertIn("duplicate_chunk", tuple(issue.code for issue in report.blockers))
        with self.assertRaises(MigrationBlocked):
            validate_contract_ready(self.engine, self.papers)

    def test_duplicate_final_chunk_key_blocks_even_when_filenames_differ(self):
        self.write_legacy("one.pdf")
        self.write_legacy("two.pdf")
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_chunks (filename, chunk_index, content)
                VALUES ('one.pdf', 0, 'one'), ('two.pdf', 0, 'two')
            """))
        backfill_all(self.engine, self.papers)
        first_paper = self.scalar(
            "SELECT id FROM papers_metadata WHERE filename='one.pdf'"
        )
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE papers_chunks SET paper_id=:paper_id, revision_number=1
                WHERE filename='two.pdf'
            """), {"paper_id": first_paper})

        with self.assertRaisesRegex(MigrationBlocked, "duplicate chunk"):
            validate_contract_ready(self.engine, self.papers)

    def test_null_chunk_index_blocks_exact_prospective_contract_key(self):
        self.write_legacy("paper.pdf")
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_chunks (filename, chunk_index, content)
                VALUES ('paper.pdf', NULL, 'chunk')
            """))
        backfill_all(self.engine, self.papers)

        with self.assertRaisesRegex(MigrationBlocked, "prospective chunk key"):
            validate_contract_ready(self.engine, self.papers)

    def test_contract_rejection_leaves_precontract_primary_key_unchanged(self):
        engine = self._legacy_engine("invalid-contract.sqlite")
        (self.papers / "paper.pdf").write_bytes(b"%PDF-1.4\n")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO papers_metadata (filename) VALUES ('paper.pdf')"
            ))
            conn.execute(text("""
                INSERT INTO papers_chunks (filename, chunk_index, content)
                VALUES ('paper.pdf', NULL, 'chunk')
            """))
        config = self._alembic_config(engine)
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0002_publishing_backfill")

        with self.assertRaisesRegex(MigrationBlocked, "prospective chunk key"):
            command.upgrade(config, "0003_publishing_contract")

        self.assertEqual(
            inspect(engine).get_pk_constraint("papers_metadata")["constrained_columns"],
            ["filename"],
        )

    def test_migration_state_has_persisted_ddl_phase(self):
        columns = {
            column["name"]: column
            for column in inspect(self.engine).get_columns("publishing_migration_state")
        }
        self.assertIn("ddl_phase", columns)
        self.assertFalse(columns["ddl_phase"]["nullable"])

    def test_expanded_schema_phase_requires_fence_singleton(self):
        self.assertEqual(
            publishing_migration.publishing_schema_phase(self.engine),
            "expanded",
        )
        with self.engine.begin() as connection:
            connection.execute(text(
                "DELETE FROM submission_identity_fence WHERE name = 'global'"
            ))

        self.assertEqual(
            publishing_migration.publishing_schema_phase(self.engine),
            "unexpected",
        )

    def test_expanded_schema_phase_requires_fence_primary_key(self):
        with self.engine.begin() as connection:
            connection.execute(text("DROP TABLE submission_identity_fence"))
            connection.execute(text("""
                CREATE TABLE submission_identity_fence (
                    name VARCHAR(32) NOT NULL,
                    generation BIGINT NOT NULL
                )
            """))
            connection.execute(text("""
                INSERT INTO submission_identity_fence (name, generation)
                VALUES ('global', 0)
            """))

        self.assertEqual(
            publishing_migration.publishing_schema_phase(self.engine),
            "unexpected",
        )

    def test_contract_refuses_persisted_issue_except_two_submission_kinds(self):
        self.write_legacy("paper.pdf")
        backfill_all(self.engine, self.papers)
        validate_contract_ready(self.engine, self.papers)
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO publishing_migration_issues (
                    id, kind, legacy_key, paper_id, details, blocking,
                    resolved_at, created_at, updated_at
                ) VALUES (
                    'issue', 'unresolved_filename', 'old.pdf', NULL,
                    'operator must resolve', 1, NULL,
                    '2026-07-20 00:00:00', '2026-07-20 00:00:00'
                )
            """))
        with self.assertRaisesRegex(MigrationBlocked, "persisted migration issue"):
            validate_contract_ready(self.engine, self.papers)

    def test_contract_refuses_flat_pdf_changed_after_journaling(self):
        self.write_legacy("paper.pdf", b"original")
        backfill_all(self.engine, self.papers)
        (self.papers / "paper.pdf").write_bytes(b"changed")
        with self.assertRaisesRegex(MigrationBlocked, "journaled source hash"):
            validate_contract_ready(self.engine, self.papers)

    def test_contract_capacity_does_not_charge_verified_copy_twice(self):
        self.write_legacy("paper.pdf", b"source bytes")
        backfill_all(self.engine, self.papers)

        with mock.patch(
            "services.publishing_migration.shutil.disk_usage",
            return_value=shutil._ntuple_diskusage(100, 100, 0),
        ):
            validate_contract_ready(self.engine, self.papers)

    def test_contract_refuses_chunk_bytes_changed_after_fingerprint(self):
        self.write_legacy("paper.pdf")
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_chunks
                    (filename, chunk_index, content, embedding_vec, lang)
                VALUES ('paper.pdf', 0, 'chunk', X'0001', 'en')
            """))
        backfill_all(self.engine, self.papers)
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE papers_chunks SET embedding_vec=X'0002'"))
        with self.assertRaisesRegex(MigrationBlocked, "chunk fingerprint"):
            validate_contract_ready(self.engine, self.papers)

    def test_contract_requires_index_state_derived_from_preserved_chunks(self):
        self.write_legacy("paper.pdf")
        backfill_all(self.engine, self.papers)
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM publishing_jobs"))
        with self.assertRaisesRegex(MigrationBlocked, "index job"):
            validate_contract_ready(self.engine, self.papers)

    def test_contract_requires_ready_state_for_preserved_chunks(self):
        self.write_legacy("paper.pdf")
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_chunks (
                    filename, chunk_index, content, embedding_vec, lang
                ) VALUES ('paper.pdf', 0, 'chunk', X'00', 'en')
            """))
        backfill_all(self.engine, self.papers)
        with self.engine.begin() as conn:
            conn.execute(text("""
                UPDATE papers_metadata
                SET index_status='pending', indexed_revision=NULL
            """))
        with self.assertRaisesRegex(MigrationBlocked, "indexed state"):
            validate_contract_ready(self.engine, self.papers)

    def test_contract_refuses_orphan_relationships_before_foreign_key_ddl(self):
        self.write_legacy("paper.pdf")
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_chunks (
                    filename, chunk_index, content, embedding_vec, lang
                ) VALUES ('paper.pdf', 0, 'chunk', X'00', 'en')
            """))
        backfill_all(self.engine, self.papers)
        paper_id = self.scalar("SELECT id FROM papers_metadata")
        with self.engine.begin() as conn:
            conn.execute(text(
                "UPDATE papers_chunks SET paper_id='orphan-paper'"
            ))
        with self.assertRaisesRegex(MigrationBlocked, "orphan"):
            validate_contract_ready(self.engine, self.papers)

        with self.engine.begin() as conn:
            conn.execute(text(
                "UPDATE papers_chunks SET paper_id=:paper_id"
            ), {"paper_id": paper_id})
            conn.execute(text("""
                INSERT INTO publishing_migration_issues (
                    id, kind, legacy_key, paper_id, details, blocking,
                    resolved_at, created_at, updated_at
                ) VALUES (
                    'orphan-issue', 'submission_unmatched', 'submission',
                    'orphan-paper', 'unmatched', 0, NULL,
                    '2026-07-20 00:00:00', '2026-07-20 00:00:00'
                )
            """))
        with self.assertRaisesRegex(MigrationBlocked, "orphan"):
            validate_contract_ready(self.engine, self.papers)

    def test_preflight_cli_reports_exact_keys_and_never_mutates(self):
        from tools.preflight_publishing_migration import main

        self.insert_legacy_paper("missing.pdf")
        output = io.StringIO()
        with mock.patch("sys.stdout", output):
            result = main([
                "--database-url", str(self.engine.url),
                "--papers-dir", str(self.papers),
            ])
        self.assertEqual(result, 2)
        self.assertIn("metadata_count=1", output.getvalue())
        self.assertIn("missing_pdf\tmissing.pdf", output.getvalue())
        self.assertNotIn("backup", output.getvalue().casefold())
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM publishing_migration_journal"), 0)
        self.assertFalse((self.papers / ".publishing-migration-stage").exists())

    def test_clean_preflight_cli_prints_exact_file_only_key(self):
        from tools.preflight_publishing_migration import main

        (self.papers / "orphan.pdf").write_bytes(b"%PDF-1.4\n")
        output = io.StringIO()
        with mock.patch("sys.stdout", output):
            result = main([
                "--database-url", str(self.engine.url),
                "--papers-dir", str(self.papers),
            ])
        self.assertEqual(result, 0)
        self.assertIn("file_only\torphan.pdf", output.getvalue())

    def test_ordered_revisions_upgrade_real_legacy_schema_and_preserve_bytes(self):
        engine = self._legacy_engine()
        contents = b"%PDF-1.4\nlegacy-migration"
        (self.papers / "paper.pdf").write_bytes(contents)
        vector = b"\x00\x01\x02\xff"
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_metadata (filename, title)
                VALUES ('paper.pdf', 'Paper')
            """))
            conn.execute(text("""
                INSERT INTO papers_chunks
                    (filename, chunk_index, content, embedding_vec, lang)
                VALUES ('paper.pdf', 0, 'chunk', :vector, 'en')
            """), {"vector": vector})
            conn.execute(text("""
                INSERT INTO submissions
                    (id, status, pdf_filename, feedback)
                VALUES ('submission', 'accepted', 'paper.pdf', 'approved')
            """))
        config = self._alembic_config(engine)
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "head")

        with engine.connect() as conn:
            current = MigrationContext.configure(conn).get_current_revision()
            paper = conn.execute(text("""
                SELECT id, filename, lifecycle_state, current_revision,
                       index_status, indexed_revision
                FROM papers_metadata
            """)).mappings().one()
            raw_vector = conn.execute(text(
                "SELECT embedding_vec FROM papers_chunks"
            )).scalar_one()
            submission = conn.execute(text("""
                SELECT paper_id, feedback, comment FROM submissions
            """)).mappings().one()
        self.assertEqual(current, "0003_publishing_contract")
        self.assertEqual(paper.filename, "paper.pdf")
        self.assertEqual(paper.lifecycle_state, "published")
        self.assertEqual(paper.current_revision, 1)
        self.assertEqual(paper.index_status, "ready")
        self.assertEqual(paper.indexed_revision, 1)
        self.assertEqual(raw_vector, vector)
        self.assertEqual(submission.paper_id, paper.id)
        self.assertEqual(submission.feedback, "approved")
        self.assertEqual(submission.comment, "approved")
        self.assertEqual((self.papers / paper.id / "1.pdf").read_bytes(), contents)
        self.assertEqual((self.papers / "paper.pdf").read_bytes(), contents)
        primary_key = inspect(engine).get_pk_constraint("papers_metadata")
        self.assertEqual(primary_key["constrained_columns"], ["id"])
        chunk_columns = {
            column["name"]: column for column in inspect(engine).get_columns("papers_chunks")
        }
        self.assertFalse(chunk_columns["paper_id"]["nullable"])
        self.assertFalse(chunk_columns["revision_number"]["nullable"])
        self.assertFalse(chunk_columns["chunk_index"]["nullable"])
        chunk_foreign_keys = inspect(engine).get_foreign_keys("papers_chunks")
        self.assertEqual(chunk_foreign_keys[0]["referred_table"], "paper_revisions")
        submission_foreign_keys = inspect(engine).get_foreign_keys("submissions")
        self.assertEqual(submission_foreign_keys[0]["referred_table"], "papers_metadata")
        submission_uniques = {
            tuple(constraint["column_names"])
            for constraint in inspect(engine).get_unique_constraints("submissions")
        }
        self.assertIn(("paper_id",), submission_uniques)
        with engine.connect() as conn:
            chunk_actions = {
                row[6] for row in conn.execute(text("PRAGMA foreign_key_list(papers_chunks)"))
            }
            submission_actions = {
                row[6] for row in conn.execute(text("PRAGMA foreign_key_list(submissions)"))
            }
        self.assertEqual(chunk_actions, {"CASCADE"})
        self.assertEqual(submission_actions, {"SET NULL"})
        for table_name in (
            "paper_revisions", "paper_filename_aliases", "publishing_jobs",
            "publishing_migration_journal", "publishing_migration_issues",
        ):
            with self.subTest(table_name=table_name):
                foreign_keys = inspect(engine).get_foreign_keys(table_name)
                self.assertEqual(foreign_keys[0]["referred_table"], "papers_metadata")
                self.assertEqual(foreign_keys[0]["options"]["ondelete"], "CASCADE")

    def test_expand_revision_keeps_filename_primary_key(self):
        engine = self._legacy_engine("expand.sqlite")
        config = self._alembic_config(engine)
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")
        self.assertEqual(
            inspect(engine).get_pk_constraint("papers_metadata")["constrained_columns"],
            ["filename"],
        )
        self.assertIn("id", {column["name"] for column in inspect(engine).get_columns("papers_metadata")})
        with engine.connect() as connection:
            self.assertEqual(
                connection.execute(text("""
                    SELECT generation FROM submission_identity_fence
                    WHERE name = 'global'
                """)).scalar_one(),
                0,
            )

    def test_expand_revision_installs_publishing_due_order_index(self):
        engine = self._legacy_engine("expand-due-order-index.sqlite")
        config = self._alembic_config(engine)
        command.stamp(config, "0000_legacy_baseline")

        command.upgrade(config, "0001_publishing_expand")

        indexes = {
            index["name"]: tuple(index["column_names"])
            for index in inspect(engine).get_indexes("publishing_jobs")
        }
        self.assertEqual(
            indexes.get("ix_publishing_jobs_due_order"),
            ("available_at", "created_at", "id"),
        )

    def test_exact_expanded_shape_stamped_legacy_replays_idempotently(self):
        engine = self._legacy_engine("expand-replay.sqlite")
        config = self._alembic_config(engine)
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")

        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")

        with engine.connect() as connection:
            self.assertEqual(
                MigrationContext.configure(connection).get_current_revision(),
                "0001_publishing_expand",
            )
        self.assertEqual(
            inspect(engine).get_pk_constraint("papers_metadata")["constrained_columns"],
            ["filename"],
        )
        self.assertIn("publishing_migration_state", inspect(engine).get_table_names())

    def test_contract_downgrade_refuses_partial_database_rollback(self):
        engine = self._legacy_engine("downgrade.sqlite")
        (self.papers / "paper.pdf").write_bytes(b"%PDF-1.4\n")
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO papers_metadata (filename) VALUES ('paper.pdf')"
            ))
        config = self._alembic_config(engine)
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "head")
        with self.assertRaisesRegex(RuntimeError, "coordinated database and file snapshots"):
            command.downgrade(config, "0002_publishing_backfill")


class LifecycleCheckNormalizationTests(unittest.TestCase):
    def test_rejects_unsupported_suffix_instead_of_ignoring_it(self):
        migration = _contract_migration_module()
        expected = migration._LIFECYCLE_CHECK

        self.assertIsNotNone(migration._normalized_check_expression(expected))
        self.assertIsNone(
            migration._normalized_check_expression(f"({expected}) AND TRUE")
        )


if __name__ == "__main__":
    unittest.main()
