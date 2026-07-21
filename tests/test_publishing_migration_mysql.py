import json
import os
import re
import tempfile
import threading
import time
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

# Capture only the caller's explicit environment before importing application
# config, which may load .env.prod.  The migration suite must never discover or
# use ambient deployment credentials.
MYSQL_ADMIN_URL = os.environ.get("PAPERQUERY_TEST_MYSQL_ADMIN_URL")

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from config import RAG_EMBED_DIM
from models import BASE
from services.publishing_migration import (
    backfill_one_paper,
    legacy_chunk_fingerprint,
    run_preflight,
)


ROOT = Path(__file__).resolve().parents[1]
_TEST_DATABASE_RE = re.compile(r"keydion_test_[0-9a-f]{32}\Z")


@unittest.skipUnless(
    MYSQL_ADMIN_URL,
    "PAPERQUERY_TEST_MYSQL_ADMIN_URL is absent; real MySQL migration test skipped",
)
class PublishingMigrationMySQLTests(unittest.TestCase):
    def setUp(self):
        maintenance = mock.patch.dict(
            os.environ, {"PAPERQUERY_PUBLISHING_MAINTENANCE": "1"},
        )
        maintenance.start()
        self.addCleanup(maintenance.stop)
        admin_url = make_url(MYSQL_ADMIN_URL)
        if admin_url.get_backend_name() != "mysql":
            raise ValueError("PAPERQUERY_TEST_MYSQL_ADMIN_URL must use MySQL")
        if admin_url.database and not admin_url.database.startswith("keydion_test_"):
            raise ValueError("refusing a non-test database in the MySQL admin URL")

        self.database_name = f"keydion_test_{uuid.uuid4().hex}"
        if not _TEST_DATABASE_RE.fullmatch(self.database_name):
            raise ValueError("refusing unsafe generated MySQL test database name")
        self.server_url = admin_url.set(database=None)
        self.database_url = admin_url.set(database=self.database_name)
        self.admin_engine = create_engine(self.server_url, pool_pre_ping=True)
        self.addCleanup(self.admin_engine.dispose)
        created = False
        try:
            with self.admin_engine.begin() as conn:
                conn.execute(text(
                    f"CREATE DATABASE `{self.database_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                ))
            created = True
        finally:
            # Register exact cleanup even if a driver error follows a partial
            # create; DROP DATABASE IF EXISTS is safe for this validated name.
            self.addCleanup(self._drop_exact_database)
        self.assertTrue(created)

        self.engine = create_engine(self.database_url, pool_pre_ping=True)
        self.addCleanup(self.engine.dispose)
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.papers = Path(self.temp_dir.name) / "papers"
        self.papers.mkdir()
        self._create_legacy_schema()

    def _drop_exact_database(self):
        if not _TEST_DATABASE_RE.fullmatch(self.database_name):
            raise ValueError("refusing unsafe MySQL cleanup target")
        if hasattr(self, "engine"):
            self.engine.dispose()
        with self.admin_engine.begin() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{self.database_name}`"))

    def _create_legacy_schema(self):
        excluded = {
            "papers_metadata", "papers_chunks", "submissions",
            "paper_revisions", "paper_filename_aliases", "publishing_jobs",
            "publishing_migration_journal", "publishing_migration_state",
            "publishing_migration_issues", "submission_identity_fence",
        }
        for table in BASE.metadata.sorted_tables:
            if table.name not in excluded:
                table.create(self.engine, checkfirst=True)
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE papers_metadata (
                    filename VARCHAR(255) NOT NULL PRIMARY KEY,
                    title VARCHAR(255), journal VARCHAR(255), category VARCHAR(255),
                    language VARCHAR(255), keywords TEXT, abstract TEXT,
                    author_name VARCHAR(255), author_email VARCHAR(255),
                    author_school VARCHAR(255), published_at VARCHAR(255),
                    ib_ee_data TEXT, is_ib_sample VARCHAR(10),
                    is_anonymous VARCHAR(10), cp_data TEXT, ia_data TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            conn.execute(text(f"""
                CREATE TABLE papers_chunks (
                    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    filename VARCHAR(255), chunk_index INTEGER, content TEXT,
                    embedding_vec VECTOR({RAG_EMBED_DIM}), lang VARCHAR(10),
                    INDEX ix_papers_chunks_filename (filename)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            conn.execute(text("""
                CREATE TABLE submissions (
                    id VARCHAR(255) NOT NULL PRIMARY KEY,
                    pdf_filename VARCHAR(255), pending_filename VARCHAR(255),
                    title VARCHAR(255), author_name VARCHAR(255),
                    author_email VARCHAR(255), author_school VARCHAR(255),
                    status VARCHAR(50), submitted_at VARCHAR(255), feedback TEXT,
                    abstract TEXT, keywords TEXT, journal VARCHAR(255),
                    category VARCHAR(255), language VARCHAR(255),
                    submitted_by VARCHAR(255), original_filename VARCHAR(255),
                    ib_ee_data TEXT, is_ib_sample VARCHAR(10),
                    is_anonymous VARCHAR(10), cp_data TEXT, ia_data TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

    def _config(self):
        config_path = Path(self.temp_dir.name) / "alembic.ini"
        config_path.write_text(
            "[alembic]\n"
            f"script_location = {ROOT / 'migrations'}\n",
            encoding="utf-8",
        )
        config = Config(str(config_path))
        config.attributes["papers_dir"] = self.papers
        connection = self.engine.connect()
        self.addCleanup(connection.close)
        config.attributes["connection"] = connection
        return config

    def test_real_mysql_upgrade_preserves_vectors_and_installs_contract(self):
        (self.papers / "one.pdf").write_bytes(b"%PDF-1.4\none")
        (self.papers / "two.pdf").write_bytes(b"%PDF-1.4\ntwo")
        vector_json = json.dumps([0.25] + [0.0] * (RAG_EMBED_DIM - 1))
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_metadata (filename, title)
                VALUES ('one.pdf', 'One'), ('two.pdf', 'Two')
            """))
            conn.execute(text("""
                INSERT INTO papers_chunks (
                    filename, chunk_index, content, embedding_vec, lang
                ) VALUES (
                    'one.pdf', 0, 'chunk', STRING_TO_VECTOR(:vector), 'en'
                )
            """), {"vector": vector_json})
            conn.execute(text("""
                INSERT INTO submissions (id, status, pdf_filename, pending_filename, feedback)
                VALUES
                    ('linked', 'accepted', 'one.pdf', NULL, 'approved'),
                    ('unmatched', 'accepted', 'absent.pdf', NULL, NULL),
                    ('ambiguous', 'accepted', 'one.pdf', 'two.pdf', NULL),
                    ('pending', 'pending', 'one.pdf', NULL, NULL),
                    ('rejected', 'rejected', NULL, 'gone.pdf', 'declined')
            """))
            raw_before = conn.execute(text(
                "SELECT embedding_vec FROM papers_chunks WHERE filename='one.pdf'"
            )).scalar_one()
        count_before, fingerprint_before = legacy_chunk_fingerprint(
            self.engine, "one.pdf",
        )
        self.assertEqual(count_before, 1)

        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "head")

        with self.engine.connect() as conn:
            current = MigrationContext.configure(conn).get_current_revision()
            paper = conn.execute(text("""
                SELECT id, lifecycle_state, current_revision, index_status,
                       indexed_revision FROM papers_metadata
                WHERE filename='one.pdf'
            """)).mappings().one()
            raw_after = conn.execute(text(
                "SELECT embedding_vec FROM papers_chunks WHERE filename='one.pdf'"
            )).scalar_one()
            stored_fingerprint = conn.execute(text("""
                SELECT legacy_chunk_fingerprint
                FROM publishing_migration_journal WHERE legacy_key='one.pdf'
            """)).scalar_one()
            issues = conn.execute(text("""
                SELECT kind, paper_id, blocking
                FROM publishing_migration_issues ORDER BY kind
            """)).mappings().all()
            alias_count = conn.execute(text(
                "SELECT COUNT(*) FROM paper_filename_aliases"
            )).scalar_one()
            chunk_ddl = conn.execute(text("SHOW CREATE TABLE papers_chunks")).one()[1]
            submission_ddl = conn.execute(text("SHOW CREATE TABLE submissions")).one()[1]
        self.assertEqual(current, ScriptDirectory.from_config(config).get_current_head())
        self.assertEqual(current, "0003_publishing_contract")
        self.assertEqual(bytes(raw_after), bytes(raw_before))
        self.assertEqual(stored_fingerprint, fingerprint_before)
        self.assertEqual(paper.lifecycle_state, "published")
        self.assertEqual(paper.current_revision, 1)
        self.assertEqual(paper.index_status, "ready")
        self.assertEqual(paper.indexed_revision, 1)
        self.assertEqual(alias_count, 2)
        self.assertEqual(
            [(row.kind, row.paper_id, bool(row.blocking)) for row in issues],
            [
                ("submission_ambiguous", None, False),
                ("submission_unmatched", None, False),
            ],
        )
        self.assertIn("uq_papers_chunks_paper_revision_chunk", chunk_ddl)
        self.assertIn("ON DELETE CASCADE", chunk_ddl.upper())
        self.assertIn("ON DELETE SET NULL", submission_ddl.upper())

        replay = backfill_one_paper(self.engine, self.papers, "one.pdf")
        self.assertTrue(replay.resumed)
        self.assertEqual(replay.paper_id, paper.id)
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text(
                "SELECT COUNT(*) FROM paper_revisions WHERE paper_id=:paper_id"
            ), {"paper_id": paper.id}).scalar_one(), 1)
            self.assertEqual(conn.execute(text(
                "SELECT value FROM rag_index_meta WHERE name='chunks_version'"
            )).scalar_one(), 1)

        # The upgraded database must agree with the Task 2 ORM metadata.
        command.check(config)

    def test_mixed_utf8mb4_collations_block_before_foreign_key_ddl(self):
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE submissions
                CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
            """))
        report = run_preflight(self.engine, self.papers)
        self.assertIn(
            ("unexpected_legacy_schema", "paper_identity_collation"),
            tuple((issue.code, issue.legacy_key) for issue in report.blockers),
        )

    def test_opaque_identity_keys_are_binary_in_expand_and_final_schema(self):
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")

        def paper_collations():
            with self.engine.connect() as conn:
                return dict(conn.execute(text("""
                    SELECT COLUMN_NAME, COLLATION_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=DATABASE()
                      AND TABLE_NAME='papers_metadata'
                      AND COLUMN_NAME IN (
                          'filename', 'direct_idempotency_key',
                          'origin_submission_id'
                      )
                """)).all())

        def submission_collations():
            with self.engine.connect() as conn:
                return dict(conn.execute(text("""
                    SELECT COLUMN_NAME, COLLATION_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=DATABASE()
                      AND TABLE_NAME='submissions'
                      AND COLUMN_NAME IN ('id', 'decision_idempotency_key')
                """)).all())

        self.assertEqual(
            paper_collations(),
            {
                "filename": "utf8mb4_bin",
                "direct_idempotency_key": "utf8mb4_bin",
                "origin_submission_id": "utf8mb4_bin",
            },
        )
        self.assertEqual(
            submission_collations(),
            {
                "id": "utf8mb4_bin",
                "decision_idempotency_key": "utf8mb4_bin",
            },
        )
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE submissions
                    MODIFY COLUMN id VARCHAR(255)
                        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                    MODIFY COLUMN decision_idempotency_key VARCHAR(255)
                        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL
            """))
            conn.execute(text("""
                ALTER TABLE papers_metadata
                    MODIFY COLUMN origin_submission_id VARCHAR(255)
                        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL
            """))
        command.upgrade(config, "head")
        self.assertEqual(
            paper_collations(),
            {
                "filename": "utf8mb4_bin",
                "direct_idempotency_key": "utf8mb4_bin",
                "origin_submission_id": "utf8mb4_bin",
            },
        )
        self.assertEqual(
            submission_collations(),
            {
                "id": "utf8mb4_bin",
                "decision_idempotency_key": "utf8mb4_bin",
            },
        )
        with self.engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO papers_metadata (
                    id, filename, lifecycle_state, current_revision,
                    row_version, index_status, direct_idempotency_key
                ) VALUES
                    ('00000000-0000-4000-8000-000000000001', 'Case.pdf',
                     'publishing', NULL, 0, 'pending', 'Opaque-Key'),
                    ('00000000-0000-4000-8000-000000000002', 'case.pdf',
                     'publishing', NULL, 0, 'pending', 'opaque-key')
            """))
            conn.execute(text("""
                INSERT INTO submissions (id, decision_idempotency_key) VALUES
                    ('Case-Submission', 'Decision-Key'),
                    ('case-submission', 'decision-key')
            """))
            conn.execute(text("""
                UPDATE papers_metadata
                SET origin_submission_id = CASE filename
                    WHEN 'Case.pdf' THEN 'Case-Submission'
                    ELSE 'case-submission'
                END
            """))
        with self.engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("""
                    SELECT COUNT(*) FROM papers_metadata
                    WHERE filename IN ('Case.pdf', 'case.pdf')
                """)).scalar_one(),
                2,
            )
            self.assertEqual(
                conn.execute(text("""
                    SELECT COUNT(*) FROM submissions
                    WHERE decision_idempotency_key
                        IN ('Decision-Key', 'decision-key')
                """)).scalar_one(),
                2,
            )
            self.assertEqual(
                conn.execute(text("""
                    SELECT COUNT(*) FROM papers_metadata
                    WHERE origin_submission_id
                        IN ('Case-Submission', 'case-submission')
                """)).scalar_one(),
                2,
            )

    def test_submission_creation_waits_for_global_identity_fence(self):
        from services.submissions import _save_submission

        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "head")
        session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

        @contextmanager
        def test_db_session():
            session = session_factory()
            try:
                yield session
            finally:
                session.close()

        started = threading.Event()
        finished = threading.Event()
        errors = []

        def create_submission():
            started.set()
            try:
                _save_submission({"id": "mysql-fenced-submission"})
            except Exception as exc:  # pragma: no cover - surfaced below
                errors.append(exc)
            finally:
                finished.set()

        with self.engine.connect() as holder:
            transaction = holder.begin()
            holder.execute(text("""
                UPDATE submission_identity_fence
                SET generation = generation + 1
                WHERE name = 'global'
            """))
            with mock.patch("services.submissions.db_session", test_db_session):
                worker = threading.Thread(target=create_submission)
                worker.start()
                self.assertTrue(started.wait(1))
                time.sleep(0.2)
                self.assertFalse(finished.is_set())
                transaction.commit()
                worker.join(5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        with self.engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("""
                    SELECT COUNT(*) FROM submissions
                    WHERE id = 'mysql-fenced-submission'
                """)).scalar_one(),
                1,
            )

    def test_expanded_preflight_rejects_nonbinary_raw_identity_collations(self):
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE papers_metadata
                    MODIFY COLUMN filename VARCHAR(255)
                        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                    MODIFY COLUMN direct_idempotency_key VARCHAR(255)
                        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
                    MODIFY COLUMN origin_submission_id VARCHAR(255)
                        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL
            """))
            conn.execute(text("""
                ALTER TABLE submissions
                    MODIFY COLUMN id VARCHAR(255)
                        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
                    MODIFY COLUMN decision_idempotency_key VARCHAR(255)
                        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL
            """))

        report = run_preflight(self.engine, self.papers)
        blocker_keys = {
            (issue.code, issue.legacy_key) for issue in report.blockers
        }
        self.assertIn(
            ("unexpected_legacy_schema", "papers_metadata.filename"),
            blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "papers_metadata.direct_idempotency_key"),
            blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "papers_metadata.origin_submission_id"),
            blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "submissions.id"),
            blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "submissions.decision_idempotency_key"),
            blocker_keys,
        )

    def test_partial_expand_shape_is_refused_before_more_ddl(self):
        with self.engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE submissions ADD COLUMN paper_id VARCHAR(36) NULL"
            ))
        report = run_preflight(self.engine, self.papers)
        self.assertIn(
            ("unexpected_legacy_schema", "publishing_schema_phase"),
            tuple((issue.code, issue.legacy_key) for issue in report.blockers),
        )
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        with self.assertRaisesRegex(
            RuntimeError,
            "unsafe partial publishing expand shape.*restore coordinated database and file snapshots",
        ):
            command.upgrade(config, "0001_publishing_expand")
        with self.engine.connect() as conn:
            paper_id_column = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME='papers_metadata' AND COLUMN_NAME='id'
            """)).scalar_one()
        self.assertEqual(paper_id_column, 0)

    def test_exact_expanded_shape_stamped_legacy_replays_without_duplicate_ddl(self):
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")

        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")

        with self.engine.connect() as connection:
            self.assertEqual(
                MigrationContext.configure(connection).get_current_revision(),
                "0001_publishing_expand",
            )
        self.assertEqual(
            inspect(self.engine).get_pk_constraint("papers_metadata")[
                "constrained_columns"
            ],
            ["filename"],
        )
        self.assertIn(
            "publishing_migration_state", inspect(self.engine).get_table_names(),
        )

    def test_missing_rag_index_meta_blocks_before_expand(self):
        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE rag_index_meta"))

        report = run_preflight(self.engine, self.papers)

        self.assertIn(
            ("unexpected_legacy_schema", "rag_index_meta"),
            tuple((issue.code, issue.legacy_key) for issue in report.blockers),
        )

    def test_malformed_expanded_index_contract_blocks_backfill(self):
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE papers_metadata
                RENAME INDEX ux_papers_metadata_migration_id
                TO renamed_migration_uuid_index
            """))

        report = run_preflight(self.engine, self.papers)

        self.assertIn(
            ("unexpected_legacy_schema", "papers_metadata"),
            tuple((issue.code, issue.legacy_key) for issue in report.blockers),
        )

    def test_expanded_core_and_rag_types_are_exactly_validated(self):
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE papers_metadata
                MODIFY COLUMN reservation_expires_at VARCHAR(255) NULL
            """))
            conn.execute(text("""
                ALTER TABLE submissions
                MODIFY COLUMN reviewer VARCHAR(36) NULL
            """))
            conn.execute(text("""
                ALTER TABLE rag_index_meta
                MODIFY COLUMN value BIGINT NOT NULL
            """))

        report = run_preflight(self.engine, self.papers)
        blocker_keys = {
            (issue.code, issue.legacy_key) for issue in report.blockers
        }
        self.assertIn(
            ("unexpected_legacy_schema", "papers_metadata.reservation_expires_at"),
            blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "submissions.reviewer"),
            blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "rag_index_meta"),
            blocker_keys,
        )

    def test_expanded_infrastructure_types_preserve_exact_mysql_definitions(self):
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0001_publishing_expand")
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE publishing_jobs
                MODIFY COLUMN attempts BIGINT NOT NULL,
                MODIFY COLUMN available_at TIMESTAMP NOT NULL
            """))
            conn.execute(text("""
                ALTER TABLE publishing_migration_state
                MODIFY COLUMN paper_count INT UNSIGNED NOT NULL
            """))
            conn.execute(text("""
                ALTER TABLE publishing_migration_issues
                MODIFY COLUMN details MEDIUMTEXT NOT NULL
            """))
            conn.execute(text("""
                ALTER TABLE rag_index_meta
                MODIFY COLUMN value INT UNSIGNED NOT NULL
            """))

        report = run_preflight(self.engine, self.papers)
        blocker_keys = {
            (issue.code, issue.legacy_key) for issue in report.blockers
        }
        self.assertIn(
            ("unexpected_legacy_schema", "publishing_jobs"), blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "publishing_migration_state"),
            blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "publishing_migration_issues"),
            blocker_keys,
        )
        self.assertIn(
            ("unexpected_legacy_schema", "rag_index_meta"), blocker_keys,
        )

    def test_same_named_malformed_lifecycle_check_requires_snapshot_restore(self):
        (self.papers / "paper.pdf").write_bytes(b"%PDF-1.4\n")
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO papers_metadata (filename) VALUES ('paper.pdf')"
            ))
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0002_publishing_backfill")
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE papers_metadata
                    MODIFY COLUMN id VARCHAR(36) NOT NULL,
                    MODIFY COLUMN lifecycle_state VARCHAR(16) NOT NULL,
                    MODIFY COLUMN row_version INTEGER NOT NULL,
                    MODIFY COLUMN index_status VARCHAR(16) NOT NULL,
                    DROP PRIMARY KEY,
                    ADD CONSTRAINT pk_papers_metadata PRIMARY KEY (id),
                    ADD CONSTRAINT uq_papers_metadata_filename UNIQUE (filename),
                    DROP INDEX ux_papers_metadata_migration_id,
                    ADD CONSTRAINT ck_papers_metadata_lifecycle_revision CHECK (
                        lifecycle_state IN ('publishing', 'published', 'deleting')
                    )
            """))

        with self.assertRaisesRegex(
            RuntimeError,
            "unsafe partial publishing contract shape.*restore coordinated database and file snapshots",
        ):
            command.upgrade(config, "0003_publishing_contract")

    def test_lifecycle_check_with_unsupported_suffix_requires_snapshot_restore(self):
        (self.papers / "paper.pdf").write_bytes(b"%PDF-1.4\n")
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO papers_metadata (filename) VALUES ('paper.pdf')"
            ))
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0002_publishing_backfill")
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE papers_metadata
                    MODIFY COLUMN id VARCHAR(36) NOT NULL,
                    MODIFY COLUMN lifecycle_state VARCHAR(16) NOT NULL,
                    MODIFY COLUMN row_version INTEGER NOT NULL,
                    MODIFY COLUMN index_status VARCHAR(16) NOT NULL,
                    DROP PRIMARY KEY,
                    ADD CONSTRAINT pk_papers_metadata PRIMARY KEY (id),
                    ADD CONSTRAINT uq_papers_metadata_filename UNIQUE (filename),
                    DROP INDEX ux_papers_metadata_migration_id,
                    ADD CONSTRAINT ck_papers_metadata_lifecycle_revision CHECK (
                        (
                            (lifecycle_state = 'publishing' AND current_revision IS NULL)
                            OR
                            (lifecycle_state IN ('published', 'deleting')
                             AND current_revision IS NOT NULL)
                        ) + 1
                    )
            """))

        with self.assertRaisesRegex(
            RuntimeError,
            "unsafe partial publishing contract shape.*restore coordinated database and file snapshots",
        ):
            command.upgrade(config, "0003_publishing_contract")

    def test_partial_mysql_contract_shape_repairs_forward(self):
        (self.papers / "paper.pdf").write_bytes(b"%PDF-1.4\n")
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO papers_metadata (filename) VALUES ('paper.pdf')"
            ))
        config = self._config()
        command.stamp(config, "0000_legacy_baseline")
        command.upgrade(config, "0002_publishing_backfill")

        # Simulate MySQL committing the first atomic contract group while
        # Alembic remains stamped at 0002 and the checkpoint remains expanded.
        with self.engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE papers_metadata
                    MODIFY COLUMN id VARCHAR(36) NOT NULL,
                    MODIFY COLUMN lifecycle_state VARCHAR(16) NOT NULL,
                    MODIFY COLUMN row_version INTEGER NOT NULL,
                    MODIFY COLUMN index_status VARCHAR(16) NOT NULL,
                    DROP PRIMARY KEY,
                    ADD CONSTRAINT pk_papers_metadata PRIMARY KEY (id),
                    ADD CONSTRAINT uq_papers_metadata_filename UNIQUE (filename),
                    DROP INDEX ux_papers_metadata_migration_id,
                    ADD CONSTRAINT ck_papers_metadata_lifecycle_revision CHECK (
                        (lifecycle_state = 'publishing' AND current_revision IS NULL) OR
                        (lifecycle_state IN ('published', 'deleting') AND current_revision IS NOT NULL)
                    )
            """))
            conn.execute(text("""
                ALTER TABLE paper_revisions
                    ADD CONSTRAINT fk_paper_revisions_paper
                    FOREIGN KEY (paper_id) REFERENCES papers_metadata (id)
                    ON DELETE CASCADE
            """))
        self.assertEqual(
            inspect(self.engine).get_pk_constraint("papers_metadata")["constrained_columns"],
            ["id"],
        )

        command.upgrade(config, "0003_publishing_contract")

        self.assertEqual(
            inspect(self.engine).get_pk_constraint("papers_metadata")["constrained_columns"],
            ["id"],
        )
        with self.engine.connect() as conn:
            self.assertEqual(conn.execute(text("""
                SELECT ddl_phase FROM publishing_migration_state
                WHERE name='pre_backfill'
            """)).scalar_one(), "complete")


if __name__ == "__main__":
    unittest.main()
