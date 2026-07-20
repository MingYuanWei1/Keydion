import json
import os
import re
import tempfile
import unittest
import uuid
from pathlib import Path

# Capture only the caller's explicit environment before importing application
# config, which may load .env.prod.  The migration suite must never discover or
# use ambient deployment credentials.
MYSQL_ADMIN_URL = os.environ.get("PAPERQUERY_TEST_MYSQL_ADMIN_URL")

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

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
            "publishing_migration_issues",
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
        with self.assertRaisesRegex(RuntimeError, "publishing expand preflight blocked"):
            command.upgrade(config, "0001_publishing_expand")
        with self.engine.connect() as conn:
            paper_id_column = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=DATABASE()
                  AND TABLE_NAME='papers_metadata' AND COLUMN_NAME='id'
            """)).scalar_one()
        self.assertEqual(paper_id_column, 0)


if __name__ == "__main__":
    unittest.main()
