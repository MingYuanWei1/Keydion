import tempfile
import unittest
from pathlib import Path
from unittest import mock

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

import db
import models


ROOT = Path(__file__).resolve().parents[1]


class AlembicRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        database_path = Path(self.temp_dir.name) / "runtime.sqlite"
        self.engine = create_engine(f"sqlite:///{database_path}")
        self.addCleanup(self.engine.dispose)

        config_path = Path(self.temp_dir.name) / "alembic.ini"
        config_path.write_text(
            "[alembic]\n"
            f"script_location = {ROOT / 'migrations'}\n",
            encoding="utf-8",
        )
        alembic_config = Config(str(config_path))
        self.config_patch = mock.patch.object(
            models,
            "_alembic_config",
            return_value=alembic_config,
            create=True,
        )
        self.config_patch.start()
        self.addCleanup(self.config_patch.stop)

    def test_empty_database_is_created_and_stamped(self):
        models.ensure_schema_current(self.engine)

        with self.engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("SELECT version_num FROM alembic_version")).scalar(),
                models._alembic_head(),
            )
            self.assertEqual(
                conn.execute(
                    text(
                        "SELECT value FROM rag_index_meta "
                        "WHERE name = 'chunks_version'"
                    )
                ).scalar(),
                0,
            )

    def test_current_database_is_accepted(self):
        models.ensure_schema_current(self.engine)
        models.ensure_schema_current(self.engine)

    def test_fresh_database_has_no_alembic_drift(self):
        models.ensure_schema_current(self.engine)
        alembic_config = models._alembic_config()
        with self.engine.connect() as conn:
            alembic_config.attributes["connection"] = conn
            try:
                command.check(alembic_config)
            finally:
                alembic_config.attributes.pop("connection", None)

    def test_nonempty_unversioned_database_refuses_startup(self):
        with self.engine.begin() as conn:
            conn.execute(text("CREATE TABLE legacy_marker (id INTEGER PRIMARY KEY)"))

        with self.assertRaisesRegex(RuntimeError, "database schema is unversioned"):
            models.ensure_schema_current(self.engine)

    def test_behind_or_ahead_database_refuses_startup(self):
        with self.engine.begin() as conn:
            conn.execute(text("CREATE TABLE legacy_marker (id INTEGER PRIMARY KEY)"))
            conn.execute(
                text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
            )
            conn.execute(
                text("INSERT INTO alembic_version (version_num) VALUES ('unexpected')")
            )

        with self.assertRaisesRegex(RuntimeError, "does not match code head"):
            models.ensure_schema_current(self.engine)

    def test_failed_init_does_not_publish_engine_or_session_factory(self):
        first_engine = mock.Mock()
        second_engine = mock.Mock()
        session_factory = mock.sentinel.session_factory

        with mock.patch.object(db, "DB_URL", "sqlite://"), \
             mock.patch.object(db, "_ENGINE", None), \
             mock.patch.object(db, "_SESSION_LOCAL", None), \
             mock.patch.object(
                 models,
                 "create_engine",
                 side_effect=[first_engine, second_engine],
             ), \
             mock.patch.object(
                 models,
                 "ensure_schema_current",
                 side_effect=[RuntimeError("schema mismatch"), None],
                 create=True,
             ) as ensure, \
             mock.patch.object(
                 models,
                 "sessionmaker",
                 return_value=session_factory,
             ):
            with self.assertRaisesRegex(RuntimeError, "schema mismatch"):
                models.init_db()

            self.assertIsNone(db._ENGINE)
            self.assertIsNone(db._SESSION_LOCAL)
            first_engine.dispose.assert_called_once_with()

            models.init_db()

            self.assertIs(db._ENGINE, second_engine)
            self.assertIs(db._SESSION_LOCAL, session_factory)
            self.assertEqual(ensure.call_count, 2)


if __name__ == "__main__":
    unittest.main()
