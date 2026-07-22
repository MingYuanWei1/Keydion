import tempfile
import unittest
from pathlib import Path
from unittest import mock

from alembic.config import Config
from sqlalchemy import create_engine, text

import models
from tools.verify_alembic_state import single_code_head, verify_database


ROOT = Path(__file__).resolve().parents[1]


class VerifyAlembicStateTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.database_path = Path(self.temp_dir.name) / "verify.sqlite"
        self.database_url = f"sqlite:///{self.database_path}"
        self.config_path = Path(self.temp_dir.name) / "alembic.ini"
        self.config_path.write_text(
            "[alembic]\n" f"script_location = {ROOT / 'migrations'}\n",
            encoding="utf-8",
        )
        self.config = Config(str(self.config_path))

    def test_reports_current_single_head(self):
        self.assertEqual(single_code_head(self.config), "0007_content_integrity_jobs")

    def test_current_database_and_model_drift_check_pass(self):
        engine = create_engine(self.database_url)
        self.addCleanup(engine.dispose)
        with mock.patch.object(models, "_alembic_config", return_value=self.config):
            models.bootstrap_empty_database(engine)
        self.assertEqual(
            verify_database(self.database_url, self.config),
            "0007_content_integrity_jobs",
        )

    def test_wrong_database_revision_fails_closed(self):
        engine = create_engine(self.database_url)
        self.addCleanup(engine.dispose)
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(64))"))
            connection.execute(text("INSERT INTO alembic_version VALUES ('unexpected')"))
        with self.assertRaisesRegex(RuntimeError, "do not match"):
            verify_database(self.database_url, self.config)

    def test_multiple_code_heads_fail_closed(self):
        directory = mock.Mock()
        directory.get_heads.return_value = ["one", "two"]
        with mock.patch(
            "tools.verify_alembic_state.ScriptDirectory.from_config",
            return_value=directory,
        ):
            with self.assertRaisesRegex(RuntimeError, "exactly one"):
                single_code_head(self.config)


if __name__ == "__main__":
    unittest.main()
