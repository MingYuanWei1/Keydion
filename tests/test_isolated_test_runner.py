import importlib.util
import os
import unittest
from pathlib import Path
from unittest import mock

from sqlalchemy.engine import make_url


ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "tools" / "run_isolated_tests.py"
ADMIN_URL = "mysql+pymysql://root:test@127.0.0.1:3306/?charset=utf8mb4"


class IsolatedTestRunnerTests(unittest.TestCase):
    def _load_runner(self):
        self.assertTrue(RUNNER_PATH.is_file(), "isolated test runner is not implemented")
        spec = importlib.util.spec_from_file_location("run_isolated_tests", RUNNER_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_child_failure_is_returned_and_generated_database_is_dropped(self):
        runner = self._load_runner()
        captured_env = {}

        def run_child(argv, env):
            self.assertEqual(argv, ["tests/test_example.py", "-v"])
            captured_env.update(env)
            return 7

        with mock.patch.dict(
            os.environ,
            {"PAPERQUERY_TEST_MYSQL_ADMIN_URL": ADMIN_URL},
            clear=True,
        ), mock.patch.object(runner, "_create_database") as create_database, \
             mock.patch.object(runner, "_run_child", side_effect=run_child), \
             mock.patch.object(runner, "_drop_database") as drop_database, \
             mock.patch.object(runner.uuid, "uuid4") as uuid4:
            uuid4.return_value.hex = "0123456789abcdef0123456789abcdef"
            result = runner.main(["tests/test_example.py", "-v"])

        self.assertEqual(result, 7)
        generated_name = "keydion_test_0123456789abcdef0123456789abcdef"
        create_database.assert_called_once_with(ADMIN_URL, generated_name)
        drop_database.assert_called_once_with(ADMIN_URL, generated_name)
        self.assertEqual(
            make_url(captured_env["PAPERQUERY_DATABASE_URL"]).database,
            generated_name,
        )
        self.assertEqual(
            captured_env["PAPERQUERY_TEST_MYSQL_URL"],
            captured_env["PAPERQUERY_DATABASE_URL"],
        )
        self.assertNotEqual(
            captured_env["PAPERQUERY_DATA_DIR"],
            captured_env["PAPERQUERY_UPLOAD_DIR"],
        )

    def test_create_failure_still_attempts_exact_generated_database_cleanup(self):
        runner = self._load_runner()
        generated_name = "keydion_test_0123456789abcdef0123456789abcdef"

        with mock.patch.dict(
            os.environ,
            {"PAPERQUERY_TEST_MYSQL_ADMIN_URL": ADMIN_URL},
            clear=True,
        ), mock.patch.object(
            runner,
            "_create_database",
            side_effect=RuntimeError("create failed"),
        ) as create_database, mock.patch.object(
            runner,
            "_drop_database",
            side_effect=RuntimeError("cleanup failed"),
        ) as drop_database, mock.patch.object(runner.uuid, "uuid4") as uuid4:
            uuid4.return_value.hex = "0123456789abcdef0123456789abcdef"
            with self.assertRaisesRegex(RuntimeError, "create failed"):
                runner.main([])

        create_database.assert_called_once_with(ADMIN_URL, generated_name)
        drop_database.assert_called_once_with(ADMIN_URL, generated_name)

    def test_cleanup_failure_does_not_mask_child_exception(self):
        runner = self._load_runner()
        generated_name = "keydion_test_0123456789abcdef0123456789abcdef"

        with mock.patch.dict(
            os.environ,
            {"PAPERQUERY_TEST_MYSQL_ADMIN_URL": ADMIN_URL},
            clear=True,
        ), mock.patch.object(runner, "_create_database"), mock.patch.object(
            runner,
            "_run_child",
            side_effect=RuntimeError("child failed"),
        ), mock.patch.object(
            runner,
            "_drop_database",
            side_effect=RuntimeError("cleanup failed"),
        ) as drop_database, mock.patch.object(runner.uuid, "uuid4") as uuid4:
            uuid4.return_value.hex = "0123456789abcdef0123456789abcdef"
            with self.assertRaisesRegex(RuntimeError, "child failed"):
                runner.main([])

        drop_database.assert_called_once_with(ADMIN_URL, generated_name)

    def test_drop_database_is_idempotent_for_validated_generated_name(self):
        runner = self._load_runner()
        generated_name = "keydion_test_0123456789abcdef0123456789abcdef"
        engine = mock.MagicMock()
        connection = engine.begin.return_value.__enter__.return_value

        with mock.patch.object(runner, "create_engine", return_value=engine):
            runner._drop_database(ADMIN_URL, generated_name)

        statement = str(connection.execute.call_args.args[0])
        self.assertEqual(
            statement,
            f"DROP DATABASE IF EXISTS `{generated_name}`",
        )

    def test_production_looking_admin_target_is_refused_before_create(self):
        runner = self._load_runner()
        production_url = (
            "mysql+pymysql://root:test@127.0.0.1:3306/keydion"
            "?charset=utf8mb4"
        )

        with mock.patch.dict(
            os.environ,
            {"PAPERQUERY_TEST_MYSQL_ADMIN_URL": production_url},
            clear=True,
        ), mock.patch.object(runner, "_create_database") as create_database:
            with self.assertRaisesRegex(ValueError, "refusing non-test database"):
                runner.main([])

        create_database.assert_not_called()

    def test_missing_admin_url_is_rejected(self):
        runner = self._load_runner()

        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "PAPERQUERY_TEST_MYSQL_ADMIN_URL"):
                runner.main([])


if __name__ == "__main__":
    unittest.main()
