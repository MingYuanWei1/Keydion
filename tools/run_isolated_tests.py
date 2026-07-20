"""Run unittest targets against a generated disposable MySQL database."""
import os
import re
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


_TEST_DATABASE_RE = re.compile(r"keydion_test_[0-9a-f]{32}\Z")


def _validated_admin_url(raw_url: str) -> None:
    url = make_url(raw_url)
    if url.get_backend_name() != "mysql":
        raise ValueError("PAPERQUERY_TEST_MYSQL_ADMIN_URL must use MySQL")
    if url.database and not url.database.startswith("keydion_test_"):
        raise ValueError(
            f"refusing non-test database in admin URL: {url.database!r}"
        )


def _validate_generated_database(database_name: str) -> None:
    if not _TEST_DATABASE_RE.fullmatch(database_name):
        raise ValueError(f"refusing unsafe generated database name: {database_name!r}")


def _server_url(raw_url: str):
    return make_url(raw_url).set(database=None)


def _create_database(admin_url: str, database_name: str) -> None:
    _validate_generated_database(database_name)
    engine = create_engine(_server_url(admin_url), pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"CREATE DATABASE `{database_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            )
    finally:
        engine.dispose()


def _drop_database(admin_url: str, database_name: str) -> None:
    _validate_generated_database(database_name)
    engine = create_engine(_server_url(admin_url), pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{database_name}`"))
    finally:
        engine.dispose()


def _run_child(argv: list[str], env: dict[str, str]) -> int:
    completed = subprocess.run(
        [sys.executable, "-m", "unittest", *argv],
        env=env,
        check=False,
    )
    return completed.returncode


def main(argv: list[str] | None = None) -> int:
    admin_url = os.environ.get("PAPERQUERY_TEST_MYSQL_ADMIN_URL")
    if not admin_url:
        raise RuntimeError("PAPERQUERY_TEST_MYSQL_ADMIN_URL is required")
    _validated_admin_url(admin_url)

    database_name = f"keydion_test_{uuid.uuid4().hex}"
    _validate_generated_database(database_name)
    test_url = make_url(admin_url).set(database=database_name)

    with tempfile.TemporaryDirectory(prefix="keydion-test-data-") as data_dir, \
         tempfile.TemporaryDirectory(prefix="keydion-test-papers-") as upload_dir:
        child_env = os.environ.copy()
        child_env.update(
            {
                "PAPERQUERY_DATABASE_URL": test_url.render_as_string(
                    hide_password=False
                ),
                "PAPERQUERY_TEST_MYSQL_URL": test_url.render_as_string(
                    hide_password=False
                ),
                "PAPERQUERY_DATA_DIR": str(Path(data_dir)),
                "PAPERQUERY_UPLOAD_DIR": str(Path(upload_dir)),
                "PAPERQUERY_SECRET": "isolated-test-secret",
                "PAPERQUERY_ALLOW_DEV_SECRET": "1",
                "PAPERQUERY_COOKIE_SECURE": "0",
            }
        )

        primary_error = None
        child_return_code = None
        try:
            _create_database(admin_url, database_name)
            child_return_code = _run_child(list(argv or ()), child_env)
            return child_return_code
        except BaseException as exc:
            primary_error = exc
            raise
        finally:
            try:
                _drop_database(admin_url, database_name)
            except BaseException:
                if primary_error is not None:
                    primary_error.add_note("isolated database cleanup also failed")
                elif child_return_code is not None and child_return_code != 0:
                    print(
                        "warning: isolated database cleanup failed; "
                        "preserving child exit code",
                        file=sys.stderr,
                    )
                else:
                    raise


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
