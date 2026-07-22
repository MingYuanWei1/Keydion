#!/usr/bin/env python3
"""Verify that code and database expose one identical Alembic head.

This is intentionally executable deployment behavior rather than a revision
string embedded in documentation.  It exits non-zero for multiple code heads,
an unversioned/behind/ahead database, or ORM migration drift.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


ROOT = Path(__file__).resolve().parents[1]


def alembic_config() -> Config:
    return Config(str(ROOT / "alembic.ini"))


def single_code_head(config: Config | None = None) -> str:
    configured = config or alembic_config()
    heads = tuple(ScriptDirectory.from_config(configured).get_heads())
    if len(heads) != 1:
        raise RuntimeError(
            f"expected exactly one Alembic code head, found {len(heads)}: {heads!r}"
        )
    return heads[0]


def verify_database(database_url: str, config: Config | None = None) -> str:
    configured = config or alembic_config()
    expected = single_code_head(configured)
    engine = create_engine(database_url, poolclass=NullPool)
    try:
        with engine.connect() as connection:
            current = tuple(MigrationContext.configure(connection).get_current_heads())
            if current != (expected,):
                raise RuntimeError(
                    "database Alembic heads do not match code head: "
                    f"database={current!r}, code={(expected,)!r}"
                )
            configured.attributes["connection"] = connection
            try:
                command.check(configured)
            finally:
                configured.attributes.pop("connection", None)
    finally:
        engine.dispose()
    return expected


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--code-only",
        action="store_true",
        help="verify and print the single code head without connecting to a database",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.code_only:
        head = single_code_head()
    else:
        database_url = (os.environ.get("PAPERQUERY_DATABASE_URL") or "").strip()
        if not database_url:
            raise SystemExit("PAPERQUERY_DATABASE_URL is required")
        head = verify_database(database_url)
    print(head)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
