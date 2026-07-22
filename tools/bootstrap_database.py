"""Explicit, locked bootstrap for a completely empty Keydion database."""

from __future__ import annotations

import argparse
import os

from sqlalchemy import create_engine

from models import bootstrap_empty_database


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("PAPERQUERY_DATABASE_URL"),
        help="SQLAlchemy URL (defaults to PAPERQUERY_DATABASE_URL)",
    )
    parser.add_argument(
        "--confirm-empty-bootstrap",
        action="store_true",
        help="Confirm that the target must be completely empty",
    )
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or PAPERQUERY_DATABASE_URL is required")
    if not args.confirm_empty_bootstrap:
        parser.error("--confirm-empty-bootstrap is required")

    engine = create_engine(args.database_url, pool_pre_ping=True)
    try:
        head = bootstrap_empty_database(engine)
    finally:
        engine.dispose()
    print(f"Bootstrapped empty database at Alembic head {head}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
