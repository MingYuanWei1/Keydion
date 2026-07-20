#!/usr/bin/env python3
"""Read-only operator preflight for the Paper identity migration."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine

import config
from services.publishing_migration import run_preflight


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inventory Paper migration blockers without changing SQL or files",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("PAPERQUERY_DATABASE_URL"),
        help="SQLAlchemy URL (defaults to PAPERQUERY_DATABASE_URL)",
    )
    parser.add_argument(
        "--papers-dir",
        type=Path,
        default=config.PAPERS_DIR,
        help="flat legacy PDF directory (defaults to PAPERQUERY_UPLOAD_DIR)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.database_url:
        _parser().error(
            "--database-url or PAPERQUERY_DATABASE_URL is required"
        )
    engine = create_engine(args.database_url, pool_pre_ping=True)
    try:
        report = run_preflight(engine, args.papers_dir)
    finally:
        engine.dispose()

    for name in (
        "metadata_count", "flat_pdf_count", "total_pdf_bytes",
        "submission_count", "accepted_submission_count",
        "pending_submission_count", "rejected_submission_count",
        "chunk_count", "vector_count",
    ):
        print(f"{name}={getattr(report, name)}")
    print(f"importable_file_only={len(report.importable_file_only)}")
    for legacy_key in report.importable_file_only:
        print(f"file_only\t{legacy_key}")
    for submission_id in report.unavailable_rejected_pdfs:
        print(f"rejected_pdf_unavailable\t{submission_id}")
    print(f"issues={len(report.issues)}")
    print(f"blockers={len(report.blockers)}")
    for issue in report.issues:
        key = "-" if issue.legacy_key is None else issue.legacy_key
        print(
            f"{issue.code}\t{key}\t{issue.details}\t"
            f"blocking={'yes' if issue.blocking else 'no'}"
        )
    return 2 if report.blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
