"""Enqueue/recover lifecycle-owned indexing for visible Paper revisions.

Usage:
    python3 tools/build_embeddings.py
    python3 tools/build_embeddings.py --rebuild
"""

from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import or_  # noqa: E402

import llm_client  # noqa: E402
from db import db_session  # noqa: E402
from models import PaperMetadataModel, init_db  # noqa: E402
from services.publishing_wiring import build_publishing_lifecycle  # noqa: E402


def _targets(*, rebuild: bool):
    with db_session() as db:
        query = db.query(
            PaperMetadataModel.id,
            PaperMetadataModel.current_revision,
            PaperMetadataModel.filename,
        ).filter(
            PaperMetadataModel.lifecycle_state == "published",
            PaperMetadataModel.current_revision.isnot(None),
        )
        if not rebuild:
            query = query.filter(
                or_(
                    PaperMetadataModel.index_status.in_(("pending", "failed")),
                    PaperMetadataModel.indexed_revision.is_(None),
                    PaperMetadataModel.indexed_revision
                    != PaperMetadataModel.current_revision,
                )
            )
        return list(query.order_by(PaperMetadataModel.id).all())


def main() -> int:
    if not llm_client.embedding_enabled():
        print("Embedding credentials are not configured.", file=sys.stderr)
        return 1

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    rebuild = "--rebuild" in sys.argv
    init_db()
    lifecycle = build_publishing_lifecycle()
    targets = _targets(rebuild=rebuild)

    recovered = 0
    for index, (paper_id, revision, filename) in enumerate(targets, 1):
        print(f"[{index}/{len(targets)}] {filename}")
        outcome = lifecycle.ensure_index_job(paper_id, revision)
        if outcome.job_id is not None:
            lifecycle.recover_job(outcome.job_id)
            recovered += 1

    print(f"Done: {len(targets)} visible revisions checked; {recovered} jobs recovered.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
