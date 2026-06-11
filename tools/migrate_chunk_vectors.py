# tools/migrate_chunk_vectors.py
"""One-time backfill: papers_chunks.embedding (JSON text) -> embedding_vec (VECTOR).

Requires MySQL 9.x and the release that added embedding_vec. Resumable: only
rows with embedding_vec IS NULL are touched, so an interrupted run continues
where it left off. Bumps the rag_index_meta stamp at the end, so live gunicorn
workers pick up the result on their next query — no restart needed.

Usage:
    python3 tools/migrate_chunk_vectors.py              # backfill missing vectors
    python3 tools/migrate_chunk_vectors.py --drop-json  # drop the legacy column
                                                        # (refuses while rows remain unmigrated)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app  # noqa: E402  -- runs init_db (schema migrations) at import

from sqlalchemy import text  # noqa: E402

import db as db_module       # noqa: E402
from db import db_session    # noqa: E402
from services.ask import bump_chunks_version  # noqa: E402

BATCH = 500


def _legacy_column_exists(engine) -> bool:
    with engine.connect() as conn:
        count = conn.execute(text(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'papers_chunks' "
            "AND column_name = 'embedding'")).scalar()
    return bool(count)


def _vector_column_exists(engine) -> bool:
    with engine.connect() as conn:
        count = conn.execute(text(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'papers_chunks' "
            "AND column_name = 'embedding_vec'")).scalar()
    return bool(count)


def backfill(engine) -> int:
    migrated = 0
    failed = 0
    last_id = 0
    while True:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, embedding FROM papers_chunks "
                "WHERE id > :last AND embedding_vec IS NULL "
                "AND embedding IS NOT NULL AND embedding != '' AND embedding != '[]' "
                "ORDER BY id LIMIT :n"), {"last": last_id, "n": BATCH}).fetchall()
            if not rows:
                break
            for rid, emb in rows:
                last_id = rid       # keyset pagination: failed rows don't loop forever
                try:
                    conn.execute(text(
                        "UPDATE papers_chunks "
                        "SET embedding_vec = STRING_TO_VECTOR(:e) WHERE id = :id"),
                        {"e": emb, "id": rid})
                    migrated += 1
                except Exception as exc:
                    failed += 1
                    print(f"  row {rid}: {exc}", file=sys.stderr)
            conn.commit()
            print(f"  ...{migrated} rows migrated")
    if failed:
        print(f"{failed} rows failed to convert and were left unmigrated.",
              file=sys.stderr)
    return migrated


def _unmigrated_count(engine) -> int:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT COUNT(*) FROM papers_chunks "
            "WHERE embedding_vec IS NULL "
            "AND embedding IS NOT NULL AND embedding != '' AND embedding != '[]'"
        )).scalar()


def drop_json_column(engine) -> int:
    remaining = _unmigrated_count(engine)
    if remaining:
        print(f"Refusing to drop: {remaining} rows still hold legacy JSON without "
              "a converted vector (never migrated, or failed conversion — check "
              "the backfill run's stderr). Fix or delete them first.",
              file=sys.stderr)
        return 1
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE papers_chunks DROP COLUMN embedding"))
        conn.commit()
    print("Dropped legacy papers_chunks.embedding column.")
    return 0


def main() -> int:
    app.init_db()
    engine = db_module.get_engine()
    if not _legacy_column_exists(engine):
        print("No legacy embedding column found — nothing to migrate.")
        return 0
    if not _vector_column_exists(engine):
        print("papers_chunks.embedding_vec is missing — the schema migration "
              "did not run. Upgrade MySQL to 9.x and restart the app first.",
              file=sys.stderr)
        return 1
    if "--drop-json" in sys.argv:
        return drop_json_column(engine)
    before = _unmigrated_count(engine)
    backfill(engine)
    after = _unmigrated_count(engine)
    converted = before - after
    if converted:
        with db_session() as db:
            bump_chunks_version(db)
        print(f"Done: {converted} vectors backfilled; version stamp bumped "
              "(live workers refresh on their next query).")
    else:
        print("Nothing to backfill.")
    if after:
        print(f"{after} rows remain unmigrated — fix or delete them, then re-run.",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
