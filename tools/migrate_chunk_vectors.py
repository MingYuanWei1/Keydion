"""Retired compatibility entry point for the former vector backfill tool.

Chunk schema/data transitions are Alembic-owned. Runtime tools must never
mutate Paper chunks outside the publishing lifecycle transaction.
"""

from __future__ import annotations

import sys


def main() -> int:
    print(
        "Vector migration is schema-managed; run the approved Alembic upgrade.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
