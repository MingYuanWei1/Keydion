"""Hash current Paper revisions and cache verified/corrupt visibility state."""

from __future__ import annotations

import argparse

from db import get_session_factory
from services.paper_integrity import scan_current_revisions
from services.publishing_wiring import _build_storage


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-id", help="scan exactly one canonical Paper UUID")
    args = parser.parse_args(argv)
    session_factory = get_session_factory()
    storage = _build_storage()
    try:
        result = scan_current_revisions(
            session_factory=session_factory,
            storage=storage,
            paper_id=args.paper_id,
        )
    finally:
        storage.close()
    print(
        f"checked={result.checked} verified={result.verified} "
        f"corrupt={result.corrupt} stale={result.stale}"
    )
    return 1 if result.corrupt else 0


if __name__ == "__main__":
    raise SystemExit(main())
