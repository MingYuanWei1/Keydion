# tools/build_embeddings.py
"""Build the Ask-the-Library retrieval index over all published papers.

By default this RESUMES: papers that already have stored chunks are skipped, so
an interrupted run picks up where it left off. Pass --rebuild to force a full
re-index of every paper.

Usage:
    python3 tools/build_embeddings.py            # resume (skip already-indexed)
    python3 tools/build_embeddings.py --rebuild  # full rebuild
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app          # noqa: E402
import llm_client   # noqa: E402
import rag_index    # noqa: E402


def main() -> int:
    if not llm_client.llm_enabled():
        print("LLM_API_KEY is not set — cannot build embeddings.", file=sys.stderr)
        return 1
    # Surface rag_index/pdf_text progress (the [i/n] lines and OCR notes).
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    rebuild = "--rebuild" in sys.argv
    app.init_db()
    app.configure_rag()
    if rebuild:
        print("Rebuilding embedding index (all papers)...")
    else:
        print("Building embedding index (resuming; --rebuild forces a full rebuild)...")
    stats = rag_index.build_index(skip_existing=not rebuild)
    print(f"Done: {stats['papers']} papers indexed, "
          f"{stats.get('skipped', 0)} skipped, {stats['chunks']} chunks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
