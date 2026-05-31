# tools/build_embeddings.py
"""Build the Ask-the-Library retrieval index over all published papers.

Usage:  python3 tools/build_embeddings.py
"""

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
    app.init_db()
    app.configure_rag()
    print("Building embedding index...")
    stats = rag_index.build_index()
    print(f"Done: {stats['papers']} papers, {stats['chunks']} chunks indexed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
