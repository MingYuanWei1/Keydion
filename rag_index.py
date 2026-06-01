# rag_index.py
"""In-process RAG index for Ask-the-Library.

Paper text -> overlapping chunks -> embeddings (Gemini OpenAI-compatible) stored
in MySQL (papers_chunks) -> pure-Python cosine retrieval over an in-memory cache.
No numpy / vector DB dependency.
"""

from __future__ import annotations

import logging
import math

_log = logging.getLogger(__name__)


def chunk_text(text: str, size: int = 800, overlap: int = 120) -> list[str]:
    """Split text into overlapping character chunks. Returns [] for blank text."""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]
    if overlap >= size:
        overlap = size // 4
    chunks: list[str] = []
    start = 0
    step = size - overlap
    while start < len(text):
        chunks.append(text[start:start + size])
        if start + size >= len(text):
            break
        start += step
    return chunks


def cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity; 0.0 if either vector is zero-length or empty."""
    if not a or not b:
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


import json

# --- dependency injection wiring (set by app.py at startup; faked in tests) ---
_DEPS: dict = {}


def configure(**deps) -> None:
    """Wire DB + paper access. Keys: build_embed_client, embed_model, iter_papers,
    paper_text, store_replace, store_all, store_delete, paper_meta."""
    _DEPS.update(deps)
    invalidate_cache()


_CACHE: list | None = None  # list of (filename, chunk_index, content, vector)


def invalidate_cache() -> None:
    global _CACHE
    _CACHE = None


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Batch-embed via the configured embedding client."""
    if not texts:
        return []
    client = _DEPS["build_embed_client"]()
    resp = client.embeddings.create(model=_DEPS["embed_model"](), input=texts)
    return [d.embedding for d in resp.data]


def build_index(filenames: list[str] | None = None, skip_existing: bool = False) -> dict:
    """(Re)build chunks+embeddings for the given papers (or all).

    Logs `[i/n] <filename>` progress per paper so a slow run (e.g. OCR of a
    scanned PDF) doesn't look hung. With skip_existing=True, papers that already
    have stored chunks are left untouched, so an interrupted run resumes instead
    of restarting from zero.
    """
    papers = _DEPS["iter_papers"]()
    if filenames is not None:
        wanted = set(filenames)
        papers = [p for p in papers if p["filename"] in wanted]
    already: set = set()
    if skip_existing:
        getter = _DEPS.get("indexed_filenames")
        already = set(getter()) if getter else {
            r["filename"] for r in _DEPS["store_all"]()
        }
    n_total = len(papers)
    n_papers = 0
    n_chunks = 0
    n_skipped = 0
    for i, p in enumerate(papers, 1):
        fn = p["filename"]
        if skip_existing and fn in already:
            n_skipped += 1
            _log.info("[%d/%d] %s — already indexed, skipping", i, n_total, fn)
            continue
        _log.info("[%d/%d] %s", i, n_total, fn)
        try:
            text = _DEPS["paper_text"](fn)
        except Exception:
            _log.warning("failed to extract text from %s", fn, exc_info=True)
            text = ""
        chunks = chunk_text(text)
        if not chunks:
            _DEPS["store_replace"](fn, [])
            continue
        vectors = embed_texts(chunks)
        rows = [
            {"filename": fn, "chunk_index": j, "content": chunks[j],
             "embedding": vectors[j], "lang": p.get("language", "")}
            for j in range(len(chunks))
        ]
        _DEPS["store_replace"](fn, rows)
        n_papers += 1
        n_chunks += len(rows)
    invalidate_cache()
    _log.info("Index build complete: %d indexed, %d skipped, %d chunks",
              n_papers, n_skipped, n_chunks)
    return {"papers": n_papers, "chunks": n_chunks, "skipped": n_skipped}


def purge(filename: str) -> None:
    _DEPS["store_delete"](filename)
    invalidate_cache()


def _load_cache() -> list:
    global _CACHE
    if _CACHE is None:
        _CACHE = [
            (r["filename"], r["chunk_index"], r["content"], r["embedding"])
            for r in _DEPS["store_all"]()
        ]
    return _CACHE


def retrieve(query: str, k: int = 6, min_sim: float = 0.20) -> list[dict]:
    """Return up to k grounding chunks (with paper metadata) ranked by similarity."""
    query = (query or "").strip()
    if not query:
        return []
    cache = _load_cache()
    if not cache:
        return []
    qvec = embed_texts([query])[0]
    scored = []
    for filename, chunk_index, content, vector in cache:
        scored.append((cosine(qvec, vector), filename, chunk_index, content))
    scored.sort(key=lambda t: t[0], reverse=True)
    hits = []
    for score, filename, chunk_index, content in scored:
        if score < min_sim:
            continue
        meta = _DEPS["paper_meta"](filename) or {}
        hits.append({
            "filename": filename,
            "chunk_index": chunk_index,
            "content": content,
            "score": score,
            "title": meta.get("title", filename),
            "author_name": meta.get("author_name", ""),
        })
        if len(hits) >= k:
            break
    return hits
