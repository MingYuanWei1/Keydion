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


CHUNK_SIZE = 800
CHUNK_OVERLAP = 120

# Paper-level similarity thresholds for semantic search / related papers (idea #4).
# STARTING VALUES — not yet tuned against the real corpus (see the design spec).
PAPER_SEARCH_MIN_SIM = 0.25     # query -> paper (pooled vector, short query)
RELATED_MIN_SIM = 0.30          # paper -> paper (same vector space)


def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
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


def reassemble(contents: list[str], overlap: int = CHUNK_OVERLAP) -> str:
    """Stitch overlapping chunks back into the original text.

    Strips the leading `overlap` characters from every chunk after the first.
    This exactly undoes chunk_text *only when* the caller passes the overlap
    value that chunk_text actually used.  Note that chunk_text silently clamps
    ``overlap`` to ``size // 4`` whenever ``overlap >= size``; in that case
    the caller must pass the clamped value here, not the original one.
    If ``overlap`` exceeds a chunk's length, those characters are silently
    dropped rather than raising an error.
    Returns "" for an empty list.
    """
    if not contents:
        return ""
    return contents[0] + "".join(c[overlap:] for c in contents[1:])


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
_PAPER_VECS: dict | None = None  # filename -> mean-pooled paper vector (idea #4)


def invalidate_cache() -> None:
    global _CACHE, _PAPER_VECS
    _CACHE = None
    _PAPER_VECS = None


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Batch-embed via the configured embedding client.

    Inputs are split into sub-batches of at most embed_batch_size() items,
    because some providers cap an embeddings request (e.g. DashScope: 10).
    Results are returned in input order.
    """
    if not texts:
        return []
    client = _DEPS["build_embed_client"]()
    model = _DEPS["embed_model"]()
    getter = _DEPS.get("embed_batch_size")
    size = max(1, int(getter())) if getter else 10
    out: list[list[float]] = []
    for start in range(0, len(texts), size):
        batch = texts[start:start + size]
        resp = client.embeddings.create(model=model, input=batch)
        out.extend(d.embedding for d in resp.data)
    return out


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


# ---------------------------------------------------------------------------
# Paper-level semantic search & related papers (idea #4).
# Reuse the chunk vectors already in _CACHE; no new embeddings at index time.
# ---------------------------------------------------------------------------

_QVEC_CACHE: dict = {}              # (embed_model, query) -> query vector
_QVEC_CACHE_MAX = 256


def _mean_pool(vectors: list[list[float]]) -> list[float]:
    """Component-wise mean of equal-length vectors. Skips any vector whose
    dimensionality differs from the first (only possible if the embed model
    changed without a reindex). Returns [] if nothing is poolable."""
    if not vectors:
        return []
    dim = len(vectors[0])
    acc = [0.0] * dim
    n = 0
    for v in vectors:
        if len(v) != dim:
            _log.warning("skipping chunk vector of dim %d (expected %d)", len(v), dim)
            continue
        for i in range(dim):
            acc[i] += v[i]
        n += 1
    if n == 0:
        return []
    return [x / n for x in acc]


def paper_vectors() -> dict:
    """{filename: pooled_vector} — group _CACHE chunk vectors by paper and
    mean-pool. Lazily computed; reset by invalidate_cache()."""
    global _PAPER_VECS
    if _PAPER_VECS is None:
        groups: dict = {}
        for filename, _idx, _content, vec in _load_cache():
            if vec:
                groups.setdefault(filename, []).append(vec)
        pooled = {}
        for fn, vecs in groups.items():
            mp = _mean_pool(vecs)
            if mp:
                pooled[fn] = mp
        _PAPER_VECS = pooled
    return _PAPER_VECS


def _embed_query_cached(query: str) -> list[float]:
    """Embed a search query once, memoized by (embed_model, query) so pagination
    and repeat queries don't re-embed. Corpus-independent (no invalidation)."""
    model = _DEPS["embed_model"]()
    key = (model, query)
    vec = _QVEC_CACHE.get(key)
    if vec is None:
        vec = embed_texts([query])[0]
        if len(_QVEC_CACHE) >= _QVEC_CACHE_MAX:
            _QVEC_CACHE.clear()
        _QVEC_CACHE[key] = vec
    return vec


def search_papers_semantic(query: str, min_sim: float = PAPER_SEARCH_MIN_SIM,
                           k: int = 100) -> list:
    """Rank papers by cosine of the query against each pooled paper vector.
    One embeddings call per distinct query. Returns [(filename, score)] desc,
    only scores >= min_sim, capped at k. [] for blank query / empty index."""
    query = (query or "").strip()
    if not query:
        return []
    pv = paper_vectors()
    if not pv:
        return []
    qvec = _embed_query_cached(query)
    scored = [(fn, cosine(qvec, vec)) for fn, vec in pv.items()]
    scored = [t for t in scored if t[1] >= min_sim]
    scored.sort(key=lambda t: t[1], reverse=True)
    return scored[:k]


def related_papers(filename: str, k: int = 5,
                   min_sim: float = RELATED_MIN_SIM) -> list:
    """Papers most similar to `filename` by pooled-vector cosine, excluding
    itself. Zero LLM calls (the paper is already embedded). [] if the paper
    isn't embedded."""
    pv = paper_vectors()
    target = pv.get(filename)
    if not target:
        return []
    scored = [(fn, cosine(target, vec)) for fn, vec in pv.items() if fn != filename]
    scored = [t for t in scored if t[1] >= min_sim]
    scored.sort(key=lambda t: t[1], reverse=True)
    return scored[:k]
