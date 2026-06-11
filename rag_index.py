# rag_index.py
"""In-process RAG index for Ask-the-Library.

Paper text -> overlapping chunks -> embeddings (Gemini OpenAI-compatible) stored
in MySQL (papers_chunks). Vectors are stored in MySQL as binary VECTOR columns;
retrieval scores an L2-normalized float32 numpy matrix per process, refreshed
whenever the rag_index_meta.chunks_version stamp moves (cross-process
invalidation).
"""

from __future__ import annotations

import logging
import math
import numpy as np

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
    """Wire DB + paper access. Keys: build_embed_client, embed_model,
    embed_batch_size, iter_papers, paper_text, store_replace, store_delete,
    store_version, store_vectors, fetch_chunks, indexed_filenames, paper_meta."""
    _DEPS.update(deps)
    invalidate_cache()


_SNAPSHOT = None   # _Snapshot | None — per-process; rebuilt when the DB stamp moves


class _Snapshot:
    """Immutable per-process view of the chunk index at one DB version."""
    __slots__ = ("version", "ids", "filenames", "chunk_indexes", "matrix",
                 "paper_filenames", "paper_matrix")

    def __init__(self, version, ids, filenames, chunk_indexes, matrix,
                 paper_filenames, paper_matrix):
        self.version = version
        self.ids = ids
        self.filenames = filenames
        self.chunk_indexes = chunk_indexes
        self.matrix = matrix                  # (N, dim) float32, L2-normalized rows; None if empty
        self.paper_filenames = paper_filenames
        self.paper_matrix = paper_matrix      # (P, dim) float32, L2-normalized rows; None if empty


def invalidate_cache() -> None:
    global _SNAPSHOT
    _SNAPSHOT = None


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
        already = set(_DEPS["indexed_filenames"]())
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


def _row_vector(raw):
    """Decode stored little-endian float32 bytes. None for NULL/empty/ragged."""
    if not raw or len(raw) % 4:
        return None
    return np.frombuffer(raw, dtype="<f4")


def _normalize_rows(matrix):
    """L2-normalize rows in place-safe form; zero rows stay zero (score 0)."""
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return matrix / norms


def _build_snapshot(version) -> "_Snapshot":
    ids, filenames, chunk_indexes, vectors = [], [], [], []
    dim = None
    skipped = 0
    for row in _DEPS["store_vectors"]():
        vec = _row_vector(row.get("embedding"))
        if vec is None:
            skipped += 1
            continue
        if dim is None:
            dim = vec.shape[0]
        if vec.shape[0] != dim:
            skipped += 1
            continue
        ids.append(row["id"])
        filenames.append(row["filename"])
        chunk_indexes.append(row["chunk_index"])
        vectors.append(vec)
    if skipped:
        _log.warning("snapshot: skipped %d chunk rows (missing/mismatched vectors)", skipped)
    if not vectors:
        return _Snapshot(version, [], [], [], None, [], None)
    raw = np.vstack(vectors).astype(np.float32, copy=False)
    # Pool per paper on RAW vectors (mean), then normalize the pooled vector —
    # cosine(query, mean) == dot(query_norm, mean_norm).
    groups: dict = {}
    for i, fn in enumerate(filenames):
        groups.setdefault(fn, []).append(i)
    paper_filenames = list(groups)
    pooled = np.vstack([raw[groups[fn]].mean(axis=0) for fn in paper_filenames])
    return _Snapshot(
        version, ids, filenames, chunk_indexes, _normalize_rows(raw),
        paper_filenames, _normalize_rows(pooled.astype(np.float32, copy=False)),
    )


def _current_snapshot() -> "_Snapshot":
    """Stamp-checked snapshot: any process's write bumps the DB version, so the
    next query in THIS process rebuilds. On stamp-read failure, serve the
    existing snapshot (stale-but-available) rather than hard-failing."""
    global _SNAPSHOT
    try:
        version = _DEPS["store_version"]()
    except Exception:
        if _SNAPSHOT is not None:
            _log.warning("version stamp read failed; serving cached snapshot",
                         exc_info=True)
            return _SNAPSHOT
        raise
    if _SNAPSHOT is None or _SNAPSHOT.version != version:
        _SNAPSHOT = _build_snapshot(version)
    return _SNAPSHOT


def warm() -> int:
    """Pre-load the snapshot (gunicorn post_fork). Returns cached chunk count."""
    snap = _current_snapshot()
    return 0 if snap.matrix is None else int(snap.matrix.shape[0])


def _normalized_qvec(raw_vec, dim):
    """Query vector as unit-norm float32; None if blank or dim-mismatched
    (embed model changed without a reindex)."""
    q = np.asarray(raw_vec, dtype=np.float32)
    if q.shape[0] != dim:
        _log.warning("query vector dim %d != index dim %d "
                     "(embed model changed without reindex?)", q.shape[0], dim)
        return None
    n = float(np.linalg.norm(q))
    if n == 0.0:
        return None
    return q / n


def retrieve(query: str, k: int = 6, min_sim: float = 0.20) -> list[dict]:
    """Return up to k grounding chunks (with paper metadata) ranked by similarity."""
    query = (query or "").strip()
    if not query:
        return []
    snap = _current_snapshot()
    if snap.matrix is None:
        return []
    qvec = _normalized_qvec(embed_texts([query])[0], snap.matrix.shape[1])
    if qvec is None:
        return []
    scores = snap.matrix @ qvec
    top = []
    for i in np.argsort(scores)[::-1]:
        if scores[i] < min_sim or len(top) >= k:
            break
        top.append(int(i))
    if not top:
        return []
    contents = {c["id"]: c for c in _DEPS["fetch_chunks"]([snap.ids[i] for i in top])}
    hits = []
    for i in top:
        chunk = contents.get(snap.ids[i])
        if chunk is None:        # row deleted between scoring and fetch
            continue
        meta = _DEPS["paper_meta"](snap.filenames[i]) or {}
        hits.append({
            "filename": snap.filenames[i],
            "chunk_index": snap.chunk_indexes[i],
            "content": chunk["content"],
            "score": float(scores[i]),
            "title": meta.get("title", snap.filenames[i]),
            "author_name": meta.get("author_name", ""),
        })
    return hits


# ---------------------------------------------------------------------------
# Paper-level semantic search & related papers (idea #4).
# ---------------------------------------------------------------------------

_QVEC_CACHE: dict = {}              # (embed_model, query) -> query vector
_QVEC_CACHE_MAX = 256


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
    """Rank papers by similarity of the query to each pooled paper vector.
    One embeddings call per distinct query. Returns [(filename, score)] desc,
    only scores >= min_sim, capped at k. [] for blank query / empty index."""
    query = (query or "").strip()
    if not query:
        return []
    snap = _current_snapshot()
    if snap.paper_matrix is None:
        return []
    qvec = _normalized_qvec(_embed_query_cached(query), snap.paper_matrix.shape[1])
    if qvec is None:
        return []
    scores = snap.paper_matrix @ qvec
    out = []
    for i in np.argsort(scores)[::-1]:
        if scores[i] < min_sim or len(out) >= k:
            break
        out.append((snap.paper_filenames[int(i)], float(scores[i])))
    return out


def related_papers(filename: str, k: int = 5,
                   min_sim: float = RELATED_MIN_SIM) -> list:
    """Papers most similar to `filename` by pooled-vector similarity, excluding
    itself. Zero LLM calls (the paper is already embedded). [] if the paper
    isn't embedded."""
    snap = _current_snapshot()
    if snap.paper_matrix is None:
        return []
    try:
        pos = snap.paper_filenames.index(filename)
    except ValueError:
        return []
    scores = snap.paper_matrix @ snap.paper_matrix[pos]
    out = []
    for i in np.argsort(scores)[::-1]:
        i = int(i)
        if snap.paper_filenames[i] == filename:
            continue
        if scores[i] < min_sim or len(out) >= k:
            break
        out.append((snap.paper_filenames[i], float(scores[i])))
    return out
