# rag_index.py
"""In-process RAG index for Keydion AI.

Paper text -> overlapping chunks -> embeddings (Gemini OpenAI-compatible) stored
in MySQL (papers_chunks). Vectors are stored in MySQL as binary VECTOR columns;
retrieval scores an L2-normalized float32 numpy matrix per process, refreshed
whenever the rag_index_meta.chunks_version stamp moves (cross-process
invalidation).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
import numpy as np

from services.publishing_contracts import (
    IndexDeadlineExceeded,
    raise_deadline_if_expired as _raise_deadline_if_expired,
    remaining_timeout as _remaining_timeout,
)

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
    """Wire provider and read-model dependencies.

    Production indexing writes belong exclusively to ``PublishingLifecycle``.
    This module receives only embedding-provider callables plus current-visible
    snapshot/fetch readers.
    """
    _DEPS.update(deps)
    invalidate_cache()


_SNAPSHOT = None   # _Snapshot | None — per-process; rebuilt when the DB stamp moves


@dataclass(slots=True, eq=False)
class _Snapshot:
    """Immutable per-process view of the chunk index at one DB version."""
    version: object
    ids: list
    paper_ids: list
    revision_numbers: list
    chunk_indexes: list
    matrix: object
    pooled_paper_ids: list
    pooled_revisions: list
    paper_matrix: object


def invalidate_cache() -> None:
    global _SNAPSHOT
    _SNAPSHOT = None


def embed_texts(
    texts: list[str],
    *,
    deadline: float | None = None,
    build_embed_client=None,
    embed_model=None,
    embed_batch_size=None,
) -> list[list[float]]:
    """Batch-embed via the configured embedding client.

    Inputs are split into sub-batches of at most embed_batch_size() items,
    because some providers cap an embeddings request (e.g. DashScope: 10).
    Results are returned in input order.
    """
    if not texts:
        return []
    _remaining_timeout(deadline)
    builder = build_embed_client or _DEPS["build_embed_client"]
    model_getter = embed_model or _DEPS["embed_model"]
    batch_size_getter = embed_batch_size or _DEPS.get("embed_batch_size")
    try:
        client = builder() if deadline is None else builder(deadline=deadline)
        model = model_getter()
        getter = batch_size_getter
        size = max(1, int(getter())) if getter else 10
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _remaining_timeout(deadline)
    out: list[list[float]] = []
    for start in range(0, len(texts), size):
        batch = texts[start:start + size]
        kwargs = {"model": model, "input": batch}
        if deadline is not None:
            kwargs["timeout"] = _remaining_timeout(deadline)
        try:
            resp = client.embeddings.create(**kwargs)
            data = list(resp.data)
        except IndexDeadlineExceeded:
            raise
        except Exception as exc:
            _raise_deadline_if_expired(deadline, exc)
            raise
        _remaining_timeout(deadline)
        if len(data) != len(batch):
            raise ValueError("embedding provider returned the wrong result count")
        out.extend(item.embedding for item in data)
    return out


def _row_vector(raw):
    """Decode stored little-endian float32 bytes. None for NULL/empty/ragged."""
    if not raw or len(raw) % 4:
        return None
    return np.frombuffer(raw, dtype="<f4")


def _normalize_rows(matrix):
    """Return a row-L2-normalized copy; zero rows stay zero (score 0)."""
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    return matrix / norms


def _build_snapshot(version) -> "_Snapshot":
    ids, paper_ids, revision_numbers, chunk_indexes, vectors = [], [], [], [], []
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
        paper_ids.append(row["paper_id"])
        revision_numbers.append(row["revision_number"])
        chunk_indexes.append(row["chunk_index"])
        vectors.append(vec)
    if skipped:
        _log.warning("snapshot: skipped %d chunk rows (missing or != %s-dim vectors)",
                     skipped, dim)
    if not vectors:
        return _Snapshot(version, [], [], [], [], None, [], [], None)
    raw = np.vstack(vectors).astype(np.float32, copy=False)
    # Pool per paper on RAW vectors (mean), then normalize the pooled vector —
    # cosine(query, mean) == dot(query_norm, mean_norm).
    groups: dict = {}
    for i, paper_id in enumerate(paper_ids):
        groups.setdefault(paper_id, []).append(i)
    pooled_paper_ids = list(groups)
    pooled_revisions = [revision_numbers[groups[paper_id][0]] for paper_id in pooled_paper_ids]
    pooled = np.vstack(
        [raw[groups[paper_id]].mean(axis=0) for paper_id in pooled_paper_ids]
    )
    return _Snapshot(
        version,
        ids,
        paper_ids,
        revision_numbers,
        chunk_indexes,
        _normalize_rows(raw),
        pooled_paper_ids,
        pooled_revisions,
        _normalize_rows(pooled.astype(np.float32, copy=False)),
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
    contents = {
        c["id"]: c for c in _DEPS["fetch_chunks"]([snap.ids[i] for i in top])
    }
    hits = []
    for i in top:
        chunk = contents.get(snap.ids[i])
        if (
            chunk is None
            or chunk.get("paper_id") != snap.paper_ids[i]
            or chunk.get("revision_number") != snap.revision_numbers[i]
        ):
            # Deleted, hidden, or superseded between scoring and the fresh
            # current-visible fetch.
            continue
        hits.append({
            "paper_id": chunk["paper_id"],
            "revision_number": chunk["revision_number"],
            "filename": chunk["filename"],
            "chunk_index": chunk["chunk_index"],
            "content": chunk["content"],
            "score": float(scores[i]),
            "title": chunk.get("title") or chunk["filename"],
            "author_name": chunk.get("author_name") or "",
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
    One embeddings call per distinct query. Returns [(paper_uuid, score)] desc,
    only scores >= min_sim, capped at k. [] for blank query / empty index."""
    query = (query or "").strip()
    if not query or k <= 0:
        return []
    snap = _current_snapshot()
    if snap.paper_matrix is None:
        return []
    qvec = _normalized_qvec(_embed_query_cached(query), snap.paper_matrix.shape[1])
    if qvec is None:
        return []
    scores = snap.paper_matrix @ qvec
    candidates = []
    for i in np.argsort(scores)[::-1]:
        if scores[i] < min_sim:
            break
        candidates.append(int(i))
    visible = {
        row["paper_id"]: row
        for row in _DEPS["fetch_papers"](
            [snap.pooled_paper_ids[i] for i in candidates]
        )
    }
    out = []
    for i in candidates:
        paper_id = snap.pooled_paper_ids[i]
        paper = visible.get(paper_id)
        if paper is None or paper.get("current_revision") != snap.pooled_revisions[i]:
            continue
        out.append((paper_id, float(scores[i])))
        if len(out) >= k:
            break
    return out


def related_papers(paper_id: str, k: int = 5,
                   min_sim: float = RELATED_MIN_SIM) -> list:
    """Papers most similar to ``paper_id`` by pooled-vector similarity, excluding
    itself. Zero LLM calls (the paper is already embedded). [] if the paper
    isn't embedded."""
    if k <= 0:
        return []
    snap = _current_snapshot()
    if snap.paper_matrix is None:
        return []
    try:
        pos = snap.pooled_paper_ids.index(paper_id)
    except ValueError:
        return []
    scores = snap.paper_matrix @ snap.paper_matrix[pos]
    candidates = []
    for i in np.argsort(scores)[::-1]:
        i = int(i)
        if snap.pooled_paper_ids[i] == paper_id:
            continue
        if scores[i] < min_sim:
            break
        candidates.append(i)
    visible = {
        row["paper_id"]: row
        for row in _DEPS["fetch_papers"](
            [paper_id] + [snap.pooled_paper_ids[i] for i in candidates]
        )
    }
    source = visible.get(paper_id)
    if source is None or source.get("current_revision") != snap.pooled_revisions[pos]:
        return []
    out = []
    for i in candidates:
        candidate_id = snap.pooled_paper_ids[i]
        paper = visible.get(candidate_id)
        if paper is None or paper.get("current_revision") != snap.pooled_revisions[i]:
            continue
        out.append((candidate_id, float(scores[i])))
        if len(out) >= k:
            break
    return out
