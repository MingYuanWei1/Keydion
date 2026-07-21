"""Paper search: lexical (substring + fulltext) and hybrid semantic ranking."""
import json
from typing import Dict, List

import llm_client
import rag_index
from config import (
    MAX_SEARCH_RESULTS,
    METADATA_FIELDS,
    MIN_SEMANTIC_QUERY_LEN,
    PAPERS_DIR,
    PENDING_PAPERS_DIR,
)
from db import db_session
from models import PaperChunkModel, PaperMetadataModel
from services.paper_storage import PaperStorage
from services.papers import extract_pdf_text


def _query_in_metadata(record: Dict[str, str], normalized: str) -> bool:
    """True if the (already-lowercased) query appears in a paper's metadata:
    title, author, keywords, EE subjects, or CP global context / action types."""
    title_str = (record.get("title") or "").lower()
    author_str = (record.get("author_name") or "").lower()
    keywords_str = (record.get("keywords") or "").lower()

    ee_subjects_str = ""
    raw_ib = record.get("ib_ee_data", "")
    if raw_ib:
        try:
            ib = json.loads(raw_ib)
            ee_subjects_str = (ib.get("core_subject", "") + " " + ib.get("interdisciplinary_subject", "")).lower()
        except (json.JSONDecodeError, TypeError):
            pass
    cp_context_str = ""
    raw_cp = record.get("cp_data", "")
    if raw_cp:
        try:
            cp = json.loads(raw_cp)
            cp_context_str = (cp.get("global_context", "") + " " + " ".join(cp.get("action_types", []))).lower()
        except (json.JSONDecodeError, TypeError):
            pass

    return (normalized in title_str or normalized in author_str
            or normalized in keywords_str or normalized in ee_subjects_str
            or normalized in cp_context_str)


def _visible_paper_records() -> List[Dict[str, str]]:
    """Project the current visible Paper rows used by lexical and hybrid search."""
    with db_session() as db:
        papers = (
            db.query(PaperMetadataModel)
            .filter(
                PaperMetadataModel.lifecycle_state == "published",
                PaperMetadataModel.current_revision.isnot(None),
            )
            .all()
        )
        records = []
        for paper in papers:
            record = {
                field: (getattr(paper, field, None) or "")
                for field in METADATA_FIELDS
            }
            record["paper_id"] = paper.id
            record["current_revision"] = paper.current_revision
            record["revision_number"] = paper.current_revision
            if not record["title"]:
                record["title"] = paper.filename.rsplit(".", 1)[0]
            records.append(record)
        return records


def _revision_path(paper_id: str, revision: int):
    """Resolve only canonical UUID/revision storage, never a flat filename."""
    storage = PaperStorage(PAPERS_DIR, PENDING_PAPERS_DIR)
    try:
        return storage.revision_path(paper_id, revision)
    finally:
        storage.close()


def _fulltext_index() -> Dict[str, tuple[int, str]]:
    """{paper_uuid: (revision, lowercased text)} for current-visible chunks.

    Carrying the revision lets the later Paper projection reject an entry if a
    revision switch occurs between the two read phases. The embedding column is
    deliberately not read.

    Lets the /search lexical fallback match paper body text without re-reading
    every PDF from disk on each request. Papers with no stored chunks (not yet
    indexed) are simply absent from the dict; search_papers OCR-extracts those
    on demand, preserving the old behaviour for the unindexed case.
    """
    groups: Dict[str, List[str]] = {}
    with db_session() as db:
        rows = (
            db.query(
                PaperChunkModel.paper_id,
                PaperChunkModel.revision_number,
                PaperChunkModel.chunk_index,
                PaperChunkModel.content,
            )
            .join(PaperMetadataModel, PaperMetadataModel.id == PaperChunkModel.paper_id)
            .filter(
                PaperMetadataModel.lifecycle_state == "published",
                PaperMetadataModel.current_revision == PaperChunkModel.revision_number,
            )
            .order_by(PaperChunkModel.paper_id, PaperChunkModel.chunk_index)
            .all()
        )
    revisions = {}
    for paper_id, revision, _idx, content in rows:
        revisions[paper_id] = revision
        groups.setdefault(paper_id, []).append(content or "")
    return {
        paper_id: (revisions[paper_id], rag_index.reassemble(parts).lower())
        for paper_id, parts in groups.items()
    }


def search_papers(keyword: str) -> List[Dict[str, str]]:
    matches: List[Dict[str, str]] = []
    normalized = keyword.lower()
    fulltext = _fulltext_index()   # one bulk read of stored chunks

    for record in _visible_paper_records():
        if _query_in_metadata(record, normalized):
            matches.append(record)
            continue

        # Full-text fallback: prefer indexed chunks; OCR-extract only papers
        # that have no stored chunks yet.
        paper_id = record["paper_id"]
        indexed = fulltext.get(paper_id)
        if indexed is None or indexed[0] != record["current_revision"]:
            try:
                text = extract_pdf_text(
                    _revision_path(paper_id, record["current_revision"])
                ).lower()
            except Exception as exc:  # pragma: no cover - logging placeholder
                print(f"Failed to read {record['filename']}: {exc}")
                continue
        else:
            text = indexed[1]
        if normalized in text:
            matches.append(record)

    matches.sort(key=lambda row: row.get("published_at") or "", reverse=True)
    return matches[:MAX_SEARCH_RESULTS]


def _order_hybrid_paper_ids(lexical_records: List[Dict[str, str]],
                            semantic_pairs: list, normalized_query: str) -> List[str]:
    """Order Paper UUIDs for hybrid search (pure; no IO):
      1. lexical metadata matches (sorted by semantic score, desc)
      2. semantic-only hits (by score)
      3. lexical full-text-only matches (original lexical/date order)
    `semantic_pairs` is [(paper_uuid, score)] already sorted desc."""
    sem_score = dict(semantic_pairs)
    metadata_ids = [
        r["paper_id"]
        for r in lexical_records
        if _query_in_metadata(r, normalized_query)
    ]
    metadata_set = set(metadata_ids)

    tier1 = sorted(
        metadata_ids,
        key=lambda paper_id: sem_score.get(paper_id, -1.0),
        reverse=True,
    )
    tier2 = [paper_id for paper_id, _ in semantic_pairs if paper_id not in metadata_set]
    seen = set(tier1) | set(tier2)
    tier3 = [r["paper_id"] for r in lexical_records if r["paper_id"] not in seen]
    return tier1 + tier2 + tier3


def _order_hybrid_filenames(lexical_records, semantic_pairs, normalized_query):
    """Backward-compatible export name; identity and return values are UUIDs."""
    return _order_hybrid_paper_ids(lexical_records, semantic_pairs, normalized_query)


def _hybrid_search_records(query: str) -> List[Dict[str, str]]:
    """Hybrid search candidate set: semantic ranking unioned with lexical recall.
    Falls back to pure lexical search whenever embeddings are unavailable, the
    query is too short, the index is empty, or embedding fails."""
    lexical = search_papers(query)
    if not llm_client.embedding_enabled() or len(query.strip()) < MIN_SEMANTIC_QUERY_LEN:
        return lexical
    try:
        sem = rag_index.search_papers_semantic(query)
    except Exception as exc:  # embedding failure must never break search
        print(f"semantic search failed; falling back to lexical: {exc}")
        return lexical
    if not sem:
        return lexical

    ordered = _order_hybrid_paper_ids(lexical, sem, query.strip().lower())
    visible_by_id = {
        record["paper_id"]: record for record in _visible_paper_records()
    }
    out: List[Dict[str, str]] = []
    for paper_id in ordered:
        record = visible_by_id.get(paper_id)
        if record is not None:
            out.append(record)
    return out
