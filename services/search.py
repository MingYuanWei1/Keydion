"""Paper search: lexical (substring + fulltext) and hybrid semantic ranking."""
import json
from typing import Dict, List

import llm_client
import rag_index
from config import MAX_SEARCH_RESULTS, MIN_SEMANTIC_QUERY_LEN, PAPERS_DIR
from db import db_session
from models import PaperChunkModel
from services.papers import build_paper_record, extract_pdf_text, load_paper_metadata


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


def _fulltext_index() -> Dict[str, str]:
    """{filename: lowercased full text} reassembled from stored chunks in a
    single DB query (content column only — the embedding column is not read).

    Lets the /search lexical fallback match paper body text without re-reading
    every PDF from disk on each request. Papers with no stored chunks (not yet
    indexed) are simply absent from the dict; search_papers OCR-extracts those
    on demand, preserving the old behaviour for the unindexed case.
    """
    groups: Dict[str, List[str]] = {}
    with db_session() as db:
        rows = (db.query(PaperChunkModel.filename,
                         PaperChunkModel.chunk_index,
                         PaperChunkModel.content)
                  .order_by(PaperChunkModel.filename,
                            PaperChunkModel.chunk_index)
                  .all())
    for fn, _idx, content in rows:
        groups.setdefault(fn, []).append(content or "")
    return {fn: rag_index.reassemble(parts).lower() for fn, parts in groups.items()}


def search_papers(keyword: str) -> List[Dict[str, str]]:
    metadata_index = {row["filename"]: row for row in load_paper_metadata()}
    matches: List[Dict[str, str]] = []
    normalized = keyword.lower()
    fulltext = _fulltext_index()   # one bulk read of stored chunks

    for pdf_path in PAPERS_DIR.glob("*.pdf"):
        record = build_paper_record(pdf_path.name, metadata_index)

        if _query_in_metadata(record, normalized):
            matches.append(record)
            continue

        # Full-text fallback: prefer indexed chunks; OCR-extract only papers
        # that have no stored chunks yet.
        text = fulltext.get(pdf_path.name)
        if text is None:
            try:
                text = extract_pdf_text(pdf_path).lower()
            except Exception as exc:  # pragma: no cover - logging placeholder
                print(f"Failed to read {pdf_path.name}: {exc}")
                continue
        if normalized in text:
            matches.append(record)

    matches.sort(key=lambda row: row.get("published_at") or "", reverse=True)
    return matches[:MAX_SEARCH_RESULTS]


def _order_hybrid_filenames(lexical_records: List[Dict[str, str]],
                            semantic_pairs: list, normalized_query: str) -> List[str]:
    """Order filenames for hybrid search (pure; no IO):
      1. lexical metadata matches (sorted by semantic score, desc)
      2. semantic-only hits (by score)
      3. lexical full-text-only matches (original lexical/date order)
    `semantic_pairs` is [(filename, score)] already sorted desc."""
    sem_score = dict(semantic_pairs)
    meta_fns = [r["filename"] for r in lexical_records
                if _query_in_metadata(r, normalized_query)]
    meta_set = set(meta_fns)

    tier1 = sorted(meta_fns, key=lambda fn: sem_score.get(fn, -1.0), reverse=True)
    tier2 = [fn for fn, _ in semantic_pairs if fn not in meta_set]
    seen = set(tier1) | set(tier2)
    tier3 = [r["filename"] for r in lexical_records if r["filename"] not in seen]
    return tier1 + tier2 + tier3


def _hybrid_search_records(query: str) -> List[Dict[str, str]]:
    """Hybrid search candidate set: semantic ranking unioned with lexical recall.
    Falls back to pure lexical search whenever embeddings are unavailable, the
    query is too short, the index is empty, or embedding fails."""
    lexical = search_papers(query)
    if not llm_client.llm_enabled() or len(query.strip()) < MIN_SEMANTIC_QUERY_LEN:
        return lexical
    try:
        sem = rag_index.search_papers_semantic(query)
    except Exception as exc:  # embedding failure must never break search
        print(f"semantic search failed; falling back to lexical: {exc}")
        return lexical
    if not sem:
        return lexical

    ordered = _order_hybrid_filenames(lexical, sem, query.strip().lower())
    lexical_by_fn = {r["filename"]: r for r in lexical}
    index = None
    out: List[Dict[str, str]] = []
    for fn in ordered:
        rec = lexical_by_fn.get(fn)
        if rec is None:                                  # semantic-only paper
            if not (PAPERS_DIR / fn).exists():           # skip stale index entries
                continue
            if index is None:
                index = {row["filename"]: row for row in load_paper_metadata()}
            rec = build_paper_record(fn, index)
        out.append(rec)
    return out
