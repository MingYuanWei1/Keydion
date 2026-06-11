"""Ask-the-Library: RAG index glue, prompt building, attachments, rate limiting."""
import json
import logging
import re
import types

import numpy as np

from flask import session, url_for
from flask_babel import gettext as _

import llm_client
import pdf_text
import rag_index
import web_search
from config import PAPERS_DIR
from db import db_session
from models import AttachmentChunkModel, PaperChunkModel, RagIndexMetaModel
from services.papers import build_paper_record, load_paper_metadata

logger = logging.getLogger(__name__)


def _rag_iter_papers():
    index = {row["filename"]: row for row in load_paper_metadata()}
    return [build_paper_record(p.name, index) for p in PAPERS_DIR.glob("*.pdf")]


def _index_ocr_langs(language: str) -> str:
    """Tesseract lang string for the INDEXING path, biased by the paper's
    declared language. en -> English-only (skips the Chinese model, ~2x
    faster); zh -> Chinese+English; unknown/empty -> both (safe default so an
    untagged scanned Chinese paper isn't garbled). Kept separate from
    llm_metadata._ocr_langs_for, which intentionally defaults unknown -> eng
    for its own request path."""
    lang = (language or "").strip().lower()
    if lang == "en":
        return "eng"
    if lang == "zh":
        return "chi_sim+eng"
    return "eng+chi_sim"


def _rag_paper_text(filename):
    # Indexing path: OCR scanned published papers so they're retrievable by
    # chat grounding AND readable in full by read_paper. Uses a higher page
    # cap (50) than request-path callers (10) and biases OCR by the paper's
    # declared language. (The live /search full-text fallback still uses the
    # pypdf-only extract_pdf_text(pdf_path) to avoid OCR per request.)
    record = build_paper_record(filename)
    ocr_langs = _index_ocr_langs(record.get("language", ""))
    return pdf_text.extract_pdf_text(
        (PAPERS_DIR / filename).read_bytes(),
        ocr_langs=ocr_langs, max_ocr_pages=50)


def _rag_paper_meta(filename):
    return build_paper_record(filename)


def bump_chunks_version(db) -> None:
    """Bump the cross-process RAG invalidation stamp. Must run inside the same
    db_session as the chunk write so data + stamp commit atomically."""
    row = (db.query(RagIndexMetaModel)
             .filter(RagIndexMetaModel.name == "chunks_version")
             .with_for_update().first())
    if row is None:
        row = RagIndexMetaModel(name="chunks_version", value=0)
        db.add(row)
    row.value = (row.value or 0) + 1


def _rag_store_replace(filename, rows):
    with db_session() as db:
        db.query(PaperChunkModel).filter(PaperChunkModel.filename == filename).delete()
        for r in rows:
            db.add(PaperChunkModel(
                filename=r["filename"],
                chunk_index=r["chunk_index"],
                content=r["content"],
                embedding_vec=json.dumps(r["embedding"]) if r["embedding"] else None,
                lang=r.get("lang", ""),
            ))
        bump_chunks_version(db)


def _rag_store_version():
    """Current invalidation stamp (0 before any write)."""
    with db_session() as db:
        row = (db.query(RagIndexMetaModel)
                 .filter(RagIndexMetaModel.name == "chunks_version").first())
        return row.value if row else 0


def _rag_store_vectors():
    """All chunk vectors as raw VECTOR bytes — content column deliberately
    NOT selected (it stays out of the per-worker cache)."""
    with db_session() as db:
        rows = db.query(PaperChunkModel.id, PaperChunkModel.filename,
                        PaperChunkModel.chunk_index,
                        PaperChunkModel.embedding_vec).all()
        return [{"id": r[0], "filename": r[1], "chunk_index": r[2],
                 "embedding": r[3]} for r in rows]


def _rag_fetch_chunks(ids):
    """Contents for the top-k scored chunk ids."""
    if not ids:
        return []
    with db_session() as db:
        rows = (db.query(PaperChunkModel.id, PaperChunkModel.filename,
                         PaperChunkModel.chunk_index, PaperChunkModel.content)
                  .filter(PaperChunkModel.id.in_(list(ids))).all())
        return [{"id": r[0], "filename": r[1], "chunk_index": r[2],
                 "content": r[3]} for r in rows]


def _rag_store_delete(filename):
    with db_session() as db:
        db.query(PaperChunkModel).filter(PaperChunkModel.filename == filename).delete()
        bump_chunks_version(db)


def _rag_indexed_filenames():
    """Distinct filenames that already have stored chunks (for resume/skip)."""
    with db_session() as db:
        rows = db.query(PaperChunkModel.filename).distinct().all()
        return {r[0] for r in rows}


def configure_rag():
    rag_index.configure(
        build_embed_client=llm_client.build_embed_client,
        embed_model=llm_client.embed_model,
        embed_batch_size=llm_client.embed_batch_size,
        iter_papers=_rag_iter_papers,
        paper_text=_rag_paper_text,
        paper_meta=_rag_paper_meta,
        store_replace=_rag_store_replace,
        store_delete=_rag_store_delete,
        store_version=_rag_store_version,
        store_vectors=_rag_store_vectors,
        fetch_chunks=_rag_fetch_chunks,
        indexed_filenames=_rag_indexed_filenames,
    )


# library_tools deps — DB-backed implementations of the four callables that
# library_tools.run_tool requires.  The agentic loop (a later task) will call
# _build_library_deps() and pass the result as `deps`.

def _lib_full_text(filename: str) -> str:
    """Return the full text of a paper by reassembling its stored chunks.

    Prefers stored chunks (reassemble undoes the overlap introduced by
    chunk_text) so this is fast and doesn't need the PDF on disk.  Falls back
    to live OCR via _rag_paper_text only when no chunks exist (unindexed
    paper); that path can be slow and may fail — errors are logged and "" is
    returned so the caller is never disrupted.
    """
    try:
        with db_session() as db:
            rows = (db.query(PaperChunkModel)
                      .filter(PaperChunkModel.filename == filename)
                      .order_by(PaperChunkModel.chunk_index)
                      .all())
            contents = [r.content or "" for r in rows]
        if contents:
            return rag_index.reassemble(contents)
        # No stored chunks — try live extraction as a last resort.
        try:
            return _rag_paper_text(filename)
        except Exception:
            logger.exception("_lib_full_text fallback failed for %s", filename)
            return ""
    except Exception:
        logger.exception("_lib_full_text failed for %s", filename)
        return ""


def _lib_search(query: str) -> list:
    """Semantic library search for the search_library tool.

    Returns [] on any retrieval error so tool failures degrade gracefully.
    """
    try:
        hits = rag_index.retrieve(query)
        hits = _dedupe_hits_by_paper(hits)
        return [
            {
                "filename": h["filename"],
                "title": h.get("title") or h["filename"],
                "authors": h.get("author_name", ""),
                "url": url_for("preview_paper", filename=h["filename"]),
                "snippet": (h.get("content") or "")[:400],
            }
            for h in hits
        ]
    except Exception:
        logger.exception("_lib_search failed for query: %s", query)
        return []


def _lib_paper_meta(filename: str) -> dict:
    rec = build_paper_record(filename)
    return {"title": rec.get("title") or filename, "authors": rec.get("author_name", "")}


def _lib_paper_url(filename: str) -> str:
    return url_for("preview_paper", filename=filename)


def _build_library_deps(conv_db_id=None):
    """Return a deps object for library_tools.run_tool. Library callables plus
    optional web_search (Phase A), fetch_url (Phase B), and read_attachment (Phase C)."""
    return types.SimpleNamespace(
        search=_lib_search,
        full_text=_lib_full_text,
        paper_meta=_lib_paper_meta,
        paper_url=_lib_paper_url,
        web_search=web_search.web_search,
        fetch_url=web_search.fetch_url,
        read_attachment=lambda fn: _read_attachment_text(conv_db_id, fn),
    )


MAX_QUESTION_CHARS = 2000
MAX_ATTACH_BYTES = 5 * 1024 * 1024   # 5 MB cap on ad-hoc attachments
_ASK_HITS: dict = {}   # ip -> list[timestamp]; best-effort per worker
ASK_RATE_LIMIT = 20    # requests
ASK_RATE_WINDOW = 60   # seconds


def _ask_rate_ok(ip: str) -> bool:
    import time
    now = time.time()
    hits = [t for t in _ASK_HITS.get(ip, []) if now - t < ASK_RATE_WINDOW]
    if len(hits) >= ASK_RATE_LIMIT:
        _ASK_HITS[ip] = hits
        return False
    hits.append(now)
    _ASK_HITS[ip] = hits
    return True


MAX_TOOL_ROUNDS = 5
WEB_SEARCH_CALL_CAP = 3   # max web_search calls per Ask turn
FETCH_URL_CALL_CAP = 3   # max fetch_url calls per Ask turn


def _build_agentic_ask_prompt(question, candidates, web_sources, locale_code,
                              include_web=False, include_attachment=False,
                              attachment_names=None):
    """System prompt for the agentic Ask loop.

    Seeds the model with the candidate papers (and any web sources) already
    retrieved, and instructs it to use search_library / read_paper to gather
    more, citing sources with bracketed [n].
    """
    lang = "Chinese" if locale_code == "zh" else "English"
    candidates = candidates or []
    web_sources = web_sources or []
    lines = []
    for c in candidates:
        head = (f"[{c['n']}] {c.get('title') or c.get('filename')} — "
                f"{c.get('authors', '')} (filename: {c.get('filename')})")
        if c.get("is_attachment"):
            head += " (attached file)"
        lines.append(head)
        snippet = (c.get("snippet") or "").strip()
        if snippet:
            lines.append(snippet)
    for w in web_sources:
        lines.append(f"[{w['n']}] (web) {w.get('title', '')} ({w.get('url', '')})")
        snippet = (w.get("snippet") or "").strip()
        if snippet:
            lines.append(snippet)
    sources_block = "\n".join(lines)

    system = (
        "You are Keydion's library assistant. You help users find and understand "
        "papers in Keydion's published library, attaching citations. "
        f"Answer in {lang}.\n\n"
        "You have these tools:\n"
        "- search_library(query): search the library for more relevant papers. "
        "Use it to discover papers beyond the candidates already listed below.\n"
        "- read_paper(filename): fetch a paper's FULL text. Use it when the user "
        "asks you to explain or summarize a specific paper, or when a candidate "
        "snippet is insufficient to answer well.\n"
        + ("- web_search(query): search the public web. Prefer the library FIRST; "
           "use web_search only for current events or topics the library does not "
           "cover.\n"
           "- fetch_url(url): read the FULL text of a web page (e.g. a web_search "
           "result whose snippet is insufficient).\n" if include_web else "")
        + ("- read_attachment(filename): read the FULL text of a document the user "
           "attached to this conversation.\n" if include_attachment else "")
        + "\n"
        "Cite the sources you actually use with bracketed numbers like [n]. Each "
        "candidate and each paper you read carries its own [n]. Cite ONLY sources "
        "you actually used to answer; you do not need to cite every source, and "
        "never cite a source that is not relevant to your answer.\n"
        "If no candidates were retrieved and the user is just greeting you, making "
        "small talk, or asking who you are or what you can do, reply naturally and "
        "do not invent sources."
    )
    if sources_block:
        system += "\n\nCANDIDATE SOURCES:\n" + sources_block
    else:
        system += "\n\nNo candidate sources were retrieved for this message."
    if include_attachment and attachment_names:
        system += ("\n\nATTACHED DOCUMENTS (call read_attachment with one of these "
                   "filenames):\n" + "\n".join(f"- {n}" for n in attachment_names))
    return system


def _tool_status_text(name, arguments, registry, deps):
    """Best-effort human-readable status for a tool call. Never raises."""
    if name == "search_library":
        return str(_("Searching the library…"))
    if name == "read_paper":
        title = None
        try:
            args = arguments if isinstance(arguments, dict) else json.loads(arguments)
            filename = str(args.get("filename") or "").strip()
            if filename:
                for c in registry.as_citations():
                    if c.get("filename") == filename:
                        title = c.get("title")
                        break
                if not title:
                    try:
                        title = (deps.paper_meta(filename) or {}).get("title")
                    except Exception:
                        title = None
                title = title or filename
        except Exception:
            title = None
        if title:
            return str(_("Reading “%(title)s”…")) % {"title": title}
        return str(_("Reading a paper…"))
    if name == "web_search":
        return str(_("Searching the web…"))
    if name == "fetch_url":
        return str(_("Reading a web page…"))
    if name == "read_attachment":
        return str(_("Reading the attachment…"))
    return str(_("Working…"))


def _build_ask_prompt(question, hits, locale_code, web_results=None):
    lang = "Chinese" if locale_code == "zh" else "English"
    web_results = web_results or []
    blocks = [
        f"[{i + 1}] {h['title']} — {h.get('author_name', '')}\n{h['content']}"
        for i, h in enumerate(hits)
    ]
    offset = len(hits)
    for j, w in enumerate(web_results):
        blocks.append(f"[{offset + j + 1}] (web) {w['title']}\n{w.get('content', '')}")
    if blocks:
        sources = "\n\n".join(blocks)
        system = (
            "You are Keydion's library assistant. Answer the question using ONLY the "
            "numbered sources below. Cite claims with bracketed numbers like [1]. "
            "Cite only the sources you actually use; you do not need to cite every "
            "source, and never cite a source that is not relevant to your answer. "
            "Sources marked (web) come from a live web search; all others are library "
            f"papers. Answer in {lang}. If the sources do not contain the answer, say "
            "you could not find it.\n\nSOURCES:\n" + sources
        )
    else:
        system = (
            "You are Keydion's library assistant — you chat with users and answer "
            "questions from Keydion's published paper library, attaching citations. "
            f"Answer in {lang}. No library sources were retrieved for this message. "
            "If the user is greeting you, making small talk, or asking who you are or "
            "what you can do, reply naturally and briefly introduce your identity and "
            "what you can help with. If instead the user asked a research or library "
            "question that needs sources, explain that you could not find relevant "
            "papers and invite them to rephrase. Either way, do not invent sources."
        )
    return system


def _ask_llm_messages(question, history_rows):
    messages = []
    for row in history_rows or []:
        if isinstance(row, dict):
            raw_role = row.get("role")
            raw_content = row.get("content")
        else:
            raw_role = row.role
            raw_content = row.content
        role = raw_role if raw_role in ("user", "assistant") else ""
        content = (raw_content or "").strip()
        if role and content:
            messages.append({"role": role, "content": content})
    if not messages or messages[-1] != {"role": "user", "content": question}:
        messages.append({"role": "user", "content": question})
    return messages


# Bracketed source refs the assistant may emit: half-width [1] / [1, 2] and the
# full-width 【1】 used in Chinese answers.
_CITE_BRACKET_RE = re.compile(r"[\[【]([^\[\]【】]*)[\]】]")


def _dedupe_hits_by_paper(hits):
    """Collapse multiple chunks of the same paper into a single grounding source.

    Retrieval is chunk-level, so one long paper can occupy several of the top
    hits. Listing each chunk separately makes the assistant cite the same paper
    repeatedly. Keep the first (best-scoring) occurrence per filename and merge
    the remaining chunk text into it so the model still sees the full context.
    """
    merged = {}
    order = []
    for h in hits:
        fn = h.get("filename")
        if fn not in merged:
            merged[fn] = dict(h)
            order.append(fn)
        else:
            merged[fn]["content"] = (
                (merged[fn].get("content", "") + "\n\n" + h.get("content", "")).strip()
            )
    return [merged[fn] for fn in order]


def _cited_numbers(answer_text):
    """Source numbers the assistant actually referenced, e.g. [1], 【2】, [1, 3]."""
    nums = set()
    for group in _CITE_BRACKET_RE.findall(answer_text or ""):
        for token in re.findall(r"\d+", group):
            nums.add(int(token))
    return nums


def _filter_cited(items, answer_text):
    """Keep only the numbered sources the assistant referenced in its answer."""
    cited = _cited_numbers(answer_text)
    return [it for it in items if it.get("n") in cited]


def _cosine_f32(qvec, buf):
    """Cosine between a float32 numpy query vector and stored VECTOR bytes."""
    if not buf or len(buf) % 4:
        return 0.0
    v = np.frombuffer(buf, dtype="<f4")
    if v.size != qvec.size:
        return 0.0
    denom = float(np.linalg.norm(v)) * float(np.linalg.norm(qvec))
    if denom == 0.0:
        return 0.0
    return float(np.dot(v, qvec)) / denom


def _forced_grounding(question, filenames):
    """Ground on user-selected papers: score their stored chunks against the question."""
    chunks = []
    with db_session() as db:
        rows = (db.query(PaperChunkModel)
                  .filter(PaperChunkModel.filename.in_(filenames)).all())
        for r in rows:
            chunks.append((r.filename, r.chunk_index, r.content, r.embedding_vec))
    if not chunks:
        return []
    qvec = np.asarray(rag_index.embed_texts([question])[0], dtype=np.float32)
    scored = []
    for filename, idx, content, buf in chunks:
        scored.append((_cosine_f32(qvec, buf), filename, content))
    scored.sort(key=lambda t: t[0], reverse=True)
    min_sim = 0.20
    qualifying = [t for t in scored if t[0] >= min_sim]
    # If no chunk meets the threshold, fall back to the single best chunk so that
    # explicitly selected papers always contribute at least one grounding snippet.
    candidates = qualifying[:6] if qualifying else scored[:1]
    hits = []
    for score, filename, content in candidates:
        meta = build_paper_record(filename)
        hits.append({"filename": filename, "content": content, "score": score,
                     "title": meta.get("title", filename),
                     "author_name": meta.get("author_name", "")})
    return hits


def _attachment_grounding(question, conv_db_id):
    """Ground on documents attached to this conversation (transient scope)."""
    if conv_db_id is None:
        return []
    rows_data = []
    with db_session() as db:
        rows = (db.query(AttachmentChunkModel)
                  .filter(AttachmentChunkModel.conversation_id == conv_db_id).all())
        for r in rows:
            try:
                vec = json.loads(r.embedding) if r.embedding else []
            except (ValueError, TypeError):
                vec = []
            rows_data.append((r.filename, r.chunk_index, r.content, vec))
    if not rows_data:
        return []
    qvec = rag_index.embed_texts([question])[0]
    scored = []
    for filename, idx, content, vec in rows_data:
        scored.append((rag_index.cosine(qvec, vec), filename, content))
    scored.sort(key=lambda t: t[0], reverse=True)
    hits = []
    for score, filename, content in scored[:4]:
        hits.append({"filename": filename, "content": content, "score": score,
                     "title": filename, "author_name": str(_("Attached document")),
                     "is_attachment": True})
    return hits


def _attachment_filenames(conv_db_id):
    """Distinct attachment filenames for a conversation (for prompt + tool gating)."""
    if conv_db_id is None:
        return []
    with db_session() as db:
        rows = (db.query(AttachmentChunkModel.filename)
                  .filter(AttachmentChunkModel.conversation_id == conv_db_id)
                  .distinct().all())
        return [r[0] for r in rows]


def _read_attachment_text(conv_db_id, filename):
    """Reassembled full text of one attachment in this conversation; '' if absent."""
    if conv_db_id is None or not filename:
        return ""
    with db_session() as db:
        rows = (db.query(AttachmentChunkModel)
                  .filter(AttachmentChunkModel.conversation_id == conv_db_id,
                          AttachmentChunkModel.filename == filename)
                  .order_by(AttachmentChunkModel.chunk_index).all())
        contents = [r.content for r in rows]
    return rag_index.reassemble(contents) if contents else ""


def _ask_owner_key() -> str:
    """Stable per-browser key for owning conversations without a login."""
    session.permanent = True
    key = session.get("ask_owner")
    if not key:
        import uuid
        key = uuid.uuid4().hex
        session["ask_owner"] = key
    return key
