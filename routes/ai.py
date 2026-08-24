"""Keydion AI page + conversation/ask API routes."""
import json
from datetime import datetime

from flask import (
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    stream_with_context,
    url_for,
)
from flask_babel import get_locale, gettext as _

import llm_client
import rag_index
import web_search
from config import (
    MAX_ASK_HISTORY_MESSAGES,
    MAX_CONVERSATIONS_PER_OWNER,
    MAX_FORCED_PAPERS_PER_TURN,
    OPEN_ACCESS,
)
from db import db_session
from models import AttachmentChunkModel, ChatMessageModel, ConversationModel
from routes.shared import is_partial_request
from services.ai import (
    MAX_ATTACH_BYTES,
    MAX_QUESTION_CHARS,
    _ask_llm_messages,
    _ask_owner_key,
    _ask_rate_ok,
    _attachment_filenames,
    _attachment_grounding,
    _build_library_deps,
    _forced_grounding,
    _normalize_stored_paper_references,
    _prepare_available_grounding_hits,
)
from services.ask_turn import AskTurnInput, run_ask_turn
from services.attachment_jobs import (
    AttachmentQuotaExceeded,
    attachment_job_status_for_owner,
    cancel_attachment,
    delete_conversation_attachment_jobs,
    queue_attachment,
)
from services.attachment_processing import AttachmentProcessingError
from services.auth import get_active_user, is_ms_configured, require_login
from services.papers import extract_text_from_upload, gather_paper_records
from services.search import search_papers


def register_routes(app):

    # ==================== KEYDION AI ROUTES ====================

    def require_ask_api_access():
        if OPEN_ACCESS or get_active_user():
            return None
        return jsonify({"error": str(_("Please sign in first."))}), 401

    @app.route("/ai")
    @app.route("/ai/<serial>")
    def ai(serial=None):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        
        if serial:
            owner = _ask_owner_key()
            with db_session() as db:
                conv = db.query(ConversationModel).filter(
                    ConversationModel.serial == serial,
                    ConversationModel.owner_key == owner).first()
                if not conv:
                    return redirect(url_for("ai"))

        suggestions = [
            _("What does the research say about climate adaptation in plants?"),
            _("Summarize recent Extended Essays in economics."),
            _("Find papers about machine learning in healthcare."),
        ]
        boot = {
            "ask_url": url_for("ai"),
            "api_url": url_for("api_ai"),
            "enabled": llm_client.llm_enabled(),
            "web_enabled": web_search.web_search_enabled(),
            "i18n": {
                "title": "Keydion AI",
                "empty_title": "Keydion AI",
                "empty_sub": _("Ask a question and I'll answer from the published library, with citations."),
                "placeholder": _("Message Keydion AI…"),
                "flash": _("Flash"),
                "thinking": _("Thinking"),
                "send": _("Send"),
                "sources": _("Cited from your library"),
                "copy": _("Copy"),
                "regenerate": _("Regenerate"),
                "rename": _("Rename"),
                "today": _("Today"),
                "yesterday": _("Yesterday"),
                "previous_7_days": _("Previous 7 days"),
                "older": _("Older"),
                "no_conversations_match": _("No conversations match."),
                "thinking_state": _("Thinking…"),
                "error": _("Something went wrong. Please try again."),
                "disabled": _("AI assistant is not configured."),
                "no_sources": _("No matching papers were found in the library."),
                "searched_web": _("Searched the web"),
                "selected": _("selected"),
                "select_hint": _("Select papers to attach as citations"),
                "preview_abstract_label": _("Abstract"),
                "preview_no_abstract": _("No abstract available."),
                "preview_hint": _("Hover a paper to preview its abstract before citing."),
            },
            "active_serial": serial,
        }
        return render_template(
            "ai.html",
            partial=is_partial_request(),
            llm_enabled=llm_client.llm_enabled(),
            web_enabled=web_search.web_search_enabled(),
            ms_enabled=is_ms_configured(),
            suggestions=suggestions,
            ask_boot=boot,
        )

    @app.route("/api/conversations", methods=["GET", "POST"])
    def api_conversations():
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        owner = _ask_owner_key()
        if request.method == "POST":
            import secrets
            now = datetime.utcnow().isoformat()
            with db_session() as db:
                # Per-owner conversation quota (security finding: unbounded
                # persistent conversation rows without admission control).
                existing = (
                    db.query(ConversationModel)
                    .filter(ConversationModel.owner_key == owner)
                    .count()
                )
                if existing >= MAX_CONVERSATIONS_PER_OWNER:
                    return jsonify({"error": str(_(
                        "You have too many conversations — delete one before starting a new one."))}), 429
                serial = secrets.token_urlsafe(5)[:6]
                conv = ConversationModel(owner_key=owner,
                                         serial=serial,
                                         title=str(_("New conversation")),
                                         created_at=now, updated_at=now)
                db.add(conv)
                db.flush()
                cid = conv.serial
            return jsonify({"id": cid, "title": str(_("New conversation"))}), 201
        with db_session() as db:
            rows = (db.query(ConversationModel)
                      .filter(ConversationModel.owner_key == owner)
                      .order_by(ConversationModel.updated_at.desc()).all())
            items = [{"id": r.serial, "title": r.title, "updated_at": r.updated_at} for r in rows]
        return jsonify({"conversations": items})

    @app.route("/api/conversations/<string:serial>", methods=["GET", "PATCH", "DELETE"])
    def api_conversation_item(serial):
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        owner = _ask_owner_key()
        with db_session() as db:
            conv = db.query(ConversationModel).filter(
                ConversationModel.serial == serial,
                ConversationModel.owner_key == owner).first()
            if not conv:
                return jsonify({"error": str(_("Not found"))}), 404
            if request.method == "DELETE":
                db.query(ChatMessageModel).filter(
                    ChatMessageModel.conversation_id == conv.id).delete()
                db.query(AttachmentChunkModel).filter(
                    AttachmentChunkModel.conversation_id == conv.id).delete()
                delete_conversation_attachment_jobs(conv.id, db)
                db.delete(conv)
                return jsonify({"ok": True})
            if request.method == "PATCH":
                data = request.get_json(silent=True) or {}
                title = (data.get("title") or "").strip()
                if title:
                    conv.title = title[:255]
                return jsonify({"ok": True, "title": conv.title})
            # GET messages
            msgs = (db.query(ChatMessageModel)
                      .filter(ChatMessageModel.conversation_id == conv.id)
                      .order_by(ChatMessageModel.id.asc()).all())
            out = []
            for m in msgs:
                try:
                    stored_cites = json.loads(m.citations) if m.citations else []
                except (ValueError, TypeError):
                    stored_cites = []
                cites = _normalize_stored_paper_references(
                    stored_cites,
                    app.extensions["paper_library"],
                    allow_legacy_aliases=True,
                    preserve_attachments=True,
                )
                for citation in cites:
                    if not citation.get("is_attachment"):
                        citation["url"] = url_for(
                            "preview_paper", paper_id=citation["paper_id"]
                        )
                try:
                    atts = json.loads(m.attachments) if m.attachments else []
                except (ValueError, TypeError):
                    atts = []
                try:
                    stored_paps = json.loads(m.cited_papers) if m.cited_papers else []
                except (ValueError, TypeError):
                    stored_paps = []
                paps = _normalize_stored_paper_references(
                    stored_paps,
                    app.extensions["paper_library"],
                    allow_legacy_aliases=True,
                )
                out.append({"role": m.role, "content": m.content,
                            "citations": cites, "attachments": atts, "papers": paps})
            att = (db.query(AttachmentChunkModel.filename)
                     .filter(AttachmentChunkModel.conversation_id == conv.id)
                     .distinct().all())
            attachments = [a[0] for a in att]
            return jsonify({"title": conv.title, "messages": out,
                            "attachments": attachments})

    @app.route("/api/ai/papers")
    def api_ai_papers():
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        q = (request.args.get("q") or "").strip()
        visible_records = gather_paper_records(
            app.extensions["paper_library"]
        )
        if q:
            visible_by_id = {
                record["paper_id"]: record for record in visible_records
            }
            records = [
                visible_by_id[record["paper_id"]]
                for record in search_papers(q)
                if record.get("paper_id") in visible_by_id
            ]
        else:
            records = visible_records
        items = [{
            "paper_id": r["paper_id"],
            "revision_number": r["current_revision"],
            "filename": r["filename"],
            "title": r.get("title") or r["filename"],
            "authors": r.get("author_name", ""),
            "category": r.get("category", ""),
            "abstract": (r.get("abstract") or "")[:400],
        } for r in records[:50]]
        return jsonify({"papers": items})

    @app.route("/api/ai/attach", methods=["POST", "DELETE"])
    def api_ai_attach():
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        if not llm_client.llm_enabled():
            return jsonify({"error": str(_("AI assistant is not configured."))}), 503
        owner = _ask_owner_key()
        conv_serial = (request.values.get("conversation_id") or "").strip()
        with db_session() as db:
            conv = db.query(ConversationModel).filter(
                ConversationModel.serial == conv_serial,
                ConversationModel.owner_key == owner).first()
            conv_id = conv.id if conv else None
        if conv_id is None:
            return jsonify({"error": str(_("Conversation not found."))}), 404

        if request.method == "DELETE":
            fname = (request.values.get("filename") or "").strip()
            cancel_attachment(conv_id, fname)
            return jsonify({"ok": True})

        # Rate-limit uploads: extraction (now including OCR for scanned PDFs) is
        # CPU-heavy, so cap per-IP to avoid a degradation-of-service via the route.
        ip = request.remote_addr or "?"
        if not _ask_rate_ok(ip, scope="attachment"):
            return jsonify({"error": str(_("Too many requests — please slow down."))}), 429

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        name = upload.filename
        if not name.lower().endswith((".pdf", ".docx", ".txt", ".md")):
            return jsonify({"error": str(_("Unsupported file type. Use PDF, DOCX, TXT, or Markdown."))}), 400
        raw = upload.read()
        if len(raw) > MAX_ATTACH_BYTES:
            return jsonify({"error": str(_("File is too large (max 5 MB)."))}), 400
        display = name.replace("\\", "/").rsplit("/", 1)[-1].strip()[:255]
        if not display or any(ord(character) < 32 for character in display):
            return jsonify({"error": str(_("Invalid filename."))}), 400
        try:
            job_id = queue_attachment(conv_id, display, raw)
        except AttachmentQuotaExceeded:
            return jsonify({"error": str(_(
                "Attachment limit reached — remove an attachment or try a smaller file."))}), 429
        except AttachmentProcessingError:
            return jsonify({"error": str(_("Could not read the file."))}), 400
        except Exception:
            app.logger.exception("attachment job enqueue failed")
            return jsonify({"error": str(_("Something went wrong. Please try again."))}), 502
        return jsonify({
            "ok": True,
            "job_id": job_id,
            "filename": display,
            "state": "queued",
            "status_url": url_for("api_ai_attachment_job", job_id=job_id),
        }), 202

    @app.get("/api/ai/attach/<string:job_id>")
    def api_ai_attachment_job(job_id):
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        owner = _ask_owner_key()
        status = attachment_job_status_for_owner(job_id, owner)
        if status is None:
            return jsonify({"error": str(_("Not found"))}), 404
        payload = {
            "job_id": status.id,
            "filename": status.filename,
            "state": status.state,
        }
        if status.state == "failed":
            payload["error"] = str(_("Could not process the attachment."))
        return jsonify(payload)

    @app.route("/api/ai", methods=["POST"])
    def api_ai():
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        if not llm_client.llm_enabled():
            return jsonify({"error": str(_("AI assistant is not configured."))}), 503

        ip = request.remote_addr or "?"
        if not _ask_rate_ok(ip):
            return jsonify({"error": str(_("Too many requests — please slow down."))}), 429

        data = request.get_json(silent=True) or {}
        question = (data.get("question") or "").strip()
        mode = data.get("mode") if data.get("mode") in ("flash", "think") else "flash"
        # Only the JSON boolean true is consent.  Truthy strings and a globally
        # configured provider must never enable outbound tools on their own.
        web_on = data.get("web") is True
        msg_attachments = data.get("message_attachments") or []
        if not isinstance(msg_attachments, list):
            msg_attachments = []
        msg_attachments = [str(x)[:255] for x in msg_attachments[:10]]
        # Library Papers cited with this message. Client display fields are
        # ignored; exact current metadata is re-projected from PaperLibrary.
        # per-message so the chip follows the message and the forced-grounding set
        # can be rebuilt as the union across the conversation (cited papers stay
        # as context for follow-ups).
        msg_papers = data.get("message_papers") or []
        if not isinstance(msg_papers, list):
            msg_papers = []
        msg_papers = _normalize_stored_paper_references(
            msg_papers[:10],
            app.extensions["paper_library"],
            allow_legacy_aliases=False,
        )
        forced = [p["paper_id"] for p in msg_papers]
        if not question:
            return jsonify({"error": str(_("Please enter a question."))}), 400
        if len(question) > MAX_QUESTION_CHARS:
            return jsonify({"error": str(_("Your question is too long."))}), 400

        conv_serial = data.get("conversation_id")
        owner = _ask_owner_key()
        history_rows = []
        db_conv_id = None
        if conv_serial is not None:
            with db_session() as db:
                conv = db.query(ConversationModel).filter(
                    ConversationModel.serial == conv_serial,
                    ConversationModel.owner_key == owner).first()
                if conv:
                    db_conv_id = conv.id
                    now = datetime.utcnow().isoformat()
                    db.add(ChatMessageModel(conversation_id=db_conv_id, role="user",
                                            content=question, citations="",
                                            attachments=json.dumps(msg_attachments),
                                            cited_papers=json.dumps(msg_papers),
                                            created_at=now))
                    # title the conversation from its first question
                    if conv.title == str(_("New conversation")):
                        conv.title = question[:60]
                    conv.updated_at = now
                    db.flush()
                    history_rows = (db.query(ChatMessageModel)
                                      .filter(ChatMessageModel.conversation_id == db_conv_id)
                                      .order_by(ChatMessageModel.id.asc()).all())
                    # History window: every turn resent the whole
                    # conversation to the provider with no bound (security
                    # finding: unbounded history accumulation). Only the most
                    # recent messages travel; the rest stay in the DB.
                    history_rows = [
                        {"role": row.role, "content": row.content}
                        for row in history_rows[-MAX_ASK_HISTORY_MESSAGES:]
                    ]
                    # Forced grounding = union of every message's cited papers, so a
                    # paper cited earlier stays in context even after its composer
                    # chip moved onto that message.
                    forced = []
                    seen_forced = set()
                    cp_rows = (db.query(ChatMessageModel.cited_papers)
                                 .filter(ChatMessageModel.conversation_id == db_conv_id,
                                         ChatMessageModel.role == "user").all())
                    for (cp,) in cp_rows:
                        try:
                            cp_items = json.loads(cp) if cp else []
                        except (ValueError, TypeError):
                            cp_items = []
                        references = _normalize_stored_paper_references(
                            cp_items,
                            app.extensions["paper_library"],
                            allow_legacy_aliases=True,
                        )
                        for reference in references:
                            paper_id = reference["paper_id"]
                            if paper_id not in seen_forced:
                                seen_forced.add(paper_id)
                                forced.append(paper_id)
                    # Bound the forced-grounding set: every cited paper is
                    # reread and appended to the prompt each turn (security
                    # finding: unbounded source accumulation).
                    forced = forced[:MAX_FORCED_PAPERS_PER_TURN]
                else:
                    conv_serial = None
        llm_messages = _ask_llm_messages(question, history_rows)

        locale_code = str(get_locale() or "en")

        # Retrieve grounding: attached docs first (highest priority), then forced
        # papers (Phase 3) or automatic retrieval, capped to a shared budget.
        try:
            attach_hits = _attachment_grounding(question, db_conv_id)
            if forced:
                lib_hits = _forced_grounding(question, forced)
            else:
                lib_hits = rag_index.retrieve(question)
            # Reject stale revisions before merging chunk-level retrieval hits.
            hits = _prepare_available_grounding_hits(
                attach_hits + lib_hits,
                app.extensions["paper_library"],
            )[:6]
        except Exception:
            app.logger.error("Ask library retrieval failed")
            hits = []

        model = llm_client.think_model() if mode == "think" else llm_client.flash_model()
        citations = []
        for i, hit in enumerate(hits):
            citation = {
                "n": i + 1,
                "filename": hit["filename"],
                "title": hit["title"],
                "authors": hit.get("author_name", ""),
                "url": None,
            }
            if hit.get("is_attachment"):
                citation["is_attachment"] = True
            else:
                citation.update({
                    "paper_id": hit["paper_id"],
                    "revision_number": hit["revision_number"],
                    "url": url_for("preview_paper", paper_id=hit["paper_id"]),
                })
            citations.append(citation)
        web_results = []
        if web_on and web_search.web_search_enabled():
            try:
                web_results = web_search.web_search(question)
            except Exception:
                app.logger.exception("web search failed")
                web_results = []
        # Pre-build the citation URL on each non-attachment hit so the turn
        # module can seed the registry without reaching into Flask's url_for.
        for h in hits:
            if not h.get("is_attachment") and h.get("paper_id"):
                h.setdefault("url", url_for("preview_paper", paper_id=h["paper_id"]))

        include_web = web_on and web_search.web_search_enabled()
        attachment_names = _attachment_filenames(db_conv_id)
        deps = _build_library_deps(db_conv_id)
        client = llm_client.build_client()

        def persist_assistant(answer_text, shown_citations):
            with db_session() as db:
                conv = db.query(ConversationModel).filter(
                    ConversationModel.id == db_conv_id,
                    ConversationModel.owner_key == owner).first()
                if conv:
                    db.add(ChatMessageModel(
                        conversation_id=db_conv_id, role="assistant",
                        content=answer_text,
                        citations=json.dumps(shown_citations),
                        created_at=datetime.utcnow().isoformat()))

        turn_input = AskTurnInput(
            question=question,
            llm_messages=llm_messages,
            mode=mode,
            model=model,
            forced=forced,
            hits=hits,
            citations=citations,
            web_results=web_results,
            locale_code=locale_code,
            include_web=include_web,
            attachment_names=attachment_names,
            client=client,
            deps=deps,
            persist_assistant=persist_assistant if db_conv_id is not None else None,
            logger=app.logger,
        )

        def sse_stream():
            for event in run_ask_turn(turn_input):
                if event.get("type") == "error":
                    event = {"type": "error",
                             "message": str(_("Something went wrong. Please try again."))}
                yield "data: " + json.dumps(event) + "\n\n"

        return Response(stream_with_context(sse_stream()), mimetype="text/event-stream")
