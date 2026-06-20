"""Ask-the-Library page + conversation/ask API routes."""
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

import library_tools
import llm_client
import rag_index
import web_search
from config import OPEN_ACCESS
from db import db_session
from models import AttachmentChunkModel, ChatMessageModel, ConversationModel
from routes.shared import is_partial_request
from services.ai import (
    FETCH_URL_CALL_CAP,
    MAX_ATTACH_BYTES,
    MAX_QUESTION_CHARS,
    MAX_TOOL_ROUNDS,
    WEB_SEARCH_CALL_CAP,
    _ask_llm_messages,
    _ask_owner_key,
    _ask_rate_ok,
    _attachment_filenames,
    _attachment_grounding,
    _build_agentic_ask_prompt,
    _build_ask_prompt,
    _build_library_deps,
    _dedupe_hits_by_paper,
    _filter_cited,
    _forced_grounding,
    _tool_status_text,
)
from services.auth import get_active_user, is_ms_configured, require_login
from services.papers import extract_text_from_upload, gather_paper_records
from services.search import search_papers


def register_routes(app):

    # ==================== ASK-THE-LIBRARY ROUTES ====================

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
                    cites = json.loads(m.citations) if m.citations else []
                except (ValueError, TypeError):
                    cites = []
                try:
                    atts = json.loads(m.attachments) if m.attachments else []
                except (ValueError, TypeError):
                    atts = []
                try:
                    paps = json.loads(m.cited_papers) if m.cited_papers else []
                except (ValueError, TypeError):
                    paps = []
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
        records = search_papers(q) if q else gather_paper_records()
        items = [{
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
            with db_session() as db:
                db.query(AttachmentChunkModel).filter(
                    AttachmentChunkModel.conversation_id == conv_id,
                    AttachmentChunkModel.filename == fname).delete()
            return jsonify({"ok": True})

        # Rate-limit uploads: extraction (now including OCR for scanned PDFs) is
        # CPU-heavy, so cap per-IP to avoid a degradation-of-service via the route.
        ip = request.remote_addr or "?"
        if not _ask_rate_ok(ip):
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
        try:
            text = extract_text_from_upload(name, raw)
        except Exception:
            app.logger.exception("attachment extraction failed")
            return jsonify({"error": str(_("Could not read the file."))}), 400
        chunks = rag_index.chunk_text(text)
        if not chunks:
            return jsonify({"error": str(_("No readable text found in the file."))}), 400
        try:
            vectors = rag_index.embed_texts(chunks)
        except Exception:
            app.logger.exception("attachment embedding failed")
            return jsonify({"error": str(_("Something went wrong. Please try again."))}), 502
        display = name[:255]
        now = datetime.utcnow().isoformat()
        with db_session() as db:
            db.query(AttachmentChunkModel).filter(
                AttachmentChunkModel.conversation_id == conv_id,
                AttachmentChunkModel.filename == display).delete()
            for i, ch in enumerate(chunks):
                db.add(AttachmentChunkModel(
                    conversation_id=conv_id, filename=display, chunk_index=i,
                    content=ch, embedding=json.dumps(vectors[i]), created_at=now))
        return jsonify({"ok": True, "filename": display, "chunks": len(chunks)})

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
        web_on = bool(data.get("web"))
        msg_attachments = data.get("message_attachments") or []
        if not isinstance(msg_attachments, list):
            msg_attachments = []
        msg_attachments = [str(x)[:255] for x in msg_attachments[:10]]
        # Library papers cited with *this* message ({filename, title}). Persisted
        # per-message so the chip follows the message and the forced-grounding set
        # can be rebuilt as the union across the conversation (cited papers stay
        # as context for follow-ups).
        msg_papers = data.get("message_papers") or []
        if not isinstance(msg_papers, list):
            msg_papers = []
        clean_papers = []
        for it in msg_papers[:10]:
            if isinstance(it, dict) and it.get("filename"):
                clean_papers.append({
                    "filename": str(it.get("filename"))[:255],
                    "title": str(it.get("title") or it.get("filename"))[:255],
                })
        # Backward-compat: older cached frontends post a flat `paper_filenames`
        # list (no titles) instead of `message_papers`. Honor it so a stale
        # client never silently loses library-cite grounding (the title falls
        # back to the filename — display-only and replaced once new JS loads).
        legacy = data.get("paper_filenames")
        if isinstance(legacy, list):
            have = {p["filename"] for p in clean_papers}
            for fn in legacy[:10]:
                fn = str(fn)[:255]
                if fn and fn not in have:
                    have.add(fn)
                    clean_papers.append({"filename": fn, "title": fn})
        msg_papers = clean_papers
        forced = [p["filename"] for p in msg_papers]   # union'd across the conversation below
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
                    history_rows = [{"role": row.role, "content": row.content} for row in history_rows]
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
                        for it in cp_items:
                            fn = it.get("filename") if isinstance(it, dict) else None
                            if fn and fn not in seen_forced:
                                seen_forced.add(fn)
                                forced.append(fn)
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
            # Dedupe by paper first: retrieval is chunk-level, so one paper can
            # fill several slots and otherwise be cited as multiple sources.
            hits = _dedupe_hits_by_paper(attach_hits + lib_hits)[:6]
        except Exception:
            app.logger.exception("retrieval failed")
            hits = []

        model = llm_client.think_model() if mode == "think" else llm_client.flash_model()
        citations = [
            {"n": i + 1, "filename": h["filename"], "title": h["title"],
             "authors": h.get("author_name", ""),
             "url": (None if h.get("is_attachment")
                     else url_for("preview_paper", filename=h["filename"]))}
            for i, h in enumerate(hits)
        ]
        web_results = []
        if web_on and web_search.web_search_enabled():
            try:
                web_results = web_search.web_search(question)
            except Exception:
                app.logger.exception("web search failed")
                web_results = []
        system = _build_ask_prompt(question, hits, locale_code, web_results)
        web_items = [
            {"n": len(hits) + j + 1, "title": w["title"], "url": w["url"]}
            for j, w in enumerate(web_results)
        ]

        def generate():
            import json as _json
            full = []

            def _finish(shown_citations, shown_web):
                # Emit citations / web, persist the assistant message, finish.
                answer_text = "".join(full)
                yield "data: " + _json.dumps({"type": "citations", "items": shown_citations}) + "\n\n"
                if shown_web:
                    yield "data: " + _json.dumps({"type": "web", "items": shown_web}) + "\n\n"
                if db_conv_id is not None:
                    try:
                        with db_session() as db:
                            conv = db.query(ConversationModel).filter(
                                ConversationModel.id == db_conv_id,
                                ConversationModel.owner_key == owner).first()
                            if conv:
                                db.add(ChatMessageModel(
                                    conversation_id=db_conv_id, role="assistant",
                                    content=answer_text,
                                    citations=_json.dumps(shown_citations),
                                    created_at=datetime.utcnow().isoformat()))
                    except Exception:
                        app.logger.exception("failed to persist assistant message")
                yield "data: " + _json.dumps({"type": "done"}) + "\n\n"

            try:
                client = llm_client.build_client()
                attachment_names = _attachment_filenames(db_conv_id)
                include_attachment = bool(attachment_names)
                deps = _build_library_deps(db_conv_id)
                include_web = web_search.web_search_enabled()
                tool_schemas = library_tools.build_tool_schemas(
                    include_web=include_web, include_attachment=include_attachment)
                web_call_count = 0
                fetch_call_count = 0
                registry = library_tools.SourceRegistry()

                # Seed the registry from the retrieved hits (library + attachment
                # candidates), preserving order so [n] matches what the model sees.
                candidates = []
                for h in hits:
                    n = registry.register(h["filename"], {
                        "title": h.get("title") or h["filename"],
                        "authors": h.get("author_name", ""),
                        "url": (None if h.get("is_attachment")
                                else url_for("preview_paper", filename=h["filename"])),
                    })
                    candidates.append({
                        "n": n,
                        "title": h.get("title") or h["filename"],
                        "authors": h.get("author_name", ""),
                        "filename": h["filename"],
                        "snippet": (h.get("content") or "")[:500],
                        "is_attachment": bool(h.get("is_attachment")),
                    })

                # Register web results into the SAME registry right after the
                # library seed, reserving contiguous numbers so papers discovered
                # during the loop never collide with web numbers.
                web_sources = []
                if web_results:
                    for w in web_results:
                        wn = registry.register(w["url"], {
                            "title": w["title"], "authors": "", "url": w["url"],
                        }, is_web=True)
                        web_sources.append({
                            "n": wn, "title": w["title"], "url": w["url"],
                            "snippet": (w.get("content") or "")[:500],
                        })

                agentic_system = _build_agentic_ask_prompt(
                    question, candidates, web_sources, locale_code,
                    include_web=include_web, include_attachment=include_attachment,
                    attachment_names=attachment_names)
                messages = [{"role": "system", "content": agentic_system}] + llm_messages

                # Pre-read forced papers so the model has their full text up front.
                if forced:
                    results = []
                    for fn in forced:
                        res = library_tools.run_tool(
                            "read_paper", _json.dumps({"filename": fn}), registry, deps)
                        if res and not res.startswith("Error:"):
                            results.append(res)
                    if results:
                        intro = ("The following are the full texts of papers the user "
                                 "explicitly referenced. Use them to answer.")
                        messages.append({"role": "system",
                                         "content": intro + "\n\n" + "\n\n".join(results)})

                create_kwargs = {}
                if mode == "think":
                    create_kwargs["extra_body"] = {"thinking": {"type": "enabled"}}

                answered = False
                for round_i in range(MAX_TOOL_ROUNDS):
                    try:
                        stream = client.chat.completions.create(
                            model=model, temperature=0.2, stream=True,
                            messages=messages, tools=tool_schemas,
                            **create_kwargs,
                        )
                    except Exception:
                        if round_i == 0:
                            # Provider likely lacks tool support — fall back to the
                            # legacy single-shot path that reproduces today's behavior.
                            app.logger.warning(
                                "tool-calling create failed on first round; "
                                "falling back to legacy single-shot ask", exc_info=True)
                            full = []
                            # `system` was computed pre-generate via _build_ask_prompt;
                            # the fallback reproduces today's exact single-shot behavior.
                            legacy_stream = client.chat.completions.create(
                                model=model, temperature=0.2, stream=True,
                                messages=[{"role": "system", "content": system}] + llm_messages,
                                **create_kwargs,
                            )
                            for chunk in legacy_stream:
                                delta = ""
                                try:
                                    delta = chunk.choices[0].delta.content or ""
                                except (AttributeError, IndexError):
                                    delta = ""
                                if delta:
                                    full.append(delta)
                                    yield "data: " + _json.dumps(
                                        {"type": "token", "text": delta}) + "\n\n"
                            answer_text = "".join(full)
                            shown_citations = _filter_cited(citations, answer_text)
                            shown_web = _filter_cited(web_items, answer_text)
                            yield from _finish(shown_citations, shown_web)
                            return
                        raise

                    # Accumulate tool calls by index (fragments stream in pieces).
                    acc = {}
                    round_content = []
                    for chunk in stream:
                        try:
                            delta = chunk.choices[0].delta
                        except (AttributeError, IndexError):
                            continue
                        content = getattr(delta, "content", None) or ""
                        if content:
                            round_content.append(content)
                            yield "data: " + _json.dumps(
                                {"type": "token", "text": content}) + "\n\n"
                        for tc in (getattr(delta, "tool_calls", None) or []):
                            idx = tc.index
                            slot = acc.setdefault(idx, {"id": "", "name": "", "arguments": ""})
                            if getattr(tc, "id", None):
                                slot["id"] = tc.id
                            fn = getattr(tc, "function", None)
                            if fn is not None:
                                if getattr(fn, "name", None):
                                    slot["name"] = fn.name
                                if getattr(fn, "arguments", None):
                                    slot["arguments"] += fn.arguments

                    if acc:
                        calls = [acc[i] for i in sorted(acc)]
                        messages.append({
                            "role": "assistant",
                            "content": "".join(round_content) or None,
                            "tool_calls": [
                                # Use synthetic id if provider omitted it; must match tool message below.
                                {"id": c["id"] or f"call_{idx}", "type": "function",
                                 "function": {"name": c["name"], "arguments": c["arguments"]}}
                                for idx, c in enumerate(calls)
                            ],
                        })
                        for idx, c in enumerate(calls):
                            cid = c["id"] or f"call_{idx}"
                            if c["name"] == "web_search":
                                if web_call_count >= WEB_SEARCH_CALL_CAP:
                                    messages.append({"role": "tool", "tool_call_id": cid,
                                        "content": "Error: web_search limit reached for "
                                                   "this turn; answer with what you have."})
                                    continue
                                web_call_count += 1
                            if c["name"] == "fetch_url":
                                if fetch_call_count >= FETCH_URL_CALL_CAP:
                                    messages.append({"role": "tool", "tool_call_id": cid,
                                        "content": "Error: fetch_url limit reached for "
                                                   "this turn; answer with what you have."})
                                    continue
                                fetch_call_count += 1
                            yield "data: " + _json.dumps({
                                "type": "status",
                                "text": _tool_status_text(
                                    c["name"], c["arguments"], registry, deps),
                            }) + "\n\n"
                            result = library_tools.run_tool(
                                c["name"], c["arguments"], registry, deps)
                            messages.append({"role": "tool", "tool_call_id": cid,
                                             "content": result})
                        continue

                    # No tool calls — this round was the final answer.
                    full.extend(round_content)
                    answered = True
                    break

                if not answered:
                    # Round cap hit — force one final answer with no more tools.
                    final_stream = client.chat.completions.create(
                        model=model, temperature=0.2, stream=True,
                        messages=messages, tools=tool_schemas,
                        tool_choice="none", **create_kwargs,
                    )
                    for chunk in final_stream:
                        delta = ""
                        try:
                            delta = chunk.choices[0].delta.content or ""
                        except (AttributeError, IndexError):
                            delta = ""
                        if delta:
                            full.append(delta)
                            yield "data: " + _json.dumps(
                                {"type": "token", "text": delta}) + "\n\n"

                # Finish (agentic path): split citations into library vs web, then
                # keep only the sources the answer actually referenced.
                answer_text = "".join(full)
                all_cites = registry.as_citations()
                lib_citations = [c for c in all_cites if not c["is_web"]]
                web_citations = [
                    {"n": c["n"], "title": c["title"], "url": c["url"]}
                    for c in all_cites if c["is_web"]
                ]
                shown_citations = _filter_cited(lib_citations, answer_text)
                shown_web = _filter_cited(web_citations, answer_text)
                yield from _finish(shown_citations, shown_web)
            except Exception:
                app.logger.exception("LLM stream failed")
                yield "data: " + _json.dumps({"type": "error",
                       "message": str(_("Something went wrong. Please try again."))}) + "\n\n"

        return Response(stream_with_context(generate()), mimetype="text/event-stream")
