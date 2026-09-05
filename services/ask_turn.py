"""The Ask conversation turn.

One deep module behind a small interface: `run_ask_turn(input)` runs a single
Keydion AI turn — the streaming tool loop, the legacy single-shot
fallback, the round-cap forced answer, the citation split, and assistant-message
persistence — and yields typed event dicts. The HTTP route serializes those
dicts to SSE.

No Flask, no ORM models, no `db_session`: HTTP entry, retrieval, web search,
user-message persistence, and assistant-message persistence are the caller's
job. The turn owns only what a turn *is*. See CONTEXT.md § Keydion AI.
"""
import json
from dataclasses import dataclass
from typing import Any

import library_tools
from services.ai import (
    FETCH_URL_CALL_CAP,
    LIBRARY_TOOL_CALL_CAP,
    MAX_TOOL_CALLS_PER_ROUND,
    MAX_TOOL_RESULT_CHARS,
    MAX_TOOL_ROUNDS,
    WEB_SEARCH_CALL_CAP,
    _build_agentic_ask_prompt,
    _build_ask_prompt,
    _filter_cited,
    _tool_status_text,
)


@dataclass(slots=True, eq=False)
class AskTurnInput:
    """Plain carrier for the inputs `run_ask_turn` needs.

    Everything the inlined `generate()` closure used to capture is reified here.
    `client`, `deps`, `persist_assistant`, and `logger` are injected so a test
    can drive the turn with a fake OpenAI client and a recording persist callable.
    """

    question: str
    llm_messages: list[dict]
    mode: str
    model: str
    forced: list
    hits: list[dict]
    citations: list[dict]
    web_results: list[dict]
    locale_code: str
    include_web: bool
    attachment_names: list[str]
    client: Any
    deps: Any
    persist_assistant: Any
    logger: Any


def run_ask_turn(inp):
    """Run one Ask turn, yielding event dicts.

    Event vocabulary:
      {"type": "token", "text": str}
      {"type": "status", "text": str}
      {"type": "citations", "items": list}
      {"type": "web", "items": list}
      {"type": "done"}
      {"type": "error"}   # bare — the route fills the i18n message
    """
    full = []

    def _finish(shown_citations, shown_web):
        answer_text = "".join(full)
        yield {"type": "citations", "items": shown_citations}
        if shown_web:
            yield {"type": "web", "items": shown_web}
        if inp.persist_assistant is not None:
            try:
                inp.persist_assistant(answer_text, shown_citations)
            except Exception:
                inp.logger.exception("failed to persist assistant message")
        yield {"type": "done"}

    try:
        # Built eagerly (mirrors the original route) so a patched
        # _build_ask_prompt observes the filtered hits even on the agentic
        # path; only the legacy fallback actually consumes `system`.
        system = _build_ask_prompt(
            inp.question, inp.hits, inp.locale_code, inp.web_results)
        web_items = [
            {"n": len(inp.hits) + j + 1, "title": w["title"], "url": w["url"]}
            for j, w in enumerate(inp.web_results)
        ]

        attachment_names = inp.attachment_names
        include_attachment = bool(attachment_names)
        deps = inp.deps
        include_web = inp.include_web
        tool_schemas = library_tools.build_tool_schemas(
            include_web=include_web, include_attachment=include_attachment)
        # The advertised schema set is the ONLY executable capability set for
        # this turn: a model can emit tool names it was never offered (web
        # tools included), and the dispatcher must not honor them. This also
        # makes withheld web consent a server-side fact, not model guidance.
        allowed_tools = {
            schema["function"]["name"] for schema in tool_schemas
        }
        web_call_count = 0
        fetch_call_count = 0
        library_call_count = 0
        registry = library_tools.SourceRegistry()

        # Seed the registry from the retrieved hits (library + attachment
        # candidates), preserving order so [n] matches what the model sees.
        candidates = []
        for h in inp.hits:
            is_attachment = bool(h.get("is_attachment"))
            source_id = h["filename"] if is_attachment else h["paper_id"]
            source_meta = {
                "filename": h["filename"],
                "title": h.get("title") or h["filename"],
                "authors": h.get("author_name", ""),
                "url": (None if is_attachment else h.get("url")),
            }
            if not is_attachment:
                source_meta.update({
                    "paper_id": h["paper_id"],
                    "revision_number": h["revision_number"],
                })
            n = registry.register(
                source_id,
                source_meta,
                is_attachment=is_attachment,
            )
            candidate = {
                "n": n,
                "title": h.get("title") or h["filename"],
                "authors": h.get("author_name", ""),
                "filename": h["filename"],
                "snippet": (h.get("content") or "")[:500],
                "is_attachment": is_attachment,
            }
            if not is_attachment:
                candidate.update({
                    "paper_id": h["paper_id"],
                    "revision_number": h["revision_number"],
                })
            candidates.append(candidate)

        # Register web results into the SAME registry right after the library
        # seed, reserving contiguous numbers so papers discovered during the
        # loop never collide with web numbers.
        web_sources = []
        if inp.web_results:
            for w in inp.web_results:
                allowed_url = registry.allow_web_fetch(w["url"])
                if allowed_url is None:
                    continue
                wn = registry.register(allowed_url, {
                    "title": w["title"], "authors": "", "url": allowed_url,
                }, is_web=True)
                web_sources.append({
                    "n": wn, "title": w["title"], "url": allowed_url,
                    "snippet": (w.get("content") or "")[:500],
                })

        agentic_system = _build_agentic_ask_prompt(
            inp.question, candidates, web_sources, inp.locale_code,
            include_web=include_web, include_attachment=include_attachment,
            attachment_names=attachment_names)
        messages = [{"role": "system", "content": agentic_system}] + inp.llm_messages

        # Pre-read forced papers so the model has their full text up front.
        if inp.forced:
            results = []
            for paper_id in inp.forced:
                res = library_tools.run_tool(
                    "read_paper",
                    json.dumps({"paper_id": paper_id}),
                    registry,
                    deps,
                )
                if res and not res.startswith("Error:"):
                    results.append(res)
            if results:
                intro = ("The following are the full texts of papers the user "
                         "explicitly referenced. Use them to answer.")
                messages.append({"role": "system",
                                 "content": intro + "\n\n" + "\n\n".join(results)})

        client = inp.client
        model = inp.model
        answered = False
        for round_i in range(MAX_TOOL_ROUNDS):
            try:
                stream = client.chat.completions.create(
                    model=model, temperature=0.2, stream=True,
                    messages=messages, tools=tool_schemas,
                )
            except Exception:
                if round_i == 0:
                    # Provider likely lacks tool support — fall back to the
                    # legacy single-shot path that reproduces today's behavior.
                    inp.logger.warning(
                        "tool-calling create failed on first round; "
                        "falling back to legacy single-shot ask", exc_info=True)
                    full = []
                    legacy_stream = client.chat.completions.create(
                        model=model, temperature=0.2, stream=True,
                        messages=[{"role": "system", "content": system}] + inp.llm_messages,
                        )
                    for chunk in legacy_stream:
                        try:
                            delta = chunk.choices[0].delta.content or ""
                        except (AttributeError, IndexError):
                            delta = ""
                        if delta:
                            full.append(delta)
                            yield {"type": "token", "text": delta}
                    answer_text = "".join(full)
                    shown_citations = _filter_cited(inp.citations, answer_text)
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
                    yield {"type": "token", "text": content}
                for tc in (getattr(delta, "tool_calls", None) or []):
                    idx = tc.index
                    slot = acc.setdefault(idx, {"id": "", "name": "", "arguments": ""})
                    if getattr(tc, "id", None):
                        slot["id"] = tc.id
                    # Gemini returns opaque thought signatures here; echo them
                    # with the tool call so the provider can resume the turn.
                    extra_content = getattr(tc, "extra_content", None)
                    if isinstance(extra_content, dict) and extra_content:
                        slot["extra_content"] = extra_content
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
                        # Synthetic id if the provider omitted it; must match the tool message below.
                        {"id": c["id"] or f"call_{idx}", "type": "function",
                         "function": {"name": c["name"], "arguments": c["arguments"]},
                         **({"extra_content": c["extra_content"]} if "extra_content" in c else {})}
                        for idx, c in enumerate(calls)
                    ],
                })
                round_calls = 0
                for idx, c in enumerate(calls):
                    cid = c["id"] or f"call_{idx}"
                    name = c["name"]
                    if round_calls >= MAX_TOOL_CALLS_PER_ROUND:
                        messages.append({"role": "tool", "tool_call_id": cid,
                            "content": "Error: too many tool calls in one round; "
                                       "answer with what you have."})
                        continue
                    if name not in allowed_tools:
                        # Never execute a tool the model was not offered this
                        # turn (web tools without consent included); a
                        # prompt-influenced or non-conforming model cannot
                        # reach unadvertised capabilities.
                        messages.append({"role": "tool", "tool_call_id": cid,
                            "content": "Error: unknown tool; use only the tools "
                                       "listed in the instructions."})
                        continue
                    if name == "web_search":
                        if web_call_count >= WEB_SEARCH_CALL_CAP:
                            messages.append({"role": "tool", "tool_call_id": cid,
                                "content": "Error: web_search limit reached for "
                                           "this turn; answer with what you have."})
                            continue
                        web_call_count += 1
                    elif name == "fetch_url":
                        if fetch_call_count >= FETCH_URL_CALL_CAP:
                            messages.append({"role": "tool", "tool_call_id": cid,
                                "content": "Error: fetch_url limit reached for "
                                           "this turn; answer with what you have."})
                            continue
                        fetch_call_count += 1
                    else:
                        if library_call_count >= LIBRARY_TOOL_CALL_CAP:
                            messages.append({"role": "tool", "tool_call_id": cid,
                                "content": "Error: library tool limit reached for "
                                           "this turn; answer with what you have."})
                            continue
                        library_call_count += 1
                    round_calls += 1
                    yield {"type": "status",
                           "text": _tool_status_text(name, c["arguments"], registry, deps)}
                    result = library_tools.run_tool(name, c["arguments"], registry, deps)
                    # Budget each tool result appended to the next provider
                    # request (security finding: unbounded tool-output growth).
                    if len(result) > MAX_TOOL_RESULT_CHARS:
                        result = result[:MAX_TOOL_RESULT_CHARS] + "\n[truncated]"
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
                tool_choice="none",
            )
            for chunk in final_stream:
                try:
                    delta = chunk.choices[0].delta.content or ""
                except (AttributeError, IndexError):
                    delta = ""
                if delta:
                    full.append(delta)
                    yield {"type": "token", "text": delta}

        # Finish (agentic path): split citations into library vs web, then keep
        # only the sources the answer actually referenced.
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
        inp.logger.exception("LLM stream failed")
        yield {"type": "error"}
