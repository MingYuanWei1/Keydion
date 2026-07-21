"""Pure tool-calling core for Ask-the-Library agentic mode.

Public surface
--------------
PAPER_TEXT_CHAR_CAP : int
    Safety-net character cap on read_paper output (200 000).

TOOL_SCHEMAS : list[dict]
    Two OpenAI function-calling tool schemas to pass as ``tools=`` to
    ``client.chat.completions.create``:
      - ``search_library(query)`` — search the paper library.
      - ``read_paper(paper_id)`` — fetch the full text of one paper.

SourceRegistry
    Assigns a stable, 1-based ``[n]`` citation number to each paper as it is
    surfaced (by search or read).  Thread-safety is out of scope; one registry
    per request is the expected pattern.

run_tool(name, arguments, registry, deps) -> str
    Dispatches one tool call and returns a plain string for the model.
    Never raises.

``deps`` contract
-----------------
``deps`` is any object with four callables:

* ``deps.search(query: str) -> list[dict]``
      Each dict has keys: ``paper_id``, ``revision_number``, ``filename``,
      ``title``, ``authors``, ``url``, and ``snippet``.
* ``deps.full_text(paper_id: str) -> str``
      Reassembled full text of the paper; ``""`` if unavailable.
* ``deps.paper_meta(paper_id: str) -> dict``
      Current ``paper_id``, ``revision_number``, ``filename``, ``title``, and
      ``authors`` display metadata.
* ``deps.paper_url(paper_id: str) -> str | None``
      Canonical URL for the paper, or ``None``.
* ``deps.web_search(query: str) -> list[dict]``  (optional)
      Each dict: ``title``, ``url``, ``content``.  Used by the web_search tool.

No Flask, SQLAlchemy, or app-level imports.  A later task wires concrete
DB-backed implementations and runs the tool loop inside a Flask route.
"""

from __future__ import annotations

import json
from uuid import UUID

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PAPER_TEXT_CHAR_CAP = 200_000

# ---------------------------------------------------------------------------
# Tool schemas (passed as tools= to the chat completions API)
# ---------------------------------------------------------------------------

TOOL_SCHEMAS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "search_library",
            "description": (
                "Search the paper library and return candidate papers that match "
                "the query. Each result includes a citation number [n], the paper "
                "title, authors, Paper UUID, filename, and a relevant text snippet. Use this "
                "tool first to discover which papers are relevant before deciding "
                "whether to read one in full."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "A natural-language search query describing the topic "
                            "or question you want to find papers about."
                        ),
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_paper",
            "description": (
                "Return the FULL text of one paper given its Paper UUID (as shown "
                "in a search_library result). Use this when you need to explain a "
                "paper in detail, quote from it, or when a snippet from "
                "search_library is insufficient to answer the question."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "paper_id": {
                        "type": "string",
                        "description": (
                            "The immutable Paper UUID from a search_library result "
                            "(for example, \"22222222-2222-4222-8222-222222222222\")."
                        ),
                    }
                },
                "required": ["paper_id"],
            },
        },
    },
]

WEB_SEARCH_SCHEMA: dict = {
    "type": "function",
    "function": {
        "name": "web_search",
        "description": (
            "Search the public web for current events or information not in the "
            "paper library. Prefer search_library FIRST; use web_search only when "
            "the library does not cover the question or you need up-to-date facts. "
            "Returns titles, URLs, and short snippets, each with a citation [n]."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A natural-language web search query.",
                }
            },
            "required": ["query"],
        },
    },
}

# Tool groups added to the base pair by build_tool_schemas(). Later phases append
# FETCH_URL_SCHEMA (Phase B) and populate ATTACHMENT_TOOL_SCHEMAS (Phase C).
FETCH_URL_SCHEMA: dict = {
    "type": "function",
    "function": {
        "name": "fetch_url",
        "description": (
            "Fetch and read the FULL text of a web page (e.g. a URL returned by "
            "web_search whose snippet is too short). Returns the page's main text."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The http(s) URL to fetch."}
            },
            "required": ["url"],
        },
    },
}

WEB_TOOL_SCHEMAS: list[dict] = [WEB_SEARCH_SCHEMA, FETCH_URL_SCHEMA]

READ_ATTACHMENT_SCHEMA: dict = {
    "type": "function",
    "function": {
        "name": "read_attachment",
        "description": (
            "Read the FULL text of a document the user attached to THIS "
            "conversation, by its filename (as listed in the prompt)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "filename": {"type": "string", "description": "The attachment filename."}
            },
            "required": ["filename"],
        },
    },
}

ATTACHMENT_TOOL_SCHEMAS: list[dict] = [READ_ATTACHMENT_SCHEMA]


def build_tool_schemas(include_web: bool = False,
                       include_attachment: bool = False) -> list[dict]:
    """Return the tool schemas to offer the model, gated by capability flags.
    Base = search_library + read_paper; +web tools when include_web; +attachment
    tools when include_attachment."""
    schemas = list(TOOL_SCHEMAS)
    if include_web:
        schemas += WEB_TOOL_SCHEMAS
    if include_attachment:
        schemas += ATTACHMENT_TOOL_SCHEMAS
    return schemas


# ---------------------------------------------------------------------------
# SourceRegistry
# ---------------------------------------------------------------------------


class SourceRegistry:
    """Assigns stable, 1-based [n] citation numbers to papers.

    Registration is idempotent per typed source identity. Papers use UUIDs,
    attachments use filenames, and web sources use URLs. Each call can
    contribute additional metadata — any stored field that is currently
    empty/falsy is filled in from the new call's non-empty value, so the
    richest available info is retained.
    """

    def __init__(self) -> None:
        self._by_identity: dict[tuple[str, str], dict] = {}
        self._counter: int = 0

    def register(self, source_id: str, meta: dict, is_web: bool = False,
                 is_attachment: bool = False) -> int | None:
        """Register a typed source and return its citation number.

        Parameters
        ----------
        source_id:
            Paper UUID, attachment filename, or web URL. Empty values are ignored and
            ``None`` is returned so callers do not need to guard themselves.
        meta:
            Dict that may contain ``"title"``, ``"authors"``, ``"url"``.
            Missing or falsy values in the stored record are backfilled from
            the newly provided non-empty values.

        Returns
        -------
        int
            The stable citation number ``n`` (1-based).
        None
            If ``source_id`` is empty/falsy.
        """
        if not source_id:
            return None

        kind = "web" if is_web else "attachment" if is_attachment else "paper"
        source_id = str(source_id)
        if kind == "paper":
            try:
                source_id = str(UUID(source_id))
            except (AttributeError, TypeError, ValueError):
                return None
            if (
                type(meta.get("revision_number")) is not int
                or not isinstance(meta.get("filename"), str)
                or not meta.get("filename")
            ):
                return None

        key = (kind, source_id)
        if key in self._by_identity:
            stored = self._by_identity[key]
            if (
                kind == "paper"
                and type(meta.get("revision_number")) is int
                and isinstance(meta.get("filename"), str)
                and meta.get("filename")
            ):
                # A later trusted Paper projection may observe a revision switch
                # during the request. Keep [n], but never retain stale display or
                # revision metadata for the same immutable UUID.
                stored.update({
                    "paper_id": source_id,
                    "revision_number": meta["revision_number"],
                    "filename": meta["filename"],
                    "title": meta.get("title") or meta["filename"],
                    "authors": meta.get("authors") or "",
                    "url": meta.get("url") or "",
                })
            else:
                # Web and attachment callers can backfill missing display fields.
                for field in (
                    "paper_id", "revision_number", "filename", "title", "authors", "url"
                ):
                    if not stored.get(field) and meta.get(field):
                        stored[field] = meta[field]
            if is_web:
                stored["is_web"] = True
            if is_attachment:
                stored["is_attachment"] = True
            return stored["n"]

        self._counter += 1
        self._by_identity[key] = {
            "n": self._counter,
            "paper_id": None if kind != "paper" else source_id,
            "revision_number": meta.get("revision_number") if kind == "paper" else None,
            "filename": meta.get("filename") or (str(source_id) if kind == "attachment" else ""),
            "title": meta.get("title") or "",
            "authors": meta.get("authors") or "",
            "url": meta.get("url") or "",
            "is_web": bool(is_web),
            "is_attachment": bool(is_attachment),
        }
        return self._counter

    def as_citations(self) -> list[dict]:
        """Return all registered sources as dicts, sorted by ascending ``n``.

        Paper rows include exact UUID/revision/display metadata. Attachment
        rows retain filename identity and web rows retain URL identity.
        """
        rows = sorted(self._by_identity.values(), key=lambda r: r["n"])
        result = []
        for r in rows:
            result.append({
                "n": r["n"],
                "paper_id": r["paper_id"],
                "revision_number": r["revision_number"],
                "filename": r["filename"],
                "title": r["title"] or r["filename"] or r["url"],
                "authors": r["authors"],
                "url": r["url"],
                "is_web": r.get("is_web", False),
                "is_attachment": r.get("is_attachment", False),
            })
        return result


# ---------------------------------------------------------------------------
# run_tool
# ---------------------------------------------------------------------------


def run_tool(name: str, arguments: str | dict, registry: SourceRegistry, deps) -> str:
    """Execute one tool call and return a string for the model.

    Parameters
    ----------
    name:
        The tool name (``"search_library"`` or ``"read_paper"``).
    arguments:
        The raw JSON string emitted by the model, or already a dict.
        Malformed JSON yields an error string (no crash).
    registry:
        The active ``SourceRegistry`` for this conversation turn.
    deps:
        An object satisfying the ``deps`` contract described in the module
        docstring.

    Returns
    -------
    str
        A plain text result to feed back to the model as the tool result.
        Never raises — every failure path returns a model-recoverable error
        string.
    """
    # Parse arguments.
    if isinstance(arguments, dict):
        args = arguments
    else:
        try:
            args = json.loads(arguments)
        except (json.JSONDecodeError, TypeError) as exc:
            return f"Error: could not parse tool arguments as JSON ({exc})."

    if not isinstance(args, dict):
        return 'Error: tool arguments must be a JSON object, e.g. {"query": "..."}.'

    if name == "search_library":
        query = str(args.get("query") or "").strip()
        if not query:
            return "Error: search_library requires a non-empty 'query' argument."

        try:
            candidates = deps.search(query)
        except Exception as exc:
            return f"Error: library search failed ({exc}). Try a different query."

        if not candidates:
            return (
                f"No papers matched the query \"{query}\". "
                "Try a different search term or a broader query."
            )

        blocks: list[str] = []
        for c in candidates:
            paper_id = c.get("paper_id") or ""
            revision_number = c.get("revision_number")
            filename = c.get("filename") or ""
            if not paper_id or type(revision_number) is not int or not filename:
                continue
            title = c.get("title") or filename
            authors = c.get("authors") or ""
            snippet = c.get("snippet") or ""
            n = registry.register(
                paper_id,
                {
                    "paper_id": paper_id,
                    "revision_number": revision_number,
                    "filename": filename,
                    "title": title,
                    "authors": authors,
                    "url": c.get("url") or "",
                },
            )
            if n is None:
                continue
            blocks.append(
                f"[{n}] {title} — {authors} "
                f"(paper_id: {paper_id}; filename: {filename})\n{snippet}"
            )
        if not blocks:
            return (
                f"No papers matched the query \"{query}\". "
                "Try a different search term or a broader query."
            )
        return "\n\n".join(blocks)

    if name == "read_paper":
        paper_id = str(args.get("paper_id") or "").strip()
        if not paper_id:
            return (
                "Error: read_paper requires a non-empty 'paper_id' argument. "
                "Use search_library first to find a Paper UUID."
            )

        try:
            paper_id = str(UUID(paper_id))
        except (AttributeError, TypeError, ValueError):
            return (
                f"Error: '{paper_id}' is not a valid Paper UUID. "
                "Use search_library first to find a Paper UUID."
            )

        # The dependency methods are separate calls, so bracket the text read
        # with current metadata. A revision switch invalidates that attempt;
        # retrying prevents text from one revision being labelled as another.
        for _attempt in range(3):
            try:
                before = deps.paper_meta(paper_id) or {}
                before_id = str(UUID(str(before.get("paper_id") or "")))
                before_revision = before.get("revision_number")
                before_filename = before.get("filename")
                if (
                    before_id != paper_id
                    or type(before_revision) is not int
                    or not isinstance(before_filename, str)
                    or not before_filename
                ):
                    return f"Error: current metadata is unavailable for Paper '{paper_id}'."

                url = deps.paper_url(paper_id)
                text = deps.full_text(paper_id)
                after = deps.paper_meta(paper_id) or {}
                after_id = str(UUID(str(after.get("paper_id") or "")))
                after_revision = after.get("revision_number")
                after_filename = after.get("filename")
            except (AttributeError, TypeError, ValueError):
                return f"Error: current metadata is unavailable for Paper '{paper_id}'."
            except Exception as exc:
                return (
                    f"Error: could not read Paper '{paper_id}' ({exc}). "
                    "Try search_library to find a valid Paper UUID."
                )

            if (
                after_id != paper_id
                or type(after_revision) is not int
                or not isinstance(after_filename, str)
                or not after_filename
            ):
                return f"Error: current metadata is unavailable for Paper '{paper_id}'."
            if before_revision != after_revision:
                continue

            if not isinstance(text, str):
                text = ""
            if not text.strip():
                return (
                    f"Error: no indexed text found for Paper \"{paper_id}\". "
                    "The file may not exist or has not been indexed. "
                    "Try search_library to find the correct Paper UUID."
                )

            title = after.get("title") or after_filename
            n = registry.register(
                paper_id,
                {
                    "paper_id": paper_id,
                    "revision_number": after_revision,
                    "filename": after_filename,
                    "title": title,
                    "authors": after.get("authors", ""),
                    "url": url or "",
                },
            )
            if n is None:
                return f"Error: current metadata is unavailable for Paper '{paper_id}'."

            if len(text) > PAPER_TEXT_CHAR_CAP:
                text = text[:PAPER_TEXT_CHAR_CAP] + "[truncated]"

            return f"Source [{n}]: {title}\n\n{text}"

        return (
            f"Error: Paper '{paper_id}' changed while it was being read. "
            "Try read_paper again."
        )

    if name == "web_search":
        query = str(args.get("query") or "").strip()
        if not query:
            return "Error: web_search requires a non-empty 'query' argument."
        web = getattr(deps, "web_search", None)
        if web is None:
            return "Error: web search is not available."
        try:
            results = web(query)
        except Exception as exc:
            return f"Error: web search failed ({exc}). Try a different query."
        if not results:
            return (f"No web results for \"{query}\". "
                    "Try a different query or rely on the library.")
        blocks = []
        for r in results:
            url = r.get("url") or ""
            if not url:
                continue
            title = r.get("title") or url
            content = r.get("content") or ""
            n = registry.register(url, {"title": title, "authors": "", "url": url},
                                  is_web=True)
            blocks.append(f"[{n}] (web) {title} ({url})\n{content}")
        if not blocks:
            return f"No usable web results for \"{query}\"."
        return "\n\n".join(blocks)

    if name == "fetch_url":
        url = str(args.get("url") or "").strip()
        if not url:
            return "Error: fetch_url requires a non-empty 'url' argument."
        fetch = getattr(deps, "fetch_url", None)
        if fetch is None:
            return "Error: web page fetching is not available."
        try:
            text = fetch(url)
        except Exception as exc:
            return f"Error: could not fetch '{url}' ({exc})."
        if not isinstance(text, str) or not text.strip():
            return (f"Error: could not read '{url}' (blocked, empty, or non-text). "
                    "Try a different page.")
        n = registry.register(url, {"title": url, "authors": "", "url": url}, is_web=True)
        return f"Source [{n}] (web page): {url}\n\n{text}"

    if name == "read_attachment":
        filename = str(args.get("filename") or "").strip()
        if not filename:
            return "Error: read_attachment requires a non-empty 'filename' argument."
        reader = getattr(deps, "read_attachment", None)
        if reader is None:
            return "Error: attachment reading is not available."
        try:
            text = reader(filename)
        except Exception as exc:
            return f"Error: could not read attachment '{filename}' ({exc})."
        if not isinstance(text, str) or not text.strip():
            return f"Error: no text found for attachment \"{filename}\"."
        n = registry.register(
            filename,
            {"filename": filename, "title": filename, "authors": "", "url": ""},
            is_attachment=True,
        )
        if len(text) > PAPER_TEXT_CHAR_CAP:
            text = text[:PAPER_TEXT_CHAR_CAP] + "[truncated]"
        return f"Attachment [{n}]: {filename}\n\n{text}"

    return f"Error: unknown tool \"{name}\". Available tools: search_library, read_paper."
