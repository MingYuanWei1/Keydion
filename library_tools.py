"""Pure tool-calling core for Ask-the-Library agentic mode.

Public surface
--------------
PAPER_TEXT_CHAR_CAP : int
    Safety-net character cap on read_paper output (200 000).

TOOL_SCHEMAS : list[dict]
    Two OpenAI function-calling tool schemas to pass as ``tools=`` to
    ``client.chat.completions.create``:
      - ``search_library(query)`` — search the paper library.
      - ``read_paper(filename)`` — fetch the full text of one paper.

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
      Each dict has keys: ``filename``, ``title``, ``authors``, ``url``,
      ``snippet``.
* ``deps.full_text(filename: str) -> str``
      Reassembled full text of the paper; ``""`` if unavailable.
* ``deps.paper_meta(filename: str) -> dict``
      At least ``title`` and ``authors`` keys.
* ``deps.paper_url(filename: str) -> str | None``
      Canonical URL for the paper, or ``None``.

No Flask, SQLAlchemy, or app-level imports.  A later task wires concrete
DB-backed implementations and runs the tool loop inside a Flask route.
"""

from __future__ import annotations

import json

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
                "title, authors, filename, and a relevant text snippet. Use this "
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
                "Return the FULL text of one paper given its filename (as shown "
                "in a search_library result). Use this when you need to explain a "
                "paper in detail, quote from it, or when a snippet from "
                "search_library is insufficient to answer the question."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": (
                            "The filename of the paper to read, exactly as it "
                            "appeared in a search_library result (e.g. "
                            "\"smith2023.pdf\")."
                        ),
                    }
                },
                "required": ["filename"],
            },
        },
    },
]

# ---------------------------------------------------------------------------
# SourceRegistry
# ---------------------------------------------------------------------------


class SourceRegistry:
    """Assigns stable, 1-based [n] citation numbers to papers.

    Registration is idempotent per filename: calling ``register`` with the
    same filename twice always returns the same ``n``.  Each call can
    contribute additional metadata — any stored field that is currently
    empty/falsy is filled in from the new call's non-empty value, so the
    richest available info is retained.
    """

    def __init__(self) -> None:
        self._by_filename: dict[str, dict] = {}  # filename -> {"n", "title", "authors", "url"}
        self._counter: int = 0

    def register(self, filename: str, meta: dict) -> int | None:
        """Register a paper and return its citation number.

        Parameters
        ----------
        filename:
            The paper's filename.  Empty/falsy values are ignored and
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
            If ``filename`` is empty/falsy.
        """
        if not filename:
            return None

        if filename in self._by_filename:
            stored = self._by_filename[filename]
            # Backfill any empty stored fields from the new meta.
            for field in ("title", "authors", "url"):
                if not stored.get(field) and meta.get(field):
                    stored[field] = meta[field]
            return stored["n"]

        self._counter += 1
        self._by_filename[filename] = {
            "n": self._counter,
            "filename": filename,
            "title": meta.get("title") or "",
            "authors": meta.get("authors") or "",
            "url": meta.get("url") or "",
        }
        return self._counter

    def as_citations(self) -> list[dict]:
        """Return all registered sources as dicts, sorted by ascending ``n``.

        Each dict has keys: ``n``, ``filename``, ``title``, ``authors``,
        ``url``.  ``title`` falls back to the filename when empty.
        """
        rows = sorted(self._by_filename.values(), key=lambda r: r["n"])
        result = []
        for r in rows:
            result.append({
                "n": r["n"],
                "filename": r["filename"],
                "title": r["title"] or r["filename"],
                "authors": r["authors"],
                "url": r["url"],
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
            filename = c.get("filename") or ""
            if not filename:
                continue
            title = c.get("title") or filename
            authors = c.get("authors") or ""
            snippet = c.get("snippet") or ""
            n = registry.register(
                filename,
                {"title": title, "authors": authors, "url": c.get("url") or ""},
            )
            blocks.append(
                f"[{n}] {title} — {authors} (filename: {filename})\n{snippet}"
            )
        if not blocks:
            return (
                f"No papers matched the query \"{query}\". "
                "Try a different search term or a broader query."
            )
        return "\n\n".join(blocks)

    if name == "read_paper":
        filename = str(args.get("filename") or "").strip()
        if not filename:
            return (
                "Error: read_paper requires a non-empty 'filename' argument. "
                "Use search_library first to find a paper and get its filename."
            )

        try:
            text = deps.full_text(filename)
        except Exception as exc:
            return (
                f"Error: could not read '{filename}' ({exc}). "
                "Try search_library to find a valid filename."
            )

        if not isinstance(text, str):
            text = ""
        if not text.strip():
            return (
                f"Error: no indexed text found for \"{filename}\". "
                "The file may not exist or has not been indexed. "
                "Try search_library to find the correct filename."
            )

        try:
            meta = deps.paper_meta(filename) or {}
            url = deps.paper_url(filename)
        except Exception as exc:
            return (
                f"Error: could not read '{filename}' ({exc}). "
                "Try search_library to find a valid filename."
            )

        title = meta.get("title") or filename
        n = registry.register(
            filename,
            {"title": title, "authors": meta.get("authors", ""), "url": url or ""},
        )

        if len(text) > PAPER_TEXT_CHAR_CAP:
            text = text[:PAPER_TEXT_CHAR_CAP] + "[truncated]"

        return f"Source [{n}]: {title}\n\n{text}"

    return f"Error: unknown tool \"{name}\". Available tools: search_library, read_paper."
