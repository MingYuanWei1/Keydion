# web_search.py
"""Pluggable web search for Ask-the-Library (Phase 5).

Default provider is Tavily (purpose-built for LLM grounding). Disabled entirely
when WEB_SEARCH_API_KEY is unset, so the feature ships off and the UI toggle is
hidden until an operator configures it.
"""

from __future__ import annotations

import os

import requests


def web_search_enabled() -> bool:
    return bool(os.environ.get("WEB_SEARCH_API_KEY"))


def _tavily(query: str, max_results: int) -> list:
    api_key = os.environ.get("WEB_SEARCH_API_KEY") or ""
    resp = requests.post(
        "https://api.tavily.com/search",
        json={"api_key": api_key, "query": query,
              "max_results": max_results, "search_depth": "basic"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json() or {}
    out = []
    for r in (data.get("results") or [])[:max_results]:
        out.append({"title": r.get("title") or r.get("url") or "",
                    "url": r.get("url") or "",
                    "content": (r.get("content") or "")[:1200]})
    return out


def web_search(query: str, max_results: int = 5) -> list:
    """Return [{title, url, content}]; [] when disabled or on any error."""
    query = (query or "").strip()
    if not query or not web_search_enabled():
        return []
    provider = (os.environ.get("WEB_SEARCH_PROVIDER") or "tavily").strip().lower()
    if provider == "tavily":
        return _tavily(query, max_results)
    return []
