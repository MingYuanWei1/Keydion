# web_search.py
"""Pluggable web search for Ask-the-Library (Phase 5).

Default provider is Tavily (purpose-built for LLM grounding). Disabled entirely
when WEB_SEARCH_API_KEY is unset, so the feature ships off and the UI toggle is
hidden until an operator configures it.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from urllib.parse import urljoin, urlparse

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
    try:
        if provider == "tavily":
            return _tavily(query, max_results)
    except Exception:
        return []
    return []


_FETCH_SCHEMES = ("http", "https")
_FETCH_CONTENT_TYPES = ("text/html", "text/plain", "application/xhtml+xml")


def _host_is_safe(host: str) -> bool:
    """True only if every IP `host` resolves to is a public, routable address."""
    if not host:
        return False
    try:
        infos = socket.getaddrinfo(host, None)
    except (socket.gaierror, UnicodeError, OSError, ValueError):
        return False
    if not infos:
        return False
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            return False
        if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved
                or ip.is_multicast or ip.is_unspecified):
            return False
    return True


def _url_is_safe(url: str) -> bool:
    try:
        parts = urlparse(url)
    except ValueError:
        return False
    if (parts.scheme or "").lower() not in _FETCH_SCHEMES:
        return False
    return _host_is_safe(parts.hostname or "")


def _extract_text(html: str, max_chars: int) -> str:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = " ".join(soup.get_text(" ", strip=True).split())
    if len(text) > max_chars:
        text = text[:max_chars] + "[truncated]"
    return text


def fetch_url(url: str, *, max_bytes: int = 3_000_000, timeout: int = 8,
              max_redirects: int = 3, max_chars: int = 10_000) -> str:
    """Fetch a public web page and return extracted text; "" on any violation.

    SSRF-guarded: http/https only; blocks URLs resolving to private/loopback/
    link-local/reserved/metadata IPs; follows redirects manually, re-validating
    each hop; caps body size, time, and content-type. NOTE (v1): resolved IPs are
    validated but the socket is not pinned to them, so DNS-rebinding is not fully
    closed — see the security review."""
    current = (url or "").strip()
    if not _url_is_safe(current):
        return ""
    for _ in range(max_redirects + 1):
        try:
            resp = requests.get(current, timeout=timeout, allow_redirects=False,
                                stream=True, headers={"User-Agent": "KeydionBot/1.0"})
        except Exception:
            return ""
        try:
            if getattr(resp, "is_redirect", False) or resp.status_code in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location") or ""
                if not loc:
                    return ""
                nxt = urljoin(current, loc)
                if not _url_is_safe(nxt):
                    return ""
                current = nxt
                continue
            if resp.status_code != 200:
                return ""
            ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            if ctype and ctype not in _FETCH_CONTENT_TYPES:
                return ""
            total = 0
            chunks = []
            for chunk in resp.iter_content(chunk_size=8192):
                if not chunk:
                    break
                total += len(chunk)
                chunks.append(chunk)
                if total >= max_bytes:
                    break
            raw = b"".join(chunks)
            html = raw.decode(getattr(resp, "encoding", None) or "utf-8", "ignore")
            return _extract_text(html, max_chars)
        finally:
            try:
                resp.close()
            except Exception:
                pass
    return ""
