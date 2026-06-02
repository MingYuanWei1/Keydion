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
import zlib
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


def _ip_is_blocked(ip) -> bool:
    """True if `ip` is non-public (private/loopback/link-local/reserved/etc.).

    IPv4-mapped IPv6 (``::ffff:a.b.c.d``) and the NAT64 well-known prefix
    (``64:ff9b::/96``) are unwrapped and re-checked against their embedded IPv4
    so an internal address smuggled inside a v6 wrapper cannot pass. This does
    not depend on the stdlib placing the mapped/NAT64 ranges in its private
    network tables, whose membership has varied across CPython patch levels
    (gh-113171)."""
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        ip = mapped
    elif getattr(ip, "version", None) == 6:
        # NAT64 well-known prefix 64:ff9b::/96 embeds an IPv4 in its low 32 bits.
        try:
            if ip in ipaddress.ip_network("64:ff9b::/96"):
                ip = ipaddress.ip_address(int(ip) & 0xFFFFFFFF)
        except ValueError:
            pass
    return bool(ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_reserved or ip.is_multicast or ip.is_unspecified)


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
        if _ip_is_blocked(ip):
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


def _read_capped_raw(resp, max_bytes: int) -> bytes | None:
    """Read at most ``max_bytes`` of the *undecoded* (still-compressed) body.

    Returns the raw bytes, or ``None`` if the body exceeds ``max_bytes``. We read
    one extra byte so we can tell "exactly at the cap" from "over the cap", and we
    iterate ``resp.raw.stream(..., decode_content=False)`` so urllib3 never
    inflates a Content-Encoding: gzip|deflate|br payload — counting decoded chunks
    would let a decompression bomb materialize gigabytes before we could stop."""
    raw = getattr(resp, "raw", None)
    chunks = []
    total = 0
    limit = max_bytes + 1
    stream = getattr(raw, "stream", None)
    if callable(stream):
        for chunk in stream(8192, decode_content=False):
            if not chunk:
                continue
            total += len(chunk)
            if total > max_bytes:
                return None
            chunks.append(chunk)
        return b"".join(chunks)
    # Fallback: read directly off the raw file object without decoding.
    read = getattr(raw, "read", None)
    if callable(read):
        data = read(limit)
        if not isinstance(data, (bytes, bytearray)):
            return None
        if len(data) > max_bytes:
            return None
        return bytes(data)
    return None


def _decode_body(raw: bytes, content_encoding: str, max_bytes: int) -> bytes | None:
    """Decompress ``raw`` per ``Content-Encoding`` with the output hard-capped.

    The compressed input is already bounded by ``max_bytes`` (see
    ``_read_capped_raw``); we additionally bound the *decompressed* output to
    ``max_bytes`` and bail (return ``None``) if a bomb tries to exceed it, so a
    high-ratio payload cannot exhaust memory. Unknown/unsupported encodings are
    rejected. Identity (no/empty encoding) passes through unchanged."""
    enc = (content_encoding or "").strip().lower()
    # Content-Encoding may be a comma list; we only support a single codec.
    if not enc or enc == "identity":
        return raw
    if enc == "gzip" or enc == "x-gzip":
        wbits = 16 + zlib.MAX_WBITS
    elif enc == "deflate":
        wbits = zlib.MAX_WBITS
    else:
        # br / compress / multiple codecs / anything else: refuse rather than
        # hand undecoded bytes to the parser or risk an unbounded decoder.
        return None
    try:
        decompressor = zlib.decompressobj(wbits)
        out = decompressor.decompress(raw, max_bytes + 1)
        if len(out) > max_bytes or decompressor.unconsumed_tail:
            return None
        out += decompressor.flush()
    except (zlib.error, OSError, ValueError):
        # Some servers send raw-deflate (no zlib header); retry with raw wbits.
        if enc == "deflate":
            try:
                decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
                out = decompressor.decompress(raw, max_bytes + 1)
                if len(out) > max_bytes or decompressor.unconsumed_tail:
                    return None
                out += decompressor.flush()
            except (zlib.error, OSError, ValueError):
                return None
        else:
            return None
    if len(out) > max_bytes:
        return None
    return out


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
            resp = requests.get(
                current, timeout=timeout, allow_redirects=False, stream=True,
                headers={"User-Agent": "KeydionBot/1.0",
                         # Ask the origin not to compress so the body we cap is
                         # the body we parse. A hostile server can ignore this,
                         # so the raw-byte cap below is the real guard.
                         "Accept-Encoding": "identity"})
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
            # Require an explicit, allowed Content-Type. A missing/blank header is
            # rejected rather than fed to the HTML parser as arbitrary bytes.
            if ctype not in _FETCH_CONTENT_TYPES:
                return ""
            # Reject up front when the server advertises an over-cap body, so we
            # never even start streaming a known-too-large response.
            clen = (resp.headers.get("Content-Length") or "").strip()
            if clen.isdigit() and int(clen) > max_bytes:
                return ""
            # Read the still-compressed body with a hard cap on the *raw* bytes,
            # then bound the decompressed output too — neither the compressed
            # input nor the inflated output may exceed max_bytes, so a gzip bomb
            # cannot exhaust memory.
            raw_body = _read_capped_raw(resp, max_bytes)
            if raw_body is None:
                return ""
            content_encoding = resp.headers.get("Content-Encoding") or ""
            body = _decode_body(raw_body, content_encoding, max_bytes)
            if body is None:
                return ""
            html = body.decode(getattr(resp, "encoding", None) or "utf-8", "ignore")
            return _extract_text(html, max_chars)
        finally:
            try:
                resp.close()
            except Exception:
                pass
    return ""
