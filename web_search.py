# web_search.py
"""Pluggable web search for Ask-the-Library (Phase 5).

Default provider is Tavily (purpose-built for LLM grounding). Disabled entirely
when WEB_SEARCH_API_KEY is unset, so the feature ships off and the UI toggle is
hidden until an operator configures it.
"""

from __future__ import annotations

import ipaddress
import http.client
import os
import socket
import ssl
import zlib
from urllib.parse import urljoin, urlsplit, urlunsplit

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
            results = []
            for result in _tavily(query, max_results):
                normalized = normalize_fetch_url(result.get("url") or "")
                if normalized is None:
                    continue
                results.append({
                    "title": result.get("title") or normalized,
                    "url": normalized,
                    "content": result.get("content") or "",
                })
            return results
    except Exception:
        return []
    return []


_FETCH_SCHEMES = ("http", "https")
_FETCH_CONTENT_TYPES = ("text/html", "text/plain", "application/xhtml+xml")


# RFC 6598 carrier-grade NAT (100.64.0.0/10) and 6to4 relay anycast
# (192.88.99.0/24): ipaddress does not flag these as private/reserved, but they
# commonly front internal infrastructure — block them explicitly.
_EXTRA_BLOCKED_V4 = (
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("192.88.99.0/24"),
)


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
    if getattr(ip, "version", None) == 4:
        for _net in _EXTRA_BLOCKED_V4:
            if ip in _net:
                return True
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


def normalize_fetch_url(url: str, *, max_length: int = 2048) -> str | None:
    """Return one canonical fetchable HTTP(S) URL, without resolving it.

    Model-emitted URLs are untrusted.  Only default web ports are supported and
    URL credentials, fragments, whitespace/control characters, malformed IDNA,
    and oversized values are rejected before an allowlist comparison or DNS
    lookup occurs.
    """
    if not isinstance(url, str):
        return None
    candidate = url.strip()
    if not candidate or len(candidate) > max_length:
        return None
    if any(ord(char) <= 0x20 or ord(char) == 0x7F for char in candidate):
        return None
    try:
        parts = urlsplit(candidate)
        scheme = (parts.scheme or "").lower()
        if scheme not in _FETCH_SCHEMES or not parts.hostname:
            return None
        if parts.username is not None or parts.password is not None:
            return None
        port = parts.port
        expected_port = 443 if scheme == "https" else 80
        if port not in (None, expected_port):
            return None
        host = parts.hostname.encode("idna").decode("ascii").lower().rstrip(".")
        if not host:
            return None
        try:
            parsed_ip = ipaddress.ip_address(host)
        except ValueError:
            parsed_ip = None
        host_for_url = f"[{host}]" if parsed_ip is not None and parsed_ip.version == 6 else host
        path = parts.path or "/"
        return urlunsplit((scheme, host_for_url, path, parts.query, ""))
    except (UnicodeError, ValueError):
        return None


def _resolve_public_ips(host: str, port: int) -> tuple[str, ...]:
    """Resolve once and return vetted public addresses for a pinned connect."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except (socket.gaierror, UnicodeError, OSError, ValueError):
        return ()
    addresses = []
    for info in infos:
        try:
            address = str(ipaddress.ip_address(info[4][0]))
            parsed = ipaddress.ip_address(address)
        except ValueError:
            return ()
        # Fail closed if any DNS answer is non-public; do not choose a public
        # answer from a mixed public/private result set.
        if _ip_is_blocked(parsed):
            return ()
        if address not in addresses:
            addresses.append(address)
    return tuple(addresses)


def url_targets_public_host(url: str) -> bool:
    """True when url is http(s) on a default port whose host resolves only to
    public, routable addresses. Used by admin actions that contact an
    operator-supplied LLM endpoint (fetch models / probe) so they cannot be
    aimed at internal networks."""
    normalized = normalize_fetch_url(url)
    if normalized is None:
        return False
    parts = urlsplit(normalized)
    return bool(_resolve_public_ips(parts.hostname or "", parts.port or (443 if parts.scheme == "https" else 80)))


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection to a numeric IP with TLS identity bound to the URL host."""

    def __init__(self, address: str, port: int, server_hostname: str, timeout: int):
        super().__init__(address, port=port, timeout=timeout, context=ssl.create_default_context())
        self._keydion_server_hostname = server_hostname

    def connect(self) -> None:
        http.client.HTTPConnection.connect(self)
        self.sock = self._context.wrap_socket(
            self.sock,
            server_hostname=self._keydion_server_hostname,
        )


def _open_pinned_connection(parts, address: str, timeout: int):
    port = parts.port or (443 if parts.scheme == "https" else 80)
    if parts.scheme == "https":
        connection = _PinnedHTTPSConnection(
            address,
            port,
            parts.hostname or "",
            timeout,
        )
    else:
        connection = http.client.HTTPConnection(address, port=port, timeout=timeout)
    connection.connect()
    peer = connection.sock.getpeername()[0] if connection.sock is not None else ""
    try:
        peer_ip = ipaddress.ip_address(peer)
        expected_ip = ipaddress.ip_address(address)
    except ValueError:
        connection.close()
        raise OSError("outbound peer address is invalid")
    if peer_ip != expected_ip or _ip_is_blocked(peer_ip):
        connection.close()
        raise OSError("outbound peer does not match the vetted address")
    return connection


def _request_pinned(url: str, *, timeout: int):
    """Open one response after resolving once, then connecting to that exact IP."""
    normalized = normalize_fetch_url(url)
    if normalized is None:
        return None, None, None
    parts = urlsplit(normalized)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    addresses = _resolve_public_ips(parts.hostname or "", port)
    if not addresses:
        return None, None, None
    target = parts.path or "/"
    if parts.query:
        target += "?" + parts.query
    host_header = parts.hostname or ""
    last_error = None
    for address in addresses:
        connection = None
        try:
            connection = _open_pinned_connection(parts, address, timeout)
            connection.putrequest("GET", target, skip_host=True, skip_accept_encoding=True)
            connection.putheader("Host", host_header)
            connection.putheader("User-Agent", "KeydionBot/1.0")
            connection.putheader("Accept-Encoding", "identity")
            connection.endheaders()
            return connection, connection.getresponse(), normalized
        except (OSError, ssl.SSLError, http.client.HTTPException) as exc:
            last_error = exc
            if connection is not None:
                connection.close()
    del last_error
    return None, None, None


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

    SSRF-guarded: http/https on default ports only; blocks private/loopback/
    link-local/reserved/metadata IPs; pins the socket to the address vetted by
    DNS; verifies TLS for the original hostname; revalidates every redirect;
    and caps body size, time, and content type."""
    current = normalize_fetch_url(url)
    if current is None:
        return ""
    for _ in range(max_redirects + 1):
        connection, resp, opened_url = _request_pinned(current, timeout=timeout)
        if connection is None or resp is None or opened_url is None:
            return ""
        try:
            if resp.status in (301, 302, 303, 307, 308):
                loc = resp.getheader("Location") or ""
                if not loc:
                    return ""
                nxt = normalize_fetch_url(urljoin(opened_url, loc))
                if nxt is None:
                    return ""
                current = nxt
                continue
            if resp.status != 200:
                return ""
            ctype = (resp.getheader("Content-Type") or "").split(";")[0].strip().lower()
            # Require an explicit, allowed Content-Type. A missing/blank header is
            # rejected rather than fed to the HTML parser as arbitrary bytes.
            if ctype not in _FETCH_CONTENT_TYPES:
                return ""
            # Reject up front when the server advertises an over-cap body, so we
            # never even start streaming a known-too-large response.
            clen = (resp.getheader("Content-Length") or "").strip()
            if clen.isdigit() and int(clen) > max_bytes:
                return ""
            raw_body = resp.read(max_bytes + 1)
            if not isinstance(raw_body, bytes) or len(raw_body) > max_bytes:
                return ""
            content_encoding = resp.getheader("Content-Encoding") or ""
            body = _decode_body(raw_body, content_encoding, max_bytes)
            if body is None:
                return ""
            html = body.decode("utf-8", "ignore")
            return _extract_text(html, max_chars)
        finally:
            connection.close()
    return ""
