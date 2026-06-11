# tests/test_web_search_contract.py
import gzip
import os
import sys
import unittest
from unittest import mock

import web_search

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import support


class WebSearchModule(unittest.TestCase):
    def test_disabled_when_no_key(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("WEB_SEARCH_API_KEY", None)
            self.assertFalse(web_search.web_search_enabled())
            self.assertEqual(web_search.web_search("anything"), [])

    def test_enabled_with_key(self):
        with mock.patch.dict(os.environ, {"WEB_SEARCH_API_KEY": "k"}, clear=False):
            self.assertTrue(web_search.web_search_enabled())

    def test_tavily_call_shape(self):
        fake = mock.Mock()
        fake.json.return_value = {"results": [
            {"title": "T1", "url": "https://a", "content": "body one"},
            {"title": "T2", "url": "https://b", "content": "body two"},
        ]}
        fake.raise_for_status.return_value = None
        with mock.patch.dict(os.environ, {"WEB_SEARCH_API_KEY": "k",
                                          "WEB_SEARCH_PROVIDER": "tavily"}, clear=False):
            with mock.patch("web_search.requests.post", return_value=fake) as post:
                out = web_search.web_search("hello", max_results=2)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["title"], "T1")
        self.assertEqual(out[0]["url"], "https://a")
        self.assertIn("body one", out[0]["content"])
        self.assertIn("api.tavily.com", post.call_args[0][0])

    def test_provider_exception_returns_empty(self):
        with mock.patch.dict(os.environ, {"WEB_SEARCH_API_KEY": "k",
                                          "WEB_SEARCH_PROVIDER": "tavily"}, clear=False):
            with mock.patch("web_search.requests.post", side_effect=Exception("boom")):
                out = web_search.web_search("hello")
        self.assertEqual(out, [])


class ApiAskReadsWebFlag(unittest.TestCase):
    def test_api_ask_reads_web_flag(self):
        text = support.source_of("api_ask")
        self.assertIn('data.get("web")', text)
        self.assertIn("web_search.web_search(", text)

    def test_prompt_accepts_web_results(self):
        import app as app_module
        import inspect
        sig = inspect.signature(app_module._build_ask_prompt)
        self.assertIn("web_results", sig.parameters)


import socket as _socket


def _addrinfo(ip):
    fam = _socket.AF_INET6 if ":" in ip else _socket.AF_INET
    return [(fam, _socket.SOCK_STREAM, 6, "", (ip, 0))]


class _FakeRaw:
    """Stand-in for requests' resp.raw (a urllib3 HTTPResponse).

    Yields the *undecoded* chunks it was given via stream(decode_content=False);
    asserts the caller never asks it to decode (that is exactly the
    decompression-bomb bypass the real fetch_url must avoid)."""

    def __init__(self, chunks):
        self._chunks = list(chunks)

    def stream(self, amt=8192, decode_content=None):
        assert decode_content is False, (
            "fetch_url must read the raw body with decode_content=False so a "
            "Content-Encoding bomb is never inflated by urllib3")
        for c in self._chunks:
            yield c


def _resp(*, status=200, headers=None, chunks=(b"",), encoding="utf-8"):
    resp = mock.Mock()
    resp.is_redirect = status in (301, 302, 303, 307, 308)
    resp.status_code = status
    resp.headers = headers or {}
    resp.encoding = encoding
    resp.raw = _FakeRaw(chunks)
    # If anything calls iter_content, blow up: that path decodes and is the bug.
    resp.iter_content = mock.Mock(
        side_effect=AssertionError("fetch_url must not use decoding iter_content"))
    resp.close = lambda: None
    return resp


class FetchUrlSafety(unittest.TestCase):
    def test_rejects_non_http_scheme(self):
        self.assertEqual(web_search.fetch_url("file:///etc/passwd"), "")
        self.assertEqual(web_search.fetch_url("ftp://x/y"), "")

    def test_rejects_private_loopback_linklocal_metadata(self):
        for ip in ("10.0.0.5", "192.168.1.1", "172.16.0.9", "127.0.0.1",
                   "169.254.169.254", "::1"):
            with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo(ip)):
                with mock.patch("web_search.requests.get") as g:
                    self.assertEqual(web_search.fetch_url("http://evil.example"), "")
                    g.assert_not_called()

    def test_rejects_cgnat_and_6to4_relay(self):
        # RFC 6598 CGNAT + 6to4 relay anycast — not flagged private by ipaddress.
        for ip in ("100.64.0.1", "192.88.99.1"):
            with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo(ip)):
                with mock.patch("web_search.requests.get") as g:
                    self.assertEqual(web_search.fetch_url("http://evil.example"), "")
                    g.assert_not_called()

    def test_allows_public_ip_and_extracts_text(self):
        html = b"<html><body><h1>Hi</h1><script>x()</script><p>World</p></body></html>"
        resp = _resp(headers={"Content-Type": "text/html; charset=utf-8"},
                     chunks=[html])
        with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo("93.184.216.34")):
            with mock.patch("web_search.requests.get", return_value=resp) as g:
                out = web_search.fetch_url("https://example.com")
        self.assertIn("Hi", out)
        self.assertIn("World", out)
        self.assertNotIn("x()", out)   # script stripped
        # Defense-in-depth: we asked the origin not to compress.
        sent = g.call_args.kwargs["headers"]
        self.assertEqual(sent.get("Accept-Encoding"), "identity")

    def test_blocks_internal_redirect(self):
        redirect = mock.Mock()
        redirect.is_redirect = True
        redirect.status_code = 302
        redirect.headers = {"Location": "http://169.254.169.254/latest/meta-data/"}
        redirect.close = lambda: None

        def fake_get(url, **kw):
            return redirect

        # First host public, redirect target internal -> blocked -> "".
        def getaddr(host, *a, **k):
            return _addrinfo("93.184.216.34") if host == "example.com" else _addrinfo("169.254.169.254")

        with mock.patch("web_search.socket.getaddrinfo", side_effect=getaddr):
            with mock.patch("web_search.requests.get", side_effect=fake_get):
                self.assertEqual(web_search.fetch_url("https://example.com"), "")

    def test_rejects_disallowed_content_type(self):
        resp = _resp(headers={"Content-Type": "application/pdf"},
                     chunks=[b"%PDF-1.4 ..."])
        with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo("93.184.216.34")):
            with mock.patch("web_search.requests.get", return_value=resp):
                self.assertEqual(web_search.fetch_url("https://example.com/x.pdf"), "")

    def test_rejects_missing_content_type(self):
        # Finding (minor): a 200 with NO Content-Type must NOT be fed to the
        # parser as arbitrary bytes — missing is treated as disallowed.
        resp = _resp(headers={}, chunks=[b"<html><body>secret</body></html>"])
        with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo("93.184.216.34")):
            with mock.patch("web_search.requests.get", return_value=resp):
                self.assertEqual(web_search.fetch_url("https://example.com"), "")


class FetchUrlDecompressionBomb(unittest.TestCase):
    """Finding (critical): max_bytes must bound the *raw/compressed* stream and
    the *decompressed* output, not the decoded iter_content chunk count."""

    def test_gzip_bomb_capped_on_raw_stream(self):
        # ~1MB compressed -> ~64MB decompressed. With a 1MB cap, the raw read
        # must abort and fetch_url must return "" — never inflating 64MB.
        bomb = gzip.compress(b"\0" * (64 * 1024 * 1024))
        self.assertLess(len(bomb), 2_000_000)
        resp = _resp(
            headers={"Content-Type": "text/html", "Content-Encoding": "gzip"},
            chunks=[bomb[i:i + 8192] for i in range(0, len(bomb), 8192)])
        with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo("93.184.216.34")):
            with mock.patch("web_search.requests.get", return_value=resp):
                out = web_search.fetch_url("https://example.com", max_bytes=1_000_000)
        self.assertEqual(out, "")

    def test_decode_body_output_is_capped(self):
        # A small compressed body that fits under the raw cap but inflates past
        # max_bytes must be rejected by the decompressed-output bound.
        raw = gzip.compress(b"A" * 5_000_000)
        self.assertLess(len(raw), 1_000_000)   # passes the raw cap
        self.assertIsNone(web_search._decode_body(raw, "gzip", max_bytes=1_000_000))

    def test_decode_body_passthrough_identity(self):
        self.assertEqual(web_search._decode_body(b"hello", "", 1_000_000), b"hello")
        self.assertEqual(web_search._decode_body(b"hi", "identity", 1_000_000), b"hi")

    def test_decode_body_rejects_unknown_encoding(self):
        # br (and any non-gzip/deflate codec) is refused rather than risk an
        # unbounded or bytes-as-html decode.
        self.assertIsNone(web_search._decode_body(b"\x1b\x07\x00", "br", 1_000_000))

    def test_decode_body_roundtrips_small_gzip(self):
        body = b"<html><body>tiny ok</body></html>"
        self.assertEqual(
            web_search._decode_body(gzip.compress(body), "gzip", 1_000_000), body)

    def test_content_length_over_cap_rejected_before_read(self):
        # Advertised over-cap body is rejected up front; the raw stream is never
        # touched (would raise from the iter_content guard if it were).
        resp = _resp(
            headers={"Content-Type": "text/html",
                     "Content-Length": str(10 * 1024 * 1024)},
            chunks=[b"<html>x</html>"])
        with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo("93.184.216.34")):
            with mock.patch("web_search.requests.get", return_value=resp):
                self.assertEqual(
                    web_search.fetch_url("https://example.com", max_bytes=1_000_000), "")

    def test_uncompressed_over_cap_raw_rejected(self):
        # Even with no Content-Encoding, a body exceeding max_bytes is dropped.
        big = b"<html><body>" + b"Z" * 2_000_000 + b"</body></html>"
        resp = _resp(headers={"Content-Type": "text/html"},
                     chunks=[big[i:i + 8192] for i in range(0, len(big), 8192)])
        with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo("93.184.216.34")):
            with mock.patch("web_search.requests.get", return_value=resp):
                self.assertEqual(
                    web_search.fetch_url("https://example.com", max_bytes=1_000_000), "")


class HostIsSafeMappedV6(unittest.TestCase):
    """Finding (important): IPv4-mapped IPv6 and NAT64 must be unwrapped and
    re-checked explicitly, not left to stdlib private-range membership."""

    def test_ipv4_mapped_loopback_and_metadata_blocked(self):
        for ip in ("::ffff:127.0.0.1", "::ffff:169.254.169.254",
                   "::ffff:10.0.0.1", "::ffff:192.168.0.5"):
            with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo(ip)):
                with mock.patch("web_search.requests.get") as g:
                    self.assertEqual(web_search.fetch_url("http://evil.example"), "")
                    g.assert_not_called()

    def test_nat64_embedded_internal_blocked(self):
        # 64:ff9b::a9fe:a9fe == NAT64-wrapped 169.254.169.254 (metadata).
        for ip in ("64:ff9b::a9fe:a9fe", "64:ff9b::7f00:1", "64:ff9b::a00:1"):
            with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo(ip)):
                with mock.patch("web_search.requests.get") as g:
                    self.assertEqual(web_search.fetch_url("http://evil.example"), "")
                    g.assert_not_called()

    def test_ip_is_blocked_unwraps_without_stdlib_dependence(self):
        # Directly exercise the unwrap so the guard does not rely on the stdlib
        # placing ::ffff:0:0/96 in its private tables (varies by patch level).
        import ipaddress as _ip
        self.assertTrue(web_search._ip_is_blocked(_ip.ip_address("::ffff:127.0.0.1")))
        self.assertTrue(web_search._ip_is_blocked(_ip.ip_address("::ffff:169.254.169.254")))
        # A mapped *public* address is still allowed (unwrap doesn't over-block).
        self.assertFalse(web_search._ip_is_blocked(_ip.ip_address("::ffff:8.8.8.8")))

    def test_mapped_public_address_allowed(self):
        # ::ffff:93.184.216.34 should pass _host_is_safe.
        with mock.patch("web_search.socket.getaddrinfo",
                        return_value=_addrinfo("::ffff:93.184.216.34")):
            self.assertTrue(web_search._host_is_safe("example.com"))


if __name__ == "__main__":
    unittest.main()
