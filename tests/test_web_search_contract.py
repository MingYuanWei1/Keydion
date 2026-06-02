# tests/test_web_search_contract.py
import os
import unittest
from unittest import mock

import web_search


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
        import inspect
        import app as app_module
        text = inspect.getsource(app_module.create_app)
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

    def test_allows_public_ip_and_extracts_text(self):
        html = b"<html><body><h1>Hi</h1><script>x()</script><p>World</p></body></html>"
        resp = mock.Mock()
        resp.is_redirect = False
        resp.status_code = 200
        resp.headers = {"Content-Type": "text/html; charset=utf-8"}
        resp.encoding = "utf-8"
        resp.iter_content = lambda chunk_size=8192: [html]
        resp.close = lambda: None
        with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo("93.184.216.34")):
            with mock.patch("web_search.requests.get", return_value=resp):
                out = web_search.fetch_url("https://example.com")
        self.assertIn("Hi", out)
        self.assertIn("World", out)
        self.assertNotIn("x()", out)   # script stripped

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
        resp = mock.Mock()
        resp.is_redirect = False
        resp.status_code = 200
        resp.headers = {"Content-Type": "application/pdf"}
        resp.close = lambda: None
        with mock.patch("web_search.socket.getaddrinfo", return_value=_addrinfo("93.184.216.34")):
            with mock.patch("web_search.requests.get", return_value=resp):
                self.assertEqual(web_search.fetch_url("https://example.com/x.pdf"), "")


if __name__ == "__main__":
    unittest.main()
