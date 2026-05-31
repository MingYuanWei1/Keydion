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


if __name__ == "__main__":
    unittest.main()
