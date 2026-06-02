# tests/test_agent_tools_wiring.py
import os
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module


class DepsExposesWebSearch(unittest.TestCase):
    def test_build_library_deps_has_web_search(self):
        deps = app_module._build_library_deps()
        self.assertTrue(hasattr(deps, "web_search"))

    def test_web_search_dep_delegates_to_web_search_module(self):
        # Build deps INSIDE the patch so deps.web_search captures the mock.
        with mock.patch.object(app_module.web_search, "web_search",
                               return_value=[{"title": "T", "url": "u", "content": "c"}]) as m:
            deps = app_module._build_library_deps()
            out = deps.web_search("hello")
        m.assert_called_once()
        self.assertEqual(out[0]["title"], "T")


class AgenticPromptMentionsWeb(unittest.TestCase):
    def test_mentions_web_search_when_included(self):
        p = app_module._build_agentic_ask_prompt("q", [], [], "en", include_web=True)
        self.assertIn("web_search", p)

    def test_omits_web_search_when_not_included(self):
        p = app_module._build_agentic_ask_prompt("q", [], [], "en", include_web=False)
        self.assertNotIn("web_search", p)


if __name__ == "__main__":
    unittest.main()
