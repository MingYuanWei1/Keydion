# tests/test_agent_tools_wiring.py
import os
import sys
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import support


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


class LoopWiring(unittest.TestCase):
    def setUp(self):
        self.src = support.source_of("api_ai")

    def test_loop_gates_web_on_enabled(self):
        # The route gates web on explicit request + provider configuration
        # before constructing the turn. (Loop internals — build_tool_schemas,
        # is_web registration, the citation split, and the per-turn caps —
        # moved into services/ask_turn.py and are now behaviorally covered in
        # tests/test_ask_turn.py, not pinned by source greps.)
        self.assertIn("web_search_enabled()", self.src)


class AttachmentDeps(unittest.TestCase):
    def test_deps_has_read_attachment_when_conv_given(self):
        deps = app_module._build_library_deps(conv_db_id=123)
        self.assertTrue(hasattr(deps, "read_attachment"))

    def test_read_attachment_returns_string(self):
        deps = app_module._build_library_deps(conv_db_id=None)
        # With no conversation, read_attachment must return "" (not raise).
        self.assertEqual(deps.read_attachment("anything.pdf"), "")

    def test_attachment_filenames_none_conv_is_empty(self):
        self.assertEqual(app_module._attachment_filenames(None), [])


class AttachmentLoopWiring(unittest.TestCase):
    def setUp(self):
        self.src = support.source_of("api_ai")

    def test_loop_builds_deps_with_conv(self):
        self.assertIn("_build_library_deps(", self.src)

    def test_prompt_lists_attachment_names(self):
        p = app_module._build_agentic_ask_prompt(
            "q", [], [], "en", include_web=False,
            include_attachment=True, attachment_names=["notes.pdf"])
        self.assertIn("read_attachment", p)
        self.assertIn("notes.pdf", p)


if __name__ == "__main__":
    unittest.main()
