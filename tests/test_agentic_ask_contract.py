# tests/test_agentic_ask_contract.py
"""Contract: the agentic Ask prompt builder and the tool-status text helper.

The streaming tool loop itself — registry seeding, the 5-round tool loop, the
legacy single-shot fallback, the round-cap forced answer, the citation split —
is now behaviorally tested through the `run_ask_turn` seam in
tests/test_ask_turn.py, with a fake OpenAI client and no Flask app context or
MySQL. What remains here are the prompt and status-text units the loop
composes, plus the round-limit constant.
"""
import json
import os
import unittest

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import app as app_module
import library_tools


PAPER_A_ID = "00000000-0000-4000-8000-000000000921"
PAPER_B_ID = "00000000-0000-4000-8000-000000000922"


class ToolRoundLimit(unittest.TestCase):
    def test_max_tool_rounds_is_five(self):
        self.assertEqual(app_module.MAX_TOOL_ROUNDS, 5)


class AgenticPrompt(unittest.TestCase):
    def _candidates(self):
        return [
            {"n": 1, "title": "Photosynthesis in Algae", "authors": "Lee",
             "paper_id": PAPER_A_ID, "revision_number": 1,
             "filename": "lee2020.pdf", "snippet": "Algae convert light...",
             "is_attachment": False},
            {"n": 2, "title": "Coral Reef Decline", "authors": "Ng",
             "paper_id": PAPER_B_ID, "revision_number": 3,
             "filename": "ng2021.pdf", "snippet": "Reefs are bleaching...",
             "is_attachment": False},
        ]

    def test_lists_candidates_and_tools_english(self):
        p = app_module._build_agentic_ask_prompt("q", self._candidates(), [], "en")
        self.assertIn("[1]", p)
        self.assertIn("[2]", p)
        self.assertIn("Photosynthesis in Algae", p)
        self.assertIn("Coral Reef Decline", p)
        self.assertIn("read_paper", p)
        self.assertIn("search_library", p)
        self.assertIn("[n]", p)
        self.assertIn("English", p)
        self.assertNotIn("Answer in Chinese", p)

    def test_answers_in_chinese_for_zh(self):
        p = app_module._build_agentic_ask_prompt("q", self._candidates(), [], "zh")
        self.assertIn("Chinese", p)

    def test_empty_candidates_still_mentions_tools(self):
        p = app_module._build_agentic_ask_prompt("q", [], [], "en")
        self.assertTrue(p.strip())
        self.assertIn("read_paper", p)
        self.assertIn("search_library", p)

    def test_includes_web_sources(self):
        web = [{"n": 3, "title": "Live Result", "url": "http://x", "snippet": "snip"}]
        p = app_module._build_agentic_ask_prompt("q", self._candidates(), web, "en")
        self.assertIn("[3]", p)
        self.assertIn("Live Result", p)
        self.assertIn("(web)", p)


class ToolStatusText(unittest.TestCase):
    def test_search_status(self):
        reg = library_tools.SourceRegistry()
        out = app_module._tool_status_text("search_library", '{"query":"x"}', reg, None)
        self.assertIn("Searching", out)

    def test_read_status_uses_registered_title(self):
        reg = library_tools.SourceRegistry()
        reg.register(PAPER_A_ID, {
            "paper_id": PAPER_A_ID,
            "revision_number": 1,
            "filename": "x.pdf",
            "title": "A Fine Paper",
            "authors": "",
            "url": "",
        })
        out = app_module._tool_status_text(
            "read_paper", json.dumps({"paper_id": PAPER_A_ID}), reg, None)
        self.assertIn("A Fine Paper", out)

    def test_read_status_malformed_args_does_not_raise(self):
        reg = library_tools.SourceRegistry()
        out = app_module._tool_status_text("read_paper", "{not json", reg, None)
        self.assertTrue(isinstance(out, str) and out.strip())

    def test_unknown_tool_returns_nonempty(self):
        reg = library_tools.SourceRegistry()
        out = app_module._tool_status_text("mystery", "{}", reg, None)
        self.assertTrue(isinstance(out, str) and out.strip())


if __name__ == "__main__":
    unittest.main()
