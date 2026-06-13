import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class AskStatusJsContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = (ROOT / "static" / "js" / "ai.js").read_text(encoding="utf-8")
        cls.css = (ROOT / "static" / "css" / "ai.css").read_text(encoding="utf-8")

    def test_handle_dispatches_status_event(self):
        # handle() must branch on "status"
        self.assertRegex(self.js, r'=== ["\']status["\']')

    def test_handle_status_calls_showstatus(self):
        self.assertIn("showStatus", self.js)
        self.assertRegex(self.js, r'"status"[\s\S]{0,40}showStatus')

    def test_showstatus_is_defined(self):
        self.assertIn("function showStatus", self.js)

    def test_clearstatus_is_defined(self):
        self.assertIn("function clearStatus", self.js)

    def test_clearstatus_called_at_least_three_times(self):
        # Must be called in token, citations, and done branches (minimum 3).
        self.assertGreaterEqual(self.js.count("clearStatus(ai)"), 3)

    def test_clearstatus_in_token_branch(self):
        # The region between '"token"' and '"citations"' in handle() must contain clearStatus(ai).
        token_idx = self.js.index('"token"')
        citations_idx = self.js.index('"citations"')
        region = self.js[token_idx:citations_idx]
        self.assertIn("clearStatus(ai)", region,
                      "clearStatus(ai) should appear in the token branch (before citations)")

    def test_clearstatus_in_citations_branch(self):
        self.assertRegex(self.js, r'"citations"[\s\S]{0,60}clearStatus\(ai\)')

    def test_clearstatus_in_done_branch(self):
        self.assertRegex(self.js, r'"done"[\s\S]{0,60}clearStatus\(ai\)')

    def test_css_defines_kd_status(self):
        self.assertIn(".kd-status", self.css)
