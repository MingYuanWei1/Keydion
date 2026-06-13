"""Contract: both headers link to the journals list for everyone (no role gate)."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class JournalHeaderLinkTest(unittest.TestCase):
    def test_base_nav_has_journals_link(self):
        html = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
        self.assertIn("url_for('journal_list_page')", html)

    def test_landing_nav_has_journals_link(self):
        html = (ROOT / "templates" / "landing.html").read_text(encoding="utf-8")
        self.assertIn("url_for('journal_list_page')", html)


if __name__ == "__main__":
    unittest.main()
