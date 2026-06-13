"""Contract: the index route supplies recent journals and the landing renders a section."""
import unittest
from pathlib import Path

from tests.support import source_of

ROOT = Path(__file__).resolve().parents[1]


class LandingRecentJournalsTest(unittest.TestCase):
    def test_index_passes_recent_journals(self):
        src = source_of("index")
        self.assertIn("recent_journals", src)
        self.assertIn("get_recent_journals", src)

    def test_landing_has_recent_journals_section(self):
        html = (ROOT / "templates" / "landing.html").read_text(encoding="utf-8")
        self.assertIn("recent_journals", html)
        self.assertIn("url_for('journal_list_page')", html)


if __name__ == "__main__":
    unittest.main()
