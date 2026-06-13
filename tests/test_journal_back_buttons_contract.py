"""Contract: journal detail links back to the list; preview links back to its journal."""
import unittest
from pathlib import Path

from tests.support import source_of

ROOT = Path(__file__).resolve().parents[1]


class JournalBackButtonsTest(unittest.TestCase):
    def test_journal_detail_has_back_to_list(self):
        html = (ROOT / "templates" / "journal_detail.html").read_text(encoding="utf-8")
        self.assertIn("url_for('journal_list_page')", html)

    def test_preview_route_passes_slug_map(self):
        self.assertIn("journal_slug_map", source_of("preview_paper"))

    def test_preview_template_links_back_to_journal(self):
        html = (ROOT / "templates" / "preview.html").read_text(encoding="utf-8")
        self.assertIn("journal_slug_map", html)
        self.assertIn("url_for('journal_detail'", html)


if __name__ == "__main__":
    unittest.main()
