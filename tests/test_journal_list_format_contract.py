"""Contract: journals render as a list (name + count + intro), no cover images."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(name):
    return (ROOT / "templates" / name).read_text(encoding="utf-8")


class JournalListFormatTest(unittest.TestCase):
    def test_journal_list_is_a_list_not_cards(self):
        html = _read("journal_list.html")
        self.assertIn("journal-list-item", html)
        self.assertIn("url_for('journal_detail', slug=j.slug)", html)
        self.assertIn("paper_count", html)
        self.assertNotIn("journal-grid", html)
        self.assertNotIn("journal_covers", html)

    def test_detail_page_has_no_cover(self):
        self.assertNotIn("journal_covers", _read("journal_detail.html"))

    def test_manage_page_has_no_cover(self):
        self.assertNotIn("journal_covers", _read("journal_manage.html"))


if __name__ == "__main__":
    unittest.main()
