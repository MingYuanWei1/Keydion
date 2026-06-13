"""Contract: paper_manage redesign — template DOM/JS hooks, route fields,
and the bulk endpoint. No DB required (AST source lookup + bare-Jinja render)."""
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

from tests.support import source_of, all_sources

ROOT = Path(__file__).resolve().parents[1]

SAMPLE = [
    {"filename": "a.pdf", "title": "Alpha", "category": "Science",
     "keywords": "x,y", "abstract": "aa", "author_name": "Ann",
     "author_email": "", "author_school": "", "published_at": "2026-01-01",
     "journal": "IB EE", "paper_type": "Extended Essay"},
    {"filename": "b.pdf", "title": "Beta", "category": "Math",
     "keywords": "", "abstract": "", "author_name": "Bob",
     "author_email": "", "author_school": "", "published_at": "2026-02-01",
     "journal": "", "paper_type": "Community Project"},
    {"filename": "c.pdf", "title": "Gamma", "category": "",
     "keywords": "", "abstract": "", "author_name": "",
     "author_email": "", "author_school": "", "published_at": "",
     "journal": "", "paper_type": "Independent Research"},
]


def _render(papers, journals):
    env = Environment(loader=FileSystemLoader(ROOT / "templates"))
    return env.get_template("paper_manage.html").render(
        _=lambda value, **kw: value % kw if kw else value,
        url_for=lambda endpoint, **kw: "/" + endpoint,
        get_flashed_messages=lambda with_categories=False: [],
        request=SimpleNamespace(full_path="/dashboard/admin/papers", args={}),
        session={},
        current_locale="en",
        partial=True,
        user=SimpleNamespace(role="3"),
        papers=papers,
        journals=journals,
    )


class PaperManageTemplateContract(unittest.TestCase):
    def test_dom_ids_for_js(self):
        html = _render(SAMPLE, ["IB EE", "Math Journal"])
        for needle in (
            'id="paperTable"', 'id="paperBulkbar"',
            'id="journalOverlay"', 'id="paperSelectAll"',
            'id="paperSearch"', 'id="paperTbody"',
            # Additional IDs wired by inline JS — renaming any of these breaks the JS
            'id="paperTypeFilter"', 'id="paperDateFilter"', 'id="paperSort"',
            'id="paperToast"', 'id="paperCountLabel"', 'id="paperNoResults"',
            'id="paperBulkForms"', 'id="bulkJournalBtn"', 'id="bulkClear"',
        ):
            self.assertIn(needle, html)

    def test_type_chips_and_journal_column(self):
        html = _render(SAMPLE, [])
        self.assertIn("kp-pill--ee", html)
        self.assertIn("kp-pill--cp", html)
        self.assertIn("kp-pill--independent", html)
        self.assertIn("IB EE", html)  # journal cell for paper a

    def test_bulk_url_present_in_js(self):
        html = _render(SAMPLE, [])
        self.assertIn("/dashboard/admin/papers/bulk", html)


if __name__ == "__main__":
    unittest.main()
