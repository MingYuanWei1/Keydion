"""Contract: paper_manage redesign — template DOM/JS hooks, route fields,
and the bulk endpoint. No DB required (AST source lookup + bare-Jinja render)."""
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

from tests.support import source_of, all_sources

ROOT = Path(__file__).resolve().parents[1]

SAMPLE = [
    {"paper_id": "00000000-0000-4000-8000-000000000911",
     "filename": "a.pdf", "title": "Alpha", "category": "Science",
     "keywords": "x,y", "abstract": "aa", "author_name": "Ann",
     "author_email": "", "author_school": "", "published_at": "2026-01-01",
     "journal": "IB EE", "paper_type": "Extended Essay"},
    {"paper_id": "00000000-0000-4000-8000-000000000912",
     "filename": "b.pdf", "title": "Beta", "category": "Math",
     "keywords": "", "abstract": "", "author_name": "Bob",
     "author_email": "", "author_school": "", "published_at": "2026-02-01",
     "journal": "", "paper_type": "Community Project"},
    {"paper_id": "00000000-0000-4000-8000-000000000913",
     "filename": "c.pdf", "title": "Gamma", "category": "",
     "keywords": "", "abstract": "", "author_name": "",
     "author_email": "", "author_school": "", "published_at": "",
     "journal": "", "paper_type": "Independent Research"},
]


def _render(papers, journals):
    env = Environment(loader=FileSystemLoader(ROOT / "templates"))
    env.globals["csrf_token"] = lambda: ""
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


class PaperManageRouteContract(unittest.TestCase):
    def test_route_derives_paper_type_and_passes_journals(self):
        src = source_of("paper_manage")
        template = (ROOT / "templates" / "paper_manage.html").read_text(
            encoding="utf-8"
        )

        self.assertIn("gather_paper_records(_paper_library())", src)
        self.assertIn('"paper_type"', src)
        self.assertIn("Community Project", src)
        self.assertIn("Extended Essay", src)
        self.assertIn("journals=get_journal_names()", src)
        self.assertIn("p.journal", template)
        self.assertIn("paper_id=p.paper_id", template)


class PaperBulkEndpointContract(unittest.TestCase):
    def test_bulk_route_exists_with_guard_and_ops(self):
        src = source_of("papers_bulk_action")
        self.assertIn("require_login(level=3)", src)
        self.assertIn('op == "delete"', src)
        self.assertIn('op == "set_journal"', src)
        self.assertIn("rag_index.purge", src)
        self.assertIn("upsert_paper_metadata", src)
        self.assertIn("remove_paper_metadata", src)
        self.assertIn("resolve_contained(", src)

    def test_bulk_route_registered_at_expected_url(self):
        self.assertIn('"/dashboard/admin/papers/bulk"', all_sources())


if __name__ == "__main__":
    unittest.main()
