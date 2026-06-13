"""Contract: the search refine panel renders a single-select journal dropdown
(name="journal", like Paper Type) reflecting the current selection — not checkboxes."""
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]


class JournalSearchFilterTest(unittest.TestCase):
    def render_search(self, journals, journal_filter):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"))
        return env.get_template("search.html").render(
            _=lambda value, **kw: value % kw if kw else value,
            ngettext=lambda s, p, n, **kw: s % kw if n == 1 else p % kw,
            url_for=lambda endpoint, **kw: f"/{endpoint}",
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/search", args={}),
            session={}, current_locale="en", current_year=2026, ms_enabled=False,
            user=None, query="", category_filter="", language_filter="", date_filter="",
            paper_type_filter="", ee_subject_filter="", cp_context_filter="",
            ee_subjects_list=[], cp_contexts=[], filtered=False, records=[],
            pagination=SimpleNamespace(page=1, pages=1, has_prev=False, has_next=False),
            is_guest=True, total_matches=0, paper_categories=[], journal_id_map={},
            journals=journals, journal_filter=journal_filter,
        )

    def test_journal_select_renders_with_selection(self):
        html = self.render_search(["IB EE", "Youth Research"], "IB EE")
        # Single-select dropdown like Paper Type, not checkboxes
        self.assertIn('name="journal"', html)
        self.assertNotIn('name="journal[]"', html)
        self.assertIn("IB EE", html)
        self.assertIn("Youth Research", html)
        # The current filter is the selected <option>
        self.assertRegex(html, r'<option value="IB EE"[^>]*selected')

    def test_no_selection_has_no_selected_option(self):
        html = self.render_search(["IB EE"], "")
        self.assertIn('name="journal"', html)
        self.assertNotRegex(html, r'<option value="IB EE"[^>]*selected')


if __name__ == "__main__":
    unittest.main()
