import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader


ROOT = Path(__file__).resolve().parents[1]


class SearchTemplateTest(unittest.TestCase):
    def render_search(self, records):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"))
        template = env.get_template("search.html")
        return template.render(
            _=lambda value, **kwargs: value % kwargs if kwargs else value,
            ngettext=lambda singular, plural, n, **kwargs: singular % kwargs if n == 1 else plural % kwargs,
            url_for=lambda endpoint, **kwargs: f"/{endpoint}",
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/search", args={}),
            session={},
            current_locale="en",
            current_year=2026,
            ms_enabled=False,
            user=None,
            query="",
            category_filter="",
            language_filter="",
            date_filter="",
            paper_type_filter="",
            ee_subject_filter="",
            cp_context_filter="",
            ee_subjects_list=[],
            cp_contexts=[],
            filtered=False,
            records=records,
            pagination=SimpleNamespace(page=1, pages=1, has_prev=False, has_next=False),
            is_guest=True,
            total_matches=len(records),
            paper_categories=[],
            journal_id_map={},
        )

    def test_legacy_ib_sample_result_hides_school_and_keeps_date(self):
        html = self.render_search([
            {
                "filename": "sample.pdf",
                "title": "Sample",
                "category": "History",
                "author_name": "IB SAMPLE",
                "author_school": "Hidden School",
                "published_at": "2026-05-21",
                "abstract": "",
                "is_ib_sample": "",
            }
        ])

        self.assertIn("IB SAMPLE", html)
        self.assertIn("2026-05-21", html)
        self.assertNotIn("Hidden School", html)
        self.assertNotIn("Hidden School · 2026-05-21", html)

    def test_anonymous_result_hides_author_and_school_keeps_date(self):
        html = self.render_search([
            {
                "filename": "anon.pdf",
                "title": "Anonymous Paper",
                "category": "Physics",
                "author_name": "",
                "author_school": "",
                "published_at": "2026-06-01",
                "abstract": "",
                "is_ib_sample": "",
                "is_anonymous": "1",
            }
        ])

        self.assertIn("2026-06-01", html)
        self.assertNotIn("IB SAMPLE", html)
        # No author/school fallback text — the author row is gone entirely.
        self.assertNotIn("Not specified", html)

    def test_cp_filter_label_is_community_project(self):
        html = self.render_search([])

        self.assertIn('<option value="cp" >Community Project</option>', html)


if __name__ == "__main__":
    unittest.main()
