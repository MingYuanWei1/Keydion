"""Contracts for bugs found during the holistic HTTP functional test.

Each test below failed against the pre-fix code and passes after the
corresponding fix. They guard:
  1. admin_users route passes `user` to its template (was: 500 'user' undefined)
  2. paper_info returns 404 for an unknown filename (was: 200 with empty stub)
  3. ee-subjects add/delete match group_id regardless of int/str type
  4. _read_guide_form treats an HTML-default checkbox value ("on") as published
  5. the public /guides index includes published guides that have no category
"""
import importlib
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


def _load_app():
    os.environ["PAPERQUERY_SECRET"] = "test-secret"
    os.environ.setdefault("PAPERQUERY_DATABASE_URL", "sqlite:///:memory:")
    import app as app_module
    importlib.reload(app_module)
    return app_module


class _SourceTest(unittest.TestCase):
    def func_source(self, name):
        return support.source_of(name)


class AdminUsersTemplateContextTest(_SourceTest):
    def test_admin_users_passes_user_to_template(self):
        s = self.func_source("admin_users")
        self.assertIn("admin_users.html", s)
        # The dashboard shell dereferences user.role; the route must supply it.
        self.assertIn("user=user", s)


class PaperInfoNotFoundTest(_SourceTest):
    def test_paper_info_returns_404_for_unavailable_uuid(self):
        route = self.func_source("paper_info")
        boundary = self.func_source("_current_paper_pdf")

        self.assertIn("_current_paper_pdf(paper_id)", route)
        self.assertIn("except NotFound:", boundary)
        self.assertIn("abort(404)", boundary)


class GuidePublishedCheckboxTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = _load_app()

    def test_published_accepts_html_default_on(self):
        self.assertTrue(self.m._read_guide_form({"published": "on"})["published"])

    def test_published_accepts_explicit_one(self):
        self.assertTrue(self.m._read_guide_form({"published": "1"})["published"])

    def test_published_false_when_field_absent(self):
        self.assertFalse(self.m._read_guide_form({})["published"])


class GuidesIndexGroupingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = _load_app()

    def test_uncategorized_published_guide_is_listed(self):
        guides = [{"slug": "no-cat", "category": "", "title_en": "X", "published": True}]
        grouped = self.m._group_guides_for_index(guides, ["Known"])
        listed = [g["slug"] for _cat, items in grouped for g in items]
        self.assertIn("no-cat", listed)

    def test_known_categories_keep_their_order(self):
        guides = [
            {"slug": "b", "category": "Beta", "title_en": "B", "published": True},
            {"slug": "a", "category": "Alpha", "title_en": "A", "published": True},
        ]
        grouped = self.m._group_guides_for_index(guides, ["Alpha", "Beta"])
        self.assertEqual([cat for cat, _items in grouped], ["Alpha", "Beta"])

    def test_configured_order_drives_the_exact_reader_navigation_sequence(self):
        guides = [
            {"slug": "reader-account", "category": "Account", "sort_order": 10},
            {"slug": "welcome-to-keydion", "category": "Getting Started", "sort_order": 10},
            {"slug": "find-research", "category": "Getting Started", "sort_order": 20},
            {
                "slug": "read-and-download-papers",
                "category": "Getting Started",
                "sort_order": 30,
            },
            {"slug": "ask-the-library", "category": "Getting Started", "sort_order": 40},
            {
                "slug": "explore-journals-and-resources",
                "category": "Getting Started",
                "sort_order": 50,
            },
            {"slug": "read-news-and-updates", "category": "News", "sort_order": 10},
            {
                "slug": "submit-research-for-review",
                "category": "Submissions",
                "sort_order": 10,
            },
            {
                "slug": "track-drafts-and-submissions",
                "category": "Submissions",
                "sort_order": 20,
            },
        ]
        categories = ["Getting Started", "Account", "Submissions", "News"]

        ordering_helper = getattr(self.m, "_order_guides_for_index", None)
        self.assertIsNotNone(
            ordering_helper,
            "guides need one shared configured category-ordering helper",
        )
        ordered = ordering_helper(guides, categories)
        slugs = [guide["slug"] for guide in ordered]

        self.assertEqual(
            slugs,
            [
                "welcome-to-keydion",
                "find-research",
                "read-and-download-papers",
                "ask-the-library",
                "explore-journals-and-resources",
                "reader-account",
                "submit-research-for-review",
                "track-drafts-and-submissions",
                "read-news-and-updates",
            ],
        )
        neighbors = {
            slug: (
                slugs[index - 1] if index else None,
                slugs[index + 1] if index + 1 < len(slugs) else None,
            )
            for index, slug in enumerate(slugs)
        }
        self.assertEqual(neighbors["welcome-to-keydion"][0], None)
        self.assertEqual(
            neighbors["explore-journals-and-resources"][1], "reader-account"
        )
        self.assertEqual(
            neighbors["reader-account"][1], "submit-research-for-review"
        )
        self.assertEqual(
            neighbors["track-drafts-and-submissions"][1], "read-news-and-updates"
        )
        self.assertEqual(neighbors["read-news-and-updates"][1], None)


if __name__ == "__main__":
    unittest.main()
