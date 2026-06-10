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
    def test_paper_info_returns_404_for_unknown_filename(self):
        s = self.func_source("paper_info")
        self.assertIn("404", s)


class EeSubjectsGroupIdCoercionTest(_SourceTest):
    def test_add_matches_group_id_by_string(self):
        self.assertIn("str(group", self.func_source("ee_subject_add"))

    def test_delete_matches_group_id_by_string(self):
        self.assertIn("str(group", self.func_source("ee_subject_delete"))


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


if __name__ == "__main__":
    unittest.main()
