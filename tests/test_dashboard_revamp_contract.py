import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class DashboardAssetsContractTest(unittest.TestCase):
    def test_dashboard_css_exists(self):
        self.assertTrue((ROOT / "static" / "css" / "dashboard.css").exists())

    def test_dashboard_js_exists(self):
        self.assertTrue((ROOT / "static" / "js" / "dashboard.js").exists())

    def test_dashboard_js_intercepts_partial_links(self):
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertIn("X-Partial-Content", src)
        self.assertIn("data-partial-href", src)

    def test_dashboard_js_persists_sidebar_state(self):
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertIn("keydion.sidebar", src)


class OverviewPartialContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / "templates" / "_dashboard" / "overview.html"

    def test_overview_exists(self):
        self.assertTrue(self.path.exists())

    def test_overview_hides_stats_row_for_role_1(self):
        src = self.path.read_text(encoding="utf-8")
        # The stats-row block must be wrapped in {% if role > 1 %}.
        self.assertRegex(src, r"\{%\s*if\s+role\s*>\s*1\s*%\}\s*\n?\s*<section class=\"stats-row\"")

    def test_overview_role_1_quick_actions_are_upload_and_change_password_only(self):
        src = self.path.read_text(encoding="utf-8")
        # "My submissions" action card (the role==1 block) must be removed.
        self.assertNotIn("View submissions", src)
        # Upload research + Change password cards must remain.
        self.assertIn("Open uploader", src)
        self.assertIn("Update security", src)

    def test_overview_published_news_tile_uses_published_news_stat(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("Published news", src)
        self.assertIn("dashboard_stats.published_news", src)

    def test_overview_pending_news_tile_uses_pending_news_stat(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("Pending news", src)
        self.assertIn("dashboard_stats.pending_news", src)


class DashboardShellTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = (ROOT / "templates" / "dashboard.html").read_text(encoding="utf-8")

    def test_shell_extends_base(self):
        self.assertIn('{% extends "base.html" %}', self.src)

    def test_shell_loads_dashboard_assets(self):
        self.assertIn("dashboard.css", self.src)
        self.assertIn("dashboard.js", self.src)

    def test_shell_includes_overview_partial(self):
        self.assertIn('include "_dashboard/overview.html"', self.src)

    def test_shell_has_sidebar_groups(self):
        # Workspace + Account are always present; others gated by role in template.
        self.assertIn("'Workspace'", self.src)
        self.assertIn("'Account'", self.src)

    def test_shell_links_use_data_partial_href(self):
        # Sidebar nav items must opt into partial loading.
        self.assertIn("data-partial-href", self.src)


if __name__ == "__main__":
    unittest.main()
