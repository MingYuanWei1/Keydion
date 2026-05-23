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


class DashboardRouteContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _dashboard_source(self):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == "dashboard":
                return ast.get_source_segment(self.app_source, node)
        self.fail("dashboard route not found")

    def test_dashboard_branches_on_partial_request(self):
        src = self._dashboard_source()
        self.assertIn("is_partial_request()", src)
        self.assertIn("_dashboard/overview.html", src)

    def test_dashboard_computes_role_gated_stats(self):
        src = self._dashboard_source()
        # Role-2+ stats.
        self.assertIn("pending_reviews", src)
        self.assertIn("published_news", src)
        self.assertIn("pending_news", src)
        # Role-3+ stat.
        self.assertIn("papers_in_library", src)

    def test_dashboard_passes_stats_to_templates(self):
        src = self._dashboard_source()
        self.assertIn("dashboard_stats=", src)

    def test_dashboard_does_not_compute_stats_for_role_1(self):
        src = self._dashboard_source()
        # Stats should be gated so role 1 gets an empty dict.
        self.assertIn("role >= 2", src)
        self.assertIn("role >= 3", src)


class BaseNavCleanupContractTest(unittest.TestCase):
    def test_base_html_no_longer_has_top_nav_upload_link(self):
        src = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
        # The Upload link inside the nav block is removed; the only remaining
        # references to 'upload' should be in user-menu / dashboard contexts, not nav-link.
        # Stronger check: no <a class="nav-link" ...url_for('upload')...> in the file.
        self.assertNotRegex(
            src,
            r"<a class=\"nav-link\"[^>]+url_for\('upload'\)",
            "base.html top-nav Upload link must be removed",
        )


if __name__ == "__main__":
    unittest.main()
