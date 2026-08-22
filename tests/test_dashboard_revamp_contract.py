import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

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

    def test_dashboard_logout_button_resets_native_text_alignment(self):
        # The sign-out label is a growing flex child. Without an explicit
        # left alignment, the button's native centered text creates a large
        # visual gap between the icon and label.
        import re

        src = (ROOT / "static" / "css" / "dashboard.css").read_text(encoding="utf-8")
        rule = re.search(r"\.logout-form button\.nav-item\s*\{([^}]*)\}", src)
        self.assertIsNotNone(rule)
        self.assertRegex(rule.group(1), r"text-align\s*:\s*left")

    def test_dashboard_js_passes_submitter_to_formdata(self):
        # Without the second arg, the clicked button's name=value (e.g.
        # action=draft vs action=publish) is dropped from the partial POST.
        import re
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertRegex(src, r"new\s+FormData\(\s*form\s*,\s*e\.submitter\s*\)")

    def test_dashboard_js_only_full_navs_when_leaving_dashboard(self):
        # The redirect-fallback in loadPartial must check that we *leave*
        # /dashboard/* before doing window.location.href. A naive path-mismatch
        # check would kick the user out on every in-shell redirect after the
        # URL-nesting refactor.
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertRegex(
            src,
            r"!.*startsWith\(\s*['\"]/dashboard['\"]\s*\)",
            "dashboard.js must guard the full-nav fallback with a /dashboard prefix check",
        )

    def test_dashboard_js_pushes_resolved_redirect_url(self):
        # When a redirect resolves in-shell, the address bar must show the
        # final URL (e.g. /dashboard/news/manage), not the originally posted one.
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertIn("res.url", src)
        self.assertRegex(src, r"history\.pushState\([^;]*res\.url|history\.pushState\([^;]*resolvedUrl")


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
        shell_path = ROOT / "templates" / "_dashboard_shell.html"
        cls.shell_src = shell_path.read_text(encoding="utf-8") if shell_path.exists() else ""

    def test_shell_extends_base(self):
        # dashboard.html now extends the shared shell, which itself extends base.html.
        self.assertIn('{% extends "_dashboard_shell.html" %}', self.src)

    def test_shell_loads_dashboard_assets(self):
        # Asset loading now lives in the shell template.
        self.assertIn("dashboard.css", self.shell_src)
        self.assertIn("dashboard.js", self.shell_src)

    def test_shell_includes_overview_partial(self):
        self.assertIn('include "_dashboard/overview.html"', self.src)

    def test_shell_has_sidebar_groups(self):
        # Workspace + Account are always present; others gated by role in template.
        # Sidebar markup moved into the shell.
        self.assertIn("'Workspace'", self.shell_src)
        self.assertIn("'Account'", self.shell_src)

    def test_shell_links_use_data_partial_href(self):
        # Sidebar nav items must opt into partial loading (now in the shell).
        self.assertIn("data-partial-href", self.shell_src)

    def test_shell_template_file_exists(self):
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[1]
        self.assertTrue((ROOT / "templates" / "_dashboard_shell.html").exists())

    def test_dashboard_extends_shell(self):
        # dashboard.html is now a thin wrapper that fills the shell's panel slot.
        self.assertIn('{% extends "_dashboard_shell.html" %}', self.src)
        self.assertIn("{% block panel %}", self.src)
        self.assertIn('include "_dashboard/overview.html"', self.src)

    def test_shell_exposes_panel_block(self):
        import re
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[1]
        shell = (ROOT / "templates" / "_dashboard_shell.html").read_text(encoding="utf-8")
        # Shell must extend base and expose a {% block panel %} slot inside the main panel.
        self.assertIn('{% extends "base.html" %}', shell)
        self.assertTrue(
            re.search(r'id="dashboardMain".*?\{%\s*block\s+panel\s*%\}', shell, flags=re.DOTALL),
            "Shell must expose {% block panel %} inside <main id=\"dashboardMain\">",
        )
        # Sidebar must still be in the shell (moved out of dashboard.html).
        self.assertIn("dashboard-sidebar", shell)
        self.assertIn("data-cycle-sidebar", shell)


class DashboardRouteContractTest(unittest.TestCase):
    def _dashboard_source(self):
        return support.source_of("dashboard")

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
