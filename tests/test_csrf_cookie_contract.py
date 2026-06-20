import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

ROOT = Path(__file__).resolve().parents[1]


class CsrfCookieContractTest(unittest.TestCase):
    def test_session_cookie_hardening_present(self):
        src = support.source_of("create_app")
        self.assertIn("SESSION_COOKIE_SAMESITE", src)
        self.assertIn("SESSION_COOKIE_HTTPONLY", src)
        self.assertIn("SESSION_COOKIE_SECURE", src)
        self.assertIn("Lax", src)

    def test_logout_is_post_only(self):
        # source_of strips the decorator, so inspect the raw app.py around the
        # logout def. The authoritative gate is test_logout_route_declares_post.
        app_src = (ROOT / "app.py").read_text(encoding="utf-8")
        before_def = app_src.split("def logout")[0][-200:]
        self.assertIn('methods=["POST"]', before_def)

    def test_logout_route_declares_post(self):
        # The @app.route decorator for logout must declare methods=["POST"]
        app_src = (ROOT / "app.py").read_text(encoding="utf-8")
        m = re.search(r'@app\.route\("/logout"[^)]*\)', app_src)
        self.assertIsNotNone(m, "logout route not found")
        self.assertIn("POST", m.group(0))

    def test_signout_controls_are_forms_not_links(self):
        for tpl in ("_dashboard_shell.html", "_header.html", "ai.html"):
            html = (ROOT / "templates" / tpl).read_text(encoding="utf-8")
            if "logout" in html:
                self.assertNotRegex(
                    html, r'<a[^>]+href="\{\{\s*url_for\(.logout.\)\s*\}\}"',
                    f"{tpl}: sign-out must be a POST form, not <a href>")
