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


if __name__ == "__main__":
    unittest.main()
