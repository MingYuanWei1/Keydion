# tests/test_ask_page_contract.py
import ast
import os
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as f:
        return f.read()


class AskRouteRegistered(unittest.TestCase):
    def test_app_defines_ask_route_and_endpoint(self):
        src = _read("app.py")
        self.assertIn('@app.route("/ask")', src)
        self.assertIn("def ask_library(", src)

    def test_app_defines_ask_api_route(self):
        src = _read("app.py")
        self.assertIn('@app.route("/api/ask"', src)


class AskTemplateDom(unittest.TestCase):
    def test_template_extends_partial_aware_base(self):
        html = _read("templates/ask.html")
        self.assertIn('_bare.html" if partial else "base.html"', html)

    def test_template_has_js_target_ids(self):
        html = _read("templates/ask.html")
        for needed in ["kd-thread", "kd-composer-input", "kd-send",
                       "kd-agent", "ask-boot"]:
            self.assertIn(needed, html)

    def test_template_loads_ask_assets(self):
        html = _read("templates/ask.html")
        self.assertIn("css/ask.css", html)
        self.assertIn("js/ask.js", html)


if __name__ == "__main__":
    unittest.main()
