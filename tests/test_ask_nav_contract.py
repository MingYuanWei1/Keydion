# tests/test_ask_nav_contract.py
import os
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as f:
        return f.read()


class NavLinks(unittest.TestCase):
    def test_base_header_links_to_ask(self):
        self.assertIn("url_for('ask_library')", _read("templates/_header.html"))

    def test_landing_header_links_to_ask(self):
        self.assertIn("url_for('ask_library')", _read("templates/_header.html"))

    def test_landing_has_ask_cta_button(self):
        html = _read("templates/landing.html")
        self.assertIn("ask_library", html)
        self.assertIn("btn-search", html)  # CTA reuses the landing button style

    def test_base_includes_header_partial(self):
        self.assertIn('include "_header.html"', _read("templates/base.html"))

    def test_landing_includes_header_partial(self):
        self.assertIn('include "_header.html"', _read("templates/landing.html"))

    def test_news_includes_header_partial(self):
        self.assertIn('include "_header.html"', _read("templates/news.html"))

    def test_news_article_includes_header_partial(self):
        self.assertIn('include "_header.html"', _read("templates/news_article.html"))


if __name__ == "__main__":
    unittest.main()
