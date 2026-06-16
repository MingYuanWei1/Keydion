"""Contract: news_manage.html category modal can actually be opened.

The modal is toggled by adding a `.show` class to #catOverlay, which the
stylesheet shows via `.cat-overlay.show { display: flex }`. An inline
`style="display:none"` on the element would win over that class rule and
keep the modal hidden no matter how many times the button is clicked.
"""
import re
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]


class NewsManageTemplateTest(unittest.TestCase):
    def render(self, categories=None, articles=None):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"))
        return env.get_template("news_manage.html").render(
            _=lambda value, **kw: value % kw if kw else value,
            url_for=lambda endpoint, **kw: f"/{endpoint}",
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/dashboard/news/manage", args={}),
            session={},
            current_locale="en",
            partial=True,
            user=SimpleNamespace(role="3"),
            categories=categories or [],
            articles=articles or [],
        )

    def test_overlay_has_no_inline_display_none(self):
        html = self.render(categories=["News"])
        overlay_tag = re.search(r"<div[^>]*id=\"catOverlay\"[^>]*>", html)
        self.assertIsNotNone(overlay_tag, "catOverlay element missing")
        self.assertNotIn(
            "display:none",
            overlay_tag.group(0).replace(" ", ""),
            "inline display:none would override the .cat-overlay.show toggle",
        )

    def test_open_and_close_hooks_present(self):
        html = self.render()
        self.assertIn('id="openCatModalBtn"', html)
        self.assertIn('id="closeCatModal"', html)


if __name__ == "__main__":
    unittest.main()
