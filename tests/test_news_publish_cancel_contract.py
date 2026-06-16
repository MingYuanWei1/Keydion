"""Contract: the news publish/edit page's Cancel link returns to the
dashboard manage page (news_manage), never the public news list."""
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]


class NewsPublishCancelTest(unittest.TestCase):
    def render(self, editing):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"))
        form_data = {
            "title": "", "author": "", "abstract": "", "body": "",
            "category": "", "image_url": "", "status": "published",
        }
        return env.get_template("news_publish.html").render(
            _=lambda value, **kw: value % kw if kw else value,
            url_for=lambda endpoint, **kw: f"/{endpoint}",
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/dashboard/news/publish", args={}),
            session={},
            current_locale="en",
            partial=True,
            user=SimpleNamespace(role="3"),
            editing=editing,
            form_data=form_data,
            categories=["News"],
        )

    def test_cancel_returns_to_manage_for_new_article(self):
        html = self.render(editing=False)
        self.assertIn('href="/news_manage"', html)
        self.assertNotIn("news_list", html)

    def test_cancel_returns_to_manage_when_editing(self):
        html = self.render(editing=True)
        self.assertIn('href="/news_manage"', html)


if __name__ == "__main__":
    unittest.main()
