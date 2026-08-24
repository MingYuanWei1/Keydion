import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class NewsBodySanitizationContractTest(unittest.TestCase):
    def test_helper_strips_script_keeps_safe_markup(self):
        from services.news import sanitize_news_body
        raw = json.dumps([
            {"type": "text", "content": "<p>ok</p><script>alert(1)</script>"},
            {"type": "image", "url": "/x.png", "caption": "c"},
        ])
        out = json.loads(sanitize_news_body(raw))
        self.assertIn("<p>ok</p>", out[0]["content"])
        self.assertNotIn("<script", out[0]["content"])
        self.assertNotIn("alert(1)", out[0]["content"])
        self.assertEqual(out[1]["url"], "/x.png")  # non-text block untouched

    def test_helper_strips_onerror(self):
        from services.news import sanitize_news_body
        raw = json.dumps([{"type": "text", "content": '<img src=x onerror=alert(1)>'}])
        out = json.loads(sanitize_news_body(raw))
        self.assertNotIn("onerror", out[0]["content"])

    def test_helper_handles_legacy_plaintext(self):
        from services.news import sanitize_news_body
        out = sanitize_news_body("<script>bad()</script>hello")
        self.assertNotIn("<script", out)

    def test_quill_html_is_sanitized_and_hardened(self):
        from services.news import sanitize_news_body
        out = sanitize_news_body(
            '<p>Hello <a href="https://example.test" target="_blank">world</a></p>'
            '<img src="/static/x.png" onerror="bad()"><script>bad()</script>'
        )
        self.assertIn("<p>Hello", out)
        self.assertIn('rel="noopener noreferrer"', out)
        self.assertNotIn("onerror", out)
        self.assertNotIn("bad()", out)

    def test_blank_quill_body_is_empty_for_publish_validation(self):
        from services.news import sanitize_news_body
        self.assertEqual(sanitize_news_body("<p><br></p>"), "")

    def test_legacy_blocks_convert_to_sanitized_editor_html(self):
        from services.news import news_body_html
        legacy = json.dumps([
            {"type": "text", "content": "First"},
            {"type": "text", "content": "Second"},
            {"type": "image", "url": "/static/legacy.png", "caption": ""},
            {"type": "divider"},
        ])
        out = news_body_html(legacy, "Legacy title")
        self.assertIn('<div class="body-paragraph">First</div>', out)
        self.assertIn('<div class="body-paragraph">Second</div>', out)
        self.assertIn('<img src="/static/legacy.png"', out)
        self.assertIn('alt="Legacy title"', out)
        self.assertIn('<hr class="news-divider"', out)

    def test_legacy_plain_text_keeps_paragraph_breaks(self):
        from services.news import news_body_html
        self.assertEqual(
            news_body_html("First\n\nSecond"),
            '<div class="body-paragraph">First</div>'
            '<div class="body-paragraph">Second</div>',
        )
