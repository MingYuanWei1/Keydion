import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


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

    def test_publish_and_edit_call_sanitizer(self):
        self.assertIn("sanitize_news_body", support.source_of("news_publish"))
        self.assertIn("sanitize_news_body", support.source_of("news_edit"))
