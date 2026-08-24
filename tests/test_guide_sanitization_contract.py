import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.guides import _sanitize_guide_html  # noqa: E402


class GuideSanitizationContractTest(unittest.TestCase):
    def test_strips_script_tags(self):
        result = _sanitize_guide_html("<p>Hi</p><script>alert(1)</script>")
        self.assertIn("<p>Hi</p>", result)
        self.assertNotIn("<script", result)
        self.assertNotIn("alert(1)", result)

    def test_strips_javascript_href(self):
        result = _sanitize_guide_html('<a href="javascript:alert(1)">x</a>')
        self.assertNotIn("javascript:", result)

    def test_allows_safe_formatting(self):
        html = (
            "<h2>Login</h2><p><strong>Click</strong> the "
            '<a href="/login">login</a> button.</p>'
            '<ul><li>Step 1</li></ul>'
            '<img src="/static/uploads/guides/x.png" alt="screenshot">'
        )
        result = _sanitize_guide_html(html)
        self.assertIn("<h2>Login</h2>", result)
        self.assertIn("<strong>Click</strong>", result)
        self.assertIn('href="/login"', result)
        self.assertIn("<li>Step 1</li>", result)
        self.assertIn('src="/static/uploads/guides/x.png"', result)

    def test_strips_event_handlers(self):
        result = _sanitize_guide_html('<p onclick="evil()">hi</p>')
        self.assertNotIn("onclick", result)
        self.assertIn("hi", result)

    def test_preserves_callout_markup(self):
        html = (
            '<div class="kd-callout">'
            '<div class="kd-callout-label">Note</div>'
            '<div class="kd-callout-body"><p>Be careful.</p></div>'
            '</div>'
        )
        result = _sanitize_guide_html(html)
        self.assertIn('class="kd-callout"', result)
        self.assertIn('class="kd-callout-label"', result)
        self.assertIn('class="kd-callout-body"', result)
        self.assertIn("Be careful.", result)

    def test_preserves_figure_markup_with_image_class(self):
        html = (
            '<div class="kd-fig">'
            '<img class="kd-fig-img" src="/static/uploads/guides/x.png" alt="">'
            '<div class="kd-fig-caption">'
            '<span class="num">Fig. 01</span>'
            '<span class="caption-text">An example.</span>'
            '</div></div>'
        )
        result = _sanitize_guide_html(html)
        self.assertIn('class="kd-fig"', result)
        self.assertIn('class="kd-fig-img"', result)
        self.assertIn('src="/static/uploads/guides/x.png"', result)
        self.assertIn('class="num"', result)
        self.assertIn("Fig. 01", result)
        self.assertIn("An example.", result)


if __name__ == "__main__":
    unittest.main()
