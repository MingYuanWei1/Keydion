import unittest
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


class GuidesCssContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        css_path = ROOT / "static" / "css" / "guides.css"
        cls.css = css_path.read_text(encoding="utf-8") if css_path.exists() else ""

    def test_defines_design_tokens(self):
        # CSS custom properties under :root
        for var in [
            "--cream", "--cream-2", "--paper", "--border", "--border-soft",
            "--ink", "--ink-soft", "--muted", "--muted-2",
            "--accent", "--accent-hover", "--accent-tint", "--gold",
            "--serif", "--display", "--sans", "--mono",
        ]:
            self.assertIn(var, self.css, f"guides.css missing token {var}")

    def test_defines_component_classes(self):
        for cls in [
            "kd", "kd-header", "kd-footer", "kd-page", "kd-main", "kd-wrap",
            "kd-eyebrow", "kd-h-display", "kd-h-page", "kd-lede", "kd-meta",
            "kd-cat-row", "kd-cat-label",
            "kd-guide-list", "kd-guide-item", "kd-guide-num",
            "kd-guide-link", "kd-guide-title", "kd-guide-summary", "kd-guide-arrow",
            "kd-back", "kd-article-meta", "kd-cat-pill", "kd-body",
            "kd-callout", "kd-callout-label", "kd-callout-body",
            "kd-fig", "kd-fig-img", "kd-fig-caption",
            "kd-prevnext",
            "kd-form-head", "kd-form-meta", "kd-field", "kd-field-label", "kd-field-hint",
            "kd-input", "kd-select", "kd-input-prefix",
            "kd-toggle", "kd-toggle-track", "kd-toggle-status",
            "kd-editor-card", "kd-editor-head", "kd-editor-lang", "kd-editor-status",
            "kd-editor-fields",
            "kd-ql-toolbar", "kd-ql-group", "kd-ql-btn", "kd-ql-select", "kd-ql-canvas",
            "kd-form-footer", "kd-saved",
            "kd-btn", "kd-btn-primary", "kd-btn-ghost", "kd-btn-danger",
            "kd-hairline", "kd-panel", "kd-panel-head",
        ]:
            self.assertIn(f".{cls}", self.css, f"guides.css missing class .{cls}")

    def test_mobile_callout_stacks_into_a_shrink_safe_column(self):
        mobile_match = re.search(
            r"@media \(max-width: 767\.98px\) \{(?P<body>.*?)\n\}",
            self.css,
            re.DOTALL,
        )
        self.assertIsNotNone(mobile_match, "missing narrow guides breakpoint")
        mobile_css = mobile_match.group("body")
        callout_match = re.search(
            r"\.kd-callout\s*\{(?P<body>.*?)\}",
            mobile_css,
            re.DOTALL,
        )
        self.assertIsNotNone(callout_match, "mobile breakpoint must override .kd-callout")
        callout_css = callout_match.group("body")
        self.assertRegex(
            callout_css,
            r"grid-template-columns\s*:\s*minmax\(0,\s*1fr\)",
        )
        self.assertRegex(callout_css, r"gap\s*:\s*12px")

    def test_guide_page_surface_fills_the_base_body(self):
        main_rule = re.search(
            r"body:has\(\.kd-page\)\s*>\s*main\s*\{(?P<body>.*?)\}",
            self.css,
            re.DOTALL,
        )
        self.assertIsNotNone(main_rule)
        self.assertRegex(main_rule.group("body"), r"display\s*:\s*flex")
        self.assertRegex(main_rule.group("body"), r"flex\s*:\s*1\s+0\s+auto")
        self.assertRegex(main_rule.group("body"), r"padding\s*:\s*0\s*!important")

        container_rule = re.search(
            r"body:has\(\.kd-page\)\s*>\s*main\s*>\s*\.container\s*\{(?P<body>.*?)\}",
            self.css,
            re.DOTALL,
        )
        self.assertIsNotNone(container_rule)
        container_css = container_rule.group("body")
        self.assertRegex(container_css, r"display\s*:\s*flex")
        self.assertRegex(container_css, r"flex-direction\s*:\s*column")
        self.assertRegex(container_css, r"width\s*:\s*100%")
        self.assertRegex(container_css, r"max-width\s*:\s*none")
        self.assertRegex(container_css, r"padding\s*:\s*0")

        page_rule = re.search(
            r"\.kd-page\s*\{(?P<body>.*?)\}",
            self.css,
            re.DOTALL,
        )
        self.assertIsNotNone(page_rule)
        page_css = page_rule.group("body")
        self.assertRegex(page_css, r"flex\s*:\s*1\s+1\s+auto")
        self.assertRegex(page_css, r"width\s*:\s*100%")
        self.assertRegex(page_css, r"box-sizing\s*:\s*border-box")

    def test_reader_content_uses_approved_desktop_widths(self):
        wrap_rule = re.search(
            r"\.kd-wrap\s*\{(?P<body>.*?)\}",
            self.css,
            re.DOTALL,
        )
        self.assertIsNotNone(wrap_rule)
        self.assertRegex(wrap_rule.group("body"), r"max-width\s*:\s*1120px")

        article_rule = re.search(
            r"\.kd-wrap-article\s*\{(?P<body>.*?)\}",
            self.css,
            re.DOTALL,
        )
        self.assertIsNotNone(article_rule)
        self.assertRegex(article_rule.group("body"), r"max-width\s*:\s*920px")

        category_rule = re.search(
            r"\.kd-cat-row\s*\{(?P<body>.*?)\}",
            self.css,
            re.DOTALL,
        )
        self.assertIsNotNone(category_rule)
        self.assertRegex(
            category_rule.group("body"),
            r"grid-template-columns\s*:\s*260px\s+1fr",
        )


if __name__ == "__main__":
    unittest.main()
