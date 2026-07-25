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
            "kd-cat-row", "kd-cat-label", "kd-cat-count",
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


if __name__ == "__main__":
    unittest.main()
