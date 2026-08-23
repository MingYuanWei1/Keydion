"""Contract: news category names can never execute script in the manager.

Security-review finding [2]: a role-2 Contributor could persist a
quote-bearing category name; news_manage.html interpolated the text-context
escHtml() output into double-quoted attributes and reparsed with innerHTML,
yielding stored XSS in the shared Curator view (CSP is report-only).

Two enforced layers:
1. Server: add/rename reject names carrying markup or quote characters
   (category_name_validation_error in services/news.py) — nothing dangerous
   is ever persisted.
2. Client: escHtml() must be attribute-safe (escape quotes), because category
   names are interpolated into double-quoted attributes before innerHTML.
"""
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

TEMPLATE_PATH = ROOT / "templates" / "news_manage.html"


class CategoryNameValidator(unittest.TestCase):
    def test_rejects_double_quote_attribute_breakout(self):
        from services.news import category_name_validation_error
        self.assertIsNotNone(
            category_name_validation_error('x" onmouseover="alert(document.domain)'))

    def test_rejects_markup(self):
        from services.news import category_name_validation_error
        self.assertIsNotNone(category_name_validation_error("<script>alert(1)</script>"))

    def test_rejects_single_quote(self):
        from services.news import category_name_validation_error
        self.assertIsNotNone(category_name_validation_error("it's"))

    def test_rejects_control_characters(self):
        from services.news import category_name_validation_error
        self.assertIsNotNone(category_name_validation_error("cat\x00egory"))

    def test_enforces_server_side_max_length(self):
        from services.news import category_name_validation_error
        self.assertIsNotNone(category_name_validation_error("a" * 51))
        self.assertIsNone(category_name_validation_error("a" * 50))

    def test_accepts_normal_names(self):
        from services.news import category_name_validation_error
        # Ampersand is allowed: escHtml escapes it client-side.
        for name in ("Campus News", "学术动态", "R&D"):
            self.assertIsNone(category_name_validation_error(name), name)


class CategoryRoutesUseValidator(unittest.TestCase):
    def test_add_route_validates_name(self):
        self.assertIn("category_name_validation_error",
                      support.source_of("news_category_add"))

    def test_rename_route_validates_new_name(self):
        self.assertIn("category_name_validation_error",
                      support.source_of("news_category_rename"))


class EscHtmlAttributeSafe(unittest.TestCase):
    def _esc_html_source(self):
        text = TEMPLATE_PATH.read_text(encoding="utf-8")
        lines = text.splitlines()
        start = next(i for i, line in enumerate(lines) if "function escHtml" in line)
        body = [lines[start]]
        if lines[start].strip().endswith("{"):
            for line in lines[start + 1:]:
                body.append(line)
                if line.strip() == "}":
                    break
        return "\n".join(body)

    def test_escapes_double_quote(self):
        src = self._esc_html_source()
        self.assertIn("&quot;", src,
                      "escHtml must escape double quotes for attribute contexts")

    def test_escapes_single_quote(self):
        src = self._esc_html_source()
        self.assertIn("&#39;", src)

    def test_does_not_use_textcontent_innerhtml_trick(self):
        src = self._esc_html_source()
        self.assertNotIn("textContent", src)
        self.assertNotIn("innerHTML", src)


if __name__ == "__main__":
    unittest.main()
