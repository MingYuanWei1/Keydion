import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PartialRequestContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_is_partial_request_helper_exists(self):
        found = any(
            isinstance(node, ast.FunctionDef) and node.name == "is_partial_request"
            for node in ast.walk(self.app_tree)
        )
        self.assertTrue(found, "is_partial_request helper not found")

    def test_helper_reads_x_partial_content_header(self):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == "is_partial_request":
                src = ast.get_source_segment(self.app_source, node)
                self.assertIn("X-Partial-Content", src)
                return
        self.fail("is_partial_request helper not found")

    def test_inject_partial_flag_context_processor_exists(self):
        found = any(
            isinstance(node, ast.FunctionDef) and node.name == "inject_partial_flag"
            for node in ast.walk(self.app_tree)
        )
        self.assertTrue(found, "inject_partial_flag context processor not found")


class BareTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / "templates" / "_bare.html"

    def test_bare_template_exists(self):
        self.assertTrue(self.path.exists(), "_bare.html does not exist")

    def test_bare_template_has_content_block(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("{% block content %}{% endblock %}", src)

    def test_bare_template_renders_flash_messages(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("get_flashed_messages", src)

    def test_bare_template_has_no_html_tag(self):
        # The bare template must NOT render <html>, <head>, or <body> tags.
        src = self.path.read_text(encoding="utf-8")
        self.assertNotIn("<html", src.lower())
        self.assertNotIn("<head>", src.lower())
        self.assertNotIn("<body", src.lower())


if __name__ == "__main__":
    unittest.main()
