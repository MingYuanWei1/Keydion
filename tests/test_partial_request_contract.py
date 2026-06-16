import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

ROOT = Path(__file__).resolve().parents[1]


class PartialRequestContractTest(unittest.TestCase):
    def test_is_partial_request_helper_exists(self):
        node, _text = support.find_function("is_partial_request")
        self.assertIsNotNone(node, "is_partial_request helper not found")

    def test_helper_reads_x_partial_content_header(self):
        src = support.source_of("is_partial_request")
        self.assertIn("X-Partial-Content", src)

    def test_inject_partial_flag_context_processor_exists(self):
        node, _text = support.find_function("inject_partial_flag")
        self.assertIsNotNone(node, "inject_partial_flag context processor not found")


class BareTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / "templates" / "_bare.html"

    def test_bare_template_exists(self):
        self.assertTrue(self.path.exists(), "_bare.html does not exist")

    def test_bare_template_has_panel_block(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("{% block panel %}{% endblock %}", src)

    def test_bare_template_renders_flash_messages(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("get_flashed_messages", src)

    def test_bare_template_has_no_html_tag(self):
        # The bare template must NOT render <html>, <head>, or <body> tags.
        src = self.path.read_text(encoding="utf-8")
        self.assertNotIn("<html", src.lower())
        self.assertNotIn("<head>", src.lower())
        self.assertNotIn("<body", src.lower())


class PartialAwareTemplatesContractTest(unittest.TestCase):
    TEMPLATES = [
        "upload.html",
        "my_submissions.html",
        "review_list.html",
        "review_paper.html",
        "paper_manage.html",
        "ee_subjects_manage.html",
        "news_publish.html",
        "news_manage.html",
        "admin_users.html",
        "guide_manage.html",
        "change_password.html",
    ]

    def test_all_sidebar_destinations_extend_conditionally(self):
        # On a partial fetch, the page extends _bare.html; on a direct visit,
        # it extends the shared shell so the sidebar wraps it server-side.
        expected = '{% extends "_bare.html" if partial else "_dashboard_shell.html" %}'
        for name in self.TEMPLATES:
            path = ROOT / "templates" / name
            self.assertTrue(path.exists(), f"{name} missing")
            first_line = path.read_text(encoding="utf-8").splitlines()[0].strip()
            self.assertEqual(
                first_line,
                expected,
                f"{name} first line should be conditional extends, got: {first_line!r}",
            )

    def test_all_sidebar_destinations_use_panel_block(self):
        # The shell expects {% block panel %}, not the old {% block content %}.
        import re
        for name in self.TEMPLATES:
            src = (ROOT / "templates" / name).read_text(encoding="utf-8")
            self.assertRegex(
                src,
                r"\{%\s*block\s+panel\s*%\}",
                f"{name} must define a {{% block panel %}} block",
            )
            # The old content block must be gone (otherwise direct visits would
            # render an empty panel: dashboard_shell.html doesn't override content).
            self.assertNotRegex(
                src,
                r"\{%\s*block\s+content\s*%\}",
                f"{name} should no longer use {{% block content %}} — rename to panel",
            )

    def test_bare_template_uses_panel_block(self):
        src = (ROOT / "templates" / "_bare.html").read_text(encoding="utf-8")
        self.assertIn("{% block panel %}{% endblock %}", src)


if __name__ == "__main__":
    unittest.main()
