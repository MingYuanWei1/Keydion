import ast
import re
import unittest
from pathlib import Path

from tests import support


ROOT = Path(__file__).resolve().parents[1]

FUNCTION_RULES = (
    ("create_app", ("ProxyFix", "x_for=1", "CSRFProtect", "SESSION_COOKIE_SAMESITE",
                    "SESSION_COOKIE_HTTPONLY", "SESSION_COOKIE_SECURE"),
     ("debug=True", "PROPAGATE_EXCEPTIONS")),
    ("api_ai", ("request.remote_addr",), ("X-Forwarded-For", '.split(",")[0]')),
    ("api_ai_attach", ("request.remote_addr",), ("X-Forwarded-For", '.split(",")[0]')),
    ("resolve_contained", ("is_relative_to",), ()),
    ("pending_paper_file", ("resolve_contained(", "require_login(level=3)"),
     ("is_relative_to", "require_login(level=2)")),
    ("delete_submission", ("resolve_contained(",), ("is_relative_to",)),
    ("my_submission_file", ("resolve_contained(",), ("is_relative_to",)),
    ("upload", ("resolve_contained(",), ("is_relative_to",)),
    ("review_list", ("require_login(level=3)",), ("require_login(level=2)",)),
    ("review_detail", ("require_login(level=3)",), ("require_login(level=2)",)),
    ("review_accept", ("require_login(level=3)",), ("require_login(level=2)",)),
    ("review_reject", ("require_login(level=3)",), ("require_login(level=2)",)),
    ("paper_preview", ("_current_paper_pdf(paper_id)",), ("require_login",)),
    ("preview_paper", ("_current_paper_pdf(paper_id)",), ()),
    ("paper_delete", ("delete_paper(",), ("resolve_contained(", ".unlink")),
    ("paper_modify", ("change_paper(",),
     ("resolve_contained(", ".rename(", "set_pdf_metadata(")),
)


class StaticContractTest(unittest.TestCase):
    def test_function_rules(self):
        for function, required, forbidden in FUNCTION_RULES:
            source = support.source_of(function)
            with self.subTest(function=function):
                for token in required:
                    self.assertIn(token, source)
                for token in forbidden:
                    self.assertNotIn(token, source)

    def test_split_modules_do_not_import_app(self):
        for package in ("routes", "services"):
            for path in sorted((ROOT / package).rglob("*.py")):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                imports = []
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        imports.extend(alias.name for alias in node.names)
                    elif isinstance(node, ast.ImportFrom):
                        imports.append(node.module or "")
                self.assertFalse(
                    any(name == "app" or name.startswith("app.") for name in imports),
                    f"{path} imports app",
                )

    def test_logout_and_post_forms_keep_csrf(self):
        app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        logout = re.search(r'@app\.route\("/logout"[^)]*\)', app_source)
        self.assertIsNotNone(logout)
        self.assertIn("POST", logout.group(0))

        missing = []
        for path in (ROOT / "templates").glob("*.html"):
            html = path.read_text(encoding="utf-8")
            for form in re.finditer(
                r'<form[^>]*method=["\']post["\'][^>]*>(.*?)</form>',
                html,
                re.S | re.I,
            ):
                if "csrf_token()" not in form.group(0) and "csrf-token" not in form.group(0):
                    missing.append(path.name)
                    break
        self.assertEqual([], missing, f"POST forms missing csrf token: {missing}")

    def test_javascript_fetches_send_csrf_header(self):
        missing = []
        for path in (ROOT / "static" / "js").glob("*.js"):
            source = path.read_text(encoding="utf-8")
            if "fetch(" in source and "X-CSRFToken" not in source:
                missing.append(path.name)
        self.assertEqual([], missing, f"JS fetches missing X-CSRFToken: {missing}")

    def test_container_entrypoint_is_non_debug_gunicorn(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        command = next(
            line for line in reversed(dockerfile.splitlines()) if line.strip().startswith("CMD")
        )
        self.assertIn("gunicorn", command)
        self.assertNotIn("--debug", command)


if __name__ == "__main__":
    unittest.main()
