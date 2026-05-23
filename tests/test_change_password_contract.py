import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ChangePasswordContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.template_source = (
            ROOT / "templates" / "change_password.html"
        ).read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)
        cls.view = cls._find_function("change_password")
        cls.view_source = ast.get_source_segment(cls.app_source, cls.view)

    @classmethod
    def _find_function(cls, name):
        for node in ast.walk(cls.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        raise AssertionError(f"Could not find function {name}")

    # --- Task 2: redirect destination ---------------------------------

    def test_success_redirects_to_change_password_not_dashboard(self):
        url_for_calls = []
        for node in ast.walk(self.view):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "url_for"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                url_for_calls.append(node.args[0].value)

        self.assertIn(
            "change_password",
            url_for_calls,
            "change_password view should redirect back to itself after POST",
        )
        self.assertNotIn(
            "dashboard",
            url_for_calls,
            "change_password view should not redirect to the dashboard overview",
        )


    # --- Task 3: current-password verification ------------------------

    def test_view_reads_current_password_from_form(self):
        self.assertRegex(
            self.view_source,
            r'request\.form\.get\(\s*["\']current_password["\']',
            "change_password view must read current_password from request.form",
        )

    def test_view_calls_verify_password_on_current_password(self):
        verify_calls = [
            node for node in ast.walk(self.view)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "verify_password"
        ]
        self.assertTrue(
            verify_calls,
            "change_password view must call verify_password(...) to check the current password",
        )
        first_arg_names = [
            call.args[0].id
            for call in verify_calls
            if call.args and isinstance(call.args[0], ast.Name)
        ]
        self.assertIn(
            "current_password",
            first_arg_names,
            "verify_password must be called with current_password as its first argument",
        )


if __name__ == "__main__":
    unittest.main()
