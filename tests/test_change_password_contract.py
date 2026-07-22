import ast
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


ROOT = Path(__file__).resolve().parents[1]


class ChangePasswordContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template_source = (
            ROOT / "templates" / "change_password.html"
        ).read_text(encoding="utf-8")
        cls.view, view_text = support.find_function("change_password")
        cls.view_source = ast.get_source_segment(view_text, cls.view)

    # --- Task 2: redirect destination ---------------------------------

    def test_validation_failures_redirect_to_change_password_not_dashboard(self):
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


    # --- Task 4: composition + different-from-current -----------------

    def test_view_enforces_letters_and_digits(self):
        has_alpha = "c.isalpha()" in self.view_source
        has_digit = "c.isdigit()" in self.view_source
        self.assertTrue(
            has_alpha and has_digit,
            "change_password view must enforce a letters-and-digits rule "
            "(expected `any(c.isalpha() for c in ...)` and "
            "`any(c.isdigit() for c in ...)` in the function body)",
        )

    def test_view_rejects_unchanged_password(self):
        comparisons = re.findall(
            r"new_password\s*(?:!=|==)\s*current_password|"
            r"current_password\s*(?:!=|==)\s*new_password",
            self.view_source,
        )
        self.assertTrue(
            comparisons,
            "change_password view must compare new_password against "
            "current_password to reject reuse",
        )

    def test_success_branch_preserves_language_clears_session_and_requires_login(self):
        success_branch = next(
            (
                node.body
                for node in ast.walk(self.view)
                if isinstance(node, ast.If)
                and isinstance(node.test, ast.Name)
                and node.test.id == "success"
            ),
            None,
        )
        self.assertIsNotNone(
            success_branch,
            "change_password must branch on the password-update result",
        )
        branch_source = "\n".join(ast.unparse(node) for node in success_branch)
        self.assertRegex(
            branch_source,
            r'language\s*=\s*session\.get\(\s*["\']language["\']\s*\)',
            "the successful update branch must save the selected language",
        )
        self.assertRegex(
            branch_source,
            r"session\.clear\(\)",
            "the successful update branch must clear the browser session",
        )
        self.assertRegex(
            branch_source,
            r'session\[\s*["\']language["\']\s*\]\s*=\s*language',
            "the successful update branch must restore the selected language",
        )
        self.assertIn(
            "Password updated. Please sign in again.",
            branch_source,
            "the successful update branch must tell the user to sign in again",
        )
        self.assertRegex(
            branch_source,
            r"url_for\(\s*[\"']index[\"']\s*,\s*login\s*=\s*1\s*\)",
            "the successful update branch must send the user to the login entry point",
        )


    # --- Task 5: template render contract ------------------------------

    @staticmethod
    def _render(has_password):
        from jinja2 import DictLoader, Environment

        env = Environment(
            loader=DictLoader({
                "_bare.html": "{% block title %}{% endblock %}{% block panel %}{% endblock %}",
                "_dashboard_shell.html": "{% block title %}{% endblock %}{% block panel %}{% endblock %}",
                "change_password.html": (
                    ROOT / "templates" / "change_password.html"
                ).read_text(encoding="utf-8"),
            }),
            autoescape=True,
            extensions=["jinja2.ext.i18n"],
        )
        env.install_null_translations(newstyle=True)
        env.globals["url_for"] = lambda name, **_: "/" + name.replace("_", "-")
        env.globals["get_flashed_messages"] = lambda **_: []
        env.globals["csrf_token"] = lambda: ""
        return env.get_template("change_password.html").render(
            user={"username": "alice"},
            has_password=has_password,
            partial=False,
        )

    def test_template_has_current_password_input_when_has_password(self):
        html = self._render(has_password=True)
        self.assertRegex(
            html,
            r'<input[^>]+name="current_password"',
            "template must include a current_password input when has_password=True",
        )
        self.assertRegex(html, r'<input[^>]+name="new_password"')
        self.assertRegex(html, r'<input[^>]+name="confirm_password"')

    def test_template_omits_current_password_input_for_first_time_set(self):
        html = self._render(has_password=False)
        self.assertNotIn(
            'name="current_password"',
            html,
            "template must omit the current_password input when has_password=False",
        )
        self.assertRegex(html, r'<input[^>]+name="new_password"')
        self.assertRegex(html, r'<input[^>]+name="confirm_password"')


if __name__ == "__main__":
    unittest.main()
