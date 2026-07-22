import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class LoginPersistenceUiContractTest(unittest.TestCase):
    def test_each_login_modal_wires_checkbox_to_form_label_and_microsoft_flow(self):
        for relative in ("templates/_header.html", "templates/ai.html"):
            source = (ROOT / relative).read_text(encoding="utf-8")
            checkbox = re.search(
                r'<input(?=[^>]+type="checkbox")(?=[^>]+name="remember_me")'
                r'(?=[^>]+form="loginForm")[^>]+id="([^"]+)"[^>]*>',
                source,
            )
            self.assertIsNotNone(checkbox, relative)
            self.assertNotIn("checked", checkbox.group(0), relative)
            checkbox_id = re.escape(checkbox.group(1))
            self.assertRegex(
                source,
                rf'<label[^>]+for="{checkbox_id}"[^>]*>'
                r'.*?Stay logged in for 7 days.*?</label>',
                relative,
            )
            self.assertRegex(
                source,
                r'<button[^>]+onclick="startMicrosoftLogin\(\)"[^>]*>'
                r'.*?Sign in with Microsoft.*?</button>',
                relative,
            )
            handler = re.search(
                r'function startMicrosoftLogin\(\)\s*\{(?P<body>.*?)\n\s*\}',
                source,
                re.DOTALL,
            )
            self.assertIsNotNone(handler, relative)
            handler_source = handler.group("body")
            self.assertRegex(
                handler_source,
                rf'document\.getElementById\(["\']{checkbox_id}["\']\)',
                relative,
            )
            self.assertRegex(
                handler_source,
                r'if\s*\(\s*remember\s*&&\s*remember\.checked\s*\)\s*'
                r'params\.set\(["\']remember_me["\']\s*,\s*["\']1["\']\s*\)',
                relative,
            )
            self.assertIn("new URLSearchParams()", handler_source, relative)

    def test_chinese_catalog_translates_the_new_label(self):
        source = (
            ROOT / "translations/zh/LC_MESSAGES/messages.po"
        ).read_text(encoding="utf-8")
        self.assertIn('msgid "Stay logged in for 7 days"', source)
        self.assertIn('msgstr "保持登录状态 7 天"', source)
