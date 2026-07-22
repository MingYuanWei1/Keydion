import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class LoginPersistenceUiContractTest(unittest.TestCase):
    def test_both_login_modals_have_unchecked_labelled_control(self):
        for relative in ("templates/_header.html", "templates/ai.html"):
            source = (ROOT / relative).read_text(encoding="utf-8")
            checkbox = re.search(
                r'<input[^>]+type="checkbox"[^>]+name="remember_me"[^>]*>',
                source,
            )
            self.assertIsNotNone(checkbox, relative)
            self.assertNotIn("checked", checkbox.group(0), relative)
            self.assertIn("Stay logged in for 7 days", source)
            self.assertRegex(source, r'<label[^>]+for="[^"]+"')

    def test_both_microsoft_buttons_forward_checkbox_state(self):
        for relative in ("templates/_header.html", "templates/ai.html"):
            source = (ROOT / relative).read_text(encoding="utf-8")
            self.assertIn("startMicrosoftLogin()", source)
            self.assertIn("remember_me", source)
            self.assertIn("URLSearchParams", source)

    def test_chinese_catalog_translates_the_new_label(self):
        source = (
            ROOT / "translations/zh/LC_MESSAGES/messages.po"
        ).read_text(encoding="utf-8")
        self.assertIn('msgid "Stay logged in for 7 days"', source)
        self.assertIn('msgstr "保持登录状态 7 天"', source)
