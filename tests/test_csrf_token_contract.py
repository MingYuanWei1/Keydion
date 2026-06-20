import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

ROOT = Path(__file__).resolve().parents[1]

POST_FORM_TEMPLATES = [
    "admin_users.html", "change_password.html", "register.html",
    "account_register.html", "profile_setup.html", "upload.html",
    "paper_modify.html", "news_publish.html", "guide_publish.html",
    "journal_edit.html", "review_paper.html", "my_submissions.html",
]
FETCH_JS = ["dashboard.js", "ai.js", "upload-wizard.js",
            "ee-subjects.js", "ia-subjects.js", "guides-editor.js"]


class CsrfTokenContractTest(unittest.TestCase):
    def test_csrfprotect_enabled(self):
        self.assertIn("CSRFProtect", support.source_of("create_app"))

    def test_meta_csrf_token_exposed(self):
        shell = (ROOT / "templates" / "_dashboard_shell.html").read_text(encoding="utf-8")
        header = (ROOT / "templates" / "_header.html").read_text(encoding="utf-8")
        self.assertTrue('name="csrf-token"' in shell or 'name="csrf-token"' in header)

    def test_post_forms_carry_token(self):
        missing = []
        for tpl in POST_FORM_TEMPLATES:
            p = ROOT / "templates" / tpl
            if not p.exists():
                continue
            html = p.read_text(encoding="utf-8")
            for m in re.finditer(r'<form[^>]*method=["\']post["\'][^>]*>(.*?)</form>', html, re.S | re.I):
                if "csrf_token()" not in m.group(0) and "csrf-token" not in m.group(0):
                    missing.append(tpl)
                    break
        self.assertEqual([], missing, f"POST forms missing csrf token: {missing}")

    def test_fetches_send_csrf_header(self):
        missing = []
        for js in FETCH_JS:
            p = ROOT / "static" / "js" / js
            if not p.exists():
                continue
            text = p.read_text(encoding="utf-8")
            if "fetch(" in text and "X-CSRFToken" not in text:
                missing.append(js)
        self.assertEqual([], missing, f"JS fetches missing X-CSRFToken: {missing}")
