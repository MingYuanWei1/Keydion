import json
import re
import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape


ROOT = Path(__file__).resolve().parents[1]


class UploadWizardJinjaContractTest(unittest.TestCase):
    """templates/upload.html must render the wizard shell correctly."""

    @classmethod
    def setUpClass(cls):
        env = Environment(
            loader=FileSystemLoader(str(ROOT / "templates")),
            autoescape=select_autoescape(["html", "xml"]),
            extensions=["jinja2.ext.i18n"],
        )
        env.install_null_translations()
        # Globals the templates expect to find.
        env.globals["url_for"] = lambda name, **kw: "/" + name + (
            ("?" + "&".join(f"{k}={v}" for k, v in kw.items())) if kw else ""
        )
        env.globals["_"] = lambda s, **kw: s
        env.globals["role_label"] = lambda r: "Contributor"
        env.globals["csrf_token"] = lambda: ""
        # Flask globals used by _bare.html
        env.globals["get_flashed_messages"] = lambda with_categories=False: []
        # Flask's tojson filter is not in vanilla Jinja2
        env.filters["tojson"] = lambda v: json.dumps(v)
        cls.env = env

    def _render(self):
        ctx = {
            "partial": True,
            "user": {
                "username": "u",
                "role": "2",
                "display_name": "U",
                "first_name": "U",
                "last_name": "",
                "email": "",
            },
            "form_data": {},
            "journals": [],
            "paper_categories": ["literature", "natural-science"],
            "ee_subjects": {"groups": []},
            "cp_global_contexts": [],
            "cp_action_types": [],
            "draft_id": "",
            "dashboard_stats": {},
            "wizard_boot": {
                "submit_url": "/dashboard/upload",
                "draft_id": "",
                "form_data": {},
                "paper_categories": ["literature"],
                "ee_subjects": {"groups": []},
                "cp_global_contexts": [],
                "cp_action_types": [],
                "user_key": "u",
                "i18n": {},
            },
        }
        tpl = self.env.get_template("upload.html")
        return tpl.render(**ctx)

    def test_mount_points_present(self):
        out = self._render()
        for required_id in ("wizardStepper", "wizardSteps", "wizardFooter",
                             "uploadForm", "uploadFormFile", "autosaveIndicator"):
            self.assertIn(f'id="{required_id}"', out)

    def test_hidden_form_has_post_and_enctype(self):
        out = self._render()
        # Tolerate any whitespace between attributes.
        self.assertRegex(out, r'<form\b[^>]*\bid="uploadForm"[^>]*\bmethod="post"')
        self.assertRegex(out, r'<form\b[^>]*\bid="uploadForm"[^>]*\benctype="multipart/form-data"')
        self.assertIn('action="/dashboard/upload"', out)

    def test_boot_script_emits_window_WIZARD_BOOT(self):
        out = self._render()
        self.assertIn("window.WIZARD_BOOT =", out)
        # And the JSON payload is parseable.
        match = re.search(r'window\.WIZARD_BOOT\s*=\s*(\{.*?\})\s*;\s*</script>', out, re.DOTALL)
        self.assertIsNotNone(match, "WIZARD_BOOT assignment not found")
        # Jinja2 autoescape HTML-encodes " → &#34; inside the script tag.
        # Unescape before parsing so json.loads can read it.
        import html
        json.loads(html.unescape(match.group(1)))


if __name__ == "__main__":
    unittest.main()
