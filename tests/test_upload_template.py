import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class UploadTemplateDomContractTest(unittest.TestCase):
    """The wizard JS targets specific element IDs; they must exist in the template."""

    @classmethod
    def setUpClass(cls):
        cls.template_src = (ROOT / "templates" / "upload.html").read_text(encoding="utf-8")
        cls.wizard_js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")

    def test_template_declares_wizard_mount_points(self):
        for required_id in ("wizardStepper", "wizardSteps", "wizardFooter", "uploadForm",
                             "uploadFormFile", "autosaveIndicator"):
            self.assertIn(f'id="{required_id}"', self.template_src,
                          f"upload.html is missing #{required_id}")

    def test_wizard_js_getElementById_calls_resolve_to_template_ids(self):
        # IDs declared in this template (the wizard's panel).
        declared_in_template = set(re.findall(r'\bid="([^"]+)"', self.template_src))
        # IDs declared in the parent dashboard shell (e.g., dashboardMain).
        shell_src = (ROOT / "templates" / "_dashboard_shell.html").read_text(encoding="utf-8")
        declared_in_shell = set(re.findall(r'\bid="([^"]+)"', shell_src))
        declared = declared_in_template | declared_in_shell

        referenced = set(re.findall(
            r'getElementById\(\s*["\']([^"\']+)["\']\s*\)', self.wizard_js
        ))
        # The wizard creates many IDs dynamically (#f-title, #eeCriteria, etc.)
        # inside its rendered HTML and then queries them via querySelector — those
        # do NOT need to be in the template. Only the boot-time getElementById
        # mount points are checked here.
        missing = referenced - declared
        self.assertEqual(missing, set(),
                          f"wizard JS references missing IDs: {sorted(missing)}")


if __name__ == "__main__":
    unittest.main()
