import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EeTotalGradeContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.upload_template = (ROOT / "templates" / "upload.html").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_upload_total_grade_is_readonly_and_calculated_from_criteria(self):
        total_input = re.search(
            r'<input[^>]+id="ibTotalGradeNumber"[^>]*>',
            self.upload_template,
            re.DOTALL,
        )
        self.assertIsNotNone(total_input)
        self.assertIn("readonly", total_input.group(0))
        self.assertIn("updateIbTotal", self.upload_template)
        self.assertIn('document.querySelectorAll(".ib-crit-score")', self.upload_template)
        self.assertIn('addEventListener("input", updateIbTotal)', self.upload_template)

    def test_server_ee_total_ignores_submitted_total_field(self):
        helper = self._find_function("build_ib_ee_data_from_form")
        helper_source = ast.get_source_segment(self.app_source, helper)

        self.assertIn('"total_grade_number": str(total_score)', helper_source)
        self.assertNotIn('form.get("ib_total_grade_number"', helper_source)

    def _find_function(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        self.fail(f"Could not find function {name}")


if __name__ == "__main__":
    unittest.main()
