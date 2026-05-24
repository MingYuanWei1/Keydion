import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EeTotalGradeContractTest(unittest.TestCase):
    """EE total grade is computed server-side from per-criterion scores.

    The wizard NEVER writes a hidden input named ib_total_grade_number, so the
    server can never be tricked into accepting a client-submitted total.
    """

    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.wizard_js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_server_ee_total_recomputed_from_criteria(self):
        helper = self._find_function("build_ib_ee_data_from_form")
        src = ast.get_source_segment(self.app_source, helper)
        self.assertIn('"total_grade_number": str(total_score)', src)
        self.assertNotIn('form.get("ib_total_grade_number"', src)

    def test_wizard_computes_total_client_side_from_ee_scores(self):
        # The total readout updates live; this exists somewhere in the JS.
        self.assertIn("sumScores(state.eeScores)", self.wizard_js)
        self.assertRegex(self.wizard_js, r"#eeTotal")

    def test_wizard_does_not_serialize_ib_total_grade_number(self):
        # serializeToForm contains the wire-contract field list — verify the
        # untrusted total field never appears.
        self.assertNotIn("ib_total_grade_number", self.wizard_js)

    def _find_function(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        self.fail(f"Could not find function {name}")


if __name__ == "__main__":
    unittest.main()
