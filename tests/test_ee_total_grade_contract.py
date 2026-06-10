import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


ROOT = Path(__file__).resolve().parents[1]


class EeTotalGradeContractTest(unittest.TestCase):
    """EE total grade is computed server-side from per-criterion scores.

    The wizard NEVER writes a hidden input named ib_total_grade_number, so the
    server can never be tricked into accepting a client-submitted total.
    """

    @classmethod
    def setUpClass(cls):
        cls.wizard_js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")

    def test_server_ee_total_recomputed_from_criteria(self):
        src = support.source_of("build_ib_ee_data_from_form")
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


if __name__ == "__main__":
    unittest.main()
