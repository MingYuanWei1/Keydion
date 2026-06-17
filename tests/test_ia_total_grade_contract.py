import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


ROOT = Path(__file__).resolve().parents[1]


class IaTotalGradeContractTest(unittest.TestCase):
    """IA total + per-criterion max are computed server-side from the subject
    config, never trusted from the form.

    The per-paper ia_data blob's total_score is the sum of criterion scores,
    total_max is the sum of criterion maxes, and each criterion's max is pulled
    from load_ia_subjects() — not from a client-submitted field.
    """

    @classmethod
    def setUpClass(cls):
        cls.builder_src = support.source_of("build_ia_data_from_form")
        cls.wizard_js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")

    def test_builder_loads_criteria_from_subject_config(self):
        # The criterion list (and thus each max) comes from the on-disk taxonomy.
        self.assertIn("load_ia_subjects()", self.builder_src)

    def test_builder_pulls_each_max_from_config_not_form(self):
        # max is read off the criterion dict from config, never _form_int'd.
        self.assertNotIn('_form_int(form, f"ia_crit_{i}_max"', self.builder_src)
        self.assertNotIn("ia_crit_{i}_max", self.builder_src)
        self.assertRegex(self.builder_src, r'\bmax\b')

    def test_builder_clamps_score_to_max(self):
        # Score is clamped to the config max via min(..., max).
        self.assertRegex(self.builder_src, r"min\(")

    def test_builder_computes_totals_from_criteria(self):
        # total_score / total_max are summed server-side, not read from the form.
        self.assertRegex(self.builder_src, r'"total_score":\s*total_score')
        self.assertRegex(self.builder_src, r'"total_max":\s*total_max')
        self.assertRegex(self.builder_src, r"total_score\s*=\s*sum\(")
        self.assertRegex(self.builder_src, r"total_max\s*=\s*sum\(")

    def test_builder_does_not_trust_client_total(self):
        self.assertNotIn('form.get("ia_total_score"', self.builder_src)
        self.assertNotIn('form.get("ia_total_max"', self.builder_src)
        self.assertNotIn('form.get("total_score"', self.builder_src)

    def test_wizard_does_not_serialize_ia_totals(self):
        # serializeToForm must never wire a client-computed total/max.
        self.assertNotIn("ia_total_score", self.wizard_js)
        self.assertNotIn("ia_total_max", self.wizard_js)


if __name__ == "__main__":
    unittest.main()
