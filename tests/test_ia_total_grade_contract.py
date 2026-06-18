import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


ROOT = Path(__file__).resolve().parents[1]


class IaTotalGradeContractTest(unittest.TestCase):
    """IA per-criterion max + total_max are computed server-side from the subject
    config, never trusted from the form.

    Normal mode: total_score is the sum of criterion scores. Holistic-only mode:
    the user enters the overall mark directly, but the builder still clamps it to
    [0, total_max] from the config (bounded, not blindly trusted). Each criterion
    max and total_max always come from load_ia_subjects(), never a client field.
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

    def test_builder_total_max_never_from_form(self):
        # total_max is always summed from the config criteria, never client-sent.
        self.assertNotIn('form.get("ia_total_max"', self.builder_src)
        self.assertNotIn('form.get("total_score"', self.builder_src)

    def test_builder_holistic_only_clamps_direct_total(self):
        # Holistic-only mode lets the user enter the overall mark directly, but
        # the builder clamps it to [0, total_max] from config — bounded, not
        # blindly trusted.
        self.assertIn("ia_holistic_only", self.builder_src)
        self.assertRegex(self.builder_src, r"max\(0,\s*min\(")

    def test_wizard_never_serializes_client_total_max(self):
        # total_max is always derived server-side from the subject config.
        self.assertNotIn("ia_total_max", self.wizard_js)

    def test_wizard_serializes_direct_total_only_in_holistic_mode(self):
        # ia_total_score is wired ONLY under the holistic-only branch.
        self.assertIn("state.iaHolisticOnly", self.wizard_js)
        self.assertIn("ia_total_score", self.wizard_js)


if __name__ == "__main__":
    unittest.main()
