import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PaperModifyContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.template = (ROOT / "templates" / "paper_modify.html").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_modify_template_exposes_editable_ib_ee_and_cp_fields(self):
        required_names = {
            "is_ib_ee",
            "ib_ee_core_subject",
            "ib_total_grade_number",
            "is_cp_paper",
            "cp_global_context",
            "cp_action_type",
        }
        declared_names = set(re.findall(r'\bname="([^"]+)"', self.template))

        self.assertEqual([], sorted(required_names - declared_names))
        self.assertIn('name="ib_crit_{{ letter }}_score"', self.template)
        self.assertIn('name="cp_crit_{{ letter }}_score"', self.template)

    def test_modify_template_recognizes_legacy_sample_author(self):
        self.assertIn("author_name", self.template)
        self.assertIn("IB SAMPLE", self.template)
        self.assertRegex(
            self.template,
            r"meta\.get\('is_ib_sample'.*or.*meta\.get\('author_name'",
        )

    def test_submission_round_trip_preserves_sample_and_cp_fields(self):
        load_submissions = self._find_function("_load_submissions")
        returned_keys = {
            key.value
            for node in ast.walk(load_submissions)
            if isinstance(node, ast.Dict)
            for key in node.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }
        self.assertIn("is_ib_sample", returned_keys)
        self.assertIn("cp_data", returned_keys)

        write_submissions = self._find_function("_write_submissions")
        submission_model_keywords = {
            keyword.arg
            for node in ast.walk(write_submissions)
            if isinstance(node, ast.Call)
            and getattr(node.func, "id", "") == "SubmissionModel"
            for keyword in node.keywords
        }
        self.assertIn("is_ib_sample", submission_model_keywords)
        self.assertIn("cp_data", submission_model_keywords)

        review_accept = self._find_function("review_accept")
        accept_keys = {
            key.value
            for node in ast.walk(review_accept)
            if isinstance(node, ast.Dict)
            for key in node.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }
        self.assertIn("is_ib_sample", accept_keys)
        self.assertIn("cp_data", accept_keys)

    def test_modify_route_persists_ib_sections_without_treating_cp_as_sample(self):
        paper_modify = self._find_function("paper_modify")
        modify_keys = {
            key.value
            for node in ast.walk(paper_modify)
            if isinstance(node, ast.Dict)
            for key in node.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }
        self.assertIn("is_ib_sample", modify_keys)
        self.assertIn("ib_ee_data", modify_keys)
        self.assertIn("cp_data", modify_keys)

        paper_modify_source = ast.get_source_segment(self.app_source, paper_modify)
        self.assertIn("if is_ib_sample:", paper_modify_source)
        self.assertNotIn("if is_ib_sample or is_cp_paper:", paper_modify_source)

    def test_modify_route_preserves_published_at_when_saving(self):
        paper_modify = self._find_function("paper_modify")
        modify_keys = {
            key.value
            for node in ast.walk(paper_modify)
            if isinstance(node, ast.Dict)
            for key in node.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }

        self.assertIn("published_at", modify_keys)

    def _find_function(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        self.fail(f"Could not find function {name}")


if __name__ == "__main__":
    unittest.main()
