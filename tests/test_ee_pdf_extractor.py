import unittest
from pathlib import Path

from ee_pdf_extractor import extract_ee_metadata, EePdfExtractionError

FIXTURES = Path(__file__).resolve().parent / "fixtures"


class ExtractorShapeTest(unittest.TestCase):
    """The public API contract — shape and types — regardless of values."""

    @classmethod
    def setUpClass(cls):
        cls.pdf_bytes = (FIXTURES / "ee_commentary_subject_focused.pdf").read_bytes()

    def test_returns_dict_with_expected_top_level_keys(self):
        result = extract_ee_metadata(self.pdf_bytes)
        self.assertIsInstance(result, dict)
        for key in (
            "core_subject",
            "interdisciplinary_subject",
            "framework",
            "research_question",
            "criteria",
            "holistic_comment",
            "warnings",
        ):
            self.assertIn(key, result, f"missing key: {key}")

    def test_criteria_has_A_through_E(self):
        result = extract_ee_metadata(self.pdf_bytes)
        for letter in ("A", "B", "C", "D", "E"):
            self.assertIn(letter, result["criteria"])
            self.assertIn("score", result["criteria"][letter])
            self.assertIn("comment", result["criteria"][letter])

    def test_warnings_is_a_list(self):
        result = extract_ee_metadata(self.pdf_bytes)
        self.assertIsInstance(result["warnings"], list)


class ExtractorValuesTest(unittest.TestCase):
    """Values extracted from the subject-focused sample PDF."""

    @classmethod
    def setUpClass(cls):
        cls.pdf_bytes = (FIXTURES / "ee_commentary_subject_focused.pdf").read_bytes()
        cls.result = extract_ee_metadata(cls.pdf_bytes)

    def test_core_subject_is_biology(self):
        # 'Biology' appears verbatim in the canonical ee_subjects.json,
        # so subject normalisation must preserve it as-is.
        self.assertEqual(self.result["core_subject"], "Biology")

    def test_interdisciplinary_fields_empty_for_subject_focused(self):
        self.assertEqual(self.result["interdisciplinary_subject"], "")
        self.assertEqual(self.result["framework"], "")

    def test_research_question_extracted(self):
        rq = self.result["research_question"]
        self.assertIn("alcohol production", rq.lower())
        self.assertIn("yeast", rq.lower())
        self.assertIn("fermentation", rq.lower())

    def test_scores_are_4_4_4_6_3(self):
        expected = {"A": 4, "B": 4, "C": 4, "D": 6, "E": 3}
        actual = {k: self.result["criteria"][k]["score"] for k in "ABCDE"}
        self.assertEqual(actual, expected)

    def test_every_criterion_has_a_non_empty_comment(self):
        for letter in "ABCDE":
            comment = self.result["criteria"][letter]["comment"]
            self.assertTrue(comment.strip(), f"criterion {letter} comment is empty")

    def test_holistic_comment_is_non_empty(self):
        self.assertTrue(self.result["holistic_comment"].strip())

    def test_no_warnings_on_clean_extraction(self):
        # All fields parsed cleanly → no warnings.
        self.assertEqual(self.result["warnings"], [])


if __name__ == "__main__":
    unittest.main()
