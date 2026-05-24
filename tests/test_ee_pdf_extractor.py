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


if __name__ == "__main__":
    unittest.main()
