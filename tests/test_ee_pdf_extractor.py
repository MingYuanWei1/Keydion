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


class SubjectNormalisationTest(unittest.TestCase):
    """Subjects are matched (case-insensitive exact) against ee_subjects.json."""

    def test_known_subject_lower_case_normalised(self):
        from ee_pdf_extractor import _normalise_subject

        self.assertEqual(_normalise_subject("biology")[0], "Biology")
        self.assertEqual(_normalise_subject("BIOLOGY")[0], "Biology")
        self.assertEqual(_normalise_subject("Biology")[0], "Biology")

    def test_unknown_subject_returns_blank_and_warning(self):
        from ee_pdf_extractor import _normalise_subject

        value, warning = _normalise_subject("Quantum Underwater Basketweaving")
        self.assertEqual(value, "")
        self.assertIn("Quantum Underwater Basketweaving", warning)

    def test_empty_subject_returns_blank_no_warning(self):
        from ee_pdf_extractor import _normalise_subject

        self.assertEqual(_normalise_subject("")[0], "")
        self.assertIsNone(_normalise_subject("")[1])


class FrameworkWarningTest(unittest.TestCase):
    """Interdisciplinary framework value must produce a guidance warning."""

    def test_framework_value_produces_warning(self):
        from ee_pdf_extractor import _finalise_warnings

        partial = {
            "core_subject": "Biology",
            "interdisciplinary_subject": "",
            "framework": "Culture, language and identity",
            "research_question": "RQ",
            "criteria": {l: {"score": 4, "comment": "c"} for l in "ABCDE"},
            "holistic_comment": "h",
            "warnings": [],
        }
        _finalise_warnings(partial)
        joined = " | ".join(partial["warnings"])
        self.assertIn("framework", joined.lower())
        self.assertIn("Culture, language and identity", joined)

    def test_missing_field_produces_warning(self):
        from ee_pdf_extractor import _finalise_warnings

        partial = {
            "core_subject": "",
            "interdisciplinary_subject": "",
            "framework": "",
            "research_question": "",
            "criteria": {l: {"score": None, "comment": ""} for l in "ABCDE"},
            "holistic_comment": "",
            "warnings": [],
        }
        _finalise_warnings(partial)
        joined = " | ".join(partial["warnings"]).lower()
        self.assertIn("could not extract", joined)


class PdfplumberPathTest(unittest.TestCase):
    """The pdfplumber primary path returns a usable dict for the fixture."""

    @classmethod
    def setUpClass(cls):
        cls.pdf_bytes = (FIXTURES / "ee_commentary_subject_focused.pdf").read_bytes()

    def test_pdfplumber_path_alone_extracts_subject_and_scores(self):
        from ee_pdf_extractor import _extract_via_pdfplumber

        result = _extract_via_pdfplumber(self.pdf_bytes)
        self.assertIsNotNone(result, "pdfplumber path returned None on a clean fixture")
        # Pre-normalisation: raw 'Biology' string from the table cell.
        self.assertIn("Biology", result.get("core_subject", ""))
        # Scores should be parsed for at least the obvious criteria.
        a_score = result.get("criteria", {}).get("A", {}).get("score")
        self.assertEqual(a_score, 4)


class ExtractorErrorPathsTest(unittest.TestCase):
    def test_empty_bytes_raise_extraction_error(self):
        with self.assertRaises(EePdfExtractionError):
            extract_ee_metadata(b"")

    def test_garbage_bytes_raise_extraction_error(self):
        with self.assertRaises(EePdfExtractionError):
            extract_ee_metadata(b"this is not a pdf, just bytes")

    def test_pdf_header_without_content_raises(self):
        # A bare PDF header is enough to start parsing but yields no text.
        # PyPDF2 will likely throw, which we wrap as EePdfExtractionError.
        with self.assertRaises(EePdfExtractionError):
            extract_ee_metadata(b"%PDF-1.4\n%%EOF\n")


if __name__ == "__main__":
    unittest.main()
