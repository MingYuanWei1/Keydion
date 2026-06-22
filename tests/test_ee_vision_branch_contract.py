import unittest
from unittest import mock

import ee_pdf_extractor
import vision_extractor
from ee_pdf_extractor import extract_ee_metadata, EePdfExtractionError


class EeVisionBranchTest(unittest.TestCase):
    def _vision_payload(self):
        # 'Biology' is a real canonical subject in data/ee_subjects.json.
        return {
            "core_subject": "Biology",
            "interdisciplinary_subject": "",
            "research_question": "To what extent...?",
            "criteria": {l: {"score": 1, "comment": "c"} for l in "ABCDE"},
            "holistic_comment": "Solid.",
            "warnings": [],
        }

    def test_uses_vision_when_enabled(self):
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               return_value=self._vision_payload()) as ev:
            out = extract_ee_metadata(b"%PDF-fake")
        ev.assert_called_once()
        self.assertEqual(out["core_subject"], "Biology")     # canonicalised in post()
        self.assertEqual(out["research_question"], "To what extent...?")
        self.assertEqual(out["criteria"]["A"]["score"], 1)

    def test_unrecognised_vision_subject_warns(self):
        payload = self._vision_payload()
        payload["core_subject"] = "Underwater Basket Weaving"
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               return_value=payload):
            out = extract_ee_metadata(b"%PDF-fake")
        self.assertEqual(out["core_subject"], "")            # dropped
        self.assertTrue(any("not recognised" in w for w in out["warnings"]))

    def test_vision_error_falls_back_to_legacy(self):
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               side_effect=vision_extractor.vision_read.VisionError("boom")), \
             mock.patch.object(ee_pdf_extractor, "_read_pdf_text", return_value="some text"), \
             mock.patch.object(ee_pdf_extractor, "_extract_via_pdfplumber", return_value=None), \
             mock.patch.object(ee_pdf_extractor, "_extract_via_regex",
                               return_value=ee_pdf_extractor._empty_result()) as regex:
            extract_ee_metadata(b"%PDF-fake")
        regex.assert_called_once()


class EePromptFidelityTest(unittest.TestCase):
    """The EE vision extractor must transcribe the examiner's marks/remarks
    verbatim and leave absent fields blank — it must not grade or paraphrase."""

    def test_prompt_demands_verbatim_extraction(self):
        p = ee_pdf_extractor.EE_SYSTEM_PROMPT_EN
        self.assertIn("TRANSCRIBE", p)
        self.assertIn("WORD-FOR-WORD", p)
        self.assertIn("do NOT paraphrase", p)
        self.assertIn("null", p)
        self.assertIn("never grade the essay yourself", p)


if __name__ == "__main__":
    unittest.main()
