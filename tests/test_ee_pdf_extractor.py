import unittest
from unittest import mock

import ee_pdf_extractor
import llm_client
import vision_extractor
from ee_pdf_extractor import extract_ee_metadata, EePdfExtractionError


def _model_payload():
    """A clean model transcription of a subject-focused commentary form."""
    return {
        "core_subject": "Biology",
        "interdisciplinary_subject": "",
        "research_question": "To what extent does yeast fermentation affect alcohol production?",
        "criteria": {
            "A": {"score": 4, "comment": "Focused RQ."},
            "B": {"score": 4, "comment": "Sound sources."},
            "C": {"score": 4, "comment": "Clear analysis."},
            "D": {"score": 6, "comment": "Balanced discussion."},
            "E": {"score": 3, "comment": "Some reflection."},
        },
        "holistic_comment": "A solid essay.",
        "warnings": [],
    }


class CompleteTest(unittest.TestCase):
    """The text-LLM fallback call: wiring and error translation over chat_json."""

    def _complete(self, data, text="commentary text"):
        with mock.patch.object(ee_pdf_extractor.llm_client, "chat_json",
                               return_value=data) as cj:
            out = ee_pdf_extractor._complete(text)
        return cj, out

    def test_parses_clean_result_into_result_shape(self):
        _, out = self._complete({"core_subject": "Biology",
                                 "criteria": {"A": {"score": 4, "comment": "c"}},
                                 "warnings": []})
        self.assertEqual(out["core_subject"], "Biology")
        self.assertEqual(out["criteria"]["A"], {"score": 4, "comment": "c"})
        for letter in "ABCDE":
            self.assertIn("score", out["criteria"][letter])
            self.assertIn("comment", out["criteria"][letter])

    def test_request_crosses_the_conversation_interface(self):
        cj, _ = self._complete({"core_subject": "Biology"})
        kwargs = cj.call_args.kwargs
        messages = cj.call_args.args[0]
        self.assertEqual(kwargs["tier"], "think")
        self.assertEqual(kwargs["temperature"], 0)
        self.assertEqual(messages[0]["role"], "system")
        self.assertEqual(messages[0]["content"],
                         ee_pdf_extractor.EE_SYSTEM_PROMPT_EN)
        self.assertEqual(messages[1]["content"], "commentary text")

    def test_text_prompt_demands_verbatim_transcription(self):
        # The text fallback reuses the same transcription contract as the
        # vision branch — transcribe, never grade.
        _, _ = self._complete({"core_subject": "Biology"})
        system_msg = ee_pdf_extractor.EE_SYSTEM_PROMPT_EN
        for marker in ("TRANSCRIBE", "WORD-FOR-WORD", "do NOT paraphrase",
                       "never grade the essay yourself", "null"):
            self.assertIn(marker, system_msg)


class CompleteErrorTranslationTest(unittest.TestCase):
    def _translate(self, raised, expected_message):
        with mock.patch.object(ee_pdf_extractor.llm_client, "chat_json",
                               side_effect=raised):
            with self.assertRaises(EePdfExtractionError) as ctx:
                ee_pdf_extractor._complete("t")
        self.assertEqual(str(ctx.exception), expected_message)

    def test_unavailable_not_configured(self):
        self._translate(
            llm_client.LLMChatUnavailable("AI assist is not configured."),
            "AI assist is not configured.",
        )

    def test_unavailable_openai_missing(self):
        self._translate(
            llm_client.LLMChatUnavailable("openai package is not installed."),
            "openai package is not installed.",
        )

    def test_request_error_wrapped(self):
        self._translate(
            llm_client.LLMChatRequestError("AI request failed — please try again later."),
            "AI request failed — please try again later.",
        )

    def test_parse_error_wrapped(self):
        self._translate(
            llm_client.LLMChatParseError("The AI response could not be parsed."),
            "The AI response could not be parsed.",
        )


class PdfTextDelegationTest(unittest.TestCase):
    def test_empty_bytes_raise_extraction_error(self):
        with self.assertRaises(EePdfExtractionError):
            ee_pdf_extractor._pdf_text_from_bytes(b"")

    def test_encrypted_mapped_to_ee_error(self):
        import pdf_text
        with mock.patch.object(ee_pdf_extractor, "extract_pdf_text",
                               side_effect=pdf_text.PdfTextError("encrypted")):
            with self.assertRaises(EePdfExtractionError):
                ee_pdf_extractor._pdf_text_from_bytes(b"%PDF-fake")

    def test_corrupt_mapped_to_ee_error(self):
        import pdf_text
        with mock.patch.object(ee_pdf_extractor, "extract_pdf_text",
                               side_effect=pdf_text.PdfTextError("corrupt")):
            with self.assertRaises(EePdfExtractionError):
                ee_pdf_extractor._pdf_text_from_bytes(b"%PDF-fake")

    def test_blank_text_raises(self):
        with mock.patch.object(ee_pdf_extractor, "extract_pdf_text", return_value="   "):
            with self.assertRaises(EePdfExtractionError):
                ee_pdf_extractor._pdf_text_from_bytes(b"%PDF-fake")

    def test_caps_text_to_max(self):
        big = "x" * (ee_pdf_extractor.MAX_PDF_CHARS + 500)
        with mock.patch.object(ee_pdf_extractor, "extract_pdf_text", return_value=big):
            out = ee_pdf_extractor._pdf_text_from_bytes(b"%PDF-fake")
        self.assertEqual(len(out), ee_pdf_extractor.MAX_PDF_CHARS)


class LegacyPathTest(unittest.TestCase):
    """The fallback body: text read → completion (no client assembly)."""

    def test_orchestration(self):
        with mock.patch.object(ee_pdf_extractor, "_pdf_text_from_bytes", return_value="text"), \
             mock.patch.object(ee_pdf_extractor.llm_client, "chat_json",
                               return_value={"core_subject": "Biology", "warnings": []}) as cj:
            out = ee_pdf_extractor._legacy_extract_ee_metadata(b"%PDF-fake")
        self.assertEqual(out["core_subject"], "Biology")
        self.assertEqual(cj.call_args.args[0][1]["content"], "text")


class FallbackEndToEndTest(unittest.TestCase):
    """Vision-disabled: the façade drives the LLM fallback through post()."""

    def test_full_shape_through_post(self):
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=False), \
             mock.patch.object(ee_pdf_extractor, "_pdf_text_from_bytes", return_value="text"), \
             mock.patch.object(ee_pdf_extractor.llm_client, "chat_json",
                               return_value=_model_payload()):
            result = extract_ee_metadata(b"%PDF-fake")
        for key in ("core_subject", "interdisciplinary_subject", "framework",
                    "research_question", "criteria", "holistic_comment", "warnings"):
            self.assertIn(key, result, f"missing key: {key}")
        self.assertEqual(result["core_subject"], "Biology")   # canonicalised in post()
        self.assertEqual(result["criteria"]["A"]["score"], 4)
        self.assertEqual(result["warnings"], [])              # clean extraction

    def test_lowercase_subject_canonicalised(self):
        payload = _model_payload()
        payload["core_subject"] = "biology"
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=False), \
             mock.patch.object(ee_pdf_extractor, "_pdf_text_from_bytes", return_value="text"), \
             mock.patch.object(ee_pdf_extractor.llm_client, "chat_json",
                               return_value=payload):
            result = extract_ee_metadata(b"%PDF-fake")
        self.assertEqual(result["core_subject"], "Biology")

    def test_not_configured_error_surfaces(self):
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=False), \
             mock.patch.object(ee_pdf_extractor, "_pdf_text_from_bytes", return_value="text"), \
             mock.patch.object(ee_pdf_extractor.llm_client, "chat_json",
                               side_effect=llm_client.LLMChatUnavailable(
                                   "AI assist is not configured.")) as cj:
            with self.assertRaises(EePdfExtractionError) as ctx:
                extract_ee_metadata(b"%PDF-fake")
        self.assertEqual(str(ctx.exception), "AI assist is not configured.")
        cj.assert_called_once()

    def test_pdf_errors_surface_before_the_model_call(self):
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=False), \
             mock.patch.object(ee_pdf_extractor, "_pdf_text_from_bytes",
                               side_effect=EePdfExtractionError("PDF is encrypted")), \
             mock.patch.object(ee_pdf_extractor.llm_client, "chat_json") as cj:
            with self.assertRaises(EePdfExtractionError) as ctx:
                extract_ee_metadata(b"%PDF-fake")
        self.assertEqual(str(ctx.exception), "PDF is encrypted")
        cj.assert_not_called()


class SubjectNormalisationTest(unittest.TestCase):
    """Subjects are matched (case-insensitive exact) against ee_subjects.json."""

    def test_builtin_catalog_is_used_when_runtime_file_is_missing(self):
        from ee_pdf_extractor import _canonical_subjects, _normalise_subject

        _canonical_subjects.cache_clear()
        try:
            with mock.patch.object(__import__("pathlib").Path, "read_text", side_effect=FileNotFoundError):
                self.assertEqual(_normalise_subject("biology")[0], "Biology")
        finally:
            _canonical_subjects.cache_clear()

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


class ExtractorErrorPathsTest(unittest.TestCase):
    def test_empty_bytes_raise_extraction_error(self):
        with self.assertRaises(EePdfExtractionError):
            extract_ee_metadata(b"")

    def test_garbage_bytes_raise_extraction_error(self):
        # Real corrupt bytes: the PDF text read fails before any LLM is needed.
        with self.assertRaises(EePdfExtractionError):
            extract_ee_metadata(b"this is not a pdf, just bytes")


if __name__ == "__main__":
    unittest.main()
