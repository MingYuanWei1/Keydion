import unittest
from unittest import mock

import llm_client
import llm_metadata
import vision_extractor
from llm_metadata import (
    LLMMetadataError,
    generate_abstract_keywords,
    _complete,
    _pdf_text_from_bytes,
    _normalise_keywords,
    _normalise_authors,
)


class CompleteTest(unittest.TestCase):
    """The text fallback call: wiring and error translation over chat_json."""

    def _complete(self, data, text="paper text", language="en"):
        with mock.patch.object(llm_metadata.llm_client, "chat_json",
                               return_value=data) as cj:
            out = _complete(text, language)
        return cj, out

    def test_parses_clean_result(self):
        _, out = self._complete({"abstract": "An abstract.", "keywords": ["alpha", "beta"]})
        self.assertEqual(out["abstract"], "An abstract.")
        self.assertEqual(out["keywords"], ["alpha", "beta"])
        self.assertEqual(out["warnings"], [])

    def test_normalises_string_keywords(self):
        _, out = self._complete({"abstract": "x", "keywords": "a, b, b, c"})
        self.assertEqual(out["keywords"], ["a", "b", "c"])

    def test_warns_both_when_both_empty(self):
        _, out = self._complete({"abstract": "", "keywords": []})
        self.assertEqual(len(out["warnings"]), 2)

    def test_warns_only_keywords_when_abstract_present(self):
        _, out = self._complete({"abstract": "present", "keywords": []})
        self.assertEqual(len(out["warnings"]), 1)
        self.assertIn("keyword", out["warnings"][0].lower())

    def test_non_string_abstract_is_safe(self):
        _, out = self._complete({"abstract": ["a", "b"], "keywords": ["k"]})
        self.assertEqual(out["abstract"], "")
        self.assertEqual(out["keywords"], ["k"])
        self.assertTrue(any("abstract" in w.lower() for w in out["warnings"]))

    def test_returns_certain_title_and_authors(self):
        _, out = self._complete({"abstract": "a", "keywords": ["k"],
                                 "title": "On Widgets", "authors": ["Ada Lovelace", "Alan Turing"]})
        self.assertEqual(out["title"], "On Widgets")
        self.assertEqual(out["authors"], ["Ada Lovelace", "Alan Turing"])

    def test_omits_uncertain_title_and_authors(self):
        _, out = self._complete({"abstract": "a", "keywords": ["k"]})  # no title/authors
        self.assertEqual(out["title"], "")
        self.assertEqual(out["authors"], [])

    def test_nonstring_title_is_safe(self):
        _, out = self._complete({"abstract": "a", "keywords": ["k"],
                                 "title": ["x"], "authors": "Ada"})
        self.assertEqual(out["title"], "")           # non-string -> dropped
        self.assertEqual(out["authors"], ["Ada"])    # string -> single-item list


class CompleteWiringTest(unittest.TestCase):
    def test_request_crosses_the_conversation_interface(self):
        payload = {"abstract": "x", "keywords": ["k"]}
        with mock.patch.object(llm_metadata.llm_client, "chat_json",
                               return_value=payload) as cj:
            _complete("paper text", "zh")
        kwargs = cj.call_args.kwargs
        messages = cj.call_args.args[0]
        self.assertEqual(kwargs["tier"], "flash")
        self.assertEqual(kwargs["temperature"], 0.2)
        system_msg = messages[0]["content"]
        self.assertEqual(messages[0]["role"], "system")
        self.assertIn("Chinese", system_msg)
        self.assertEqual(messages[1]["content"], "paper text")

    def test_language_en_prompt(self):
        with mock.patch.object(llm_metadata.llm_client, "chat_json",
                               return_value={"abstract": "x"}) as cj:
            _complete("t", "en")
        self.assertIn("English", cj.call_args.args[0][0]["content"])


class CompleteErrorTranslationTest(unittest.TestCase):
    def _translate(self, raised, expected_message):
        with mock.patch.object(llm_metadata.llm_client, "chat_json",
                               side_effect=raised):
            with self.assertRaises(LLMMetadataError) as ctx:
                _complete("t", "en")
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
    def test_empty_bytes_raises(self):
        with self.assertRaises(LLMMetadataError):
            _pdf_text_from_bytes(b"")

    def test_encrypted_mapped_to_llm_error(self):
        import pdf_text
        with mock.patch.object(llm_metadata, "extract_pdf_text",
                               side_effect=pdf_text.PdfTextError("encrypted")):
            with self.assertRaises(LLMMetadataError):
                _pdf_text_from_bytes(b"%PDF-fake")

    def test_corrupt_mapped_to_llm_error(self):
        import pdf_text
        with mock.patch.object(llm_metadata, "extract_pdf_text",
                               side_effect=pdf_text.PdfTextError("corrupt")):
            with self.assertRaises(LLMMetadataError):
                _pdf_text_from_bytes(b"%PDF-fake")

    def test_blank_text_raises(self):
        with mock.patch.object(llm_metadata, "extract_pdf_text", return_value="   "):
            with self.assertRaises(LLMMetadataError):
                _pdf_text_from_bytes(b"%PDF-fake")

    def test_caps_text_to_max(self):
        big = "x" * (llm_metadata.MAX_PDF_CHARS + 500)
        with mock.patch.object(llm_metadata, "extract_pdf_text", return_value=big):
            out = _pdf_text_from_bytes(b"%PDF-fake")
        self.assertEqual(len(out), llm_metadata.MAX_PDF_CHARS)

    def test_language_zh_passes_chinese_ocr_langs(self):
        captured = {}
        def _fake(file_bytes, *, ocr_langs=None, **k):
            captured["langs"] = ocr_langs
            return "some readable text"
        with mock.patch.object(llm_metadata, "extract_pdf_text", side_effect=_fake):
            _pdf_text_from_bytes(b"%PDF-fake", language="zh")
        self.assertIn("chi_sim", captured["langs"])


class HelpersTest(unittest.TestCase):
    def test_normalise_keywords_dedupes_and_caps(self):
        self.assertEqual(
            _normalise_keywords(["a", "a", "b", "c", "d", "e", "f", "g"]),
            ["a", "b", "c", "d", "e", "f"],
        )

    def test_normalise_dedupes_before_capping(self):
        # 10 duplicate 'x' + y + z collapse to x, y, z — proves dedupe runs
        # before the 6-item cap (cap-then-dedupe would drop y and z).
        self.assertEqual(_normalise_keywords(["x"] * 10 + ["y", "z"]), ["x", "y", "z"])

    def test_normalise_preserves_order(self):
        self.assertEqual(_normalise_keywords(["z", "a", "z", "m"]), ["z", "a", "m"])


class GenerateEndToEndTest(unittest.TestCase):
    def test_orchestration(self):
        with mock.patch.object(llm_metadata, "_pdf_text_from_bytes", return_value="text"), \
             mock.patch.object(llm_metadata.llm_client, "chat_json",
                               return_value={"abstract": "done", "keywords": ["x"]}):
            out = generate_abstract_keywords(b"%PDF-fake", "en")
        self.assertEqual(out["abstract"], "done")
        self.assertEqual(out["keywords"], ["x"])


class OcrLangsForTest(unittest.TestCase):
    def test_zh_uses_chi_sim_without_chi_tra(self):
        from llm_metadata import _ocr_langs_for
        langs = _ocr_langs_for("zh")
        self.assertIn("chi_sim", langs)
        self.assertNotIn("chi_tra", langs)   # dropped for speed

    def test_en_uses_eng_only(self):
        from llm_metadata import _ocr_langs_for
        self.assertEqual(_ocr_langs_for("en"), "eng")


class VisionBranchTest(unittest.TestCase):
    def test_uses_vision_when_enabled(self):
        captured = {}
        def _fake_vision(file_bytes, system_prompt, *, max_pages=10, language="en"):
            captured["prompt"] = system_prompt
            captured["language"] = language
            return {"abstract": "  V abstract  ", "keywords": "a, b, b",
                    "title": "  T  ", "authors": "Ada"}
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               side_effect=_fake_vision) as ev:
            out = generate_abstract_keywords(b"%PDF-fake", "zh")
        ev.assert_called_once()
        self.assertEqual(out["abstract"], "V abstract")          # stripped
        self.assertEqual(out["keywords"], ["a", "b"])            # normalised + deduped
        self.assertEqual(out["title"], "T")
        self.assertEqual(out["authors"], ["Ada"])
        self.assertEqual(captured["prompt"], llm_metadata.ABSTRACT_SYSTEM_PROMPT_ZH)
        self.assertEqual(captured["language"], "zh")

    def test_vision_error_falls_back_to_legacy(self):
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               side_effect=vision_extractor.vision_read.VisionError("boom")), \
             mock.patch.object(llm_metadata, "_pdf_text_from_bytes", return_value="text"), \
             mock.patch.object(llm_metadata.llm_client, "chat_json",
                               return_value={"abstract": "L", "keywords": ["k"]}):
            out = generate_abstract_keywords(b"%PDF-fake", "en")
        self.assertEqual(out["abstract"], "L")


if __name__ == "__main__":
    unittest.main()
