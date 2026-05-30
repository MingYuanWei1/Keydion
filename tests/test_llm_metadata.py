import os
import unittest
from unittest import mock

import llm_metadata
from llm_metadata import (
    LLMMetadataError,
    generate_abstract_keywords,
    _complete,
    _build_client,
    _pdf_text_from_bytes,
    _normalise_keywords,
    _parse_json,
)


# ── A minimal fake mirroring client.chat.completions.create(...) ──
class _FakeMessage:
    def __init__(self, content):
        self.content = content


class _FakeChoice:
    def __init__(self, content):
        self.message = _FakeMessage(content)


class _FakeResponse:
    def __init__(self, content):
        self.choices = [_FakeChoice(content)]


class FakeClient:
    def __init__(self, content):
        self._content = content
        self.captured = {}
        # client.chat.completions.create -> self.create
        self.chat = self
        self.completions = self

    def create(self, **kwargs):
        self.captured = kwargs
        return _FakeResponse(self._content)


class _RaisingClient:
    """A client whose create() raises — exercises the network-error path."""
    def __init__(self):
        self.chat = self
        self.completions = self

    def create(self, **kwargs):
        raise RuntimeError("boom")


class _EmptyChoicesResponse:
    choices = []


class _EmptyChoicesClient:
    """A client returning a malformed (empty choices) response."""
    def __init__(self):
        self.chat = self
        self.completions = self

    def create(self, **kwargs):
        return _EmptyChoicesResponse()


class CompleteTest(unittest.TestCase):
    def test_parses_clean_json(self):
        client = FakeClient('{"abstract": "An abstract.", "keywords": ["alpha", "beta"]}')
        out = _complete(client, "paper text", "en")
        self.assertEqual(out["abstract"], "An abstract.")
        self.assertEqual(out["keywords"], ["alpha", "beta"])
        self.assertEqual(out["warnings"], [])

    def test_extracts_json_from_noisy_text(self):
        client = FakeClient('Sure!\n{"abstract": "x", "keywords": ["k1"]}\nHope that helps')
        out = _complete(client, "paper text", "en")
        self.assertEqual(out["abstract"], "x")
        self.assertEqual(out["keywords"], ["k1"])

    def test_normalises_string_keywords(self):
        client = FakeClient('{"abstract": "x", "keywords": "a, b, b, c"}')
        out = _complete(client, "t", "en")
        self.assertEqual(out["keywords"], ["a", "b", "c"])

    def test_warns_both_when_both_empty(self):
        client = FakeClient('{"abstract": "", "keywords": []}')
        out = _complete(client, "t", "en")
        self.assertEqual(len(out["warnings"]), 2)

    def test_warns_only_keywords_when_abstract_present(self):
        client = FakeClient('{"abstract": "present", "keywords": []}')
        out = _complete(client, "t", "en")
        self.assertEqual(len(out["warnings"]), 1)
        self.assertIn("keyword", out["warnings"][0].lower())

    def test_non_object_json_raises(self):
        client = FakeClient('["a", "b"]')   # valid JSON, but not an object
        with self.assertRaises(LLMMetadataError):
            _complete(client, "t", "en")

    def test_non_string_abstract_is_safe(self):
        client = FakeClient('{"abstract": ["a", "b"], "keywords": ["k"]}')
        out = _complete(client, "t", "en")
        self.assertEqual(out["abstract"], "")
        self.assertEqual(out["keywords"], ["k"])
        self.assertTrue(any("abstract" in w.lower() for w in out["warnings"]))

    def test_network_error_wrapped(self):
        # assertLogs both asserts we log server-side AND captures the record,
        # keeping the traceback out of the test console output.
        with self.assertLogs("llm_metadata", level="ERROR"):
            with self.assertRaises(LLMMetadataError):
                _complete(_RaisingClient(), "t", "en")

    def test_malformed_response_raises(self):
        with self.assertRaises(LLMMetadataError):
            _complete(_EmptyChoicesClient(), "t", "en")

    def test_language_zh_in_prompt(self):
        client = FakeClient('{"abstract": "x", "keywords": ["k"]}')
        _complete(client, "t", "zh")
        system_msg = client.captured["messages"][0]["content"]
        self.assertIn("Chinese", system_msg)
        self.assertEqual(client.captured["response_format"], {"type": "json_object"})

    def test_unparseable_response_raises(self):
        client = FakeClient("totally not json")
        with self.assertRaises(LLMMetadataError):
            _complete(client, "t", "en")


class BuildClientTest(unittest.TestCase):
    def test_missing_key_raises(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(LLMMetadataError):
                _build_client()


class _StubPage:
    def __init__(self, text):
        self._text = text

    def extract_text(self):
        return self._text


class _StubReader:
    def __init__(self, *args, encrypted=False, pages=None, **kwargs):
        self.is_encrypted = encrypted
        self.pages = pages or []


class PdfTextTest(unittest.TestCase):
    def test_empty_bytes_raises(self):
        with self.assertRaises(LLMMetadataError):
            _pdf_text_from_bytes(b"")

    def test_encrypted_pdf_raises(self):
        with mock.patch.object(llm_metadata, "PdfReader",
                               return_value=_StubReader(encrypted=True)):
            with self.assertRaises(LLMMetadataError):
                _pdf_text_from_bytes(b"%PDF-1.4 fake")

    def test_no_readable_text_raises(self):
        stub = _StubReader(pages=[_StubPage(""), _StubPage("   ")])
        with mock.patch.object(llm_metadata, "PdfReader", return_value=stub):
            with self.assertRaises(LLMMetadataError):
                _pdf_text_from_bytes(b"%PDF-1.4 fake")

    def test_early_break_caps_text_and_stops_reading(self):
        # Two pages alone exceed MAX_PDF_CHARS; a third sentinel page must never
        # be read. Guards both the early-break and the final [:MAX_PDF_CHARS] cap.
        read_flags = {"sentinel_read": False}

        class _SentinelPage:
            def extract_text(self):
                read_flags["sentinel_read"] = True
                return "should never be reached"

        big = "x" * (llm_metadata.MAX_PDF_CHARS // 2 + 10)
        stub = _StubReader(pages=[_StubPage(big), _StubPage(big), _SentinelPage()])
        with mock.patch.object(llm_metadata, "PdfReader", return_value=stub):
            out = _pdf_text_from_bytes(b"%PDF-1.4 fake")
        self.assertLessEqual(len(out), llm_metadata.MAX_PDF_CHARS)
        self.assertFalse(read_flags["sentinel_read"])


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

    def test_parse_json_none_on_garbage(self):
        self.assertIsNone(_parse_json("no json here"))


class GenerateEndToEndTest(unittest.TestCase):
    def test_orchestration(self):
        client = FakeClient('{"abstract": "done", "keywords": ["x"]}')
        with mock.patch.object(llm_metadata, "_pdf_text_from_bytes", return_value="text"), \
             mock.patch.object(llm_metadata, "_build_client", return_value=client):
            out = generate_abstract_keywords(b"%PDF-fake", "en")
        self.assertEqual(out["abstract"], "done")
        self.assertEqual(out["keywords"], ["x"])


if __name__ == "__main__":
    unittest.main()
