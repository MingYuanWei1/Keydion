import unittest
from unittest import mock

import vision_read
from vision_read import VisionError


class _FakeMessage:
    def __init__(self, content):
        self.content = content


class _FakeChoice:
    def __init__(self, content):
        self.message = _FakeMessage(content)


class _FakeResponse:
    def __init__(self, content):
        self.choices = [_FakeChoice(content)]


class _CapturingClient:
    """Fake OpenAI client whose chat.completions.create records its kwargs."""
    def __init__(self, content='{"ok": true}', error=None):
        self.calls = []
        self._content = content
        self._error = error
        self.chat = mock.Mock()
        self.chat.completions = mock.Mock()
        self.chat.completions.create = self._create

    def _create(self, **kwargs):
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return _FakeResponse(self._content)


def _user_parts(call):
    """The user message's content list from a captured create() call."""
    msgs = call["messages"]
    user = [m for m in msgs if m["role"] == "user"][0]
    return user["content"]


class ExtractWithVisionPayloadTest(unittest.TestCase):
    def test_builds_image_url_parts_and_json_response_format(self):
        client = _CapturingClient(content='{"abstract": "hi", "keywords": ["a"]}')
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=client), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages",
                               return_value=[b"PNG-A", b"PNG-B"]):
            out = vision_read.extract_with_vision(b"%PDF", "TASK PROMPT", language="en")

        self.assertEqual(out, {"abstract": "hi", "keywords": ["a"]})
        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["model"], "vmodel")
        self.assertEqual(call["response_format"], {"type": "json_object"})

        parts = _user_parts(call)
        image_parts = [p for p in parts if p.get("type") == "image_url"]
        self.assertEqual(len(image_parts), 2)  # one per rendered page
        for p in image_parts:
            self.assertTrue(p["image_url"]["url"].startswith("data:image/png;base64,"))
        # The system_prompt text rides along as a text part.
        text_parts = [p for p in parts if p.get("type") == "text"]
        self.assertTrue(any("TASK PROMPT" in p["text"] for p in text_parts))

    def test_tolerant_json_parse_unwraps_prose(self):
        client = _CapturingClient(content='here you go: {"title": "T"} thanks')
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=client), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[b"PNG"]):
            out = vision_read.extract_with_vision(b"%PDF", "P")
        self.assertEqual(out, {"title": "T"})


class ExtractWithVisionFailureTest(unittest.TestCase):
    def test_no_client_raises_vision_error(self):
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=None), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[b"PNG"]):
            with self.assertRaises(VisionError):
                vision_read.extract_with_vision(b"%PDF", "P")

    def test_empty_render_raises_vision_error(self):
        client = _CapturingClient()
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=client), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[]):
            with self.assertRaises(VisionError):
                vision_read.extract_with_vision(b"%PDF", "P")

    def test_provider_error_raises_vision_error(self):
        client = _CapturingClient(error=RuntimeError("rate limited"))
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=client), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[b"PNG"]):
            with self.assertRaises(VisionError):
                vision_read.extract_with_vision(b"%PDF", "P")

    def test_unparseable_response_raises_vision_error(self):
        client = _CapturingClient(content="totally not json")
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=client), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[b"PNG"]):
            with self.assertRaises(VisionError):
                vision_read.extract_with_vision(b"%PDF", "P")


class TranscribePdfTest(unittest.TestCase):
    def test_returns_concatenated_text(self):
        client = _CapturingClient(content="PAGE TEXT")
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=client), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[b"PNG"]):
            out = vision_read.transcribe_pdf(b"%PDF", language="en")
        self.assertIn("PAGE TEXT", out)
        # transcription is plain text, not JSON-mode.
        self.assertNotIn("response_format", client.calls[0])

    def test_empty_render_returns_empty_string(self):
        client = _CapturingClient()
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=client), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[]):
            self.assertEqual(vision_read.transcribe_pdf(b"%PDF"), "")

    def test_no_client_returns_empty_string(self):
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=None), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[b"PNG"]):
            self.assertEqual(vision_read.transcribe_pdf(b"%PDF"), "")

    def test_provider_error_returns_empty_string(self):
        client = _CapturingClient(error=RuntimeError("boom"))
        with mock.patch.object(vision_read.llm_client, "build_vision_client", return_value=client), \
             mock.patch.object(vision_read.llm_client, "vision_model", return_value="vmodel"), \
             mock.patch.object(vision_read.pdf_text, "render_pdf_pages", return_value=[b"PNG"]):
            self.assertEqual(vision_read.transcribe_pdf(b"%PDF"), "")


if __name__ == "__main__":
    unittest.main()
