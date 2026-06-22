import unittest
from unittest import mock

import vision_extractor
from vision_extractor import VisionFirstExtractor


class _FakeExtractor(VisionFirstExtractor):
    """Records which hooks ran so the skeleton's control flow can be asserted."""
    def __init__(self):
        self.calls = []

    def build_prompt(self):
        self.calls.append("build_prompt")
        return "PROMPT"

    def shape_vision(self, data):
        self.calls.append("shape_vision")
        return {"via": "vision", "data": data}

    def fallback(self, file_bytes):
        self.calls.append("fallback")
        return {"via": "fallback"}

    def post(self, result):
        self.calls.append("post")
        result["posted"] = True
        return result


class VisionFirstSkeletonTest(unittest.TestCase):
    def test_vision_enabled_uses_vision_then_post(self):
        ex = _FakeExtractor()
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               return_value={"k": "v"}) as ev:
            out = ex.extract(b"%PDF-fake")
        ev.assert_called_once()
        self.assertEqual(out["via"], "vision")
        self.assertTrue(out["posted"])                       # post() ran on the vision branch
        self.assertEqual(ex.calls, ["build_prompt", "shape_vision", "post"])

    def test_vision_disabled_uses_fallback_then_post(self):
        ex = _FakeExtractor()
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=False), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision") as ev:
            out = ex.extract(b"%PDF-fake")
        ev.assert_not_called()
        self.assertEqual(out["via"], "fallback")
        self.assertTrue(out["posted"])                       # post() ran on the fallback branch
        self.assertEqual(ex.calls, ["fallback", "post"])

    def test_vision_error_falls_back_and_warns(self):
        ex = _FakeExtractor()
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               side_effect=vision_extractor.vision_read.VisionError("boom")):
            with self.assertLogs("vision_extractor", level="WARNING") as logs:
                out = ex.extract(b"%PDF-fake")
        self.assertEqual(out["via"], "fallback")
        self.assertTrue(any("falling back" in m for m in logs.output))
        # shape_vision is NOT reached: extract_with_vision raises before it
        self.assertEqual(ex.calls, ["build_prompt", "fallback", "post"])


if __name__ == "__main__":
    unittest.main()
