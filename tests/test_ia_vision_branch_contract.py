import unittest
from unittest import mock

import ia_metadata
import vision_extractor
from ia_metadata import generate_ia_scores


_CRITERIA = [{"name": "Knowledge", "max": 6}, {"name": "Analysis", "max": 8}]


class IaVisionBranchTest(unittest.TestCase):
    def test_uses_vision_when_enabled_and_clamps(self):
        payload = {
            "criteria": [
                {"name": "Knowledge", "score": 99, "comment": "ok"},   # over max -> 6
                {"name": "Analysis", "score": 5, "comment": "ok"},
            ],
            "holistic_comment": "Good.",
            "warnings": [],
        }
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               return_value=payload) as ev:
            out = generate_ia_scores(b"%PDF-fake", "Biology", _CRITERIA, language="en")
        ev.assert_called_once()
        scores = {c["name"]: c["score"] for c in out["criteria"]}
        self.assertEqual(scores["Knowledge"], 6)   # clamped to max
        self.assertEqual(scores["Analysis"], 5)

    def test_vision_error_falls_back_to_legacy(self):
        with mock.patch.object(vision_extractor.llm_client, "vision_enabled", return_value=True), \
             mock.patch.object(vision_extractor.vision_read, "extract_with_vision",
                               side_effect=vision_extractor.vision_read.VisionError("boom")), \
             mock.patch.object(ia_metadata, "_legacy_generate_ia_scores",
                               return_value={"criteria": [], "holistic_comment": "",
                                             "warnings": []}) as legacy:
            generate_ia_scores(b"%PDF-fake", "Biology", _CRITERIA)
        legacy.assert_called_once()


if __name__ == "__main__":
    unittest.main()
