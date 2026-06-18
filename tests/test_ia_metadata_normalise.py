# tests/test_ia_metadata_normalise.py
"""Unit tests for the pure normalisation/parse helpers in ia_metadata.py.

`_normalise_criteria` and `_parse_json` are pure functions with no DB/LLM
dependency, so they import and run standalone. These guard the load-bearing
correctness of the IA extractor: server-side score clamping, missing-criterion
-> BLANK (None) + warning (never fabricated as 0), invented-criterion drop,
null/unreadable-score -> blank, and JSON-in-prose parsing.
"""
import sys
import unittest
from pathlib import Path

# ia_metadata.py lives at the repo root, not in tests/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ia_metadata


CRITERIA = [
    {"name": "Research", "max": 6},
    {"name": "Analysis", "max": 4},
]


class NormaliseCriteriaTest(unittest.TestCase):
    def test_clamps_score_above_max(self):
        out, _ = ia_metadata._normalise_criteria(
            [{"name": "Research", "score": 99}, {"name": "Analysis", "score": 4}],
            CRITERIA,
        )
        self.assertEqual(out[0]["score"], 6)   # clamped 99 -> 6
        self.assertEqual(out[1]["score"], 4)

    def test_clamps_negative_score_to_zero(self):
        out, _ = ia_metadata._normalise_criteria(
            [{"name": "Research", "score": -3}], CRITERIA
        )
        self.assertEqual(out[0]["score"], 0)

    def test_output_has_one_entry_per_input_criterion_in_order(self):
        out, _ = ia_metadata._normalise_criteria(
            [{"name": "Analysis", "score": 2}, {"name": "Research", "score": 1}],
            CRITERIA,
        )
        self.assertEqual([c["name"] for c in out], ["Research", "Analysis"])
        self.assertEqual([c["max"] for c in out], [6, 4])

    def test_missing_criterion_left_blank_with_warning(self):
        out, warnings = ia_metadata._normalise_criteria(
            [{"name": "Research", "score": 5}], CRITERIA
        )
        # Analysis was not returned by the model -> left blank, never fabricated 0.
        self.assertEqual(out[1]["name"], "Analysis")
        self.assertIsNone(out[1]["score"])
        self.assertEqual(out[1]["comment"], "")
        self.assertTrue(any("Analysis" in w for w in warnings))

    def test_invented_criterion_is_dropped(self):
        out, _ = ia_metadata._normalise_criteria(
            [
                {"name": "Research", "score": 3},
                {"name": "Analysis", "score": 2},
                {"name": "Bogus Made-Up Criterion", "score": 99},
            ],
            CRITERIA,
        )
        names = [c["name"] for c in out]
        self.assertEqual(names, ["Research", "Analysis"])
        self.assertNotIn("Bogus Made-Up Criterion", names)

    def test_name_match_is_case_and_whitespace_insensitive(self):
        out, warnings = ia_metadata._normalise_criteria(
            [{"name": "  research  ", "score": 5}, {"name": "ANALYSIS", "score": 3}],
            CRITERIA,
        )
        self.assertEqual(out[0]["score"], 5)
        self.assertEqual(out[1]["score"], 3)
        self.assertEqual(warnings, [])

    def test_unreadable_score_left_blank_with_warning(self):
        out, warnings = ia_metadata._normalise_criteria(
            [{"name": "Research", "score": "not a number"},
             {"name": "Analysis", "score": 2}],
            CRITERIA,
        )
        self.assertIsNone(out[0]["score"])
        self.assertTrue(any("Research" in w and "Unreadable" in w for w in warnings))

    def test_null_score_left_blank_with_warning(self):
        out, warnings = ia_metadata._normalise_criteria(
            [{"name": "Research", "score": None}], CRITERIA
        )
        self.assertIsNone(out[0]["score"])
        self.assertTrue(any("Research" in w and "blank" in w for w in warnings))

    def test_float_score_is_rounded_to_int(self):
        out, _ = ia_metadata._normalise_criteria(
            [{"name": "Research", "score": 3.6}], CRITERIA
        )
        self.assertEqual(out[0]["score"], 4)

    def test_non_string_comment_becomes_empty(self):
        out, _ = ia_metadata._normalise_criteria(
            [{"name": "Research", "score": 2, "comment": 123}], CRITERIA
        )
        self.assertEqual(out[0]["comment"], "")

    def test_returned_not_a_list_leaves_all_blank(self):
        out, warnings = ia_metadata._normalise_criteria(None, CRITERIA)
        self.assertEqual([c["score"] for c in out], [None, None])
        self.assertEqual(len(warnings), 2)


class ParseJsonTest(unittest.TestCase):
    def test_clean_json_object(self):
        self.assertEqual(
            ia_metadata._parse_json('{"a": 1, "b": [2, 3]}'), {"a": 1, "b": [2, 3]}
        )

    def test_json_wrapped_in_prose(self):
        content = 'Here is the assessment:\n{"criteria": [], "holistic_comment": "ok"}\nThanks!'
        self.assertEqual(
            ia_metadata._parse_json(content),
            {"criteria": [], "holistic_comment": "ok"},
        )

    def test_empty_string_returns_none(self):
        self.assertIsNone(ia_metadata._parse_json(""))

    def test_no_json_present_returns_none(self):
        self.assertIsNone(ia_metadata._parse_json("absolutely no json here"))

    def test_malformed_braces_return_none(self):
        self.assertIsNone(ia_metadata._parse_json("{not: valid, json"))


class PromptFidelityTest(unittest.TestCase):
    """The IA extractor must TRANSCRIBE the marker's existing scores/comments
    verbatim and leave anything absent blank — it must not grade or fabricate."""

    def test_vision_prompt_demands_verbatim_extraction(self):
        p = ia_metadata._vision_prompt("Biology", CRITERIA)
        self.assertIn("TRANSCRIBE", p)
        self.assertIn("WORD-FOR-WORD", p)
        self.assertIn("do NOT paraphrase", p)
        self.assertIn("null", p)
        self.assertNotIn("Score it against", p)  # old grade-it-yourself framing gone

    def test_text_prompt_demands_verbatim_extraction(self):
        import inspect
        src = inspect.getsource(ia_metadata._complete)
        self.assertIn("TRANSCRIBE", src)
        self.assertIn("WORD-FOR-WORD", src)
        self.assertIn("do NOT paraphrase", src)
        self.assertNotIn("grading an Internal Assessment", src)  # old framing gone


if __name__ == "__main__":
    unittest.main()
