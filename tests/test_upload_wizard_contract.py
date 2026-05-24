import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import parse_ib_ee_data_for_form, parse_cp_data_for_form  # noqa: E402


class ParseHelpersContractTest(unittest.TestCase):
    """The wizard hydrates draft EE/CP fields via these helpers."""

    def test_parse_ee_round_trips_canonical_fields(self):
        raw = json.dumps({
            "is_ib_ee": True,
            "core_subject": "Economics",
            "interdisciplinary_subject": "",
            "total_grade_number": "23",
            "holistic_comment": "Strong overall.",
            "criteria": {
                "A": {"label": "Framework", "max": 6, "score": 5, "comment": "good"},
                "B": {"label": "Knowledge", "max": 6, "score": 4, "comment": ""},
                "C": {"label": "Analysis", "max": 6, "score": 5, "comment": ""},
                "D": {"label": "Discussion", "max": 8, "score": 6, "comment": ""},
                "E": {"label": "Reflection", "max": 4, "score": 3, "comment": ""},
            },
        })
        out = parse_ib_ee_data_for_form(raw)
        self.assertEqual(out["ib_ee_core_subject"], "Economics")
        self.assertEqual(out["ib_ee_interdisciplinary_subject"], "")
        self.assertEqual(out["ib_holistic_comment"], "Strong overall.")
        self.assertEqual(out["ib_crit_A_score"], "5")
        self.assertEqual(out["ib_crit_A_comment"], "good")
        self.assertEqual(out["ib_crit_E_score"], "3")

    def test_parse_ee_handles_empty_and_invalid(self):
        self.assertEqual(parse_ib_ee_data_for_form(""), {})
        self.assertEqual(parse_ib_ee_data_for_form("not json"), {})
        self.assertEqual(parse_ib_ee_data_for_form(None), {})

    def test_parse_cp_round_trips_canonical_fields(self):
        raw = json.dumps({
            "is_cp_paper": True,
            "global_context": "Fairness and Development",
            "action_types": ["Direct Service", "Advocacy"],
            "total_score": 6,
            "criteria": {
                "A": {"label": "Investigating", "max": 8, "score": 7},
                "B": {"label": "Planning", "max": 8, "score": 6},
                "C": {"label": "Taking Action", "max": 8, "score": 6},
                "D": {"label": "Reflecting", "max": 8, "score": 5},
            },
        })
        out = parse_cp_data_for_form(raw)
        self.assertEqual(out["cp_global_context"], "Fairness and Development")
        self.assertEqual(out["cp_action_types"], ["Direct Service", "Advocacy"])
        self.assertEqual(out["cp_crit_A_score"], "7")
        self.assertEqual(out["cp_crit_D_score"], "5")

    def test_parse_cp_handles_empty_and_invalid(self):
        self.assertEqual(parse_cp_data_for_form(""), {})
        self.assertEqual(parse_cp_data_for_form("nope"), {})
        self.assertEqual(parse_cp_data_for_form(None), {})


if __name__ == "__main__":
    unittest.main()
