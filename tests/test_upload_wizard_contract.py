import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import support  # noqa: E402
from services.papers import parse_ib_ee_data_for_form, parse_cp_data_for_form  # noqa: E402


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


class UploadValidatorContractTest(unittest.TestCase):
    """The upload() validator must skip keywords/abstract when EE or CP."""

    @classmethod
    def setUpClass(cls):
        cls.app_source = support.all_sources()

    def test_upload_uses_per_type_required_cascade(self):
        src = support.source_of("upload")
        # New shape: a single `required` list that conditionally includes
        # "keywords" / "abstract" only when neither EE nor CP. Subject category
        # is required for every type except CP papers.
        self.assertIn(
            'required = ["title", "language"] if is_cp_paper else ["title", "category", "language"]',
            src,
        )
        self.assertIn('if not (is_ib_ee or is_cp_paper or is_ia):', src)
        self.assertIn('required += ["keywords", "abstract"]', src)
        self.assertIn('if not (is_ib_sample or is_anonymous):', src)
        self.assertIn(
            'required += ["author_name", "author_email", "author_school"]', src
        )

    def test_upload_handles_anonymous_author_bypass(self):
        src = support.source_of("upload")
        # IB Sample wins if both flags somehow arrive.
        self.assertIn(
            'is_anonymous = not is_ib_sample and request.form.get("is_anonymous") == "1"',
            src,
        )
        self.assertIn("elif is_anonymous:", src)
        # Carried on form_data for re-renders and the wizard boot.
        self.assertIn('"is_anonymous": "1" if is_anonymous else ""', src)
        # Persisted in: draft update, draft create, direct publish intent,
        # pending update, pending create.
        self.assertGreaterEqual(
            src.count('"is_anonymous": form_data.get("is_anonymous", "")'), 4
        )
        self.assertIn('is_anonymous=form_data.get("is_anonymous", "")', src)

    def test_upload_uses_render_helper(self):
        """The 8-way repeated render_template(...) is collapsed into one helper."""
        src = support.source_of("upload")
        # Helper exists and is called from validators.
        self.assertIn("_render_upload(", src)
        # And the helper itself exists.
        helper_src = support.source_of("_render_upload")
        self.assertIn('render_template("upload.html"', helper_src)

    def test_missing_field_messages_table_exists(self):
        # Single source of truth for the flash strings.
        self.assertIn("_MISSING_FIELD_MESSAGES = {", self.app_source)
        for key in ("title", "category", "language", "keywords", "abstract",
                    "author_name", "author_email", "author_school"):
            self.assertIn(f'"{key}":', self.app_source)


class DraftHydrationContractTest(unittest.TestCase):
    """Loading a draft must call parse_*_for_form so EE/CP fields rehydrate."""

    @classmethod
    def setUpClass(cls):
        cls.app_source = support.all_sources()

    def test_draft_get_hydrates_is_anonymous(self):
        marker = 'if request.method == "GET" and draft_id:'
        start = self.app_source.find(marker)
        self.assertNotEqual(start, -1, "draft GET branch not found")
        end = self.app_source.find("return _render_upload(", start)
        slice_ = self.app_source[start:end]
        self.assertIn('"is_anonymous": draft.get("is_anonymous", "")', slice_)

    def test_upload_get_calls_parse_ee_and_parse_cp(self):
        # Locate the draft-load branch inside upload() and confirm both
        # parse helpers are referenced there.
        # The exact slice is between `if request.method == "GET" and draft_id:`
        # and the next `return _render_upload(`.
        marker = 'if request.method == "GET" and draft_id:'
        start = self.app_source.find(marker)
        self.assertNotEqual(start, -1, "draft GET branch not found")
        end = self.app_source.find("return _render_upload(", start)
        slice_ = self.app_source[start:end]
        self.assertIn("parse_ib_ee_data_for_form(", slice_)
        self.assertIn("parse_cp_data_for_form(", slice_)
        # And the parsed dicts get merged into form_data.
        self.assertIn("form_data.update(", slice_)


class WizardBootContractTest(unittest.TestCase):
    """_render_upload must pass a wizard_boot dict with the expected keys."""

    @classmethod
    def setUpClass(cls):
        cls.app_source = support.all_sources()

    def test_render_upload_builds_wizard_boot_with_required_keys(self):
        helper_start = self.app_source.find("def _render_upload(")
        helper_end = self.app_source.find("\n    def ", helper_start + 1)
        if helper_end == -1:
            helper_end = self.app_source.find("\ndef ", helper_start + 1)
        helper_src = self.app_source[helper_start:helper_end]
        for key in (
            '"submit_url":', '"draft_id":', '"form_data":',
            '"paper_categories":', '"ee_subjects":', '"cp_global_contexts":',
            '"cp_action_types":', '"user_key":', '"i18n":',
        ):
            self.assertIn(key, helper_src, f"wizard_boot is missing key {key}")


class PaperTypeHydrationContractTest(unittest.TestCase):
    """Drafts and post-validation re-renders must carry is_ib_ee / is_cp_paper
    so the wizard JS rehydrates the correct paper type."""

    @classmethod
    def setUpClass(cls):
        cls.app_source = support.all_sources()

    def test_parse_ee_sets_is_ib_ee_flag(self):
        out = parse_ib_ee_data_for_form('{"core_subject":"Economics","criteria":{}}')
        self.assertEqual(out.get("is_ib_ee"), "1")

    def test_parse_cp_sets_is_cp_paper_flag(self):
        out = parse_cp_data_for_form('{"global_context":"Fairness","criteria":{}}')
        self.assertEqual(out.get("is_cp_paper"), "1")

    def test_parse_helpers_omit_flags_for_empty_input(self):
        # Don't set is_ib_ee on empty/invalid input — only on valid EE data.
        self.assertEqual(parse_ib_ee_data_for_form(""), {})
        self.assertEqual(parse_ib_ee_data_for_form(None), {})
        self.assertEqual(parse_cp_data_for_form(""), {})
        self.assertEqual(parse_cp_data_for_form(None), {})

    def test_upload_post_sets_is_ib_ee_flag_on_form_data(self):
        # When is_ib_ee=1 in request.form, the POST branch must set
        # form_data["is_ib_ee"] = "1" so a re-render carries the flag.
        marker = 'form_data["ib_ee_data"] = build_ib_ee_data_from_form(request.form)'
        idx = self.app_source.find(marker)
        self.assertNotEqual(idx, -1)
        # Next few lines should contain the flag assignment.
        nearby = self.app_source[idx:idx + 300]
        self.assertIn('form_data["is_ib_ee"] = "1"', nearby)

    def test_upload_post_sets_is_cp_paper_flag_on_form_data(self):
        marker = 'form_data["cp_data"] = build_cp_data_from_form(request.form)'
        idx = self.app_source.find(marker)
        self.assertNotEqual(idx, -1)
        nearby = self.app_source[idx:idx + 300]
        self.assertIn('form_data["is_cp_paper"] = "1"', nearby)


if __name__ == "__main__":
    unittest.main()
