# Upload Wizard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current single-page Bootstrap upload form at `/dashboard/upload` with a five-step wizard ported from the `Keydion-Submission Page/` mockup, including the per-type required-field changes (no keywords/abstract for IB EE/CP) and full draft round-trip for EE/CP fields.

**Architecture:** Jinja template renders a minimal shell (mount points, hidden form, boot script tag); a single IIFE module (`static/js/upload-wizard.js`) reads `window.WIZARD_BOOT` on init, owns one state object, renders each step, and on submit writes hidden inputs into the existing `<form>` and submits it. No new server endpoints — the existing `POST /dashboard/upload` handler is reused, with its required-field cascade made per-type. CSS scoped under `.kd-upload-wizard` to coexist with the Bootstrap classes still used elsewhere.

**Tech Stack:** Flask + SQLAlchemy + Jinja2 + Flask-Babel + vanilla JS (no build step) + Python `unittest` AST/template contract tests.

**Spec:** `docs/superpowers/specs/2026-05-24-upload-wizard-design.md`

**Mockup source (read-only reference):** `Keydion-Submission Page/` at repo root — full vanilla prototype. The CSS and JS port faithfully from here; deleted in Task 20.

---

## File Structure

### New files
- `static/css/upload.css` — wizard styles, scoped under `.kd-upload-wizard`. ~700 lines.
- `static/js/upload-wizard.js` — wizard module (single IIFE). ~650 lines.
- `tests/test_upload_wizard_contract.py` — AST contract tests for the `upload()` route changes and the new `parse_*_for_form` helpers.
- `tests/test_upload_wizard_dom_contract.py` — Jinja render contract for `templates/upload.html`.

### Modified files
- `app.py` — two new helpers (`parse_ib_ee_data_for_form`, `parse_cp_data_for_form`) next to existing `build_*` functions; `upload()` validator refactored to a per-type required-field cascade; `_render_upload()` helper extracted; `render_template("upload.html", ...)` call sites pass a single `wizard_boot` dict; draft GET branch widens `form_data` via the new parsers.
- `templates/upload.html` — full replacement with the Jinja shell.
- `tests/test_upload_template.py` — updated for new mount IDs.
- `tests/test_ee_total_grade_contract.py` — rewritten to assert new IDs/classes while preserving the "server recomputes EE total; client never submits a total field" invariant.

### Deleted files
- `Keydion-Submission Page/` (entire folder) — design-review material, removed after port.

---

## Phase 0 — Server foundation

The wizard sends the same field names the existing route already accepts. Two server changes are required: per-type required-field validation, and EE/CP draft round-trip. Ship these first so the wizard has a stable target.

### Task 1: Add `parse_ib_ee_data_for_form` and `parse_cp_data_for_form` helpers

**Files:**
- Modify: `app.py` — insert helpers after `build_cp_data_from_form` (currently ends around line 365)
- Test: `tests/test_upload_wizard_contract.py` (new file)

- [ ] **Step 1: Write the failing test**

Create `tests/test_upload_wizard_contract.py`:

```python
import ast
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class ParseHelpersContractTest(unittest.TestCase):
    """The wizard hydrates draft EE/CP fields via these helpers."""

    @classmethod
    def setUpClass(cls):
        # Import lazily so the AST tests below don't pay startup cost twice.
        from app import parse_ib_ee_data_for_form, parse_cp_data_for_form
        cls.parse_ee = parse_ib_ee_data_for_form
        cls.parse_cp = parse_cp_data_for_form

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
        out = self.parse_ee(raw)
        self.assertEqual(out["ib_ee_core_subject"], "Economics")
        self.assertEqual(out["ib_ee_interdisciplinary_subject"], "")
        self.assertEqual(out["ib_holistic_comment"], "Strong overall.")
        self.assertEqual(out["ib_crit_A_score"], "5")
        self.assertEqual(out["ib_crit_A_comment"], "good")
        self.assertEqual(out["ib_crit_E_score"], "3")

    def test_parse_ee_handles_empty_and_invalid(self):
        self.assertEqual(self.parse_ee(""), {})
        self.assertEqual(self.parse_ee("not json"), {})
        self.assertEqual(self.parse_ee(None), {})

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
        out = self.parse_cp(raw)
        self.assertEqual(out["cp_global_context"], "Fairness and Development")
        self.assertEqual(out["cp_action_types"], ["Direct Service", "Advocacy"])
        self.assertEqual(out["cp_crit_A_score"], "7")
        self.assertEqual(out["cp_crit_D_score"], "5")

    def test_parse_cp_handles_empty_and_invalid(self):
        self.assertEqual(self.parse_cp(""), {})
        self.assertEqual(self.parse_cp("nope"), {})
        self.assertEqual(self.parse_cp(None), {})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_upload_wizard_contract.ParseHelpersContractTest -v`

Expected: ImportError — `cannot import name 'parse_ib_ee_data_for_form' from 'app'`.

- [ ] **Step 3: Add the two helpers**

Open `app.py`, find `build_cp_data_from_form` (around line 346). Immediately after its closing `)`, insert:

```python
def parse_ib_ee_data_for_form(json_str) -> dict:
    """Flatten ib_ee_data JSON back into form-style keys for draft hydration.

    Returns {} for missing/invalid input so callers can safely .update() the result.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    out = {
        "ib_ee_core_subject": data.get("core_subject", ""),
        "ib_ee_interdisciplinary_subject": data.get("interdisciplinary_subject", ""),
        "ib_holistic_comment": data.get("holistic_comment", ""),
    }
    for letter, criterion in (data.get("criteria") or {}).items():
        out[f"ib_crit_{letter}_score"] = str(criterion.get("score", ""))
        out[f"ib_crit_{letter}_comment"] = criterion.get("comment", "")
    return out


def parse_cp_data_for_form(json_str) -> dict:
    """Flatten cp_data JSON back into form-style keys for draft hydration.

    Returns {} for missing/invalid input.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    out = {
        "cp_global_context": data.get("global_context", ""),
        "cp_action_types": data.get("action_types") or [],
    }
    for letter, criterion in (data.get("criteria") or {}).items():
        out[f"cp_crit_{letter}_score"] = str(criterion.get("score", ""))
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_upload_wizard_contract.ParseHelpersContractTest -v`

Expected: All four tests PASS.

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_upload_wizard_contract.py
git commit -m "feat(upload): add parse_ib_ee_data_for_form / parse_cp_data_for_form helpers

Wizard draft round-trip needs the inverse of build_*_from_form so that
loaded drafts repopulate the EE/CP fieldsets. Helpers return {} on
missing/invalid JSON so callers can safely .update() the result."
```

---

### Task 2: Refactor `upload()` validator to a per-type required-field cascade

The current cascade (`app.py:1457-1489`) flashes errors for keywords/abstract regardless of paper type. EE/CP submissions no longer need those fields.

**Files:**
- Modify: `app.py` — lines 1457-1489 of `upload()` function; add module-level `_MISSING_FIELD_MESSAGES` dict and `_render_upload` helper.
- Test: `tests/test_upload_wizard_contract.py` (extend existing file)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_upload_wizard_contract.py`:

```python
class UploadValidatorContractTest(unittest.TestCase):
    """The upload() validator must skip keywords/abstract when EE or CP."""

    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _find_function(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        self.fail(f"Could not find function {name}")

    def test_upload_uses_per_type_required_cascade(self):
        upload_fn = self._find_function("upload")
        src = ast.get_source_segment(self.app_source, upload_fn)
        # New shape: a single `required` list that conditionally includes
        # "keywords" / "abstract" only when neither EE nor CP.
        self.assertIn('required = ["title", "category", "language"]', src)
        self.assertIn('if not (is_ib_ee or is_cp_paper):', src)
        self.assertIn('required += ["keywords", "abstract"]', src)
        self.assertIn('if not is_ib_sample:', src)
        self.assertIn(
            'required += ["author_name", "author_email", "author_school"]', src
        )

    def test_upload_uses_render_helper(self):
        """The 8-way repeated render_template(...) is collapsed into one helper."""
        upload_fn = self._find_function("upload")
        src = ast.get_source_segment(self.app_source, upload_fn)
        # Helper exists and is called from validators.
        self.assertIn("_render_upload(", src)
        # And the helper itself exists.
        helper = self._find_function("_render_upload")
        helper_src = ast.get_source_segment(self.app_source, helper)
        self.assertIn('render_template("upload.html"', helper_src)

    def test_missing_field_messages_table_exists(self):
        # Single source of truth for the flash strings.
        self.assertIn("_MISSING_FIELD_MESSAGES = {", self.app_source)
        for key in ("title", "category", "language", "keywords", "abstract",
                    "author_name", "author_email", "author_school"):
            self.assertIn(f'"{key}":', self.app_source)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_upload_wizard_contract.UploadValidatorContractTest -v`

Expected: All three tests FAIL — the cascade, helper, and messages table don't exist yet.

- [ ] **Step 3: Add module-level table and helper**

Open `app.py`. Find the existing `IB_EE_CRITERIA_DEFS = [...]` definition (around line 290). Immediately before it, add the messages table:

```python
_MISSING_FIELD_MESSAGES = {
    "title": "Please enter the paper title",
    "category": "Please select a subject category",
    "language": "Please select a language",
    "keywords": "Please enter keywords",
    "abstract": "Please enter the abstract",
    "author_name": "Please enter the author name",
    "author_email": "Please enter the contact email",
    "author_school": "Please enter the school name",
}
```

Now find the `def upload():` function (around line 1315). At the top of the `paperquery_views()` function scope (or near other module helpers — just *above* `def upload():`), add the render helper:

```python
def _render_upload(user, form_data, draft_id):
    """Render upload.html with the standard kwargs the wizard needs."""
    return render_template(
        "upload.html",
        user=user,
        form_data=form_data,
        journals=get_journal_names(),
        paper_categories=load_paper_categories(),
        ee_subjects=load_ee_subjects(),
        cp_global_contexts=CP_GLOBAL_CONTEXTS,
        cp_action_types=CP_ACTION_TYPES,
        draft_id=draft_id,
    )
```

If you're unsure where module-level definitions can live in `app.py`'s `create_app()` shape, place `_render_upload` immediately before `def upload():` inside the same scope — match what's done for other helpers.

- [ ] **Step 4: Replace the flat validator cascade with the per-type list**

In `app.py`, locate the block in `upload()` that runs from `# 验证必填字段` (line ~1456) through the end of `if not form_data["author_school"]:` (line ~1489). Replace that entire block with:

```python
            # Per-type required-field cascade. Keywords/abstract apply to Standard
            # papers only; author fields are skipped for IB Sample submissions.
            required = ["title", "category", "language"]
            if not (is_ib_ee or is_cp_paper):
                required += ["keywords", "abstract"]
            if not is_ib_sample:
                required += ["author_name", "author_email", "author_school"]

            for field in required:
                if not form_data.get(field):
                    flash(_(_MISSING_FIELD_MESSAGES[field]), "danger")
                    return _render_upload(user, form_data, draft_id)
```

The `is_ib_ee = ...`, `is_cp_paper = ...`, and `is_ib_sample = ...` variables already exist above this block (lines 1348, 1380, 1387) — do not redeclare them.

The EE/CP-specific validators that follow (mutual-exclusivity check, core subject check, global context check, action types check) stay as-is, but replace each of their `render_template("upload.html", ...)` calls with `_render_upload(user, form_data, draft_id)`. There are 4 such calls in lines ~1493-1518; collapse them.

- [ ] **Step 5: Also collapse the GET/render-template at the function's end**

Replace the function's final line (around line 1631):

```python
        return render_template("upload.html", user=user, form_data=form_data, journals=get_journal_names(), paper_categories=load_paper_categories(), ee_subjects=load_ee_subjects(), cp_global_contexts=CP_GLOBAL_CONTEXTS, cp_action_types=CP_ACTION_TYPES, draft_id=request.args.get("draft", ""))
```

with:

```python
        return _render_upload(user, form_data, request.args.get("draft", ""))
```

And the two `flash` + `render_template` blocks earlier (the draft "title required" check at line 1397-1400, and the GET draft-render at line 1340-1342) — also swap them to `_render_upload(...)`.

- [ ] **Step 6: Run all upload-related tests**

Run: `python -m unittest tests.test_upload_wizard_contract -v`

Expected: All three new tests PASS plus the four from Task 1.

Run: `python -m unittest tests.test_upload_template -v`

Expected: PASS (this test only checks declared/referenced IDs; our refactor doesn't touch the template yet).

- [ ] **Step 7: Commit**

```bash
git add app.py tests/test_upload_wizard_contract.py
git commit -m "refactor(upload): per-type required-field cascade

Keywords and abstract are removed for IB EE and IB CP submissions per
the new wizard spec; they remain required for Standard papers. Collapses
the previous 8-way repeated render_template() into a single _render_upload
helper, with flash messages driven by a _MISSING_FIELD_MESSAGES table."
```

---

### Task 3: Widen the draft GET branch to hydrate EE/CP fields

The wizard hydrates from `form_data`; when loading a draft, EE/CP scores must round-trip.

**Files:**
- Modify: `app.py:1325-1342` (GET draft branch in `upload()`)
- Test: `tests/test_upload_wizard_contract.py` (extend existing file)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_upload_wizard_contract.py`:

```python
class DraftHydrationContractTest(unittest.TestCase):
    """Loading a draft must call parse_*_for_form so EE/CP fields rehydrate."""

    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_upload_wizard_contract.DraftHydrationContractTest -v`

Expected: FAIL — `parse_ib_ee_data_for_form(` not yet referenced in the GET branch.

- [ ] **Step 3: Widen the GET draft branch**

In `app.py`, locate the GET draft branch (around line 1325-1342). The current shape is:

```python
        if request.method == "GET" and draft_id:
            draft = _get_submission(draft_id)
            if draft and draft.get("status") == "draft" and draft.get("submitter") == user.get("username", ""):
                form_data = {
                    "title": draft.get("title", ""),
                    ...
                    "published_at": today,
                }
                return render_template(...)
```

Replace it with:

```python
        if request.method == "GET" and draft_id:
            draft = _get_submission(draft_id)
            if draft and draft.get("status") == "draft" and draft.get("submitter") == user.get("username", ""):
                form_data = {
                    "title": draft.get("title", ""),
                    "journal": draft.get("journal", ""),
                    "category": draft.get("category", ""),
                    "language": draft.get("language", ""),
                    "keywords": draft.get("keywords", ""),
                    "abstract": draft.get("abstract", ""),
                    "author_name": draft.get("author_name", ""),
                    "author_email": draft.get("author_email", ""),
                    "author_school": draft.get("author_school", ""),
                    "is_ib_sample": draft.get("is_ib_sample", ""),
                    "ib_ee_data": draft.get("ib_ee_data", ""),
                    "cp_data": draft.get("cp_data", ""),
                    "published_at": today,
                }
                # Hydrate EE/CP fieldsets so the wizard can repopulate them.
                form_data.update(parse_ib_ee_data_for_form(draft.get("ib_ee_data", "")))
                form_data.update(parse_cp_data_for_form(draft.get("cp_data", "")))
                return _render_upload(user, form_data, draft_id)
```

- [ ] **Step 4: Run all upload-related tests**

Run: `python -m unittest tests.test_upload_wizard_contract -v`

Expected: all tests PASS (8 total across the three classes).

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_upload_wizard_contract.py
git commit -m "feat(upload): hydrate EE/CP fields when loading a draft

The wizard reads form_data on init; without this expansion, IB drafts
would come back with blank score tables, subjects, and contexts."
```

---

## Phase 1 — Jinja shell + boot context

The wizard needs a target template. We rewrite `templates/upload.html` to a minimal shell now; the JS lands in Phase 3.

### Task 4: Add `wizard_boot` dict to `_render_upload`

**Files:**
- Modify: `app.py` — `_render_upload` helper added in Task 2

- [ ] **Step 1: Extend `_render_upload` to build and pass `wizard_boot`**

Replace the `_render_upload` body added in Task 2 with:

```python
def _render_upload(user, form_data, draft_id):
    """Render upload.html with the wizard_boot context the JS needs."""
    wizard_boot = {
        "submit_url": url_for("upload"),
        "draft_id": draft_id or "",
        "form_data": form_data,
        "paper_categories": load_paper_categories(),
        "ee_subjects": load_ee_subjects(),
        "cp_global_contexts": CP_GLOBAL_CONTEXTS,
        "cp_action_types": CP_ACTION_TYPES,
        "user_key": user.get("username", ""),
        "i18n": {
            "saving": _("Saving…"),
            "draft_saved_at": _("Draft saved · %(time)s"),
            "no_file_chosen": _("No file chosen"),
            "pdf_only_single": _("PDF only · single file"),
            "choose_file": _("Choose file"),
            "replace_file": _("Replace"),
            "missing_fields_one": _("1 field still needs attention"),
            "missing_fields_many": _("%(n)s fields still need attention"),
            "everything_filled": _("Everything required is filled in."),
            "submit_cta": _("Click Submit Paper below to send your submission for review."),
            "go_to": _("go to %(step)s"),
            "restore_banner_title": _("Unsaved changes from earlier"),
            "restore_banner_body": _("Your last session in this browser had changes you didn't save. Restore them?"),
            "restore_btn": _("Restore"),
            "discard_btn": _("Discard"),
            "edit": _("Edit"),
            "back": _("← Back"),
            "continue": _("Continue →"),
            "submit_paper": _("Submit Paper"),
            "save_draft": _("Save Draft"),
            "add_author": _("+ Add another author"),
            "remove_author": _("Remove author"),
            "step_label": _("Step %(n)s"),
            "not_provided": _("Not provided"),
            "not_chosen": _("Not chosen"),
            "not_written": _("Not written"),
            "no_file_uploaded": _("No file uploaded"),
            "yes_skipped": _("Yes — author info skipped"),
            "no": _("No"),
            "type_standard": _("Independent Research Paper"),
            "type_ee": _("IB Extended Essay"),
            "type_cp": _("IB Community Project"),
            "english": _("English"),
            "chinese": _("Chinese"),
        },
    }
    return render_template(
        "upload.html",
        user=user,
        form_data=form_data,
        journals=get_journal_names(),
        paper_categories=load_paper_categories(),
        ee_subjects=load_ee_subjects(),
        cp_global_contexts=CP_GLOBAL_CONTEXTS,
        cp_action_types=CP_ACTION_TYPES,
        draft_id=draft_id,
        wizard_boot=wizard_boot,
    )
```

- [ ] **Step 2: Append wizard_boot contract test**

Append to `tests/test_upload_wizard_contract.py`:

```python
class WizardBootContractTest(unittest.TestCase):
    """_render_upload must pass a wizard_boot dict with the expected keys."""

    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")

    def test_render_upload_builds_wizard_boot_with_required_keys(self):
        helper_start = self.app_source.find("def _render_upload(")
        helper_end = self.app_source.find("\ndef ", helper_start + 1)
        helper_src = self.app_source[helper_start:helper_end]
        for key in (
            '"submit_url":', '"draft_id":', '"form_data":',
            '"paper_categories":', '"ee_subjects":', '"cp_global_contexts":',
            '"cp_action_types":', '"user_key":', '"i18n":',
        ):
            self.assertIn(key, helper_src, f"wizard_boot is missing key {key}")
```

- [ ] **Step 3: Run all wizard-contract tests**

Run: `python -m unittest tests.test_upload_wizard_contract -v`

Expected: 9 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add app.py tests/test_upload_wizard_contract.py
git commit -m "feat(upload): pass wizard_boot dict from _render_upload

Wizard JS reads window.WIZARD_BOOT on init for categories, EE subjects,
CP contexts, current draft state, and i18n strings. Single source of
truth that the template will tojson-emit into a <script> tag."
```

---

### Task 5: Rewrite `templates/upload.html` as the Jinja shell

**Files:**
- Modify: `templates/upload.html` (full replacement)

- [ ] **Step 1: Replace `templates/upload.html` with the shell**

Open `templates/upload.html` and replace its entire contents with:

```jinja
{% extends "_bare.html" if partial else "_dashboard_shell.html" %}
{% block title %}{{ _('Paper Submission · Keydion') }}{% endblock %}
{% block panel %}
<link rel="stylesheet" href="{{ url_for('static', filename='css/upload.css') }}">

<div class="kd-upload-wizard">

  <div class="panel-head panel-head--upload">
    <div>
      <div class="panel-head__crumb-row">
        <a href="{{ url_for('dashboard') }}" class="panel-head__back">← {{ _('Dashboard') }}</a>
        <span class="panel-head__crumb-sep">/</span>
        <span class="panel-head__crumb">{{ _('Workspace') }}</span>
      </div>
      <h1 class="panel-head__title">{{ _('Paper Submission') }}</h1>
      <p class="panel-head__sub">{{ _('Submit your research for review by the Keydion editorial team.') }}</p>
    </div>
    <div class="panel-head__actions">
      <span class="autosave autosave--idle" id="autosaveIndicator">
        <span class="autosave__dot"></span>
        <span class="autosave__text">{{ _('Draft not yet saved') }}</span>
      </span>
    </div>
  </div>

  <div class="wizard-steps" id="wizardStepper"></div>

  <div id="wizardSteps"></div>

  <div class="wizard-footer" id="wizardFooter"></div>

  <form id="uploadForm" method="post" action="{{ wizard_boot.submit_url }}" enctype="multipart/form-data" style="display:none;">
    <input type="file" name="paper" id="uploadFormFile" accept="application/pdf,.pdf">
    {% if wizard_boot.draft_id %}
    <input type="hidden" name="draft_id" value="{{ wizard_boot.draft_id }}">
    {% endif %}
  </form>

</div>

<script>window.WIZARD_BOOT = {{ wizard_boot | tojson }};</script>
<script src="{{ url_for('static', filename='js/upload-wizard.js') }}"></script>
{% endblock %}
```

Notes:
- The `<form>` is `display:none;` because the wizard renders its own UI; on Submit, the JS writes hidden inputs into this form and calls `form.submit()`. The visible "Choose file" control lives inside the wizard; clicking it programmatically triggers `#uploadFormFile` so the file lives in the form being submitted.
- `extends "_bare.html" if partial else "_dashboard_shell.html"` preserves the partial-content behavior (`X-Partial-Content: 1` header from the dashboard SPA-style nav).

- [ ] **Step 2: Smoke-test the template renders without error**

Run the local dev server:
```bash
./start_local.sh
```
Wait for "Running on http://127.0.0.1:5000". Visit `/dashboard/upload` in a browser. Expected: the panel head ("Paper Submission") appears, but the body is blank (no wizard yet — JS doesn't exist). No 500 error, no Jinja exceptions.

Stop the server (Ctrl-C).

- [ ] **Step 3: Commit**

```bash
git add templates/upload.html
git commit -m "feat(upload): rewrite upload.html as wizard shell

Mount points (#wizardStepper / #wizardSteps / #wizardFooter), a hidden
<form id=uploadForm> that the wizard fills on submit, and a single
<script> tag emitting window.WIZARD_BOOT for the JS to read."
```

---

## Phase 2 — CSS

### Task 6: Port `Keydion-Submission Page/css/upload.css` to `static/css/upload.css`, scoped under `.kd-upload-wizard`

**Files:**
- Create: `static/css/upload.css`
- Read-only source: `Keydion-Submission Page/css/upload.css`

- [ ] **Step 1: Copy the mockup CSS verbatim**

```bash
cp "Keydion-Submission Page/css/upload.css" static/css/upload.css
```

- [ ] **Step 2: Scope every top-level rule under `.kd-upload-wizard`**

The mockup CSS uses generic class names (`.input`, `.select`, `.combobox`, `.chips`, `.field`, etc.) that would collide with Bootstrap classes still used on other pages. Prefix each top-level selector — but **not** rules that target `body`, `:root`, or the floating tweaks panel (which we're not shipping anyway).

For each rule like:

```css
.input { ... }
.input:focus { ... }
.combobox__panel { ... }
.wizard-card { ... }
```

Rewrite as:

```css
.kd-upload-wizard .input { ... }
.kd-upload-wizard .input:focus { ... }
.kd-upload-wizard .combobox__panel { ... }
.kd-upload-wizard .wizard-card { ... }
```

Rules to **delete** entirely (tweaks panel, not shipped per spec §8):
- `.tweaks-panel { ... }` and all `.tweaks-panel__*` rules
- `#tweaksOpenBtn { ... }`

Rules to **leave unscoped** (they belong to the page chrome that lives outside the wizard wrapper):
- None — the new template only puts the panel-head inside `.kd-upload-wizard`, and the autosave pill is also inside. Every rule from the mockup either applies inside the wrapper or is dead.

For `@keyframes` blocks, leave them at the top level — keyframes can't be nested under a class.

If you find media queries like `@media (max-width: 768px) { .input { ... } }`, scope the inner rule the same way: `@media (max-width: 768px) { .kd-upload-wizard .input { ... } }`.

A faster mechanical option, if the file is regular: run a one-shot sed pass, then visually inspect for any rule that wasn't matched (e.g. multi-selector lines `.a, .b { ... }` need both selectors scoped). The file is ~700 lines — a manual review pass after sed is feasible.

```bash
# Optional starting point. Hand-verify after.
python3 - <<'PY'
import re
src = open("static/css/upload.css").read()
def scope(match):
    selectors = match.group(1)
    parts = [s.strip() for s in selectors.split(",")]
    scoped = []
    for p in parts:
        if p.startswith("@") or p.startswith(":root") or p.startswith("body") or p.startswith("from") or p.startswith("to") or p.startswith("0%") or p.startswith("100%"):
            scoped.append(p)
        else:
            scoped.append(f".kd-upload-wizard {p}")
    return ", ".join(scoped) + " {"
out = re.sub(r"^([^{}@\n][^{}]*)\{", scope, src, flags=re.MULTILINE)
open("static/css/upload.css", "w").write(out)
PY
```

After running the script, open `static/css/upload.css` and check:
- No `.tweaks-panel` / `#tweaksOpenBtn` rules remain (delete if present).
- Keyframe inner rules (`from { ... }`, `to { ... }`, `0% { ... }`) are not scoped — those rules live inside `@keyframes` blocks and the regex above doesn't touch them.
- Every other top-level selector starts with `.kd-upload-wizard `.

- [ ] **Step 3: Smoke-test the CSS loads**

Run `./start_local.sh`, visit `/dashboard/upload`. Expected: page renders with the dashboard chrome; no console errors about missing CSS (open DevTools Network tab to confirm `upload.css` returns 200).

Stop the server.

- [ ] **Step 4: Commit**

```bash
git add static/css/upload.css
git commit -m "feat(upload): add upload.css scoped under .kd-upload-wizard

Ported from Keydion-Submission Page/css/upload.css. Top-level rules
prefixed with .kd-upload-wizard so the generic component classes
(.input, .select, .combobox, .chips, .field, etc.) don't collide
with Bootstrap classes still used on other dashboard pages."
```

---

## Phase 3 — Wizard JS

The JS port follows the mockup `Keydion-Submission Page/js/upload.js`. Tasks 7-17 build it up step-by-step so each task is visually verifiable in a browser.

### Task 7: Create `static/js/upload-wizard.js` skeleton

**Files:**
- Create: `static/js/upload-wizard.js`

- [ ] **Step 1: Create the file with the IIFE shell, state, and init**

Create `static/js/upload-wizard.js`:

```javascript
/* =============================================================
   Keydion · Paper-submission wizard
   Single IIFE module. Reads window.WIZARD_BOOT on init.
   ============================================================= */
(function () {
  'use strict';

  if (!window.WIZARD_BOOT) {
    console.error('[upload-wizard] WIZARD_BOOT missing; aborting.');
    return;
  }
  const BOOT = window.WIZARD_BOOT;
  const I18N = BOOT.i18n || {};

  // ─── i18n helper ───────────────────────────────────────────
  function t(key, fallback, vars) {
    let s = (I18N[key] != null ? I18N[key] : fallback) || '';
    if (vars) {
      Object.keys(vars).forEach(k => {
        s = s.replace('%(' + k + ')s', vars[k]);
      });
    }
    return s;
  }

  // ─── State ─────────────────────────────────────────────────
  const fd = BOOT.form_data || {};
  const state = {
    paperType: fd.is_ib_ee ? 'ee' : (fd.is_cp_paper ? 'cp' : (fd.title ? 'standard' : '')),
    title: fd.title || '',
    language: fd.language || '',
    category: fd.category || '',
    keywords: parseKeywords(fd.keywords),
    abstract: fd.abstract || '',
    isIbSample: !!fd.is_ib_sample,
    authors: parseAuthors(fd),
    // EE
    eeCoreSubject: fd.ib_ee_core_subject || '',
    eeInterSubject: fd.ib_ee_interdisciplinary_subject || '',
    eeScores: {
      A: fd.ib_crit_A_score || '', B: fd.ib_crit_B_score || '',
      C: fd.ib_crit_C_score || '', D: fd.ib_crit_D_score || '',
      E: fd.ib_crit_E_score || '',
    },
    eeIncludeComments: !!(fd.ib_crit_A_comment || fd.ib_crit_B_comment || fd.ib_crit_C_comment || fd.ib_crit_D_comment || fd.ib_crit_E_comment || fd.ib_holistic_comment),
    eeComments: {
      A: fd.ib_crit_A_comment || '', B: fd.ib_crit_B_comment || '',
      C: fd.ib_crit_C_comment || '', D: fd.ib_crit_D_comment || '',
      E: fd.ib_crit_E_comment || '', holistic: fd.ib_holistic_comment || '',
    },
    // CP
    cpGlobalContext: fd.cp_global_context || '',
    cpActionTypes: Array.isArray(fd.cp_action_types) ? fd.cp_action_types.slice() : [],
    cpScores: {
      A: fd.cp_crit_A_score || '', B: fd.cp_crit_B_score || '',
      C: fd.cp_crit_C_score || '', D: fd.cp_crit_D_score || '',
    },
    file: null,           // wizard tracks {name, size} only; real input lives in #uploadFormFile
    step: 0,
    visitedSteps: new Set([0]),
    lastModified: Date.now(),
  };

  function parseKeywords(raw) {
    if (!raw) return [];
    if (Array.isArray(raw)) return raw.slice();
    return raw.split(',').map(s => s.trim()).filter(Boolean);
  }
  function parseAuthors(fd) {
    const names = (fd.author_name || '').split(',').map(s => s.trim()).filter(Boolean);
    const emails = (fd.author_email || '').split(',').map(s => s.trim());
    const schools = (fd.author_school || '').split(',').map(s => s.trim());
    if (names.length === 0) return [{ name: '', email: '', school: '' }];
    return names.map((n, i) => ({
      name: n, email: emails[i] || '', school: schools[i] || ''
    }));
  }

  // ─── Step shape (dynamic per type / IB Sample) ─────────────
  function getSteps() {
    const steps = [{ id: 'type', name: t('step_name_type', 'Paper Type') }];
    if (!state.paperType) return steps;
    steps.push({ id: 'metadata', name: t('step_name_metadata', 'Metadata') });
    if (!state.isIbSample) {
      steps.push({ id: 'authors', name: t('step_name_authors', 'Authors') });
    }
    steps.push({ id: 'file', name: t('step_name_file', 'File') });
    steps.push({ id: 'review', name: t('step_name_review', 'Review') });
    return steps;
  }

  // ─── DOM refs ──────────────────────────────────────────────
  let stepperEl, stepsContainer, footerEl, autosaveEl;

  // ─── Render orchestration ──────────────────────────────────
  function render() {
    renderStepper();
    renderStep();
    renderFooter();
  }

  function renderStepper() {
    const steps = getSteps();
    stepperEl.innerHTML = '';
    steps.forEach((step, idx) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'wizard-step';
      if (idx === state.step) btn.classList.add('is-current');
      if (idx < state.step) btn.classList.add('is-done');
      btn.innerHTML = `
        <span class="wizard-step__num">${idx < state.step ? '✓' : idx + 1}</span>
        <span class="wizard-step__label">
          <span class="wizard-step__crumb">${t('step_label', 'Step %(n)s', { n: idx + 1 })}</span>
          <span class="wizard-step__name">${esc(step.name)}</span>
        </span>
      `;
      btn.addEventListener('click', () => {
        if (state.visitedSteps.has(idx) || idx <= state.step) goToStep(idx);
      });
      stepperEl.appendChild(btn);
    });
  }

  function renderStep() {
    const step = getSteps()[state.step];
    if (!step) { state.step = 0; renderStep(); return; }
    let html = '';
    switch (step.id) {
      case 'type': html = '<div class="wizard-card"><p>Step 1 placeholder</p></div>'; break;
      case 'metadata': html = '<div class="wizard-card"><p>Step 2 placeholder</p></div>'; break;
      case 'authors': html = '<div class="wizard-card"><p>Step 3 placeholder</p></div>'; break;
      case 'file': html = '<div class="wizard-card"><p>Step 4 placeholder</p></div>'; break;
      case 'review': html = '<div class="wizard-card"><p>Step 5 placeholder</p></div>'; break;
    }
    stepsContainer.innerHTML = html;
    // bindStep(step.id) — added in later tasks
  }

  function renderFooter() {
    const steps = getSteps();
    const isLast = state.step === steps.length - 1;
    const isFirst = state.step === 0;
    const nextLabel = isLast ? t('submit_paper', 'Submit Paper') : t('continue', 'Continue →');
    footerEl.innerHTML = `
      <div class="wizard-footer__left">
        ${!isFirst ? `<button type="button" class="btn btn--ghost" id="backBtn">${t('back', '← Back')}</button>` : ''}
      </div>
      <div class="wizard-footer__right">
        <button type="button" class="btn btn--text" id="saveBtn">${t('save_draft', 'Save Draft')}</button>
        <button type="button" class="btn btn--primary" id="nextBtn" ${state.step === 0 && !state.paperType ? 'disabled' : ''}>${nextLabel}</button>
      </div>
    `;
    const back = footerEl.querySelector('#backBtn');
    if (back) back.addEventListener('click', () => goToStep(state.step - 1));
    const next = footerEl.querySelector('#nextBtn');
    if (next) next.addEventListener('click', () => {
      if (isLast) { /* submit hooked up in later task */ }
      else goToStep(state.step + 1);
    });
    // saveBtn hooked up in Task 14
  }

  function goToStep(idx) {
    const steps = getSteps();
    if (idx < 0 || idx >= steps.length) return;
    state.step = idx;
    state.visitedSteps.add(idx);
    render();
    const main = document.getElementById('dashboardMain');
    if (main) main.scrollTo({ top: 0, behavior: 'smooth' });
  }

  // ─── Helpers ───────────────────────────────────────────────
  function esc(s) {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
  }
  function formatBytes(b) {
    if (b < 1024) return b + ' B';
    if (b < 1024 * 1024) return (b / 1024).toFixed(1) + ' KB';
    return (b / 1024 / 1024).toFixed(2) + ' MB';
  }

  // ─── Init ──────────────────────────────────────────────────
  function init() {
    stepperEl = document.getElementById('wizardStepper');
    stepsContainer = document.getElementById('wizardSteps');
    footerEl = document.getElementById('wizardFooter');
    autosaveEl = document.getElementById('autosaveIndicator');
    if (!stepperEl || !stepsContainer || !footerEl) {
      console.error('[upload-wizard] mount points missing');
      return;
    }
    render();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Expose for debugging only
  window.__uploadWizard = { state, goToStep, render };
})();
```

- [ ] **Step 2: Smoke-test in browser**

Run `./start_local.sh`, visit `/dashboard/upload`. Expected:
- Stepper shows "Step 1 · Paper Type" only (no other steps yet because `paperType` is unset).
- The body shows "Step 1 placeholder" inside a wizard-card.
- Footer shows Save Draft + a disabled "Continue →" button.

Open DevTools console: no errors.

Stop the server.

- [ ] **Step 3: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload): wizard JS skeleton (IIFE, state, render, goToStep)

Mounts on #wizardStepper / #wizardSteps / #wizardFooter, hydrates state
from window.WIZARD_BOOT.form_data, and renders placeholders for each
of the five steps. Subsequent tasks fill in each step renderer."
```

---

### Task 8: Render Step 1 — Paper Type

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add `renderType()` and `bindType()`**

Locate the `renderStep()` switch in `upload-wizard.js`. Replace the placeholder line for `case 'type':` with `html = renderType(); break;` and add a `bindStep(step.id)` call after the `stepsContainer.innerHTML = html;` line. The `renderStep()` should now look like:

```javascript
  function renderStep() {
    const step = getSteps()[state.step];
    if (!step) { state.step = 0; renderStep(); return; }
    let html = '';
    switch (step.id) {
      case 'type': html = renderType(); break;
      case 'metadata': html = '<div class="wizard-card"><p>Step 2 placeholder</p></div>'; break;
      case 'authors': html = '<div class="wizard-card"><p>Step 3 placeholder</p></div>'; break;
      case 'file': html = '<div class="wizard-card"><p>Step 4 placeholder</p></div>'; break;
      case 'review': html = '<div class="wizard-card"><p>Step 5 placeholder</p></div>'; break;
    }
    stepsContainer.innerHTML = html;
    bindStep(step.id);
  }

  function bindStep(id) {
    if (id === 'type') bindType();
  }
```

After the `goToStep()` function, add:

```javascript
  // ─── Step 1: Paper Type ────────────────────────────────────
  function renderType() {
    return `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: 1 })} · ${t('choose_paper_type', 'Choose paper type')}</div>
          <h2 class="wizard-card__title">${t('what_kind', 'What kind of paper are you submitting?')}</h2>
          <p class="wizard-card__sub">${t('what_kind_sub', "The fields you'll be asked for next depend on this. You can come back and change it before submitting.")}</p>
        </div>
        <div class="type-grid">
          ${renderTypeCard('standard',
            t('type_tag_standard', 'Independent Research'),
            t('type_title_standard', 'Standard Paper'),
            t('type_body_standard', 'A self-directed research paper, conference paper, or article that is not part of the IB Diploma framework.'),
            t('type_meta_standard', 'Title · authors · abstract · subject'))}
          ${renderTypeCard('ee',
            t('type_tag_ee', 'IB Diploma'),
            t('type_title_ee', 'Extended Essay (EE)'),
            t('type_body_ee', 'A 4,000-word IB Diploma research essay with structured criterion scores (A–E) and an EE subject from the six IB subject groups.'),
            t('type_meta_ee', 'Research Question · EE subject · criterion scores A–E'))}
          ${renderTypeCard('cp',
            t('type_tag_cp', 'IB Diploma'),
            t('type_title_cp', 'Community Project (CP)'),
            t('type_body_cp', 'An IB MYP Community Project graded against Criteria A–D, with a Global Context and a chosen type of action.'),
            t('type_meta_cp', 'Title · Global Context · type of action · criteria A–D'))}
        </div>
      </div>
    `;
  }

  function renderTypeCard(value, tag, title, body, meta) {
    const selected = state.paperType === value;
    return `
      <button type="button" class="type-card ${selected ? 'is-selected' : ''}" data-type="${value}">
        <span class="type-card__radio"></span>
        <span class="type-card__tag">${esc(tag)}</span>
        <h3 class="type-card__title">${esc(title)}</h3>
        <p class="type-card__body">${esc(body)}</p>
        <div class="type-card__meta">${esc(meta)}</div>
      </button>
    `;
  }

  function bindType() {
    stepsContainer.querySelectorAll('.type-card').forEach(card => {
      card.addEventListener('click', () => {
        state.paperType = card.dataset.type;
        if (state.paperType === 'standard') state.isIbSample = false;
        touch();
        render();
      });
    });
  }

  // ─── Mutation marker (used later by localStorage mirror) ───
  function touch() { state.lastModified = Date.now(); }
```

- [ ] **Step 2: Smoke-test**

Run `./start_local.sh`, visit `/dashboard/upload`. Click each of the three type cards in turn. Expected:
- The clicked card highlights.
- The stepper expands to show all 5 step labels.
- Step content for steps 2–5 is still placeholder text but the stepper navigation works (click each step).
- Clicking step 1 again still shows the three type cards.

Stop the server.

- [ ] **Step 3: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload): render Step 1 (Paper Type) with three type cards"
```

---

### Task 9: Render Step 2 — Metadata (Standard fields)

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add `renderMetadata`, `renderChips`, `bindMetadata` (Standard-only path)**

Replace the `case 'metadata':` line in the switch with `html = renderMetadata(); break;`.

Extend `bindStep`:

```javascript
  function bindStep(id) {
    if (id === 'type') bindType();
    if (id === 'metadata') bindMetadata();
  }
```

After `bindType()`, add:

```javascript
  // ─── Step 2: Metadata ──────────────────────────────────────
  function renderMetadata() {
    const isEE = state.paperType === 'ee';
    const isCP = state.paperType === 'cp';
    const isIbType = isEE || isCP;
    const titleLabel = isEE ? t('research_question', 'Research Question') : t('paper_title', 'Paper Title');
    const titlePlaceholder = isEE
      ? t('research_question_ph', 'e.g. To what extent did monetary policy contribute to the 2008 financial crisis?')
      : t('paper_title_ph', 'Enter the complete paper title');
    const head = isEE ? t('tell_us_ee', 'Tell us about your essay')
      : isCP ? t('tell_us_cp', 'Tell us about your community project')
      : t('tell_us_std', 'Tell us about your paper');
    const sub = isIbType
      ? t('metadata_sub_ib', 'IB grading information and bibliographic details for the submission.')
      : t('metadata_sub_std', 'Bibliographic information that will appear on the public paper page.');

    return `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: 2 })} · ${t('paper_details', 'Paper details')}</div>
          <h2 class="wizard-card__title">${esc(head)}</h2>
          <p class="wizard-card__sub">${esc(sub)}</p>
        </div>

        <div class="section-sub">${t('bibliographic', 'Bibliographic')} <span class="req">*</span></div>
        <div class="form-grid">
          <div class="field">
            <label class="field__label" for="f-title">${esc(titleLabel)} <span class="req">*</span></label>
            <input class="input" type="text" id="f-title" value="${esc(state.title)}" placeholder="${esc(titlePlaceholder)}">
          </div>

          <div class="field field--6">
            <label class="field__label">${t('language', 'Language')} <span class="req">*</span></label>
            <div class="segmented" role="radiogroup">
              <button type="button" class="segmented__opt ${state.language === 'en' ? 'is-active' : ''}" data-lang="en">${t('english', 'English')}</button>
              <button type="button" class="segmented__opt ${state.language === 'zh' ? 'is-active' : ''}" data-lang="zh">${t('chinese', 'Chinese')}</button>
            </div>
          </div>

          <div class="field field--6">
            <label class="field__label" for="f-category">${t('subject_category', 'Subject Category')} <span class="req">*</span></label>
            <select class="select" id="f-category">
              <option value="">${t('choose_category', 'Choose a subject category…')}</option>
              ${(BOOT.paper_categories || []).map(c => {
                const value = typeof c === 'string' ? c : c.value;
                const label = typeof c === 'string' ? c : c.label;
                return `<option value="${esc(value)}" ${state.category === value ? 'selected' : ''}>${esc(label)}</option>`;
              }).join('')}
            </select>
          </div>

          ${!isIbType ? `
          <div class="field">
            <label class="field__label" for="f-keywords">${t('keywords', 'Keywords')} <span class="req">*</span></label>
            <div class="chips" id="chipsContainer">
              ${state.keywords.map((kw, i) => `<span class="chip">${esc(kw)}<button type="button" class="chip__x" data-i="${i}">×</button></span>`).join('')}
              <input class="chips__input" id="f-keywords" type="text" placeholder="${state.keywords.length ? t('add_another', 'Add another…') : t('keyword_ph', 'Type a keyword and press Enter')}">
            </div>
            <div class="field__hint field__hint--inline">
              <span class="field__hint">${t('keyword_hint', 'Press Enter or comma to add. Aim for 3–6 keywords.')}</span>
              <span class="field__count">${state.keywords.length} ${t('added', 'added')}</span>
            </div>
          </div>

          <div class="field">
            <label class="field__label" for="f-abstract">${t('abstract', 'Abstract')} <span class="req">*</span></label>
            <textarea class="textarea" id="f-abstract" rows="6" placeholder="${t('abstract_ph', 'Briefly describe your research background, methods, and conclusions…')}">${esc(state.abstract)}</textarea>
            <div class="field__hint field__hint--inline">
              <span class="field__hint">${t('abstract_hint', 'A short summary that appears in search results.')}</span>
              <span class="field__count" id="abstractCount">${state.abstract.length} / 2000</span>
            </div>
          </div>
          ` : ''}

          ${isIbType ? `
            <div class="field">
              <label class="checkfield">
                <input type="checkbox" id="f-ibsample" ${state.isIbSample ? 'checked' : ''}>
                <span class="checkfield__body">
                  <span class="checkfield__title">${t('is_ib_sample', 'This is an IB Sample Paper')}</span>
                  <span class="checkfield__hint">${t('is_ib_sample_hint', 'Sample papers are reference essays without an identified author. Checking this will skip the Authors step.')}</span>
                </span>
              </label>
            </div>
          ` : ''}
        </div>

        ${isEE ? '<!-- EE fieldset (Task 10) -->' : ''}
        ${isCP ? '<!-- CP fieldset (Task 11) -->' : ''}
      </div>
    `;
  }

  function bindMetadata() {
    const titleEl = stepsContainer.querySelector('#f-title');
    if (titleEl) titleEl.addEventListener('input', e => { state.title = e.target.value; touch(); });

    stepsContainer.querySelectorAll('[data-lang]').forEach(b => {
      b.addEventListener('click', () => {
        state.language = b.dataset.lang;
        stepsContainer.querySelectorAll('[data-lang]').forEach(x => x.classList.toggle('is-active', x.dataset.lang === state.language));
        touch();
      });
    });

    const catEl = stepsContainer.querySelector('#f-category');
    if (catEl) catEl.addEventListener('change', e => { state.category = e.target.value; touch(); });

    const abstractEl = stepsContainer.querySelector('#f-abstract');
    const abstractCount = stepsContainer.querySelector('#abstractCount');
    if (abstractEl) abstractEl.addEventListener('input', e => {
      state.abstract = e.target.value;
      if (abstractCount) abstractCount.textContent = `${state.abstract.length} / 2000`;
      touch();
    });

    const chipsContainer = stepsContainer.querySelector('#chipsContainer');
    const chipsInput = stepsContainer.querySelector('#f-keywords');
    if (chipsInput) {
      chipsInput.addEventListener('keydown', e => {
        if (e.key === 'Enter' || e.key === ',') {
          e.preventDefault();
          const val = chipsInput.value.trim().replace(/,$/, '');
          if (val) {
            state.keywords.push(val);
            chipsInput.value = '';
            renderStep();
            const fresh = stepsContainer.querySelector('#f-keywords');
            if (fresh) fresh.focus();
            touch();
          }
        } else if (e.key === 'Backspace' && chipsInput.value === '' && state.keywords.length) {
          state.keywords.pop();
          renderStep();
          const fresh = stepsContainer.querySelector('#f-keywords');
          if (fresh) fresh.focus();
          touch();
        }
      });
    }
    if (chipsContainer) {
      chipsContainer.querySelectorAll('.chip__x').forEach(x => {
        x.addEventListener('click', () => {
          state.keywords.splice(parseInt(x.dataset.i, 10), 1);
          renderStep();
          touch();
        });
      });
    }

    const ibSampleEl = stepsContainer.querySelector('#f-ibsample');
    if (ibSampleEl) ibSampleEl.addEventListener('change', e => {
      state.isIbSample = e.target.checked;
      render();   // re-render stepper too (Authors step appears/disappears)
      touch();
    });
  }
```

- [ ] **Step 2: Smoke-test**

Restart the dev server. Visit `/dashboard/upload`. Choose "Standard Paper", click Continue. Expected:
- Step 2 shows Title, Language (segmented English/Chinese), Subject Category dropdown, Keywords chips, Abstract textarea.
- Typing in Keywords + Enter adds a chip; backspace on empty input removes the last chip; clicking × on a chip removes it.
- Language segmented buttons toggle.
- Abstract character count updates as you type.

Click step 1, switch to "Extended Essay", click Continue. Expected:
- Step 2 shows Title (labeled "Research Question"), Language, Category, **no** Keywords, **no** Abstract, **and** the "IB Sample Paper" checkbox.
- Checking IB Sample removes "Authors" from the stepper.

Same for CP.

Stop the server.

- [ ] **Step 3: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload): render Step 2 metadata (Standard fields + IB Sample toggle)

EE fieldset and CP fieldset are inserted in the next two tasks."
```

---

### Task 10: Render Step 2 — EE fieldset (combobox, criteria table, total readout, optional comments)

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add EE fieldset renderer + combobox component + EE binder**

In `renderMetadata`, replace the line `${isEE ? '<!-- EE fieldset (Task 10) -->' : ''}` with `${isEE ? renderEEFieldset() : ''}`.

After `bindMetadata()`, add:

```javascript
  // ─── EE fieldset ───────────────────────────────────────────
  function renderEEFieldset() {
    const total = sumScores(state.eeScores);
    const criteria = [
      ['A', t('crit_ee_A', 'Framework for the essay'), 6],
      ['B', t('crit_ee_B', 'Knowledge and understanding'), 6],
      ['C', t('crit_ee_C', 'Analysis and line of argument'), 6],
      ['D', t('crit_ee_D', 'Discussion and evaluation'), 8],
      ['E', t('crit_ee_E', 'Reflection'), 4],
    ];
    return `
      <div class="section-sub">${t('ee_subject', 'EE Subject')} <span class="req">*</span></div>
      <div class="form-grid">
        <div class="field field--6">
          <label class="field__label">${t('core_subject', 'Core Subject')} <span class="req">*</span></label>
          ${renderCombobox('ee-core', state.eeCoreSubject, t('select_core', 'Select a core subject…'), (BOOT.ee_subjects && BOOT.ee_subjects.groups) || [])}
        </div>
        <div class="field field--6">
          <label class="field__label">${t('inter_subject', 'Interdisciplinary Subject')} <span class="opt">${t('optional', 'Optional')}</span></label>
          ${renderCombobox('ee-inter', state.eeInterSubject, t('select_inter', 'Optional — select if applicable…'), (BOOT.ee_subjects && BOOT.ee_subjects.groups) || [])}
        </div>
      </div>

      <div class="section-sub">${t('crit_scores', 'Criterion Scores')} <span class="req">*</span></div>
      <table class="crit-table" id="eeCriteria">
        <thead><tr><th>${t('crit', 'Crit.')}</th><th>${t('criterion', 'Criterion')}</th><th style="width:140px;">${t('score', 'Score')}</th></tr></thead>
        <tbody>
          ${criteria.map(([k, name, max]) => `
            <tr>
              <td class="crit-letter">${k}</td>
              <td class="crit-name">${esc(name)}</td>
              <td class="crit-score">
                <span class="crit-score__input">
                  <input type="number" min="0" max="${max}" value="${esc(state.eeScores[k])}" data-crit="${k}" placeholder="0">
                  <span class="crit-score__max">/ ${max}</span>
                </span>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>

      <div class="total-readout">
        <div>
          <div class="total-readout__label">${t('overall_grade', 'Overall Grade')}</div>
          <div class="total-readout__sub">${t('overall_ee_sub', 'Calculated server-side from the criteria above')}</div>
        </div>
        <div class="total-readout__value"><span id="eeTotal">${total}</span><small>/ 30</small></div>
      </div>

      <div class="section-sub" style="margin-top:28px;">${t('crit_comments', 'Criterion Commentaries')} <span class="opt">${t('optional', 'Optional')}</span></div>
      <label class="checkfield">
        <input type="checkbox" id="eeIncComments" ${state.eeIncludeComments ? 'checked' : ''}>
        <span class="checkfield__body">
          <span class="checkfield__title">${t('include_comments', 'Include commentaries for all criteria')}</span>
          <span class="checkfield__hint">${t('include_comments_hint', 'Provide short remarks on each criterion plus an optional overall holistic commentary.')}</span>
        </span>
      </label>
      <div id="eeCommentsBox" class="${state.eeIncludeComments ? '' : 'is-hidden'}" style="margin-top:16px;">
        ${criteria.map(([k, name]) => `
          <div class="field" style="margin-bottom:14px;">
            <label class="field__label">${t('crit', 'Crit.')} ${k} — ${esc(name)}</label>
            <textarea class="textarea" rows="2" data-comment="${k}" placeholder="${t('crit_comment_ph', 'Commentary for Criterion %(k)s…', { k: k })}">${esc(state.eeComments[k] || '')}</textarea>
          </div>
        `).join('')}
        <div class="field">
          <label class="field__label">${t('holistic_comment', 'Holistic Commentary')} <span class="opt">${t('optional', 'Optional')}</span></label>
          <textarea class="textarea" rows="3" data-comment="holistic" placeholder="${t('holistic_ph', 'An overall holistic commentary for the essay…')}">${esc(state.eeComments.holistic || '')}</textarea>
        </div>
      </div>
    `;
  }

  function sumScores(obj) {
    return Object.values(obj).reduce((s, v) => s + (parseInt(v, 10) || 0), 0);
  }
```

Now extend `bindMetadata` so it wires the EE-specific controls. At the end of the existing `bindMetadata()` function, before its closing `}`, add:

```javascript
    if (state.paperType === 'ee') bindEEFieldset();
    if (state.paperType === 'cp') bindCPFieldset();   // hooked up in Task 11
    bindComboboxes();                                  // operates on all comboboxes rendered in this step
```

Append `bindEEFieldset`:

```javascript
  function bindEEFieldset() {
    stepsContainer.querySelectorAll('#eeCriteria input[data-crit]').forEach(inp => {
      inp.addEventListener('input', e => {
        const k = inp.dataset.crit;
        const max = parseInt(inp.max, 10);
        let v = parseInt(e.target.value, 10);
        if (!isNaN(v)) { if (v < 0) v = 0; if (v > max) v = max; }
        state.eeScores[k] = isNaN(v) ? '' : String(v);
        const totalEl = stepsContainer.querySelector('#eeTotal');
        if (totalEl) totalEl.textContent = sumScores(state.eeScores);
        touch();
      });
    });
    const inc = stepsContainer.querySelector('#eeIncComments');
    if (inc) inc.addEventListener('change', e => {
      state.eeIncludeComments = e.target.checked;
      const box = stepsContainer.querySelector('#eeCommentsBox');
      if (box) box.classList.toggle('is-hidden', !state.eeIncludeComments);
      touch();
    });
    stepsContainer.querySelectorAll('#eeCommentsBox textarea[data-comment]').forEach(ta => {
      ta.addEventListener('input', e => {
        state.eeComments[ta.dataset.comment] = e.target.value;
        touch();
      });
    });
  }
```

And the combobox component:

```javascript
  // ─── Combobox component ────────────────────────────────────
  function renderCombobox(id, value, placeholder, groups) {
    return `
      <div class="combobox" data-cb="${id}">
        <button type="button" class="combobox__toggle ${value ? 'has-value' : ''}">
          <span class="${value ? '' : 'placeholder'}">${value ? esc(value) : esc(placeholder)}</span>
          <svg class="combobox__chevron" width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M3 6l5 5 5-5" stroke-linecap="round" stroke-linejoin="round"/></svg>
        </button>
        <div class="combobox__panel">
          <input class="combobox__search" type="text" placeholder="${t('search', 'Search…')}" autocomplete="off">
          <div class="combobox__list">
            ${groups.map(g => `
              <div class="combobox__group">
                ${g.name ? `<div class="combobox__group-label">${esc(g.name)}</div>` : ''}
                ${(g.subjects || []).map(s => `<button type="button" class="combobox__option ${value === s ? 'is-selected' : ''}" data-value="${esc(s)}">${esc(s)}</button>`).join('')}
              </div>
            `).join('')}
          </div>
        </div>
      </div>
    `;
  }

  function bindComboboxes() {
    stepsContainer.querySelectorAll('.combobox').forEach(cb => {
      const id = cb.dataset.cb;
      const toggle = cb.querySelector('.combobox__toggle');
      const search = cb.querySelector('.combobox__search');
      const options = cb.querySelectorAll('.combobox__option');
      const labelEl = toggle.querySelector('span');

      toggle.addEventListener('click', (e) => {
        e.stopPropagation();
        const open = cb.classList.toggle('is-open');
        if (open) {
          stepsContainer.querySelectorAll('.combobox').forEach(other => { if (other !== cb) other.classList.remove('is-open'); });
          setTimeout(() => search && search.focus(), 50);
        }
      });
      if (search) search.addEventListener('input', () => {
        const q = search.value.toLowerCase();
        let anyMatch = 0;
        cb.querySelectorAll('.combobox__group').forEach(g => {
          let groupAny = false;
          g.querySelectorAll('.combobox__option').forEach(o => {
            const m = o.textContent.toLowerCase().includes(q);
            o.style.display = m ? '' : 'none';
            if (m) groupAny = true;
          });
          const lbl = g.querySelector('.combobox__group-label');
          if (lbl) lbl.style.display = groupAny ? '' : 'none';
          if (groupAny) anyMatch++;
        });
        let empty = cb.querySelector('.combobox__empty');
        if (anyMatch === 0) {
          if (!empty) {
            empty = document.createElement('div');
            empty.className = 'combobox__empty';
            empty.textContent = t('no_matches', 'No matches');
            cb.querySelector('.combobox__list').appendChild(empty);
          }
        } else if (empty) empty.remove();
      });

      options.forEach(opt => {
        opt.addEventListener('click', () => {
          const value = opt.dataset.value;
          if (id === 'ee-core') {
            if (state.eeInterSubject === value) state.eeInterSubject = '';
            state.eeCoreSubject = value;
          } else if (id === 'ee-inter') {
            if (state.eeCoreSubject === value) return;
            state.eeInterSubject = value;
          } else if (id === 'cp-global') {
            state.cpGlobalContext = value;
          }
          labelEl.textContent = value;
          labelEl.classList.remove('placeholder');
          toggle.classList.add('has-value');
          options.forEach(o => o.classList.toggle('is-selected', o === opt));
          cb.classList.remove('is-open');
          touch();
        });
      });
    });
  }

  document.addEventListener('click', (e) => {
    document.querySelectorAll('.combobox.is-open').forEach(cb => {
      if (!cb.contains(e.target)) cb.classList.remove('is-open');
    });
  });
```

Also add a stub `bindCPFieldset` so `bindMetadata` doesn't ReferenceError (real impl is Task 11):

```javascript
  function bindCPFieldset() { /* implemented in Task 11 */ }
```

- [ ] **Step 2: Smoke-test**

Restart the dev server. Choose Extended Essay → Continue. Expected:
- EE subject combobox opens on click, search filters, clicking an option selects it.
- Choosing the same subject for Core and Interdisciplinary is prevented (silent).
- Criterion scores A..E accept 0..max, clamp out-of-range, update the total readout live.
- Toggling "Include commentaries for all criteria" reveals/hides 5 textareas + holistic.

Stop the server.

- [ ] **Step 3: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload): EE fieldset (subject comboboxes, criteria, total, comments)"
```

---

### Task 11: Render Step 2 — CP fieldset (combobox, pill-checks, criteria table)

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add CP fieldset renderer + binder**

In `renderMetadata`, replace `${isCP ? '<!-- CP fieldset (Task 11) -->' : ''}` with `${isCP ? renderCPFieldset() : ''}`.

After the EE-fieldset block, add:

```javascript
  // ─── CP fieldset ───────────────────────────────────────────
  function renderCPFieldset() {
    const criteria = [
      ['A', t('crit_cp_A', 'Investigating')],
      ['B', t('crit_cp_B', 'Planning')],
      ['C', t('crit_cp_C', 'Taking Action')],
      ['D', t('crit_cp_D', 'Reflecting')],
    ];
    const filled = Object.values(state.cpScores).filter(v => v !== '' && !isNaN(parseInt(v, 10)));
    const avg = filled.length ? Math.round(sumScores(state.cpScores) / 4) : 0;
    const contexts = BOOT.cp_global_contexts || [];
    const actions = BOOT.cp_action_types || [];

    return `
      <div class="section-sub">${t('global_context', 'Global Context')} <span class="req">*</span></div>
      <div class="field">
        ${renderCombobox('cp-global', state.cpGlobalContext, t('select_global', 'Select a Global Context…'),
          [{ name: t('global_contexts', 'Global Contexts'), subjects: contexts }])}
      </div>

      <div class="section-sub" style="margin-top:24px;">${t('type_of_action', 'Type of Action')} <span class="req">*</span></div>
      <div class="pill-checks">
        ${actions.map(a => `
          <label class="pill-check ${state.cpActionTypes.includes(a) ? 'is-checked' : ''}">
            <input type="checkbox" value="${esc(a)}" ${state.cpActionTypes.includes(a) ? 'checked' : ''}>${esc(a)}
          </label>
        `).join('')}
      </div>

      <div class="section-sub" style="margin-top:24px;">${t('crit_scores', 'Criterion Scores')} <span class="req">*</span></div>
      <table class="crit-table" id="cpCriteria">
        <thead><tr><th>${t('crit', 'Crit.')}</th><th>${t('criterion', 'Criterion')}</th><th style="width:140px;">${t('score', 'Score')}</th></tr></thead>
        <tbody>
          ${criteria.map(([k, name]) => `
            <tr>
              <td class="crit-letter">${k}</td>
              <td class="crit-name">${esc(name)}</td>
              <td class="crit-score">
                <span class="crit-score__input">
                  <input type="number" min="0" max="8" value="${esc(state.cpScores[k])}" data-crit="${k}" placeholder="0">
                  <span class="crit-score__max">/ 8</span>
                </span>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>

      <div class="total-readout">
        <div>
          <div class="total-readout__label">${t('overall_grade', 'Overall Grade')}</div>
          <div class="total-readout__sub">${t('overall_cp_sub', 'Mean of the four criterion scores, rounded')}</div>
        </div>
        <div class="total-readout__value"><span id="cpTotal">${avg}</span><small>/ 8</small></div>
      </div>
    `;
  }
```

Replace the empty `bindCPFieldset` stub with the real implementation:

```javascript
  function bindCPFieldset() {
    stepsContainer.querySelectorAll('#cpCriteria input[data-crit]').forEach(inp => {
      inp.addEventListener('input', e => {
        const k = inp.dataset.crit;
        let v = parseInt(e.target.value, 10);
        if (!isNaN(v)) { if (v < 0) v = 0; if (v > 8) v = 8; }
        state.cpScores[k] = isNaN(v) ? '' : String(v);
        const filled = Object.values(state.cpScores).filter(x => x !== '' && !isNaN(parseInt(x, 10)));
        const totalEl = stepsContainer.querySelector('#cpTotal');
        if (totalEl) totalEl.textContent = filled.length ? Math.round(sumScores(state.cpScores) / 4) : 0;
        touch();
      });
    });
    stepsContainer.querySelectorAll('.pill-check input[type="checkbox"]').forEach(cb => {
      cb.addEventListener('change', () => {
        const v = cb.value;
        if (cb.checked && !state.cpActionTypes.includes(v)) state.cpActionTypes.push(v);
        if (!cb.checked) state.cpActionTypes = state.cpActionTypes.filter(x => x !== v);
        cb.closest('.pill-check').classList.toggle('is-checked', cb.checked);
        touch();
      });
    });
  }
```

- [ ] **Step 2: Smoke-test**

Restart server, choose Community Project → Continue. Expected:
- Global Context combobox lists six contexts, search works, selection persists.
- Four "pill-check" buttons (Direct Service / Indirect Service / Research / Advocacy) toggle on click.
- Criterion A..D inputs (0..8) update the average readout live.

Stop the server.

- [ ] **Step 3: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload): CP fieldset (global context, action pills, criteria)"
```

---

### Task 12: Render Step 3 — Authors

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add `renderAuthors` + `bindAuthors`**

Replace `case 'authors':` with `html = renderAuthors(); break;` and add `if (id === 'authors') bindAuthors();` to `bindStep`.

After `bindCPFieldset()`, add:

```javascript
  // ─── Step 3: Authors ───────────────────────────────────────
  function renderAuthors() {
    return `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: 3 })} · ${t('author_info', 'Author information')}</div>
          <h2 class="wizard-card__title">${t('who_wrote', 'Who wrote this?')}</h2>
          <p class="wizard-card__sub">${t('authors_sub', "The first author's contact details are required. Add co-authors as needed.")}</p>
        </div>

        <div id="authorsList">
          ${state.authors.map((a, i) => `
            <div class="author-row" data-i="${i}">
              <input class="input" type="text" placeholder="${t('name', 'Name')}${i === 0 ? ' *' : ''}" value="${esc(a.name)}" data-field="name">
              <input class="input" type="email" placeholder="${t('email', 'Email')}${i === 0 ? ' *' : ''}" value="${esc(a.email)}" data-field="email">
              <input class="input" type="text" placeholder="${t('school', 'School / Institution')}${i === 0 ? ' *' : ''}" value="${esc(a.school)}" data-field="school">
              <button type="button" class="author-row__remove" data-i="${i}" ${state.authors.length === 1 ? 'disabled' : ''} aria-label="${t('remove_author', 'Remove author')}">×</button>
            </div>
          `).join('')}
        </div>

        <button type="button" class="btn-add-author" id="addAuthorBtn">${t('add_author', '+ Add another author')}</button>
      </div>
    `;
  }

  function bindAuthors() {
    stepsContainer.querySelectorAll('.author-row').forEach(row => {
      const i = parseInt(row.dataset.i, 10);
      row.querySelectorAll('input').forEach(inp => {
        inp.addEventListener('input', e => {
          state.authors[i][inp.dataset.field] = e.target.value;
          touch();
        });
      });
      const rem = row.querySelector('.author-row__remove');
      rem.addEventListener('click', () => {
        if (state.authors.length > 1) {
          state.authors.splice(i, 1);
          renderStep();
          touch();
        }
      });
    });
    const add = stepsContainer.querySelector('#addAuthorBtn');
    if (add) add.addEventListener('click', () => {
      state.authors.push({ name: '', email: '', school: '' });
      renderStep();
      touch();
    });
  }
```

- [ ] **Step 2: Smoke-test**

Restart, complete steps 1-2, click Continue. Expected on Step 3:
- One author row (name / email / school), Remove button disabled.
- Click "+ Add another author" → second row appears with active Remove.
- Removing a row works.

Stop the server.

- [ ] **Step 3: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload): Step 3 authors with add / remove"
```

---

### Task 13: Render Step 4 — File

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add `renderFile` + `bindFile`**

Replace `case 'file':` with `html = renderFile(); break;` and add `if (id === 'file') bindFile();` to `bindStep`.

After `bindAuthors()`, add:

```javascript
  // ─── Step 4: File ──────────────────────────────────────────
  function renderFile() {
    const fileIdx = getSteps().findIndex(s => s.id === 'file') + 1;
    return `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: fileIdx })} · ${t('file_upload', 'File upload')}</div>
          <h2 class="wizard-card__title">${t('upload_pdf', 'Upload your PDF')}</h2>
          <p class="wizard-card__sub">${t('upload_pdf_sub', 'Submit a single PDF, up to 50 MB. You can change this before publishing.')}</p>
        </div>

        <label class="filefield" id="fileLabel">
          <span class="filefield__icon">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/>
            </svg>
          </span>
          <span class="filefield__body">
            <span class="filefield__name" id="fileName">${state.file ? esc(state.file.name) : t('no_file_chosen', 'No file chosen')}</span>
            <span class="filefield__meta" id="fileMeta">${state.file ? formatBytes(state.file.size) + ' · PDF' : t('pdf_only_single', 'PDF only · single file')}</span>
          </span>
          <span class="filefield__btn">
            <span class="btn-file" id="chooseFileBtn">${state.file ? t('replace_file', 'Replace') : t('choose_file', 'Choose file')}</span>
          </span>
        </label>
        <p class="field__hint" style="margin-top:10px;">${t('file_save_hint', "If you'd like to come back to this later, click Save Draft below — your form will be restored next time you visit.")}</p>
      </div>
    `;
  }

  function bindFile() {
    const choose = stepsContainer.querySelector('#chooseFileBtn');
    const realInput = document.getElementById('uploadFormFile');
    if (choose && realInput) {
      choose.addEventListener('click', () => realInput.click());
      realInput.addEventListener('change', () => {
        const f = realInput.files && realInput.files[0];
        if (f) {
          state.file = { name: f.name, size: f.size };
          const nameEl = stepsContainer.querySelector('#fileName');
          const metaEl = stepsContainer.querySelector('#fileMeta');
          if (nameEl) nameEl.textContent = f.name;
          if (metaEl) metaEl.textContent = formatBytes(f.size) + ' · PDF';
          choose.textContent = t('replace_file', 'Replace');
          touch();
        }
      });
    }
  }
```

Note: `uploadFormFile` is the real `<input type=file name=paper>` inside the hidden `#uploadForm`. The wizard button programmatically clicks it; the file lives directly in the form that gets submitted.

- [ ] **Step 2: Smoke-test**

Restart, walk through steps 1-3 then Continue. Expected on Step 4:
- "Choose file" button opens the system file picker. Choosing a PDF updates the displayed name/size and the button label becomes "Replace".

Stop the server.

- [ ] **Step 3: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload): Step 4 file picker (delegates to hidden #uploadFormFile)"
```

---

### Task 14: Render Step 5 — Review + missing-fields summary; wire Submit + Save Draft

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add `getMissing`, `renderReview`, `bindReview`**

Replace `case 'review':` with `html = renderReview(); break;` and add `if (id === 'review') bindReview();` to `bindStep`.

After `bindFile()`, add:

```javascript
  // ─── Step 5: Review + missing-field summary ───────────────
  function getMissing() {
    const steps = getSteps();
    const stepIdx = (id) => steps.findIndex(s => s.id === id);
    const missing = [];
    if (!state.paperType) missing.push({ label: t('paper_type', 'Paper type'), step: stepIdx('type') });
    if (!state.title.trim()) missing.push({
      label: state.paperType === 'ee' ? t('research_question', 'Research question') : t('paper_title', 'Paper title'),
      step: stepIdx('metadata'),
    });
    if (!state.language) missing.push({ label: t('language', 'Language'), step: stepIdx('metadata') });
    if (!state.category) missing.push({ label: t('subject_category', 'Subject category'), step: stepIdx('metadata') });
    if (state.paperType === 'standard') {
      if (!state.keywords.length) missing.push({ label: t('keywords', 'Keywords'), step: stepIdx('metadata') });
      if (!state.abstract.trim()) missing.push({ label: t('abstract', 'Abstract'), step: stepIdx('metadata') });
    }
    if (!state.isIbSample) {
      const a0 = state.authors[0] || {};
      if (!a0.name || !a0.email || !a0.school) {
        missing.push({ label: t('first_author', 'First author (name, email, school)'), step: stepIdx('authors') });
      }
    }
    if (state.paperType === 'ee') {
      if (!state.eeCoreSubject) missing.push({ label: t('ee_core', 'EE core subject'), step: stepIdx('metadata') });
      Object.entries(state.eeScores).forEach(([k, v]) => {
        if (v === '' || v == null) missing.push({ label: t('ee_score_x', 'EE criterion score %(k)s', { k }), step: stepIdx('metadata') });
      });
    }
    if (state.paperType === 'cp') {
      if (!state.cpGlobalContext) missing.push({ label: t('cp_global', 'Global context'), step: stepIdx('metadata') });
      if (!state.cpActionTypes.length) missing.push({ label: t('cp_action_label', 'Type of action'), step: stepIdx('metadata') });
      Object.entries(state.cpScores).forEach(([k, v]) => {
        if (v === '' || v == null) missing.push({ label: t('cp_score_x', 'CP criterion score %(k)s', { k }), step: stepIdx('metadata') });
      });
    }
    if (!state.file) missing.push({ label: t('pdf_file', 'PDF file'), step: stepIdx('file') });
    return missing.filter(m => m.step >= 0);
  }

  function renderReview() {
    const steps = getSteps();
    const idx = (id) => steps.findIndex(s => s.id === id);
    const missing = getMissing();
    const typeName = state.paperType === 'standard' ? t('type_standard', 'Independent Research Paper')
      : state.paperType === 'ee' ? t('type_ee', 'IB Extended Essay')
      : state.paperType === 'cp' ? t('type_cp', 'IB Community Project')
      : '—';
    const langName = state.language === 'en' ? t('english', 'English')
      : state.language === 'zh' ? t('chinese', 'Chinese') : '';
    const cats = BOOT.paper_categories || [];
    const matchedCat = cats.find(c => (typeof c === 'string' ? c : c.value) === state.category);
    const categoryName = matchedCat ? (typeof matchedCat === 'string' ? matchedCat : matchedCat.label) : '';

    let html = `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: steps.length })} · ${t('review_submit', 'Review & submit')}</div>
          <h2 class="wizard-card__title">${t('almost_there', 'Almost there — review your submission')}</h2>
          <p class="wizard-card__sub">${t('review_sub', 'Make sure everything looks right. You can jump back to any section to make changes.')}</p>
        </div>

        ${missing.length ? `
          <div class="review-missing">
            <div class="review-missing__head">
              ${missing.length === 1 ? t('missing_fields_one', '1 field still needs attention') : t('missing_fields_many', '%(n)s fields still need attention', { n: missing.length })}
            </div>
            <ul>
              ${missing.map(m => `<li><strong>${esc(m.label)}</strong> — <button type="button" class="review-section__edit" data-jump="${m.step}">${t('go_to', 'go to %(step)s', { step: steps[m.step].name })}</button></li>`).join('')}
            </ul>
          </div>
        ` : `
          <div class="review-missing review-missing--clean">
            <div class="review-missing__head">${t('everything_filled', 'Everything required is filled in.')}</div>
            <p style="margin:0;font-size:13.5px;">${t('submit_cta', 'Click Submit Paper below to send your submission for review.')}</p>
          </div>
        `}

        <div class="review-section">
          <div class="review-section__head">
            <div class="review-section__title">${t('paper_type', 'Paper Type')}</div>
            <button type="button" class="review-section__edit" data-jump="${idx('type')}">${t('edit', 'Edit')}</button>
          </div>
          <dl class="review-grid"><dt>${t('type', 'Type')}</dt><dd>${esc(typeName)}</dd></dl>
        </div>

        <div class="review-section">
          <div class="review-section__head">
            <div class="review-section__title">${t('metadata_title', 'Metadata')}</div>
            <button type="button" class="review-section__edit" data-jump="${idx('metadata')}">${t('edit', 'Edit')}</button>
          </div>
          <dl class="review-grid">
            <dt>${state.paperType === 'ee' ? t('research_q_short', 'Research Q.') : t('title_short', 'Title')}</dt>
            <dd${state.title ? '' : ' class="is-missing"'}>${state.title ? esc(state.title) : t('not_provided', 'Not provided')}</dd>
            <dt>${t('language', 'Language')}</dt><dd${langName ? '' : ' class="is-missing"'}>${langName || t('not_chosen', 'Not chosen')}</dd>
            <dt>${t('subject', 'Subject')}</dt><dd${categoryName ? '' : ' class="is-missing"'}>${categoryName || t('not_chosen', 'Not chosen')}</dd>
            ${state.paperType === 'standard' ? `
              <dt>${t('keywords', 'Keywords')}</dt><dd${state.keywords.length ? '' : ' class="is-missing"'}>${state.keywords.length ? state.keywords.map(esc).join(', ') : t('none', 'None')}</dd>
              <dt>${t('abstract', 'Abstract')}</dt><dd${state.abstract ? '' : ' class="is-missing"'}>${state.abstract ? esc(state.abstract.slice(0, 280)) + (state.abstract.length > 280 ? '…' : '') : t('not_written', 'Not written')}</dd>
            ` : ''}
            ${(state.paperType === 'ee' || state.paperType === 'cp') ? `<dt>${t('ib_sample', 'IB Sample')}</dt><dd>${state.isIbSample ? t('yes_skipped', 'Yes — author info skipped') : t('no', 'No')}</dd>` : ''}
          </dl>
        </div>

        ${!state.isIbSample ? `
          <div class="review-section">
            <div class="review-section__head">
              <div class="review-section__title">${t('authors', 'Authors')}</div>
              <button type="button" class="review-section__edit" data-jump="${idx('authors')}">${t('edit', 'Edit')}</button>
            </div>
            <dl class="review-grid">
              ${state.authors.map((a, i) => `
                <dt>${t('author', 'Author')} ${i + 1}</dt>
                <dd${(i === 0 && (!a.name || !a.email || !a.school)) ? ' class="is-missing"' : ''}>
                  ${a.name ? esc(a.name) : '<em>name?</em>'}${a.email ? ' · ' + esc(a.email) : ''}${a.school ? ' · ' + esc(a.school) : ''}
                </dd>
              `).join('')}
            </dl>
          </div>
        ` : ''}

        ${state.paperType === 'ee' ? renderReviewEE(idx('metadata')) : ''}
        ${state.paperType === 'cp' ? renderReviewCP(idx('metadata')) : ''}

        <div class="review-section">
          <div class="review-section__head">
            <div class="review-section__title">${t('file', 'File')}</div>
            <button type="button" class="review-section__edit" data-jump="${idx('file')}">${t('edit', 'Edit')}</button>
          </div>
          <dl class="review-grid">
            <dt>PDF</dt><dd${state.file ? '' : ' class="is-missing"'}>${state.file ? esc(state.file.name) + ' · ' + formatBytes(state.file.size) : t('no_file_uploaded', 'No file uploaded')}</dd>
          </dl>
        </div>
      </div>
    `;
    return html;
  }

  function renderReviewEE(jumpIdx) {
    const total = sumScores(state.eeScores);
    return `
      <div class="review-section">
        <div class="review-section__head">
          <div class="review-section__title">${t('ee_details', 'EE Details')}</div>
          <button type="button" class="review-section__edit" data-jump="${jumpIdx}">${t('edit', 'Edit')}</button>
        </div>
        <dl class="review-grid">
          <dt>${t('core_subject', 'Core Subject')}</dt><dd${state.eeCoreSubject ? '' : ' class="is-missing"'}>${esc(state.eeCoreSubject) || t('not_chosen', 'Not chosen')}</dd>
          ${state.eeInterSubject ? `<dt>${t('inter_subject', 'Interdisciplinary')}</dt><dd>${esc(state.eeInterSubject)}</dd>` : ''}
          <dt>${t('crit', 'Crit.')} A</dt><dd>${state.eeScores.A || 0} / 6</dd>
          <dt>${t('crit', 'Crit.')} B</dt><dd>${state.eeScores.B || 0} / 6</dd>
          <dt>${t('crit', 'Crit.')} C</dt><dd>${state.eeScores.C || 0} / 6</dd>
          <dt>${t('crit', 'Crit.')} D</dt><dd>${state.eeScores.D || 0} / 8</dd>
          <dt>${t('crit', 'Crit.')} E</dt><dd>${state.eeScores.E || 0} / 4</dd>
          <dt>${t('total', 'Total')}</dt><dd><strong>${total} / 30</strong></dd>
        </dl>
      </div>
    `;
  }

  function renderReviewCP(jumpIdx) {
    const sum = sumScores(state.cpScores);
    const avg = Math.round(sum / 4);
    return `
      <div class="review-section">
        <div class="review-section__head">
          <div class="review-section__title">${t('cp_details', 'CP Details')}</div>
          <button type="button" class="review-section__edit" data-jump="${jumpIdx}">${t('edit', 'Edit')}</button>
        </div>
        <dl class="review-grid">
          <dt>${t('global_context', 'Global Context')}</dt><dd${state.cpGlobalContext ? '' : ' class="is-missing"'}>${esc(state.cpGlobalContext) || t('not_chosen', 'Not chosen')}</dd>
          <dt>${t('type_of_action', 'Type of Action')}</dt><dd${state.cpActionTypes.length ? '' : ' class="is-missing"'}>${state.cpActionTypes.length ? state.cpActionTypes.map(esc).join(', ') : t('none_selected', 'None selected')}</dd>
          <dt>${t('crit', 'Crit.')} A</dt><dd>${state.cpScores.A || 0} / 8</dd>
          <dt>${t('crit', 'Crit.')} B</dt><dd>${state.cpScores.B || 0} / 8</dd>
          <dt>${t('crit', 'Crit.')} C</dt><dd>${state.cpScores.C || 0} / 8</dd>
          <dt>${t('crit', 'Crit.')} D</dt><dd>${state.cpScores.D || 0} / 8</dd>
          <dt>${t('total', 'Total')}</dt><dd><strong>${avg} / 8</strong></dd>
        </dl>
      </div>
    `;
  }

  function bindReview() {
    stepsContainer.querySelectorAll('[data-jump]').forEach(b => {
      b.addEventListener('click', () => goToStep(parseInt(b.dataset.jump, 10)));
    });
  }
```

- [ ] **Step 2: Add `serializeToForm` and wire Submit + Save Draft**

After `bindReview()`, add:

```javascript
  // ─── Submit / Save Draft ───────────────────────────────────
  function serializeToForm(extraInputs) {
    const form = document.getElementById('uploadForm');
    if (!form) { console.error('[upload-wizard] #uploadForm missing'); return; }
    // Remove any previously injected hidden inputs (keep #uploadFormFile and draft_id).
    form.querySelectorAll('input[data-wiz]').forEach(el => el.remove());

    const add = (name, value) => {
      if (value == null) return;
      const i = document.createElement('input');
      i.type = 'hidden'; i.name = name; i.value = String(value);
      i.setAttribute('data-wiz', '1');
      form.appendChild(i);
    };

    if (state.paperType === 'ee') add('is_ib_ee', '1');
    if (state.paperType === 'cp') add('is_cp_paper', '1');
    if (state.isIbSample && state.paperType !== 'standard') add('is_ib_sample', '1');

    add('title', state.title);
    add('language', state.language);
    add('category', state.category);

    if (state.paperType === 'standard') {
      add('keywords', state.keywords.join(', '));
      add('abstract', state.abstract);
    }

    if (!state.isIbSample) {
      state.authors.forEach(a => {
        add('author_name', a.name);
        add('author_email', a.email);
        add('author_school', a.school);
      });
    }

    if (state.paperType === 'ee') {
      add('ib_ee_core_subject', state.eeCoreSubject);
      add('ib_ee_interdisciplinary_subject', state.eeInterSubject);
      ['A', 'B', 'C', 'D', 'E'].forEach(k => add(`ib_crit_${k}_score`, state.eeScores[k] || '0'));
      if (state.eeIncludeComments) {
        ['A', 'B', 'C', 'D', 'E'].forEach(k => add(`ib_crit_${k}_comment`, state.eeComments[k] || ''));
        add('ib_holistic_comment', state.eeComments.holistic || '');
      }
    }

    if (state.paperType === 'cp') {
      add('cp_global_context', state.cpGlobalContext);
      state.cpActionTypes.forEach(a => add('cp_action_type', a));
      ['A', 'B', 'C', 'D'].forEach(k => add(`cp_crit_${k}_score`, state.cpScores[k] || '0'));
    }

    (extraInputs || []).forEach(([n, v]) => add(n, v));

    form.submit();
  }
```

Now wire the footer buttons. Find `renderFooter()` and replace its body's button handlers section with:

```javascript
    const back = footerEl.querySelector('#backBtn');
    if (back) back.addEventListener('click', () => goToStep(state.step - 1));
    const next = footerEl.querySelector('#nextBtn');
    if (next) next.addEventListener('click', () => {
      if (isLast) {
        // Submit only when nothing is missing; otherwise re-render Review so
        // the missing-fields summary updates.
        if (getMissing().length) { renderStep(); return; }
        clearLocalStorage();
        serializeToForm();
      } else {
        goToStep(state.step + 1);
      }
    });
    const save = footerEl.querySelector('#saveBtn');
    if (save) save.addEventListener('click', () => {
      autosaveSaving();
      // For Save Draft we tolerate missing fields; the server only requires a title.
      serializeToForm([['save_draft', '1']]);
    });
```

Add the helpers (`autosaveSaving`, `autosaveSaved`, `clearLocalStorage` — `clearLocalStorage` is filled in Task 15; for now make it a no-op):

```javascript
  function autosaveSaving() {
    if (!autosaveEl) return;
    autosaveEl.classList.remove('autosave--idle', 'autosave--saved');
    autosaveEl.classList.add('autosave--saving');
    const text = autosaveEl.querySelector('.autosave__text');
    if (text) text.textContent = t('saving', 'Saving…');
  }
  function autosaveSaved() {
    if (!autosaveEl) return;
    autosaveEl.classList.remove('autosave--idle', 'autosave--saving');
    autosaveEl.classList.add('autosave--saved');
    const text = autosaveEl.querySelector('.autosave__text');
    if (text) {
      const now = new Date();
      const hh = String(now.getHours()).padStart(2, '0');
      const mm = String(now.getMinutes()).padStart(2, '0');
      text.textContent = t('draft_saved_at', 'Draft saved · %(time)s', { time: hh + ':' + mm });
    }
  }
  function clearLocalStorage() { /* implemented in Task 15 */ }
```

- [ ] **Step 3: Smoke-test happy path**

Restart server. As a contributor account:
1. Choose Standard → fill title/language/category, add 2 keyword chips, write abstract, Continue.
2. Step 3: name+email+school. Continue.
3. Step 4: choose a PDF. Continue.
4. Step 5: Review summary lists all fields, alert is green, Submit Paper → redirects to upload_success page.

Repeat for EE: choose EE → enter Research Question, language, category, EE core subject, all 5 scores, Continue. Authors, File, Submit. Check `submissions` / `papers_metadata` row contains the EE JSON with the correct scores.

Repeat for CP similarly.

Try Submit with a missing required field → confirm the Review step shows the orange "X fields still need attention" alert and clicking "go to X" jumps to that step.

Try Save Draft after step 2 → see flash "Draft saved successfully", redirected to my_submissions, draft row created.

Stop the server.

- [ ] **Step 4: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload): Step 5 Review + serialize wizard state into hidden form

serializeToForm writes the wizard's state to the existing
#uploadForm and submits. Save Draft adds save_draft=1.
EE total is intentionally NOT written; server recomputes from
the per-criterion scores."
```

---

### Task 15: localStorage mirror + Restore/Discard banner

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add `mirrorToLocalStorage`, `loadLocalStorage`, `clearLocalStorage`, and the Restore banner**

After the `autosave*` helpers, add:

```javascript
  // ─── localStorage mirror ───────────────────────────────────
  const STORAGE_KEY = 'kd:upload-draft:' + (BOOT.user_key || 'anon') + (BOOT.draft_id ? ':' + BOOT.draft_id : '');
  let mirrorTimer = null;

  function mirrorToLocalStorage() {
    clearTimeout(mirrorTimer);
    mirrorTimer = setTimeout(() => {
      try {
        const payload = {
          ts: Date.now(),
          state: serializableState(),
        };
        localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
      } catch (e) { /* quota / disabled — silent fallback */ }
    }, 600);
  }

  function serializableState() {
    const s = Object.assign({}, state);
    s.visitedSteps = Array.from(state.visitedSteps);
    // The wizard's `file` is just {name, size}; the real File object lives in
    // #uploadFormFile and isn't restorable from localStorage anyway.
    return s;
  }

  function loadLocalStorage() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (e) { return null; }
  }

  function clearLocalStorage() {
    try { localStorage.removeItem(STORAGE_KEY); } catch (e) { /* silent */ }
  }
```

- [ ] **Step 2: Replace `touch()` with one that also mirrors**

Replace the existing `touch()` (single-line) with:

```javascript
  function touch() {
    state.lastModified = Date.now();
    autosaveSaving();
    mirrorToLocalStorage();
    // Mark as "saved" visually after the debounce period — note this only
    // reflects localStorage, NOT a server save. Server save is gated by
    // the Save Draft button. The visual lie matches user intent here: the
    // wizard remembers their work between tabs.
    setTimeout(() => autosaveSaved(), 700);
  }
```

- [ ] **Step 3: Show the Restore banner if localStorage is newer than the loaded draft**

In `init()`, after the mount-points null check and before `render()`, add:

```javascript
    const stored = loadLocalStorage();
    const fdEmpty = !state.title && !state.paperType && state.keywords.length === 0 && !state.eeCoreSubject && !state.cpGlobalContext;
    if (stored && stored.state && (fdEmpty || stored.ts > (Number(fd.last_modified) || 0))) {
      showRestoreBanner(stored);
      return;   // wait for user to click Restore or Discard before rendering
    }
    render();
```

Add the banner function:

```javascript
  function showRestoreBanner(stored) {
    const banner = document.createElement('div');
    banner.className = 'restore-banner';
    banner.innerHTML = `
      <div class="restore-banner__body">
        <div class="restore-banner__title">${t('restore_banner_title', 'Unsaved changes from earlier')}</div>
        <div class="restore-banner__sub">${t('restore_banner_body', "Your last session in this browser had changes you didn't save. Restore them?")}</div>
      </div>
      <div class="restore-banner__actions">
        <button type="button" class="btn btn--ghost" id="restoreDiscardBtn">${t('discard_btn', 'Discard')}</button>
        <button type="button" class="btn btn--primary" id="restoreApplyBtn">${t('restore_btn', 'Restore')}</button>
      </div>
    `;
    stepsContainer.appendChild(banner);
    banner.querySelector('#restoreApplyBtn').addEventListener('click', () => {
      Object.assign(state, stored.state);
      state.visitedSteps = new Set(stored.state.visitedSteps || [0]);
      banner.remove();
      render();
    });
    banner.querySelector('#restoreDiscardBtn').addEventListener('click', () => {
      clearLocalStorage();
      banner.remove();
      render();
    });
  }
```

- [ ] **Step 4: Add minimal CSS for the banner**

Append to `static/css/upload.css`:

```css
.kd-upload-wizard .restore-banner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  background: var(--paper-warm, #fffdf6);
  border: 1px solid var(--border, #e7e1d5);
  border-radius: 12px;
  padding: 18px 22px;
  margin-bottom: 22px;
}
.kd-upload-wizard .restore-banner__title { font-weight: 600; margin-bottom: 4px; }
.kd-upload-wizard .restore-banner__sub { font-size: 13.5px; color: var(--ink-muted, #6b6052); }
.kd-upload-wizard .restore-banner__actions { display: flex; gap: 10px; flex-shrink: 0; }
```

- [ ] **Step 5: Smoke-test**

Restart server. Visit `/dashboard/upload`. Fill some fields without Save Draft. Close the tab. Reopen `/dashboard/upload`. Expected: Restore banner appears at the top. Click Restore → fields come back. Reload, click Discard → banner gone, fields blank.

Stop the server.

- [ ] **Step 6: Commit**

```bash
git add static/js/upload-wizard.js static/css/upload.css
git commit -m "feat(upload): localStorage mirror + Restore/Discard banner

Per-edit mirror (debounced 600ms) with namespaced key per user + draft.
On init, if localStorage is newer than the loaded draft, prompt the
user to Restore or Discard. Silently degrades when localStorage is
disabled or full."
```

---

## Phase 4 — Test updates

### Task 16: Update `tests/test_upload_template.py` for new mount IDs

**Files:**
- Modify: `tests/test_upload_template.py`

- [ ] **Step 1: Replace the file**

Replace `tests/test_upload_template.py` with:

```python
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class UploadTemplateDomContractTest(unittest.TestCase):
    """The wizard JS targets specific element IDs; they must exist in the template."""

    @classmethod
    def setUpClass(cls):
        cls.template_src = (ROOT / "templates" / "upload.html").read_text(encoding="utf-8")
        cls.wizard_js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")

    def test_template_declares_wizard_mount_points(self):
        for required_id in ("wizardStepper", "wizardSteps", "wizardFooter", "uploadForm",
                             "uploadFormFile", "autosaveIndicator"):
            self.assertIn(f'id="{required_id}"', self.template_src,
                          f"upload.html is missing #{required_id}")

    def test_wizard_js_getElementById_calls_resolve_to_template_ids(self):
        declared = set(re.findall(r'\bid="([^"]+)"', self.template_src))
        referenced = set(re.findall(
            r'getElementById\(\s*["\']([^"\']+)["\']\s*\)', self.wizard_js
        ))
        # The wizard creates many IDs dynamically (#f-title, #eeCriteria, etc.)
        # inside its rendered HTML and then queries them via querySelector — those
        # do NOT need to be in the template. Only the boot-time getElementById
        # mount points are checked here. The wizard's getElementById calls only
        # target template-declared IDs.
        self.assertTrue(referenced.issubset(declared),
                        f"wizard JS references missing IDs: {sorted(referenced - declared)}")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run**

Run: `python -m unittest tests.test_upload_template -v`

Expected: 2 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_upload_template.py
git commit -m "test(upload): update DOM contract for wizard mount IDs"
```

---

### Task 17: Rewrite `tests/test_ee_total_grade_contract.py` for new IDs

**Files:**
- Modify: `tests/test_ee_total_grade_contract.py`

- [ ] **Step 1: Replace the file**

Replace `tests/test_ee_total_grade_contract.py` with:

```python
import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EeTotalGradeContractTest(unittest.TestCase):
    """EE total grade is computed server-side from per-criterion scores.

    The wizard NEVER writes a hidden input named ib_total_grade_number, so the
    server can never be tricked into accepting a client-submitted total.
    """

    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.wizard_js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_server_ee_total_recomputed_from_criteria(self):
        helper = self._find_function("build_ib_ee_data_from_form")
        src = ast.get_source_segment(self.app_source, helper)
        self.assertIn('"total_grade_number": str(total_score)', src)
        self.assertNotIn('form.get("ib_total_grade_number"', src)

    def test_wizard_computes_total_client_side_from_ee_scores(self):
        # The total readout updates live; this exists somewhere in the JS.
        self.assertIn("sumScores(state.eeScores)", self.wizard_js)
        self.assertRegex(self.wizard_js, r"#eeTotal")

    def test_wizard_does_not_serialize_ib_total_grade_number(self):
        # serializeToForm contains the wire-contract field list — verify the
        # untrusted total field never appears.
        self.assertNotIn("ib_total_grade_number", self.wizard_js)

    def _find_function(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        self.fail(f"Could not find function {name}")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run**

Run: `python -m unittest tests.test_ee_total_grade_contract -v`

Expected: 3 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_ee_total_grade_contract.py
git commit -m "test(upload): rewrite EE-total contract for wizard

Preserves the invariant (server recomputes total; client never
submits one) against the new wizard's IDs and serialize contract."
```

---

### Task 18: Add `tests/test_upload_wizard_dom_contract.py`

**Files:**
- Create: `tests/test_upload_wizard_dom_contract.py`

- [ ] **Step 1: Create the file**

```python
import json
import unittest
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape


ROOT = Path(__file__).resolve().parents[1]


class UploadWizardJinjaContractTest(unittest.TestCase):
    """templates/upload.html must render the wizard shell correctly."""

    @classmethod
    def setUpClass(cls):
        env = Environment(
            loader=FileSystemLoader(str(ROOT / "templates")),
            autoescape=select_autoescape(["html", "xml"]),
            extensions=["jinja2.ext.i18n"],
        )
        env.install_null_translations()
        # The bare layout is the easiest to render without a Flask request ctx;
        # set partial=True to take that branch.
        cls.env = env

    def _render(self):
        ctx = {
            "partial": True,
            "user": {"username": "u", "role": "2", "display_name": "U", "first_name": "U", "last_name": "", "email": ""},
            "form_data": {},
            "journals": [],
            "paper_categories": ["literature", "natural-science"],
            "ee_subjects": {"groups": []},
            "cp_global_contexts": [],
            "cp_action_types": [],
            "draft_id": "",
            "wizard_boot": {
                "submit_url": "/dashboard/upload",
                "draft_id": "",
                "form_data": {},
                "paper_categories": ["literature"],
                "ee_subjects": {"groups": []},
                "cp_global_contexts": [],
                "cp_action_types": [],
                "user_key": "u",
                "i18n": {},
            },
            "url_for": lambda name, **kw: "/" + name,
            "_": lambda s, **kw: s,
            "role_label": lambda r: "Contributor",
            "dashboard_stats": {},
        }
        # _bare.html provides no `content` block by default; we render the
        # upload template directly and capture the `panel` block content.
        tpl = self.env.get_template("upload.html")
        return tpl.render(**ctx)

    def test_mount_points_present(self):
        out = self._render()
        for required_id in ("wizardStepper", "wizardSteps", "wizardFooter",
                             "uploadForm", "uploadFormFile", "autosaveIndicator"):
            self.assertIn(f'id="{required_id}"', out)

    def test_hidden_form_has_post_and_enctype(self):
        out = self._render()
        self.assertRegex(out, r'<form\s+id="uploadForm"[^>]*method="post"')
        self.assertRegex(out, r'<form\s+id="uploadForm"[^>]*enctype="multipart/form-data"')
        self.assertIn('action="/dashboard/upload"', out)

    def test_boot_script_emits_window_WIZARD_BOOT(self):
        out = self._render()
        self.assertIn("window.WIZARD_BOOT =", out)
        # And the JSON is valid (parse the slice between `= ` and the next `;</script>`).
        start = out.find("window.WIZARD_BOOT =")
        end = out.find(";</script>", start)
        json_blob = out[start + len("window.WIZARD_BOOT ="):end].strip()
        json.loads(json_blob)   # raises if malformed


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run**

Run: `python -m unittest tests.test_upload_wizard_dom_contract -v`

Expected: 3 tests PASS. If the Jinja env can't find `_bare.html`'s parent block, simplify by inspecting the raw template source instead (open `templates/upload.html` and assert the strings appear). The above approach is preferred when it works.

- [ ] **Step 3: Commit**

```bash
git add tests/test_upload_wizard_dom_contract.py
git commit -m "test(upload): Jinja render contract for the wizard shell"
```

---

## Phase 5 — Cleanup + smoke

### Task 19: Delete the mockup folder

**Files:**
- Delete: `Keydion-Submission Page/` (entire folder)

- [ ] **Step 1: Remove the folder**

```bash
git rm -r "Keydion-Submission Page"
```

- [ ] **Step 2: Commit**

```bash
git commit -m "chore(upload): remove Keydion-Submission Page mockup folder

Port is complete; mockup is preserved in git history for reference."
```

---

### Task 20: Full manual smoke test (8 scenarios from spec §7.3)

This is a final, focused walk-through before merging. No code changes unless you find a bug — if you do, fix it as a separate commit.

- [ ] **Run all tests:**

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

Expected: all tests pass. Pay attention to the four upload-related tests (template, ee-total, wizard contract, dom contract).

- [ ] **Compile translations (if you added .po entries):**

```bash
python tools/compile_translations.py
```

- [ ] **Boot the dev server and complete each scenario:**

```bash
./start_local.sh
```

1. **Fresh Standard paper, single author** — log in as a contributor; fill all fields; upload a PDF; Submit. Confirm published (curator/admin role) or pending (reader role). Inspect the row in `papers_metadata` or `submissions` to confirm fields are stored.
2. **Fresh EE submission, all five criteria scored, commentaries enabled** — confirm the total readout updates live; after Submit, confirm `ib_ee_data.total_grade_number` equals the sum of the per-criterion scores.
3. **Fresh CP submission, two action types selected** — pill-checks toggle, CP average displays, after Submit confirm `cp_data.total_score` matches the mean.
4. **EE marked as IB Sample** — Authors step disappears from stepper; after Submit the stored `author_name` equals `IB SAMPLE`.
5. **Save draft → reload → continue editing** — exercise this three times (one Standard, one EE, one CP). EE scores, subjects, and commentaries must repopulate.
6. **Missing-field summary on Review** — submit half-filled wizard; orange alert lists every missing field with a working "go to step" link; fill everything; alert turns green; Submit succeeds.
7. **localStorage Restore banner** — fill fields, close tab without Save Draft, reopen `/dashboard/upload`, confirm Restore/Discard banner appears and Restore brings the fields back.
8. **Tampered EE total** — `curl -F is_ib_ee=1 -F title=Foo -F category=literature -F language=en -F author_name=X -F author_email=x@x.x -F author_school=Y -F ib_crit_A_score=1 -F ib_crit_B_score=1 -F ib_crit_C_score=1 -F ib_crit_D_score=1 -F ib_crit_E_score=1 -F ib_total_grade_number=999 -F paper=@some.pdf <upload-url-with-session>` — confirm stored `total_grade_number` is `"5"`, not `"999"`.

Stop the server.

- [ ] **If everything passes, no commit needed.** If any scenario fails, fix it as a focused commit before merging.

---

## Done

The wizard ships. Open a PR off `worktree-upload-wizard-spec`; the merge target is `main`.
