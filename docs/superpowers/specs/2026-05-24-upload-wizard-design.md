# Upload Page Revamp — Five-Step Wizard

**Status:** Approved design, ready for implementation plan.
**Date:** 2026-05-24
**Source mockup:** `Keydion-Submission Page/` (vanilla HTML/CSS/JS prototype dropped in the repo root)
**Supersedes:** the current single-page Bootstrap form in `templates/upload.html`.

---

## 1. Goal

Replace the existing one-long-scrolling Bootstrap form at `/dashboard/upload` with a five-step wizard that:

- Decides paper type up front (Standard / IB Extended Essay / IB Community Project) instead of revealing IB sections behind nested checkboxes.
- Asks only the fields that apply to the chosen type — keywords and abstract are removed for EE/CP per the new spec.
- Uses the cream/serif `dashboard.css` design vocabulary (no Bootstrap in the form chrome).
- Adds quality-of-life UX: searchable subject pickers, keyword chips, a live total readout for criterion scores, a missing-fields summary on Review, and an autosave indicator backed by `localStorage` mirroring.
- Preserves the existing server contract: one `POST /dashboard/upload` with the same field names, same draft mechanism, same submission/publish path.

---

## 2. Scope decisions (locked)

| # | Decision | Implication |
|---|---|---|
| 1 | **Full port** — adopt the wizard, the new component vocabulary, AND the field changes (no keywords/abstract for EE/CP; IB-Sample hidden for Standard). | Server validation in `app.py:upload()` becomes per-type. |
| 2 | **Hybrid autosave** — manual Save Draft button + per-edit `localStorage` mirror. | No new server endpoint. On reopen, wizard offers Restore/Discard if localStorage is newer than the loaded draft. |
| 3 | **Full draft round-trip** — when loading a draft, EE/CP scores, subjects, contexts, action types, and comments come back into the wizard. | New helpers `parse_ib_ee_data_for_form()` and `parse_cp_data_for_form()` next to the existing `build_*` functions; draft GET branch widens its `form_data` dict. |
| 4 | **Replace outright** — old `templates/upload.html` is overwritten. | No `/classic` fallback route. Existing contract tests are rewritten, not duplicated. |

---

## 3. Architecture overview

Three units, each with one responsibility:

- **`templates/upload.html`** — Jinja shell. Extends `_dashboard_shell.html`, renders the panel head with autosave pill, three empty wizard mount points (`#wizardStepper`, `#wizardSteps`, `#wizardFooter`), one hidden `<form id="uploadForm" method="post" enctype="multipart/form-data">`, and a single `<script>` tag exposing `window.WIZARD_BOOT = {{ wizard_boot | tojson }}`. ~120 lines. No business logic.
- **`static/css/upload.css`** — ported from `Keydion-Submission Page/css/upload.css`, all rules scoped under `.kd-upload-wizard` to avoid colliding with the Bootstrap classes still in use across other pages.
- **`static/js/upload-wizard.js`** — single IIFE. Reads `window.WIZARD_BOOT` on init, owns one state object, renders steps, manages stepper/footer, debounces a localStorage mirror, and on Submit writes hidden inputs into `#uploadForm` and submits it.

The wizard never makes its own XHR. The only way state reaches the server is through the existing `POST /dashboard/upload` (full submission or `save_draft=1` partial save).

```
templates/
  upload.html                      ← Jinja shell only
  upload_success.html              ← unchanged

static/css/
  upload.css                       ← NEW; scoped under .kd-upload-wizard

static/js/
  upload-wizard.js                 ← NEW

app.py
  upload() handler                 ← per-type required cascade (Section 4.1)
  parse_ib_ee_data_for_form()      ← NEW helper (Section 4.2)
  parse_cp_data_for_form()         ← NEW helper (Section 4.2)
  (render_template call sites)     ← pass wizard_boot dict (Section 4.4)

tests/
  test_upload_template.py          ← updated for new mount IDs
  test_ee_total_grade_contract.py  ← updated for new IDs / classes
  test_upload_wizard_contract.py   ← NEW; per-type validation contract
  test_upload_wizard_dom_contract.py ← NEW; Jinja render contract
```

---

## 4. Server-side changes (`app.py`)

### 4.1 Per-type required-field cascade

Today the route (`app.py:1457-1489`) flashes "Please enter X" for every standard field regardless of paper type. Replace the flat cascade with a single computed list:

```python
required = ["title", "category", "language"]
if not (is_ib_ee or is_cp_paper):
    required += ["keywords", "abstract"]
if not is_ib_sample:
    required += ["author_name", "author_email", "author_school"]

for field in required:
    if not form_data.get(field):
        flash(_(MISSING_MESSAGES[field]), "danger")
        return _render_upload(user, form_data, draft_id)
```

`_render_upload(user, form_data, draft_id)` is a small extracted helper that wraps the existing `render_template("upload.html", ...)` call so the per-type cascade doesn't need to repeat the kwargs eight times. EE/CP-specific validation (core subject, global context, action types) follows the cascade as today.

### 4.2 Draft round-trip for EE/CP

Two new helpers next to `build_ib_ee_data_from_form`:

```python
def parse_ib_ee_data_for_form(json_str: str) -> dict:
    """Flatten ib_ee_data JSON back into form-style keys.
    Returns {} on missing/invalid JSON."""
    try:
        data = json.loads(json_str) if json_str else {}
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

def parse_cp_data_for_form(json_str: str) -> dict:
    """Flatten cp_data JSON back into form-style keys.
    Returns {} on missing/invalid JSON."""
    try:
        data = json.loads(json_str) if json_str else {}
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

The GET `?draft=` branch (`app.py:1325`) merges their output into `form_data` so the wizard's `WIZARD_BOOT` blob picks up everything in one shot.

### 4.3 No changes to

- `build_ib_ee_data_from_form` / `build_cp_data_from_form` (the wizard sends the same field names; these stay byte-identical).
- `SubmissionModel` / `PaperMetadataModel` schemas.
- Role gating (`require_login(level=1)`).
- PDF save logic, `set_pdf_metadata`, `upsert_paper_metadata`.
- `upload_success` route, `upload_legacy`, `upload_success_legacy`.
- Partial-request rendering (`_bare.html` branch).

### 4.4 `wizard_boot` context

Every `render_template("upload.html", ...)` site in `upload()` passes a single `wizard_boot` dict:

```python
wizard_boot = {
    "submit_url": url_for("upload"),
    "draft_id": draft_id,
    "form_data": form_data,
    "paper_categories": load_paper_categories(),
    "ee_subjects": load_ee_subjects(),
    "cp_global_contexts": CP_GLOBAL_CONTEXTS,
    "cp_action_types": CP_ACTION_TYPES,
    "user_key": user.get("username", ""),  # for localStorage namespacing
    "i18n": {
        # UI strings the JS needs to display; everything else is server-rendered
        "saving": _("Saving…"),
        "draft_saved_at": _("Draft saved · %(time)s"),  # %(time)s replaced client-side
        "no_file_chosen": _("No file chosen"),
        "pdf_only_single": _("PDF only · single file"),
        "choose_file": _("Choose file"),
        "replace_file": _("Replace"),
        # ... per the full string table in upload-wizard.js
    },
}
```

Rendered in the template as:

```html
<script>window.WIZARD_BOOT = {{ wizard_boot | tojson }};</script>
```

---

## 5. Client wizard architecture (`upload-wizard.js`)

Single IIFE, one state object, five public functions, several private renderers/binders. Same shape as the mockup's `js/upload.js`, with three tightenings:

1. All EE_SUBJECTS / PAPER_CATEGORIES / CP_GLOBAL_CONTEXTS / CP_ACTION_TYPES constants come from `WIZARD_BOOT`, not hard-coded.
2. All user-visible strings flow through `i18n(key, fallback)` reading `WIZARD_BOOT.i18n`.
3. The `alert("Submitted!")` stub is replaced by `serializeToForm()` writing hidden inputs into `#uploadForm` and submitting.

### 5.1 State

```js
state = {
  paperType: '',            // 'standard' | 'ee' | 'cp'
  title: '',
  language: '',             // 'en' | 'zh'
  category: '',
  keywords: [],             // string[]
  abstract: '',
  isIbSample: false,
  authors: [{name, email, school}],
  // EE
  eeCoreSubject: '',
  eeInterSubject: '',
  eeScores: {A:'', B:'', C:'', D:'', E:''},
  eeIncludeComments: false,
  eeComments: {A:'', B:'', C:'', D:'', E:'', holistic:''},
  // CP
  cpGlobalContext: '',
  cpActionTypes: [],
  cpScores: {A:'', B:'', C:'', D:''},
  // file (metadata only; actual <input type=file> lives in #uploadForm)
  file: null,               // {name, size} | null
  // wizard
  step: 0,
  visitedSteps: new Set([0]),
  lastModified: 0,          // epoch ms — written on every mutation
}
```

Hydrated on `init()` from `WIZARD_BOOT.form_data`. If `localStorage[storageKey]` exists and its `lastModified` is greater than `form_data.last_modified` (or `form_data` is empty), show the Restore/Discard banner before painting.

### 5.2 Public surface

| Function | What it does | Used by |
|---|---|---|
| `init()` | Reads boot blob, hydrates state, restores localStorage if newer, mounts stepper/steps/footer, calls `render()`. | DOMContentLoaded |
| `render()` | Re-renders stepper + current step + footer. Idempotent. | All event handlers |
| `goToStep(idx)` | Validates `idx` ∈ visited set, sets `state.step`, calls `render()`, scrolls `#dashboardMain` to top. | Stepper clicks, footer Back/Continue, Review's Edit jumps |
| `serializeToForm()` | Writes hidden inputs into `#uploadForm`, copies the file input by reference (keeps existing `<input type=file name=paper>` in place), `form.submit()`. | Submit button on Review step, Save Draft button (with `save_draft=1` hidden input) |
| `mirrorToLocalStorage()` | Debounced 600 ms. Writes `JSON.stringify({state, ts: Date.now()})` to `kd:upload-draft:{user_key}`. Wrapped in try/catch (quota / disabled storage degrades silently). | Every state mutation |

Everything else (step renderers, binders, helpers, the combobox/chips/segmented/pill-check components) is private.

### 5.3 Step shape

`getSteps()` returns a dynamic array depending on `paperType` and `isIbSample`:

| # | Step id | When shown | Renderer |
|---|---|---|---|
| 1 | `type` | always | `renderType()` |
| 2 | `metadata` | once `paperType` is set | `renderMetadata()` — reflows per type; embeds EE or CP fieldset inline |
| 3 | `authors` | when not `isIbSample` | `renderAuthors()` |
| 4 | `file` | always (after type chosen) | `renderFile()` |
| 5 | `review` | always (after type chosen) | `renderReview()` |

Forward navigation is allowed even with empty fields (per mockup); the Review step renders the missing-fields summary that links back to each offending step.

### 5.4 Server-field mapping

The `serializeToForm()` contract — single place where wizard state → server field names. Matches the existing route's expectations exactly.

| Wizard state | Hidden input name(s) | Notes |
|---|---|---|
| `paperType === 'ee'` | `is_ib_ee=1` | only when EE |
| `paperType === 'cp'` | `is_cp_paper=1` | only when CP |
| `isIbSample` (EE/CP only) | `is_ib_sample=1` | hidden for Standard |
| `title` | `title` | — |
| `language` | `language` | — |
| `category` | `category` | — |
| `keywords` (Standard only) | `keywords` | comma-joined |
| `abstract` (Standard only) | `abstract` | — |
| `authors[]` | `author_name[]`, `author_email[]`, `author_school[]` | one entry per author, in order |
| `eeCoreSubject` | `ib_ee_core_subject` | EE only |
| `eeInterSubject` | `ib_ee_interdisciplinary_subject` | EE only, may be empty |
| `eeScores.X` | `ib_crit_X_score` | A..E, EE only |
| `eeComments.X` (when `eeIncludeComments`) | `ib_crit_X_comment` | A..E, EE only |
| `eeComments.holistic` (when `eeIncludeComments`) | `ib_holistic_comment` | EE only |
| `cpGlobalContext` | `cp_global_context` | CP only |
| `cpActionTypes` | `cp_action_type` (repeated) | CP only |
| `cpScores.X` | `cp_crit_X_score` | A..D, CP only |
| (no submitted total) | — | server recomputes EE total from criteria; never trust client |
| `draft_id` (from boot) | `draft_id` | hidden, round-trips |
| File input | `paper` | the real `<input type=file>` lives inside `#uploadForm` already |

### 5.5 Reusable internal components

- **`renderCombobox(id, value, placeholder, groups)`** + `bindComboboxes()` — searchable grouped dropdown. Used for EE core/inter subject and CP global context. Closes on outside click. Empty-search shows "No matches".
- **`renderChips(items)`** + chip handlers — keywords input. Enter or comma adds, Backspace-on-empty removes last.
- **`renderSegmented(options, value)`** — language toggle.
- **`renderCritTable(criteria, scores, onChange)`** — score table used by both EE and CP.
- **`renderTotalReadout(label, sub, value, max)`** — the EE total / CP average card under the criterion table.

These stay inline in `upload-wizard.js`. Each is one function — splitting into separate files would obscure that they all share the same state object and binding pattern.

### 5.6 Things deliberately not abstracted

- The five per-step renderers stay inline rather than going through a generic engine. The steps are heterogeneous enough (Type is button-cards, Metadata reflows by type, Review is dl-based summary) that a generic engine would obscure more than it saves.
- No framework. The wizard is ~600 lines of vanilla JS; introducing Alpine/htmx for one page would violate the codebase's existing pattern.

---

## 6. Error handling and edge cases

| Surface | Behavior |
|---|---|
| User submits with empty required field | Server's per-type cascade flashes the message and re-renders. Wizard rehydrates from round-tripped `form_data`, jumps to Review step on init when missing-fields exist, summary appears. |
| User uploads non-PDF | Existing `allowed_file()` check stays; flash; wizard rehydrates. |
| File > 50 MB | Flask's `MAX_CONTENT_LENGTH` (50 MB, from `PAPERQUERY_MAX_UPLOAD_MB`) returns 413; existing handler shows the message. No wizard change. |
| User switches paper type mid-wizard after filling EE scores | Scores stay in `state` but aren't serialized when `paperType !== 'ee'`. Switching back restores them. |
| User checks IB Sample after filling Authors | Authors stay in state, not serialized on submit, Authors step removed from stepper. Unchecking restores it. |
| Network drops during Save Draft | Standard form POST; browser shows its own error. localStorage mirror is the safety net. |
| localStorage disabled / quota full | `mirrorToLocalStorage()` wrapped in try/catch; silently degrades to manual-save-only. No user-visible error. |
| Two tabs open editing the same draft | Last-write-wins on the server. localStorage mirror is per-browser; the second tab on `init()` sees the Restore/Discard banner. |
| Draft contains malformed `ib_ee_data` / `cp_data` JSON | `parse_*_for_form` returns `{}`; wizard treats those fields as blank. No crash. |
| i18n missing translation key | `i18n(key, fallback)` returns the English fallback string. |
| User on a browser without modern JS | We use only `localStorage`, `querySelector`, `classList`, `addEventListener` — same baseline as existing `dashboard.js`. No `fetch`, no async/await. |
| EE total submitted by client tampering | Server recomputes total from per-criterion scores in `build_ib_ee_data_from_form` and ignores any `ib_total_grade_number` input. Existing invariant, preserved. |

---

## 7. Tests

### 7.1 Updated

- **`tests/test_upload_template.py`** — re-derive `declared_ids` / `referenced_ids` from the new template. Mount points are `wizardStepper`, `wizardSteps`, `wizardFooter`, `uploadForm`, `autosaveIndicator`. The JS side moves to a separate file (`static/js/upload-wizard.js`); test should also assert that every `getElementById` call in `upload-wizard.js` resolves to an ID declared in the rendered template.
- **`tests/test_ee_total_grade_contract.py`** — rewrite. Keep the invariant (EE total computed from criteria, server ignores submitted total field). New assertions:
  - `build_ib_ee_data_from_form` still uses `str(total_score)` (no change — already correct, just re-assert).
  - The wizard JS source contains the client-side total reduction over `state.eeScores` (regex check in `static/js/upload-wizard.js`).
  - `serializeToForm()` in the wizard does NOT write a hidden input named `ib_total_grade_number` (regex / source check).

### 7.2 New

- **`tests/test_upload_wizard_contract.py`** — three contract checks on `app.py`:
  1. `upload()` validator does not flash `"Please enter keywords"` or `"Please enter the abstract"` when `is_ib_ee=1` or `is_cp_paper=1`. AST walk on the function body confirms the per-type cascade lives in a single list comprehension or loop, not a flat if-chain on those keys.
  2. `wizard_boot` keys passed to `render_template` include `{submit_url, draft_id, form_data, paper_categories, ee_subjects, cp_global_contexts, cp_action_types, user_key, i18n}`.
  3. The draft-load branch (when `draft.get("status") == "draft"`) calls `parse_ib_ee_data_for_form` and `parse_cp_data_for_form` when the corresponding JSON fields are present on the draft.

- **`tests/test_upload_wizard_dom_contract.py`** — Jinja render check:
  - Render `templates/upload.html` with a mock `wizard_boot` context.
  - Assert: three wizard mount divs exist; panel head contains the autosave pill (`#autosaveIndicator`); hidden `<form id="uploadForm" method="post" enctype="multipart/form-data">` is present with `action` matching `submit_url`; the boot script tag exists and contains the JSON.

### 7.3 Manual test plan

Each scenario should be exercised in the browser before sign-off:

1. **Fresh Standard paper, single author** — fill all fields, upload PDF, submit. Confirm published (admin role) or pending (reader role).
2. **Fresh EE submission, three criteria scored, commentaries enabled** — confirm total readout updates live, server-side EE total matches sum, commentary fields persist.
3. **Fresh CP submission, two action types selected** — confirm pill-checks toggle, CP average displays, server-side CP total matches mean.
4. **EE marked as IB Sample** — Authors step disappears from stepper, server stores `IB SAMPLE` as author name.
5. **Save draft → reload → continue editing** — full round-trip for Standard, EE, and CP drafts. EE scores, subjects, and comments all reappear.
6. **Missing-field summary on Review** — submit half-filled wizard, confirm summary lists each missing field with a working "go to step" link; submit with everything filled, confirm the alert turns green and Submit Paper succeeds.
7. **localStorage Restore banner** — fill fields, close tab without saving, reopen `/dashboard/upload`, confirm Restore/Discard banner appears and Restore works.
8. **Tampered EE total** — POST `ib_total_grade_number=99` directly with curl; confirm stored `total_grade_number` is the computed sum, not 99.

---

## 8. Out of scope

- Real debounced server autosave (a future enhancement; the hybrid choice keeps this option open without paying its cost now).
- Drag-and-drop file dropzone (the mockup uses a plain file input; can be added later behind the same `renderFile()` boundary).
- Multi-file uploads.
- Inline PDF preview on the File step.
- The `Keydion-Submission Page/js/tweaks.js` floating panel — design-review tool only; not shipped.
- Mobile-specific layout tuning beyond what `dashboard.css` already provides via existing breakpoints.

---

## 9. Open questions

None at sign-off.
