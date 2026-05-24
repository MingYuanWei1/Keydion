# EE Commentary PDF Auto-fill — Design

**Date:** 2026-05-24
**Scope:** Upload wizard, EE (Extended Essay) section only

## Problem

Curators submitting IB Extended Essays must today re-type the same data that already exists in IB's official "Extended essay: commentary for example essay" PDF: DP subject(s), research question, criteria A–E scores and comments, and the holistic comment. This is tedious and error-prone.

Goal: let a user upload that commentary PDF on the EE metadata step and have the form fields auto-populate.

## Non-goals

- Multi-language IB commentary PDFs (only English template handled)
- Scanned / image-based PDFs (no OCR)
- Auto-fill for CP (Community Project) papers — EE-only
- Adding a new "Interdisciplinary framework" field to the data model
- Extracting metadata from the actual essay PDF itself (separate problem)

## User-facing behavior

1. User selects **Extended Essay** as paper type in the upload wizard.
2. On the EE metadata step, an **"Auto-fill from commentary PDF"** button appears at the top of the EE fieldset.
3. User clicks it, a file picker opens, user selects an IB commentary PDF.
4. Backend parses the PDF and returns extracted fields.
5. **Overwrite gate:** if any EE field already has user content (`eeCoreSubject`, `eeInterSubject`, any `eeScores[*]`, any `eeComments[*]`, or `title`), a `confirm()` dialog asks: *"Replace your existing EE entries with values from the PDF?"* — user must confirm.
6. Confirmed values are written into wizard `state.*`; the EE fieldset re-renders with populated fields.
7. An inline status line below the button summarises the outcome: *"Extracted 12 of 13 fields. <warnings>"* — green if all parsed, amber if partial, red on error.

   (Field count = 1 core subject + 1 research question + 5 criterion scores + 5 criterion comments + 1 holistic comment = **13** for a subject-focused EE; **14** for an interdisciplinary EE because `interdisciplinary_subject` joins the count. `framework` is informational only and contributes a warning, not a counted field.)

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ static/js/upload-wizard.js   (existing, edited)              │
│  • renderEEFieldset(): prepend autofill button + status      │
│  • bindEEFieldset(): click → file picker → POST → mutate     │
│    state → re-render                                         │
└────────────────────────┬─────────────────────────────────────┘
                         │ POST /api/upload/extract-ee-metadata
                         │ multipart/form-data
                         ▼
┌──────────────────────────────────────────────────────────────┐
│ app.py   (existing, edited — adds one route)                 │
│  • require_login(2) guard                                    │
│  • validates file (PDF magic, size)                          │
│  • delegates to ee_pdf_extractor.extract_ee_metadata         │
│  • returns JSON                                              │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│ ee_pdf_extractor.py   (new module)                           │
│  1. _try_pdfplumber(bytes) → table-based extraction          │
│  2. _try_regex(bytes)      → PyPDF2 text + anchor regex      │
│     (fallback when (1) fails or returns no usable rows)      │
│  3. _normalize_subjects()  → match against ee_subjects.json  │
│  4. assemble result dict + warnings list                     │
└──────────────────────────────────────────────────────────────┘
```

### Why a new module (not more code in `app.py`)?

`app.py` is already ~3,500 lines. The extractor has a clear single responsibility, no Flask coupling, and is much easier to unit-test as a free function.

## Backend — `ee_pdf_extractor.py`

### Public API

```python
class EePdfExtractionError(Exception):
    """Raised when the PDF cannot be processed at all (corrupt, encrypted, scanned)."""

def extract_ee_metadata(file_bytes: bytes) -> dict:
    """
    Parse an IB 'Extended essay: commentary for example essay' PDF.

    Returns:
      {
        "core_subject": str,                  # e.g. "Biology" or "" if not present / not recognised
        "interdisciplinary_subject": str,     # second subject for interdisciplinary EEs, else ""
        "framework": str,                     # interdisciplinary framework name, "" otherwise
        "research_question": str,
        "criteria": {
          "A": {"score": int | None, "comment": str},
          "B": {...}, "C": {...}, "D": {...}, "E": {...},
        },
        "holistic_comment": str,
        "warnings": list[str],                # human-readable, surfaced to the user
      }

    Raises:
      EePdfExtractionError if no text could be extracted at all
      (e.g. encrypted, corrupt, or pure-image PDF).
    """
```

### Parsing pipeline

1. **`_try_pdfplumber(bytes) -> dict | None`**
   - Open with `pdfplumber.open(io.BytesIO(file_bytes))`.
   - Call `page.extract_tables()` on each page.
   - Walk rows looking for known left-column labels: `"DP subject:"`, `"DP subjects:"`, `"Interdisciplinary framework:"`, `"Research question:"`, `"A:"` … `"E:"`, `"Total marks awarded"`, `"Holistic comment on the essay:"`.
   - For each match, pull the value from the adjacent right column(s).
   - Returns a partial dict on success; `None` if no recognised structure found (signal to fall through).

2. **`_try_regex(bytes) -> dict`**
   - Extract concatenated text via `PyPDF2.PdfReader`.
   - Apply anchor-based regex patterns (compiled once at module load). Example patterns:
     - Core subject: `r"DP subject:\s*\(Subject-focused essay\)\s*(.+?)(?=DP subjects:)"` (with `re.DOTALL`)
     - Research question: `r"Research question:\s*(.+?)(?=Assessment details)"`
     - Per-criterion block: `r"^([A-E]):\s.+?\[Maximum possible\s*mark:\s*\d+\]\s*(\d+)\s+(.+?)(?=^[A-E]:|^Total marks)"` (multiline)
     - Holistic: `r"Holistic comment on\s*the essay:\s*(.+?)$"`
   - Always returns a dict — missing strings default to `""`, missing scores default to `None`, and every missing field adds a warning.

3. **`_normalize_subjects(raw_core: str, raw_inter: str) -> tuple[str, str, list[str]]`**
   - Load `data/ee_subjects.json` once (module-level cache).
   - Build a flat set of canonical subject names.
   - For each raw value, do case-insensitive exact match.
   - On miss, return `""` for that field and add a warning: *"Subject 'X' not recognised — please pick from the dropdown manually."*

4. **Assembly**
   - Use the pdfplumber result if it covers most fields; merge regex results for any fields it missed.
   - If framework is non-empty, add a warning: *"Interdisciplinary framework 'X' has no field on this form — please add it to the holistic comment if relevant."*
   - For any field that came back empty after both passes, add a warning like *"Could not extract: research question, Criterion D score."*

### New dependency

Add `pdfplumber>=0.11` to `requirements.txt`. PyPDF2 stays for the regex fallback path (it's already a transitive dep of pdfplumber too).

## Backend — new route in `app.py`

```python
@app.route("/api/upload/extract-ee-metadata", methods=["POST"])
@require_login(2)
def api_extract_ee_metadata():
    file = request.files.get("file")
    if not file or not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": _("File must be a PDF")}), 400

    raw = file.read()
    if not raw.startswith(b"%PDF-"):
        return jsonify({"error": _("File is not a valid PDF")}), 400

    try:
        result = extract_ee_metadata(raw)
    except EePdfExtractionError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify(result), 200
```

- **Auth:** `require_login(2)` matches the upload page's existing gate (Contributor role).
- **Size limit:** relies on Flask's `MAX_CONTENT_LENGTH`; a `413` propagates to JS automatically.
- **i18n:** error strings wrapped in `_()` consistent with the rest of `app.py`.

## Frontend — `static/js/upload-wizard.js`

### State additions

In the `state` initializer block (around line 29–49):

```js
eeAutofillStatus: '',    // '' | 'loading' | 'ok' | 'partial' | 'error'
eeAutofillMessage: '',   // human-readable text shown next to the button
```

### `renderEEFieldset()` — prepend an autofill block

```html
<div class="ee-autofill mb-3">
  <button type="button" id="eeAutofillBtn" class="btn btn-outline-primary btn-sm">
    ${t('ee_autofill_btn', 'Auto-fill from commentary PDF')}
  </button>
  <input type="file" id="eeAutofillFile" accept="application/pdf,.pdf" hidden>
  <span id="eeAutofillStatus" class="ee-autofill__status ee-autofill__status--${state.eeAutofillStatus || 'idle'}">
    ${escapeHtml(state.eeAutofillMessage)}
  </span>
</div>
```

The status span uses CSS classes (`--ok`, `--partial`, `--error`) for green/amber/red. Add a small style block to `static/css/upload.css`.

### `bindEEFieldset()` — wire it up

1. Click on `#eeAutofillBtn` → `document.getElementById('eeAutofillFile').click()`.
2. `change` on `#eeAutofillFile`:
   - Disable button, set `state.eeAutofillStatus = 'loading'`, message *"Extracting…"*, re-render.
   - `fetch('/api/upload/extract-ee-metadata', { method: 'POST', body: <FormData with file> })`.
3. On response:
   - If non-2xx: `state.eeAutofillStatus = 'error'`, message from `error` field, re-render. Done.
   - If 2xx: check dirty flag.
     - **Dirty flag:** any non-empty value in `state.eeCoreSubject`, `state.eeInterSubject`, `state.title`, `state.eeScores[A..E]`, `state.eeComments[A..E]`, `state.eeComments.holistic`.
     - If dirty → `if (!confirm(t('ee_autofill_overwrite', 'Replace your existing EE entries with values from the PDF?'))) { reset status; return; }`.
   - Mutate state:
     ```js
     state.title = data.research_question || state.title;
     state.eeCoreSubject = data.core_subject || '';
     state.eeInterSubject = data.interdisciplinary_subject || '';
     ['A','B','C','D','E'].forEach(k => {
       state.eeScores[k] = data.criteria[k].score ?? '';
       state.eeComments[k] = data.criteria[k].comment || '';
     });
     state.eeComments.holistic = data.holistic_comment || '';
     if (data.holistic_comment || Object.values(state.eeComments).some(Boolean)) {
       state.eeIncludeComments = true;
     }
     ```
   - Compute filled / total count, build status message:
     - All filled, no warnings → `'ok'`, *"Extracted all fields."*
     - Some warnings → `'partial'`, *"Extracted N/M fields. <warnings joined by '; '>"* (M = 13 for subject-focused, 14 for interdisciplinary)
   - Re-render the fieldset (existing render path).

### CSS additions (small)

Add to `static/css/upload.css`:

```css
.ee-autofill { display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; }
.ee-autofill__status { font-size: 0.875rem; }
.ee-autofill__status--ok      { color: var(--bs-success); }
.ee-autofill__status--partial { color: var(--bs-warning); }
.ee-autofill__status--error   { color: var(--bs-danger); }
.ee-autofill__status--loading { color: var(--bs-secondary); }
```

## Error handling

| Failure | Response |
|---|---|
| Non-PDF file | Browser filter; backend `400 "File must be a PDF"` |
| File over `MAX_CONTENT_LENGTH` | Flask `413`; JS shows *"File too large"* |
| Encrypted / corrupt PDF | Backend `400 "Could not read PDF"` |
| Pure-image PDF (no text) | Backend `400 "No readable text — is this a scanned image?"` |
| Unknown template / missing fields | `200` with partial fill + warnings list |
| Subject not in `ee_subjects.json` | Field blank + warning *"Subject 'X' not recognised…"* |
| Network / server error | JS catch → red status *"Auto-fill failed — try again or fill manually"* |

No retries, no queuing — extraction is a one-shot synchronous call.

## Testing

Follows the project's contract-test pattern (`tests/test_*.py`, `unittest`).

### `tests/test_ee_pdf_extractor.py` — unit tests

Fixture files in `tests/fixtures/`:

- `ee_commentary_subject_focused.pdf` — copy of `test_function.pdf` from repo root
- `ee_commentary_interdisciplinary.pdf` — to be sourced or synthesised; if unavailable at implementation time, this test is skipped and tracked as a follow-up
- `not_an_ee.pdf` — any non-IB PDF
- (Optional) a fixture with a deliberately missing criterion to assert graceful partial fill

Cases:

- Subject-focused fixture: asserts all 13 fields parsed correctly (`core_subject == "Biology"`, RQ, A–E scores `[4,4,4,6,3]` with non-empty comments, holistic comment non-empty, empty warnings list).
- Interdisciplinary fixture: asserts both `core_subject` and `interdisciplinary_subject` set, `framework` non-empty, warning surfaced about framework.
- Non-IB PDF: asserts low or zero field count + warnings populated, **no exception**.
- Encrypted PDF: asserts `EePdfExtractionError` raised.
- Unknown subject string: asserts blank `core_subject` + recognised warning.

### `tests/test_ee_extract_route_contract.py` — AST-parses `app.py`

- Asserts a route exists at `/api/upload/extract-ee-metadata`.
- Asserts it is decorated with `require_login(2)`.
- Asserts it imports / calls `extract_ee_metadata`.

### Manual smoke test

No JS test harness in this project. After implementation, manually:

1. Start dev server (`./start_local.sh`).
2. Log in as a contributor, open the upload wizard, pick **Extended Essay**.
3. Click **Auto-fill from commentary PDF**, select `test_function.pdf`.
4. Verify fields populate, RQ becomes the title, scores `[4,4,4,6,3]`, comments and holistic present.
5. Type something into a field first, repeat — verify the overwrite confirm fires.
6. Upload a non-PDF, a scanned PDF, and a corrupt PDF — verify error UX.

## Internationalisation

- Error messages from the route wrapped in `_()`.
- JS button label, status messages, and confirm dialog text use the existing wizard `t(key, fallback)` helper.
- New keys to add to `translations/<locale>/LC_MESSAGES/messages.po`:
  - `ee_autofill_btn` — *"Auto-fill from commentary PDF"*
  - `ee_autofill_overwrite` — *"Replace your existing EE entries with values from the PDF?"*
  - `ee_autofill_extracting` — *"Extracting…"*
  - `ee_autofill_ok` — *"Extracted all fields."*
  - `ee_autofill_partial` — *"Extracted %(filled)s of %(total)s fields. %(warnings)s"*
  - `ee_autofill_error` — *"Auto-fill failed — try again or fill manually."*
  - Error string from the route ("File must be a PDF", etc.)
- After editing `.po` files, run `python tools/compile_translations.py`.

## Files touched

| File | Change |
|---|---|
| `requirements.txt` | + `pdfplumber>=0.11` |
| `ee_pdf_extractor.py` | **new** — extraction module |
| `app.py` | + import, + `POST /api/upload/extract-ee-metadata` route |
| `static/js/upload-wizard.js` | + state, + autofill block in `renderEEFieldset`, + handler in `bindEEFieldset` |
| `static/css/upload.css` | + `.ee-autofill*` rules |
| `translations/*/LC_MESSAGES/messages.po` | + new keys (then compile) |
| `tests/fixtures/ee_commentary_subject_focused.pdf` | **new** — copied from `test_function.pdf` |
| `tests/test_ee_pdf_extractor.py` | **new** |
| `tests/test_ee_extract_route_contract.py` | **new** |

## Open follow-ups (not blocking)

- Source or generate an interdisciplinary EE commentary fixture.
- If usage data later shows users routinely uploading interdisciplinary EEs, consider adding a proper "Interdisciplinary framework" field to the data model (deferred — out of scope here).
