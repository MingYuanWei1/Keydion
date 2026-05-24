# EE Commentary PDF Auto-fill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an "Auto-fill from commentary PDF" button to the EE step of the upload wizard that parses an IB "Extended essay: commentary for example essay" PDF and populates the EE form fields.

**Architecture:** New `ee_pdf_extractor.py` module (pdfplumber-first with PyPDF2/regex fallback) called by a new Flask JSON route `POST /api/upload/extract-ee-metadata`. The wizard's EE fieldset gets a new button + hidden file input + status line; on file pick it POSTs the PDF, applies the response to wizard state (after an overwrite-confirm if any EE field is dirty), and re-renders.

**Tech Stack:** Python 3 / Flask / PyPDF2 (already installed) / **pdfplumber (new dep)** / vanilla JS (no build step) / Flask-Babel for i18n / unittest for tests.

**Spec:** [`docs/superpowers/specs/2026-05-24-ee-pdf-autofill-design.md`](../specs/2026-05-24-ee-pdf-autofill-design.md)

**Conventions used by this project:**
- Routes live inside `def create_app()` in `app.py` (registered via `@app.route(...)`).
- `require_login(level=N)` is called *inside* the function and returns `user or None`. If `None`, return `jsonify({"error": "Unauthorized"}), 401`.
- Tests live in `tests/test_*.py` and use `unittest`. Run all tests with `python -m unittest discover -s tests -p "test_*.py" -v`.
- Conventional commit messages (`feat:`, `fix:`, `test:`, `docs:`, with optional scope).

---

## Task 1: Setup — add pdfplumber, copy fixture

**Files:**
- Modify: `requirements.txt`
- Create: `tests/fixtures/` (directory)
- Create: `tests/fixtures/ee_commentary_subject_focused.pdf` (copied from repo root `test_function.pdf`)

- [ ] **Step 1: Add pdfplumber to requirements.txt**

Append `pdfplumber>=0.11` after the existing `PyPDF2>=3.0` line. Final relevant block:

```
PyPDF2>=3.0
pdfplumber>=0.11
msal>=1.28
```

- [ ] **Step 2: Install the new dependency**

Run: `pip install -r requirements.txt`

Expected: pdfplumber and its dependencies (`pdfminer.six`, `pypdfium2`, etc.) install without error. Verify with `python -c "import pdfplumber; print(pdfplumber.__version__)"` — should print 0.11.x or newer.

- [ ] **Step 3: Create the fixtures directory and copy the sample PDF**

Run:
```bash
mkdir -p tests/fixtures
cp test_function.pdf tests/fixtures/ee_commentary_subject_focused.pdf
```

Verify with `ls tests/fixtures/` — should list `ee_commentary_subject_focused.pdf`.

- [ ] **Step 4: Commit**

```bash
git add requirements.txt tests/fixtures/ee_commentary_subject_focused.pdf
git commit -m "chore(deps): add pdfplumber for EE commentary PDF parsing"
```

---

## Task 2: Extractor skeleton — public API shape

Create the module with the public function and error class. First test asserts the **return shape** only, so we have a stable contract before filling in values.

**Files:**
- Create: `ee_pdf_extractor.py`
- Create: `tests/test_ee_pdf_extractor.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_ee_pdf_extractor.py`:

```python
import unittest
from pathlib import Path

from ee_pdf_extractor import extract_ee_metadata, EePdfExtractionError

FIXTURES = Path(__file__).resolve().parent / "fixtures"


class ExtractorShapeTest(unittest.TestCase):
    """The public API contract — shape and types — regardless of values."""

    @classmethod
    def setUpClass(cls):
        cls.pdf_bytes = (FIXTURES / "ee_commentary_subject_focused.pdf").read_bytes()

    def test_returns_dict_with_expected_top_level_keys(self):
        result = extract_ee_metadata(self.pdf_bytes)
        self.assertIsInstance(result, dict)
        for key in (
            "core_subject",
            "interdisciplinary_subject",
            "framework",
            "research_question",
            "criteria",
            "holistic_comment",
            "warnings",
        ):
            self.assertIn(key, result, f"missing key: {key}")

    def test_criteria_has_A_through_E(self):
        result = extract_ee_metadata(self.pdf_bytes)
        for letter in ("A", "B", "C", "D", "E"):
            self.assertIn(letter, result["criteria"])
            self.assertIn("score", result["criteria"][letter])
            self.assertIn("comment", result["criteria"][letter])

    def test_warnings_is_a_list(self):
        result = extract_ee_metadata(self.pdf_bytes)
        self.assertIsInstance(result["warnings"], list)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_ee_pdf_extractor -v`

Expected: `ModuleNotFoundError: No module named 'ee_pdf_extractor'`.

- [ ] **Step 3: Write the minimal skeleton module**

Create `ee_pdf_extractor.py`:

```python
"""IB Extended Essay 'commentary for example essay' PDF parser.

Public surface:
    extract_ee_metadata(file_bytes) -> dict
    EePdfExtractionError
"""

from __future__ import annotations


class EePdfExtractionError(Exception):
    """Raised when the PDF cannot be processed at all (corrupt, encrypted, scanned)."""


def _empty_result() -> dict:
    return {
        "core_subject": "",
        "interdisciplinary_subject": "",
        "framework": "",
        "research_question": "",
        "criteria": {letter: {"score": None, "comment": ""} for letter in "ABCDE"},
        "holistic_comment": "",
        "warnings": [],
    }


def extract_ee_metadata(file_bytes: bytes) -> dict:
    """Parse an IB EE commentary PDF. See module docstring for contract."""
    if not file_bytes:
        raise EePdfExtractionError("Empty file")
    return _empty_result()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_ee_pdf_extractor -v`

Expected: all three tests pass.

- [ ] **Step 5: Commit**

```bash
git add ee_pdf_extractor.py tests/test_ee_pdf_extractor.py
git commit -m "feat(ee-extractor): add module skeleton and shape contract tests"
```

---

## Task 3: Regex extraction path

Implement the PyPDF2 + anchor-regex parser. This is the **fallback** path in the architecture, but we implement it first because it's simpler and proves end-to-end behavior.

**Files:**
- Modify: `ee_pdf_extractor.py`
- Modify: `tests/test_ee_pdf_extractor.py`

- [ ] **Step 1: Add value tests that should pass after this task**

Append to `tests/test_ee_pdf_extractor.py`:

```python
class ExtractorValuesTest(unittest.TestCase):
    """Values extracted from the subject-focused sample PDF."""

    @classmethod
    def setUpClass(cls):
        cls.pdf_bytes = (FIXTURES / "ee_commentary_subject_focused.pdf").read_bytes()
        cls.result = extract_ee_metadata(cls.pdf_bytes)

    def test_core_subject_is_biology(self):
        # 'Biology' appears verbatim in the canonical ee_subjects.json,
        # so subject normalisation must preserve it as-is.
        self.assertEqual(self.result["core_subject"], "Biology")

    def test_interdisciplinary_fields_empty_for_subject_focused(self):
        self.assertEqual(self.result["interdisciplinary_subject"], "")
        self.assertEqual(self.result["framework"], "")

    def test_research_question_extracted(self):
        rq = self.result["research_question"]
        self.assertIn("alcohol production", rq.lower())
        self.assertIn("yeast", rq.lower())
        self.assertIn("fermentation", rq.lower())

    def test_scores_are_4_4_4_6_3(self):
        expected = {"A": 4, "B": 4, "C": 4, "D": 6, "E": 3}
        actual = {k: self.result["criteria"][k]["score"] for k in "ABCDE"}
        self.assertEqual(actual, expected)

    def test_every_criterion_has_a_non_empty_comment(self):
        for letter in "ABCDE":
            comment = self.result["criteria"][letter]["comment"]
            self.assertTrue(comment.strip(), f"criterion {letter} comment is empty")

    def test_holistic_comment_is_non_empty(self):
        self.assertTrue(self.result["holistic_comment"].strip())

    def test_no_warnings_on_clean_extraction(self):
        # All fields parsed cleanly → no warnings.
        self.assertEqual(self.result["warnings"], [])
```

- [ ] **Step 2: Run tests to confirm new ones fail**

Run: `python -m unittest tests.test_ee_pdf_extractor.ExtractorValuesTest -v`

Expected: 7 failures — values are all empty/None.

- [ ] **Step 3: Implement the regex parser**

Replace `ee_pdf_extractor.py` entirely with:

```python
"""IB Extended Essay 'commentary for example essay' PDF parser.

Public surface:
    extract_ee_metadata(file_bytes) -> dict
    EePdfExtractionError

Parsing strategy:
    Primary: pdfplumber table extraction (added in a later task).
    Fallback: PyPDF2 text + anchor regex.
"""

from __future__ import annotations

import io
import re
from typing import Optional

from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError


class EePdfExtractionError(Exception):
    """Raised when the PDF cannot be processed at all (corrupt, encrypted, scanned)."""


# ── regex anchors ────────────────────────────────────────────────────────────
#
# pdftotext/PyPDF2 emit the IB commentary form's table cells in roughly the
# visual order: each cell becomes a chunk separated by newlines. The exact
# whitespace is whitespace-insensitive in these patterns (we use \s+).

_RE_CORE_SUBJECT = re.compile(
    r"DP subject:\s*\(Subject-focused essay\)\s*(.+?)\s*DP subjects:",
    re.DOTALL,
)
_RE_INTER_SUBJECTS = re.compile(
    # Captures the cell after 'DP subjects: (Interdisciplinary essay)'.
    r"DP subjects:\s*\(Interdisciplinary essay\)\s*(.+?)\s*Interdisciplinary\s+framework:",
    re.DOTALL,
)
_RE_FRAMEWORK = re.compile(
    r"Interdisciplinary\s+framework:\s*(.+?)\s*Research question:",
    re.DOTALL,
)
_RE_RESEARCH_QUESTION = re.compile(
    r"Research question:\s*(.+?)\s*Assessment details",
    re.DOTALL,
)
_RE_HOLISTIC = re.compile(
    r"Holistic comment on\s*the essay:\s*(.+?)\s*\Z",
    re.DOTALL,
)
# A criterion block: 'A: Framework for the essay [Maximum possible mark: 6]  4  <commentary…>'
# Stops at the next criterion or at 'Total marks awarded'.
_RE_CRITERION = re.compile(
    r"(?P<letter>[A-E]):\s*[^\[]*?\[Maximum possible\s*mark:\s*\d+\]\s*"
    r"(?P<score>\d+)\s+(?P<comment>.+?)"
    r"(?=(?:[A-E]:\s*[^\[]*?\[Maximum possible)|Total marks awarded|\Z)",
    re.DOTALL,
)


def _empty_result() -> dict:
    return {
        "core_subject": "",
        "interdisciplinary_subject": "",
        "framework": "",
        "research_question": "",
        "criteria": {letter: {"score": None, "comment": ""} for letter in "ABCDE"},
        "holistic_comment": "",
        "warnings": [],
    }


def _read_pdf_text(file_bytes: bytes) -> str:
    """Concatenate text from every page of the PDF, or raise on failure."""
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except PdfReadError as exc:
        raise EePdfExtractionError(f"Could not read PDF: {exc}") from exc

    if reader.is_encrypted:
        raise EePdfExtractionError("PDF is encrypted")

    parts = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:  # pragma: no cover - PyPDF2 can throw a variety
            parts.append("")
    text = "\n".join(parts).strip()
    if not text:
        raise EePdfExtractionError("No readable text — is this a scanned image?")
    return text


def _collapse(value: str) -> str:
    """Collapse internal whitespace runs to single spaces and strip."""
    return re.sub(r"\s+", " ", value).strip()


def _extract_via_regex(text: str) -> dict:
    """Apply anchor regex to extracted text. Always returns a partial dict."""
    result = _empty_result()

    if m := _RE_CORE_SUBJECT.search(text):
        result["core_subject"] = _collapse(m.group(1))

    if m := _RE_INTER_SUBJECTS.search(text):
        # The interdisciplinary cell may contain two subjects (comma- or
        # 'and'-separated). The current data model has a single field for the
        # second subject, so we keep the full string as captured and let
        # downstream normalisation decide. The first part will be taken as
        # core_subject, the rest as interdisciplinary_subject.
        raw = _collapse(m.group(1))
        parts = re.split(r"\s*(?:,|\band\b|;|/)\s*", raw, maxsplit=1)
        if parts:
            result["core_subject"] = result["core_subject"] or parts[0]
            if len(parts) > 1:
                result["interdisciplinary_subject"] = parts[1]

    if m := _RE_FRAMEWORK.search(text):
        result["framework"] = _collapse(m.group(1))

    if m := _RE_RESEARCH_QUESTION.search(text):
        result["research_question"] = _collapse(m.group(1))

    for m in _RE_CRITERION.finditer(text):
        letter = m.group("letter")
        try:
            score = int(m.group("score"))
        except ValueError:
            score = None
        comment = _collapse(m.group("comment"))
        # Strip a trailing '[Maximum possible mark: N]' header chunk that the
        # next criterion's anchor sometimes leaves dangling on a line that
        # belongs to this comment.
        comment = re.sub(r"\s*\[Maximum possible\s*mark:\s*\d+\]\s*$", "", comment)
        result["criteria"][letter] = {"score": score, "comment": comment}

    if m := _RE_HOLISTIC.search(text):
        result["holistic_comment"] = _collapse(m.group(1))

    return result


def extract_ee_metadata(file_bytes: bytes) -> dict:
    """Parse an IB EE commentary PDF. See module docstring for contract."""
    if not file_bytes:
        raise EePdfExtractionError("Empty file")

    text = _read_pdf_text(file_bytes)
    result = _extract_via_regex(text)
    return result
```

- [ ] **Step 4: Run all extractor tests**

Run: `python -m unittest tests.test_ee_pdf_extractor -v`

Expected: all tests pass. If a regex misses a field on the fixture, inspect the actual extracted text with:

```bash
python -c "from PyPDF2 import PdfReader; print('\n'.join(p.extract_text() for p in PdfReader('tests/fixtures/ee_commentary_subject_focused.pdf').pages))"
```

Adjust the relevant anchor pattern until tests pass. **Do not weaken the assertions.**

- [ ] **Step 5: Commit**

```bash
git add ee_pdf_extractor.py tests/test_ee_pdf_extractor.py
git commit -m "feat(ee-extractor): implement PyPDF2 + regex extraction path"
```

---

## Task 4: Subject normalization + framework warning

Map the raw subject strings to the canonical names in `data/ee_subjects.json`; surface a warning when the framework is non-empty (no schema slot for it) or a subject doesn't match.

**Files:**
- Modify: `ee_pdf_extractor.py`
- Modify: `tests/test_ee_pdf_extractor.py`

- [ ] **Step 1: Add tests**

Append to `tests/test_ee_pdf_extractor.py`:

```python
class SubjectNormalisationTest(unittest.TestCase):
    """Subjects are matched (case-insensitive exact) against ee_subjects.json."""

    def test_known_subject_lower_case_normalised(self):
        # Build a tiny synthetic PDF? Too heavy. Instead, monkey-patch the
        # extraction path: call the private helper directly.
        from ee_pdf_extractor import _normalise_subject

        self.assertEqual(_normalise_subject("biology")[0], "Biology")
        self.assertEqual(_normalise_subject("BIOLOGY")[0], "Biology")
        self.assertEqual(_normalise_subject("Biology")[0], "Biology")

    def test_unknown_subject_returns_blank_and_warning(self):
        from ee_pdf_extractor import _normalise_subject

        value, warning = _normalise_subject("Quantum Underwater Basketweaving")
        self.assertEqual(value, "")
        self.assertIn("Quantum Underwater Basketweaving", warning)

    def test_empty_subject_returns_blank_no_warning(self):
        from ee_pdf_extractor import _normalise_subject

        self.assertEqual(_normalise_subject("")[0], "")
        self.assertIsNone(_normalise_subject("")[1])


class FrameworkWarningTest(unittest.TestCase):
    """Interdisciplinary framework value must produce a guidance warning."""

    def test_framework_value_produces_warning(self):
        # Reach into the assembler with a fabricated parsed-dict.
        from ee_pdf_extractor import _finalise_warnings

        partial = {
            "core_subject": "Biology",
            "interdisciplinary_subject": "",
            "framework": "Culture, language and identity",
            "research_question": "RQ",
            "criteria": {l: {"score": 4, "comment": "c"} for l in "ABCDE"},
            "holistic_comment": "h",
            "warnings": [],
        }
        _finalise_warnings(partial)
        joined = " | ".join(partial["warnings"])
        self.assertIn("framework", joined.lower())
        self.assertIn("Culture, language and identity", joined)

    def test_missing_field_produces_warning(self):
        from ee_pdf_extractor import _finalise_warnings

        partial = {
            "core_subject": "",
            "interdisciplinary_subject": "",
            "framework": "",
            "research_question": "",
            "criteria": {l: {"score": None, "comment": ""} for l in "ABCDE"},
            "holistic_comment": "",
            "warnings": [],
        }
        _finalise_warnings(partial)
        joined = " | ".join(partial["warnings"]).lower()
        self.assertIn("could not extract", joined)
```

- [ ] **Step 2: Run new tests to confirm failure**

Run: `python -m unittest tests.test_ee_pdf_extractor.SubjectNormalisationTest tests.test_ee_pdf_extractor.FrameworkWarningTest -v`

Expected: failures with `ImportError` (helpers don't exist yet).

- [ ] **Step 3: Add the helpers and wire them into `extract_ee_metadata`**

Edit `ee_pdf_extractor.py`. Add these imports near the top:

```python
import json
from functools import lru_cache
from pathlib import Path
```

Add this constant under the existing regex constants:

```python
_DATA_DIR = Path(__file__).resolve().parent / "data"
```

Add two new helpers (place them above `extract_ee_metadata`):

```python
@lru_cache(maxsize=1)
def _canonical_subjects() -> dict:
    """Return a {lowercase_name: CanonicalName} mapping from ee_subjects.json."""
    path = _DATA_DIR / "ee_subjects.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out: dict[str, str] = {}
    for group in data.get("groups", []):
        for subject in group.get("subjects", []):
            out[subject.lower()] = subject
    return out


def _normalise_subject(raw: str) -> tuple[str, Optional[str]]:
    """Map a raw subject string to a canonical IB subject name.

    Returns (canonical_name, warning_or_None). Empty input returns ("", None)
    — only an unrecognised non-empty value yields a warning.
    """
    raw = (raw or "").strip()
    if not raw:
        return "", None
    canonical = _canonical_subjects().get(raw.lower())
    if canonical:
        return canonical, None
    return "", (
        f"Subject '{raw}' not recognised — please pick from the dropdown manually."
    )


def _finalise_warnings(result: dict) -> None:
    """Append warnings for missing fields and the framework gap. Mutates in place."""
    missing: list[str] = []
    if not result["research_question"]:
        missing.append("research question")
    if not result["core_subject"]:
        missing.append("core subject")
    for letter in "ABCDE":
        crit = result["criteria"][letter]
        if crit["score"] is None:
            missing.append(f"Criterion {letter} score")
    if missing:
        result["warnings"].append(
            f"Could not extract: {', '.join(missing)}. Please fill these fields manually."
        )
    if result["framework"]:
        result["warnings"].append(
            f"Interdisciplinary framework '{result['framework']}' has no field on this form — "
            "please add it to the holistic comment if relevant."
        )
```

Now wire normalisation and warnings into the main function. Replace the body of `extract_ee_metadata` with:

```python
def extract_ee_metadata(file_bytes: bytes) -> dict:
    """Parse an IB EE commentary PDF. See module docstring for contract."""
    if not file_bytes:
        raise EePdfExtractionError("Empty file")

    text = _read_pdf_text(file_bytes)
    result = _extract_via_regex(text)

    # Normalise both subject fields against the canonical IB subject list.
    core, core_warn = _normalise_subject(result["core_subject"])
    inter, inter_warn = _normalise_subject(result["interdisciplinary_subject"])
    result["core_subject"] = core
    result["interdisciplinary_subject"] = inter
    if core_warn:
        result["warnings"].append(core_warn)
    if inter_warn:
        result["warnings"].append(inter_warn)

    _finalise_warnings(result)
    return result
```

- [ ] **Step 4: Run the full extractor test file**

Run: `python -m unittest tests.test_ee_pdf_extractor -v`

Expected: every test passes. `test_no_warnings_on_clean_extraction` should still pass because Biology is in `ee_subjects.json` and the framework is empty for the subject-focused fixture.

- [ ] **Step 5: Commit**

```bash
git add ee_pdf_extractor.py tests/test_ee_pdf_extractor.py
git commit -m "feat(ee-extractor): normalise subjects and surface framework warning"
```

---

## Task 5: pdfplumber primary path

Layer pdfplumber's table extraction in front of the regex path. If pdfplumber yields a usable result, use it; otherwise fall through to regex (which we already have).

**Files:**
- Modify: `ee_pdf_extractor.py`
- Modify: `tests/test_ee_pdf_extractor.py`

- [ ] **Step 1: Add a sanity test**

Append to `tests/test_ee_pdf_extractor.py`:

```python
class PdfplumberPathTest(unittest.TestCase):
    """The pdfplumber primary path returns a usable dict for the fixture."""

    @classmethod
    def setUpClass(cls):
        cls.pdf_bytes = (FIXTURES / "ee_commentary_subject_focused.pdf").read_bytes()

    def test_pdfplumber_path_alone_extracts_subject_and_scores(self):
        from ee_pdf_extractor import _extract_via_pdfplumber

        result = _extract_via_pdfplumber(self.pdf_bytes)
        self.assertIsNotNone(result, "pdfplumber path returned None on a clean fixture")
        # Pre-normalisation: raw 'Biology' string from the table cell.
        self.assertIn("Biology", result.get("core_subject", ""))
        # Scores should be parsed for at least the obvious criteria.
        a_score = result.get("criteria", {}).get("A", {}).get("score")
        self.assertEqual(a_score, 4)
```

- [ ] **Step 2: Run the new test to confirm failure**

Run: `python -m unittest tests.test_ee_pdf_extractor.PdfplumberPathTest -v`

Expected: `ImportError` for `_extract_via_pdfplumber`.

- [ ] **Step 3: Implement the pdfplumber path**

Edit `ee_pdf_extractor.py`. Add at the top with the other imports:

```python
import pdfplumber
```

Add this helper (place it above `extract_ee_metadata`, below `_extract_via_regex`):

```python
def _extract_via_pdfplumber(file_bytes: bytes) -> Optional[dict]:
    """Extract by walking pdfplumber's table rows. Returns None if unrecognised."""
    result = _empty_result()
    found_anything = False
    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            rows: list[list[str]] = []
            for page in pdf.pages:
                for table in page.extract_tables() or []:
                    for row in table:
                        rows.append([(cell or "").strip() for cell in row])
    except Exception:
        return None

    def _row_label(row: list[str]) -> str:
        return row[0] if row else ""

    def _row_value_cells(row: list[str]) -> list[str]:
        return [c for c in row[1:] if c]

    for row in rows:
        label = _row_label(row).lower()

        if label.startswith("dp subject:"):
            cells = _row_value_cells(row)
            if cells:
                result["core_subject"] = _collapse(cells[0])
                found_anything = True

        elif label.startswith("dp subjects:"):
            cells = _row_value_cells(row)
            if cells:
                raw = _collapse(" ".join(cells))
                parts = re.split(r"\s*(?:,|\band\b|;|/)\s*", raw, maxsplit=1)
                if parts:
                    if not result["core_subject"]:
                        result["core_subject"] = parts[0]
                    if len(parts) > 1:
                        result["interdisciplinary_subject"] = parts[1]
                found_anything = True

        elif label.startswith("interdisciplinary") and "framework" in label:
            cells = _row_value_cells(row)
            if cells:
                result["framework"] = _collapse(" ".join(cells))
                found_anything = True

        elif label.startswith("research question"):
            cells = _row_value_cells(row)
            if cells:
                result["research_question"] = _collapse(" ".join(cells))
                found_anything = True

        elif label.startswith("holistic comment"):
            cells = _row_value_cells(row)
            if cells:
                result["holistic_comment"] = _collapse(" ".join(cells))
                found_anything = True

        elif len(label) >= 2 and label[0].lower() in "abcde" and label[1] == ":":
            letter = label[0].upper()
            # Expected row layout: [letter-with-name, score, commentary]
            cells = _row_value_cells(row)
            # Score is usually the first numeric-only cell.
            score: Optional[int] = None
            comment_parts: list[str] = []
            for cell in cells:
                if score is None and cell.strip().isdigit():
                    score = int(cell.strip())
                else:
                    comment_parts.append(cell)
            if score is not None or comment_parts:
                result["criteria"][letter] = {
                    "score": score,
                    "comment": _collapse(" ".join(comment_parts)),
                }
                found_anything = True

    return result if found_anything else None
```

Update `extract_ee_metadata` to try pdfplumber first, merging missed fields from the regex fallback. Replace its body with:

```python
def extract_ee_metadata(file_bytes: bytes) -> dict:
    """Parse an IB EE commentary PDF. See module docstring for contract."""
    if not file_bytes:
        raise EePdfExtractionError("Empty file")

    # Always read the text first — also covers encrypted / scanned detection
    # before we attempt the more expensive pdfplumber pass.
    text = _read_pdf_text(file_bytes)

    plumber = _extract_via_pdfplumber(file_bytes)
    regex_result = _extract_via_regex(text)

    if plumber is None:
        result = regex_result
    else:
        # Merge: pdfplumber wins where it has a value; regex fills gaps.
        result = _empty_result()
        for key in ("core_subject", "interdisciplinary_subject", "framework",
                    "research_question", "holistic_comment"):
            result[key] = plumber.get(key) or regex_result.get(key) or ""
        for letter in "ABCDE":
            p_crit = plumber["criteria"].get(letter, {})
            r_crit = regex_result["criteria"].get(letter, {})
            result["criteria"][letter] = {
                "score": p_crit.get("score") if p_crit.get("score") is not None else r_crit.get("score"),
                "comment": p_crit.get("comment") or r_crit.get("comment") or "",
            }

    # Subject normalisation (same as before).
    core, core_warn = _normalise_subject(result["core_subject"])
    inter, inter_warn = _normalise_subject(result["interdisciplinary_subject"])
    result["core_subject"] = core
    result["interdisciplinary_subject"] = inter
    if core_warn:
        result["warnings"].append(core_warn)
    if inter_warn:
        result["warnings"].append(inter_warn)

    _finalise_warnings(result)
    return result
```

- [ ] **Step 4: Run every extractor test**

Run: `python -m unittest tests.test_ee_pdf_extractor -v`

Expected: every test passes. If pdfplumber returns `core_subject` as something like `"Biology\n"` (with the page-2 leakage of "(Subject-focused essay)"), update the row-walking logic — but **do not weaken the test assertions**.

- [ ] **Step 5: Commit**

```bash
git add ee_pdf_extractor.py tests/test_ee_pdf_extractor.py
git commit -m "feat(ee-extractor): add pdfplumber primary path with regex fallback"
```

---

## Task 6: Error path tests

Cover the failure modes the spec calls out: empty input, corrupt PDF, image-only PDF.

**Files:**
- Modify: `tests/test_ee_pdf_extractor.py`

- [ ] **Step 1: Add error tests**

Append to `tests/test_ee_pdf_extractor.py`:

```python
class ExtractorErrorPathsTest(unittest.TestCase):
    def test_empty_bytes_raise_extraction_error(self):
        with self.assertRaises(EePdfExtractionError):
            extract_ee_metadata(b"")

    def test_garbage_bytes_raise_extraction_error(self):
        with self.assertRaises(EePdfExtractionError):
            extract_ee_metadata(b"this is not a pdf, just bytes")

    def test_pdf_header_without_content_raises(self):
        # A bare PDF header is enough to start parsing but yields no text.
        # PyPDF2 will likely throw, which we wrap as EePdfExtractionError.
        with self.assertRaises(EePdfExtractionError):
            extract_ee_metadata(b"%PDF-1.4\n%%EOF\n")
```

- [ ] **Step 2: Run the new tests**

Run: `python -m unittest tests.test_ee_pdf_extractor.ExtractorErrorPathsTest -v`

Expected: all three pass. (`_read_pdf_text` already raises `EePdfExtractionError` on PyPDF2 failure and on empty extracted text.)

If `test_pdf_header_without_content_raises` *doesn't* fail and *doesn't* raise — PyPDF2 silently returns "" for the bogus PDF — the test will still pass because `_read_pdf_text` raises on empty text. If PyPDF2 throws an unexpected exception type (not `PdfReadError`), broaden the except in `_read_pdf_text` to also catch `Exception` and wrap it as `EePdfExtractionError`.

- [ ] **Step 3: Commit**

```bash
git add tests/test_ee_pdf_extractor.py
git commit -m "test(ee-extractor): cover empty/corrupt/empty-content PDF cases"
```

---

## Task 7: API route + route contract test

Add the JSON endpoint and an AST-based contract test in the style of the project's existing contract tests.

**Files:**
- Modify: `app.py`
- Create: `tests/test_ee_extract_route_contract.py`

- [ ] **Step 1: Write the failing contract test**

Create `tests/test_ee_extract_route_contract.py`:

```python
import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class EeExtractRouteContractTest(unittest.TestCase):
    """The /api/upload/extract-ee-metadata route must:
       - exist
       - require contributor (level=2) login
       - delegate to extract_ee_metadata from ee_pdf_extractor
    """

    @classmethod
    def setUpClass(cls):
        cls.source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_route_path_present(self):
        self.assertIn(
            '"/api/upload/extract-ee-metadata"',
            self.source,
            "API route path not registered with @app.route",
        )

    def test_imports_extractor(self):
        # Either 'from ee_pdf_extractor import extract_ee_metadata' or
        # 'import ee_pdf_extractor' is acceptable.
        self.assertTrue(
            re.search(r"from\s+ee_pdf_extractor\s+import\s+[^\n]*extract_ee_metadata", self.source)
            or re.search(r"import\s+ee_pdf_extractor", self.source),
            "ee_pdf_extractor must be imported in app.py",
        )

    def test_route_uses_require_login_level_2(self):
        # Find the function that owns the route decorator and verify it calls
        # require_login(level=2). We grep within ~40 lines following the route
        # to keep this robust to formatting tweaks.
        idx = self.source.index('"/api/upload/extract-ee-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertRegex(
            window,
            r"require_login\s*\(\s*level\s*=\s*2\s*\)",
            "route handler must call require_login(level=2)",
        )

    def test_route_calls_extractor(self):
        idx = self.source.index('"/api/upload/extract-ee-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertIn("extract_ee_metadata(", window, "route must call extract_ee_metadata(...)")

    def test_route_returns_json_on_extractor_error(self):
        idx = self.source.index('"/api/upload/extract-ee-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertIn("EePdfExtractionError", window, "route must catch EePdfExtractionError")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the new test to confirm failure**

Run: `python -m unittest tests.test_ee_extract_route_contract -v`

Expected: all five tests fail (route not registered, extractor not imported).

- [ ] **Step 3: Add the import to app.py**

Find the imports block at the top of `app.py` (lines 1–48). Add the new import on a fresh line **immediately after** `from werkzeug.utils import secure_filename` (currently ~line 47), and **before** the `BASE_DIR = Path(...)` constant:

```python
from ee_pdf_extractor import extract_ee_metadata, EePdfExtractionError
```

Note: `PyPDF2` is *not* imported at module top in this codebase — it's pulled in lazily inside the few functions that need it. Our extractor module owns its own PyPDF2 import, so we don't need to add one here.

- [ ] **Step 4: Add the route inside `create_app()`**

Find an existing JSON API route in `create_app()` — e.g. search for `news_upload_image` (around line 2218) — and add the new route nearby (after that function and before the next `@app.route`). Use this exact code:

```python
    @app.route("/api/upload/extract-ee-metadata", methods=["POST"])
    def api_extract_ee_metadata():
        user = require_login(level=2)
        if not user:
            return jsonify({"error": str(_("Unauthorized"))}), 401

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        if not upload.filename.lower().endswith(".pdf"):
            return jsonify({"error": str(_("File must be a PDF"))}), 400

        raw = upload.read()
        if not raw.startswith(b"%PDF-"):
            return jsonify({"error": str(_("File is not a valid PDF"))}), 400

        try:
            result = extract_ee_metadata(raw)
        except EePdfExtractionError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 200
```

- [ ] **Step 5: Run the contract test and the full test suite**

Run:
```bash
python -m unittest tests.test_ee_extract_route_contract -v
python -m unittest discover -s tests -p "test_*.py"
```

Expected: contract tests pass; no existing test regresses.

- [ ] **Step 6: Smoke-test the route manually**

Start the dev server: `./start_local.sh` (in another terminal).

In a third terminal, log in via the browser to get a session cookie, then export it (right-click → Inspect → Application → Cookies) and run:

```bash
curl -X POST http://localhost:5000/api/upload/extract-ee-metadata \
  -H "Cookie: session=<paste-cookie-here>" \
  -F "file=@test_function.pdf"
```

Expected output: JSON with `"core_subject":"Biology"`, the research question, scores `4,4,4,6,3`, and warnings `[]`.

If you don't want to copy a cookie, skip this step — Task 11 verifies it via the wizard UI.

- [ ] **Step 7: Commit**

```bash
git add app.py tests/test_ee_extract_route_contract.py
git commit -m "feat(api): add POST /api/upload/extract-ee-metadata route"
```

---

## Task 8: Frontend — autofill button render

Add the button + hidden file input + status span at the top of the EE fieldset. State first, render second, behaviour in the next task.

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add new state fields**

Find the `state` initializer (around lines 27–53 in `static/js/upload-wizard.js`; search for `eeCoreSubject: fd.ib_ee_core_subject`). Add these two lines just *after* the EE block (e.g. right before the closing brace of the `state` object):

```js
    // EE auto-fill UI
    eeAutofillStatus: '',    // '' | 'loading' | 'ok' | 'partial' | 'error'
    eeAutofillMessage: '',
```

- [ ] **Step 2: Insert the autofill block at the top of `renderEEFieldset()`**

Edit the template literal returned by `renderEEFieldset()` (starts at line ~420 with `return \``). Prepend the new block immediately after the opening backtick, **before** the `<div class="section-sub">${t('ee_subject', ...)}</div>` line:

```js
      <div class="ee-autofill">
        <button type="button" id="eeAutofillBtn" class="btn btn-outline-primary btn-sm">
          ${t('ee_autofill_btn', 'Auto-fill from commentary PDF')}
        </button>
        <input type="file" id="eeAutofillFile" accept="application/pdf,.pdf" hidden>
        <span id="eeAutofillStatus" class="ee-autofill__status ee-autofill__status--${state.eeAutofillStatus || 'idle'}">
          ${esc(state.eeAutofillMessage || '')}
        </span>
      </div>

```

(Keep the trailing blank line so the new block visually separates from the existing subject grid.)

- [ ] **Step 3: Smoke-render check**

Reload the upload wizard in the browser, choose paper type **Extended Essay**, advance to the metadata step.

Expected: the "Auto-fill from commentary PDF" button appears at the top of the EE section. Clicking it does nothing yet (the file input is hidden and unwired). Status span is empty.

- [ ] **Step 4: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload-wizard): render EE auto-fill button + state"
```

---

## Task 9: Frontend — file picker, fetch, overwrite confirm, state mutation

Wire the button: click → file picker → POST → confirm-if-dirty → mutate state → re-render with status.

**Files:**
- Modify: `static/js/upload-wizard.js`

- [ ] **Step 1: Add the auto-fill handler to `bindEEFieldset()`**

Find `function bindEEFieldset()` (around line 487). Append the following code **inside** the function, after the existing comment-textarea binding loop:

```js
    // ── EE auto-fill from commentary PDF ─────────────────────────
    const autoBtn = stepsContainer.querySelector('#eeAutofillBtn');
    const autoFile = stepsContainer.querySelector('#eeAutofillFile');
    if (autoBtn && autoFile) {
      autoBtn.addEventListener('click', () => autoFile.click());
      autoFile.addEventListener('change', async (e) => {
        const file = e.target.files && e.target.files[0];
        e.target.value = ''; // allow re-selecting the same file
        if (!file) return;
        await runEEAutofill(file);
      });
    }
```

- [ ] **Step 2: Add the `runEEAutofill` helper alongside `bindEEFieldset`**

Place this new function immediately after `bindEEFieldset` (just before the `// ─── CP fieldset ───` comment block):

```js
  async function runEEAutofill(file) {
    state.eeAutofillStatus = 'loading';
    state.eeAutofillMessage = t('ee_autofill_extracting', 'Extracting…');
    render();

    try {
      const form = new FormData();
      form.append('file', file);
      const resp = await fetch('/api/upload/extract-ee-metadata', {
        method: 'POST',
        body: form,
        credentials: 'same-origin',
      });
      const data = await resp.json().catch(() => ({}));

      if (!resp.ok) {
        state.eeAutofillStatus = 'error';
        state.eeAutofillMessage = data.error || t('ee_autofill_error',
          'Auto-fill failed — try again or fill manually.');
        render();
        return;
      }

      if (isEEDirty()) {
        const ok = window.confirm(t('ee_autofill_overwrite',
          'Replace your existing EE entries with values from the PDF?'));
        if (!ok) {
          state.eeAutofillStatus = '';
          state.eeAutofillMessage = '';
          render();
          return;
        }
      }

      applyEEAutofill(data);
      const summary = summariseAutofill(data);
      state.eeAutofillStatus = summary.status;
      state.eeAutofillMessage = summary.message;
      touch();
      render();
    } catch (err) {
      state.eeAutofillStatus = 'error';
      state.eeAutofillMessage = t('ee_autofill_error',
        'Auto-fill failed — try again or fill manually.');
      render();
    }
  }

  function isEEDirty() {
    if ((state.title || '').trim()) return true;
    if ((state.eeCoreSubject || '').trim()) return true;
    if ((state.eeInterSubject || '').trim()) return true;
    for (const k of ['A','B','C','D','E']) {
      if ((state.eeScores[k] || '').toString().trim()) return true;
      if ((state.eeComments[k] || '').trim()) return true;
    }
    if ((state.eeComments.holistic || '').trim()) return true;
    return false;
  }

  function applyEEAutofill(data) {
    if (data.research_question) state.title = data.research_question;
    state.eeCoreSubject = data.core_subject || '';
    state.eeInterSubject = data.interdisciplinary_subject || '';
    const criteria = data.criteria || {};
    ['A','B','C','D','E'].forEach(k => {
      const crit = criteria[k] || {};
      state.eeScores[k] = (crit.score === null || crit.score === undefined) ? '' : String(crit.score);
      state.eeComments[k] = crit.comment || '';
    });
    state.eeComments.holistic = data.holistic_comment || '';
    // Auto-reveal the commentary section if anything came back for it.
    const anyComment = ['A','B','C','D','E'].some(k => state.eeComments[k])
      || !!state.eeComments.holistic;
    if (anyComment) state.eeIncludeComments = true;
  }

  function summariseAutofill(data) {
    const warnings = (data.warnings || []);
    // Count populated fields out of the maximum (13 subject-focused, 14 interdisciplinary).
    const max = (data.interdisciplinary_subject ? 14 : 13);
    let filled = 0;
    if (data.core_subject) filled++;
    if (data.interdisciplinary_subject) filled++;
    if (data.research_question) filled++;
    if (data.holistic_comment) filled++;
    ['A','B','C','D','E'].forEach(k => {
      const crit = (data.criteria || {})[k] || {};
      if (crit.score !== null && crit.score !== undefined) filled++;
      if (crit.comment) filled++;
    });
    if (warnings.length === 0 && filled >= max) {
      return { status: 'ok', message: t('ee_autofill_ok', 'Extracted all fields.') };
    }
    const tail = warnings.length ? ' ' + warnings.join(' ') : '';
    return {
      status: 'partial',
      message: t('ee_autofill_partial',
        'Extracted %(filled)s of %(total)s fields.', { filled: String(filled), total: String(max) })
        + tail,
    };
  }
```

- [ ] **Step 3: Confirm `t(...)` parameter-substitution works for the new key**

Open the upload wizard, set paper type to EE, click the autofill button, pick `test_function.pdf`. The status should read **"Extracted 13 of 13 fields."** (or similar, depending on what gets parsed cleanly).

If the literal `%(filled)s` and `%(total)s` appear instead of the substituted numbers, find another `t(..., {...})` call in the file (e.g. `t('crit_comment_ph', 'Commentary for Criterion %(k)s…', { k: k })`) and copy its parameter-syntax exactly.

- [ ] **Step 4: Smoke-test in browser**

1. Open the upload wizard, pick **Extended Essay**, advance to metadata.
2. Click **Auto-fill from commentary PDF**, select `test_function.pdf`.
3. Verify: subject = Biology, title = the research question text, scores = 4/4/4/6/3, commentary boxes populated, holistic populated, status span shows green "Extracted all fields."
4. Refresh, manually type into the title field, click autofill again → expect a `confirm()` dialog. Cancel → no fields change.
5. Confirm → fields overwrite.
6. Pick a non-PDF file (e.g. a `.txt`) → expect red error status.

- [ ] **Step 5: Commit**

```bash
git add static/js/upload-wizard.js
git commit -m "feat(upload-wizard): wire EE auto-fill handler with overwrite confirm"
```

---

## Task 10: CSS styling for status pill

Add the small style block for the new status span colour states.

**Files:**
- Modify: `static/css/upload.css`

- [ ] **Step 1: Append styles to `static/css/upload.css`**

Append this block at the end of the file:

```css
/* EE commentary PDF auto-fill */
.ee-autofill {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  flex-wrap: wrap;
  margin-bottom: 16px;
}
.ee-autofill__status { font-size: 0.875rem; }
.ee-autofill__status--ok      { color: var(--bs-success, #198754); }
.ee-autofill__status--partial { color: var(--bs-warning, #fd7e14); }
.ee-autofill__status--error   { color: var(--bs-danger, #dc3545); }
.ee-autofill__status--loading { color: var(--bs-secondary, #6c757d); }
.ee-autofill__status--idle:empty { display: none; }
```

- [ ] **Step 2: Smoke-test**

Reload the upload wizard EE step, run an auto-fill. Status should be green; trigger an error (upload a `.txt` renamed to `.pdf` so the magic-byte check fails) → status should be red.

- [ ] **Step 3: Commit**

```bash
git add static/css/upload.css
git commit -m "style(upload-wizard): colour states for EE auto-fill status"
```

---

## Task 11: i18n — add new translation keys

Add the new strings to both locale catalogues and compile.

**Files:**
- Modify: `translations/en/LC_MESSAGES/messages.po`
- Modify: `translations/zh/LC_MESSAGES/messages.po`

- [ ] **Step 1: Inspect the existing `.po` format**

Run: `head -30 translations/en/LC_MESSAGES/messages.po`

Familiarise yourself with the entry format:

```
msgid "Some English string"
msgstr "Translated string"
```

- [ ] **Step 2: Append the new keys to `translations/en/LC_MESSAGES/messages.po`**

Append at the end of the file (and reuse pattern from other entries — note `%(name)s` substitutions are supported):

```
msgid "Auto-fill from commentary PDF"
msgstr "Auto-fill from commentary PDF"

msgid "Replace your existing EE entries with values from the PDF?"
msgstr "Replace your existing EE entries with values from the PDF?"

msgid "Extracting…"
msgstr "Extracting…"

msgid "Extracted all fields."
msgstr "Extracted all fields."

msgid "Extracted %(filled)s of %(total)s fields."
msgstr "Extracted %(filled)s of %(total)s fields."

msgid "Auto-fill failed — try again or fill manually."
msgstr "Auto-fill failed — try again or fill manually."

msgid "No file provided"
msgstr "No file provided"

msgid "File must be a PDF"
msgstr "File must be a PDF"

msgid "File is not a valid PDF"
msgstr "File is not a valid PDF"
```

(If any of these `msgid`s already exist elsewhere in the catalogue — `"No file provided"` does, for example — leave the existing one in place and skip the duplicate here.)

- [ ] **Step 3: Append translations to `translations/zh/LC_MESSAGES/messages.po`**

Append the same `msgid` block, with Chinese translations:

```
msgid "Auto-fill from commentary PDF"
msgstr "从评语 PDF 自动填充"

msgid "Replace your existing EE entries with values from the PDF?"
msgstr "用 PDF 中的值替换现有的 EE 内容吗？"

msgid "Extracting…"
msgstr "正在提取…"

msgid "Extracted all fields."
msgstr "已提取全部字段。"

msgid "Extracted %(filled)s of %(total)s fields."
msgstr "已提取 %(filled)s / %(total)s 个字段。"

msgid "Auto-fill failed — try again or fill manually."
msgstr "自动填充失败——请重试或手动填写。"

msgid "File must be a PDF"
msgstr "文件必须是 PDF"

msgid "File is not a valid PDF"
msgstr "文件不是有效的 PDF"
```

(Skip any `msgid`s that already exist in the zh catalogue.)

- [ ] **Step 4: Compile translations**

Run: `python tools/compile_translations.py`

Expected: prints success for both `en` and `zh`. Verify `translations/en/LC_MESSAGES/messages.mo` and the `zh` equivalent have new mtimes.

- [ ] **Step 5: Smoke-test the zh locale**

Set your browser to Chinese (or temporarily change `BABEL_DEFAULT_LOCALE` in `.env` to `zh` and restart), open the EE step. The button should read **"从评语 PDF 自动填充"** and the status messages should be Chinese.

- [ ] **Step 6: Commit**

```bash
git add translations/en/LC_MESSAGES/messages.po translations/en/LC_MESSAGES/messages.mo \
        translations/zh/LC_MESSAGES/messages.po translations/zh/LC_MESSAGES/messages.mo
git commit -m "i18n: add strings for EE PDF auto-fill"
```

---

## Task 12: Final integration verification

Run the full test suite and a complete manual walk-through.

- [ ] **Step 1: Run the full test suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`

Expected: all tests pass — including pre-existing tests. If any old test broke, **stop and fix the regression**; do not skip or mask it.

- [ ] **Step 2: Manual end-to-end walk-through**

Start the dev server (`./start_local.sh`), log in as a Contributor (role 2+), then:

1. Click **Upload paper** → wizard opens.
2. On the file step, upload any small PDF (it can be `test_function.pdf` itself for now — the essay-vs-commentary distinction is irrelevant for this test).
3. Pick **Extended Essay** as paper type.
4. Advance to metadata. Click **Auto-fill from commentary PDF** → pick `test_function.pdf`.
5. Confirm fields populate:
   - Title: contains "alcohol production" and "yeast"
   - Core Subject combobox shows "Biology"
   - Scores: A=4, B=4, C=4, D=6, E=3 (total = 21 / 30)
   - "Include commentaries" checkbox is on
   - Each of A–E has commentary; holistic comment is non-empty
   - Status span: green "Extracted all fields."
6. Now click **Auto-fill** again → confirm dialog appears (fields are dirty) → click Cancel → nothing changes.
7. Click **Auto-fill** again → confirm → fields are re-extracted identically.
8. Pick a non-PDF file (rename `start_local.sh` to `fake.pdf`, upload that) → status turns red with "File is not a valid PDF".
9. Pick an empty/garbage PDF → status turns red with "Could not read PDF" or "No readable text" or similar.

- [ ] **Step 3: Final commit (if any tweaks were needed)**

```bash
git status
# If clean, no commit needed. Otherwise:
git add -p   # review changes
git commit -m "fix(ee-autofill): address issues found in integration walk-through"
```

- [ ] **Step 4: Summarise & hand off**

Write a brief PR description summarising what shipped, what was deferred (the interdisciplinary fixture and any new schema field for the framework), and link to the spec.

---

## Out-of-scope follow-ups

- Source or generate an interdisciplinary EE commentary fixture and add tests for the `interdisciplinary_subject` + `framework` extraction paths.
- If usage shows users routinely uploading interdisciplinary EEs, add an `interdisciplinary_framework` field to `ib_ee_data` and a matching form field.
- OCR support for scanned PDFs.

---

## Self-review notes (from plan author)

After writing the plan, I checked it against the spec and fixed these issues inline before saving:

1. **`require_login` shape:** the spec showed it as a decorator (`@require_login(2)`), but in this codebase it's a regular function called *inside* the route. Plan task 7 uses the real pattern.
2. **Field-count math:** the spec correctly says 13 fields for subject-focused / 14 for interdisciplinary; the plan's `summariseAutofill` JS now matches that exactly (toggles based on whether `interdisciplinary_subject` came back).
3. **Re-render path:** the wizard already exposes a top-level `render()` function — the plan uses it instead of inventing a new mechanism.
4. **i18n parameter syntax:** confirmed via existing `t('crit_comment_ph', '…%(k)s…', { k: k })` call in the wizard JS.
5. **Test fixture for interdisciplinary EE:** acknowledged as deferred in the spec; the plan does not gate any task on it.
