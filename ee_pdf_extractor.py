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
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pdfplumber
from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

_DATA_DIR = Path(__file__).resolve().parent / "data"


class EePdfExtractionError(Exception):
    """Raised when the PDF cannot be processed at all (corrupt, encrypted, scanned)."""


# ── regex anchors ────────────────────────────────────────────────────────────
#
# pdftotext/PyPDF2 emit the IB commentary form's table cells in roughly the
# visual order: each cell becomes a chunk separated by newlines. The exact
# whitespace is whitespace-insensitive in these patterns (we use \s+).
#
# Noteworthy quirks observed in the fixture (ee_commentary_subject_focused.pdf):
#   - "Subject-focused" is rendered with a stray space: "Subject -focused"
#   - "DP subjects :" has a space before the colon on the second label line
#   - "Interdisciplinary framework:" is split across two lines as
#     "Interdisciplinary\nframework :" (space before colon too)
#   - Criterion E's score appears BEFORE its [Maximum possible mark: N] block,
#     unlike A–D where the score follows the bracket.  The criterion regex
#     below handles this by looking for the score either before or after the
#     bracket, but to keep it simple we parse criteria in two passes:
#     one for A–D (score after bracket) and one for E (score before bracket).

_RE_CORE_SUBJECT = re.compile(
    # "DP subject:" … "(Subject -focused essay)  Biology" … "DP subjects"
    # The IB form may render "Subject-focused" with a stray space before the hyphen.
    r"DP subject:\s*\([Ss]ubject\s*-\s*focused essay\)\s+(.+?)\s+DP subjects",
    re.DOTALL,
)

_RE_INTER_SUBJECTS = re.compile(
    # Captures the cell after '(Interdisciplinary essay)'.
    # Stops at the start of the 'Interdisciplinary framework:' label.
    r"\(Interdisciplinary essay\)\s+(.+?)\s+Interdisciplinary\s+framework",
    re.DOTALL,
)

_RE_FRAMEWORK = re.compile(
    # "Interdisciplinary framework :" (space before colon possible) … "Research question:"
    r"Interdisciplinary\s+framework\s*:\s+(.+?)\s+Research question:",
    re.DOTALL,
)

_RE_RESEARCH_QUESTION = re.compile(
    # "Research question:" … content … "Assessment details"
    r"Research question:\s+(.+?)\s+Assessment details",
    re.DOTALL,
)

# Criteria A–D: '[Maximum possible mark/\nmark: N]  <score>  <comment>'
# Each block stops at the next criterion letter or at 'E: Reflection'.
_RE_CRITERION_AD = re.compile(
    r"(?P<letter>[A-D]):[^\[]*?\[Maximum possible\s*\n?mark:\s*\d+\]\s+"
    r"(?P<score>\d+)\s+(?P<comment>.+?)"
    r"(?=(?:[A-E]:)|(?:Total marks awarded)|\Z)",
    re.DOTALL,
)

# Criterion E is different: "E: Reflection\n 3 Some text...\n[Maximum possible\nmark: 4] more text"
# The score appears BEFORE the bracket.
_RE_CRITERION_E = re.compile(
    r"E:\s+Reflection\s+(?P<score>\d+)\s+(?P<comment>.+?)"
    r"(?=Total marks awarded|\Z)",
    re.DOTALL,
)

_RE_HOLISTIC = re.compile(
    # "Holistic comment on\nthe essay :" (space before colon possible)
    r"Holistic comment on\s+the essay\s*:\s+(.+?)\s*\Z",
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


def _strip_max_mark_header(comment: str) -> str:
    """Remove a trailing or leading '[Maximum possible mark: N]' fragment from a comment.

    Criterion E's comment contains the bracket mid-text because the bracket
    appears on the page AFTER the first sentence of the comment.
    """
    # Remove any '[Maximum possible mark: N]' or '[Maximum possible\nmark: N]'
    return re.sub(r"\[Maximum possible\s*\n?mark:\s*\d+\]\s*", "", comment)


def _extract_via_regex(text: str) -> dict:
    """Apply anchor regex to extracted text. Always returns a partial dict."""
    result = _empty_result()

    if m := _RE_CORE_SUBJECT.search(text):
        result["core_subject"] = _collapse(m.group(1))

    if m := _RE_INTER_SUBJECTS.search(text):
        raw = _collapse(m.group(1))
        # Split on common separators if multiple subjects listed.
        parts = re.split(r"\s*(?:,|\band\b|;|/)\s*", raw, maxsplit=1)
        if parts and parts[0]:
            # Only override core_subject from this block if not already set.
            result["core_subject"] = result["core_subject"] or parts[0]
            if len(parts) > 1:
                result["interdisciplinary_subject"] = parts[1]

    if m := _RE_FRAMEWORK.search(text):
        result["framework"] = _collapse(m.group(1))

    if m := _RE_RESEARCH_QUESTION.search(text):
        result["research_question"] = _collapse(m.group(1))

    # Parse criteria A–D (score follows the bracket)
    for m in _RE_CRITERION_AD.finditer(text):
        letter = m.group("letter")
        try:
            score = int(m.group("score"))
        except ValueError:
            score = None
        comment = _collapse(_strip_max_mark_header(m.group("comment")))
        result["criteria"][letter] = {"score": score, "comment": comment}

    # Parse criterion E separately (score precedes the bracket)
    if m := _RE_CRITERION_E.search(text):
        try:
            score = int(m.group("score"))
        except ValueError:
            score = None
        comment = _collapse(_strip_max_mark_header(m.group("comment")))
        result["criteria"]["E"] = {"score": score, "comment": comment}

    if m := _RE_HOLISTIC.search(text):
        result["holistic_comment"] = _collapse(m.group(1))

    return result


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


def _extract_via_pdfplumber(file_bytes: bytes) -> Optional[dict]:
    """Extract by walking pdfplumber's table rows. Returns None if unrecognised.

    Structural observations from the IB EE commentary fixture:
      - Most rows use the layout: [label_cell, None, value_cell, comment_cell, ...]
        where None cells are literal None (not empty strings).
      - Criteria rows: [label_with_max_mark, None, score_str, comment_str, ...]
        The score cell contains only digits (e.g. '4').
      - Holistic comment row: [label_cell, value_cell, None]
      - The `_row_value_cells` helper skips None and empty-string cells, so
        `row[1:]` filtered for truthiness reliably yields [score, comment] for
        criteria rows and [value] for metadata rows.
      - Criterion E's continuation row on page 2 ('[Maximum possible mark: 4]'...)
        has no score digit, so its comment fragment is appended to E's comment.
    """
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
        # Skip None→"" cells; only return genuinely non-empty cells after [0].
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
            # Expected row layout (after None→"" normalisation):
            #   [letter-with-name-and-max-mark, "", score_str, comment_str, ...]
            # _row_value_cells filters blanks, giving [score_str, comment_str, ...]
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
