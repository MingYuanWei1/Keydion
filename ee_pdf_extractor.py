"""IB Extended Essay 'commentary for example essay' PDF parser.

Public surface:
    extract_ee_metadata(file_bytes) -> dict
    EePdfExtractionError

Vision-first: the vision model reads the rendered pages of the commentary
form (see CONTEXT.md 'Vision-first extraction'); when no vision model is
configured or the vision read fails, the fallback path reads the PDF's
embedded text (with OCR for scanned pages) and has the think-tier chat model
transcribe it. Both paths only TRANSCRIBE the examiner's marks — they never
grade the essay.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pdf_text import extract_pdf_text, PdfTextError

from config import _EE_SUBJECTS_DEFAULT
import llm_client
from vision_extractor import VisionFirstExtractor

_DATA_DIR = Path(__file__).resolve().parent / "data"

MAX_PDF_CHARS = 12_000          # bound the prompt size / token cost

_EE_PROMPT_BODY = (
    "The images are the rendered pages of an IB Extended Essay examiner "
    "commentary form that has ALREADY been marked. Your ONLY job is to TRANSCRIBE "
    "what the examiner wrote — never grade the essay yourself or invent feedback. "
    "Return a JSON object with these keys: "
    '"core_subject" — the DP subject of a subject-focused essay (empty string if '
    'the essay is interdisciplinary); "interdisciplinary_subject" — the second DP '
    'subject for an interdisciplinary essay (empty string otherwise); '
    '"research_question" — the stated research question; "criteria" — an object '
    'with keys "A","B","C","D","E", each an object {"score": <the integer the '
    'examiner awarded, or null if no score for it is written>, "comment": <the '
    "examiner's remark for that criterion copied WORD-FOR-WORD, or \"\" if none>}; "
    '"holistic_comment" — the examiner\'s holistic comment copied word-for-word '
    '(or ""); and "warnings" — an array of short strings naming anything not found '
    "or unreadable. "
    "STRICT RULES: copy every value (comments, research question, subject names) "
    "EXACTLY as printed/written — do NOT paraphrase, summarise, translate, "
    "rephrase, complete, or add anything of your own. If a score or comment is not "
    "present on the form, use null / empty string — never guess, infer, or grade "
    "the essay yourself. Return ONLY the JSON object, no prose."
)

EE_SYSTEM_PROMPT_EN = (
    "You transcribe IB assessment forms verbatim; you never grade essays "
    "yourself. " + _EE_PROMPT_BODY
)
EE_SYSTEM_PROMPT_ZH = EE_SYSTEM_PROMPT_EN  # field values are copied verbatim; locale affects nothing structural here


class EePdfExtractionError(Exception):
    """Raised when the PDF is unusable or the LLM request/response fails."""


def _pdf_text_from_bytes(file_bytes: bytes) -> str:
    """Extract concatenated PDF text (pypdf + OCR fallback), capped to MAX_PDF_CHARS."""
    if not file_bytes:
        raise EePdfExtractionError("Empty file")
    try:
        text = extract_pdf_text(file_bytes)
    except PdfTextError as exc:
        if exc.reason == "encrypted":
            raise EePdfExtractionError("PDF is encrypted") from exc
        raise EePdfExtractionError("Could not read PDF — the file may be corrupt.") from exc
    text = (text or "").strip()
    if not text:
        raise EePdfExtractionError("No readable text — is this a scanned image?")
    return text[:MAX_PDF_CHARS]


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


@lru_cache(maxsize=1)
def _canonical_subjects() -> dict:
    """Return a {lowercase_name: CanonicalName} mapping from ee_subjects.json."""
    path = _DATA_DIR / "ee_subjects.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        data = _EE_SUBJECTS_DEFAULT
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


def _result_from_vision(data: dict) -> dict:
    """Coerce a model-returned dict into the _empty_result() shape."""
    result = _empty_result()
    for key in ("core_subject", "interdisciplinary_subject", "framework",
                "research_question", "holistic_comment"):
        val = data.get(key)
        result[key] = val.strip() if isinstance(val, str) else ""
    crit_in = data.get("criteria") or {}
    for letter in "ABCDE":
        c = crit_in.get(letter) or {}
        raw_score = c.get("score")
        score = raw_score if isinstance(raw_score, int) else None
        comment = c.get("comment")
        result["criteria"][letter] = {
            "score": score,
            "comment": comment.strip() if isinstance(comment, str) else "",
        }
    extra = data.get("warnings")
    if isinstance(extra, list):
        result["warnings"] = [str(w) for w in extra if str(w).strip()]
    return result


def _complete(text: str) -> dict:
    """Call the chat endpoint and return the coerced EE result shape."""
    try:
        data = llm_client.chat_json(
            [
                {"role": "system", "content": EE_SYSTEM_PROMPT_EN},
                {"role": "user", "content": text},
            ],
            tier="think",
            temperature=0,
        )
    except llm_client.LLMChatUnavailable as exc:
        raise EePdfExtractionError(str(exc)) from exc
    except llm_client.LLMChatRequestError as exc:
        raise EePdfExtractionError("AI request failed — please try again later.") from exc
    except llm_client.LLMChatParseError as exc:
        raise EePdfExtractionError("The AI response could not be parsed.") from exc
    return _result_from_vision(data)


def _legacy_extract_ee_metadata(file_bytes: bytes) -> dict:
    """OCR+text-LLM path: PDF bytes -> the full EE result shape."""
    text = _pdf_text_from_bytes(file_bytes)
    return _complete(text)


class EeExtractor(VisionFirstExtractor):
    """Vision-first IB EE commentary parser; OCR+text-LLM fallback."""

    def build_prompt(self) -> str:
        return EE_SYSTEM_PROMPT_EN

    def shape_vision(self, data: dict) -> dict:
        return _result_from_vision(data)

    def fallback(self, file_bytes: bytes) -> dict:
        return _legacy_extract_ee_metadata(file_bytes)

    def post(self, result: dict) -> dict:
        # Subject normalisation runs on whichever branch produced the result.
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


def extract_ee_metadata(file_bytes: bytes) -> dict:
    """Parse an IB EE commentary PDF. See module docstring for contract."""
    if not file_bytes:
        raise EePdfExtractionError("Empty file")
    return EeExtractor().extract(file_bytes)
