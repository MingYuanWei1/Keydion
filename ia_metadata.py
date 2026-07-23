# ia_metadata.py
"""Draft IA criterion scores + comments from an IA paper PDF via an OpenAI-compatible LLM.

Public surface:
    generate_ia_scores(file_bytes, subject, criteria, language="en") -> dict
    IAMetadataError

Provider-agnostic: the chat client base URL, API key, and model are resolved via
``llm_client`` (LLM_BASE_URL / LLM_API_KEY / LLM_DEFAULT_THINK), so the same code
works against OpenAI, a local model (Ollama/vLLM), or any OpenAI-style API.

The scoring is advisory only: every returned score is clamped server-side to the
criterion's configured max, and the criterion maxes / totals shown on the paper
come from the subject config, never from the model.
"""

from __future__ import annotations

import json
import logging
import re

from pdf_text import extract_pdf_text, PdfTextError

import llm_client
from vision_extractor import VisionFirstExtractor

MAX_PDF_CHARS = 12_000          # bound the prompt size / token cost

_log = logging.getLogger(__name__)


class IAMetadataError(Exception):
    """Raised when the PDF is unusable or the LLM request/response fails."""


def _ocr_langs_for(language: str) -> str:
    """Tesseract lang string biased by the paper's declared language."""
    return "chi_sim+eng" if language == "zh" else "eng"   # chi_tra dropped for speed


def _pdf_text_from_bytes(file_bytes: bytes, language: str = "en") -> str:
    """Extract concatenated PDF text (pypdf + OCR fallback), capped to MAX_PDF_CHARS."""
    if not file_bytes:
        raise IAMetadataError("Empty file")
    try:
        text = extract_pdf_text(file_bytes, ocr_langs=_ocr_langs_for(language))
    except PdfTextError as exc:
        if exc.reason == "encrypted":
            raise IAMetadataError("PDF is encrypted") from exc
        raise IAMetadataError("Could not read PDF — the file may be corrupt.") from exc
    text = (text or "").strip()
    if not text:
        raise IAMetadataError("No readable text — is this a scanned image?")
    return text[:MAX_PDF_CHARS]


def _build_client():
    """Construct the chat client; raise the module error if AI is unconfigured."""
    if not llm_client.llm_enabled():
        raise IAMetadataError("AI assist is not configured.")
    try:
        return llm_client.build_client()
    except ImportError as exc:  # openai not installed
        raise IAMetadataError("openai package is not installed.") from exc


def _parse_json(content: str):
    """Parse JSON, tolerating a model that wraps it in prose. Returns dict or None."""
    if not content:
        return None
    try:
        return json.loads(content)
    except (ValueError, TypeError):
        pass
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except ValueError:
        return None


def _normalise_criteria(returned, criteria: list) -> tuple[list, list]:
    """Reconcile the model's criteria against the input subject criteria.

    `criteria` is the authoritative list [{"name", "max"}, ...] from the subject
    config. Returns (out, warnings) where `out` has exactly one entry per input
    criterion, in input order, each {"name", "max", "score", "comment"}. A score
    the marker actually wrote is clamped to [0, max]; anything the model omitted,
    returned null/empty for, or that is unreadable is left BLANK (score None,
    comment "") with a warning — never fabricated as 0. Criteria the model
    invented are dropped.
    """
    warnings: list = []
    by_name = {}
    if isinstance(returned, list):
        for item in returned:
            if isinstance(item, dict) and isinstance(item.get("name"), str):
                by_name[item["name"].strip().lower()] = item

    out = []
    for crit in criteria:
        name = str(crit.get("name", "")).strip()
        try:
            cmax = int(crit.get("max", 0))
        except (TypeError, ValueError):
            cmax = 0
        match = by_name.get(name.lower())
        if match is None:
            warnings.append(f"No entry for “{name}” in the document — left blank.")
            out.append({"name": name, "max": cmax, "score": None, "comment": ""})
            continue
        raw_score = match.get("score")
        if raw_score is None or (isinstance(raw_score, str) and not raw_score.strip()):
            score = None  # marker left it unscored / not found in the document
            warnings.append(f"No score found for “{name}” — left blank.")
        else:
            try:
                score = max(0, min(int(round(float(raw_score))), cmax))
            except (TypeError, ValueError):
                score = None
                warnings.append(f"Unreadable score for “{name}” — left blank.")
        comment = match.get("comment")
        comment = comment.strip() if isinstance(comment, str) else ""
        out.append({"name": name, "max": cmax, "score": score, "comment": comment})
    return out, warnings


def _complete(client, text: str, subject: str, criteria: list, language: str) -> dict:
    """Call the chat endpoint and return {criteria, holistic_comment, warnings}."""
    model = llm_client.think_model()
    crit_lines = "\n".join(
        f'- "{c.get("name", "")}" (max score {int(c.get("max", 0))})' for c in criteria
    )
    system = (
        "You are a careful data-entry assistant, NOT an examiner. The text is an "
        f'Internal Assessment for "{subject}" that a teacher/examiner has ALREADY '
        "marked — their scores and written comments are in the document. Your ONLY "
        "job is to TRANSCRIBE what the marker wrote for each criterion. You must "
        "never grade the work yourself, infer a score, or write any feedback of "
        "your own.\n"
        "Assessment criteria:\n"
        f"{crit_lines}\n\n"
        "Return a JSON object with these keys:\n"
        '- "criteria": an array of objects, one PER criterion above, each with '
        '"name" (exactly as given above), "score" (the integer the marker awarded '
        "for that criterion, or null if no score for it is written in the "
        'document), and "comment" (the marker\'s comment for that criterion copied '
        'WORD-FOR-WORD from the document, or "" if none is written).\n'
        '- "holistic_comment": the marker\'s overall/summary comment copied '
        'word-for-word, or "" if there is none.\n'
        '- "warnings": an array of short strings naming any criterion whose score '
        "or comment was not found; use [] if none.\n"
        "STRICT RULES: Copy comments exactly as written — do NOT paraphrase, "
        "summarise, translate, rephrase, complete, or add anything of your own. If "
        "a score or comment is not clearly present in the document, leave it "
        "null/empty — never guess, infer, or invent. Return ONLY the JSON object, "
        "no prose."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": text},
            ],
        )
    except Exception as exc:  # network/auth/rate-limit from any provider
        _log.exception("LLM request failed")
        raise IAMetadataError("AI request failed — please try again later.") from exc

    try:
        content = (resp.choices[0].message.content or "").strip()
    except (AttributeError, IndexError, TypeError) as exc:
        raise IAMetadataError("The AI response could not be parsed.") from exc

    data = _parse_json(content)
    if not isinstance(data, dict):  # valid JSON can still be a list / str / number
        raise IAMetadataError("The AI response could not be parsed.")

    out_criteria, warnings = _normalise_criteria(data.get("criteria"), criteria)
    raw_holistic = data.get("holistic_comment")
    holistic = raw_holistic.strip() if isinstance(raw_holistic, str) else ""
    extra = data.get("warnings")
    if isinstance(extra, list):
        for w in extra:
            if isinstance(w, str) and w.strip():
                warnings.append(w.strip())
    return {"criteria": out_criteria, "holistic_comment": holistic, "warnings": warnings}


def _legacy_generate_ia_scores(file_bytes: bytes, subject: str, criteria: list,
                               language: str = "en") -> dict:
    """OCR+text-LLM path: PDF bytes + subject + its criteria ->
    {criteria, holistic_comment, warnings}.

    `criteria` is the authoritative [{"name", "max"}, ...] list from the subject
    config; returned scores are clamped to those maxes server-side.
    """
    text = _pdf_text_from_bytes(file_bytes, language)
    client = _build_client()
    return _complete(client, text, subject, criteria, language)


def _vision_prompt(subject: str, criteria: list) -> str:
    lines = "\n".join(
        f'- "{c.get("name", "")}" (max score {int(c.get("max", 0))})' for c in criteria
    )
    return (
        "You are a careful data-entry assistant, NOT an examiner. The images are "
        f"the rendered pages of an Internal Assessment for {subject} that a "
        "teacher/examiner has ALREADY marked. Look for the scores and comments THEY "
        "wrote on the pages — in the margins, on a criterion/marking grid, or in "
        "end-of-paper feedback. Your ONLY job is to TRANSCRIBE what the marker "
        "wrote; never grade the work yourself, infer a score, or write feedback of "
        "your own.\n"
        "Assessment criteria:\n"
        f"{lines}\n"
        'Return a JSON object: "criteria" — an array, one object per criterion '
        'above, each {"name" (exactly as given), "score" (the integer the marker '
        'awarded, or null if no score for it is written), "comment" (the marker\'s '
        'comment for that criterion copied WORD-FOR-WORD, or "" if none)}; '
        '"holistic_comment" — the marker\'s overall comment copied word-for-word '
        '(or ""); "warnings" — short strings naming anything not found.\n'
        "STRICT RULES: Copy comments exactly as written — do NOT paraphrase, "
        "summarise, translate, rephrase, complete, or add anything of your own. If "
        "a score or comment is not clearly visible, leave it null/empty — never "
        "guess or invent. Return ONLY the JSON object, no prose."
    )


def _result_from_vision(data: dict, criteria: list) -> dict:
    """Shape a vision extract_with_vision dict like _complete's return value.

    Reuses _normalise_criteria for the same name-reconcile + clamp-to-max logic
    the text path uses, then folds in any model-reported warnings.
    """
    out_criteria, warnings = _normalise_criteria(data.get("criteria"), criteria)
    raw_holistic = data.get("holistic_comment")
    holistic = raw_holistic.strip() if isinstance(raw_holistic, str) else ""
    extra = data.get("warnings")
    if isinstance(extra, list):
        for w in extra:
            if isinstance(w, str) and w.strip():
                warnings.append(w.strip())
    return {"criteria": out_criteria, "holistic_comment": holistic, "warnings": warnings}


class IaExtractor(VisionFirstExtractor):
    """Vision-first IA criterion scoring; OCR+text-LLM fallback."""

    def __init__(self, subject: str, criteria: list, language: str = "en"):
        self.subject = subject
        self.criteria = criteria
        self.language = language

    def build_prompt(self) -> str:
        return _vision_prompt(self.subject, self.criteria)

    def shape_vision(self, data: dict) -> dict:
        return _result_from_vision(data, self.criteria)

    def fallback(self, file_bytes: bytes) -> dict:
        return _legacy_generate_ia_scores(file_bytes, self.subject, self.criteria, self.language)


def generate_ia_scores(file_bytes: bytes, subject: str, criteria: list,
                       language: str = "en") -> dict:
    """Public entry point: vision-first IA scoring; OCR+text-LLM fallback.

    `criteria` is the authoritative [{"name", "max"}, ...] list from the subject
    config; returned scores are clamped to those maxes server-side. A vision
    failure falls back to the legacy path rather than hard-erroring.
    """
    if not criteria:
        raise IAMetadataError("This subject has no assessment criteria configured.")
    return IaExtractor(subject, criteria, language).extract(file_bytes)
