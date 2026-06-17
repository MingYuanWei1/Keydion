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
import vision_read

MAX_PDF_CHARS = 12_000          # bound the prompt size / token cost

_log = logging.getLogger(__name__)


class IAMetadataError(Exception):
    """Raised when the PDF is unusable or the LLM request/response fails."""


def _ocr_langs_for(language: str) -> str:
    """Tesseract lang string biased by the paper's declared language."""
    return "chi_sim+eng" if language == "zh" else "eng"   # chi_tra dropped for speed


def _pdf_text_from_bytes(file_bytes: bytes, language: str = "en") -> str:
    """Extract concatenated PDF text (PyPDF2 + OCR fallback), capped to MAX_PDF_CHARS."""
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
    criterion, in input order, each {"name", "max", "score", "comment"} with
    score clamped to [0, max]. Criteria the model omitted default to score 0 with
    a warning; criteria the model invented are dropped.
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
            warnings.append(f"No score returned for “{name}” — defaulted to 0.")
            out.append({"name": name, "max": cmax, "score": 0, "comment": ""})
            continue
        try:
            score = int(round(float(match.get("score", 0))))
        except (TypeError, ValueError):
            score = 0
            warnings.append(f"Unreadable score for “{name}” — defaulted to 0.")
        score = max(0, min(score, cmax))
        comment = match.get("comment")
        comment = comment.strip() if isinstance(comment, str) else ""
        out.append({"name": name, "max": cmax, "score": score, "comment": comment})
    return out, warnings


def _complete(client, text: str, subject: str, criteria: list, language: str) -> dict:
    """Call the chat endpoint and return {criteria, holistic_comment, warnings}."""
    model = llm_client.think_model()
    lang_name = "Chinese" if language == "zh" else "English"
    crit_lines = "\n".join(
        f'- "{c.get("name", "")}" (max score {int(c.get("max", 0))})' for c in criteria
    )
    system = (
        "You are an experienced IB examiner grading an Internal Assessment (IA). "
        f'The subject is "{subject}". Read the paper text and assess it against '
        "each of these official assessment criteria:\n"
        f"{crit_lines}\n\n"
        "Return a JSON object with these keys:\n"
        '- "criteria": an array of objects, one PER criterion above, each with '
        '"name" (exactly as given above), "score" (an integer from 0 to that '
        'criterion\'s max), and "comment" (a one-to-three sentence justification '
        f"written in {lang_name}).\n"
        f'- "holistic_comment": an overall paragraph of feedback written in '
        f"{lang_name}.\n"
        '- "warnings": an array of short strings noting anything that made grading '
        "uncertain (e.g. missing sections); use [] if none.\n"
        "Score conservatively and never exceed a criterion's max. "
        "Return ONLY the JSON object, no prose."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.2,
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


def _vision_prompt(subject: str, criteria: list, language: str) -> str:
    lang_name = "Chinese" if language == "zh" else "English"
    lines = "\n".join(
        f'- "{c.get("name", "")}" (maximum {int(c.get("max", 0))})' for c in criteria
    )
    return (
        "You are an IB examiner. The images are the rendered pages of a student "
        f"Internal Assessment for {subject}. Score it against these criteria:\n"
        f"{lines}\n"
        'Return a JSON object: "criteria" — an array of objects '
        '{"name","score","comment"} (one per criterion above, score an integer not '
        f'exceeding the maximum, comment in {lang_name}); "holistic_comment" — an '
        f'overall comment in {lang_name}; "warnings" — an array of short strings. '
        "Return ONLY the JSON object, no prose."
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


def generate_ia_scores(file_bytes: bytes, subject: str, criteria: list,
                       language: str = "en") -> dict:
    """Public entry point: vision-first IA scoring; OCR+text-LLM fallback.

    `criteria` is the authoritative [{"name", "max"}, ...] list from the subject
    config; returned scores are clamped to those maxes server-side. A vision
    failure falls back to the legacy path rather than hard-erroring.
    """
    if not criteria:
        raise IAMetadataError("This subject has no assessment criteria configured.")
    if llm_client.vision_enabled():
        prompt = _vision_prompt(subject, criteria, language)
        try:
            data = vision_read.extract_with_vision(file_bytes, prompt, language=language)
            return _result_from_vision(data, criteria)
        except vision_read.VisionError:
            _log.warning("vision IA scoring failed; falling back to OCR path",
                         exc_info=True)
    return _legacy_generate_ia_scores(file_bytes, subject, criteria, language)
