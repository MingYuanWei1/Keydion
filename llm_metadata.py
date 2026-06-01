"""Draft an abstract + keywords from a paper PDF via an OpenAI-compatible LLM.

Public surface:
    generate_abstract_keywords(file_bytes, language="en") -> dict
    LLMMetadataError

Provider-agnostic: the client base URL, API key, and model are resolved via
``llm_client`` (LLM_BASE_URL / LLM_API_KEY / LLM_DEFAULT_FLASH), so the same
code works against OpenAI, a local model (Ollama/vLLM), or any OpenAI-style API.
"""

from __future__ import annotations

import json
import logging
import re

from pdf_text import extract_pdf_text, PdfTextError

import llm_client

MAX_PDF_CHARS = 12_000          # bound the prompt size / token cost
MAX_KEYWORDS = 6

_log = logging.getLogger(__name__)


class LLMMetadataError(Exception):
    """Raised when the PDF is unusable or the LLM request/response fails."""


def _ocr_langs_for(language: str) -> str:
    """Tesseract lang string biased by the paper's declared language."""
    return "chi_sim+chi_tra+eng" if language == "zh" else "eng"


def _pdf_text_from_bytes(file_bytes: bytes, language: str = "en") -> str:
    """Extract concatenated PDF text (PyPDF2 + OCR fallback), capped to MAX_PDF_CHARS."""
    if not file_bytes:
        raise LLMMetadataError("Empty file")
    try:
        text = extract_pdf_text(file_bytes, ocr_langs=_ocr_langs_for(language))
    except PdfTextError as exc:
        if exc.reason == "encrypted":
            raise LLMMetadataError("PDF is encrypted") from exc
        raise LLMMetadataError("Could not read PDF — the file may be corrupt.") from exc
    text = (text or "").strip()
    if not text:
        raise LLMMetadataError("No readable text — is this a scanned image?")
    return text[:MAX_PDF_CHARS]


def _build_client():
    """Construct the chat client; raise the module error if AI is unconfigured."""
    if not llm_client.llm_enabled():
        raise LLMMetadataError("AI assist is not configured.")
    try:
        return llm_client.build_client()
    except ImportError as exc:  # openai not installed
        raise LLMMetadataError("openai package is not installed.") from exc


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


def _normalise_keywords(value) -> list:
    """Coerce the model's keywords into a clean, de-duplicated, capped list."""
    if isinstance(value, str):
        value = value.split(",")
    if not isinstance(value, list):
        return []
    out: list = []
    for item in value:
        s = str(item).strip()
        if s and s not in out:
            out.append(s)
    return out[:MAX_KEYWORDS]


def _normalise_authors(value) -> list:
    """Coerce the model's author value into a clean list of name strings."""
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        s = str(item).strip()
        if s and s not in out:
            out.append(s)
    return out


def _complete(client, text: str, language: str) -> dict:
    """Call the chat endpoint and return {abstract, keywords, title, authors, warnings}."""
    warnings: list = []
    model = llm_client.flash_model()
    lang_name = "Chinese" if language == "zh" else "English"
    system = (
        "You are an academic editor. Read the paper text and return a JSON object "
        f'with these keys: "abstract" — a concise summary of at most 250 words '
        f'written in {lang_name}; "keywords" — an array of 3 to 6 short topical '
        'keyword strings; "title" — the paper title; and "authors" — an array of '
        "author full names. Include \"title\" and \"authors\" ONLY if you are "
        "certain they are correct from the text; otherwise set \"title\" to null "
        "and \"authors\" to []. Return ONLY the JSON object, no prose."
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
        # Log full detail server-side; never echo provider internals to the client.
        _log.exception("LLM request failed")
        raise LLMMetadataError("AI request failed — please try again later.") from exc

    try:
        content = (resp.choices[0].message.content or "").strip()
    except (AttributeError, IndexError, TypeError) as exc:
        raise LLMMetadataError("The AI response could not be parsed.") from exc

    data = _parse_json(content)
    if not isinstance(data, dict):  # valid JSON can still be a list / str / number
        raise LLMMetadataError("The AI response could not be parsed.")

    raw_abstract = data.get("abstract")
    abstract = raw_abstract.strip() if isinstance(raw_abstract, str) else ""
    keywords = _normalise_keywords(data.get("keywords"))
    if not abstract:
        warnings.append("No abstract was generated — please write one manually.")
    if not keywords:
        warnings.append("No keywords were generated — please add them manually.")
    raw_title = data.get("title")
    title = raw_title.strip() if isinstance(raw_title, str) else ""
    authors = _normalise_authors(data.get("authors"))
    return {"abstract": abstract, "keywords": keywords,
            "title": title, "authors": authors, "warnings": warnings}


def generate_abstract_keywords(file_bytes: bytes, language: str = "en") -> dict:
    """Public entry point: PDF bytes -> {abstract, keywords, warnings}."""
    text = _pdf_text_from_bytes(file_bytes, language)
    client = _build_client()
    return _complete(client, text, language)
