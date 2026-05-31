"""Draft an abstract + keywords from a paper PDF via an OpenAI-compatible LLM.

Public surface:
    generate_abstract_keywords(file_bytes, language="en") -> dict
    LLMMetadataError

Provider-agnostic: the client base URL, API key, and model are resolved via
``llm_client`` (LLM_BASE_URL / LLM_API_KEY / LLM_DEFAULT_FLASH), so the same
code works against OpenAI, a local model (Ollama/vLLM), or any OpenAI-style API.
"""

from __future__ import annotations

import io
import json
import logging
import re

from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

import llm_client

MAX_PDF_CHARS = 12_000          # bound the prompt size / token cost
MAX_KEYWORDS = 6

_log = logging.getLogger(__name__)


class LLMMetadataError(Exception):
    """Raised when the PDF is unusable or the LLM request/response fails."""


def _pdf_text_from_bytes(file_bytes: bytes) -> str:
    """Extract concatenated text from a PDF given as bytes, capped to MAX_PDF_CHARS."""
    if not file_bytes:
        raise LLMMetadataError("Empty file")
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except PdfReadError as exc:
        # Keep the library-internal detail in the server log chain, not the client message.
        raise LLMMetadataError("Could not read PDF — the file may be corrupt.") from exc
    if reader.is_encrypted:
        raise LLMMetadataError("PDF is encrypted")
    parts = []
    total = 0
    for page in reader.pages:
        try:
            page_text = page.extract_text() or ""
        except Exception:  # PyPDF2 can throw a variety on odd pages
            page_text = ""
        parts.append(page_text)
        total += len(page_text)
        if total >= MAX_PDF_CHARS:  # stop early — we only send MAX_PDF_CHARS anyway
            break
    text = "\n".join(parts).strip()
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


def _complete(client, text: str, language: str) -> dict:
    """Call the chat endpoint and return {abstract, keywords, warnings}."""
    warnings: list = []
    model = llm_client.flash_model()
    lang_name = "Chinese" if language == "zh" else "English"
    system = (
        "You are an academic editor. Read the paper text and return a JSON object "
        f'with exactly two keys: "abstract" — a concise summary of at most 250 words '
        f"written in {lang_name} — and \"keywords\" — an array of 3 to 6 short topical "
        "keyword strings. Return ONLY the JSON object, no prose."
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
    return {"abstract": abstract, "keywords": keywords, "warnings": warnings}


def generate_abstract_keywords(file_bytes: bytes, language: str = "en") -> dict:
    """Public entry point: PDF bytes -> {abstract, keywords, warnings}."""
    text = _pdf_text_from_bytes(file_bytes)
    client = _build_client()
    return _complete(client, text, language)
