"""Draft an abstract + keywords from a paper PDF via an OpenAI-compatible LLM.

Public surface:
    generate_abstract_keywords(file_bytes, language="en") -> dict
    LLMMetadataError

Provider-agnostic: the client base URL, API key, and model are resolved via
``llm_client`` (LLM_BASE_URL / LLM_API_KEY / LLM_DEFAULT_FLASH), so the same
code works against OpenAI, a local model (Ollama/vLLM), or any OpenAI-style API.
"""

from __future__ import annotations

from pdf_text import extract_pdf_text, PdfTextError

import llm_client
from vision_extractor import VisionFirstExtractor

MAX_PDF_CHARS = 12_000          # bound the prompt size / token cost
MAX_KEYWORDS = 6

ABSTRACT_SYSTEM_PROMPT_EN = (
    "You are an academic editor. The images are the rendered pages of a paper. "
    'Return a JSON object with these keys: "abstract" — a concise summary of at '
    'most 250 words written in English; "keywords" — an array of 3 to 6 short '
    'topical keyword strings; "title" — the paper title; and "authors" — an array '
    'of author full names. Include "title" and "authors" ONLY if you are certain '
    'they are correct from the pages; otherwise set "title" to null and "authors" '
    "to []. Return ONLY the JSON object, no prose."
)

ABSTRACT_SYSTEM_PROMPT_ZH = (
    "You are an academic editor. The images are the rendered pages of a paper. "
    'Return a JSON object with these keys: "abstract" — a concise summary of at '
    'most 250 words written in Chinese; "keywords" — an array of 3 to 6 short '
    'topical keyword strings; "title" — the paper title; and "authors" — an array '
    'of author full names. Include "title" and "authors" ONLY if you are certain '
    'they are correct from the pages; otherwise set "title" to null and "authors" '
    "to []. Return ONLY the JSON object, no prose."
)

class LLMMetadataError(Exception):
    """Raised when the PDF is unusable or the LLM request/response fails."""


def _ocr_langs_for(language: str) -> str:
    """Tesseract lang string biased by the paper's declared language."""
    return "chi_sim+eng" if language == "zh" else "eng"   # chi_tra dropped for speed


def _pdf_text_from_bytes(file_bytes: bytes, language: str = "en") -> str:
    """Extract concatenated PDF text (pypdf + OCR fallback), capped to MAX_PDF_CHARS."""
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


def _complete(text: str, language: str) -> dict:
    """Call the chat endpoint and return {abstract, keywords, title, authors, warnings}."""
    warnings: list = []
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
        data = llm_client.chat_json(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": text},
            ],
            tier="flash",
            temperature=0.2,
        )
    except llm_client.LLMChatUnavailable as exc:
        raise LLMMetadataError(str(exc)) from exc
    except llm_client.LLMChatRequestError as exc:
        raise LLMMetadataError("AI request failed — please try again later.") from exc
    except llm_client.LLMChatParseError as exc:
        raise LLMMetadataError("The AI response could not be parsed.") from exc

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


def _result_from_vision(data: dict) -> dict:
    """Shape a vision extract_with_vision dict like _complete's return value."""
    warnings: list = []
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


class AbstractExtractor(VisionFirstExtractor):
    """Vision-first abstract + keywords drafting; OCR+text-LLM fallback."""

    def __init__(self, language: str = "en"):
        self.language = language

    def build_prompt(self) -> str:
        return ABSTRACT_SYSTEM_PROMPT_ZH if self.language == "zh" else ABSTRACT_SYSTEM_PROMPT_EN

    def shape_vision(self, data: dict) -> dict:
        return _result_from_vision(data)

    def fallback(self, file_bytes: bytes) -> dict:
        text = _pdf_text_from_bytes(file_bytes, self.language)
        return _complete(text, self.language)


def generate_abstract_keywords(file_bytes: bytes, language: str = "en") -> dict:
    """Public entry point: PDF bytes -> {abstract, keywords, title, authors, warnings}.

    Vision-first when a vision model is configured; OCR+text-LLM otherwise.
    A vision failure falls back to the legacy path rather than hard-erroring.
    """
    if not file_bytes:
        raise LLMMetadataError("Empty file")
    return AbstractExtractor(language).extract(file_bytes)
