"""Vision-tier PDF reading: render pages to images and reason over them with a
multimodal LLM. Two entry points:

    transcribe_pdf(file_bytes, *, max_pages=50, language="en") -> str
        Vision-as-OCR. Returns concatenated plain text, "" on ANY failure (never raises).
    extract_with_vision(file_bytes, system_prompt, *, max_pages=10, language="en") -> dict
        Structured extraction with response_format=json_object. Raises VisionError
        on hard failure (no client / empty render / provider error / unparseable).

Provider/model resolved via llm_client.build_vision_client() + vision_model()
(LLM_VISION / LLM_VISION_API_KEY / LLM_VISION_BASE_URL). Page images go out as
OpenAI-style data:image/png;base64 content parts.
"""

from __future__ import annotations

import base64
import json
import logging
import re

import llm_client
import pdf_text

_log = logging.getLogger(__name__)


class VisionError(Exception):
    """Raised on a hard vision failure (no client / no pages / provider / parse)."""


def _parse_json(content: str):
    """Parse JSON, tolerating a model that wraps it in prose. Returns dict/list/None.

    Mirrors llm_metadata._parse_json (llm_metadata.py:63-77).
    """
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


def _image_parts(pages: list) -> list:
    """OpenAI-style image_url content parts (data:image/png;base64) for each page."""
    parts = []
    for png in pages:
        b64 = base64.b64encode(png).decode("ascii")
        parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}"},
        })
    return parts


def _lang_name(language: str) -> str:
    return "Chinese" if language == "zh" else "English"


def extract_with_vision(file_bytes: bytes, system_prompt: str, *,
                        max_pages: int = 10, language: str = "en") -> dict:
    """Render pages and ask the vision model for a JSON object. Raises VisionError."""
    if not llm_client.vision_model():
        raise VisionError("Vision model is not configured.")
    client = llm_client.build_vision_client()
    if client is None:
        raise VisionError("Vision model is not configured.")

    pages = pdf_text.render_pdf_pages(file_bytes, max_pages=max_pages)
    if not pages:
        raise VisionError("Could not render any PDF pages.")

    content = [{"type": "text", "text": system_prompt}] + _image_parts(pages)
    try:
        resp = client.chat.completions.create(
            model=llm_client.vision_model(),
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": content}],
        )
    except Exception as exc:  # network/auth/rate-limit from any provider
        _log.exception("Vision request failed")
        raise VisionError("Vision request failed.") from exc

    try:
        text = (resp.choices[0].message.content or "").strip()
    except (AttributeError, IndexError, TypeError) as exc:
        raise VisionError("The vision response could not be parsed.") from exc

    data = _parse_json(text)
    if not isinstance(data, dict):
        raise VisionError("The vision response could not be parsed.")
    return data


_TRANSCRIBE_INSTRUCTION = (
    "Transcribe the text of these document pages into plain text, in natural "
    "reading order. Output only the transcription, in {lang}. Do not summarise, "
    "translate, or add commentary."
)


def transcribe_pdf(file_bytes: bytes, *, max_pages: int = 50,
                   language: str = "en") -> str:
    """Vision-as-OCR. Concatenated plain-text transcription, "" on ANY failure."""
    try:
        if not llm_client.vision_model():
            return ""
        client = llm_client.build_vision_client()
        if client is None:
            return ""
        pages = pdf_text.render_pdf_pages(file_bytes, max_pages=max_pages)
        if not pages:
            return ""
        instruction = _TRANSCRIBE_INSTRUCTION.format(lang=_lang_name(language))
        content = [{"type": "text", "text": instruction}] + _image_parts(pages)
        resp = client.chat.completions.create(
            model=llm_client.vision_model(),
            temperature=0,
            messages=[{"role": "user", "content": content}],
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:  # transcription is best-effort; degrade to ""
        _log.exception("Vision transcription failed")
        return ""
