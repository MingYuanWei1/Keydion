"""Vision-tier PDF reading: render pages to images and reason over them with a
multimodal LLM. Two entry points:

    transcribe_pdf(file_bytes, *, max_pages=50, language="en", strict=False) -> str
        Vision-as-OCR. Interactive callers receive "" on failure; strict
        publishing callers receive provider/render failures.
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
import time

import llm_client
import pdf_text
from services.publishing_contracts import IndexDeadlineExceeded

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


def _remaining_timeout(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    remaining = max(float(deadline) - time.monotonic(), 0.0)
    if remaining == 0.0:
        raise IndexDeadlineExceeded()
    return remaining


def _raise_deadline_if_expired(deadline: float | None, error: Exception) -> None:
    if deadline is not None and time.monotonic() >= float(deadline):
        raise IndexDeadlineExceeded() from error


def extract_with_vision(file_bytes: bytes, system_prompt: str, *,
                        max_pages: int = 10, language: str = "en",
                        deadline: float | None = None) -> dict:
    """Render pages and ask the vision model for a JSON object. Raises VisionError."""
    _remaining_timeout(deadline)
    if not llm_client.vision_model():
        raise VisionError("Vision model is not configured.")
    try:
        client = (
            llm_client.build_vision_client()
            if deadline is None
            else llm_client.build_vision_client(deadline=deadline)
        )
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _remaining_timeout(deadline)
    if client is None:
        raise VisionError("Vision model is not configured.")

    render_kwargs = {"max_pages": max_pages}
    if deadline is not None:
        render_kwargs["deadline"] = deadline
    try:
        pages = pdf_text.render_pdf_pages(file_bytes, **render_kwargs)
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _remaining_timeout(deadline)
    if not pages:
        raise VisionError("Could not render any PDF pages.")

    content = [{"type": "text", "text": system_prompt}] + _image_parts(pages)
    try:
        request = dict(
            model=llm_client.vision_model(),
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": content}],
        )
        if deadline is not None:
            request["timeout"] = _remaining_timeout(deadline)
        resp = client.chat.completions.create(**request)
        _remaining_timeout(deadline)
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:  # network/auth/rate-limit from any provider
        _raise_deadline_if_expired(deadline, exc)
        _log.exception("Vision request failed")
        raise VisionError("Vision request failed.") from exc

    try:
        text = (resp.choices[0].message.content or "").strip()
    except (AttributeError, IndexError, TypeError) as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise VisionError("The vision response could not be parsed.") from exc

    data = _parse_json(text)
    _remaining_timeout(deadline)
    if not isinstance(data, dict):
        raise VisionError("The vision response could not be parsed.")
    return data


_TRANSCRIBE_INSTRUCTION = (
    "Transcribe the text of these document pages into plain text, in natural "
    "reading order. Output only the transcription, in {lang}. Do not summarise, "
    "translate, or add commentary."
)


def transcribe_pdf(file_bytes: bytes, *, max_pages: int = 50,
                   language: str = "en",
                   deadline: float | None = None,
                   strict: bool = False) -> str:
    """Vision-as-OCR; optionally propagate failures for publishing work."""
    try:
        _remaining_timeout(deadline)
        if not llm_client.vision_model():
            if strict:
                raise VisionError("Vision model is not configured.")
            return ""
        client = (
            llm_client.build_vision_client()
            if deadline is None
            else llm_client.build_vision_client(deadline=deadline)
        )
        _remaining_timeout(deadline)
        if client is None:
            if strict:
                raise VisionError("Vision model is not configured.")
            return ""
        render_kwargs = {"max_pages": max_pages}
        if deadline is not None:
            render_kwargs["deadline"] = deadline
        if strict:
            render_kwargs["strict"] = True
        pages = pdf_text.render_pdf_pages(file_bytes, **render_kwargs)
        _remaining_timeout(deadline)
        if not pages:
            if strict:
                raise VisionError("Could not render any PDF pages.")
            return ""
        instruction = _TRANSCRIBE_INSTRUCTION.format(lang=_lang_name(language))
        content = [{"type": "text", "text": instruction}] + _image_parts(pages)
        request = dict(
            model=llm_client.vision_model(),
            temperature=0,
            messages=[{"role": "user", "content": content}],
        )
        if deadline is not None:
            request["timeout"] = _remaining_timeout(deadline)
        resp = client.chat.completions.create(**request)
        _remaining_timeout(deadline)
        text = (resp.choices[0].message.content or "").strip()
        _remaining_timeout(deadline)
        if strict and not text:
            raise VisionError("Vision transcription was empty.")
        return text
    except IndexDeadlineExceeded:
        raise
    except VisionError as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    except Exception as exc:  # transcription is best-effort unless strict
        _raise_deadline_if_expired(deadline, exc)
        _log.exception("Vision transcription failed")
        if strict:
            raise VisionError("Vision transcription failed.") from exc
        return ""
