"""Vision-tier PDF reading: render pages to images and reason over them with a
multimodal LLM. Two entry points:

    transcribe_pdf(file_bytes, *, max_pages=50, language="en", strict=False) -> str
        Vision-as-OCR. Interactive callers receive "" on failure; strict
        publishing callers receive provider/render failures.
    extract_with_vision(file_bytes, system_prompt, *, max_pages=10, language="en") -> dict
        Structured extraction with response_format=json_object. Raises VisionError
        on hard failure (no client / empty render / provider error / unparseable).

Provider/model resolved via llm_client.build_vision_client() + vision_model()
through the Worker. Page images go out as
OpenAI-style data:image/png;base64 content parts; the completion itself crosses
llm_client's conversation interface (chat / chat_json).
"""

from __future__ import annotations

import base64
import logging

import llm_client
import pdf_text
from services.publishing_contracts import (
    IndexDeadlineExceeded,
    raise_deadline_if_expired as _raise_deadline_if_expired,
    remaining_timeout as _remaining_timeout,
)

_log = logging.getLogger(__name__)


class VisionError(Exception):
    """Raised on a hard vision failure (no client / no pages / provider / parse)."""


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
        return llm_client.chat_json(
            [{"role": "user", "content": content}],
            tier="vision",
            temperature=0.2,
            deadline=deadline,
            client=client,
        )
    except llm_client.LLMChatParseError as exc:
        raise VisionError("The vision response could not be parsed.") from exc
    except llm_client.LLMChatRequestError as exc:
        raise VisionError("Vision request failed.") from exc


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
        text = llm_client.chat(
            [{"role": "user", "content": content}],
            tier="vision",
            temperature=0,
            deadline=deadline,
            client=client,
        )
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
