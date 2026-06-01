"""Shared PDF-bytes-to-text extraction: PyPDF2 first, Tesseract OCR fallback.

Public surface:
    extract_pdf_text(file_bytes, *, ocr_langs=..., max_ocr_pages=10) -> str
    PdfTextError   (.reason is "corrupt" | "encrypted")

OCR is a fallback only: when PyPDF2 yields fewer than MIN_TEXT_CHARS
non-whitespace characters (a scanned / image-only PDF) the pages are rasterised
with PyMuPDF and run through Tesseract. OCR dependencies are imported lazily, so
a host without them (or without the `tesseract` binary) keeps serving text-based
PDFs and silently degrades scanned ones to empty text.
"""
from __future__ import annotations

import io
import logging
import re

from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

MIN_TEXT_CHARS = 50
DEFAULT_OCR_LANGS = "eng+chi_sim+chi_tra"
DEFAULT_MAX_OCR_PAGES = 10

_log = logging.getLogger(__name__)


class PdfTextError(Exception):
    """Unreadable (corrupt) or encrypted PDF. `.reason` is 'corrupt' | 'encrypted'."""
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _meaningful_len(text: str) -> int:
    return len(re.sub(r"\s+", "", text or ""))


def _pypdf_text(file_bytes: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except PdfReadError as exc:
        raise PdfTextError("corrupt") from exc
    if reader.is_encrypted:
        raise PdfTextError("encrypted")
    parts = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:  # PyPDF2 can throw a variety on odd pages
            parts.append("")
    return "\n".join(parts)


def _ocr_pdf(file_bytes: bytes, langs: str, max_pages: int) -> str:
    """OCR placeholder — implemented in Task 3."""
    return ""


def extract_pdf_text(file_bytes: bytes, *, ocr_langs: str = DEFAULT_OCR_LANGS,
                     max_ocr_pages: int = DEFAULT_MAX_OCR_PAGES) -> str:
    """PDF bytes -> text. PyPDF2 first; OCR fallback for scanned PDFs.

    Raises PdfTextError for a corrupt or encrypted PDF.
    """
    text = _pypdf_text(file_bytes)
    if _meaningful_len(text) >= MIN_TEXT_CHARS:
        return text
    ocr_text = _ocr_pdf(file_bytes, ocr_langs, max_ocr_pages)
    return ocr_text if ocr_text.strip() else text
