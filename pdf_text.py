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
OCR_PAGE_TIMEOUT = 30   # seconds per page — bounds a hung/slow Tesseract subprocess

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
    """Rasterise pages with PyMuPDF and OCR them with Tesseract.

    Returns "" on any failure (missing deps / `tesseract` binary / render error /
    per-page OCR timeout), logged server-side. Never raises. Each page is bounded
    by OCR_PAGE_TIMEOUT so a hung/slow Tesseract subprocess can't block forever.
    """
    try:
        import fitz                       # PyMuPDF
        import pytesseract
        from PIL import Image
    except Exception:
        _log.warning("OCR dependencies unavailable; skipping OCR fallback", exc_info=True)
        return ""
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
    except Exception:
        _log.warning("PyMuPDF could not open PDF for OCR", exc_info=True)
        return ""
    parts = []
    try:
        try:
            for i, page in enumerate(doc):
                if i >= max_pages:
                    _log.info("OCR truncated at %d pages (document has more)", max_pages)
                    break
                try:
                    pix = page.get_pixmap(dpi=300)
                    img = Image.open(io.BytesIO(pix.tobytes("png")))
                    parts.append(pytesseract.image_to_string(
                        img, lang=langs, timeout=OCR_PAGE_TIMEOUT))
                except Exception:
                    _log.warning("OCR failed on page %d", i, exc_info=True)
                    parts.append("")
        except Exception:
            _log.warning("OCR failed during page iteration", exc_info=True)
            return ""
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return "\n".join(parts)


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
