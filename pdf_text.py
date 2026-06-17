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
import os
import re
from concurrent.futures import ThreadPoolExecutor

from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

MIN_TEXT_CHARS = 50
DEFAULT_OCR_LANGS = "eng+chi_sim"   # chi_tra dropped: fewer langs = faster Tesseract
DEFAULT_MAX_OCR_PAGES = 10
OCR_PAGE_TIMEOUT = 30   # seconds per page — bounds a hung/slow Tesseract subprocess

# Each tesseract subprocess uses OpenMP internally; without this limit N parallel
# processes would all grab all cores and contend. setdefault respects any operator
# override already present in the environment.
os.environ.setdefault("OMP_THREAD_LIMIT", "1")

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


def _ocr_pool_size() -> int:
    """Return number of worker threads for parallel Tesseract OCR.

    Reads OCR_WORKERS from the environment (positive int). Defaults to
    cpu_count - 1, leaving a core free for the live gunicorn site during reindex.
    """
    env_val = os.environ.get("OCR_WORKERS", "").strip()
    if env_val:
        try:
            n = int(env_val)
            if n > 0:
                return n
        except ValueError:
            pass
    return max(1, (os.cpu_count() or 1) - 1)


def _ocr_pdf(file_bytes: bytes, langs: str, max_pages: int) -> str:
    """Rasterise pages with PyMuPDF and OCR them with Tesseract.

    Returns "" on any failure (missing deps / `tesseract` binary / render error /
    per-page OCR timeout), logged server-side. Never raises. Each page is bounded
    by OCR_PAGE_TIMEOUT so a hung/slow Tesseract subprocess can't block forever.

    Two-phase approach for parallelism:
      Phase 1 (main thread): render each page to PNG bytes sequentially — fitz is
        not thread-safe for concurrent access on one document.
      Phase 2 (thread pool): OCR each PNG concurrently — pytesseract shells out to
        `tesseract` (a subprocess), so threads give real parallelism here.
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

    # --- Phase 1: render pages to PNG bytes (main thread, sequential) ---
    _SENTINEL = object()  # marks a failed render; OCR will degrade to "" for it
    pngs = []
    try:
        try:
            for i, page in enumerate(doc):
                if i >= max_pages:
                    _log.info("OCR truncated at %d pages (document has more)", max_pages)
                    break
                try:
                    pngs.append(page.get_pixmap(dpi=300).tobytes("png"))
                except Exception:
                    _log.warning("OCR render failed on page %d", i, exc_info=True)
                    pngs.append(_SENTINEL)
        except Exception:
            _log.warning("OCR failed during page iteration", exc_info=True)
            return ""
    finally:
        try:
            doc.close()
        except Exception:
            pass

    if not pngs:
        return ""

    # --- Phase 2: OCR each PNG in a thread pool (order preserved) ---
    def _ocr_page(png) -> str:
        if png is _SENTINEL:
            return ""
        try:
            img = Image.open(io.BytesIO(png))
            return pytesseract.image_to_string(img, lang=langs, timeout=OCR_PAGE_TIMEOUT)
        except Exception:
            _log.warning("OCR failed on a page", exc_info=True)
            return ""

    workers = min(_ocr_pool_size(), len(pngs))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        parts = list(pool.map(_ocr_page, pngs))

    if parts:
        _log.info("OCR'd %d page(s) of a scanned PDF", len(parts))
    return "\n".join(parts)


def render_pdf_pages(file_bytes: bytes, *, max_pages: int = 10, dpi: int = 200) -> list[bytes]:
    """Rasterise up to max_pages pages to PNG bytes (one entry per rendered page).

    Reuses the PyMuPDF render path from _ocr_pdf. Returns [] on any failure
    (missing PyMuPDF / unopenable PDF / iteration error); pages that fail to
    render individually are skipped. Lazy fitz import keeps this a leaf concern.
    """
    try:
        import fitz                       # PyMuPDF
    except Exception:
        _log.warning("PyMuPDF unavailable; cannot render PDF pages", exc_info=True)
        return []
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
    except Exception:
        _log.warning("PyMuPDF could not open PDF for rendering", exc_info=True)
        return []

    pngs: list[bytes] = []
    try:
        try:
            for i, page in enumerate(doc):
                if i >= max_pages:
                    _log.info("render truncated at %d pages (document has more)", max_pages)
                    break
                try:
                    pngs.append(page.get_pixmap(dpi=dpi).tobytes("png"))
                except Exception:
                    _log.warning("render failed on page %d", i, exc_info=True)
        except Exception:
            _log.warning("render failed during page iteration", exc_info=True)
            return []
    finally:
        try:
            doc.close()
        except Exception:
            pass
    return pngs


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
