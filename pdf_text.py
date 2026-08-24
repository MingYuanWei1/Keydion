"""Shared PDF-bytes-to-text extraction: pypdf first, Tesseract OCR fallback.

Public surface:
    extract_pdf_text(file_bytes, *, ocr_langs=..., max_ocr_pages=10) -> str
    PdfTextError   (.reason is "corrupt" | "encrypted")

OCR is a fallback only: when pypdf yields fewer than MIN_TEXT_CHARS
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

from pypdf import PdfReader
from pypdf.errors import PdfReadError
from config import MAX_PDF_PAGES
from services.publishing_contracts import (
    IndexDeadlineExceeded,
    raise_deadline_if_expired as _raise_deadline_if_expired,
    remaining_timeout as _remaining_timeout,
)

MIN_TEXT_CHARS = 50
DEFAULT_OCR_LANGS = "eng+chi_sim"   # chi_tra dropped: fewer langs = faster Tesseract
DEFAULT_MAX_OCR_PAGES = 10
OCR_PAGE_TIMEOUT = 30   # seconds per page — bounds a hung/slow Tesseract subprocess

# Conservative DoS guard: a crafted PDF with pathological page dimensions could make
# get_pixmap allocate a huge bitmap. These caps only ever scale a page DOWN; ordinary
# documents at the normal 200/300 DPI stay well under both limits, so this is a no-op
# for real papers (a US-Letter page at 300 DPI is ~2550x3300 ≈ 8.4MP).
MAX_RENDER_PIXELS = 5000        # max width or height in pixels
MAX_RENDER_AREA = 25_000_000    # max width*height (~25 megapixels)

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


class PdfStructureError(Exception):
    """Untrusted PDF fails a structural budget (corrupt, encrypted, oversized)."""


def check_pdf_bytes(raw: bytes) -> int:
    """Structural admission check for one untrusted PDF before synchronous
    parsing (security finding: untrusted parser without isolation).

    Rejects non-PDF bytes, corrupt page trees, encrypted files, empty files,
    and documents over MAX_PDF_PAGES BEFORE rewrite/extraction work begins.
    This cannot preempt one blocking native call, but it bounds the page
    iteration that surrounds it and refuses pathological documents at the
    door. Returns the page count.
    """
    if not isinstance(raw, (bytes, bytearray)) or not raw.startswith(b"%PDF-"):
        raise PdfStructureError("not a PDF")
    try:
        reader = PdfReader(io.BytesIO(bytes(raw)), strict=True)
    except Exception as exc:
        raise PdfStructureError("corrupt PDF") from exc
    try:
        if reader.is_encrypted:
            raise PdfStructureError("encrypted PDF")
        page_count = len(reader.pages)
    except PdfStructureError:
        raise
    except Exception as exc:
        raise PdfStructureError("corrupt PDF page tree") from exc
    if page_count < 1:
        raise PdfStructureError("PDF has no pages")
    if page_count > MAX_PDF_PAGES:
        raise PdfStructureError(f"PDF has too many pages (limit {MAX_PDF_PAGES})")
    return page_count


def _meaningful_len(text: str) -> int:
    return len(re.sub(r"\s+", "", text or ""))


def _check_deadline(deadline: float | None) -> None:
    _remaining_timeout(deadline)


def _pypdf_text(
    file_bytes: bytes,
    *,
    deadline: float | None = None,
    strict: bool = False,
    max_pages: int | None = MAX_PDF_PAGES,
) -> str:
    _check_deadline(deadline)
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except PdfReadError as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise PdfTextError("corrupt") from exc
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _check_deadline(deadline)
    try:
        encrypted = reader.is_encrypted
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _check_deadline(deadline)
    if encrypted:
        raise PdfTextError("encrypted")
    try:
        pages = iter(reader.pages)
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _check_deadline(deadline)

    parts = []
    pages_read = 0
    while True:
        try:
            page = next(pages)
        except StopIteration:
            _check_deadline(deadline)
            break
        except IndexDeadlineExceeded:
            raise
        except Exception as exc:
            _raise_deadline_if_expired(deadline, exc)
            raise
        # Structural budget: stop extracting beyond the page cap so a
        # pathological document cannot expand unbounded work (security
        # finding: untrusted parser without isolation).
        if max_pages is not None and pages_read >= max_pages:
            _log.warning("text extraction truncated at %d pages", max_pages)
            break
        pages_read += 1
        _check_deadline(deadline)
        try:
            parts.append(page.extract_text() or "")
        except IndexDeadlineExceeded:
            raise
        except Exception as exc:
            _raise_deadline_if_expired(deadline, exc)
            if strict:
                raise
            # pypdf can throw a variety on odd pages. Interactive callers
            # retain the historical best-effort behavior.
            parts.append("")
        _check_deadline(deadline)
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


def _render_page_pixmap(page, dpi: int):
    """get_pixmap(dpi=...) with a conservative downward clamp on output size.

    Mirrors PyMuPDF's dpi scaling (zoom = dpi/72) but, for a page whose rendered
    bitmap would exceed MAX_RENDER_PIXELS in either dimension or MAX_RENDER_AREA in
    total, scales the zoom down so the output stays under the caps. A no-op for
    ordinary pages, which never approach the limits at normal DPI.
    """
    import fitz                            # PyMuPDF (caller already imported it)

    zoom = dpi / 72.0
    try:
        w = page.rect.width * zoom
        h = page.rect.height * zoom
    except IndexDeadlineExceeded:
        raise
    except Exception:
        return page.get_pixmap(dpi=dpi)     # can't measure → render as before
    if w <= 0 or h <= 0:
        return page.get_pixmap(dpi=dpi)
    scale = min(1.0, MAX_RENDER_PIXELS / max(w, h), (MAX_RENDER_AREA / (w * h)) ** 0.5)
    if scale >= 1.0:
        return page.get_pixmap(dpi=dpi)
    _log.warning("clamping pathological page render (%.0fx%.0f px scaled by %.3f)", w, h, scale)
    return page.get_pixmap(matrix=fitz.Matrix(zoom * scale, zoom * scale))


def _ocr_pdf(
    file_bytes: bytes,
    langs: str,
    max_pages: int,
    *,
    deadline: float | None = None,
    strict: bool = False,
) -> str:
    """Rasterise pages with PyMuPDF and OCR them with Tesseract.

    Best-effort callers receive "" on missing dependencies, render errors, or
    per-page OCR failures. Strict publishing callers receive those failures,
    and an exhausted deadline always raises ``IndexDeadlineExceeded``. Each
    page is bounded by OCR_PAGE_TIMEOUT so a hung/slow Tesseract subprocess
    cannot block forever.

    Two-phase approach for parallelism:
      Phase 1 (main thread): render each page to PNG bytes sequentially — fitz is
        not thread-safe for concurrent access on one document.
      Phase 2 (thread pool): OCR each PNG concurrently — pytesseract shells out to
        `tesseract` (a subprocess), so threads give real parallelism here.
    """
    _check_deadline(deadline)
    try:
        import fitz                       # PyMuPDF
        import pytesseract
        from PIL import Image
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        if strict:
            raise
        _log.warning("OCR dependencies unavailable; skipping OCR fallback", exc_info=True)
        return ""
    try:
        _check_deadline(deadline)
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        _check_deadline(deadline)
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        if strict:
            raise
        _log.warning("PyMuPDF could not open PDF for OCR", exc_info=True)
        return ""

    # --- Phase 1: render pages to PNG bytes (main thread, sequential) ---
    _SENTINEL = object()  # marks a failed render; OCR will degrade to "" for it
    pngs = []
    try:
        try:
            for i, page in enumerate(doc):
                _check_deadline(deadline)
                if i >= max_pages:
                    _log.info("OCR truncated at %d pages (document has more)", max_pages)
                    break
                try:
                    pngs.append(_render_page_pixmap(page, 300).tobytes("png"))
                    _check_deadline(deadline)
                except IndexDeadlineExceeded:
                    raise
                except Exception as exc:
                    _raise_deadline_if_expired(deadline, exc)
                    if strict:
                        raise
                    _log.warning("OCR render failed on page %d", i, exc_info=True)
                    pngs.append(_SENTINEL)
        except IndexDeadlineExceeded:
            raise
        except Exception as exc:
            _raise_deadline_if_expired(deadline, exc)
            if strict:
                raise
            _log.warning("OCR failed during page iteration", exc_info=True)
            return ""
    finally:
        try:
            doc.close()
        except Exception:
            pass

    _check_deadline(deadline)

    if not pngs:
        return ""

    # --- Phase 2: OCR each PNG in a thread pool (order preserved) ---
    def _ocr_page(png) -> str:
        if png is _SENTINEL:
            return ""
        try:
            timeout = _remaining_timeout(deadline)
            img = Image.open(io.BytesIO(png))
            _check_deadline(deadline)
            text = pytesseract.image_to_string(
                img,
                lang=langs,
                timeout=(
                    OCR_PAGE_TIMEOUT
                    if timeout is None
                    else min(float(OCR_PAGE_TIMEOUT), timeout)
                ),
            )
            _check_deadline(deadline)
            return text
        except IndexDeadlineExceeded:
            raise
        except Exception as exc:
            _raise_deadline_if_expired(deadline, exc)
            if strict:
                raise
            _log.warning("OCR failed on a page", exc_info=True)
            return ""

    workers = min(_ocr_pool_size(), len(pngs))
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            parts = list(pool.map(_ocr_page, pngs))
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _check_deadline(deadline)

    if parts:
        _log.info("OCR'd %d page(s) of a scanned PDF", len(parts))
    return "\n".join(parts)


def render_pdf_pages(
    file_bytes: bytes,
    *,
    max_pages: int = 10,
    dpi: int = 200,
    deadline: float | None = None,
    strict: bool = False,
) -> list[bytes]:
    """Rasterise up to max_pages pages to PNG bytes (one entry per rendered page).

    Reuses the PyMuPDF render path from _ocr_pdf. Best-effort callers receive []
    on a whole-document failure and skip individual failed pages; strict callers
    receive those failures. Lazy fitz import keeps this a leaf concern.
    """
    _check_deadline(deadline)
    try:
        import fitz                       # PyMuPDF
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        if strict:
            raise
        _log.warning("PyMuPDF unavailable; cannot render PDF pages", exc_info=True)
        return []
    try:
        _check_deadline(deadline)
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        _check_deadline(deadline)
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        if strict:
            raise
        _log.warning("PyMuPDF could not open PDF for rendering", exc_info=True)
        return []

    pngs: list[bytes] = []
    try:
        try:
            for i, page in enumerate(doc):
                _check_deadline(deadline)
                if i >= max_pages:
                    _log.info("render truncated at %d pages (document has more)", max_pages)
                    break
                try:
                    pngs.append(_render_page_pixmap(page, dpi).tobytes("png"))
                    _check_deadline(deadline)
                except IndexDeadlineExceeded:
                    raise
                except Exception as exc:
                    _raise_deadline_if_expired(deadline, exc)
                    if strict:
                        raise
                    _log.warning("render failed on page %d", i, exc_info=True)
        except IndexDeadlineExceeded:
            raise
        except Exception as exc:
            _raise_deadline_if_expired(deadline, exc)
            if strict:
                raise
            _log.warning("render failed during page iteration", exc_info=True)
            return []
    finally:
        try:
            doc.close()
        except Exception:
            pass
    _check_deadline(deadline)
    return pngs


def extract_pdf_text(file_bytes: bytes, *, ocr_langs: str = DEFAULT_OCR_LANGS,
                     max_ocr_pages: int = DEFAULT_MAX_OCR_PAGES,
                     vision_fallback=None,
                     deadline: float | None = None,
                     strict: bool = False,
                     max_pages: int | None = MAX_PDF_PAGES) -> str:
    """PDF bytes -> text. pypdf first; scanned-doc fallback for thin text layers.

    For a scanned/image-only PDF (pypdf yields < MIN_TEXT_CHARS), the fallback is:
      - vision_fallback(file_bytes, max_ocr_pages) when a callable is injected
        (caller pre-binds language), else
      - the local Tesseract _ocr_pdf path (unchanged).
    Blank fallback output degrades to the pypdf text for best-effort callers and
    raises for strict publishing callers. Raises PdfTextError for a corrupt or
    encrypted PDF.
    """
    _check_deadline(deadline)
    text = _pypdf_text(file_bytes, deadline=deadline, strict=strict,
                       max_pages=max_pages)
    _check_deadline(deadline)
    if _meaningful_len(text) >= MIN_TEXT_CHARS:
        return text
    try:
        if vision_fallback is not None:
            scanned_text = vision_fallback(file_bytes, max_ocr_pages)
        else:
            scanned_text = _ocr_pdf(
                file_bytes,
                ocr_langs,
                max_ocr_pages,
                deadline=deadline,
                strict=strict,
            )
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _check_deadline(deadline)
    if strict and not scanned_text.strip():
        raise ValueError("PDF fallback produced no indexable text")
    return scanned_text if scanned_text.strip() else text
