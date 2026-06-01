import sys
import unittest
from unittest import mock

import pdf_text
from pdf_text import extract_pdf_text, PdfTextError
from PyPDF2.errors import PdfReadError


class _StubPage:
    def __init__(self, text):
        self._text = text
    def extract_text(self):
        return self._text


class _StubReader:
    def __init__(self, *a, encrypted=False, pages=None, **k):
        self.is_encrypted = encrypted
        self.pages = pages or []


class PypdfPassTest(unittest.TestCase):
    def test_returns_pypdf_text_without_ocr(self):
        long_text = "word " * 40  # > 50 meaningful chars
        stub = _StubReader(pages=[_StubPage(long_text)])
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub), \
             mock.patch.object(pdf_text, "_ocr_pdf") as ocr:
            out = extract_pdf_text(b"%PDF-fake")
        self.assertIn("word", out)
        ocr.assert_not_called()

    def test_corrupt_raises_reason_corrupt(self):
        with mock.patch.object(pdf_text, "PdfReader", side_effect=PdfReadError("bad")):
            with self.assertRaises(PdfTextError) as ctx:
                extract_pdf_text(b"not a pdf")
        self.assertEqual(ctx.exception.reason, "corrupt")

    def test_encrypted_raises_reason_encrypted(self):
        stub = _StubReader(encrypted=True)
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub):
            with self.assertRaises(PdfTextError) as ctx:
                extract_pdf_text(b"%PDF-fake")
        self.assertEqual(ctx.exception.reason, "encrypted")


def _fake_ocr_modules(call_log, page_count):
    """Return a dict of fake fitz / pytesseract / PIL modules for sys.modules."""
    class _Pix:
        def tobytes(self, fmt):
            return b"PNGDATA"
    class _Page:
        def get_pixmap(self, dpi=300):
            return _Pix()
    class _Doc:
        def __init__(self):
            self._pages = [_Page() for _ in range(page_count)]
        def __iter__(self):
            return iter(self._pages)
        def close(self):
            pass
    fitz = mock.Mock()
    fitz.open.return_value = _Doc()
    pyt = mock.Mock()
    def _img_to_str(img, lang=None, timeout=None):
        call_log.append(lang)
        return "ocr-text"
    pyt.image_to_string.side_effect = _img_to_str
    pil = mock.Mock()
    pil.Image.open.return_value = object()
    return {"fitz": fitz, "pytesseract": pyt, "PIL": pil}


class OcrFallbackTest(unittest.TestCase):
    def test_empty_pypdf_triggers_ocr(self):
        stub = _StubReader(pages=[_StubPage("")])
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub), \
             mock.patch.object(pdf_text, "_ocr_pdf", return_value="recovered") as ocr:
            out = extract_pdf_text(b"%PDF-fake")
        self.assertEqual(out, "recovered")
        ocr.assert_called_once()

    def test_short_pypdf_triggers_ocr(self):
        stub = _StubReader(pages=[_StubPage("abc")])  # < 50 meaningful chars
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub), \
             mock.patch.object(pdf_text, "_ocr_pdf", return_value="recovered"):
            out = extract_pdf_text(b"%PDF-fake")
        self.assertEqual(out, "recovered")

    def test_ocr_empty_returns_pypdf_text(self):
        stub = _StubReader(pages=[_StubPage("")])
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub), \
             mock.patch.object(pdf_text, "_ocr_pdf", return_value=""):
            out = extract_pdf_text(b"%PDF-fake")
        self.assertEqual(out, "")

    def test_ocr_deps_missing_returns_empty(self):
        # No fitz/pytesseract/PIL in sys.modules -> lazy import fails -> "".
        with mock.patch.dict(sys.modules, {"fitz": None, "pytesseract": None, "PIL": None}):
            out = pdf_text._ocr_pdf(b"%PDF-fake", "eng", 10)
        self.assertEqual(out, "")

    def test_page_cap_respected_and_lang_passed(self):
        log = []
        with mock.patch.dict(sys.modules, _fake_ocr_modules(log, page_count=25)):
            out = pdf_text._ocr_pdf(b"%PDF-fake", "eng+chi_sim", max_pages=10)
        self.assertEqual(len(log), 10)            # capped at 10 pages
        self.assertTrue(all(l == "eng+chi_sim" for l in log))
        self.assertIn("ocr-text", out)

    def test_page_iteration_error_returns_empty(self):
        class _BadDoc:
            def __iter__(self):
                raise RuntimeError("corrupt page tree")
            def close(self):
                pass
        fitz = mock.Mock()
        fitz.open.return_value = _BadDoc()
        mods = {"fitz": fitz, "pytesseract": mock.Mock(), "PIL": mock.Mock()}
        with mock.patch.dict(sys.modules, mods):
            out = pdf_text._ocr_pdf(b"%PDF-fake", "eng", 10)
        self.assertEqual(out, "")

    def test_ocr_passes_a_positive_timeout(self):
        # Tesseract must be bounded — a hung page would otherwise block forever.
        captured = {}
        class _Pix:
            def tobytes(self, fmt):
                return b"PNGDATA"
        class _Page:
            def get_pixmap(self, dpi=300):
                return _Pix()
        class _Doc:
            def __iter__(self):
                return iter([_Page()])
            def close(self):
                pass
        fitz = mock.Mock()
        fitz.open.return_value = _Doc()
        pyt = mock.Mock()
        def _img(img, lang=None, timeout=None):
            captured["timeout"] = timeout
            return "text"
        pyt.image_to_string.side_effect = _img
        pil = mock.Mock()
        pil.Image.open.return_value = object()
        with mock.patch.dict(sys.modules, {"fitz": fitz, "pytesseract": pyt, "PIL": pil}):
            pdf_text._ocr_pdf(b"%PDF-fake", "eng", 10)
        self.assertIsNotNone(captured.get("timeout"))
        self.assertGreater(captured["timeout"], 0)

    def test_ocr_page_timeout_degrades_to_empty(self):
        # A timed-out page raises RuntimeError -> caught per-page -> that page "".
        class _Pix:
            def tobytes(self, fmt):
                return b"PNGDATA"
        class _Page:
            def get_pixmap(self, dpi=300):
                return _Pix()
        class _Doc:
            def __iter__(self):
                return iter([_Page()])
            def close(self):
                pass
        fitz = mock.Mock()
        fitz.open.return_value = _Doc()
        pyt = mock.Mock()
        pyt.image_to_string.side_effect = RuntimeError("Tesseract process timeout")
        pil = mock.Mock()
        pil.Image.open.return_value = object()
        with mock.patch.dict(sys.modules, {"fitz": fitz, "pytesseract": pyt, "PIL": pil}):
            out = pdf_text._ocr_pdf(b"%PDF-fake", "eng", 10)
        self.assertEqual(out, "")


if __name__ == "__main__":
    unittest.main()
