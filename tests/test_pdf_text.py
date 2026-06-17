import io
import os
import sys
import time
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


    def test_parallel_ocr_preserves_page_order(self):
        """pool.map must return results in input order even if later pages finish first.

        Each page encodes its index in its PNG bytes.  image_to_string sleeps
        longer for lower-indexed pages so higher-indexed pages finish first —
        this makes any order bug observable.  The joined output must still be
        "p0\\np1\\np2\\np3\\np4".
        """
        NUM_PAGES = 5
        SLEEP_SCALE = 0.01  # seconds; tiny but enough to force out-of-order completion

        class _Pix:
            def __init__(self, idx):
                self._idx = idx
            def tobytes(self, fmt):
                # Encode index so the worker can recover it.
                return b"PAGE:%d" % self._idx

        class _Page:
            def __init__(self, idx):
                self._idx = idx
            def get_pixmap(self, dpi=300):
                return _Pix(self._idx)

        class _Doc:
            def __init__(self):
                self._pages = [_Page(i) for i in range(NUM_PAGES)]
            def __iter__(self):
                return iter(self._pages)
            def close(self):
                pass

        fitz = mock.Mock()
        fitz.open.return_value = _Doc()

        pyt = mock.Mock()
        def _img_to_str(img, lang=None, timeout=None):
            # img is the PIL image object returned by Image.open; we stored the
            # raw bytes on it so we can recover the index.
            raw = img._raw_bytes
            idx = int(raw.split(b":")[1])
            # Earlier pages sleep longer → later pages finish first.
            time.sleep((NUM_PAGES - idx) * SLEEP_SCALE)
            return f"p{idx}"
        pyt.image_to_string.side_effect = _img_to_str

        # PIL.Image.open: instead of the real thing, return a small carrier
        # object that holds the raw bytes so the worker can decode the index.
        pil = mock.Mock()
        def _image_open(buf):
            carrier = mock.Mock()
            carrier._raw_bytes = buf.read() if hasattr(buf, "read") else buf
            return carrier
        pil.Image.open.side_effect = _image_open

        with mock.patch.dict(sys.modules, {"fitz": fitz, "pytesseract": pyt, "PIL": pil}):
            out = pdf_text._ocr_pdf(b"%PDF-fake", "eng", NUM_PAGES)

        self.assertEqual(out, "\n".join(f"p{i}" for i in range(NUM_PAGES)))

    def test_single_page_render_failure_sentinel(self):
        """A failed get_pixmap on the middle page degrades to "" for that slot
        while the surrounding pages keep their text and positions.
        """
        class _GoodPix:
            def tobytes(self, fmt):
                return b"PNGDATA"

        class _GoodPage:
            def get_pixmap(self, dpi=300):
                return _GoodPix()

        class _BadPage:
            def get_pixmap(self, dpi=300):
                raise RuntimeError("render failed")

        class _Doc:
            def __init__(self):
                self._pages = [_GoodPage(), _BadPage(), _GoodPage()]
            def __iter__(self):
                return iter(self._pages)
            def close(self):
                pass

        fitz = mock.Mock()
        fitz.open.return_value = _Doc()
        pyt = mock.Mock()
        pyt.image_to_string.return_value = "ok-text"
        pil = mock.Mock()
        pil.Image.open.return_value = object()

        with mock.patch.dict(sys.modules, {"fitz": fitz, "pytesseract": pyt, "PIL": pil}):
            out = pdf_text._ocr_pdf(b"%PDF-fake", "eng", 10)

        parts = out.split("\n")
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], "ok-text")
        self.assertEqual(parts[1], "")        # middle page degraded to sentinel
        self.assertEqual(parts[2], "ok-text")


class OcrPoolSizeTest(unittest.TestCase):
    """Unit tests for pdf_text._ocr_pool_size()."""

    def _call(self, env_override):
        with mock.patch.dict(os.environ, env_override, clear=False):
            return pdf_text._ocr_pool_size()

    def test_positive_int_env_honored(self):
        result = self._call({"OCR_WORKERS": "7"})
        self.assertEqual(result, 7)

    def test_zero_falls_back_to_default(self):
        result = self._call({"OCR_WORKERS": "0"})
        expected = max(1, (os.cpu_count() or 1) - 1)
        self.assertEqual(result, expected)

    def test_negative_falls_back_to_default(self):
        result = self._call({"OCR_WORKERS": "-3"})
        expected = max(1, (os.cpu_count() or 1) - 1)
        self.assertEqual(result, expected)

    def test_garbage_falls_back_to_default(self):
        result = self._call({"OCR_WORKERS": "auto"})
        expected = max(1, (os.cpu_count() or 1) - 1)
        self.assertEqual(result, expected)

    def test_unset_falls_back_to_default(self):
        # Remove OCR_WORKERS entirely from env for this call.
        env = {k: v for k, v in os.environ.items() if k != "OCR_WORKERS"}
        with mock.patch.dict(os.environ, env, clear=True):
            result = pdf_text._ocr_pool_size()
        expected = max(1, (os.cpu_count() or 1) - 1)
        self.assertEqual(result, expected)

    def test_result_is_at_least_one(self):
        # Even on a single-core machine the pool must have at least 1 worker.
        with mock.patch("os.cpu_count", return_value=1):
            result = self._call({"OCR_WORKERS": ""})
        self.assertGreaterEqual(result, 1)


class OcrLangsTest(unittest.TestCase):
    def test_default_langs_drop_chi_tra(self):
        # Speed: traditional-Chinese detection is dropped from the default langs.
        self.assertNotIn("chi_tra", pdf_text.DEFAULT_OCR_LANGS)
        self.assertIn("chi_sim", pdf_text.DEFAULT_OCR_LANGS)
        self.assertIn("eng", pdf_text.DEFAULT_OCR_LANGS)


class RenderPdfPagesTest(unittest.TestCase):
    def test_renders_png_bytes_per_page_capped(self):
        class _Pix:
            def tobytes(self, fmt):
                return b"PNGDATA"
        class _Page:
            def get_pixmap(self, dpi=200):
                return _Pix()
        class _Doc:
            def __init__(self):
                self._pages = [_Page() for _ in range(25)]
            def __iter__(self):
                return iter(self._pages)
            def close(self):
                pass
        fitz = mock.Mock()
        fitz.open.return_value = _Doc()
        with mock.patch.dict(sys.modules, {"fitz": fitz}):
            out = pdf_text.render_pdf_pages(b"%PDF-fake", max_pages=10)
        self.assertEqual(len(out), 10)              # capped at max_pages
        self.assertTrue(all(p == b"PNGDATA" for p in out))

    def test_dpi_is_forwarded_to_get_pixmap(self):
        captured = {}
        class _Pix:
            def tobytes(self, fmt):
                return b"PNGDATA"
        class _Page:
            def get_pixmap(self, dpi=200):
                captured["dpi"] = dpi
                return _Pix()
        class _Doc:
            def __iter__(self):
                return iter([_Page()])
            def close(self):
                pass
        fitz = mock.Mock()
        fitz.open.return_value = _Doc()
        with mock.patch.dict(sys.modules, {"fitz": fitz}):
            pdf_text.render_pdf_pages(b"%PDF-fake", dpi=200)
        self.assertEqual(captured.get("dpi"), 200)

    def test_bad_pdf_returns_empty_list(self):
        fitz = mock.Mock()
        fitz.open.side_effect = RuntimeError("not a pdf")
        with mock.patch.dict(sys.modules, {"fitz": fitz}):
            out = pdf_text.render_pdf_pages(b"garbage")
        self.assertEqual(out, [])

    def test_missing_deps_returns_empty_list(self):
        with mock.patch.dict(sys.modules, {"fitz": None}):
            out = pdf_text.render_pdf_pages(b"%PDF-fake")
        self.assertEqual(out, [])

    def test_per_page_render_failure_is_skipped(self):
        class _Pix:
            def tobytes(self, fmt):
                return b"PNGDATA"
        class _GoodPage:
            def get_pixmap(self, dpi=200):
                return _Pix()
        class _BadPage:
            def get_pixmap(self, dpi=200):
                raise RuntimeError("render failed")
        class _Doc:
            def __init__(self):
                self._pages = [_GoodPage(), _BadPage(), _GoodPage()]
            def __iter__(self):
                return iter(self._pages)
            def close(self):
                pass
        fitz = mock.Mock()
        fitz.open.return_value = _Doc()
        with mock.patch.dict(sys.modules, {"fitz": fitz}):
            out = pdf_text.render_pdf_pages(b"%PDF-fake", max_pages=10)
        self.assertEqual(out, [b"PNGDATA", b"PNGDATA"])   # bad page dropped


class VisionFallbackTest(unittest.TestCase):
    def test_thin_text_uses_injected_vision_fallback(self):
        stub = _StubReader(pages=[_StubPage("abc")])      # < 50 meaningful chars
        vf = mock.Mock(return_value="vision-transcribed")
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub), \
             mock.patch.object(pdf_text, "_ocr_pdf") as ocr:
            out = extract_pdf_text(b"%PDF-fake", vision_fallback=vf)
        self.assertEqual(out, "vision-transcribed")
        vf.assert_called_once()
        ocr.assert_not_called()                            # vision replaces Tesseract

    def test_vision_fallback_receives_bytes_and_max_pages(self):
        stub = _StubReader(pages=[_StubPage("")])
        captured = {}
        def _vf(b, mp):
            captured["bytes"] = b
            captured["max_pages"] = mp
            return "ok"
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub):
            extract_pdf_text(b"%PDF-fake", max_ocr_pages=42, vision_fallback=_vf)
        self.assertEqual(captured["bytes"], b"%PDF-fake")
        self.assertEqual(captured["max_pages"], 42)

    def test_no_fallback_falls_back_to_tesseract(self):
        stub = _StubReader(pages=[_StubPage("")])
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub), \
             mock.patch.object(pdf_text, "_ocr_pdf", return_value="tess") as ocr:
            out = extract_pdf_text(b"%PDF-fake")            # no vision_fallback
        self.assertEqual(out, "tess")
        ocr.assert_called_once()

    def test_sufficient_text_skips_vision_fallback(self):
        long_text = "word " * 40                            # > 50 meaningful chars
        stub = _StubReader(pages=[_StubPage(long_text)])
        vf = mock.Mock(return_value="should-not-be-used")
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub):
            out = extract_pdf_text(b"%PDF-fake", vision_fallback=vf)
        self.assertIn("word", out)
        vf.assert_not_called()

    def test_empty_vision_result_returns_pypdf_text(self):
        # Mirrors the existing OCR-empty contract: blank vision output -> pypdf text.
        stub = _StubReader(pages=[_StubPage("")])
        with mock.patch.object(pdf_text, "PdfReader", return_value=stub):
            out = extract_pdf_text(b"%PDF-fake", vision_fallback=lambda b, mp: "")
        self.assertEqual(out, "")


if __name__ == "__main__":
    unittest.main()
