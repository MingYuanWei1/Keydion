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


if __name__ == "__main__":
    unittest.main()
