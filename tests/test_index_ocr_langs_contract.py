# tests/test_index_ocr_langs_contract.py
"""Contract: _index_ocr_langs returns the correct Tesseract language string
based on the paper's declared language, and _rag_paper_text calls
pdf_text.extract_pdf_text with max_ocr_pages=50 and the language-derived
ocr_langs string."""
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from flask import Flask

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import services.ai as ask_module


class IndexOcrLangsHelper(unittest.TestCase):
    def _langs(self, lang):
        return ask_module._index_ocr_langs(lang)

    def test_en_returns_eng(self):
        self.assertEqual(self._langs("en"), "eng")

    def test_zh_returns_chi_sim_eng(self):
        self.assertEqual(self._langs("zh"), "chi_sim+eng")

    def test_empty_returns_both(self):
        self.assertEqual(self._langs(""), "eng+chi_sim")

    def test_unknown_lang_returns_both(self):
        self.assertEqual(self._langs("fr"), "eng+chi_sim")

    def test_case_insensitive_en(self):
        self.assertEqual(self._langs("EN"), "eng")

    def test_case_insensitive_zh_with_whitespace(self):
        self.assertEqual(self._langs(" Zh "), "chi_sim+eng")


class RagPaperTextBehavior(unittest.TestCase):
    def _call_rag_paper_text(self, language):
        paper_id = "11111111-1111-4111-8111-111111111111"
        fake_bytes = b"%PDF-fake"
        library = mock.Mock()
        library.current_pdf.return_value = SimpleNamespace(
            paper=SimpleNamespace(language=language),
            path=Path("/safe/2.pdf"),
        )
        app = Flask(__name__)
        app.extensions["paper_library"] = library
        with app.app_context(), mock.patch.object(
            ask_module.pdf_text, "extract_pdf_text",
            return_value="extracted text",
        ) as mock_extract, mock.patch(
            "pathlib.Path.read_bytes", return_value=fake_bytes
        ):
            result = ask_module._rag_paper_text(paper_id)
        library.current_pdf.assert_called_once_with(paper_id)
        return mock_extract, result

    def test_en_paper_uses_eng_and_50_page_cap(self):
        mock_extract, result = self._call_rag_paper_text("en")
        kwargs = mock_extract.call_args.kwargs
        self.assertEqual(kwargs["max_ocr_pages"], 50)
        self.assertEqual(kwargs["ocr_langs"], "eng")
        self.assertEqual(result, "extracted text")

    def test_zh_paper_uses_chi_sim_eng_and_50_page_cap(self):
        mock_extract, result = self._call_rag_paper_text("zh")
        kwargs = mock_extract.call_args.kwargs
        self.assertEqual(kwargs["max_ocr_pages"], 50)
        self.assertEqual(kwargs["ocr_langs"], "chi_sim+eng")
        self.assertEqual(result, "extracted text")
