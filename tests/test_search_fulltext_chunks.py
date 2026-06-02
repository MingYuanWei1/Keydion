# tests/test_search_fulltext_chunks.py
import os
import pathlib
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module


def _rec(filename, **kw):
    r = {"filename": filename, "title": "", "author_name": "", "keywords": "",
         "ib_ee_data": "", "cp_data": "", "published_at": ""}
    r.update(kw)
    return r


def _fake_db_session(rows):
    """A mock matching `with db_session() as db: db.query(...).order_by(...).all()`."""
    fake_db = mock.MagicMock()
    fake_db.query.return_value.order_by.return_value.all.return_value = rows
    cm = mock.MagicMock()
    cm.__enter__.return_value = fake_db
    return cm


class FulltextIndex(unittest.TestCase):
    def test_groups_chunks_reassembles_and_lowercases(self):
        # Multi-chunk paper: build via the real chunker so reassemble is exercised.
        body_a = ("Cellular " * 150) + "mitochondria" + (" energy" * 150)
        chunks_a = app_module.rag_index.chunk_text(body_a)
        self.assertGreater(len(chunks_a), 1)  # ensure overlap is actually exercised
        rows = [("a.pdf", i, c) for i, c in enumerate(chunks_a)]
        rows.append(("b.pdf", 0, "Photosynthesis IN Plants"))

        with mock.patch.object(app_module, "db_session",
                               return_value=_fake_db_session(rows)):
            result = app_module._fulltext_index()

        self.assertEqual(set(result), {"a.pdf", "b.pdf"})
        self.assertEqual(result["a.pdf"], body_a.lower())       # grouped + reassembled + lowered
        self.assertEqual(result["b.pdf"], "photosynthesis in plants")


class SearchPapersFallback(unittest.TestCase):
    def _run(self, fulltext, extract_mock, query="mitochondria", filename="a.pdf"):
        with mock.patch.object(app_module, "load_paper_metadata", return_value=[]), \
             mock.patch.object(app_module, "build_paper_record",
                               side_effect=lambda fn, idx: _rec(fn)), \
             mock.patch.object(app_module, "_fulltext_index", return_value=fulltext), \
             mock.patch.object(app_module, "extract_pdf_text", extract_mock), \
             mock.patch.object(app_module, "PAPERS_DIR") as papers_dir:
            papers_dir.glob.return_value = [pathlib.Path(filename)]
            return app_module.search_papers(query)

    def test_prefers_indexed_chunks_without_extracting(self):
        extract = mock.Mock(side_effect=AssertionError("must not extract indexed paper"))
        out = self._run({"a.pdf": "cells contain mitochondria"}, extract)
        self.assertEqual([r["filename"] for r in out], ["a.pdf"])
        extract.assert_not_called()

    def test_ocr_fallback_for_unindexed_paper(self):
        extract = mock.Mock(return_value="Cells contain MITOCHONDRIA.")
        out = self._run({}, extract)              # no chunks for a.pdf
        self.assertEqual([r["filename"] for r in out], ["a.pdf"])
        extract.assert_called_once()


class ChunkPathEquivalence(unittest.TestCase):
    def test_chunk_path_matches_same_term_as_extraction(self):
        # Body longer than CHUNK_SIZE so chunk_text splits it and overlap matters.
        body = ("Alpha " * 200) + "mitochondria" + (" omega" * 200)
        term = "mitochondria"

        # The old extraction path would match this term.
        self.assertIn(term, body.lower())

        # The new stored-chunk path: chunk then reassemble (what _fulltext_index does).
        chunks = app_module.rag_index.chunk_text(body)
        self.assertGreater(len(chunks), 1)                      # overlap is exercised
        reassembled = app_module.rag_index.reassemble(chunks).lower()
        self.assertIn(term, reassembled)                        # term survives round-trip

        # search_papers via the chunk path returns the paper for the same term,
        # and never falls back to extraction.
        extract = mock.Mock(side_effect=AssertionError("must not extract indexed paper"))
        with mock.patch.object(app_module, "load_paper_metadata", return_value=[]), \
             mock.patch.object(app_module, "build_paper_record",
                               side_effect=lambda fn, idx: _rec(fn)), \
             mock.patch.object(app_module, "_fulltext_index",
                               return_value={"a.pdf": reassembled}), \
             mock.patch.object(app_module, "extract_pdf_text", extract), \
             mock.patch.object(app_module, "PAPERS_DIR") as papers_dir:
            papers_dir.glob.return_value = [pathlib.Path("a.pdf")]
            out = app_module.search_papers(term)

        self.assertEqual([r["filename"] for r in out], ["a.pdf"])
        extract.assert_not_called()


class IndexedEmptyString(unittest.TestCase):
    def test_indexed_empty_text_does_not_extract(self):
        # a.pdf is indexed but its stored text is empty ("" — present, not None).
        extract = mock.Mock(side_effect=AssertionError("must not extract indexed paper"))
        with mock.patch.object(app_module, "load_paper_metadata", return_value=[]), \
             mock.patch.object(app_module, "build_paper_record",
                               side_effect=lambda fn, idx: _rec(fn)), \
             mock.patch.object(app_module, "_fulltext_index", return_value={"a.pdf": ""}), \
             mock.patch.object(app_module, "extract_pdf_text", extract), \
             mock.patch.object(app_module, "PAPERS_DIR") as papers_dir:
            papers_dir.glob.return_value = [pathlib.Path("a.pdf")]
            out = app_module.search_papers("mitochondria")
        self.assertEqual(out, [])              # indexed, empty -> no match
        extract.assert_not_called()            # and crucially, no OCR fallback


if __name__ == "__main__":
    unittest.main()
