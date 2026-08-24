# tests/test_search_fulltext_chunks.py
import os
import inspect
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import services.search as app_module
import services.search as search_module


PAPER_A = "11111111-1111-4111-8111-111111111111"
PAPER_B = "22222222-2222-4222-8222-222222222222"


def _rec(filename, paper_id=PAPER_A, **kw):
    r = {"paper_id": paper_id, "current_revision": 2, "filename": filename,
         "title": "", "author_name": "", "keywords": "",
         "ib_ee_data": "", "cp_data": "", "published_at": ""}
    r.update(kw)
    return r


def _fake_db_session(rows):
    """Mock the visible Paper/chunk join used by the lexical index."""
    fake_db = mock.MagicMock()
    (fake_db.query.return_value.join.return_value.filter.return_value
     .order_by.return_value.all.return_value) = rows
    cm = mock.MagicMock()
    cm.__enter__.return_value = fake_db
    return cm


class FulltextIndex(unittest.TestCase):
    def test_groups_chunks_reassembles_and_lowercases(self):
        # Multi-chunk paper: build via the real chunker so reassemble is exercised.
        body_a = ("Cellular " * 150) + "mitochondria" + (" energy" * 150)
        chunks_a = app_module.rag_index.chunk_text(body_a)
        self.assertGreater(len(chunks_a), 1)  # ensure overlap is actually exercised
        rows = [(PAPER_A, 2, i, c) for i, c in enumerate(chunks_a)]
        rows.append((PAPER_B, 2, 0, "Photosynthesis IN Plants"))

        with mock.patch.object(search_module, "db_session",
                               return_value=_fake_db_session(rows)):
            result = app_module._fulltext_index()

        self.assertEqual(set(result), {PAPER_A, PAPER_B})
        self.assertEqual(result[PAPER_A], (2, body_a.lower()))
        self.assertEqual(result[PAPER_B], (2, "photosynthesis in plants"))


class SearchPapersFallback(unittest.TestCase):
    def _run(self, fulltext, extract_mock, query="mitochondria", filename="a.pdf"):
        record = _rec(filename)
        with mock.patch.object(
            search_module,
            "_visible_paper_records",
            return_value=[record],
        ), \
             mock.patch.object(search_module, "_fulltext_index", return_value=fulltext), \
             mock.patch.object(search_module, "extract_pdf_text", extract_mock,
                               create=True):
            result = app_module.search_papers(query)
        return result

    def test_prefers_indexed_chunks_without_extracting(self):
        extract = mock.Mock(side_effect=AssertionError("must not extract indexed paper"))
        out = self._run({PAPER_A: (2, "cells contain mitochondria")}, extract)
        self.assertEqual([r["filename"] for r in out], ["a.pdf"])
        extract.assert_not_called()

    def test_unindexed_paper_never_triggers_request_time_extraction(self):
        # Security: /search is anonymous; parsing/OCR-ing an unindexed PDF per
        # request is an unbounded-work sink. Unindexed papers match on metadata
        # only until the async indexer catches up.
        extract = mock.Mock(side_effect=AssertionError("must not extract on search"))
        out = self._run({}, extract)
        self.assertEqual(out, [])
        extract.assert_not_called()

    def test_revision_mismatch_never_matches_obsolete_indexed_text(self):
        extract = mock.Mock(side_effect=AssertionError("must not extract on search"))
        out = self._run({PAPER_A: (1, "obsolete")}, extract, query="obsolete")
        self.assertEqual(out, [])
        extract.assert_not_called()

    def test_search_never_globs_flat_pdfs_or_parses_them(self):
        source = inspect.getsource(search_module.search_papers)
        self.assertNotIn("glob(", source)
        self.assertNotIn("extract_pdf_text", source)
        self.assertIn("_visible_paper_records", source)


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
        with mock.patch.object(
            search_module,
            "_visible_paper_records",
            return_value=[_rec("a.pdf")],
        ), \
             mock.patch.object(search_module, "_fulltext_index",
                               return_value={PAPER_A: (2, reassembled)}), \
             mock.patch.object(search_module, "extract_pdf_text", extract,
                               create=True):
            out = app_module.search_papers(term)

        self.assertEqual([r["filename"] for r in out], ["a.pdf"])
        extract.assert_not_called()


class IndexedEmptyString(unittest.TestCase):
    def test_indexed_empty_text_does_not_extract(self):
        # a.pdf is indexed but its stored text is empty ("" — present, not None).
        extract = mock.Mock(side_effect=AssertionError("must not extract indexed paper"))
        with mock.patch.object(
            search_module,
            "_visible_paper_records",
            return_value=[_rec("a.pdf")],
        ), \
             mock.patch.object(
                 search_module, "_fulltext_index", return_value={PAPER_A: (2, "")}
             ), \
             mock.patch.object(search_module, "extract_pdf_text", extract,
                               create=True):
            out = app_module.search_papers("mitochondria")
        self.assertEqual(out, [])              # indexed, empty -> no match
        extract.assert_not_called()            # and crucially, no OCR fallback


if __name__ == "__main__":
    unittest.main()
