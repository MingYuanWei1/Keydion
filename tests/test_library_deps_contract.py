"""Contract tests for _lib_full_text, _lib_search, _lib_paper_meta,
_lib_paper_url, and _build_library_deps — the DB-backed deps for library_tools.

No real DB or Flask app context is needed; everything is mocked at the
app_module attribute level.
"""
import os
import types
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module


def _make_db_cm(rows):
    """Build a mock context manager whose db.query(...).filter(...).order_by(...).all() returns rows."""
    fake_db = mock.MagicMock()
    fake_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = rows
    cm = mock.MagicMock()
    cm.__enter__.return_value = fake_db
    cm.__exit__.return_value = False
    return cm


class TestLibFullText(unittest.TestCase):

    def test_reassembles_stored_chunks_in_order(self):
        chunk_a = types.SimpleNamespace(content="hello world this is chunk A extra text here")
        chunk_b = types.SimpleNamespace(content="extra text here and some more content B")
        cm = _make_db_cm([chunk_a, chunk_b])
        with mock.patch.object(app_module, "db_session", return_value=cm), \
             mock.patch.object(app_module, "_rag_paper_text") as mock_fallback:
            result = app_module._lib_full_text("paper.pdf")
        expected = app_module.rag_index.reassemble(["hello world this is chunk A extra text here",
                                                    "extra text here and some more content B"])
        self.assertEqual(result, expected)
        mock_fallback.assert_not_called()

    def test_falls_back_when_no_chunks(self):
        cm = _make_db_cm([])
        with mock.patch.object(app_module, "db_session", return_value=cm), \
             mock.patch.object(app_module, "_rag_paper_text", return_value="fallback text") as mock_fallback:
            result = app_module._lib_full_text("paper.pdf")
        mock_fallback.assert_called_once_with("paper.pdf")
        self.assertEqual(result, "fallback text")

    def test_fallback_failure_returns_empty_string(self):
        cm = _make_db_cm([])
        with mock.patch.object(app_module, "db_session", return_value=cm), \
             mock.patch.object(app_module, "_rag_paper_text", side_effect=RuntimeError("OCR failed")), \
             mock.patch.object(app_module.app.logger, "exception"):
            result = app_module._lib_full_text("paper.pdf")
        self.assertEqual(result, "")

    def test_none_content_treated_as_empty_string(self):
        chunk = types.SimpleNamespace(content=None)
        cm = _make_db_cm([chunk])
        with mock.patch.object(app_module, "db_session", return_value=cm), \
             mock.patch.object(app_module, "_rag_paper_text") as mock_fallback:
            result = app_module._lib_full_text("paper.pdf")
        # Single chunk with None content — reassemble([""])
        self.assertEqual(result, app_module.rag_index.reassemble([""]))
        mock_fallback.assert_not_called()


class TestLibSearch(unittest.TestCase):

    def _make_hits(self):
        return [
            {"filename": "a.pdf", "title": "Paper A", "author_name": "Smith", "content": "x" * 500, "score": 0.9},
            {"filename": "a.pdf", "title": "Paper A", "author_name": "Smith", "content": "y" * 100, "score": 0.8},
            {"filename": "b.pdf", "title": "",        "author_name": "Jones", "content": "z" * 50,  "score": 0.7},
        ]

    def test_maps_and_dedupes(self):
        hits = self._make_hits()
        with mock.patch.object(app_module.rag_index, "retrieve", return_value=hits), \
             mock.patch.object(app_module, "url_for", return_value="/preview/a.pdf"):
            results = app_module._lib_search("test query")
        # Two distinct filenames after dedup
        self.assertEqual(len(results), 2)
        filenames = [r["filename"] for r in results]
        self.assertIn("a.pdf", filenames)
        self.assertIn("b.pdf", filenames)

    def test_result_has_required_keys(self):
        hits = self._make_hits()
        with mock.patch.object(app_module.rag_index, "retrieve", return_value=hits), \
             mock.patch.object(app_module, "url_for", return_value="/preview/x.pdf"):
            results = app_module._lib_search("q")
        for r in results:
            self.assertSetEqual(set(r.keys()), {"filename", "title", "authors", "url", "snippet"})

    def test_snippet_max_400_chars(self):
        hits = self._make_hits()
        with mock.patch.object(app_module.rag_index, "retrieve", return_value=hits), \
             mock.patch.object(app_module, "url_for", return_value="/p"):
            results = app_module._lib_search("q")
        for r in results:
            self.assertLessEqual(len(r["snippet"]), 400)

    def test_title_falls_back_to_filename_when_missing(self):
        hits = [{"filename": "b.pdf", "title": "", "author_name": "Jones", "content": "z", "score": 0.7}]
        with mock.patch.object(app_module.rag_index, "retrieve", return_value=hits), \
             mock.patch.object(app_module, "url_for", return_value="/p"):
            results = app_module._lib_search("q")
        self.assertEqual(results[0]["title"], "b.pdf")

    def test_retrieve_error_returns_empty_list(self):
        with mock.patch.object(app_module.rag_index, "retrieve", side_effect=RuntimeError("boom")), \
             mock.patch.object(app_module.app.logger, "exception"):
            results = app_module._lib_search("q")
        self.assertEqual(results, [])


class TestLibPaperMeta(unittest.TestCase):

    def test_returns_title_and_authors(self):
        with mock.patch.object(app_module, "build_paper_record",
                               return_value={"title": "My Paper", "author_name": "Doe"}):
            result = app_module._lib_paper_meta("paper.pdf")
        self.assertEqual(result, {"title": "My Paper", "authors": "Doe"})

    def test_title_falls_back_to_filename_when_empty(self):
        with mock.patch.object(app_module, "build_paper_record",
                               return_value={"title": "", "author_name": ""}):
            result = app_module._lib_paper_meta("paper.pdf")
        self.assertEqual(result["title"], "paper.pdf")


class TestLibPaperUrl(unittest.TestCase):

    def test_returns_url_for_result(self):
        with mock.patch.object(app_module, "url_for", return_value="/preview/paper.pdf"):
            result = app_module._lib_paper_url("paper.pdf")
        self.assertEqual(result, "/preview/paper.pdf")


class TestBuildLibraryDeps(unittest.TestCase):

    def test_has_all_four_callable_attributes(self):
        deps = app_module._build_library_deps()
        for attr in ("search", "full_text", "paper_meta", "paper_url"):
            self.assertTrue(hasattr(deps, attr), f"missing attribute: {attr}")
            self.assertTrue(callable(getattr(deps, attr)), f"not callable: {attr}")

    def test_attributes_wired_to_helpers(self):
        deps = app_module._build_library_deps()
        self.assertIs(deps.search, app_module._lib_search)
        self.assertIs(deps.full_text, app_module._lib_full_text)
        self.assertIs(deps.paper_meta, app_module._lib_paper_meta)
        self.assertIs(deps.paper_url, app_module._lib_paper_url)


if __name__ == "__main__":
    unittest.main()
