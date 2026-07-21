"""Contract tests for _lib_full_text, _lib_search, _lib_paper_meta,
_lib_paper_url, and _build_library_deps — the DB-backed deps for library_tools.

No real DB is needed; collaborators are mocked at the module attribute level.
Read helpers intentionally run in Flask request contexts so they can exercise
the injected PaperLibrary boundary. The helpers live in services.ai (split
refactor), while public objects stay reachable as app_module.<name> through the
back-compat re-exports.
"""
import os
import types
import unittest
from unittest import mock

from flask import Flask

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module
import services.ai as ask_module
from services.publishing_contracts import NotFound


PAPER_A_ID = "00000000-0000-4000-8000-000000000801"
PAPER_B_ID = "00000000-0000-4000-8000-000000000802"
MISSING_ID = "00000000-0000-4000-8000-000000000803"
PAPER_C_ID = "00000000-0000-4000-8000-000000000804"
PAPER_D_ID = "00000000-0000-4000-8000-000000000805"


def _app_with_library(library):
    app = Flask(__name__)
    app.extensions["paper_library"] = library
    return app


def _app_with_live_document(
    *,
    paper_id=PAPER_A_ID,
    current_revision=2,
    filename="paper.pdf",
    title="Paper Title",
    author_name="Paper Author",
):
    record = types.SimpleNamespace(
        paper_id=paper_id,
        current_revision=current_revision,
        filename=filename,
        title=title,
        author_name=author_name,
    )
    library = mock.Mock()
    library.resolve_alias.return_value = record
    library.current_pdf.return_value = types.SimpleNamespace(paper=record)
    return _app_with_library(library), library


def _make_db_cm(rows):
    """Build a mock context manager whose db.query(...).filter(...).order_by(...).all() returns rows."""
    fake_db = mock.MagicMock()
    fake_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = rows
    cm = mock.MagicMock()
    cm.__enter__.return_value = fake_db
    cm.__exit__.return_value = False
    return cm


class TestLibFullText(unittest.TestCase):

    def test_unavailable_current_file_never_returns_stored_chunks(self):
        record = types.SimpleNamespace(
            paper_id=PAPER_A_ID,
            current_revision=2,
            filename="paper.pdf",
        )
        library = mock.Mock()
        library.resolve_alias.return_value = record
        library.current_pdf.side_effect = NotFound()
        app = _app_with_library(library)
        cm = _make_db_cm([
            types.SimpleNamespace(content="stale indexed secret"),
        ])

        with app.test_request_context(), mock.patch.object(
            ask_module,
            "db_session",
            return_value=cm,
        ):
            result = app_module._lib_full_text("paper.pdf")

        self.assertEqual(result, "")
        library.resolve_alias.assert_called_once_with("paper.pdf")
        library.current_pdf.assert_called_once_with(PAPER_A_ID)
        cm.__enter__.assert_not_called()

    def test_reassembles_stored_chunks_in_order(self):
        chunk_a = types.SimpleNamespace(content="hello world this is chunk A extra text here")
        chunk_b = types.SimpleNamespace(content="extra text here and some more content B")
        cm = _make_db_cm([chunk_a, chunk_b])
        app, _library = _app_with_live_document()
        with app.test_request_context(), \
             mock.patch.object(ask_module, "db_session", return_value=cm), \
             mock.patch.object(ask_module, "_rag_paper_text") as mock_fallback:
            result = app_module._lib_full_text("paper.pdf")
        expected = app_module.rag_index.reassemble(["hello world this is chunk A extra text here",
                                                    "extra text here and some more content B"])
        self.assertEqual(result, expected)
        mock_fallback.assert_not_called()

    def test_falls_back_when_no_chunks(self):
        cm = _make_db_cm([])
        app, _library = _app_with_live_document()
        with app.test_request_context(), \
             mock.patch.object(ask_module, "db_session", return_value=cm), \
             mock.patch.object(ask_module, "_rag_paper_text", return_value="fallback text") as mock_fallback:
            result = app_module._lib_full_text("paper.pdf")
        mock_fallback.assert_called_once_with("paper.pdf")
        self.assertEqual(result, "fallback text")

    def test_fallback_failure_returns_empty_string(self):
        cm = _make_db_cm([])
        app, _library = _app_with_live_document()
        with app.test_request_context(), \
             mock.patch.object(ask_module, "db_session", return_value=cm), \
             mock.patch.object(ask_module, "_rag_paper_text", side_effect=RuntimeError("OCR failed")), \
             mock.patch.object(ask_module.logger, "exception"):
            result = app_module._lib_full_text("paper.pdf")
        self.assertEqual(result, "")

    def test_chunk_query_orders_by_chunk_index(self):
        chunk = types.SimpleNamespace(content="only chunk")
        fake_db = mock.MagicMock()
        fake_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [chunk]
        cm = mock.MagicMock()
        cm.__enter__.return_value = fake_db
        cm.__exit__.return_value = False
        app, _library = _app_with_live_document()
        with app.test_request_context(), \
             mock.patch.object(ask_module, "db_session", return_value=cm), \
             mock.patch.object(ask_module, "_rag_paper_text"):
            app_module._lib_full_text("paper.pdf")
        filters = fake_db.query.return_value.filter.call_args.args
        self.assertEqual(filters[0].right.value, PAPER_A_ID)
        self.assertEqual(filters[1].right.value, 2)
        fake_db.query.return_value.filter.return_value.order_by.assert_called_once_with(
            app_module.PaperChunkModel.chunk_index
        )

    def test_db_error_returns_empty_string(self):
        cm = mock.MagicMock()
        cm.__enter__.side_effect = RuntimeError("DB connection failed")
        app, _library = _app_with_live_document()
        with app.test_request_context(), \
             mock.patch.object(ask_module, "db_session", return_value=cm), \
             mock.patch.object(ask_module.logger, "exception"):
            result = app_module._lib_full_text("paper.pdf")
        self.assertEqual(result, "")

    def test_none_content_treated_as_empty_string(self):
        chunk = types.SimpleNamespace(content=None)
        cm = _make_db_cm([chunk])
        app, _library = _app_with_live_document()
        with app.test_request_context(), \
             mock.patch.object(ask_module, "db_session", return_value=cm), \
             mock.patch.object(ask_module, "_rag_paper_text") as mock_fallback:
            result = app_module._lib_full_text("paper.pdf")
        # Single chunk with None content — reassemble([""])
        self.assertEqual(result, app_module.rag_index.reassemble([""]))
        mock_fallback.assert_not_called()


class TestLibSearch(unittest.TestCase):

    def _make_hits(self):
        return [
            {"paper_id": PAPER_A_ID, "revision_number": 2, "filename": "a.pdf", "title": "Paper A", "author_name": "Smith", "content": "x" * 500, "score": 0.9},
            {"paper_id": PAPER_A_ID, "revision_number": 2, "filename": "a.pdf", "title": "Paper A", "author_name": "Smith", "content": "y" * 100, "score": 0.8},
            {"paper_id": PAPER_B_ID, "revision_number": 4, "filename": "b.pdf", "title": "",        "author_name": "Jones", "content": "z" * 50,  "score": 0.7},
        ]

    @staticmethod
    def _visible_app():
        library = mock.Mock()
        library.list_visible.return_value = (
            types.SimpleNamespace(paper_id=PAPER_A_ID, current_revision=2),
            types.SimpleNamespace(paper_id=PAPER_B_ID, current_revision=4),
        )
        return _app_with_library(library)

    def test_maps_and_dedupes(self):
        hits = self._make_hits()
        with self._visible_app().test_request_context(), \
             mock.patch.object(ask_module.rag_index, "retrieve", return_value=hits), \
             mock.patch.object(ask_module, "url_for", return_value="/preview/a.pdf"):
            results = app_module._lib_search("test query")
        # Two distinct filenames after dedup
        self.assertEqual(len(results), 2)
        filenames = [r["filename"] for r in results]
        self.assertIn("a.pdf", filenames)
        self.assertIn("b.pdf", filenames)

    def test_result_has_required_keys(self):
        hits = self._make_hits()
        with self._visible_app().test_request_context(), \
             mock.patch.object(ask_module.rag_index, "retrieve", return_value=hits), \
             mock.patch.object(ask_module, "url_for", return_value="/preview/x.pdf"):
            results = app_module._lib_search("q")
        for r in results:
            self.assertSetEqual(set(r.keys()), {"filename", "title", "authors", "url", "snippet"})

    def test_snippet_max_400_chars(self):
        hits = self._make_hits()
        with self._visible_app().test_request_context(), \
             mock.patch.object(ask_module.rag_index, "retrieve", return_value=hits), \
             mock.patch.object(ask_module, "url_for", return_value="/p"):
            results = app_module._lib_search("q")
        for r in results:
            self.assertLessEqual(len(r["snippet"]), 400)

    def test_title_falls_back_to_filename_when_missing(self):
        hits = [{"paper_id": PAPER_B_ID, "revision_number": 4, "filename": "b.pdf", "title": "", "author_name": "Jones", "content": "z", "score": 0.7}]
        with self._visible_app().test_request_context(), \
             mock.patch.object(ask_module.rag_index, "retrieve", return_value=hits), \
             mock.patch.object(ask_module, "url_for", return_value="/p"):
            results = app_module._lib_search("q")
        self.assertEqual(results[0]["title"], "b.pdf")

    def test_retrieve_error_returns_empty_list(self):
        with mock.patch.object(ask_module.rag_index, "retrieve", side_effect=RuntimeError("boom")), \
             mock.patch.object(ask_module.logger, "exception"):
            results = app_module._lib_search("q")
        self.assertEqual(results, [])

    def test_drops_unavailable_and_unresolved_hits_while_preserving_rank(self):
        hits = [
            {"paper_id": MISSING_ID, "revision_number": 1, "filename": "missing.pdf", "title": "Missing"},
            {"paper_id": PAPER_B_ID, "revision_number": 4, "filename": "b.pdf", "title": "Paper B"},
            {"filename": "filename-only.pdf", "title": "Unresolved"},
            {"paper_id": PAPER_A_ID, "revision_number": 2, "filename": "a.pdf", "title": "Paper A"},
        ]
        library = mock.Mock()
        library.list_visible.return_value = (
            types.SimpleNamespace(paper_id=PAPER_A_ID, current_revision=2),
            types.SimpleNamespace(paper_id=PAPER_B_ID, current_revision=4),
        )
        app = _app_with_library(library)

        with app.test_request_context(), mock.patch.object(
            ask_module.rag_index,
            "retrieve",
            return_value=hits,
        ), mock.patch.object(
            ask_module,
            "url_for",
            side_effect=lambda _endpoint, paper_id: f"/paper/{paper_id}",
        ):
            results = app_module._lib_search("q")

        self.assertEqual(
            [result["filename"] for result in results],
            ["b.pdf", "a.pdf"],
        )
        self.assertEqual(
            [result["url"] for result in results],
            [f"/paper/{PAPER_B_ID}", f"/paper/{PAPER_A_ID}"],
        )
        library.list_visible.assert_called_once_with()

    def test_drops_stale_and_non_integer_revision_hits(self):
        hits = [
            {
                "filename": "notes.txt",
                "is_attachment": True,
            },
            {
                "paper_id": PAPER_A_ID,
                "revision_number": 1,
                "filename": "truthy-false-attachment.pdf",
                "is_attachment": "false",
            },
            {
                "paper_id": PAPER_A_ID,
                "revision_number": 1,
                "filename": "stale.pdf",
            },
            {
                "paper_id": PAPER_B_ID,
                "revision_number": "2",
                "filename": "string-revision.pdf",
            },
            {
                "paper_id": PAPER_C_ID,
                "filename": "id-only.pdf",
            },
            {
                "paper_id": PAPER_D_ID,
                "revision_number": 5,
                "filename": "current.pdf",
            },
        ]
        library = mock.Mock()
        library.list_visible.return_value = (
            types.SimpleNamespace(paper_id=PAPER_A_ID, current_revision=2),
            types.SimpleNamespace(paper_id=PAPER_B_ID, current_revision=2),
            types.SimpleNamespace(paper_id=PAPER_C_ID, current_revision=4),
            types.SimpleNamespace(paper_id=PAPER_D_ID, current_revision=5),
        )

        results = ask_module._filter_available_grounding_hits(hits, library)

        self.assertEqual(
            [result["filename"] for result in results],
            ["notes.txt", "current.pdf"],
        )

    def test_stale_revision_text_never_merges_into_a_current_hit(self):
        hits = [
            {
                "paper_id": PAPER_A_ID,
                "revision_number": 2,
                "filename": "paper.pdf",
                "title": "Current",
                "content": "CURRENT TEXT",
            },
            {
                "paper_id": PAPER_A_ID,
                "revision_number": 1,
                "filename": "paper.pdf",
                "title": "Stale",
                "content": "STALE SECRET",
            },
        ]
        library = mock.Mock()
        library.list_visible.return_value = (
            types.SimpleNamespace(
                paper_id=PAPER_A_ID,
                current_revision=2,
            ),
        )
        app = _app_with_library(library)

        with app.test_request_context(), mock.patch.object(
            ask_module.rag_index,
            "retrieve",
            return_value=hits,
        ), mock.patch.object(
            ask_module,
            "url_for",
            return_value=f"/paper/{PAPER_A_ID}",
        ):
            results = app_module._lib_search("q")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["snippet"], "CURRENT TEXT")


class TestLibPaperMeta(unittest.TestCase):

    def test_unavailable_current_file_never_discloses_stored_metadata(self):
        record = types.SimpleNamespace(paper_id=PAPER_A_ID)
        library = mock.Mock()
        library.resolve_alias.return_value = record
        library.current_pdf.side_effect = NotFound()
        app = _app_with_library(library)

        with app.test_request_context(), mock.patch.object(
            ask_module,
            "_visible_paper_by_filename",
            return_value={
                "title": "Unavailable Secret Title",
                "author_name": "Secret Author",
            },
        ):
            result = app_module._lib_paper_meta("paper.pdf")

        self.assertEqual(result, {})
        library.resolve_alias.assert_called_once_with("paper.pdf")
        library.current_pdf.assert_called_once_with(PAPER_A_ID)

    def test_returns_title_and_authors(self):
        app, _library = _app_with_live_document(
            title="My Paper",
            author_name="Doe",
        )
        with app.test_request_context():
            result = app_module._lib_paper_meta("paper.pdf")
        self.assertEqual(result, {"title": "My Paper", "authors": "Doe"})

    def test_title_falls_back_to_filename_when_empty(self):
        app, _library = _app_with_live_document(
            title="",
            author_name="",
        )
        with app.test_request_context():
            result = app_module._lib_paper_meta("paper.pdf")
        self.assertEqual(result["title"], "paper.pdf")


class TestLibPaperUrl(unittest.TestCase):

    def test_resolves_live_alias_and_current_file_to_uuid_url(self):
        record = types.SimpleNamespace(paper_id=PAPER_A_ID)
        library = mock.Mock()
        library.resolve_alias.return_value = record
        library.current_pdf.return_value = types.SimpleNamespace(paper=record)
        app = _app_with_library(library)

        with app.test_request_context(), mock.patch.object(
            ask_module,
            "url_for",
            return_value=f"/paper/{PAPER_A_ID}",
        ) as build_url:
            result = app_module._lib_paper_url("paper.pdf")

        self.assertEqual(result, f"/paper/{PAPER_A_ID}")
        library.resolve_alias.assert_called_once_with("paper.pdf")
        library.current_pdf.assert_called_once_with(PAPER_A_ID)
        build_url.assert_called_once_with("preview_paper", paper_id=PAPER_A_ID)

    def test_returns_none_when_current_file_is_unavailable(self):
        record = types.SimpleNamespace(paper_id=PAPER_A_ID)
        library = mock.Mock()
        library.resolve_alias.return_value = record
        library.current_pdf.side_effect = NotFound()
        app = _app_with_library(library)

        with app.test_request_context(), mock.patch.object(
            ask_module,
            "url_for",
        ) as build_url:
            result = app_module._lib_paper_url("paper.pdf")

        self.assertIsNone(result)
        library.resolve_alias.assert_called_once_with("paper.pdf")
        library.current_pdf.assert_called_once_with(PAPER_A_ID)
        build_url.assert_not_called()


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
