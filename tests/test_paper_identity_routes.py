"""Canonical UUID Paper routes and filename-alias compatibility."""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from uuid import UUID

from flask import Flask
from pypdf import PdfReader, PdfWriter
from werkzeug.routing import PathConverter

from routes.papers import _LegacyPaperPathConverter, register_routes
from routes.ai import register_routes as register_ai_routes
from routes.submissions import register_routes as register_submission_routes
from services.paper_library import PaperPdf, PaperRecord
from services.publishing_contracts import NotFound


PAPER_ID = "00000000-0000-4000-8000-000000000701"


class RecordingPaperLibrary:
    def __init__(self, document):
        self.document = document
        self.documents = {document.paper.paper_id: document}
        self.current_error = None
        self.current_calls = []
        self.alias_error = None
        self.alias_calls = []

    def current_pdf(self, paper_id):
        self.current_calls.append(paper_id)
        if self.current_error is not None:
            raise self.current_error
        try:
            return self.documents[paper_id]
        except KeyError as exc:
            raise NotFound() from exc

    def resolve_alias(self, filename):
        self.alias_calls.append(filename)
        if self.alias_error is not None:
            raise self.alias_error
        return self.document.paper

    def list_visible(self):
        return tuple(document.paper for document in self.documents.values())


class PaperIdentityRouteMapTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "paper-identity-route-test"
        register_routes(self.app)
        self.adapter = self.app.url_map.bind("example.test")

    def test_uuid_rules_keep_public_endpoint_names(self):
        cases = (
            (f"/paper/{PAPER_ID}", "preview_paper", {"paper_id": UUID(PAPER_ID)}),
            (f"/paper/{PAPER_ID}/pdf", "paper_file", {"paper_id": UUID(PAPER_ID)}),
            (f"/paper/{PAPER_ID}/preview.pdf", "paper_preview", {"paper_id": UUID(PAPER_ID)}),
            (f"/paper/{PAPER_ID}/info", "paper_info", {"paper_id": UUID(PAPER_ID)}),
            (f"/dashboard/paper/{PAPER_ID}/modify", "paper_modify", {"paper_id": UUID(PAPER_ID)}),
            (f"/dashboard/paper/{PAPER_ID}/revisions/1/pdf", "paper_revision_file", {"paper_id": UUID(PAPER_ID), "revision": 1}),
        )

        for path, expected_endpoint, expected_values in cases:
            with self.subTest(path=path):
                endpoint, values = self.adapter.match(path, method="GET")
                self.assertEqual(endpoint, expected_endpoint)
                self.assertEqual(values, expected_values)

    def test_filename_rules_are_path_alias_fallbacks_only(self):
        filename = "archive/nested paper.pdf"
        cases = (
            (f"/preview/{filename}", "preview_paper_legacy"),
            (f"/papers/preview/{filename}", "paper_preview_legacy"),
            (f"/papers/raw/{filename}", "paper_file_legacy"),
            (f"/papers/{filename}", "download_legacy"),
            (f"/paper/{filename}/info", "paper_info_legacy"),
            (f"/dashboard/paper/{filename}/modify", "paper_modify_legacy_dashboard"),
        )

        for path, expected_endpoint in cases:
            with self.subTest(path=path):
                endpoint, values = self.adapter.match(path, method="GET")
                self.assertEqual(endpoint, expected_endpoint)
                self.assertEqual(values, {"filename": filename})

    def test_uuid_info_uses_canonical_rule_while_filename_uses_fallback(self):
        canonical_endpoint, canonical_values = self.adapter.match(
            f"/paper/{PAPER_ID}/info",
            method="GET",
        )
        legacy_endpoint, legacy_values = self.adapter.match(
            "/paper/paper.pdf/info",
            method="GET",
        )

        self.assertEqual(canonical_endpoint, "paper_info")
        self.assertEqual(canonical_values, {"paper_id": UUID(PAPER_ID)})
        self.assertEqual(legacy_endpoint, "paper_info_legacy")
        self.assertEqual(legacy_values, {"filename": "paper.pdf"})

    def test_uuid_excluding_path_converter_is_scoped_to_legacy_info(self):
        rules = {rule.endpoint: rule for rule in self.app.url_map.iter_rules()}

        for endpoint in (
            "paper_info_legacy",
            "paper_modify_legacy_dashboard",
            "paper_delete_legacy",
            "paper_modify_legacy",
        ):
            with self.subTest(endpoint=endpoint):
                self.assertIsInstance(
                    rules[endpoint]._converters["filename"],
                    _LegacyPaperPathConverter,
                )
        self.assertIs(
            type(rules["paper_preview_legacy"]._converters["filename"]),
            PathConverter,
        )
        self.assertIs(self.app.url_map.converters["path"], PathConverter)

    def test_uuid_management_paths_never_dispatch_to_filename_handlers(self):
        calls = []

        def view(endpoint):
            def dispatch(**_values):
                calls.append(endpoint)
                return "legacy writer", 204

            return dispatch

        for endpoint in (
            "paper_modify",
            "paper_delete",
            "paper_modify_legacy_dashboard",
            "paper_delete_legacy",
            "paper_modify_legacy",
        ):
            self.app.view_functions[endpoint] = view(endpoint)
        client = self.app.test_client()

        uuid_paths = (
            ("GET", f"/dashboard/paper/{PAPER_ID}/modify", "paper_modify"),
            ("POST", f"/dashboard/paper/{PAPER_ID}/modify", "paper_modify"),
            ("POST", f"/dashboard/paper/{PAPER_ID}/delete", "paper_delete"),
        )
        for method, path, endpoint in uuid_paths:
            with self.subTest(kind="uuid", method=method, path=path):
                response = client.open(path, method=method)
                self.assertEqual(response.status_code, 204)
                self.assertEqual(calls[-1], endpoint)

        response = client.get(f"/paper/{PAPER_ID}/modify")
        self.assertEqual(response.status_code, 404)

        filename_paths = (
            ("GET", "/dashboard/paper/paper.pdf/modify", "paper_modify_legacy_dashboard"),
            ("POST", "/dashboard/paper/paper.pdf/modify", "paper_modify_legacy_dashboard"),
            ("POST", "/dashboard/paper/paper.pdf/delete", "paper_delete_legacy"),
            ("GET", "/paper/paper.pdf/modify", "paper_modify_legacy"),
        )
        for method, path, endpoint in filename_paths:
            with self.subTest(kind="filename", method=method, path=path):
                response = client.open(path, method=method)
                self.assertEqual(response.status_code, 204)
                self.assertEqual(calls[-1], endpoint)


class CanonicalPaperPdfRouteTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        path = Path(self.tmp.name) / "current.pdf"
        stream = io.BytesIO()
        writer = PdfWriter()
        for _ in range(3):
            writer.add_blank_page(width=72, height=72)
        writer.write(stream)
        pdf_bytes = stream.getvalue()
        path.write_bytes(pdf_bytes)

        record = PaperRecord(
            paper_id=PAPER_ID,
            current_revision=2,
            row_version=3,
            filename="canonical paper.pdf",
            title="Canonical Paper",
            journal="Journal",
            category="science",
            language="en",
            keywords="canonical",
            abstract="Abstract",
            author_name="Ada Author",
            author_email="ada@example.test",
            author_school="Example School",
            published_at="2026-07-21",
            ib_ee_data="",
            is_ib_sample="",
            cp_data="",
            is_anonymous="",
            ia_data="",
        )
        document = PaperPdf(
            paper=record,
            revision=2,
            path=path,
            sha256="a" * 64,
            size_bytes=len(pdf_bytes),
        )
        self.library = RecordingPaperLibrary(document)
        self.app = Flask(__name__)
        self.app.secret_key = "paper-pdf-route-test"

        @self.app.get("/login")
        def login():
            return "login"

        register_routes(self.app)
        self.app.extensions["paper_library"] = self.library
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def test_closed_paper_file_authenticates_before_current_lookup(self):
        with mock.patch("routes.papers.OPEN_ACCESS", False), mock.patch(
            "routes.papers.require_login",
            return_value=None,
        ):
            response = self.client.get(f"/paper/{PAPER_ID}/pdf")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/login")
        self.assertEqual(self.library.current_calls, [])

    def test_paper_file_serves_current_pdf_inline_or_as_attachment(self):
        with mock.patch("routes.papers.OPEN_ACCESS", False), mock.patch(
            "routes.papers.require_login",
            return_value={"username": "reader", "role": "1"},
        ):
            inline = self.client.get(f"/paper/{PAPER_ID}/pdf")
            attachment = self.client.get(
                f"/paper/{PAPER_ID}/pdf?download=1"
            )

        self.assertEqual(inline.status_code, 200)
        self.assertEqual(attachment.status_code, 200)
        self.assertEqual(inline.mimetype, "application/pdf")
        self.assertIn("inline", inline.headers["Content-Disposition"])
        self.assertIn(
            "canonical paper.pdf",
            inline.headers["Content-Disposition"],
        )
        self.assertIn("attachment", attachment.headers["Content-Disposition"])
        self.assertIn(
            "canonical paper.pdf",
            attachment.headers["Content-Disposition"],
        )
        self.assertEqual(self.library.current_calls, [PAPER_ID, PAPER_ID])
        inline.close()
        attachment.close()

    def test_paper_file_returns_404_when_current_pdf_is_unavailable(self):
        self.library.current_error = NotFound()

        with mock.patch("routes.papers.OPEN_ACCESS", True):
            response = self.client.get(f"/paper/{PAPER_ID}/pdf")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(self.library.current_calls, [PAPER_ID])

    def test_guest_preview_has_two_pages_from_the_current_revision(self):
        with mock.patch("routes.papers.OPEN_ACCESS", False), mock.patch(
            "routes.papers.get_active_user",
            return_value=None,
        ):
            response = self.client.get(f"/paper/{PAPER_ID}/preview.pdf")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(PdfReader(io.BytesIO(response.data)).pages), 2)
        self.assertEqual(self.library.current_calls, [PAPER_ID])
        response.close()

    def test_authenticated_or_open_access_preview_has_the_full_current_pdf(self):
        cases = (
            (False, {"username": "reader"}),
            (True, None),
        )
        for open_access, active_user in cases:
            with self.subTest(
                open_access=open_access,
                active_user=active_user,
            ), mock.patch(
                "routes.papers.OPEN_ACCESS",
                open_access,
            ), mock.patch(
                "routes.papers.get_active_user",
                return_value=active_user,
            ):
                response = self.client.get(
                    f"/paper/{PAPER_ID}/preview.pdf"
                )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(len(PdfReader(io.BytesIO(response.data)).pages), 3)
            response.close()

        self.assertEqual(self.library.current_calls, [PAPER_ID, PAPER_ID])

    def test_preview_returns_404_when_current_pdf_is_unavailable(self):
        self.library.current_error = NotFound()

        with mock.patch("routes.papers.OPEN_ACCESS", False), mock.patch(
            "routes.papers.get_active_user",
            return_value=None,
        ):
            response = self.client.get(f"/paper/{PAPER_ID}/preview.pdf")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(self.library.current_calls, [PAPER_ID])

    def test_paper_info_authenticates_curator_before_current_lookup(self):
        with mock.patch(
            "routes.papers.require_login",
            return_value=None,
        ) as require:
            response = self.client.get(f"/paper/{PAPER_ID}/info")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json(), {"error": "Unauthorized"})
        require.assert_called_once_with(level=3)
        self.assertEqual(self.library.current_calls, [])

    def test_paper_info_projects_the_current_detached_record(self):
        with mock.patch(
            "routes.papers.require_login",
            return_value={"username": "curator", "role": "3"},
        ):
            response = self.client.get(f"/paper/{PAPER_ID}/info")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["paper_id"], PAPER_ID)
        self.assertEqual(payload["current_revision"], 2)
        self.assertEqual(payload["filename"], "canonical paper.pdf")
        self.assertEqual(payload["title"], "Canonical Paper")
        self.assertEqual(payload["journal"], "Journal")
        self.assertEqual(payload["author_email"], "ada@example.test")
        self.assertEqual(payload["pdf_url"], f"/paper/{PAPER_ID}/pdf")
        self.assertNotIn("row_version", payload)
        self.assertEqual(self.library.current_calls, [PAPER_ID])

    def test_paper_info_returns_404_when_current_pdf_is_unavailable(self):
        self.library.current_error = NotFound()

        with mock.patch(
            "routes.papers.require_login",
            return_value={"username": "curator", "role": "3"},
        ):
            response = self.client.get(f"/paper/{PAPER_ID}/info")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(self.library.current_calls, [PAPER_ID])

    def test_preview_page_uses_current_uuid_record_and_search_context(self):
        rendered = {}

        def render(template_name, **context):
            rendered["template"] = template_name
            rendered.update(context)
            return "preview page"

        with mock.patch("routes.papers.OPEN_ACCESS", False), mock.patch(
            "routes.papers.get_active_user",
            return_value=None,
        ), mock.patch(
            "routes.papers.llm_client.llm_enabled",
            return_value=False,
        ), mock.patch(
            "routes.papers.render_template",
            side_effect=render,
        ), mock.patch(
            "routes.papers.get_journal_id_map",
            return_value={},
        ), mock.patch(
            "routes.papers.get_journal_slug_map",
            return_value={},
        ):
            response = self.client.get(
                f"/paper/{PAPER_ID}?q=quantum&page=3"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(rendered["template"], "preview.html")
        self.assertEqual(rendered["paper"]["paper_id"], PAPER_ID)
        self.assertEqual(rendered["paper"]["current_revision"], 2)
        self.assertEqual(rendered["paper"]["filename"], "canonical paper.pdf")
        self.assertNotIn("row_version", rendered["paper"])
        self.assertEqual(
            rendered["pdf_url"],
            f"/paper/{PAPER_ID}/preview.pdf",
        )
        self.assertEqual(rendered["source_query"], "quantum")
        self.assertEqual(rendered["source_page"], "3")
        self.assertTrue(rendered["is_guest"])
        self.assertEqual(self.library.current_calls, [PAPER_ID])

    def test_preview_page_uses_uuid_related_api_and_uuid_records(self):
        related_id = "00000000-0000-4000-8000-000000000702"
        related_record = replace(
            self.library.document.paper,
            paper_id=related_id,
            filename="related.pdf",
            title="Related Paper",
        )
        self.library.documents[related_id] = replace(
            self.library.document,
            paper=related_record,
        )
        rendered = {}

        def render(_template_name, **context):
            rendered.update(context)
            return "preview page"

        with mock.patch("routes.papers.OPEN_ACCESS", True), mock.patch(
            "routes.papers.get_active_user",
            return_value=None,
        ), mock.patch(
            "routes.papers.llm_client.llm_enabled",
            return_value=True,
        ), mock.patch(
            "routes.papers.rag_index.related_papers",
            return_value=[(related_id, 0.9)],
        ) as related, mock.patch(
            "routes.papers.render_template",
            side_effect=render,
        ), mock.patch(
            "routes.papers.get_journal_id_map",
            return_value={},
        ), mock.patch(
            "routes.papers.get_journal_slug_map",
            return_value={},
        ):
            response = self.client.get(f"/paper/{PAPER_ID}")

        self.assertEqual(response.status_code, 200)
        related.assert_called_once_with(PAPER_ID, k=5)
        self.assertEqual(
            [paper["paper_id"] for paper in rendered["related_papers"]],
            [related_id],
        )
        self.assertEqual(
            rendered["pdf_url"],
            f"/paper/{PAPER_ID}/pdf",
        )
        self.assertEqual(self.library.current_calls, [PAPER_ID, related_id])

    def test_preview_related_failure_log_never_includes_provider_detail(self):
        with mock.patch("routes.papers.OPEN_ACCESS", True), mock.patch(
            "routes.papers.get_active_user",
            return_value=None,
        ), mock.patch(
            "routes.papers.llm_client.llm_enabled",
            return_value=True,
        ), mock.patch(
            "routes.papers.rag_index.related_papers",
            side_effect=RuntimeError("provider token=do-not-log"),
        ), mock.patch.object(
            self.app.logger,
            "warning",
        ) as warning, mock.patch(
            "routes.papers.render_template",
            return_value="preview page",
        ), mock.patch(
            "routes.papers.get_journal_id_map",
            return_value={},
        ), mock.patch(
            "routes.papers.get_journal_slug_map",
            return_value={},
        ):
            response = self.client.get(f"/paper/{PAPER_ID}")

        self.assertEqual(response.status_code, 200)
        warning.assert_called_once_with("related-paper ranking failed")

    def test_preview_page_returns_404_when_current_pdf_is_unavailable(self):
        self.library.current_error = NotFound()

        with mock.patch("routes.papers.render_template") as render:
            response = self.client.get(f"/paper/{PAPER_ID}")

        self.assertEqual(response.status_code, 404)
        render.assert_not_called()
        self.assertEqual(self.library.current_calls, [PAPER_ID])

    def test_live_filename_aliases_redirect_permanently_to_uuid_urls(self):
        filename = "archive/Old Paper.pdf"
        cases = (
            (
                f"/preview/{filename}?q=quantum&page=2",
                f"/paper/{PAPER_ID}?q=quantum&page=2",
            ),
            (
                f"/papers/preview/{filename}",
                f"/paper/{PAPER_ID}/preview.pdf",
            ),
            (
                f"/papers/raw/{filename}",
                f"/paper/{PAPER_ID}/pdf",
            ),
            (
                f"/papers/{filename}",
                f"/paper/{PAPER_ID}/pdf?download=1",
            ),
            (
                f"/paper/{filename}/info",
                f"/paper/{PAPER_ID}/info",
            ),
        )

        with mock.patch("routes.papers.OPEN_ACCESS", True), mock.patch(
            "routes.papers.require_login",
            return_value={"username": "curator", "role": "3"},
        ) as require:
            for path, expected_location in cases:
                with self.subTest(path=path):
                    self.library.alias_calls.clear()
                    self.library.current_calls.clear()

                    response = self.client.get(path)

                    self.assertEqual(response.status_code, 301)
                    self.assertEqual(
                        response.headers["Location"],
                        expected_location,
                    )
                    self.assertEqual(self.library.alias_calls, [filename])
                    self.assertEqual(self.library.current_calls, [PAPER_ID])

        require.assert_called_once_with(level=3)

    def test_legacy_preview_without_search_context_has_no_empty_query(self):
        response = self.client.get("/preview/old.pdf")

        self.assertEqual(response.status_code, 301)
        self.assertEqual(response.headers["Location"], f"/paper/{PAPER_ID}")

    def test_authenticated_reader_can_resolve_closed_file_aliases(self):
        with mock.patch("routes.papers.OPEN_ACCESS", False), mock.patch(
            "routes.papers.require_login",
            return_value={"username": "reader", "role": "1"},
        ) as require:
            raw = self.client.get("/papers/raw/old.pdf")
            download = self.client.get("/papers/old.pdf")

        self.assertEqual(raw.status_code, 301)
        self.assertEqual(raw.headers["Location"], f"/paper/{PAPER_ID}/pdf")
        self.assertEqual(download.status_code, 301)
        self.assertEqual(
            download.headers["Location"],
            f"/paper/{PAPER_ID}/pdf?download=1",
        )
        self.assertEqual(require.call_args_list, [mock.call(), mock.call()])

    def test_missing_or_unavailable_aliases_return_404_directly(self):
        paths = (
            "/preview/missing.pdf",
            "/papers/preview/missing.pdf",
            "/papers/raw/missing.pdf",
            "/papers/missing.pdf",
            "/paper/missing.pdf/info",
        )
        with mock.patch("routes.papers.OPEN_ACCESS", True), mock.patch(
            "routes.papers.require_login",
            return_value={"username": "curator", "role": "3"},
        ):
            self.library.alias_error = NotFound()
            for path in paths:
                with self.subTest(kind="missing-alias", path=path):
                    response = self.client.get(path)
                    self.assertEqual(response.status_code, 404)
                    self.assertNotIn("Location", response.headers)

            self.library.alias_error = None
            self.library.current_error = NotFound()
            for path in paths:
                with self.subTest(kind="missing-current", path=path):
                    response = self.client.get(path)
                    self.assertEqual(response.status_code, 404)
                    self.assertNotIn("Location", response.headers)

    def test_protected_legacy_routes_authenticate_before_alias_lookup(self):
        cases = (
            (f"/paper/secret.pdf/info", 401),
            (f"/papers/raw/secret.pdf", 302),
            (f"/papers/secret.pdf", 302),
        )

        with mock.patch("routes.papers.OPEN_ACCESS", False), mock.patch(
            "routes.papers.require_login",
            return_value=None,
        ):
            for path, expected_status in cases:
                with self.subTest(path=path):
                    self.library.alias_calls.clear()

                    response = self.client.get(path)

                    self.assertEqual(response.status_code, expected_status)
                    self.assertEqual(self.library.alias_calls, [])

    def test_search_query_intersects_ranked_results_with_visible_library(self):
        visible = asdict(self.library.document.paper)
        visible.pop("row_version")
        unavailable = {
            **visible,
            "paper_id": "00000000-0000-4000-8000-000000000703",
            "filename": "missing-current.pdf",
            "title": "Missing Current",
        }
        rendered = {}

        def render(_template_name, **context):
            rendered.update(context)
            return "search page"

        with mock.patch(
            "routes.papers._hybrid_search_records",
            return_value=[unavailable, visible],
        ), mock.patch(
            "routes.papers.render_template",
            side_effect=render,
        ), mock.patch(
            "routes.papers.get_active_user",
            return_value=None,
        ), mock.patch(
            "routes.papers.consume_rate_limit",
            return_value=SimpleNamespace(allowed=True, retry_after=0, count=1),
        ), mock.patch(
            "routes.papers.load_journals",
            return_value=[],
        ), mock.patch(
            "routes.papers.load_paper_categories",
            return_value=[],
        ), mock.patch(
            "routes.papers.get_journal_id_map",
            return_value={},
        ), mock.patch(
            "routes.papers.get_journal_names",
            return_value=[],
        ), mock.patch(
            "routes.papers._get_ee_subjects_list",
            return_value=[],
        ), mock.patch(
            "routes.papers._get_ia_subjects_list",
            return_value=[],
        ):
            response = self.client.get("/search?q=paper")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [record["paper_id"] for record in rendered["records"]],
            [PAPER_ID],
        )
        self.assertEqual(
            [record["revision_number"] for record in rendered["records"]],
            [2],
        )
        self.assertEqual(rendered["total_matches"], 1)


class AiPaperVisibilityRouteTest(unittest.TestCase):
    def setUp(self):
        record = PaperRecord(
            paper_id=PAPER_ID,
            current_revision=2,
            row_version=3,
            filename="canonical paper.pdf",
            title="Canonical Paper",
            journal="Journal",
            category="science",
            language="en",
            keywords="canonical",
            abstract="Abstract",
            author_name="Ada Author",
            author_email="ada@example.test",
            author_school="Example School",
            published_at="2026-07-21",
            ib_ee_data="",
            is_ib_sample="",
            cp_data="",
            is_anonymous="",
            ia_data="",
        )
        self.library = RecordingPaperLibrary(SimpleNamespace(paper=record))
        self.app = Flask(__name__)
        self.app.secret_key = "ai-paper-visibility-test"
        register_ai_routes(self.app)
        self.app.extensions["paper_library"] = self.library
        self.client = self.app.test_client()

    def test_ai_paper_query_intersects_results_with_visible_library(self):
        visible = asdict(self.library.document.paper)
        visible.pop("row_version")
        unavailable = {
            **visible,
            "paper_id": "00000000-0000-4000-8000-000000000704",
            "filename": "unavailable.pdf",
            "title": "Unavailable",
        }

        with mock.patch("routes.ai.OPEN_ACCESS", True), mock.patch(
            "routes.ai.search_papers",
            return_value=[unavailable, visible],
        ):
            response = self.client.get("/api/ai/papers?q=paper")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json()["papers"],
            [
                {
                    "paper_id": PAPER_ID,
                    "revision_number": 2,
                    "filename": "canonical paper.pdf",
                    "title": "Canonical Paper",
                    "authors": "Ada Author",
                    "category": "science",
                    "abstract": "Abstract",
                }
            ],
        )


class AskCitationAvailabilityRouteTest(unittest.TestCase):
    def setUp(self):
        record = PaperRecord(
            paper_id=PAPER_ID,
            current_revision=2,
            row_version=3,
            filename="available.pdf",
            title="Available",
            journal="",
            category="",
            language="en",
            keywords="",
            abstract="",
            author_name="Ada Author",
            author_email="",
            author_school="",
            published_at="2026-07-21",
            ib_ee_data="",
            is_ib_sample="",
            cp_data="",
            is_anonymous="",
            ia_data="",
        )
        self.library = RecordingPaperLibrary(SimpleNamespace(paper=record))
        self.app = Flask(__name__)
        self.app.secret_key = "ask-citation-availability-test"

        @self.app.get("/paper/<uuid:paper_id>", endpoint="preview_paper")
        def preview_paper(paper_id):
            return str(paper_id)

        register_ai_routes(self.app)
        self.app.extensions["paper_library"] = self.library
        self.client = self.app.test_client()

    @staticmethod
    def _client_answering_with_two_citations():
        delta = SimpleNamespace(
            content="Attachment evidence [1] and available paper [2].",
            tool_calls=None,
        )
        chunk = SimpleNamespace(
            choices=[SimpleNamespace(delta=delta)],
        )
        completions = SimpleNamespace(
            create=lambda *args, **kwargs: iter([chunk]),
        )
        return SimpleNamespace(
            chat=SimpleNamespace(completions=completions),
        )

    def test_ask_drops_missing_current_and_filename_only_library_hits(self):
        attachment = {
            "filename": "notes.txt",
            "title": "Notes",
            "content": "Attachment content",
            "is_attachment": True,
        }
        unavailable = {
            "paper_id": "00000000-0000-4000-8000-000000000702",
            "revision_number": 1,
            "filename": "missing.pdf",
            "title": "Missing",
            "content": "Stale indexed content",
        }
        filename_only = {
            "filename": "legacy-only.pdf",
            "title": "Unresolved",
            "content": "Old indexed content",
        }
        available = {
            "paper_id": PAPER_ID,
            "revision_number": 2,
            "filename": "available.pdf",
            "title": "Available",
            "author_name": "Ada Author",
            "content": "Current content",
        }
        stale_same_paper = {
            **available,
            "revision_number": 1,
            "content": "STALE SECRET",
        }
        grounded_hits = []

        def build_prompt(_question, hits, _locale, _web_results):
            grounded_hits.extend(hits)
            return "grounded system prompt"

        with mock.patch("routes.ai.OPEN_ACCESS", True), mock.patch(
            "routes.ai.llm_client.llm_enabled",
            return_value=True,
        ), mock.patch(
            "routes.ai._ask_rate_ok",
            return_value=True,
        ), mock.patch(
            "routes.ai._attachment_grounding",
            return_value=[attachment],
        ), mock.patch(
            "routes.ai.rag_index.retrieve",
            return_value=[
                unavailable,
                filename_only,
                available,
                stale_same_paper,
            ],
        ), mock.patch(
            "services.ask_turn._build_ask_prompt",
            side_effect=build_prompt,
        ), mock.patch(
            "routes.ai.get_locale",
            return_value="en",
        ), mock.patch(
            "routes.ai._attachment_filenames",
            return_value=[],
        ), mock.patch(
            "routes.ai.web_search.web_search_enabled",
            return_value=False,
        ), mock.patch(
            "routes.ai.llm_client.build_client",
            return_value=self._client_answering_with_two_citations(),
        ):
            response = self.client.post(
                "/api/ai",
                json={"question": "What is current?", "mode": "flash"},
            )
            body = response.get_data(as_text=True)

        events = [
            json.loads(line[len("data: "):])
            for line in body.splitlines()
            if line.startswith("data: ")
        ]
        self.assertNotIn("error", [event.get("type") for event in events])
        citation_events = [
            event for event in events if event.get("type") == "citations"
        ]
        self.assertEqual(
            [item["filename"] for item in citation_events[-1]["items"]],
            ["notes.txt", "available.pdf"],
        )
        self.assertEqual(
            citation_events[-1]["items"][1]["url"],
            f"/paper/{PAPER_ID}",
        )
        self.assertEqual(
            [hit["content"] for hit in grounded_hits],
            ["Attachment content", "Current content"],
        )

    def test_ask_retrieval_failure_log_omits_provider_exception_details(self):
        sentinel = "SENTINEL_ASK_PROVIDER_SECRET_DO_NOT_LOG"

        with mock.patch("routes.ai.OPEN_ACCESS", True), mock.patch(
            "routes.ai.llm_client.llm_enabled",
            return_value=True,
        ), mock.patch(
            "routes.ai._ask_rate_ok",
            return_value=True,
        ), mock.patch(
            "routes.ai._attachment_grounding",
            return_value=[],
        ), mock.patch(
            "routes.ai.rag_index.retrieve",
            side_effect=RuntimeError(sentinel),
        ), mock.patch(
            "routes.ai.get_locale",
            return_value="en",
        ), mock.patch(
            "routes.ai._attachment_filenames",
            return_value=[],
        ), mock.patch(
            "routes.ai.web_search.web_search_enabled",
            return_value=False,
        ), mock.patch(
            "routes.ai.llm_client.build_client",
            return_value=self._client_answering_with_two_citations(),
        ), self.assertLogs(self.app.logger, level="ERROR") as logs:
            response = self.client.post(
                "/api/ai",
                json={"question": "What is current?", "mode": "flash"},
            )
            body = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn('"type": "done"', body)
        self.assertEqual(
            [record.getMessage() for record in logs.records],
            ["Ask library retrieval failed"],
        )
        self.assertTrue(all(record.exc_info is None for record in logs.records))
        self.assertNotIn(sentinel, "\n".join(logs.output))


class AcceptedSubmissionLinkTest(unittest.TestCase):
    def setUp(self):
        record = PaperRecord(
            paper_id=PAPER_ID,
            current_revision=1,
            row_version=1,
            filename="canonical.pdf",
            title="Canonical",
            journal="",
            category="",
            language="",
            keywords="",
            abstract="",
            author_name="",
            author_email="",
            author_school="",
            published_at="",
            ib_ee_data="",
            is_ib_sample="",
            cp_data="",
            is_anonymous="",
            ia_data="",
        )
        self.library = RecordingPaperLibrary(SimpleNamespace(paper=record))
        self.app = Flask(__name__)
        self.app.secret_key = "accepted-submission-link-test"

        @self.app.get("/paper/<uuid:paper_id>/pdf", endpoint="paper_file")
        def paper_file(paper_id):
            return str(paper_id)

        register_submission_routes(self.app)
        self.app.extensions["paper_library"] = self.library
        self.client = self.app.test_client()

    def test_accepted_submission_uses_only_its_persisted_paper_id(self):
        rendered = {}
        submission = {
            "id": "submission-1",
            "submitter": "owner",
            "status": "accepted",
            "paper_id": PAPER_ID,
            "filename": "wrong-legacy-name.pdf",
        }

        def render(_template_name, **context):
            rendered.update(context)
            return "submission"

        with mock.patch(
            "routes.submissions.require_login",
            return_value={"username": "owner"},
        ), mock.patch(
            "routes.submissions._get_submission",
            return_value=submission,
        ), mock.patch(
            "routes.submissions.render_template",
            side_effect=render,
        ):
            response = self.client.get(
                "/dashboard/my-submissions/submission-1"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(rendered["pdf_url"], f"/paper/{PAPER_ID}/pdf")
        self.assertEqual(self.library.current_calls, [PAPER_ID])

    def test_accepted_submission_never_guesses_from_a_filename(self):
        rendered = {}
        submission = {
            "id": "submission-2",
            "submitter": "owner",
            "status": "accepted",
            "paper_id": None,
            "filename": "guess-me.pdf",
        }

        def render(_template_name, **context):
            rendered.update(context)
            return "submission"

        with mock.patch(
            "routes.submissions.require_login",
            return_value={"username": "owner"},
        ), mock.patch(
            "routes.submissions._get_submission",
            return_value=submission,
        ), mock.patch(
            "routes.submissions.render_template",
            side_effect=render,
        ):
            response = self.client.get(
                "/dashboard/my-submissions/submission-2"
            )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(rendered["pdf_url"])
        self.assertEqual(self.library.current_calls, [])


if __name__ == "__main__":
    unittest.main()
