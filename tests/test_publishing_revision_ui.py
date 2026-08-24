"""UUID/version management routes and revision-aware command mapping."""

from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from flask import Flask, abort
from flask_babel import Babel

from routes.papers import register_routes
from services.paper_library import PaperPdf, PaperRecord
from services.publishing_contracts import (
    Actor,
    DeletePaper,
    DeletionProgress,
    DeletionState,
    EditMetadata,
    BulkPapersChanged,
    BulkEditMetadata,
    InvalidInput,
    IndexingOutcome,
    IndexingState,
    PaperChanged,
    NotFound,
    RestoreRevision,
    RevisePdf,
    StaleVersion,
)


PAPER_ID = "00000000-0000-4000-8000-000000000014"


class RecordingLifecycle:
    def __init__(self):
        self.changes = []
        self.deletions = []
        self.bulk_changes = []
        self.delete_state = DeletionState.DELETED
        self.change_error = None
        self.change_indexing = None

    def change_paper(self, intent):
        self.changes.append(intent)
        if self.change_error is not None:
            raise self.change_error
        return PaperChanged(
            PAPER_ID,
            "paper.pdf",
            2,
            8,
            indexing=self.change_indexing,
        )

    def delete_paper(self, intent):
        self.deletions.append(intent)
        return DeletionProgress(PAPER_ID, self.delete_state)

    def change_many_metadata(self, intent):
        self.bulk_changes.append(intent)
        return BulkPapersChanged(
            tuple(
                PaperChanged(
                    patch.paper_id,
                    "paper.pdf",
                    1,
                    patch.expected_row_version + 1,
                )
                for patch in intent.patches
            )
        )


class PaperMutationRouteTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        path = Path(self.temp.name) / "paper.pdf"
        path.write_bytes(b"%PDF-1.4\n%%EOF\n")
        self.record = PaperRecord(
            paper_id=PAPER_ID,
            current_revision=1,
            row_version=7,
            filename="paper.pdf",
            title="Paper",
            journal="",
            category="science",
            language="en",
            keywords="paper",
            abstract="Abstract",
            author_name="Author",
            author_email="author@example.test",
            author_school="School",
            published_at="2026-07-21",
            ib_ee_data="",
            is_ib_sample="",
            cp_data="",
            is_anonymous="",
            ia_data="",
        )
        self.document = PaperPdf(self.record, 1, path, "a" * 64, path.stat().st_size)
        self.app = Flask(__name__)
        self.app.secret_key = "task-14-paper-route"
        Babel(self.app)

        @self.app.get("/login")
        def login():
            return "login"

        @self.app.get("/dashboard")
        def dashboard():
            return "dashboard"

        register_routes(self.app)
        self.lifecycle = RecordingLifecycle()
        self.app.extensions["publishing_lifecycle"] = self.lifecycle
        self.client = self.app.test_client()
        self.user = {"username": "contributor@example.test", "role": "2"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(self.user)
        self.auth_user = dict(self.user)
        auth = mock.patch(
            "routes.publishing_http.get_active_user",
            side_effect=lambda: self.auth_user,
        )
        auth.start()
        self.addCleanup(auth.stop)
        lookups = mock.patch.multiple(
            "routes.papers",
            get_journal_names=mock.Mock(return_value=[]),
            load_paper_categories=mock.Mock(return_value=[]),
            load_ee_subjects=mock.Mock(return_value={"groups": []}),
            load_ia_subjects=mock.Mock(return_value={"groups": []}),
        )
        lookups.start()
        self.addCleanup(lookups.stop)

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _metadata_form():
        return {
            "row_version": "7",
            "title": "Changed Paper",
            "category": "science",
            "language": "en",
            "keywords": "changed",
            "abstract": "Changed abstract",
            "author_name": "Author",
            "author_email": "author@example.test",
            "author_school": "School",
        }

    @staticmethod
    def _unchanged_metadata_form():
        return {
            "row_version": "7",
            "title": "Paper",
            "category": "science",
            "language": "en",
            "keywords": "paper",
            "abstract": "Abstract",
            "author_name": "Author",
            "author_email": "author@example.test",
            "author_school": "School",
        }

    def _route_patches(self):
        return mock.patch.multiple(
            "routes.papers",
            require_login=mock.DEFAULT,
            _current_paper_pdf=mock.DEFAULT,
            # Restore is throttled against the shared rate-limit store; these
            # command-mapping tests have no database, so allow every request.
            consume_rate_limit=mock.Mock(
                return_value=SimpleNamespace(allowed=True, retry_after=0, count=1)
            ),
        )

    def test_uuid_metadata_post_maps_edit_metadata(self):
        with self._route_patches() as patched:
            patched["require_login"].return_value = self.user
            patched["_current_paper_pdf"].return_value = self.document
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/modify",
                data=self._metadata_form(),
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/dashboard")
        self.assertEqual(len(self.lifecycle.changes), 1)
        intent = self.lifecycle.changes[0]
        self.assertIsInstance(intent, EditMetadata)
        self.assertEqual(intent.actor, Actor("contributor@example.test", 2))
        self.assertEqual(intent.patch.paper_id, PAPER_ID)
        self.assertEqual(intent.patch.expected_row_version, 7)

    def test_uuid_replacement_post_maps_revise_pdf(self):
        form = self._unchanged_metadata_form()
        form["replacement_pdf"] = (
            io.BytesIO(b"%PDF-1.4\nreplacement\n%%EOF\n"),
            "replacement.pdf",
        )
        with self._route_patches() as patched:
            patched["require_login"].return_value = self.user
            patched["_current_paper_pdf"].return_value = self.document
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/modify",
                data=form,
                content_type="multipart/form-data",
            )
        self.assertEqual(response.status_code, 302)
        self.assertIsInstance(self.lifecycle.changes[0], RevisePdf)
        self.assertEqual(self.lifecycle.changes[0].expected_row_version, 7)

    def test_uuid_replacement_index_failure_uses_exact_success_warning(self):
        self.lifecycle.change_indexing = IndexingOutcome(IndexingState.FAILED)
        form = self._unchanged_metadata_form()
        form["replacement_pdf"] = (
            io.BytesIO(b"%PDF-1.4\nreplacement\n%%EOF\n"),
            "replacement.pdf",
        )
        with self._route_patches() as patched:
            patched["require_login"].return_value = self.user
            patched["_current_paper_pdf"].return_value = self.document
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/modify",
                data=form,
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["_flashes"][-1],
                ("warning", "Paper PDF revised, but RAG indexing failed."),
            )

    def test_metadata_edit_keeps_normal_success_when_change_outcome_has_indexing(self):
        self.lifecycle.change_indexing = IndexingOutcome(IndexingState.FAILED)
        with self._route_patches() as patched:
            patched["require_login"].return_value = self.user
            patched["_current_paper_pdf"].return_value = self.document
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/modify",
                data=self._metadata_form(),
            )

        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["_flashes"][-1],
                ("success", "Paper information updated."),
            )

    def test_uuid_replacement_rejects_simultaneous_metadata_edits(self):
        form = self._metadata_form()
        form["replacement_pdf"] = (
            io.BytesIO(b"%PDF-1.4\nreplacement\n%%EOF\n"),
            "replacement.pdf",
        )
        with self._route_patches() as patched, mock.patch(
            "routes.papers.render_template",
            return_value="Save metadata changes separately",
        ):
            patched["require_login"].return_value = self.user
            patched["_current_paper_pdf"].return_value = self.document
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/modify",
                data=form,
                content_type="multipart/form-data",
            )
        self.assertEqual(response.status_code, 422)
        self.assertIn(b"Save metadata changes separately", response.data)
        self.assertEqual(self.lifecycle.changes, [])

    def test_modify_invalid_input_renders_sanitized_message_and_fields(self):
        self.lifecycle.change_error = InvalidInput({"title": "is too long"})

        def render(_template, **context):
            error = context["publishing_error"]
            return f"{error['message']}|{error['field_errors']['title']}"

        with self._route_patches() as patched, mock.patch(
            "routes.papers.render_template", side_effect=render
        ):
            patched["require_login"].return_value = self.user
            patched["_current_paper_pdf"].return_value = self.document
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/modify",
                data=self._metadata_form(),
            )
        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.data,
            b"Please correct the highlighted fields.|is too long",
        )

    def test_modify_stale_version_renders_exact_message_and_submitted_state(self):
        self.lifecycle.change_error = StaleVersion(8)
        form = self._metadata_form()
        form["row_version"] = "6"

        def render(_template, **context):
            error = context["publishing_error"]
            return f"{error['message']}|{context['row_version']}|{context['meta']['title']}"

        with self._route_patches() as patched, mock.patch(
            "routes.papers.render_template", side_effect=render
        ):
            patched["require_login"].return_value = self.user
            patched["_current_paper_pdf"].return_value = self.document
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/modify",
                data=form,
            )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.data,
            b"This paper changed while you were editing it. Reload and try again.|6|Changed Paper",
        )

    def test_uuid_restore_and_delete_map_versioned_commands(self):
        with mock.patch("routes.papers.require_login", return_value=self.user):
            restore = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/restore/1",
                data={"row_version": "7"},
            )
            delete = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/delete",
                data={"row_version": "8"},
            )
        self.assertEqual((restore.status_code, delete.status_code), (302, 302))
        self.assertEqual(
            (restore.headers["Location"], delete.headers["Location"]),
            ("/dashboard", "/dashboard"),
        )
        self.assertEqual(
            self.lifecycle.changes[-1],
            RestoreRevision(Actor("contributor@example.test", 2), PAPER_ID, 7, 1),
        )
        self.assertEqual(
            self.lifecycle.deletions,
            [DeletePaper(Actor("contributor@example.test", 2), PAPER_ID, 8)],
        )

    def test_uuid_restore_index_failure_uses_exact_success_warning(self):
        self.lifecycle.change_indexing = IndexingOutcome(IndexingState.FAILED)
        with mock.patch("routes.papers.require_login", return_value=self.user):
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/restore/1",
                data={"row_version": "7"},
            )

        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["_flashes"][-1],
                ("warning", "Paper revision restored, but RAG indexing failed."),
            )

    def test_legacy_management_redirects_preserve_method_contract(self):
        with mock.patch(
            "routes.papers._legacy_paper_document", return_value=self.document
        ), mock.patch(
            "routes.papers.require_login", return_value=self.user
        ):
            get_response = self.client.get("/dashboard/paper/paper.pdf/modify")
            post_response = self.client.post("/dashboard/paper/paper.pdf/modify")
            delete_response = self.client.post("/dashboard/paper/paper.pdf/delete")
        target = f"/dashboard/paper/{PAPER_ID}"
        self.assertEqual(get_response.status_code, 301)
        self.assertEqual(post_response.status_code, 308)
        self.assertEqual(delete_response.status_code, 308)
        self.assertTrue(get_response.headers["Location"].startswith(target))

    def test_legacy_management_authenticates_before_alias_lookup(self):
        with self.client.session_transaction() as flask_session:
            flask_session.clear()
        with mock.patch(
            "routes.papers.require_login", return_value=None
        ), mock.patch("routes.papers._legacy_paper_document") as alias_lookup:
            response = self.client.get("/dashboard/paper/secret.pdf/modify")
        self.assertEqual(response.status_code, 302)
        alias_lookup.assert_not_called()

    def test_deleting_outcome_uses_only_progress_warning_and_route_is_hidden(self):
        self.lifecycle.delete_state = DeletionState.DELETING
        with mock.patch("routes.papers.require_login", return_value=self.user):
            response = self.client.post(
                f"/dashboard/paper/{PAPER_ID}/delete",
                data={"row_version": "7"},
            )
        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["_flashes"][-1],
                ("warning", "Paper deletion is in progress."),
            )
        with mock.patch("routes.papers.require_login", return_value=self.user), mock.patch(
            "routes.papers._current_paper_pdf", side_effect=lambda _paper_id: abort(404)
        ):
            hidden = self.client.get(f"/dashboard/paper/{PAPER_ID}/modify")
        self.assertEqual(hidden.status_code, 404)

    def test_bulk_set_journal_maps_uuid_version_batch(self):
        curator = {"username": "curator@example.test", "role": "3"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(curator)
        self.auth_user = dict(curator)
        with mock.patch("routes.papers.require_login", return_value=curator):
            response = self.client.post(
                "/dashboard/admin/papers/bulk",
                json={
                    "op": "set_journal",
                    "journal": "Journal A",
                    "paper_ids": [PAPER_ID],
                    "row_versions": {PAPER_ID: 7},
                },
            )
        self.assertEqual(response.status_code, 200)
        intent = self.lifecycle.bulk_changes[0]
        self.assertIsInstance(intent, BulkEditMetadata)
        self.assertEqual(intent.actor, Actor("curator@example.test", 3))
        self.assertEqual(intent.patches[0].paper_id, PAPER_ID)
        self.assertEqual(intent.patches[0].expected_row_version, 7)
        self.assertEqual(intent.patches[0].changes, (("journal", "Journal A"),))

    def test_bulk_delete_reports_deleting_count_without_deleted_success(self):
        curator = {"username": "curator@example.test", "role": "3"}
        self.lifecycle.delete_state = DeletionState.DELETING
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(curator)
        self.auth_user = dict(curator)
        with mock.patch("routes.papers.require_login", return_value=curator):
            response = self.client.post(
                "/dashboard/admin/papers/bulk",
                json={
                    "op": "delete",
                    "paper_ids": [PAPER_ID],
                    "row_versions": {PAPER_ID: 7},
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["count"], 0)
        self.assertEqual(response.get_json()["deleting_count"], 1)

    def test_revision_management_template_exposes_state_history_and_versions(self):
        template = (
            Path(__file__).resolve().parents[1] / "templates" / "paper_modify.html"
        ).read_text(encoding="utf-8")
        for marker in (
            'name="row_version"',
            'name="replacement_pdf"',
            "management.index_status",
            "management.index_error",
            "management.revisions",
            "paper_revision_file",
            "paper_restore",
        ):
            self.assertIn(marker, template)


if __name__ == "__main__":
    unittest.main()
