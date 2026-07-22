"""Permanent inventory guard for publishing cutover writer boundaries."""

from __future__ import annotations

import inspect
import io
import json
import unittest
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from unittest import mock

from flask import Flask
from flask_babel import Babel

from routes import journals, papers, submissions, upload
from models import SubmissionModel
from services import papers as paper_service
from services import submissions as submission_service
from services.publishing_contracts import (
    BulkPapersChanged,
    PaperChanged,
    StaleVersion,
)
from tests.publishing_support import PublishingLifecycleTestCase


PAPER_A = "00000000-0000-4000-8000-000000000a11"
PAPER_B = "00000000-0000-4000-8000-000000000a12"


class FakeLibrary:
    def __init__(self, records):
        self.records = tuple(records)

    def list_visible(self):
        return self.records


class BatchLifecycle:
    def __init__(self):
        self.intents = []
        self.error = None

    def change_many_metadata(self, intent):
        self.intents.append(intent)
        if self.error:
            raise self.error
        return BulkPapersChanged(
            tuple(
                PaperChanged(p.paper_id, "paper.pdf", 1, p.expected_row_version + 1)
                for p in intent.patches
            )
        )


class LegacyWriterInventoryTest(unittest.TestCase):
    def test_whole_table_and_filename_metadata_writers_are_gone(self):
        for module, names in (
            (
                paper_service,
                ("save_paper_metadata", "upsert_paper_metadata", "remove_paper_metadata"),
            ),
            (submission_service, ("_write_submissions",)),
        ):
            for name in names:
                self.assertFalse(hasattr(module, name), f"legacy writer remains: {name}")

        combined = "\n".join(
            inspect.getsource(module)
            for module in (paper_service, submission_service)
        )
        self.assertNotIn("db.query(PaperMetadataModel).delete()", combined)
        self.assertNotIn("db.query(SubmissionModel).delete()", combined)
        get_source = inspect.getsource(submission_service._get_submission)
        self.assertIn("SubmissionModel.id == sub_id", get_source)
        self.assertNotIn("_load_submissions", get_source)

    def test_route_mutation_adapters_do_not_orchestrate_paper_storage_or_rag(self):
        functions = (
            upload.register_routes,
            submissions.register_routes,
            papers.register_routes,
            journals.register_routes,
        )
        combined = "\n".join(inspect.getsource(function) for function in functions)
        for forbidden in (
            "set_pdf_metadata(",
            "upsert_paper_metadata(",
            "remove_paper_metadata(",
            "file.save(",
            "shutil.",
            "rag_index.purge(",
            "db_session(",
            "PaperChunkModel",
        ):
            self.assertNotIn(forbidden, combined)

    def test_decided_draft_update_never_writes_pending_bytes(self):
        row = SimpleNamespace(
            id="s1", submitted_by="reader@example.test", status="rejected"
        )

        class Query:
            def filter(self, *_args): return self
            def with_for_update(self): return self
            def one_or_none(self): return row

        db = SimpleNamespace(query=lambda _model: Query())
        pending_write = mock.Mock()
        with mock.patch.object(
            submission_service, "db_session", return_value=nullcontext(db)
        ), mock.patch.object(submission_service, "lock_submission_creation_fence"):
            result = submission_service._update_submission(
                "s1",
                {"status": "pending"},
                expected_submitter="reader@example.test",
                expected_status="draft",
                pending_write=pending_write,
            )
        self.assertIsNone(result)
        pending_write.assert_not_called()

    def test_targeted_writes_flush_before_bytes_and_never_commit_inside_context(self):
        insert_events = []
        insert_db = SimpleNamespace(
            add=lambda _row: insert_events.append("add"),
            flush=lambda: insert_events.append("flush"),
            commit=mock.Mock(),
        )
        with mock.patch.object(
            submission_service, "db_session", return_value=nullcontext(insert_db)
        ), mock.patch.object(submission_service, "lock_submission_creation_fence"):
            submission_service._save_submission(
                {"id": "s2"},
                pending_write=lambda: insert_events.append("write"),
            )
        self.assertEqual(insert_events, ["add", "flush", "write"])
        insert_db.commit.assert_not_called()

        row = SimpleNamespace(**{
            model_name: None
            for model_name in submission_service._EXTERNAL_TO_MODEL.values()
        })
        row.id = "s1"
        row.submitted_by = "reader@example.test"
        row.status = "draft"

        class Query:
            def filter(self, *_args): return self
            def with_for_update(self): return self
            def one_or_none(self): return row

        update_events = []
        update_db = SimpleNamespace(
            query=lambda _model: Query(),
            flush=lambda: update_events.append(("flush", row.status)),
            commit=mock.Mock(),
        )
        with mock.patch.object(
            submission_service, "db_session", return_value=nullcontext(update_db)
        ), mock.patch.object(submission_service, "lock_submission_creation_fence"):
            result = submission_service._update_submission(
                "s1",
                {"status": "pending"},
                expected_submitter="reader@example.test",
                expected_status="draft",
                pending_write=lambda: update_events.append(("write", row.status)),
            )
        self.assertEqual(
            update_events,
            [("flush", "pending"), ("write", "pending")],
        )
        self.assertEqual(result["status"], "pending")
        update_db.commit.assert_not_called()

    def test_context_exit_failure_after_write_cleans_pending_bytes(self):
        cleanup = mock.Mock()
        pending_write = mock.Mock()

        class FailingExit:
            def __init__(self, db):
                self.db = db
            def __enter__(self):
                return self.db
            def __exit__(self, *_args):
                raise RuntimeError("context commit failed")

        insert_db = SimpleNamespace(add=lambda _row: None, flush=lambda: None)
        with mock.patch.object(
            submission_service, "db_session", return_value=FailingExit(insert_db)
        ), mock.patch.object(submission_service, "lock_submission_creation_fence"):
            with self.assertRaises(RuntimeError):
                submission_service._save_submission(
                    {"id": "s2"},
                    pending_write=pending_write,
                    pending_cleanup_on_failure=cleanup,
                )
        pending_write.assert_called_once()
        cleanup.assert_called_once()

        row = SimpleNamespace(
            id="s1", submitted_by="reader@example.test", status="draft"
        )

        class Query:
            def filter(self, *_args): return self
            def with_for_update(self): return self
            def one_or_none(self): return row

        cleanup.reset_mock()
        pending_write.reset_mock()
        update_db = SimpleNamespace(
            query=lambda _model: Query(),
            flush=lambda: None,
        )
        with mock.patch.object(
            submission_service, "db_session", return_value=FailingExit(update_db)
        ), mock.patch.object(submission_service, "lock_submission_creation_fence"):
            with self.assertRaises(RuntimeError):
                submission_service._update_submission(
                    "s1",
                    {"status": "pending"},
                    expected_submitter="reader@example.test",
                    expected_status="draft",
                    pending_write=pending_write,
                    pending_cleanup_on_failure=cleanup,
                )
        pending_write.assert_called_once()
        cleanup.assert_called_once()

    def test_update_snapshots_model_before_context_exit_expires_it(self):
        values = {
            model_name: None
            for model_name in submission_service._EXTERNAL_TO_MODEL.values()
        }
        row = SimpleNamespace(**values)
        row.id = "s1"
        row.submitted_by = "reader@example.test"
        row.status = "draft"

        class Query:
            def filter(self, *_args): return self
            def with_for_update(self): return self
            def one_or_none(self): return row

        class ExpiringExit:
            def __enter__(self):
                return SimpleNamespace(
                    query=lambda _model: Query(),
                    flush=lambda: None,
                )
            def __exit__(self, *_args):
                for model_name in values:
                    setattr(row, model_name, "expired")
                return False

        with mock.patch.object(
            submission_service, "db_session", return_value=ExpiringExit()
        ), mock.patch.object(submission_service, "lock_submission_creation_fence"):
            result = submission_service._update_submission(
                "s1",
                {"status": "pending"},
                expected_submitter="reader@example.test",
                expected_status="draft",
            )
        self.assertEqual(result["id"], "s1")
        self.assertEqual(result["status"], "pending")


class ReaderIntakePersistenceTest(PublishingLifecycleTestCase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.app = Flask(__name__)
        self.app.testing = True
        self.app.secret_key = "reader-intake-sqlite"
        Babel(self.app)

        @self.app.get("/login")
        def login():
            return "login"

        @self.app.get("/dashboard")
        def dashboard():
            return "dashboard"

        @self.app.get("/dashboard/my-submissions", endpoint="my_submissions")
        def my_submissions():
            return "submissions"

        upload.register_routes(self.app)
        self.client = self.app.test_client()
        self.reader = {
            "username": "reader@example.test",
            "display_name": "Reader",
            "role": "1",
        }
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(self.reader)
        auth = mock.patch(
            "routes.publishing_http.get_active_user",
            return_value=self.reader,
        )
        auth.start()
        self.addCleanup(auth.stop)

    @contextmanager
    def _real_db_session(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _pending_form(pdf_bytes):
        return {
            "title": "Reader Paper",
            "category": "science",
            "language": "en",
            "keywords": "reader, intake",
            "abstract": "Reader intake persistence.",
            "author_name": "Reader Author",
            "author_email": "reader@example.test",
            "author_school": "Reader School",
            "paper": (io.BytesIO(pdf_bytes), "reader.pdf"),
        }

    def _rows(self):
        with self.session_factory() as session:
            return session.query(SubmissionModel).all()

    def test_reader_draft_creation_persists_null_reviewed_at_in_sqlite(self):
        with mock.patch.object(
            submission_service, "db_session", side_effect=self._real_db_session
        ), mock.patch("routes.upload.require_login", return_value=self.reader):
            response = self.client.post(
                "/dashboard/upload",
                data={"save_draft": "1", "title": "Reader Draft"},
            )
        self.assertEqual(response.status_code, 302)
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].status, "draft")
        self.assertIsNone(rows[0].reviewed_at)

    def test_reader_pending_creation_persists_row_and_private_bytes(self):
        pdf_bytes = self.valid_pdf_bytes("reader-pending")
        with mock.patch.object(
            submission_service, "db_session", side_effect=self._real_db_session
        ), mock.patch("routes.upload.require_login", return_value=self.reader), mock.patch(
            "routes.upload.PENDING_PAPERS_DIR", self.storage.pending_dir
        ):
            response = self.client.post(
                "/dashboard/upload",
                data=self._pending_form(pdf_bytes),
                content_type="multipart/form-data",
            )
        self.assertEqual(response.status_code, 302)
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].status, "pending")
        self.assertIsNone(rows[0].reviewed_at)
        pending_path = self.storage.pending_dir / rows[0].pending_filename
        self.assertTrue(pending_path.is_file())
        self.assertTrue(pending_path.read_bytes().startswith(b"%PDF-"))

    def test_reader_pending_commit_failure_removes_written_bytes_and_row(self):
        @contextmanager
        def failing_commit():
            session = self.session_factory()
            try:
                yield session
                raise RuntimeError("injected context commit failure")
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        with mock.patch.object(
            submission_service, "db_session", side_effect=failing_commit
        ), mock.patch("routes.upload.require_login", return_value=self.reader), mock.patch(
            "routes.upload.PENDING_PAPERS_DIR", self.storage.pending_dir
        ):
            with self.assertRaisesRegex(RuntimeError, "context commit failure"):
                self.client.post(
                    "/dashboard/upload",
                    data=self._pending_form(self.valid_pdf_bytes("commit-failure")),
                    content_type="multipart/form-data",
                )
        self.assertEqual(self._rows(), [])
        self.assertEqual(
            [
                path
                for path in self.storage.pending_dir.iterdir()
                if path.name != ".trash"
            ],
            [],
        )

    def test_draft_delete_commit_failure_preserves_row_and_pending_file(self):
        pending_filename = "draft-commit-failure.pdf"
        pending_path = self.storage.pending_dir / pending_filename
        pending_path.write_bytes(self.valid_pdf_bytes("draft-commit-failure"))
        with self.session_factory() as session:
            session.add(
                SubmissionModel(
                    id="draft-commit-failure",
                    pending_filename=pending_filename,
                    status="draft",
                    submitted_by=self.reader["username"],
                )
            )
            session.commit()

        @contextmanager
        def failing_delete_commit():
            session = self.session_factory()
            try:
                yield session
                session.flush()
                raise RuntimeError("injected delete commit failure")
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        cleanup = mock.Mock(side_effect=lambda _filename: pending_path.unlink())
        with mock.patch.object(
            submission_service,
            "db_session",
            side_effect=failing_delete_commit,
        ):
            with self.assertRaisesRegex(RuntimeError, "delete commit failure"):
                submission_service._delete_submission(
                    "draft-commit-failure",
                    expected_submitter=self.reader["username"],
                    expected_status="draft",
                    pending_cleanup=cleanup,
                )

        cleanup.assert_not_called()
        self.assertTrue(pending_path.is_file())
        with self.session_factory() as session:
            self.assertIsNotNone(
                session.get(SubmissionModel, "draft-commit-failure")
            )


class AtomicMetadataBatchRouteTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "task-14-batch"
        Babel(self.app)

        @self.app.get("/login")
        def login():
            return "login"

        @self.app.get("/dashboard")
        def dashboard():
            return "dashboard"

        papers.register_routes(self.app)
        journals.register_routes(self.app)
        self.lifecycle = BatchLifecycle()
        self.records = (
            SimpleNamespace(
                paper_id=PAPER_A,
                row_version=99,
                category="Old Category",
                journal="Journal A",
                ib_ee_data=json.dumps({"core_subject": "Old EE"}),
                ia_data=json.dumps({"subject": "Old IA"}),
            ),
            SimpleNamespace(
                paper_id=PAPER_B,
                row_version=11,
                category="Other",
                journal="",
                ib_ee_data="",
                ia_data="",
            ),
        )
        self.app.extensions["paper_library"] = FakeLibrary(self.records)
        self.app.extensions["publishing_lifecycle"] = self.lifecycle
        self.client = self.app.test_client()
        self.curator = {"username": "curator@example.test", "role": "3"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(self.curator)
        auth = mock.patch(
            "routes.publishing_http.get_active_user",
            return_value=self.curator,
        )
        auth.start()
        self.addCleanup(auth.stop)

    def test_category_conflict_does_not_write_taxonomy(self):
        self.lifecycle.error = StaleVersion(100)
        with mock.patch("routes.papers.require_login", return_value=self.curator), mock.patch(
            "routes.papers.load_paper_categories", return_value=["Old Category"]
        ), mock.patch("routes.papers.save_paper_categories") as save:
            response = self.client.post(
                "/dashboard/admin/paper-categories/rename",
                json={"old_name": "Old Category", "new_name": "New Category"},
            )
        self.assertEqual(response.status_code, 409)
        save.assert_not_called()
        patch = self.lifecycle.intents[0].patches[0]
        self.assertEqual(
            (patch.paper_id, patch.expected_row_version, patch.changes),
            (PAPER_A, 99, (("category", "New Category"),)),
        )

    def test_ee_and_ia_conflicts_do_not_write_subject_files(self):
        cases = (
            (
                "/dashboard/admin/ee-subjects/save",
                "routes.papers.reconcile_ee_subjects",
                "routes.papers.save_ee_subjects",
                {"tree": {"groups": []}, "renames": [("Old EE", "New EE")], "deletions": [], "errors": []},
                "ib_ee_data",
            ),
            (
                "/dashboard/admin/ia-subjects/save",
                "routes.papers.reconcile_ia_subjects",
                "routes.papers.save_ia_subjects",
                {"tree": {"groups": []}, "renames": [("Old IA", "New IA")], "deletions": [], "errors": []},
                "ia_data",
            ),
        )
        for path, reconcile_name, save_name, result, field in cases:
            with self.subTest(path=path):
                self.lifecycle.intents.clear()
                self.lifecycle.error = StaleVersion(100)
                with mock.patch("routes.papers.require_login", return_value=self.curator), mock.patch(
                    reconcile_name, return_value=result
                ), mock.patch(save_name) as save:
                    response = self.client.post(path, json={})
                self.assertEqual(response.status_code, 409)
                save.assert_not_called()
                patch = self.lifecycle.intents[0].patches[0]
                self.assertEqual(patch.expected_row_version, 99)
                self.assertEqual(patch.changes[0][0], field)

    def test_journal_membership_uses_client_version_for_stale_removal(self):
        journal = {"id": "j1", "name": "Journal A"}
        with mock.patch("routes.journals.require_login", return_value=self.curator), mock.patch(
            "routes.journals.get_journal_by_id", return_value=journal
        ):
            response = self.client.post(
                "/dashboard/admin/journal/j1/papers",
                json={
                    "paper_ids": [PAPER_B],
                    "row_versions": {PAPER_A: 10, PAPER_B: 11},
                },
            )
        self.assertEqual(response.status_code, 200)
        patches = {patch.paper_id: patch for patch in self.lifecycle.intents[0].patches}
        self.assertEqual(patches[PAPER_A].expected_row_version, 10)
        self.assertEqual(patches[PAPER_A].changes, (("journal", ""),))
        self.assertEqual(patches[PAPER_B].changes, (("journal", "Journal A"),))

    def test_journal_membership_rejects_non_string_paper_id_collections(self):
        journal = {"id": "j1", "name": "Journal A"}
        invalid_values = (None, {"paper": PAPER_A}, [[PAPER_A]])
        for paper_ids in invalid_values:
            with self.subTest(paper_ids=paper_ids), mock.patch(
                "routes.journals.require_login", return_value=self.curator
            ), mock.patch(
                "routes.journals.get_journal_by_id", return_value=journal
            ):
                response = self.client.post(
                    "/dashboard/admin/journal/j1/papers",
                    json={
                        "paper_ids": paper_ids,
                        "row_versions": {PAPER_A: 99, PAPER_B: 11},
                    },
                )
            self.assertEqual(response.status_code, 422)
        self.assertEqual(self.lifecycle.intents, [])

    def test_journal_rename_conflict_does_not_write_journal(self):
        self.lifecycle.error = StaleVersion(100)
        journal = {
            "id": "j1",
            "name": "Journal A",
            "slug": "journal-a",
            "introduction": "",
        }
        with mock.patch("routes.journals.require_login", return_value=self.curator), mock.patch(
            "routes.journals.get_journal_by_id", return_value=journal
        ), mock.patch("routes.journals.load_journals", return_value=[dict(journal)]), mock.patch(
            "routes.journals.save_journals"
        ) as save, mock.patch("routes.journals.set_unique_slug", return_value="journal-b"):
            response = self.client.post(
                "/dashboard/admin/journal/j1/edit",
                data={"name": "Journal B", "introduction": ""},
            )
        self.assertEqual(response.status_code, 303)
        save.assert_not_called()


if __name__ == "__main__":
    unittest.main()
