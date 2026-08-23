"""HTTP command mapping for Paper and Submission lifecycle mutations."""

from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

from flask import Flask
from flask_babel import Babel
from flask_wtf.csrf import CSRFProtect

from routes.upload import register_routes as register_upload_routes
from routes.submissions import register_routes as register_submission_routes
from models import SubmissionModel
from services import submissions as submission_service
from services.publishing_contracts import (
    Actor,
    AcceptSubmission,
    CancelSubmission,
    DirectPublish,
    DecisionRecorded,
    IndexingOutcome,
    IndexingState,
    InvalidInput,
    Published,
    RejectSubmission,
    SubmissionCancelled,
)
from tests.publishing_support import PublishingLifecycleTestCase


class RecordingLifecycle:
    def __init__(self):
        self.published = []
        self.publish_indexing_state = IndexingState.INDEXED
        self.cancelled = []
        self.reviewed = []
        self.review_indexing_state = IndexingState.INDEXED
        self.review_error = None
        self.publish_error = None

    def publish_direct(self, intent):
        self.published.append(intent)
        if self.publish_error is not None:
            raise self.publish_error
        return Published(
            paper_id="00000000-0000-4000-8000-000000000a01",
            filename=intent.metadata.filename,
            revision=1,
            row_version=1,
            replayed=False,
            indexing=IndexingOutcome(self.publish_indexing_state),
        )

    def cancel_submission(self, intent):
        self.cancelled.append(intent)
        return SubmissionCancelled(intent.submission_id)

    def review_submission(self, intent):
        if self.review_error is not None:
            raise self.review_error
        self.reviewed.append(intent)
        accepted = isinstance(intent, AcceptSubmission)
        return DecisionRecorded(
            submission_id=intent.submission_id,
            accepted=accepted,
            paper_id=("00000000-0000-4000-8000-000000000a02" if accepted else None),
            replayed=False,
            indexing=(
                IndexingOutcome(self.review_indexing_state) if accepted else None
            ),
        )


class DelegatingReviewLifecycle:
    """Record real lifecycle outcomes while preserving the production behavior."""

    def __init__(self, lifecycle):
        self.lifecycle = lifecycle
        self.intents = []
        self.outcomes = []

    def review_submission(self, intent):
        self.intents.append(intent)
        outcome = self.lifecycle.review_submission(intent)
        self.outcomes.append(outcome)
        return outcome


class DirectPublicationRouteTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = Flask(__name__)
        self.app.secret_key = "task-14-route-test"
        Babel(self.app)
        self.app.jinja_env.globals["csrf_token"] = lambda: "test-csrf"

        @self.app.get("/login")
        def login():
            return "login"

        @self.app.get("/dashboard")
        def dashboard():
            return "dashboard"

        @self.app.get("/dashboard/my-submissions", endpoint="my_submissions")
        def my_submissions():
            return "submissions"

        register_upload_routes(self.app)
        self.lifecycle = RecordingLifecycle()
        self.app.extensions["publishing_lifecycle"] = self.lifecycle
        self.client = self.app.test_client()
        self.auth_user = None
        auth = mock.patch(
            "routes.publishing_http.get_active_user",
            side_effect=lambda: self.auth_user,
        )
        auth.start()
        self.addCleanup(auth.stop)

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _minimal_pdf_bytes():
        # Intake runs a strict structural budget (security finding: untrusted
        # parser without isolation), so the fixture must be a real PDF, not
        # magic-bytes-only placeholder bytes.
        from pypdf import PdfWriter

        stream = io.BytesIO()
        writer = PdfWriter()
        writer.add_blank_page(width=72, height=72)
        writer.write(stream)
        return stream.getvalue()

    @classmethod
    def _valid_form(cls):
        return {
            "publishing_idempotency_key": "publish-request-0001",
            "title": "Canonical Paper",
            "category": "science",
            "language": "en",
            "keywords": "canonical, lifecycle",
            "abstract": "A lifecycle-routed paper.",
            "author_name": "Ada Author",
            "author_email": "ada@example.test",
            "author_school": "Example School",
            "paper": (io.BytesIO(cls._minimal_pdf_bytes()), "upload.pdf"),
        }

    def test_contributor_post_maps_exact_direct_publish_intent(self):
        legacy_path = Path(self.temp.name) / "legacy.pdf"
        user = {
            "username": "contributor@example.test",
            "display_name": "Contributor",
            "role": "2",
        }
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)

        with mock.patch("routes.upload.require_login", return_value=user), mock.patch(
            "routes.upload._build_safe_paper_filename",
            return_value="canonical-paper.pdf",
        ), mock.patch(
            "routes.upload.resolve_contained",
            return_value=legacy_path,
        ):
            response = self.client.post(
                "/dashboard/upload",
                data=self._valid_form(),
                content_type="multipart/form-data",
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(self.lifecycle.published), 1)
        intent = self.lifecycle.published[0]
        self.assertIsInstance(intent, DirectPublish)
        self.assertEqual(intent.actor, Actor("contributor@example.test", 2))
        self.assertEqual(intent.idempotency_key, "publish-request-0001")
        self.assertEqual(intent.metadata.filename, "canonical-paper.pdf")
        self.assertEqual(intent.metadata.title, "Canonical Paper")
        self.assertEqual(intent.metadata.category, "science")
        self.assertEqual(intent.metadata.language, "en")
        self.assertEqual(intent.metadata.author_name, "Ada Author")
        self.assertEqual(intent.pdf.filename, "upload.pdf")

    def test_direct_publish_failed_index_uses_exact_success_warning(self):
        self.lifecycle.publish_indexing_state = IndexingState.FAILED
        user = {
            "username": "contributor@example.test",
            "display_name": "Contributor",
            "role": "2",
        }
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)

        with mock.patch("routes.upload.require_login", return_value=user), mock.patch(
            "routes.upload._build_safe_paper_filename",
            return_value="canonical-paper.pdf",
        ):
            response = self.client.post(
                "/dashboard/upload",
                data=self._valid_form(),
                content_type="multipart/form-data",
            )
        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["_flashes"][-1],
                (
                    "warning",
                    "canonical-paper.pdf uploaded successfully, but RAG indexing failed.",
                ),
            )

    def test_direct_idempotency_key_survives_validation_rerender(self):
        user = {"username": "contributor@example.test", "role": "2"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        form = self._valid_form()
        form["title"] = ""
        captured = {}
        with mock.patch("routes.upload.require_login", return_value=user), mock.patch(
            "routes.upload.load_paper_categories", return_value=[]
        ), mock.patch("routes.upload.get_journal_names", return_value=[]), mock.patch(
            "routes.upload.load_ee_subjects", return_value={"groups": []}
        ), mock.patch(
            "routes.upload.load_ia_subjects", return_value={"groups": []}
        ), mock.patch(
            "routes.upload.render_template",
            side_effect=lambda _template, **context: captured.update(context) or "upload",
        ):
            response = self.client.post(
                "/dashboard/upload", data=form, content_type="multipart/form-data"
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            captured["form_data"]["publishing_idempotency_key"],
            "publish-request-0001",
        )

    def test_direct_lifecycle_error_rerender_preserves_idempotency_key(self):
        self.lifecycle.publish_error = InvalidInput({"title": "is invalid"})
        user = {"username": "contributor@example.test", "role": "2"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        captured = {}

        def render(_template, **context):
            captured.update(context)
            error = context["publishing_error"]
            return f"{error['message']}|{error['field_errors']['title']}"

        with mock.patch("routes.upload.require_login", return_value=user), mock.patch(
            "routes.upload.render_template", side_effect=render
        ), mock.patch("routes.upload.load_paper_categories", return_value=[]), mock.patch(
            "routes.upload.get_journal_names", return_value=[]
        ), mock.patch(
            "routes.upload.load_ee_subjects", return_value={"groups": []}
        ), mock.patch(
            "routes.upload.load_ia_subjects", return_value={"groups": []}
        ):
            response = self.client.post(
                "/dashboard/upload",
                data=self._valid_form(),
                content_type="multipart/form-data",
            )
        self.assertEqual(response.status_code, 422)
        self.assertIn(b"Please correct the highlighted fields.|is invalid", response.data)
        self.assertEqual(
            captured["form_data"]["publishing_idempotency_key"],
            "publish-request-0001",
        )

    def test_direct_lifecycle_error_xhr_returns_structured_json_and_keeps_key(self):
        self.lifecycle.publish_error = InvalidInput({"title": "is invalid"})
        user = {"username": "contributor@example.test", "role": "2"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        with mock.patch("routes.upload.require_login", return_value=user):
            response = self.client.post(
                "/dashboard/upload",
                data=self._valid_form(),
                content_type="multipart/form-data",
                headers={"X-Requested-With": "XMLHttpRequest"},
            )
        self.assertEqual(response.status_code, 422)
        self.assertNotIn("Location", response.headers)
        self.assertEqual(
            response.get_json(),
            {
                "error": {
                    "code": "invalid_input",
                    "message": "Please correct the highlighted fields.",
                    "field_errors": {"title": "is invalid"},
                }
            },
        )
        self.assertEqual(
            self.lifecycle.published[-1].idempotency_key,
            "publish-request-0001",
        )

    def test_pending_direct_outcome_does_not_claim_index_failure(self):
        self.lifecycle.publish_indexing_state = IndexingState.PENDING
        user = {"username": "contributor@example.test", "role": "2"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        with mock.patch("routes.upload.require_login", return_value=user), mock.patch(
            "routes.upload._build_safe_paper_filename", return_value="canonical-paper.pdf"
        ):
            self.client.post(
                "/dashboard/upload",
                data=self._valid_form(),
                content_type="multipart/form-data",
            )
        with self.client.session_transaction() as flask_session:
            category, message = flask_session["_flashes"][-1]
        self.assertEqual(category, "success")
        self.assertNotIn("indexing failed", message)

class SubmissionCancellationRouteTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "task-14-cancel-test"
        Babel(self.app)
        self.app.jinja_env.globals["csrf_token"] = lambda: "test-csrf"

        @self.app.get("/login")
        def login():
            return "login"

        @self.app.get("/dashboard")
        def dashboard():
            return "dashboard"

        register_submission_routes(self.app)
        self.lifecycle = RecordingLifecycle()
        self.app.extensions["publishing_lifecycle"] = self.lifecycle
        self.client = self.app.test_client()
        self.auth_user = None
        auth = mock.patch(
            "routes.publishing_http.get_active_user",
            side_effect=lambda: self.auth_user,
        )
        auth.start()
        self.addCleanup(auth.stop)

    def test_owner_pending_delete_maps_to_cancel_submission(self):
        user = {"username": "reader@example.test", "role": "1"}
        pending = {
            "id": "submission-1",
            "submitter": user["username"],
            "status": "pending",
            "pending_filename": "submission-1.pdf",
        }
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)

        with mock.patch(
            "routes.submissions.require_login", return_value=user
        ), mock.patch(
            "routes.submissions._get_submission", return_value=pending
        ), mock.patch("routes.submissions._delete_submission") as legacy_delete:
            response = self.client.post(
                "/dashboard/my-submissions/submission-1/delete"
            )

        self.assertEqual(response.status_code, 302)
        legacy_delete.assert_not_called()
        self.assertEqual(
            self.lifecycle.cancelled,
            [CancelSubmission(Actor("reader@example.test", 1), "submission-1")],
        )

    def test_decided_submission_delete_is_permanent(self):
        user = {"username": "reader@example.test", "role": "1"}
        rejected = {
            "id": "submission-2",
            "submitter": user["username"],
            "status": "rejected",
        }
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)

        with mock.patch(
            "routes.submissions.require_login", return_value=user
        ), mock.patch(
            "routes.submissions._get_submission", return_value=rejected
        ), mock.patch("routes.submissions._delete_submission") as legacy_delete:
            response = self.client.post(
                "/dashboard/my-submissions/submission-2/delete"
            )

        self.assertEqual(response.status_code, 302)
        legacy_delete.assert_not_called()
        self.assertEqual(self.lifecycle.cancelled, [])

    @staticmethod
    def _pending_submission():
        return {
            "id": "submission-review",
            "submitter": "reader@example.test",
            "status": "pending",
            "pending_filename": "submission-review.pdf",
            "pdf_filename": "reviewed-paper.pdf",
            "title": "Reviewed Paper",
            "category": "science",
            "language": "en",
            "keywords": "review",
            "abstract": "Review abstract",
            "author_name": "Reader",
            "author_email": "reader@example.test",
            "author_school": "Example School",
        }

    def test_curator_accept_maps_exact_review_intent(self):
        user = {"username": "curator@example.test", "role": "3"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        with mock.patch(
            "routes.submissions.require_login", return_value=user
        ), mock.patch(
            "routes.submissions._get_submission",
            return_value=self._pending_submission(),
        ), mock.patch(
            "routes.submissions._utc_today", return_value="2026-07-21"
        ):
            response = self.client.post(
                "/dashboard/review/submission-review/accept",
                data={
                    "decision_idempotency_key": "decision-request-1",
                    "comment": "  Strong evidence.  ",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(self.lifecycle.reviewed), 1)
        intent = self.lifecycle.reviewed[0]
        self.assertIsInstance(intent, AcceptSubmission)
        self.assertEqual(intent.actor, Actor("curator@example.test", 3))
        self.assertEqual(intent.submission_id, "submission-review")
        self.assertEqual(intent.idempotency_key, "decision-request-1")
        self.assertEqual(intent.metadata.filename, "reviewed-paper.pdf")
        self.assertEqual(intent.metadata.published_at, "2026-07-21")
        self.assertEqual(intent.comment, "Strong evidence.")

    def test_pending_review_form_offers_acceptance_comment(self):
        template = (
            Path(__file__).resolve().parents[1] / "templates" / "review_paper.html"
        ).read_text(encoding="utf-8")

        self.assertIn('id="accept-comment"', template)

    def test_curator_reject_maps_exact_review_intent(self):
        user = {"username": "curator@example.test", "role": "3"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        with mock.patch(
            "routes.submissions.require_login", return_value=user
        ), mock.patch(
            "routes.submissions._get_submission",
            return_value=self._pending_submission(),
        ):
            response = self.client.post(
                "/dashboard/review/submission-review/reject",
                data={
                    "decision_idempotency_key": "decision-request-2",
                    "comment": "Needs revision.",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            self.lifecycle.reviewed,
            [
                RejectSubmission(
                    actor=Actor("curator@example.test", 3),
                    submission_id="submission-review",
                    idempotency_key="decision-request-2",
                    feedback="Needs revision.",
                )
            ],
        )

    def test_decided_review_detail_prunes_obsolete_cookie_key(self):
        user = {"username": "curator@example.test", "role": "3"}
        rejected = {
            **self._pending_submission(),
            "status": "rejected",
            "decision_idempotency_key": "persisted-decision",
        }
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
            flask_session["publishing_decision_keys"] = {
                "submission-review": "obsolete-cookie-key",
                "another-submission": "keep-me",
            }
        self.auth_user = dict(user)
        captured = {}
        with mock.patch(
            "routes.submissions.require_login", return_value=user
        ), mock.patch(
            "routes.submissions._get_submission", return_value=rejected
        ), mock.patch(
            "routes.submissions.render_template",
            side_effect=lambda _template, **context: captured.update(context) or "review",
        ):
            response = self.client.get("/dashboard/review/submission-review")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["decision_idempotency_key"], "")
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["publishing_decision_keys"],
                {"another-submission": "keep-me"},
            )

    def test_pending_review_detail_reuses_durable_decision_key(self):
        user = {"username": "curator@example.test", "role": "3"}
        pending = {
            **self._pending_submission(),
            "decision_idempotency_key": "durable-reservation-key",
        }
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
            flask_session["publishing_decision_keys"] = {
                "submission-review": "stale-cookie-key",
            }
        self.auth_user = dict(user)
        captured = {}
        with mock.patch(
            "routes.submissions.require_login", return_value=user
        ), mock.patch(
            "routes.submissions._get_submission", return_value=pending
        ), mock.patch(
            "routes.submissions.render_template",
            side_effect=lambda _template, **context: captured.update(context) or "review",
        ):
            response = self.client.get("/dashboard/review/submission-review")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            captured["decision_idempotency_key"],
            "durable-reservation-key",
        )
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["publishing_decision_keys"]["submission-review"],
                "durable-reservation-key",
            )

    def test_accept_failed_index_uses_exact_success_warning(self):
        self.lifecycle.review_indexing_state = IndexingState.FAILED
        user = {"username": "curator@example.test", "role": "3"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        with mock.patch(
            "routes.submissions.require_login", return_value=user
        ), mock.patch(
            "routes.submissions._get_submission",
            return_value=self._pending_submission(),
        ):
            response = self.client.post(
                "/dashboard/review/submission-review/accept",
                data={"decision_idempotency_key": "decision-request-3"},
            )
        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["_flashes"][-1],
                (
                    "warning",
                    "reviewed-paper.pdf published successfully, but RAG indexing failed.",
                ),
            )

    def test_pending_accept_outcome_does_not_claim_index_failure(self):
        self.lifecycle.review_indexing_state = IndexingState.PENDING
        user = {"username": "curator@example.test", "role": "3"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        with mock.patch("routes.submissions.require_login", return_value=user), mock.patch(
            "routes.submissions._get_submission", return_value=self._pending_submission()
        ):
            self.client.post(
                "/dashboard/review/submission-review/accept",
                data={"decision_idempotency_key": "decision-pending"},
            )
        with self.client.session_transaction() as flask_session:
            category, message = flask_session["_flashes"][-1]
        self.assertEqual((category, message), ("success", "Paper accepted and published."))

    def test_decision_key_survives_lifecycle_validation_redirect(self):
        self.lifecycle.review_error = InvalidInput({"title": "required"})
        user = {"username": "curator@example.test", "role": "3"}
        pending = self._pending_submission()
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        with mock.patch("routes.submissions.require_login", return_value=user), mock.patch(
            "routes.submissions._get_submission", return_value=pending
        ):
            response = self.client.post(
                "/dashboard/review/submission-review/accept",
                data={"decision_idempotency_key": "stable-decision-key"},
            )
        self.assertEqual(response.status_code, 303)
        with self.client.session_transaction() as flask_session:
            self.assertEqual(
                flask_session["publishing_decision_keys"]["submission-review"],
                "stable-decision-key",
            )

    def test_submission_detail_distinguishes_private_and_unavailable_pdfs(self):
        user = {"username": "reader@example.test", "role": "1"}
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = dict(user)
        self.auth_user = dict(user)
        captured = []

        def renderer(_template, **context):
            captured.append(context)
            return "detail"

        rejected = {
            **self._pending_submission(),
            "submitter": user["username"],
            "status": "rejected",
        }
        with mock.patch("routes.submissions.require_login", return_value=user), mock.patch(
            "routes.submissions._get_submission", return_value=rejected
        ), mock.patch("routes.submissions.resolve_contained", return_value=Path("/private/rejected.pdf")), mock.patch(
            "routes.submissions.render_template", side_effect=renderer
        ):
            self.client.get("/dashboard/my-submissions/submission-review")
        self.assertIn("/file", captured[-1]["pdf_url"])

        with mock.patch("routes.submissions.require_login", return_value=user), mock.patch(
            "routes.submissions._get_submission", return_value=rejected
        ), mock.patch("routes.submissions.resolve_contained", return_value=None), mock.patch(
            "routes.submissions.render_template", side_effect=renderer
        ):
            self.client.get("/dashboard/my-submissions/submission-review")
        self.assertIsNone(captured[-1]["pdf_url"])

        accepted = {**rejected, "status": "accepted", "paper_id": None}
        with mock.patch("routes.submissions.require_login", return_value=user), mock.patch(
            "routes.submissions._get_submission", return_value=accepted
        ), mock.patch("routes.submissions.render_template", side_effect=renderer):
            self.client.get("/dashboard/my-submissions/submission-review")
        self.assertIsNone(captured[-1]["pdf_url"])


class MutationCsrfTest(unittest.TestCase):
    def test_every_post_mutation_is_rejected_without_csrf(self):
        app = Flask(__name__)
        app.secret_key = "task-14-real-csrf"
        CSRFProtect(app)

        @app.get("/login")
        def login():
            return "login"

        @app.get("/dashboard")
        def dashboard():
            return "dashboard"

        register_upload_routes(app)
        register_submission_routes(app)
        from routes.papers import register_routes as register_paper_routes
        from routes.journals import register_routes as register_journal_routes

        register_paper_routes(app)
        register_journal_routes(app)
        client = app.test_client()
        paths = (
            "/dashboard/upload",
            "/dashboard/my-submissions/s1/delete",
            "/dashboard/review/s1/accept",
            "/dashboard/review/s1/reject",
            "/dashboard/paper/00000000-0000-4000-8000-000000000014/modify",
            "/dashboard/paper/00000000-0000-4000-8000-000000000014/delete",
            "/dashboard/paper/00000000-0000-4000-8000-000000000014/restore/1",
            "/dashboard/admin/papers/bulk",
            "/dashboard/admin/paper-categories/rename",
            "/dashboard/admin/journals/delete",
            "/dashboard/admin/journal/j1/edit",
            "/dashboard/admin/journal/j1/papers",
            "/dashboard/admin/ee-subjects/save",
            "/dashboard/admin/ia-subjects/save",
        )
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(client.post(path).status_code, 400)

    def test_unauthorized_and_under_role_requests_stop_before_lifecycle(self):
        app = Flask(__name__)
        app.secret_key = "task-14-auth"
        Babel(app)

        @app.get("/login")
        def login(): return "login"

        @app.get("/dashboard")
        def dashboard(): return "dashboard"

        register_upload_routes(app)
        register_submission_routes(app)
        from routes.papers import register_routes as register_paper_routes
        from routes.journals import register_routes as register_journal_routes
        register_paper_routes(app)
        register_journal_routes(app)
        lifecycle = RecordingLifecycle()
        app.extensions["publishing_lifecycle"] = lifecycle
        client = app.test_client()
        with mock.patch("routes.upload.require_login", return_value=None), mock.patch(
            "routes.submissions.require_login", return_value=None
        ), mock.patch("routes.papers.require_login", return_value=None), mock.patch(
            "routes.journals.require_login", return_value=None
        ):
            statuses = (
                client.post("/dashboard/upload").status_code,
                client.post("/dashboard/my-submissions/s1/delete").status_code,
                client.post("/dashboard/review/s1/accept").status_code,
                client.post("/dashboard/admin/papers/bulk", json={}).status_code,
                client.post("/dashboard/admin/paper-categories/rename", json={}).status_code,
                client.post("/dashboard/admin/journals/delete", json={}).status_code,
            )
        self.assertEqual(statuses, (302, 302, 302, 401, 401, 401))
        self.assertEqual(lifecycle.published, [])
        self.assertEqual(lifecycle.reviewed, [])
        self.assertEqual(lifecycle.cancelled, [])


class SubmissionDecisionReplayRouteTest(
    PublishingLifecycleTestCase,
    unittest.TestCase,
):
    """Exercise Flask retry mapping against the real publishing lifecycle."""

    def setUp(self):
        super().setUp()
        self.app = Flask(__name__)
        self.app.testing = True
        self.app.secret_key = "task-14-decision-replay"
        Babel(self.app)

        @self.app.get("/login")
        def login():
            return "login"

        @self.app.get("/dashboard")
        def dashboard():
            return "dashboard"

        register_submission_routes(self.app)
        self.recording_lifecycle = DelegatingReviewLifecycle(self.lifecycle)
        self.app.extensions["publishing_lifecycle"] = self.recording_lifecycle
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

        self.require_login = mock.patch(
            "routes.submissions.require_login", return_value=self.curator
        )
        self.lookup = mock.patch.object(
            submission_service,
            "db_session",
            side_effect=self._real_db_session,
        )
        self.today = mock.patch(
            "routes.submissions._utc_today", return_value="2026-07-21"
        )
        self.require_login.start()
        self.lookup.start()
        self.today.start()
        self.addCleanup(self.require_login.stop)
        self.addCleanup(self.lookup.stop)
        self.addCleanup(self.today.stop)

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

    def _seed_submission(self, submission_id):
        pending_filename = f"pending-{submission_id}.pdf"
        (self.storage.pending_dir / pending_filename).write_bytes(
            self.valid_pdf_bytes(submission_id)
        )
        with self.session_factory() as session:
            session.add(
                SubmissionModel(
                    id=submission_id,
                    pdf_filename=f"{submission_id}.pdf",
                    pending_filename=pending_filename,
                    title="Reviewed Paper",
                    author_name="Reader",
                    author_email="reader@example.test",
                    author_school="Example School",
                    status="pending",
                    submitted_at="2026-07-21",
                    abstract="Review abstract",
                    keywords="review",
                    journal="",
                    category="science",
                    language="en",
                    submitted_by="reader@example.test",
                    original_filename="original.pdf",
                    ib_ee_data="",
                    is_ib_sample="",
                    is_anonymous="",
                    cp_data="",
                    ia_data="",
                    submitter_name="Reader",
                )
            )
            session.commit()

    def _last_flash(self):
        with self.client.session_transaction() as flask_session:
            return flask_session["_flashes"][-1]

    def test_lost_accept_response_retries_through_lifecycle_and_conflicts_stay_strict(self):
        submission_id = "route-accept-replay"
        self._seed_submission(submission_id)
        form = {"decision_idempotency_key": "accept-form-token"}

        first = self.client.post(
            f"/dashboard/review/{submission_id}/accept", data=form
        )
        replay = self.client.post(
            f"/dashboard/review/{submission_id}/accept", data=form
        )

        self.assertEqual((first.status_code, replay.status_code), (302, 302))
        self.assertEqual(
            [outcome.replayed for outcome in self.recording_lifecycle.outcomes],
            [False, True],
        )
        self.assertEqual(self._last_flash(), ("success", "Paper accepted and published."))
        with self.session_factory() as session:
            row = session.get(SubmissionModel, submission_id)
            self.assertEqual(row.decision_idempotency_key, "accept-form-token")

        different_token = self.client.post(
            f"/dashboard/review/{submission_id}/accept",
            data={"decision_idempotency_key": "different-accept-token"},
        )
        different_decision = self.client.post(
            f"/dashboard/review/{submission_id}/reject",
            data={
                "decision_idempotency_key": "accept-form-token",
                "comment": "No longer accept.",
            },
        )
        self.assertEqual(
            (different_token.status_code, different_decision.status_code),
            (303, 303),
        )

    def test_lost_reject_response_retries_exact_form_intent_and_conflicts_stay_strict(self):
        submission_id = "route-reject-replay"
        self._seed_submission(submission_id)
        form = {
            "decision_idempotency_key": "reject-form-token",
            "comment": "Needs revision.",
        }

        first = self.client.post(
            f"/dashboard/review/{submission_id}/reject", data=form
        )
        replay = self.client.post(
            f"/dashboard/review/{submission_id}/reject", data=form
        )

        self.assertEqual((first.status_code, replay.status_code), (302, 302))
        self.assertEqual(
            [outcome.replayed for outcome in self.recording_lifecycle.outcomes],
            [False, True],
        )
        self.assertEqual(self._last_flash(), ("info", "Paper rejected."))
        first_intent = self.recording_lifecycle.intents[0]
        self.assertEqual(
            (
                first_intent.idempotency_key,
                first_intent.feedback,
                first_intent.actor,
            ),
            (
                "reject-form-token",
                "Needs revision.",
                Actor("curator@example.test", 3),
            ),
        )
        with self.session_factory() as session:
            row = session.get(SubmissionModel, submission_id)
            self.assertEqual(row.decision_idempotency_key, "reject-form-token")

        different_token = self.client.post(
            f"/dashboard/review/{submission_id}/reject",
            data={
                "decision_idempotency_key": "different-reject-token",
                "comment": "Needs revision.",
            },
        )
        different_comment = self.client.post(
            f"/dashboard/review/{submission_id}/reject",
            data={
                "decision_idempotency_key": "reject-form-token",
                "comment": "Different feedback.",
            },
        )
        different_decision = self.client.post(
            f"/dashboard/review/{submission_id}/accept",
            data={"decision_idempotency_key": "reject-form-token"},
        )
        self.assertEqual(
            (
                different_token.status_code,
                different_comment.status_code,
                different_decision.status_code,
            ),
            (303, 303, 303),
        )


if __name__ == "__main__":
    unittest.main()
