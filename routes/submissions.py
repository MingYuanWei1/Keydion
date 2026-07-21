"""My-submissions + curator review routes."""
import io
from datetime import datetime, timezone
from uuid import uuid4

from flask import (
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from flask_babel import gettext as _

from config import (
    PENDING_PAPERS_DIR,
)
from services.auth import require_login
from services.publishing_contracts import (
    AcceptSubmission,
    CancelSubmission,
    IndexingState,
    LifecycleError,
    NormalizedPaperMetadata,
    NotFound,
    PdfUpload,
    RejectSubmission,
)
from routes.publishing_http import (
    actor_from_session,
    lifecycle_error_response,
    lifecycle_from_app,
)
from services.papers import _build_safe_paper_filename, resolve_contained
from services.submissions import (
    _delete_submission,
    _get_submission,
    _load_submissions,
    _update_submission,
)


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def register_routes(app):

    def stable_decision_key(sub_id, persisted_key=""):
        keys = dict(session.get("publishing_decision_keys", {}))
        key = persisted_key or keys.get(sub_id) or str(uuid4())
        keys[sub_id] = key
        session["publishing_decision_keys"] = keys
        return key

    def clear_decision_key(sub_id):
        keys = dict(session.get("publishing_decision_keys", {}))
        keys.pop(sub_id, None)
        session["publishing_decision_keys"] = keys

    # ---- Submission review routes ----

    @app.route("/dashboard/my-submissions")
    def my_submissions():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        username = user.get("username", "")
        subs = [s for s in _load_submissions() if s.get("submitter") == username]
        subs.sort(key=lambda s: s.get("submitted_at", ""), reverse=True)
        return render_template("my_submissions.html", user=user, submissions=subs)

    @app.route("/dashboard/my-submissions/<sub_id>/delete", methods=["POST"], endpoint="my_submission_delete")
    def delete_submission(sub_id):
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        username = user.get("username", "")
        submission = _get_submission(sub_id)
        if not submission or submission.get("submitter") != username:
            flash(_("Submission not found."), "warning")
            return redirect(url_for("my_submissions"))

        status = submission.get("status")
        if status == "pending":
            try:
                lifecycle_from_app().cancel_submission(
                    CancelSubmission(
                        actor=actor_from_session(),
                        submission_id=sub_id,
                    )
                )
            except LifecycleError as error:
                return lifecycle_error_response(
                    error,
                    redirect_endpoint="my_submissions",
                )
            flash(_("Submission cancelled."), "success")
            return redirect(url_for("my_submissions"))
        if status != "draft":
            flash(_("Reviewed submissions are permanent records."), "warning")
            return redirect(url_for("my_submissions"))

        def remove_pending(pending_file):
            if not pending_file:
                return
            pending_path = resolve_contained(
                PENDING_PAPERS_DIR,
                pending_file,
                must_exist=True,
            )
            if pending_path is not None:
                pending_path.unlink()

        # Authorization, exact row identity, and file cleanup all happen while
        # the shared creation fence and row lock exclude same-ID replacement.
        if not _delete_submission(
            sub_id,
            expected_submitter=username,
            expected_status="draft",
            pending_cleanup=remove_pending,
        ):
            flash(_("Submission not found."), "warning")
            return redirect(url_for("my_submissions"))
        flash(_("Submission deleted."), "success")
        return redirect(url_for("my_submissions"))

    @app.route("/dashboard/my-submissions/<sub_id>", endpoint="my_submission_view")
    def submission_detail(sub_id):
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("submitter") != user.get("username", ""):
            flash(_("Submission not found."), "warning")
            return redirect(url_for("my_submissions"))

        # Determine PDF URL based on status
        pdf_url = None
        if sub.get("status") in {"pending", "rejected"}:
            if resolve_contained(
                PENDING_PAPERS_DIR,
                sub.get("pending_filename", ""),
                must_exist=True,
            ) is not None:
                pdf_url = url_for("my_submission_file", sub_id=sub_id)
        elif sub.get("status") == "accepted":
            paper_id = sub.get("paper_id")
            if paper_id:
                try:
                    document = app.extensions["paper_library"].current_pdf(
                        paper_id
                    )
                except NotFound:
                    pass
                else:
                    pdf_url = url_for(
                        "paper_file",
                        paper_id=document.paper.paper_id,
                    )
        return render_template("submission_detail.html", user=user, submission=sub, pdf_url=pdf_url)

    @app.route("/dashboard/my-submissions/<sub_id>/file")
    def my_submission_file(sub_id):
        """Serve a pending paper file to the submitter only."""
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if (
            not sub
            or sub.get("submitter") != user.get("username", "")
            or sub.get("status") not in {"pending", "rejected"}
        ):
            abort(403)
        pending_filename = sub.get("pending_filename", "")
        if resolve_contained(PENDING_PAPERS_DIR, pending_filename, must_exist=True) is None:
            abort(404)
        return send_from_directory(str(PENDING_PAPERS_DIR), pending_filename)

    @app.route("/my-submissions", endpoint="my_submissions_legacy")
    def my_submissions_legacy():
        return redirect(url_for("my_submissions"), code=301)

    @app.route("/my-submissions/<sub_id>", endpoint="my_submission_view_legacy")
    def my_submission_view_legacy(sub_id):
        return redirect(url_for("my_submission_view", sub_id=sub_id), code=301)

    @app.route("/my-submissions/<sub_id>/file", endpoint="my_submission_file_legacy")
    def my_submission_file_legacy(sub_id):
        return redirect(url_for("my_submission_file", sub_id=sub_id), code=301)

    @app.route("/dashboard/review")
    def review_list():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        status_filter = request.args.get("status", "pending")
        subs = _load_submissions()
        if status_filter == "pending":
            subs = [s for s in subs if s.get("status") == "pending"]
        elif status_filter == "accepted":
            subs = [s for s in subs if s.get("status") == "accepted"]
        elif status_filter == "rejected":
            subs = [s for s in subs if s.get("status") == "rejected"]
        subs.sort(key=lambda s: s.get("submitted_at", ""), reverse=True)
        return render_template("review_list.html", user=user, submissions=subs, status_filter=status_filter)

    @app.route("/dashboard/review/<sub_id>", endpoint="review_paper")
    def review_detail(sub_id):
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        sub = _get_submission(sub_id)
        if not sub:
            flash(_("Submission not found."), "warning")
            return redirect(url_for("review_list"))
        decision_idempotency_key = stable_decision_key(
            sub_id,
            sub.get("decision_idempotency_key") or "",
        )
        pdf_url = url_for("pending_paper_file", filename=sub.get("pending_filename", ""))
        return render_template(
            "review_paper.html",
            user=user,
            submission=sub,
            pdf_url=pdf_url,
            decision_idempotency_key=decision_idempotency_key,
        )

    @app.route("/dashboard/review/<sub_id>/accept", methods=["POST"])
    def review_accept(sub_id):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("status") != "pending":
            flash(_("Submission not found or already reviewed."), "warning")
            return redirect(url_for("review_list"))

        filename = sub.get("pdf_filename") or sub.get("filename")
        if not filename:
            filename = _build_safe_paper_filename(
                sub.get("title", "paper"),
                sub.get("author_name", "author"),
            )
        intent = AcceptSubmission(
            actor=actor_from_session(),
            submission_id=sub_id,
            idempotency_key=stable_decision_key(
                sub_id,
                request.form.get("decision_idempotency_key", "").strip(),
            ),
            metadata=NormalizedPaperMetadata(
                filename=filename,
                title=sub.get("title", ""),
                journal=sub.get("journal", ""),
                category=sub.get("category", ""),
                language=sub.get("language", ""),
                keywords=sub.get("keywords", ""),
                abstract=sub.get("abstract", ""),
                author_name=sub.get("author_name", ""),
                author_email=sub.get("author_email", ""),
                author_school=sub.get("author_school", ""),
                published_at=_utc_today(),
                ib_ee_data=sub.get("ib_ee_data", ""),
                is_ib_sample=sub.get("is_ib_sample", ""),
                is_anonymous=sub.get("is_anonymous", ""),
                cp_data=sub.get("cp_data", ""),
                ia_data=sub.get("ia_data", ""),
            ),
            # Acceptance stages the authoritative pending object by Submission ID;
            # the upload record remains only for the shared validation contract.
            pdf=PdfUpload(filename=filename, stream=io.BytesIO()),
        )
        try:
            outcome = lifecycle_from_app().review_submission(intent)
        except LifecycleError as error:
            return lifecycle_error_response(
                error,
                redirect_endpoint="review_list",
            )
        if outcome.indexing and outcome.indexing.state is IndexingState.FAILED:
            flash(
                _(
                    "%(paper_name)s published successfully, but RAG indexing failed.",
                    paper_name=filename,
                ),
                "warning",
            )
        else:
            flash(_("Paper accepted and published."), "success")
        clear_decision_key(sub_id)
        return redirect(url_for("review_list"))

    @app.route("/dashboard/review/<sub_id>/reject", methods=["POST"])
    def review_reject(sub_id):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("status") != "pending":
            flash(_("Submission not found or already reviewed."), "warning")
            return redirect(url_for("review_list"))

        intent = RejectSubmission(
            actor=actor_from_session(),
            submission_id=sub_id,
            feedback=request.form.get("comment", "").strip(),
        )
        try:
            lifecycle_from_app().review_submission(intent)
        except LifecycleError as error:
            return lifecycle_error_response(
                error,
                redirect_endpoint="review_list",
            )
        clear_decision_key(sub_id)
        flash(_("Paper rejected."), "info")
        return redirect(url_for("review_list"))

    @app.route("/review", endpoint="review_list_legacy")
    def review_list_legacy():
        return redirect(url_for("review_list"), code=301)

    @app.route("/review/<sub_id>", endpoint="review_paper_legacy")
    def review_paper_legacy(sub_id):
        return redirect(url_for("review_paper", sub_id=sub_id), code=301)

    @app.route("/pending-papers/<path:filename>")
    def pending_paper_file(filename):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        if resolve_contained(PENDING_PAPERS_DIR, filename, must_exist=True) is None:
            abort(404)
        return send_from_directory(str(PENDING_PAPERS_DIR), filename)
