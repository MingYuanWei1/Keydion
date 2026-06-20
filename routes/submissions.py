"""My-submissions + curator review routes."""
import shutil
from datetime import datetime

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

import llm_client
import rag_index
from config import (
    PAPERS_DIR,
    PENDING_PAPERS_DIR,
)
from services.auth import require_login
from services.papers import (
    _build_safe_paper_filename,
    upsert_paper_metadata,
)
from services.submissions import (
    _get_submission,
    _load_submissions,
    _update_submission,
    _write_submissions,
)


def register_routes(app):

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
        sub = _get_submission(sub_id)
        if not sub or sub.get("submitter") != username:
            flash(_("Submission not found."), "warning")
            return redirect(url_for("my_submissions"))
        # Remove pending PDF file if it exists
        pending_file = sub.get("pending_filename", "")
        if pending_file:
            pending_path = PENDING_PAPERS_DIR / pending_file
            if pending_path.exists():
                pending_path.unlink()
        # Remove submission record
        subs = _load_submissions()
        subs = [s for s in subs if s.get("id") != sub_id]
        _write_submissions(subs)
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
        if sub.get("status") == "pending":
            pending_path = PENDING_PAPERS_DIR / sub.get("pending_filename", "")
            if pending_path.exists():
                pdf_url = url_for("my_submission_file", sub_id=sub_id)
        elif sub.get("status") == "accepted":
            filename = sub.get("filename", "")
            publish_path = PAPERS_DIR / filename
            if not publish_path.exists():
                # Try with sub_id prefix (collision avoidance)
                filename = f"{sub_id}_{sub.get('filename', '')}"
                publish_path = PAPERS_DIR / filename
            if publish_path.exists():
                pdf_url = url_for("paper_file", filename=filename)
        # rejected: file deleted, pdf_url stays None

        return render_template("submission_detail.html", user=user, submission=sub, pdf_url=pdf_url)

    @app.route("/dashboard/my-submissions/<sub_id>/file")
    def my_submission_file(sub_id):
        """Serve a pending paper file to the submitter only."""
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("submitter") != user.get("username", ""):
            abort(403)
        pending_filename = sub.get("pending_filename", "")
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
        pdf_url = url_for("pending_paper_file", filename=sub.get("pending_filename", ""))
        return render_template("review_paper.html", user=user, submission=sub, pdf_url=pdf_url)

    @app.route("/dashboard/review/<sub_id>/accept", methods=["POST"])
    def review_accept(sub_id):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("status") != "pending":
            flash(_("Submission not found or already reviewed."), "warning")
            return redirect(url_for("review_list"))

        # Move file from pending to published
        pending_path = PENDING_PAPERS_DIR / sub.get("pending_filename", "")
        filename = sub.get("pdf_filename") or sub.get("filename")
        if not filename:
            filename = _build_safe_paper_filename(
                sub.get("title", "paper"), sub.get("author_name", "author")
            )
        publish_path = PAPERS_DIR / filename
        if publish_path.exists():
            # Add sub_id prefix to avoid collision
            filename = f"{sub_id}_{filename}"
            publish_path = PAPERS_DIR / filename

        if pending_path.exists():
            shutil.move(str(pending_path), str(publish_path))

        # Save paper metadata
        today = datetime.utcnow().date().isoformat()
        upsert_paper_metadata(
            filename,
            {
                "title": sub.get("title", ""),
                "journal": sub.get("journal", ""),
                "category": sub.get("category", ""),
                "language": sub.get("language", ""),
                "keywords": sub.get("keywords", ""),
                "abstract": sub.get("abstract", ""),
                "author_name": sub.get("author_name", ""),
                "author_email": sub.get("author_email", ""),
                "author_school": sub.get("author_school", ""),
                "published_at": today,
                "ib_ee_data": sub.get("ib_ee_data", ""),
                "is_ib_sample": sub.get("is_ib_sample", ""),
                "is_anonymous": sub.get("is_anonymous", ""),
                "cp_data": sub.get("cp_data", ""),
                "ia_data": sub.get("ia_data", ""),
            },
        )

        reviewer_name = user.get("display_name", "") or user.get("first_name", "") or user.get("username", "")
        _update_submission(sub_id, {
            "status": "accepted",
            "reviewed_at": datetime.utcnow().isoformat(),
            "reviewer": reviewer_name,
        })
        flash(_("Paper accepted and published."), "success")
        try:
            if llm_client.llm_enabled():
                rag_index.build_index([filename])
        except Exception:
            app.logger.exception("Failed to index accepted paper")
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

        comment = request.form.get("comment", "").strip()

        # Remove the pending file
        pending_path = PENDING_PAPERS_DIR / sub.get("pending_filename", "")
        if pending_path.exists():
            pending_path.unlink()

        reviewer_name = user.get("display_name", "") or user.get("first_name", "") or user.get("username", "")
        _update_submission(sub_id, {
            "status": "rejected",
            "reviewed_at": datetime.utcnow().isoformat(),
            "reviewer": reviewer_name,
            "comment": comment,
        })
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
        return send_from_directory(str(PENDING_PAPERS_DIR), filename)
