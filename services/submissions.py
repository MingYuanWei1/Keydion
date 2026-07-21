"""Submissions: pending-paper CRUD on the submissions table."""
from db import db_session
from models import SubmissionModel
from services.submission_fence import lock_submission_creation_fence
from services.papers import set_pdf_metadata


_EXTERNAL_TO_MODEL = {
    "submitter": "submitted_by",
    **{
        name: name
        for name in (
            "id", "pdf_filename", "pending_filename", "title", "author_name",
            "author_email", "author_school", "status", "submitted_at", "feedback",
            "abstract", "keywords", "journal", "category", "language",
            "original_filename", "ib_ee_data", "is_ib_sample", "is_anonymous",
            "cp_data", "ia_data", "paper_id", "submitter_name", "reviewed_at",
            "reviewer", "comment", "decision_idempotency_key",
            "decision_payload_hash",
        )
    },
}


def _submission_dict(row):
    return {
        external: getattr(row, model_name)
        for external, model_name in _EXTERNAL_TO_MODEL.items()
    }


def _submission_model(values):
    model_values = {
        model_name: values.get(external)
        for external, model_name in _EXTERNAL_TO_MODEL.items()
    }
    if model_values.get("reviewed_at") == "":
        model_values["reviewed_at"] = None
    return SubmissionModel(**model_values)


def _load_submissions():
    with db_session() as db:
        subs = db.query(SubmissionModel).all()
        return [{
            "id": row.id,
            "pdf_filename": row.pdf_filename,
            "pending_filename": row.pending_filename,
            "title": row.title,
            "author_name": row.author_name,
            "author_email": row.author_email,
            "author_school": row.author_school,
            "status": row.status,
            "submitted_at": row.submitted_at,
            "feedback": row.feedback,
            "abstract": row.abstract,
            "keywords": row.keywords,
            "journal": row.journal,
            "category": row.category,
            "language": row.language,
            "submitter": row.submitted_by,
            "original_filename": row.original_filename,
            "ib_ee_data": row.ib_ee_data,
            "is_ib_sample": row.is_ib_sample,
            "is_anonymous": row.is_anonymous,
            "cp_data": row.cp_data,
            "ia_data": row.ia_data,
            "paper_id": row.paper_id,
            "submitter_name": row.submitter_name,
            "reviewed_at": row.reviewed_at,
            "reviewer": row.reviewer,
            "comment": row.comment,
            "decision_idempotency_key": row.decision_idempotency_key,
            "decision_payload_hash": row.decision_payload_hash,
        } for row in subs]


def _save_submission(sub, *, pending_write=None, pending_cleanup_on_failure=None):
    attempted_write = False
    try:
        with db_session() as db:
            lock_submission_creation_fence(db)
            db.add(_submission_model(sub))
            db.flush()
            if pending_write is not None:
                attempted_write = True
                pending_write()
    except Exception:
        if attempted_write and pending_cleanup_on_failure is not None:
            pending_cleanup_on_failure()
        raise


def _store_pending_submission_pdf(upload, pending_path, *, title, author):
    """Persist Reader intake bytes in the private Submission namespace."""
    upload.save(pending_path)
    set_pdf_metadata(pending_path, title, author)


def _delete_submission(
    sub_id,
    *,
    expected_submitter=None,
    expected_status=None,
    pending_cleanup=None,
):
    pending_filename = ""
    with db_session() as db:
        lock_submission_creation_fence(db)
        submission = (
            db.query(SubmissionModel)
            .filter(SubmissionModel.id == sub_id)
            .with_for_update()
            .one_or_none()
        )
        if (
            submission is None
            or submission.id != sub_id
            or (
                expected_submitter is not None
                and submission.submitted_by != expected_submitter
            )
            or (
                expected_status is not None
                and submission.status != expected_status
            )
        ):
            return False
        pending_filename = submission.pending_filename or ""
        db.delete(submission)

    if pending_cleanup is None:
        return True

    # The row deletion is durable before any irreversible file unlink.  Fence
    # the no-row check and cleanup as a second transaction so a same-ID writer
    # either wins in the gap (and protects its namespace) or waits until the
    # old pending object has been removed.
    with db_session() as db:
        lock_submission_creation_fence(db)
        replacement = (
            db.query(SubmissionModel)
            .filter(SubmissionModel.id == sub_id)
            .with_for_update()
            .one_or_none()
        )
        if replacement is not None and replacement.id == sub_id:
            return True
        pending_cleanup(pending_filename)
    return True


def _get_submission(sub_id):
    with db_session() as db:
        submission = (
            db.query(SubmissionModel)
            .filter(SubmissionModel.id == sub_id)
            .one_or_none()
        )
        if submission is None or submission.id != sub_id:
            return None
        return _submission_dict(submission)


def _update_submission(
    sub_id,
    updates,
    *,
    expected_submitter=None,
    expected_status=None,
    pending_write=None,
    pending_cleanup_on_failure=None,
):
    attempted_write = False
    try:
        with db_session() as db:
            lock_submission_creation_fence(db)
            submission = (
                db.query(SubmissionModel)
                .filter(SubmissionModel.id == sub_id)
                .with_for_update()
                .one_or_none()
            )
            if (
                submission is None
                or submission.id != sub_id
                or (
                    expected_submitter is not None
                    and submission.submitted_by != expected_submitter
                )
                or (
                    expected_status is not None
                    and submission.status != expected_status
                )
            ):
                return None
            for external, value in updates.items():
                model_name = _EXTERNAL_TO_MODEL.get(external)
                if model_name is not None and external != "id":
                    setattr(submission, model_name, value)
            db.flush()
            if pending_write is not None:
                attempted_write = True
                pending_write()
            result = _submission_dict(submission)
        return result
    except Exception:
        if attempted_write and pending_cleanup_on_failure is not None:
            pending_cleanup_on_failure()
        raise
