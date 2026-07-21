"""Submissions: pending-paper CRUD on the submissions table."""
from db import db_session
from models import SubmissionModel
from services.submission_fence import lock_submission_creation_fence


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
    return SubmissionModel(**{
        model_name: values.get(external)
        for external, model_name in _EXTERNAL_TO_MODEL.items()
    })


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


def _write_submissions(subs):
    with db_session() as db:
        lock_submission_creation_fence(db)
        db.query(SubmissionModel).delete()
        for s in subs:
            db.add(SubmissionModel(
                id=s.get("id"),
                pdf_filename=s.get("pdf_filename"),
                pending_filename=s.get("pending_filename"),
                title=s.get("title"),
                author_name=s.get("author_name"),
                author_email=s.get("author_email"),
                author_school=s.get("author_school"),
                status=s.get("status"),
                submitted_at=s.get("submitted_at"),
                feedback=s.get("feedback"),
                abstract=s.get("abstract"),
                keywords=s.get("keywords"),
                journal=s.get("journal"),
                category=s.get("category"),
                language=s.get("language"),
                submitted_by=s.get("submitter"),
                original_filename=s.get("original_filename"),
                ib_ee_data=s.get("ib_ee_data"),
                is_ib_sample=s.get("is_ib_sample"),
                is_anonymous=s.get("is_anonymous"),
                cp_data=s.get("cp_data"),
                ia_data=s.get("ia_data"),
                paper_id=s.get("paper_id"),
                submitter_name=s.get("submitter_name"),
                reviewed_at=s.get("reviewed_at"),
                reviewer=s.get("reviewer"),
                comment=s.get("comment"),
                decision_idempotency_key=s.get("decision_idempotency_key"),
                decision_payload_hash=s.get("decision_payload_hash"),
            ))
        db.commit()


def _save_submission(sub):
    with db_session() as db:
        lock_submission_creation_fence(db)
        db.add(_submission_model(sub))
        db.commit()


def _delete_submission(
    sub_id,
    *,
    expected_submitter=None,
    pending_cleanup=None,
):
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
        ):
            return False
        if pending_cleanup is not None:
            pending_cleanup(submission.pending_filename or "")
        db.delete(submission)
        db.commit()
        return True


def _get_submission(sub_id):
    for s in _load_submissions():
        if s.get("id") == sub_id:
            return s
    return None


def _update_submission(sub_id, updates):
    with db_session() as db:
        lock_submission_creation_fence(db)
        submission = (
            db.query(SubmissionModel)
            .filter(SubmissionModel.id == sub_id)
            .with_for_update()
            .one_or_none()
        )
        if submission is None or submission.id != sub_id:
            return None
        for external, value in updates.items():
            model_name = _EXTERNAL_TO_MODEL.get(external)
            if model_name is not None and external != "id":
                setattr(submission, model_name, value)
        db.commit()
        return _submission_dict(submission)
