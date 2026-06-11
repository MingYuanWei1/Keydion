"""Submissions: pending-paper CRUD on the submissions table."""
from db import db_session
from models import SubmissionModel


def _load_submissions():
    with db_session() as db:
        subs = db.query(SubmissionModel).all()
        return [{
            "id": s.id,
            "pdf_filename": s.pdf_filename,
            "pending_filename": s.pending_filename,
            "title": s.title,
            "author_name": s.author_name,
            "author_email": s.author_email,
            "author_school": s.author_school,
            "status": s.status,
            "submitted_at": s.submitted_at,
            "feedback": s.feedback,
            "abstract": s.abstract,
            "keywords": s.keywords,
            "journal": s.journal,
            "category": s.category,
            "language": s.language,
            "submitter": s.submitted_by,
            "original_filename": s.original_filename,
            "ib_ee_data": s.ib_ee_data,
            "is_ib_sample": s.is_ib_sample,
            "cp_data": s.cp_data,
        } for s in subs]


def _write_submissions(subs):
    with db_session() as db:
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
                cp_data=s.get("cp_data"),
            ))
        db.commit()


def _save_submission(sub):
    subs = _load_submissions()
    subs.append(sub)
    _write_submissions(subs)


def _get_submission(sub_id):
    for s in _load_submissions():
        if s.get("id") == sub_id:
            return s
    return None


def _update_submission(sub_id, updates):
    subs = _load_submissions()
    for s in subs:
        if s.get("id") == sub_id:
            s.update(updates)
            _write_submissions(subs)
            return s
    return None
