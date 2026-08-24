"""Durable extraction and embedding jobs for Keydion AI attachments."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import func

import rag_index
from db import db_session
from models import AttachmentChunkModel, AttachmentJobModel, ConversationModel
from services.attachment_processing import extract_in_subprocess, preflight_docx


_LOG = logging.getLogger(__name__)
MAX_ATTEMPTS = 3
LEASE_SECONDS = 180

# Aggregate admission budgets for attachment work (security finding: one small
# upload could fan out into thousands of chunks and paid embedding calls, and
# unbounded distinct uploads filled durable queue storage). Each upload is
# still bounded per-file by MAX_ATTACH_BYTES; these cap the accumulation.
MAX_ACTIVE_JOBS_PER_CONVERSATION = 8            # queued + running jobs
MAX_QUEUED_BYTES_PER_CONVERSATION = 20 * 1024 * 1024
MAX_QUEUED_BYTES_PER_OWNER = 50 * 1024 * 1024
MAX_CHUNKS_PER_ATTACHMENT = 1500                # caps chunk/embedding fan-out


class AttachmentQuotaExceeded(Exception):
    """An attachment budget is exhausted; the upload was refused."""


@dataclass(frozen=True)
class AttachmentJobStatus:
    id: str
    filename: str
    state: str


def _queued_payload_bytes(query) -> int:
    """Total payload bytes held by the given job query (NULL payloads ignored)."""
    total = query.with_entities(
        func.coalesce(func.sum(func.length(AttachmentJobModel.payload)), 0)
    ).scalar()
    return int(total or 0)


def _enforce_attachment_quota(database, conversation_id: int, incoming_bytes: int) -> None:
    """Refuse work past per-conversation and per-owner attachment budgets."""
    active_states = ("queued", "running")
    active_jobs = (
        database.query(AttachmentJobModel)
        .filter(
            AttachmentJobModel.conversation_id == conversation_id,
            AttachmentJobModel.state.in_(active_states),
        )
        .count()
    )
    if active_jobs >= MAX_ACTIVE_JOBS_PER_CONVERSATION:
        raise AttachmentQuotaExceeded("too many attachments pending in this conversation")

    conversation_bytes = _queued_payload_bytes(
        database.query(AttachmentJobModel).filter(
            AttachmentJobModel.conversation_id == conversation_id,
            AttachmentJobModel.state.in_(active_states),
        )
    )
    if conversation_bytes + incoming_bytes > MAX_QUEUED_BYTES_PER_CONVERSATION:
        raise AttachmentQuotaExceeded("attachment storage limit reached for this conversation")

    owner_key = (
        database.query(ConversationModel.owner_key)
        .filter(ConversationModel.id == conversation_id)
        .scalar()
    )
    if owner_key:
        owner_bytes = _queued_payload_bytes(
            database.query(AttachmentJobModel)
            .join(
                ConversationModel,
                ConversationModel.id == AttachmentJobModel.conversation_id,
            )
            .filter(
                ConversationModel.owner_key == owner_key,
                AttachmentJobModel.state.in_(active_states),
            )
        )
        if owner_bytes + incoming_bytes > MAX_QUEUED_BYTES_PER_OWNER:
            raise AttachmentQuotaExceeded("attachment storage limit reached for this account")


def queue_attachment(conversation_id: int, filename: str, raw: bytes) -> str:
    if filename.lower().endswith(".docx"):
        preflight_docx(raw)
    now = datetime.utcnow()
    job_id = str(uuid4())
    with db_session() as database:
        # A newer upload of the same display name supersedes queued work.
        old_jobs = database.query(AttachmentJobModel).filter(
            AttachmentJobModel.conversation_id == conversation_id,
            AttachmentJobModel.filename == filename,
            AttachmentJobModel.state.in_(("queued", "running")),
        )
        for old in old_jobs:
            old.state = "canceled"
            old.payload = None
            old.updated_at = now
        # Admission budgets run after supersede (so re-uploading the same name
        # never counts the job it replaces) and before the payload is stored.
        _enforce_attachment_quota(database, conversation_id, len(raw))
        database.add(
            AttachmentJobModel(
                id=job_id,
                conversation_id=conversation_id,
                filename=filename,
                payload=raw,
                state="queued",
                attempts=0,
                available_at=now,
                lease_token=None,
                lease_expires_at=None,
                last_error=None,
                created_at=now,
                updated_at=now,
            )
        )
    return job_id


def cancel_attachment(conversation_id: int, filename: str) -> None:
    now = datetime.utcnow()
    with db_session() as database:
        database.query(AttachmentChunkModel).filter(
            AttachmentChunkModel.conversation_id == conversation_id,
            AttachmentChunkModel.filename == filename,
        ).delete(synchronize_session=False)
        for job in database.query(AttachmentJobModel).filter(
            AttachmentJobModel.conversation_id == conversation_id,
            AttachmentJobModel.filename == filename,
            AttachmentJobModel.state.in_(("queued", "running")),
        ):
            job.state = "canceled"
            job.payload = None
            job.updated_at = now


def attachment_job_status_for_owner(
    job_id: str,
    owner_key: str,
) -> AttachmentJobStatus | None:
    with db_session() as database:
        job = (
            database.query(AttachmentJobModel)
            .join(
                ConversationModel,
                ConversationModel.id == AttachmentJobModel.conversation_id,
            )
            .filter(
                AttachmentJobModel.id == job_id,
                ConversationModel.owner_key == owner_key,
            )
            .one_or_none()
        )
        if job is None:
            return None
        return AttachmentJobStatus(job.id, job.filename, job.state)


def delete_conversation_attachment_jobs(conversation_id: int, database) -> None:
    database.query(AttachmentJobModel).filter(
        AttachmentJobModel.conversation_id == conversation_id
    ).delete(synchronize_session=False)


def _claim_one():
    now = datetime.utcnow()
    lease_token = str(uuid4())
    with db_session() as database:
        database.query(AttachmentJobModel).filter(
            AttachmentJobModel.state == "running",
            AttachmentJobModel.lease_expires_at <= now,
        ).update(
            {
                AttachmentJobModel.state: "queued",
                AttachmentJobModel.lease_token: None,
                AttachmentJobModel.lease_expires_at: None,
                AttachmentJobModel.available_at: now,
                AttachmentJobModel.updated_at: now,
            },
            synchronize_session=False,
        )
        query = (
            database.query(AttachmentJobModel)
            .filter(
                AttachmentJobModel.state == "queued",
                AttachmentJobModel.available_at <= now,
            )
            .order_by(
                AttachmentJobModel.available_at,
                AttachmentJobModel.created_at,
                AttachmentJobModel.id,
            )
        )
        if database.bind.dialect.name == "mysql":
            query = query.with_for_update(skip_locked=True)
        else:
            query = query.with_for_update()
        job = query.first()
        if job is None:
            return None
        job.state = "running"
        job.attempts += 1
        job.lease_token = lease_token
        job.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
        job.updated_at = now
        database.flush()
        return (
            job.id,
            lease_token,
            job.conversation_id,
            job.filename,
            bytes(job.payload or b""),
            job.attempts,
        )


def _finish_success(claim, chunks, vectors) -> None:
    job_id, lease_token, conversation_id, filename, _raw, _attempts = claim
    now = datetime.utcnow()
    with db_session() as database:
        job = database.query(AttachmentJobModel).filter(
            AttachmentJobModel.id == job_id,
            AttachmentJobModel.state == "running",
            AttachmentJobModel.lease_token == lease_token,
        ).with_for_update().one_or_none()
        if job is None:
            return
        database.query(AttachmentChunkModel).filter(
            AttachmentChunkModel.conversation_id == conversation_id,
            AttachmentChunkModel.filename == filename,
        ).delete(synchronize_session=False)
        created_at = now.isoformat()
        for index, content in enumerate(chunks):
            database.add(
                AttachmentChunkModel(
                    conversation_id=conversation_id,
                    filename=filename,
                    chunk_index=index,
                    content=content,
                    embedding=json.dumps(vectors[index]),
                    created_at=created_at,
                )
            )
        job.state = "succeeded"
        job.payload = None
        job.lease_token = None
        job.lease_expires_at = None
        job.last_error = None
        job.updated_at = now


def _finish_failure(claim, error: Exception) -> None:
    job_id, lease_token, _conversation_id, _filename, _raw, attempts = claim
    now = datetime.utcnow()
    with db_session() as database:
        job = database.query(AttachmentJobModel).filter(
            AttachmentJobModel.id == job_id,
            AttachmentJobModel.state == "running",
            AttachmentJobModel.lease_token == lease_token,
        ).with_for_update().one_or_none()
        if job is None:
            return
        terminal = attempts >= MAX_ATTEMPTS
        job.state = "failed" if terminal else "queued"
        job.available_at = now + timedelta(seconds=min(2 ** attempts, 60))
        job.lease_token = None
        job.lease_expires_at = None
        job.last_error = type(error).__name__[:255]
        job.updated_at = now
        if terminal:
            job.payload = None
    _LOG.warning(
        "attachment job failed job_id=%s attempt=%s error_type=%s",
        job_id,
        attempts,
        type(error).__name__,
    )


def run_one() -> bool:
    claim = _claim_one()
    if claim is None:
        return False
    try:
        text = extract_in_subprocess(claim[3], claim[4])
        # Cap the chunk fan-out so one upload cannot expand into unbounded
        # embedding calls and persisted vector rows.
        chunks = rag_index.chunk_text(text)[:MAX_CHUNKS_PER_ATTACHMENT]
        if not chunks:
            raise ValueError("no readable attachment text")
        vectors = rag_index.embed_texts(chunks)
        if len(vectors) != len(chunks):
            raise ValueError("embedding count mismatch")
        _finish_success(claim, chunks, vectors)
    except Exception as exc:
        _finish_failure(claim, exc)
    return True


def queue_status() -> tuple[int, int]:
    with db_session() as database:
        queued = database.query(AttachmentJobModel).filter(
            AttachmentJobModel.state == "queued"
        ).count()
        running = database.query(AttachmentJobModel).filter(
            AttachmentJobModel.state == "running"
        ).count()
        return queued, running
