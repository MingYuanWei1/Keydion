"""Durable two-kind queue and standalone publishing-worker primitives."""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from sqlalchemy import and_, or_

from models import (
    PaperMetadataModel,
    PaperRevisionModel,
    PublishingJobModel,
    SubmissionModel,
)
from services.publishing_contracts import JobLease, JobProgress, JobState
from services.publishing_time import require_db_utc


_LOG = logging.getLogger(__name__)
_ERROR_LIMIT = 500
_CREDENTIAL_PATTERNS = (
    re.compile(r"(?i)\bbearer\s+[^\s,;]+"),
    re.compile(
        r"(?i)\b(api[_-]?key|secret|token|password)\s*[:=]\s*[^\s,;]+"
    ),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]+"),
)


@dataclass(frozen=True)
class JobStatus:
    pending: int
    running: int
    oldest_age_seconds: int | None


def retry_delay_seconds(attempts: int, jitter: float) -> int:
    """Return the frozen exponential retry policy from the lifecycle design."""
    base = min(30 * (2 ** max(attempts - 1, 0)), 3600)
    return round(base * (1 + min(max(jitter, 0.0), 0.1)))


def redact_job_error(error: BaseException | str) -> str:
    """Render one bounded diagnostic after removing common credential forms."""
    if isinstance(error, BaseException):
        try:
            detail = str(error)
        except Exception:
            detail = "unprintable error"
        rendered = f"{type(error).__name__}: {detail}"
    else:
        rendered = str(error)
    for pattern in _CREDENTIAL_PATTERNS:
        rendered = pattern.sub(
            lambda match: (
                f"{match.group(1)}=[REDACTED]"
                if match.lastindex
                else "[REDACTED]"
            ),
            rendered,
        )
    return rendered[:_ERROR_LIMIT]


def _validated_claim_inputs(now: datetime, lease_seconds: int) -> None:
    require_db_utc(now)
    if (
        isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, int)
        or lease_seconds <= 0
    ):
        raise ValueError("lease_seconds must be a positive integer")


def _new_lease_token(factory: Callable[[], str]) -> str:
    value = factory()
    canonical = str(uuid.UUID(value))
    if value != canonical:
        raise ValueError("lease token factory must return a canonical UUID")
    return canonical


def _claim(
    session_factory,
    now: datetime,
    lease_seconds: int,
    lease_token_factory: Callable[[], str],
    *,
    job_id: str | None,
) -> JobLease | None:
    _validated_claim_inputs(now, lease_seconds)
    session = session_factory()
    try:
        due = or_(
            and_(
                PublishingJobModel.state == JobState.PENDING.value,
                PublishingJobModel.available_at <= now,
            ),
            and_(
                PublishingJobModel.state == JobState.RUNNING.value,
                PublishingJobModel.lease_expires_at.is_not(None),
                PublishingJobModel.lease_expires_at <= now,
            ),
        )
        query = session.query(PublishingJobModel).filter(due)
        if job_id is not None:
            query = query.filter(PublishingJobModel.id == job_id)
        job = (
            query.order_by(
                PublishingJobModel.available_at,
                PublishingJobModel.created_at,
                PublishingJobModel.id,
            )
            .with_for_update(skip_locked=True)
            .first()
        )
        if job is None:
            session.rollback()
            return None

        token = _new_lease_token(lease_token_factory)
        previous_updated_at = job.updated_at
        job.state = JobState.RUNNING.value
        job.attempts += 1
        job.lease_token = token
        job.lease_expires_at = now + timedelta(seconds=lease_seconds)
        job.updated_at = now
        lease = JobLease(
            job_id=job.id,
            paper_id=job.paper_id,
            revision=job.revision_number,
            kind=job.kind,
            attempts=job.attempts,
            lease_token=token,
            lease_expires_at=job.lease_expires_at,
            created_at=job.created_at,
            previous_updated_at=previous_updated_at,
        )
        session.commit()
        return lease
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def claim_one_due(
    session_factory,
    now: datetime,
    lease_seconds: int,
    lease_token_factory: Callable[[], str],
) -> JobLease | None:
    """Atomically lease the oldest due or lease-expired queue row."""
    return _claim(
        session_factory,
        now,
        lease_seconds,
        lease_token_factory,
        job_id=None,
    )


def claim_job_id(
    session_factory,
    job_id: str,
    now: datetime,
    lease_seconds: int,
    lease_token_factory: Callable[[], str],
) -> JobLease | None:
    """Lease one requested row only when it satisfies the normal due rule."""
    return _claim(
        session_factory,
        now,
        lease_seconds,
        lease_token_factory,
        job_id=job_id,
    )


def _job_matches_lease(job, lease: JobLease) -> bool:
    return bool(
        isinstance(lease, JobLease)
        and job is not None
        and job.id == lease.job_id
        and job.kind == lease.kind
        and job.paper_id == lease.paper_id
        and job.revision_number == lease.revision
        and job.state == JobState.RUNNING.value
        and job.attempts == lease.attempts
        and job.lease_token == lease.lease_token
        and job.lease_expires_at is not None
    )


def _progress_from_job(job) -> JobProgress:
    return JobProgress(
        job_id=job.id,
        paper_id=job.paper_id,
        revision=job.revision_number,
        state=JobState(job.state),
        attempts=job.attempts,
        next_retry_at=(
            job.available_at if job.state == JobState.PENDING.value else None
        ),
    )


def release_failed_job(
    session_factory,
    lease: JobLease,
    error: BaseException | str,
    now: datetime,
    *,
    jitter: float,
    logger: logging.Logger | None = None,
) -> JobProgress | None:
    """Release only a still-owned unexpired lease; stale authority is a no-op."""
    require_db_utc(now)
    redacted = redact_job_error(error)
    logger = logger or _LOG
    session = session_factory()
    try:
        # Lifecycle finalizers lock in Paper -> job order.  Keep that order here.
        paper = (
            session.query(PaperMetadataModel)
            .filter(PaperMetadataModel.id == lease.paper_id)
            .with_for_update()
            .one_or_none()
        )
        job = (
            session.query(PublishingJobModel)
            .filter(PublishingJobModel.id == lease.job_id)
            .with_for_update()
            .one_or_none()
        )
        if (
            not _job_matches_lease(job, lease)
            or job.lease_expires_at <= now
        ):
            session.rollback()
            return None

        if lease.kind == "index_revision" and (
            paper is None
            or paper.lifecycle_state != "published"
            or paper.current_revision != lease.revision
        ):
            session.delete(job)
            session.commit()
            return JobProgress(
                job_id=lease.job_id,
                paper_id=lease.paper_id,
                revision=lease.revision,
                state=JobState.RUNNING,
                attempts=lease.attempts,
            )

        delay = retry_delay_seconds(lease.attempts, float(jitter))
        retry_at = now + timedelta(seconds=delay)
        job.state = JobState.PENDING.value
        job.available_at = retry_at
        job.lease_token = None
        job.lease_expires_at = None
        job.last_error = redacted
        job.updated_at = now
        if (
            lease.kind == "index_revision"
            and paper is not None
            and paper.lifecycle_state == "published"
            and paper.current_revision == lease.revision
        ):
            paper.index_status = "failed"
            paper.indexed_revision = None
            paper.index_error = redacted
        progress = _progress_from_job(job)
        session.commit()
        logger.warning(
            "publishing job failed job_id=%s paper_id=%s revision=%s error=%s",
            lease.job_id,
            lease.paper_id,
            lease.revision,
            redacted,
        )
        return progress
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def job_warning_due(lease: JobLease, now: datetime) -> bool:
    """Gate attempt/age warnings without adding queue schema state."""
    require_db_utc(now)
    if lease.attempts == 3:
        return True
    age = max((now - lease.created_at).total_seconds(), 0.0)
    previous_age = max(
        (lease.previous_updated_at - lease.created_at).total_seconds(),
        0.0,
    )
    if age >= 900 and previous_age < 900:
        return True
    return age >= 900 and int(age // 3600) > int(previous_age // 3600)


def job_status(session_factory, now: datetime) -> JobStatus:
    """Read queue counts and oldest age without locks or mutations."""
    require_db_utc(now)
    session = session_factory()
    try:
        rows = session.query(
            PublishingJobModel.state,
            PublishingJobModel.created_at,
        ).all()
        pending = sum(state == JobState.PENDING.value for state, _ in rows)
        running = sum(state == JobState.RUNNING.value for state, _ in rows)
        oldest = min((created_at for _, created_at in rows), default=None)
        age = None if oldest is None else max(int((now - oldest).total_seconds()), 0)
        return JobStatus(pending=pending, running=running, oldest_age_seconds=age)
    finally:
        session.close()


def run_one_due_job(
    lifecycle,
    *,
    lease_seconds: int,
    lease_token_factory: Callable[[], str] | None = None,
    logger: logging.Logger | None = None,
) -> JobProgress | None:
    """Claim, commit, then dispatch at most one lifecycle job."""
    logger = logger or _LOG
    lease = claim_one_due(
        lifecycle._session_factory,
        lifecycle._clock(),
        lease_seconds,
        lease_token_factory or lifecycle._uuid_factory,
    )
    if lease is None:
        return None
    if job_warning_due(lease, lifecycle._clock()):
        logger.warning(
            "publishing job delayed job_id=%s paper_id=%s revision=%s attempts=%s",
            lease.job_id,
            lease.paper_id,
            lease.revision,
            lease.attempts,
        )
    return lifecycle._recover_claimed(
        lease,
        lifecycle._monotonic_clock() + lease_seconds,
    )


def _log_reconcile_error(
    logger: logging.Logger,
    *,
    candidate: str,
    error: BaseException,
) -> None:
    logger.warning(
        "publishing reconciliation failed candidate=%s error=%s",
        candidate,
        redact_job_error(error),
    )


def reconcile_stale_publications(
    lifecycle,
    *,
    grace_seconds: int,
    logger: logging.Logger | None = None,
) -> int:
    """Best-effort storage-first recovery for stale private lifecycle residue."""
    if (
        isinstance(grace_seconds, bool)
        or not isinstance(grace_seconds, int)
        or grace_seconds <= 0
    ):
        raise ValueError("grace_seconds must be a positive integer")
    logger = logger or _LOG
    now = lifecycle._clock()
    require_db_utc(now)
    cutoff = now - timedelta(seconds=grace_seconds)
    reconciled = 0

    with lifecycle._session() as session:
        stale = session.query(
            PaperMetadataModel.id,
            PaperMetadataModel.origin_submission_id,
        ).filter(
            PaperMetadataModel.lifecycle_state == "publishing",
            PaperMetadataModel.current_revision.is_(None),
            PaperMetadataModel.reservation_expires_at.is_not(None),
            PaperMetadataModel.reservation_expires_at <= now,
        ).order_by(PaperMetadataModel.id).all()

    for paper_id, submission_id in stale:
        try:
            with lifecycle._session() as session:
                submission = None
                if submission_id is not None:
                    submission = lifecycle._submission_by_id(
                        session,
                        submission_id,
                        locked=True,
                    )
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                if (
                    paper is None
                    or paper.lifecycle_state != "publishing"
                    or paper.current_revision is not None
                    or paper.reservation_expires_at is None
                    or paper.reservation_expires_at > now
                ):
                    continue
                if not lifecycle._remove_expired_reservation_locked(session, paper):
                    continue
                if submission is not None and submission.status == "pending":
                    submission.paper_id = None
                    submission.reviewed_at = None
                    submission.reviewer = None
                    submission.comment = None
                    submission.decision_idempotency_key = None
                    submission.decision_payload_hash = None
                session.commit()
                reconciled += 1
        except Exception as exc:
            _log_reconcile_error(
                logger,
                candidate=f"paper:{paper_id}",
                error=exc,
            )

    try:
        reconciled += lifecycle.reconcile_submissions(
            on_error=lambda candidate, error: _log_reconcile_error(
                logger,
                candidate=candidate,
                error=error,
            )
        )
    except Exception as exc:
        _log_reconcile_error(logger, candidate="submissions", error=exc)

    try:
        with lifecycle._session() as session:
            referenced = tuple(
                session.query(
                    PaperRevisionModel.paper_id,
                    PaperRevisionModel.revision_number,
                )
                .order_by(
                    PaperRevisionModel.paper_id,
                    PaperRevisionModel.revision_number,
                )
                .all()
            )
        reconciled += lifecycle._storage.reconcile_expired(cutoff, referenced)
    except Exception as exc:
        _log_reconcile_error(logger, candidate="paper-storage", error=exc)
    return reconciled


class PublishingWorker:
    """Dependency-injected worker facade used by the CLI and fast tests."""

    def __init__(
        self,
        *,
        lifecycle,
        session_factory,
        clock: Callable[[], datetime],
        monotonic_clock: Callable[[], float],
        lease_token_factory: Callable[[], str],
        jitter: Callable[[], float],
        lease_seconds: int,
        reservation_grace_seconds: int,
        poll_seconds: int,
        logger: logging.Logger | None = None,
    ):
        self.lifecycle = lifecycle
        self.session_factory = session_factory
        self.clock = clock
        self.monotonic_clock = monotonic_clock
        self.lease_token_factory = lease_token_factory
        self.jitter = jitter
        self.lease_seconds = lease_seconds
        self.reservation_grace_seconds = reservation_grace_seconds
        self.poll_seconds = poll_seconds
        self.logger = logger or _LOG

    def reconcile(self) -> int:
        try:
            return reconcile_stale_publications(
                self.lifecycle,
                grace_seconds=self.reservation_grace_seconds,
                logger=self.logger,
            )
        except Exception as exc:
            _log_reconcile_error(self.logger, candidate="pass", error=exc)
            return 0

    def run_one(self) -> JobProgress | None:
        return run_one_due_job(
            self.lifecycle,
            lease_seconds=self.lease_seconds,
            lease_token_factory=self.lease_token_factory,
            logger=self.logger,
        )

    def status(self) -> JobStatus:
        return job_status(self.session_factory, self.clock())
