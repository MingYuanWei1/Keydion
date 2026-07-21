"""Durable publishing queue, retry, recovery, and reconciliation tests."""

from __future__ import annotations

import hashlib
import io
import logging
import os
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import get_type_hints
from unittest import mock

from models import (
    PaperMetadataModel,
    PaperRevisionModel,
    PublishingJobModel,
    RagIndexMetaModel,
    SubmissionModel,
)
from services.paper_storage import StorageError
from services.publishing_contracts import (
    Actor,
    DirectPublish,
    IndexingOutcome,
    IndexingState,
    JobLease,
    JobState,
    NormalizedPaperMetadata,
    NotFound,
    PdfUpload,
    PublishingLifecyclePort,
    StorageFailed,
)
from services.publishing_jobs import (
    PublishingWorker,
    claim_job_id,
    claim_one_due,
    job_status,
    job_warning_due,
    reconcile_stale_publications,
    redact_job_error,
    release_failed_job,
    retry_delay_seconds,
    run_one_due_job,
)
from tests.publishing_support import FakeRevisionIndexer, PublishingLifecycleTestCase


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record):
        self.messages.append(record.getMessage())


class PublishingJobTests(PublishingLifecycleTestCase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.paper_number = 0

    def direct_intent(self, *, filename: str | None = None, key: str | None = None):
        self.paper_number += 1
        number = self.paper_number
        return DirectPublish(
            actor=Actor("worker-test-contributor", 2),
            idempotency_key=key or f"worker-publish-{number}",
            metadata=NormalizedPaperMetadata(
                filename=filename or f"worker-paper-{number}.pdf",
                title=f"Worker Paper {number}",
                journal="Journal",
                category="science",
                language="en",
                author_name="Worker Author",
            ),
            pdf=PdfUpload(
                "source.pdf",
                io.BytesIO(self.valid_pdf_bytes(f"worker-{number}")),
            ),
        )

    def publish_without_job(self):
        lifecycle = self.new_lifecycle(FakeRevisionIndexer(enabled=False))
        return lifecycle.publish_direct(self.direct_intent())

    def seed_paper(
        self,
        *,
        paper_id: str | None = None,
        lifecycle_state: str = "published",
        current_revision: int | None = 1,
        index_status: str = "pending",
        indexed_revision: int | None = None,
        index_error: str | None = None,
        reservation_expires_at: datetime | None = None,
        origin_submission_id: str | None = None,
    ) -> str:
        paper_id = paper_id or self.uuid_factory()
        filename = f"{paper_id}.pdf"
        with self.session_factory() as session:
            session.add(
                PaperMetadataModel(
                    id=paper_id,
                    filename=filename,
                    title="Seed Paper",
                    language="en",
                    lifecycle_state=lifecycle_state,
                    current_revision=current_revision,
                    row_version=1 if current_revision is not None else 0,
                    index_status=index_status,
                    indexed_revision=indexed_revision,
                    index_error=index_error,
                    reservation_expires_at=reservation_expires_at,
                    origin_submission_id=origin_submission_id,
                )
            )
            if current_revision is not None:
                for revision in range(1, current_revision + 1):
                    session.add(
                        PaperRevisionModel(
                            paper_id=paper_id,
                            revision_number=revision,
                            sha256=str(revision) * 64,
                            size_bytes=revision,
                            created_at=self.now,
                            created_by="seed",
                        )
                    )
            session.commit()
        return paper_id

    def seed_job(
        self,
        *,
        paper_id: str | None = None,
        kind: str = "index_revision",
        revision: int = 1,
        state: str = "pending",
        attempts: int = 0,
        available_at: datetime | None = None,
        lease_token: str | None = None,
        lease_expires_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        job_id: str | None = None,
    ) -> PublishingJobModel:
        paper_id = paper_id or self.seed_paper()
        job_id = job_id or self.uuid_factory()
        created_at = created_at or self.now - timedelta(minutes=1)
        job = PublishingJobModel(
            id=job_id,
            kind=kind,
            paper_id=paper_id,
            revision_number=revision,
            dedupe_key=(
                f"delete:{paper_id}"
                if kind == "delete_paper"
                else f"{kind}:{paper_id}:{revision}"
            ),
            state=state,
            attempts=attempts,
            available_at=available_at or self.now,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
            last_error=None,
            created_at=created_at,
            updated_at=updated_at or created_at,
        )
        with self.session_factory() as session:
            session.add(job)
            session.commit()
        return job

    def job(self, job_id: str):
        with self.session_factory() as session:
            return session.get(PublishingJobModel, job_id)

    def stamp(self) -> int:
        with self.session_factory() as session:
            row = session.get(RagIndexMetaModel, "chunks_version")
            return 0 if row is None else row.value

    @staticmethod
    def lease_token(number: int) -> str:
        return f"aaaaaaaa-aaaa-4aaa-8aaa-{number:012d}"

    def claim(self, token_number=1, *, lease_seconds=1800):
        return claim_one_due(
            self.session_factory,
            self.now,
            lease_seconds,
            lease_token_factory=lambda: self.lease_token(token_number),
        )

    def test_protocol_uses_indexing_outcome_for_ensure_index_job(self):
        annotation = get_type_hints(
            PublishingLifecyclePort.ensure_index_job
        )["return"]
        self.assertIs(annotation, IndexingOutcome)

    def test_claim_leases_one_due_job_and_increments_attempts(self):
        job = self.seed_job(state="pending", available_at=self.now)
        claimed = self.claim()
        self.assertEqual(claimed.job_id, job.id)
        self.assertEqual(claimed.lease_token, self.lease_token(1))
        self.assertEqual(claimed.attempts, 1)
        persisted = self.job(job.id)
        self.assertEqual(persisted.state, "running")
        self.assertEqual(persisted.lease_expires_at, self.now + timedelta(seconds=1800))

    def test_expired_running_lease_is_reclaimed(self):
        job = self.seed_job(
            state="running",
            attempts=4,
            lease_token="crashed-worker",
            lease_expires_at=self.now - timedelta(seconds=1),
        )
        claimed = self.claim(token_number=2)
        self.assertEqual(claimed.job_id, job.id)
        self.assertEqual(claimed.lease_token, self.lease_token(2))
        self.assertEqual(claimed.attempts, 5)

    def test_claim_uses_exact_due_predicate_and_deterministic_order(self):
        future = self.seed_job(available_at=self.now + timedelta(seconds=1))
        running = self.seed_job(
            paper_id=self.seed_paper(),
            state="running",
            lease_token=self.lease_token(90),
            lease_expires_at=self.now + timedelta(seconds=1),
        )
        later_created = self.seed_job(
            paper_id=self.seed_paper(),
            available_at=self.now - timedelta(minutes=1),
            created_at=self.now - timedelta(minutes=2),
            job_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
        )
        winner = self.seed_job(
            paper_id=self.seed_paper(),
            available_at=self.now - timedelta(minutes=1),
            created_at=self.now - timedelta(minutes=3),
            job_id="00000000-0000-4000-8000-000000000099",
        )
        claimed = self.claim()
        self.assertEqual(claimed.job_id, winner.id)
        self.assertEqual(self.job(future.id).state, "pending")
        self.assertEqual(self.job(running.id).lease_token, self.lease_token(90))
        self.assertEqual(self.job(later_created.id).state, "pending")

    def test_claim_tiebreaks_equal_dates_by_job_id(self):
        common_created = self.now - timedelta(hours=1)
        high = self.seed_job(
            job_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
            available_at=self.now,
            created_at=common_created,
        )
        low = self.seed_job(
            paper_id=self.seed_paper(),
            job_id="00000000-0000-4000-8000-000000000001",
            available_at=self.now,
            created_at=common_created,
        )
        self.assertEqual(self.claim().job_id, low.id)
        self.assertEqual(self.job(high.id).state, "pending")

    def test_claim_emits_skip_locked_and_commits_before_return(self):
        self.seed_job()
        with mock.patch(
            "sqlalchemy.orm.Query.with_for_update",
            autospec=True,
            wraps=__import__("sqlalchemy").orm.Query.with_for_update,
        ) as locked:
            claimed = self.claim()
        self.assertIsNotNone(claimed)
        self.assertTrue(any(call.kwargs.get("skip_locked") is True for call in locked.mock_calls))
        self.assertEqual(self.job(claimed.job_id).state, "running")

    def test_no_due_job_returns_none(self):
        self.seed_job(available_at=self.now + timedelta(seconds=1))
        self.seed_job(
            paper_id=self.seed_paper(),
            state="running",
            lease_token=self.lease_token(3),
            lease_expires_at=self.now + timedelta(seconds=1),
        )
        self.assertIsNone(self.claim())

    def test_claim_job_id_obeys_same_due_rule_and_never_claims_a_different_job(self):
        due = self.seed_job()
        future = self.seed_job(
            paper_id=self.seed_paper(),
            available_at=self.now + timedelta(seconds=1),
        )
        self.assertIsNone(
            claim_job_id(
                self.session_factory,
                future.id,
                self.now,
                1800,
                lease_token_factory=lambda: self.lease_token(4),
            )
        )
        self.assertEqual(self.job(due.id).state, "pending")
        claimed = claim_job_id(
            self.session_factory,
            due.id,
            self.now,
            1800,
            lease_token_factory=lambda: self.lease_token(5),
        )
        self.assertEqual(claimed.job_id, due.id)

    def test_retry_delay_is_exponential_capped_and_jittered(self):
        self.assertEqual(retry_delay_seconds(attempts=1, jitter=0.0), 30)
        self.assertEqual(retry_delay_seconds(attempts=8, jitter=0.0), 3600)
        self.assertEqual(retry_delay_seconds(attempts=8, jitter=0.1), 3960)
        self.assertEqual(retry_delay_seconds(attempts=20, jitter=9.0), 3960)
        self.assertEqual(retry_delay_seconds(attempts=0, jitter=-1.0), 30)

    def test_redaction_removes_credentials_and_caps_persisted_text(self):
        error = RuntimeError(
            "Bearer abc.def API_KEY=provider-secret secret: hidden "
            "password=pw sk-live-credential " + ("x" * 1000)
        )
        redacted = redact_job_error(error)
        self.assertLessEqual(len(redacted), 500)
        for secret in (
            "abc.def",
            "provider-secret",
            "hidden",
            "password=pw",
            "sk-live-credential",
        ):
            self.assertNotIn(secret, redacted)

    def test_release_requires_exact_unexpired_id_token_and_attempt(self):
        seeded = self.seed_job()
        first = self.claim()
        self.now += timedelta(seconds=1801)
        second = self.claim(token_number=2)
        self.assertEqual(second.job_id, seeded.id)
        stale = release_failed_job(
            self.session_factory,
            first,
            RuntimeError("old worker"),
            self.now,
            jitter=0.0,
        )
        self.assertIsNone(stale)
        self.assertEqual(self.job(seeded.id).lease_token, second.lease_token)

        wrong_attempt = JobLease(
            job_id=second.job_id,
            paper_id=second.paper_id,
            revision=second.revision,
            kind=second.kind,
            attempts=second.attempts + 1,
            lease_token=second.lease_token,
            lease_expires_at=second.lease_expires_at,
            created_at=second.created_at,
            previous_updated_at=second.previous_updated_at,
        )
        self.assertIsNone(
            release_failed_job(
                self.session_factory,
                wrong_attempt,
                RuntimeError("wrong attempt"),
                self.now,
                jitter=0.0,
            )
        )
        self.assertEqual(self.job(seeded.id).state, "running")

    def test_expired_lease_cannot_release_even_with_matching_token(self):
        seeded = self.seed_job()
        lease = self.claim()
        self.now = lease.lease_expires_at
        self.assertIsNone(
            release_failed_job(
                self.session_factory,
                lease,
                RuntimeError("late failure"),
                self.now,
                jitter=0.0,
            )
        )
        self.assertEqual(self.job(seeded.id).state, "running")

    def test_mysql_datetime_zero_roundtrip_does_not_require_exact_expiry_equality(self):
        self.now = self.now.replace(microsecond=654321)
        seeded = self.seed_job(available_at=self.now)
        lease = self.claim()
        with self.session_factory() as session:
            job = session.get(PublishingJobModel, seeded.id)
            job.lease_expires_at = job.lease_expires_at.replace(microsecond=0)
            session.commit()
        progress = release_failed_job(
            self.session_factory,
            lease,
            RuntimeError("provider unavailable"),
            self.now,
            jitter=0.0,
        )
        self.assertEqual(progress.state, JobState.PENDING)

    def test_release_uses_exact_backoff_preserves_error_and_has_no_terminal_branch(self):
        paper_id = self.seed_paper(index_error="older failure")
        seeded = self.seed_job(paper_id=paper_id, attempts=7)
        lease = self.claim()
        progress = release_failed_job(
            self.session_factory,
            lease,
            RuntimeError("provider down secret=do-not-store"),
            self.now,
            jitter=0.1,
        )
        self.assertEqual(progress.state, JobState.PENDING)
        self.assertEqual(progress.attempts, 8)
        self.assertEqual(progress.next_retry_at, self.now + timedelta(seconds=3960))
        persisted = self.job(seeded.id)
        self.assertEqual(persisted.state, "pending")
        self.assertIsNone(persisted.lease_token)
        self.assertIsNone(persisted.lease_expires_at)
        self.assertNotIn("do-not-store", persisted.last_error)
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            self.assertEqual(paper.index_status, "failed")
            self.assertEqual(paper.index_error, persisted.last_error)

    def test_release_logs_only_redacted_text_without_exception_metadata(self):
        self.seed_job()
        lease = self.claim()
        logger = logging.getLogger(f"publishing-jobs-test-{id(self)}")
        logger.propagate = False
        logger.setLevel(logging.WARNING)
        handler = _ListHandler()
        logger.addHandler(handler)
        try:
            release_failed_job(
                self.session_factory,
                lease,
                RuntimeError("Bearer unredacted-token sk-secret-value"),
                self.now,
                jitter=0.0,
                logger=logger,
            )
        finally:
            logger.removeHandler(handler)
        rendered = "\n".join(handler.messages)
        self.assertNotIn("unredacted-token", rendered)
        self.assertNotIn("sk-secret-value", rendered)
        self.assertIn(lease.job_id, rendered)
        self.assertIn(lease.paper_id, rendered)

    def test_warning_thresholds_are_attempt_age_and_hour_bucket_based(self):
        base = self.seed_job(
            attempts=2,
            created_at=self.now - timedelta(minutes=5),
            updated_at=self.now - timedelta(minutes=1),
        )
        lease = self.claim()
        self.assertTrue(job_warning_due(lease, self.now))

        age_job = self.seed_job(
            paper_id=self.seed_paper(),
            attempts=0,
            created_at=self.now - timedelta(minutes=16),
            updated_at=self.now - timedelta(minutes=14),
        )
        age_lease = claim_job_id(
            self.session_factory,
            age_job.id,
            self.now,
            1800,
            lease_token_factory=lambda: self.lease_token(6),
        )
        self.assertTrue(job_warning_due(age_lease, self.now))

        same_bucket = JobLease(
            job_id=base.id,
            paper_id=lease.paper_id,
            revision=1,
            kind="index_revision",
            attempts=9,
            lease_token=self.lease_token(7),
            lease_expires_at=self.now + timedelta(minutes=30),
            created_at=self.now - timedelta(hours=2, minutes=30),
            previous_updated_at=self.now - timedelta(minutes=10),
        )
        self.assertFalse(job_warning_due(same_bucket, self.now))
        next_bucket = JobLease(
            **{
                **same_bucket.__dict__,
                "previous_updated_at": self.now - timedelta(minutes=31),
            }
        )
        self.assertTrue(job_warning_due(next_bucket, self.now))

    def test_job_status_is_read_only_and_uses_naive_utc_age(self):
        self.seed_job(created_at=self.now - timedelta(seconds=91))
        self.seed_job(
            paper_id=self.seed_paper(),
            state="running",
            lease_token=self.lease_token(8),
            lease_expires_at=self.now + timedelta(seconds=10),
            created_at=self.now - timedelta(seconds=31),
        )
        status = job_status(self.session_factory, self.now)
        self.assertEqual((status.pending, status.running), (1, 1))
        self.assertEqual(status.oldest_age_seconds, 91)
        self.assertEqual(self.job_status_rows(), ("pending", "running"))

    def job_status_rows(self):
        with self.session_factory() as session:
            return tuple(
                row.state
                for row in session.query(PublishingJobModel)
                .order_by(PublishingJobModel.created_at)
                .all()
            )

    def test_ensure_index_job_disabled_and_ready_have_no_job_states(self):
        published = self.publish_without_job()
        disabled = self.new_lifecycle(FakeRevisionIndexer(enabled=False))
        before_stamp = self.stamp()
        outcome = disabled.ensure_index_job(published.paper_id)
        self.assertEqual(outcome.state, IndexingState.NOT_REQUIRED)
        self.assertEqual(self.jobs(published.paper_id), [])
        self.assertEqual(self.stamp(), before_stamp)

        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, published.paper_id)
            paper.index_status = "ready"
            paper.indexed_revision = 1
            session.commit()
        enabled = self.new_lifecycle(FakeRevisionIndexer())
        outcome = enabled.ensure_index_job(published.paper_id)
        self.assertEqual(outcome.state, IndexingState.INDEXED)
        self.assertEqual(self.jobs(published.paper_id), [])
        self.assertEqual(self.stamp(), before_stamp)

    def test_ensure_index_job_forced_current_revision_dedupes_and_bumps_once(self):
        published = self.publish_without_job()
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, published.paper_id)
            paper.index_status = "ready"
            paper.indexed_revision = 1
            paper.index_error = "retain until successful replacement"
            session.commit()
        lifecycle = self.new_lifecycle(FakeRevisionIndexer())
        before_stamp = self.stamp()
        first = lifecycle.ensure_index_job(published.paper_id, revision=1)
        second = lifecycle.ensure_index_job(published.paper_id, revision=1)
        self.assertEqual(first.state, IndexingState.PENDING)
        self.assertEqual(second.job_id, first.job_id)
        self.assertEqual(len(self.jobs(published.paper_id)), 1)
        self.assertEqual(self.stamp(), before_stamp + 1)
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, published.paper_id)
            self.assertEqual(paper.index_status, "pending")
            self.assertEqual(paper.index_error, "retain until successful replacement")

    def test_ensure_index_job_preserves_failed_error_and_does_not_bump_without_state_change(self):
        published = self.publish_without_job()
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, published.paper_id)
            paper.index_status = "failed"
            paper.index_error = "redacted old failure"
            session.commit()
        lifecycle = self.new_lifecycle(FakeRevisionIndexer())
        before_stamp = self.stamp()
        outcome = lifecycle.ensure_index_job(published.paper_id)
        self.assertEqual(outcome.state, IndexingState.FAILED)
        self.assertEqual(self.job(outcome.job_id).last_error, "redacted old failure")
        self.assertEqual(self.stamp(), before_stamp)

    def test_ensure_index_job_rejects_missing_hidden_and_noncurrent_targets(self):
        lifecycle = self.new_lifecycle(FakeRevisionIndexer())
        with self.assertRaises(NotFound):
            lifecycle.ensure_index_job("00000000-0000-4000-8000-999999999999")
        hidden = self.seed_paper(
            lifecycle_state="publishing",
            current_revision=None,
            reservation_expires_at=self.now + timedelta(hours=1),
        )
        with self.assertRaises(NotFound):
            lifecycle.ensure_index_job(hidden)
        published = self.seed_paper(current_revision=2)
        with self.assertRaises(NotFound):
            lifecycle.ensure_index_job(published, revision=1)

    def test_index_dispatch_succeeds_and_removes_job(self):
        published = self.publish_without_job()
        indexer = FakeRevisionIndexer()
        lifecycle = self.new_lifecycle(indexer)
        queued = lifecycle.ensure_index_job(published.paper_id)
        progress = run_one_due_job(lifecycle, lease_seconds=1800)
        self.assertEqual(progress.job_id, queued.job_id)
        self.assertIsNone(self.job(queued.job_id))
        self.assertEqual(self.paper(published.paper_id).index_status, "ready")
        self.assertEqual(len(indexer.calls), 1)

    def test_obsolete_index_target_is_owned_success_noop_and_job_is_removed(self):
        paper_id = self.seed_paper(current_revision=2)
        job = self.seed_job(paper_id=paper_id, revision=1)
        indexer = FakeRevisionIndexer()
        lifecycle = self.new_lifecycle(indexer)
        progress = run_one_due_job(lifecycle, lease_seconds=1800)
        self.assertEqual(progress.job_id, job.id)
        self.assertIsNone(self.job(job.id))
        self.assertEqual(indexer.calls, [])
        self.assertEqual(self.paper(paper_id).index_status, "pending")

    def test_index_failure_releases_pending_with_redacted_error(self):
        published = self.publish_without_job()
        indexer = FakeRevisionIndexer(
            RuntimeError("Bearer provider-token API_KEY=provider-key")
        )
        lifecycle = self.new_lifecycle(indexer)
        queued = lifecycle.ensure_index_job(published.paper_id)
        progress = run_one_due_job(lifecycle, lease_seconds=1800)
        self.assertEqual(progress.state, JobState.PENDING)
        persisted = self.job(queued.job_id)
        self.assertEqual(persisted.state, "pending")
        self.assertNotIn("provider-token", persisted.last_error)
        self.assertNotIn("provider-key", persisted.last_error)

    def test_unknown_job_kind_is_released_instead_of_dropped(self):
        paper_id = self.seed_paper()
        job = self.seed_job(paper_id=paper_id, kind="future_kind", revision=0)
        progress = run_one_due_job(self.lifecycle, lease_seconds=1800)
        self.assertEqual(progress.state, JobState.PENDING)
        persisted = self.job(job.id)
        self.assertEqual(persisted.state, "pending")
        self.assertIn("unknown", persisted.last_error.lower())

    def test_delete_dispatch_delegates_to_task_nine_seam(self):
        paper_id = self.seed_paper(lifecycle_state="deleting")
        job = self.seed_job(
            paper_id=paper_id,
            kind="delete_paper",
            revision=0,
        )
        with mock.patch.object(
            self.lifecycle,
            "_run_delete_job",
            wraps=self.lifecycle._run_delete_job,
        ) as cleanup:
            progress = run_one_due_job(self.lifecycle, lease_seconds=1800)
        self.assertEqual(progress.job_id, job.id)
        cleanup.assert_called_once()
        self.assertIsNone(self.paper(paper_id))

    def test_recover_job_claims_only_requested_due_job(self):
        first = self.publish_without_job()
        second = self.publish_without_job()
        lifecycle = self.new_lifecycle(FakeRevisionIndexer())
        first_job = lifecycle.ensure_index_job(first.paper_id)
        second_job = lifecycle.ensure_index_job(second.paper_id)
        progress = lifecycle.recover_job(second_job.job_id)
        self.assertEqual(progress.job_id, second_job.job_id)
        self.assertIsNotNone(self.job(first_job.job_id))
        self.assertIsNone(self.job(second_job.job_id))

    def test_stale_claim_cannot_complete_after_reclaim(self):
        published = self.publish_without_job()
        lifecycle = self.new_lifecycle(FakeRevisionIndexer())
        queued = lifecycle.ensure_index_job(published.paper_id)
        old = claim_job_id(
            self.session_factory,
            queued.job_id,
            self.now,
            1,
            lease_token_factory=lambda: self.lease_token(10),
        )
        self.now += timedelta(seconds=2)
        new = claim_job_id(
            self.session_factory,
            queued.job_id,
            self.now,
            1800,
            lease_token_factory=lambda: self.lease_token(11),
        )
        stale_progress = lifecycle._recover_claimed(
            old,
            self.monotonic_now + 1800,
        )
        self.assertEqual(stale_progress.job_id, old.job_id)
        self.assertEqual(self.job(old.job_id).lease_token, new.lease_token)
        self.assertEqual(self.paper(published.paper_id).index_status, "pending")

    def test_reconcile_removes_stale_direct_reservations_and_continues_after_one_failure(self):
        first = self.seed_paper(
            lifecycle_state="publishing",
            current_revision=None,
            reservation_expires_at=self.now - timedelta(hours=2),
        )
        second = self.seed_paper(
            lifecycle_state="publishing",
            current_revision=None,
            reservation_expires_at=self.now - timedelta(hours=2),
        )
        real_delete = self.storage.delete_paper

        def fail_first(paper_id, filenames):
            if paper_id == first:
                raise StorageError("Bearer storage-secret")
            return real_delete(paper_id, filenames)

        with mock.patch.object(self.storage, "delete_paper", side_effect=fail_first):
            reconciled = reconcile_stale_publications(
                self.lifecycle,
                grace_seconds=3600,
            )
        self.assertGreaterEqual(reconciled, 1)
        self.assertIsNotNone(self.paper(first))
        self.assertIsNone(self.paper(second))

    def test_reconcile_removes_reservation_when_its_one_hour_deadline_expires(self):
        paper_id = self.seed_paper(
            lifecycle_state="publishing",
            current_revision=None,
            reservation_expires_at=self.now - timedelta(seconds=1),
        )

        reconcile_stale_publications(self.lifecycle, grace_seconds=3600)

        self.assertIsNone(self.paper(paper_id))

    def test_reconcile_continues_after_one_stale_submission_candidate_fails(self):
        for submission_id in ("a-cancelling", "b-cancelling"):
            pending_name = f"{submission_id}.pdf"
            pending_path = self.storage.pending_dir / pending_name
            pending_path.write_bytes(self.valid_pdf_bytes(submission_id))
            pending_path.chmod(0o600)
            with self.session_factory() as session:
                session.add(
                    SubmissionModel(
                        id=submission_id,
                        pdf_filename=pending_name,
                        pending_filename=pending_name,
                        title="Cancelling",
                        status="cancelling",
                        submitted_at="2026-07-20",
                        submitted_by="reader",
                        reviewed_at=self.now - timedelta(hours=2),
                    )
                )
                session.commit()

        calls = []
        real_finish = self.lifecycle._finish_cancellation

        def fail_first(submission_id, *, expected_owner):
            calls.append(submission_id)
            if submission_id == "a-cancelling":
                raise StorageFailed("secret=first-candidate")
            return real_finish(submission_id, expected_owner=expected_owner)

        handler = _ListHandler()
        logger = logging.getLogger(f"{__name__}.submission-candidates")
        logger.handlers = [handler]
        logger.propagate = False
        with mock.patch.object(
            self.lifecycle,
            "_finish_cancellation",
            side_effect=fail_first,
        ):
            reconcile_stale_publications(
                self.lifecycle,
                grace_seconds=3600,
                logger=logger,
            )

        self.assertEqual(calls, ["a-cancelling", "b-cancelling"])
        with self.session_factory() as session:
            self.assertIsNotNone(session.get(SubmissionModel, "a-cancelling"))
            self.assertIsNone(session.get(SubmissionModel, "b-cancelling"))
        self.assertTrue(any("submission:a-cancelling" in line for line in handler.messages))
        self.assertFalse(any("first-candidate" in line for line in handler.messages))

    def test_reconcile_clears_pending_submission_acceptance_reservation(self):
        submission_id = "stale-acceptance"
        with self.session_factory() as session:
            session.add(
                SubmissionModel(
                    id=submission_id,
                    pdf_filename="submission.pdf",
                    pending_filename="pending-submission.pdf",
                    title="Submission",
                    status="pending",
                    submitted_at="2026-07-21",
                    submitted_by="reader",
                    decision_idempotency_key="reserved-key",
                    decision_payload_hash="f" * 64,
                    reviewer="curator",
                    comment="reserved",
                    reviewed_at=self.now - timedelta(hours=2),
                )
            )
            session.commit()
        paper_id = self.seed_paper(
            lifecycle_state="publishing",
            current_revision=None,
            reservation_expires_at=self.now - timedelta(hours=2),
            origin_submission_id=submission_id,
        )
        reconcile_stale_publications(self.lifecycle, grace_seconds=3600)
        self.assertIsNone(self.paper(paper_id))
        with self.session_factory() as session:
            submission = session.get(SubmissionModel, submission_id)
            self.assertEqual(submission.status, "pending")
            self.assertIsNone(submission.decision_idempotency_key)
            self.assertIsNone(submission.decision_payload_hash)
            self.assertIsNone(submission.reviewer)
            self.assertIsNone(submission.reviewed_at)

    def test_reconcile_keeps_accepted_pending_bytes_until_linked_revision_is_durable(self):
        paper_id = self.seed_paper(
            lifecycle_state="publishing",
            current_revision=None,
            reservation_expires_at=self.now + timedelta(hours=1),
        )
        pending_name = "accepted-not-durable.pdf"
        pending_path = self.storage.pending_dir / pending_name
        pending_path.write_bytes(self.valid_pdf_bytes("accepted-pending"))
        pending_path.chmod(0o600)
        with self.session_factory() as session:
            session.add(
                SubmissionModel(
                    id="accepted-not-durable",
                    pdf_filename="accepted.pdf",
                    pending_filename=pending_name,
                    title="Accepted",
                    status="accepted",
                    submitted_at="2026-07-20",
                    submitted_by="reader",
                    reviewed_at=self.now - timedelta(hours=2),
                    paper_id=paper_id,
                )
            )
            session.commit()
        reconcile_stale_publications(self.lifecycle, grace_seconds=3600)
        self.assertTrue(pending_path.exists())

        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            paper.lifecycle_state = "published"
            paper.current_revision = 1
            session.add(
                PaperRevisionModel(
                    paper_id=paper_id,
                    revision_number=1,
                    sha256="a" * 64,
                    size_bytes=1,
                    created_at=self.now,
                    created_by="curator",
                )
            )
            session.commit()
        reconcile_stale_publications(self.lifecycle, grace_seconds=3600)
        self.assertTrue(pending_path.exists())

        revision_bytes = self.valid_pdf_bytes("accepted-durable")
        revision_path = self.storage.revision_path(paper_id, 1)
        revision_path.parent.mkdir(mode=0o700, exist_ok=True)
        revision_path.write_bytes(revision_bytes)
        revision_path.chmod(0o600)
        with self.session_factory() as session:
            revision = session.get(PaperRevisionModel, (paper_id, 1))
            revision.sha256 = hashlib.sha256(revision_bytes).hexdigest()
            revision.size_bytes = len(revision_bytes)
            session.commit()
        reconcile_stale_publications(self.lifecycle, grace_seconds=3600)
        self.assertFalse(pending_path.exists())

    def test_reconcile_cleans_decided_accepted_copy_after_paper_unlink_only(self):
        pending_paths = {}
        for submission_id, decided in (
            ("accepted-after-delete", True),
            ("unresolved-migrated-accepted", False),
        ):
            pending_name = f"{submission_id}.pdf"
            pending_path = self.storage.pending_dir / pending_name
            pending_path.write_bytes(self.valid_pdf_bytes(submission_id))
            pending_path.chmod(0o600)
            pending_paths[submission_id] = pending_path
            with self.session_factory() as session:
                session.add(
                    SubmissionModel(
                        id=submission_id,
                        pdf_filename=pending_name,
                        pending_filename=pending_name,
                        title="Accepted",
                        status="accepted",
                        submitted_at="2026-07-20",
                        submitted_by="reader",
                        reviewed_at=self.now - timedelta(hours=2),
                        paper_id=None,
                        decision_idempotency_key=(
                            "accepted-decision" if decided else None
                        ),
                        decision_payload_hash=("d" * 64 if decided else None),
                    )
                )
                session.commit()

        reconcile_stale_publications(self.lifecycle, grace_seconds=3600)

        self.assertFalse(pending_paths["accepted-after-delete"].exists())
        self.assertTrue(pending_paths["unresolved-migrated-accepted"].exists())

    def test_storage_reconciliation_references_every_registered_historical_revision(self):
        published = self.seed_paper(current_revision=2)
        deleting = self.seed_paper(lifecycle_state="deleting", current_revision=1)
        orphan = "00000000-0000-4000-8000-777777777777"
        for paper_id, revisions in ((published, (1, 2)), (deleting, (1,)), (orphan, (1,))):
            for revision in revisions:
                directory = self.storage.papers_dir / paper_id
                directory.mkdir(mode=0o700, exist_ok=True)
                path = directory / f"{revision}.pdf"
                path.write_bytes(self.valid_pdf_bytes(f"{paper_id}-{revision}"))
                path.chmod(0o600)
                old = time.time() - 7200
                os.utime(path, (old, old))
        reconcile_stale_publications(self.lifecycle, grace_seconds=3600)
        self.assertTrue(self.storage.revision_path(published, 1).exists())
        self.assertTrue(self.storage.revision_path(published, 2).exists())
        self.assertTrue(self.storage.revision_path(deleting, 1).exists())
        self.assertFalse((self.storage.papers_dir / orphan / "1.pdf").exists())

    def test_publishing_worker_reconcile_failure_never_suppresses_job_recovery(self):
        published = self.publish_without_job()
        lifecycle = self.new_lifecycle(FakeRevisionIndexer())
        queued = lifecycle.ensure_index_job(published.paper_id)
        worker = PublishingWorker(
            lifecycle=lifecycle,
            session_factory=self.session_factory,
            clock=lambda: self.now,
            monotonic_clock=lambda: self.monotonic_now,
            lease_token_factory=lambda: self.lease_token(12),
            jitter=lambda: 0.0,
            lease_seconds=1800,
            reservation_grace_seconds=3600,
            poll_seconds=5,
        )
        with mock.patch(
            "services.publishing_jobs.reconcile_stale_publications",
            side_effect=RuntimeError("Bearer reconcile-secret"),
        ):
            worker.reconcile()
            progress = worker.run_one()
        self.assertEqual(progress.job_id, queued.job_id)
        self.assertIsNone(self.job(queued.job_id))


if __name__ == "__main__":
    unittest.main()
