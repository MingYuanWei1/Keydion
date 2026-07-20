import io
import os
import threading
import time
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

from models import (
    PaperMetadataModel,
    PaperRevisionModel,
    SubmissionModel,
)
from services.paper_storage import PaperStorage, StorageError
from services.publishing import _submission_trash_operation_id
from services.publishing_contracts import (
    AcceptSubmission,
    Actor,
    CancelSubmission,
    DecisionConflict,
    Forbidden,
    IndexingState,
    NormalizedPaperMetadata,
    PdfUpload,
    PersistenceFailed,
    RejectSubmission,
    StorageFailed,
    SubmissionNotPending,
)
from tests.publishing_support import FakeRevisionIndexer, PublishingLifecycleTestCase


class InjectedCrash(BaseException):
    """Model process death: lifecycle exception compensation must not run."""


class CommitFailingFactory:
    def __init__(self, session_factory, failure_number):
        self.session_factory = session_factory
        self.failure_number = failure_number
        self.commits = 0

    def __call__(self):
        session = self.session_factory()
        real_commit = session.commit

        def commit():
            self.commits += 1
            if self.commits == self.failure_number:
                raise RuntimeError("injected database failure")
            return real_commit()

        session.commit = commit
        return session


class AmbiguousCommitFactory(CommitFailingFactory):
    """Commit the requested transaction, then make its result ambiguous."""

    def __call__(self):
        session = self.session_factory()
        real_commit = session.commit

        def commit():
            self.commits += 1
            result = real_commit()
            if self.commits == self.failure_number:
                raise RuntimeError("connection lost after commit")
            return result

        session.commit = commit
        return session


class SubmissionPublishingTests(PublishingLifecycleTestCase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.seed_submission()

    def pending_name(self, submission_id="submission-1"):
        return f"pending-{submission_id}.pdf"

    def lifecycle_trash_path(self, submission_id="submission-1"):
        operation_id = _submission_trash_operation_id(submission_id)
        return self.storage.trash_dir / f"{operation_id}.pdf"

    def seed_submission(
        self,
        submission_id="submission-1",
        *,
        status="pending",
        owner="reader",
        pending_filename=None,
    ):
        pending_filename = pending_filename or self.pending_name(submission_id)
        pending_path = self.storage.pending_dir / pending_filename
        if status in {"pending", "cancelling"} and not pending_path.exists():
            pending_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            pending_path.write_bytes(self.valid_pdf_bytes(submission_id))
            pending_path.chmod(0o600)
        with self.session_factory() as session:
            session.add(
                SubmissionModel(
                    id=submission_id,
                    pdf_filename=f"{submission_id}.pdf",
                    pending_filename=pending_filename,
                    title="Submitted Paper",
                    author_name="Reader Author",
                    author_email="reader@example.test",
                    author_school="Reader School",
                    status=status,
                    submitted_at="2026-07-21",
                    abstract="Submitted abstract",
                    keywords="evidence, methods",
                    journal="Submission Journal",
                    category="science",
                    language="en",
                    submitted_by=owner,
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
        return pending_path

    def metadata(self, **changes):
        values = {
            "filename": "accepted-paper.pdf",
            "title": "Submitted Paper",
            "journal": "Submission Journal",
            "category": "science",
            "language": "en",
            "keywords": "evidence, methods",
            "abstract": "Submitted abstract",
            "author_name": "Reader Author",
            "author_email": "reader@example.test",
            "author_school": "Reader School",
            "published_at": "2026-07-21",
        }
        values.update(changes)
        return NormalizedPaperMetadata(**values)

    def accept_intent(
        self,
        submission_id="submission-1",
        *,
        actor_id="curator",
        role=3,
        key="accept-key",
        metadata=None,
        caller_pdf=b"caller bytes are not the pending source",
    ):
        return AcceptSubmission(
            actor=Actor(actor_id, role),
            submission_id=submission_id,
            idempotency_key=key,
            metadata=metadata or self.metadata(),
            pdf=PdfUpload("caller.pdf", io.BytesIO(caller_pdf)),
        )

    def reject_intent(
        self,
        submission_id="submission-1",
        *,
        actor_id="curator",
        role=3,
        comment="revise methods",
    ):
        return RejectSubmission(
            actor=Actor(actor_id, role),
            submission_id=submission_id,
            feedback=comment,
        )

    def cancel_intent(
        self,
        submission_id="submission-1",
        *,
        actor_id="reader",
        role=1,
    ):
        return CancelSubmission(
            actor=Actor(actor_id, role),
            submission_id=submission_id,
        )

    def submission(self, submission_id="submission-1"):
        with self.session_factory() as session:
            return session.get(SubmissionModel, submission_id)

    def paper_revisions(self, paper_id):
        with self.session_factory() as session:
            return (
                session.query(PaperRevisionModel)
                .filter(PaperRevisionModel.paper_id == paper_id)
                .all()
            )

    def replace_storage(self):
        papers_dir = self.storage.papers_dir
        pending_dir = self.storage.pending_dir
        self.storage.close()
        self.storage = PaperStorage(papers_dir, pending_dir)
        self.lifecycle = self.new_lifecycle(self.indexer, storage=self.storage)

    def age_reconciliation_window(self):
        self.now += timedelta(hours=2)

    def test_accept_accept_interleaving_publishes_once_and_reconstructs_decision(self):
        first = self.lifecycle.review_submission(self.accept_intent())
        second = self.lifecycle.review_submission(self.accept_intent())

        self.assertEqual(first.decision, "accepted")
        self.assertFalse(first.replayed)
        self.assertEqual(second.decision, "accepted")
        self.assertTrue(second.replayed)
        self.assertEqual(second.paper_id, first.paper_id)
        self.assertEqual(self.submission().paper_id, first.paper_id)
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())
        self.assertEqual(len(self.papers()), 1)
        self.assertEqual(len(self.paper_revisions(first.paper_id)), 1)

    def test_acceptance_uses_database_pending_pdf_not_caller_stream(self):
        outcome = self.lifecycle.review_submission(self.accept_intent(caller_pdf=b"bad"))
        self.assertEqual(outcome.decision, "accepted")
        self.assertTrue(self.storage.revision_path(outcome.paper_id, 1).exists())

    def test_accept_reject_interleaving_rejects_conflicting_curator_decision(self):
        rejected = self.lifecycle.review_submission(
            self.reject_intent(comment="insufficient evidence")
        )
        self.assertEqual(rejected.decision, "rejected")
        with self.assertRaises(DecisionConflict):
            self.lifecycle.review_submission(self.accept_intent())
        self.assertEqual(self.papers(), [])

    def test_acceptance_reservation_wins_before_rejection_can_commit(self):
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False), session_factory=failing
        )
        with self.assertRaises(PersistenceFailed):
            lifecycle.review_submission(self.accept_intent())

        with self.assertRaises(DecisionConflict):
            self.lifecycle.review_submission(self.reject_intent())
        self.assertEqual(self.submission().status, "pending")
        self.assertEqual(len(self.papers()), 1)

    def test_rejection_is_permanent_replayable_and_retains_private_pdf(self):
        first = self.lifecycle.review_submission(self.reject_intent())
        second = self.lifecycle.review_submission(self.reject_intent())

        self.assertEqual(first.decision, "rejected")
        self.assertFalse(first.replayed)
        self.assertTrue(second.replayed)
        row = self.submission()
        self.assertEqual(row.status, "rejected")
        self.assertEqual(row.comment, "revise methods")
        self.assertEqual(row.reviewer, "curator")
        self.assertEqual(row.reviewed_at, self.now)
        self.assertIsNone(row.paper_id)
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())
        self.assertEqual(self.papers(), [])

    def test_rejection_replay_with_different_comment_or_reviewer_conflicts(self):
        self.lifecycle.review_submission(self.reject_intent())
        for changed in (
            self.reject_intent(comment="different"),
            self.reject_intent(actor_id="other-curator"),
        ):
            with self.subTest(changed=changed), self.assertRaises(DecisionConflict):
                self.lifecycle.review_submission(changed)

    def test_ambiguous_rejection_commit_reconstructs_permanent_decision(self):
        ambiguous = AmbiguousCommitFactory(self.session_factory, failure_number=1)
        lifecycle = self.new_lifecycle(session_factory=ambiguous)

        outcome = lifecycle.review_submission(self.reject_intent())

        self.assertEqual(outcome.decision, "rejected")
        self.assertFalse(outcome.replayed)
        self.assertEqual(self.submission().status, "rejected")
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())

    def test_acceptance_same_key_with_changed_reviewer_or_decision_conflicts(self):
        self.lifecycle.review_submission(self.accept_intent())
        with self.assertRaises(DecisionConflict):
            self.lifecycle.review_submission(self.accept_intent(actor_id="other-curator"))
        with self.assertRaises(DecisionConflict):
            self.lifecycle.review_submission(self.reject_intent())
        with self.assertRaises(DecisionConflict):
            self.lifecycle.review_submission(self.accept_intent(key="different-key"))

    def test_contributor_cannot_review_before_storage_or_persistence(self):
        with mock.patch.object(
            self.storage, "stage_pending", wraps=self.storage.stage_pending
        ) as stage:
            with self.assertRaises(Forbidden):
                self.lifecycle.review_submission(self.accept_intent(role=2))
            with self.assertRaises(Forbidden):
                self.lifecycle.review_submission(self.reject_intent(role=2))
        stage.assert_not_called()
        self.assertEqual(self.submission().status, "pending")

    def test_reader_can_cancel_only_own_pending_submission(self):
        outcome = self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertEqual(outcome.submission_id, "submission-1")
        self.assertIsNone(self.submission())
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())
        self.assertFalse(self.lifecycle_trash_path().exists())

    def test_cancellation_uses_safe_fixed_length_trash_keys_for_arbitrary_ids(self):
        submission_ids = ("reader/path", "x" * 200)
        operation_ids = []
        real_trash = self.storage.trash_pending

        def capture_trash(filename, operation_id):
            operation_ids.append(operation_id)
            return real_trash(filename, operation_id)

        for index, submission_id in enumerate(submission_ids):
            self.seed_submission(
                submission_id,
                pending_filename=f"arbitrary-{index}.pdf",
            )
        with mock.patch.object(
            self.storage,
            "trash_pending",
            side_effect=capture_trash,
        ):
            for submission_id in submission_ids:
                self.lifecycle.cancel_submission(self.cancel_intent(submission_id))

        self.assertEqual(len(operation_ids), 2)
        self.assertEqual(len(set(operation_ids)), 2)
        for operation_id in operation_ids:
            self.assertLessEqual(len(operation_id), 128)
            self.assertRegex(operation_id, r"\Asubmission-[0-9a-f]{64}\Z")
        for submission_id in submission_ids:
            self.assertIsNone(self.submission(submission_id))

    def test_wrong_owner_cannot_cancel_before_storage_moves(self):
        with mock.patch.object(
            self.storage, "trash_pending", wraps=self.storage.trash_pending
        ) as trash:
            with self.assertRaises(Forbidden):
                self.lifecycle.cancel_submission(self.cancel_intent(actor_id="stranger"))
        trash.assert_not_called()
        self.assertEqual(self.submission().status, "pending")

    def test_only_reader_owner_can_cancel(self):
        with self.assertRaises(Forbidden):
            self.lifecycle.cancel_submission(self.cancel_intent(role=2))
        self.assertEqual(self.submission().status, "pending")

    def test_decided_submission_cannot_be_cancelled(self):
        self.lifecycle.review_submission(self.reject_intent())
        with self.assertRaises(SubmissionNotPending):
            self.lifecycle.cancel_submission(self.cancel_intent())

    def test_accept_cancel_interleaving_preserves_acceptance_reservation(self):
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False), session_factory=failing
        )
        with self.assertRaises(PersistenceFailed):
            lifecycle.review_submission(self.accept_intent())
        hidden = self.papers()[0]
        self.assertEqual(hidden.lifecycle_state, "publishing")
        self.assertEqual(hidden.origin_submission_id, "submission-1")

        with self.assertRaises(SubmissionNotPending):
            self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertEqual(self.submission().status, "pending")
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())

    def test_cancel_accept_interleaving_blocks_acceptance_at_committed_gate(self):
        with mock.patch.object(
            self.storage, "trash_pending", side_effect=InjectedCrash("stop after gate")
        ):
            with self.assertRaises(InjectedCrash):
                self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertEqual(self.submission().status, "cancelling")
        with self.assertRaises(SubmissionNotPending):
            self.lifecycle.review_submission(self.accept_intent())

    def test_cancel_cancel_interleaving_resumes_same_owned_cancelling_row(self):
        with mock.patch.object(
            self.storage, "trash_pending", side_effect=InjectedCrash("stop after gate")
        ):
            with self.assertRaises(InjectedCrash):
                self.lifecycle.cancel_submission(self.cancel_intent())
        self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertIsNone(self.submission())

    def test_acceptance_storage_failure_leaves_submission_pending_and_unlinked(self):
        with mock.patch.object(
            self.storage, "promote", side_effect=StorageError("disk full")
        ):
            with self.assertRaises(StorageFailed):
                self.lifecycle.review_submission(self.accept_intent())
        row = self.submission()
        self.assertEqual(row.status, "pending")
        self.assertIsNone(row.paper_id)
        self.assertIsNone(row.decision_idempotency_key)
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())

    def test_replay_after_previsibility_failure_reuses_origin_reservation(self):
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False), session_factory=failing
        )
        with self.assertRaises(PersistenceFailed):
            lifecycle.review_submission(self.accept_intent())
        reserved_id = self.papers()[0].id
        self.assertEqual(self.submission().status, "pending")
        self.assertIsNone(self.submission().paper_id)

        outcome = self.lifecycle.review_submission(self.accept_intent())
        self.assertEqual(outcome.paper_id, reserved_id)
        self.assertEqual(len(self.papers()), 1)

    def test_delayed_acceptance_replay_renews_and_reuses_origin_reservation(self):
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False), session_factory=failing
        )
        with self.assertRaises(PersistenceFailed):
            lifecycle.review_submission(self.accept_intent())
        reserved_id = self.papers()[0].id
        self.age_reconciliation_window()

        outcome = self.lifecycle.review_submission(self.accept_intent())

        self.assertEqual(outcome.paper_id, reserved_id)
        self.assertEqual(self.paper(reserved_id).lifecycle_state, "published")
        self.assertEqual(len(self.papers()), 1)

    def test_rejection_reconciles_expired_origin_reservation_storage_first(self):
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False), session_factory=failing
        )
        with self.assertRaises(PersistenceFailed):
            lifecycle.review_submission(self.accept_intent())
        reserved_id = self.papers()[0].id
        self.storage.revision_path(reserved_id, 1).parent.mkdir(mode=0o700)
        residual = self.storage.revision_path(reserved_id, 1)
        residual.write_bytes(self.valid_pdf_bytes("expired-origin"))
        residual.chmod(0o600)
        self.age_reconciliation_window()

        outcome = self.lifecycle.review_submission(self.reject_intent())

        self.assertEqual(outcome.decision, "rejected")
        self.assertIsNone(self.paper(reserved_id))
        self.assertFalse(residual.exists())
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())

    def test_cancellation_reconciles_expired_origin_reservation(self):
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False), session_factory=failing
        )
        with self.assertRaises(PersistenceFailed):
            lifecycle.review_submission(self.accept_intent())
        reserved_id = self.papers()[0].id
        self.age_reconciliation_window()

        self.lifecycle.cancel_submission(self.cancel_intent())

        self.assertIsNone(self.submission())
        self.assertIsNone(self.paper(reserved_id))
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())

    def test_background_reconciles_expired_origin_reservation_for_pending_submission(self):
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False), session_factory=failing
        )
        with self.assertRaises(PersistenceFailed):
            lifecycle.review_submission(self.accept_intent())
        reserved_id = self.papers()[0].id
        self.storage.revision_path(reserved_id, 1).parent.mkdir(mode=0o700)
        residual = self.storage.revision_path(reserved_id, 1)
        residual.write_bytes(self.valid_pdf_bytes("expired-background-origin"))
        residual.chmod(0o600)
        self.age_reconciliation_window()

        reconciled = self.lifecycle.reconcile_submissions()

        self.assertEqual(reconciled, 1)
        self.assertIsNone(self.paper(reserved_id))
        self.assertFalse(residual.exists())
        self.assertEqual(self.submission().status, "pending")
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())

    def test_ambiguous_final_commit_reconstructs_accepted_decision(self):
        ambiguous = AmbiguousCommitFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False), session_factory=ambiguous
        )
        outcome = lifecycle.review_submission(self.accept_intent())
        self.assertEqual(outcome.decision, "accepted")
        self.assertEqual(outcome.paper_id, self.submission().paper_id)
        self.assertEqual(self.paper(outcome.paper_id).lifecycle_state, "published")

    def test_acceptance_index_failure_is_visible_and_has_one_pending_retry(self):
        self.indexer = FakeRevisionIndexer(RuntimeError("provider down; token=secret"))
        self.lifecycle = self.new_lifecycle(self.indexer)
        outcome = self.lifecycle.review_submission(self.accept_intent())

        self.assertEqual(outcome.decision, "accepted")
        self.assertEqual(outcome.indexing.state, IndexingState.FAILED)
        self.assertEqual(self.submission().status, "accepted")
        paper = self.paper(outcome.paper_id)
        self.assertEqual(paper.lifecycle_state, "published")
        self.assertEqual(paper.index_status, "failed")
        jobs = self.jobs(outcome.paper_id)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].state, "pending")
        replay = self.lifecycle.review_submission(self.accept_intent())
        self.assertEqual(replay.indexing.state, IndexingState.FAILED)
        self.assertEqual(len(self.jobs(outcome.paper_id)), 1)

    def test_disabled_acceptance_index_is_not_required_and_has_no_warning_or_job(self):
        self.indexer = FakeRevisionIndexer(enabled=False)
        self.lifecycle = self.new_lifecycle(self.indexer)
        outcome = self.lifecycle.review_submission(self.accept_intent())

        self.assertEqual(outcome.indexing.state, IndexingState.NOT_REQUIRED)
        self.assertEqual(self.jobs(outcome.paper_id), [])
        self.assertIsNone(self.paper(outcome.paper_id).index_error)
        self.assertEqual(self.indexer.calls, [])

    def test_acceptance_cleanup_failure_keeps_permanent_decision_and_replays_cleanup(self):
        real_trash = self.storage.trash_pending
        with mock.patch.object(
            self.storage, "trash_pending", side_effect=StorageError("cleanup unavailable")
        ):
            outcome = self.lifecycle.review_submission(self.accept_intent())
        self.assertEqual(outcome.decision, "accepted")
        self.assertEqual(self.submission().status, "accepted")
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())

        with mock.patch.object(self.storage, "trash_pending", wraps=real_trash):
            replay = self.lifecycle.review_submission(self.accept_intent())
        self.assertTrue(replay.replayed)
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())

    def test_restart_reconciles_acceptance_crash_after_pending_trash_move(self):
        with mock.patch.object(
            self.storage,
            "discard_pending_trash",
            side_effect=InjectedCrash("accepted before trash cleanup"),
        ):
            with self.assertRaises(InjectedCrash):
                self.lifecycle.review_submission(self.accept_intent())
        accepted = self.submission()
        self.assertEqual(accepted.status, "accepted")
        self.assertEqual(self.paper(accepted.paper_id).lifecycle_state, "published")
        self.assertTrue(self.lifecycle_trash_path().exists())

        self.replace_storage()
        self.age_reconciliation_window()
        self.lifecycle.reconcile_submissions()
        self.assertEqual(self.submission().status, "accepted")
        self.assertFalse(self.lifecycle_trash_path().exists())

    def test_restart_finishes_accepted_cleanup_after_link_before_unlink_crash(self):
        original = self.storage.pending_dir / self.pending_name()
        trashed = self.lifecycle_trash_path()
        real_unlink = self.storage._unlink_if_matching

        def crash_before_pending_unlink(directory_fd, name, expected):
            if directory_fd != self.storage._trash_fd and name == original.name:
                raise InjectedCrash("accepted cleanup link committed")
            return real_unlink(directory_fd, name, expected)

        with mock.patch.object(
            self.storage,
            "_unlink_if_matching",
            side_effect=crash_before_pending_unlink,
        ):
            with self.assertRaises(InjectedCrash):
                self.lifecycle.review_submission(self.accept_intent())

        self.assertEqual(self.submission().status, "accepted")
        self.assertTrue(original.exists())
        self.assertTrue(trashed.exists())
        self.assertTrue(os.path.samefile(original, trashed))

        self.replace_storage()
        self.age_reconciliation_window()
        self.lifecycle.reconcile_submissions()

        self.assertEqual(self.submission().status, "accepted")
        self.assertFalse(original.exists())
        self.assertFalse(trashed.exists())

    def test_unsafe_pending_names_fail_acceptance_before_copying_any_bytes(self):
        outside = Path(self.tmp.name) / "outside.pdf"
        outside.write_bytes(self.valid_pdf_bytes("outside"))
        outside.chmod(0o600)
        symlink = self.storage.pending_dir / "escape.pdf"
        symlink.symlink_to(outside)
        reserved = self.storage.trash_dir / "reserved.pdf"
        reserved.write_bytes(self.valid_pdf_bytes("reserved"))
        reserved.chmod(0o600)
        cases = ("../outside.pdf", str(outside), "escape.pdf", ".trash/reserved.pdf")

        for index, unsafe in enumerate(cases, start=1):
            submission_id = f"unsafe-accept-{index}"
            self.seed_submission(
                submission_id,
                pending_filename=self.pending_name(submission_id),
            )
            with self.session_factory() as session:
                session.get(SubmissionModel, submission_id).pending_filename = unsafe
                session.commit()
            with mock.patch.object(
                self.storage, "_stage_stream", wraps=self.storage._stage_stream
            ) as copy_bytes:
                with self.assertRaises(StorageFailed):
                    self.lifecycle.review_submission(
                        self.accept_intent(
                            submission_id,
                            key=f"unsafe-accept-key-{index}",
                            metadata=self.metadata(filename=f"unsafe-{index}.pdf"),
                        )
                    )
            copy_bytes.assert_not_called()
            self.assertEqual(self.submission(submission_id).status, "pending")
        self.assertTrue(outside.exists())
        self.assertEqual(outside.read_bytes(), self.valid_pdf_bytes("outside"))

    def test_unsafe_pending_names_fail_cancellation_before_any_move(self):
        outside = Path(self.tmp.name) / "outside-cancel.pdf"
        outside.write_bytes(self.valid_pdf_bytes("outside-cancel"))
        outside.chmod(0o600)
        symlink = self.storage.pending_dir / "cancel-escape.pdf"
        symlink.symlink_to(outside)
        reserved = self.storage.trash_dir / "cancel-reserved.pdf"
        reserved.write_bytes(self.valid_pdf_bytes("cancel-reserved"))
        reserved.chmod(0o600)
        cases = (
            "../outside-cancel.pdf",
            str(outside),
            "cancel-escape.pdf",
            ".trash/cancel-reserved.pdf",
        )

        for index, unsafe in enumerate(cases, start=1):
            submission_id = f"unsafe-cancel-{index}"
            self.seed_submission(submission_id)
            with self.session_factory() as session:
                session.get(SubmissionModel, submission_id).pending_filename = unsafe
                session.commit()
            with mock.patch.object(
                self.storage, "_link_then_unlink", wraps=self.storage._link_then_unlink
            ) as move:
                with self.assertRaises(StorageFailed):
                    self.lifecycle.cancel_submission(self.cancel_intent(submission_id))
            move.assert_not_called()
            row = self.submission(submission_id)
            self.assertEqual(row.status, "pending")
        self.assertEqual(outside.read_bytes(), self.valid_pdf_bytes("outside-cancel"))

    def test_pending_hard_link_is_rejected_before_acceptance_or_cancellation(self):
        outside = Path(self.tmp.name) / "hard-linked.pdf"
        outside.write_bytes(self.valid_pdf_bytes("hard-link"))
        outside.chmod(0o600)
        accept_link = self.storage.pending_dir / "accept-hardlink.pdf"
        cancel_link = self.storage.pending_dir / "cancel-hardlink.pdf"
        accept_link.hardlink_to(outside)
        cancel_link.hardlink_to(outside)
        self.seed_submission("hardlink-accept")
        self.seed_submission("hardlink-cancel")
        with self.session_factory() as session:
            session.get(SubmissionModel, "hardlink-accept").pending_filename = accept_link.name
            session.get(SubmissionModel, "hardlink-cancel").pending_filename = cancel_link.name
            session.commit()

        with self.assertRaises(StorageFailed):
            self.lifecycle.review_submission(
                self.accept_intent(
                    "hardlink-accept",
                    key="hardlink-key",
                    metadata=self.metadata(filename="hardlink-accepted.pdf"),
                )
            )
        with self.assertRaises(StorageFailed):
            self.lifecycle.cancel_submission(self.cancel_intent("hardlink-cancel"))
        self.assertTrue(outside.exists())
        self.assertEqual(self.submission("hardlink-accept").status, "pending")
        self.assertEqual(self.submission("hardlink-cancel").status, "pending")

    def test_cancellation_of_missing_authorized_pending_bytes_removes_row(self):
        (self.storage.pending_dir / self.pending_name()).unlink()
        self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertIsNone(self.submission())

    def test_reconcile_crash_after_cancelling_gate_finishes_cancellation(self):
        with mock.patch.object(
            self.storage, "trash_pending", side_effect=InjectedCrash("after gate")
        ):
            with self.assertRaises(InjectedCrash):
                self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertEqual(self.submission().status, "cancelling")
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())

        self.age_reconciliation_window()
        self.lifecycle.reconcile_submissions()
        self.assertIsNone(self.submission())
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())

    def test_restart_rehydrates_deterministic_trash_after_move_crash(self):
        real_trash_pending = self.storage.trash_pending

        def move_then_crash(filename, operation_id):
            real_trash_pending(filename, operation_id)
            raise InjectedCrash("after deterministic trash move")

        with mock.patch.object(self.storage, "trash_pending", side_effect=move_then_crash):
            with self.assertRaises(InjectedCrash):
                self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertEqual(self.submission().status, "cancelling")
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())
        self.assertTrue(self.lifecycle_trash_path().exists())

        self.replace_storage()
        self.age_reconciliation_window()
        self.lifecycle.reconcile_submissions()
        self.assertIsNone(self.submission())
        self.assertFalse(self.lifecycle_trash_path().exists())

    def test_reconcile_finishes_cancellation_after_link_before_unlink_crash(self):
        original = self.storage.pending_dir / self.pending_name()
        trashed = self.lifecycle_trash_path()
        real_unlink = self.storage._unlink_if_matching

        def crash_before_source_unlink(directory_fd, name, expected):
            if directory_fd != self.storage._trash_fd and name == original.name:
                raise InjectedCrash("after trash link before source unlink")
            return real_unlink(directory_fd, name, expected)

        with mock.patch.object(
            self.storage,
            "_unlink_if_matching",
            side_effect=crash_before_source_unlink,
        ):
            with self.assertRaises(InjectedCrash):
                self.lifecycle.cancel_submission(self.cancel_intent())

        self.assertEqual(self.submission().status, "cancelling")
        self.assertTrue(original.exists())
        self.assertTrue(trashed.exists())
        self.assertTrue(os.path.samefile(original, trashed))
        self.assertEqual(original.stat().st_nlink, 2)

        self.replace_storage()
        self.age_reconciliation_window()
        self.lifecycle.reconcile_submissions()

        self.assertIsNone(self.submission())
        self.assertFalse(original.exists())
        self.assertFalse(trashed.exists())

    def test_reconcile_restores_pending_after_link_before_unlink_crash(self):
        original = self.storage.pending_dir / self.pending_name()
        trashed = self.storage.trash_dir / "submission-1.pdf"

        os.link(
            original.name,
            trashed.name,
            src_dir_fd=self.storage._pending_fd,
            dst_dir_fd=self.storage._trash_fd,
            follow_symlinks=False,
        )
        original.chmod(0o600)
        self.assertTrue(os.path.samefile(original, trashed))
        self.assertEqual(original.stat().st_nlink, 2)

        self.replace_storage()
        self.age_reconciliation_window()
        self.lifecycle.reconcile_submissions()

        self.assertEqual(self.submission().status, "pending")
        self.assertTrue(original.exists())
        self.assertEqual(original.stat().st_nlink, 1)
        self.assertFalse(trashed.exists())

    def test_restart_reaudit_issues_fresh_one_use_trash_capability(self):
        original = self.storage.trash_pending(self.pending_name(), "submission-1")
        self.replace_storage()
        with self.assertRaises(StorageError):
            self.storage.discard_pending_trash(original)

        recovered = self.storage.rehydrate_pending_trash(
            self.pending_name(),
            "submission-1",
        )
        self.storage.discard_pending_trash(recovered)
        with self.assertRaises(StorageError):
            self.storage.discard_pending_trash(recovered)

    def test_restart_removes_trash_after_row_delete_crash(self):
        with mock.patch.object(
            self.storage,
            "discard_pending_trash",
            side_effect=InjectedCrash("after row deletion"),
        ):
            with self.assertRaises(InjectedCrash):
                self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertIsNone(self.submission())
        self.assertTrue(self.lifecycle_trash_path().exists())

        self.replace_storage()
        self.age_reconciliation_window()
        self.lifecycle.reconcile_submissions()
        self.assertFalse(self.lifecycle_trash_path().exists())

    def test_reconciliation_restores_trash_referenced_by_surviving_pending_row(self):
        self.storage.trash_pending(self.pending_name(), "submission-1")
        self.assertEqual(self.submission().status, "pending")
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())
        self.replace_storage()
        self.age_reconciliation_window()

        self.lifecycle.reconcile_submissions()

        self.assertEqual(self.submission().status, "pending")
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())
        self.assertFalse((self.storage.trash_dir / "submission-1.pdf").exists())

    def test_generic_storage_reconciliation_never_deletes_submission_trash(self):
        self.storage.trash_pending(self.pending_name(), "submission-1")
        trashed = self.storage.trash_dir / "submission-1.pdf"
        old = time.time() - 120
        os.utime(trashed, (old, old))

        self.storage.reconcile_expired(time.time() - 60, set())

        self.assertEqual(self.submission().status, "pending")
        self.assertTrue(trashed.exists())
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())

    def test_final_cancellation_transaction_failure_restores_pdf_before_pending_status(self):
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(session_factory=failing)
        with self.assertRaises(PersistenceFailed):
            lifecycle.cancel_submission(self.cancel_intent())

        self.assertEqual(self.submission().status, "pending")
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())
        self.assertFalse(self.lifecycle_trash_path().exists())

    def test_final_cancellation_failure_keeps_cancelling_when_pdf_is_missing(self):
        (self.storage.pending_dir / self.pending_name()).unlink()
        failing = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(session_factory=failing)

        with self.assertRaises(PersistenceFailed):
            lifecycle.cancel_submission(self.cancel_intent())

        self.assertEqual(self.submission().status, "cancelling")
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())
        self.assertFalse(self.lifecycle_trash_path().exists())

    def test_storage_error_with_no_recoverable_bytes_keeps_cancelling(self):
        pending = self.storage.pending_dir / self.pending_name()
        calls = 0

        def lose_bytes_then_fail(_original_name, _operation_id):
            nonlocal calls
            calls += 1
            if calls == 1:
                pending.unlink()
                raise StorageError("injected storage failure")
            return None

        with mock.patch.object(
            self.storage,
            "rehydrate_pending_trash",
            side_effect=lose_bytes_then_fail,
        ):
            with self.assertRaises(StorageFailed):
                self.lifecycle.cancel_submission(self.cancel_intent())

        self.assertEqual(self.submission().status, "cancelling")
        self.assertFalse(pending.exists())
        self.assertFalse(self.lifecycle_trash_path().exists())

    def test_post_move_storage_failure_restores_pdf_and_pending_status(self):
        real_trash_pending = self.storage.trash_pending

        def move_then_fail(filename, operation_id):
            real_trash_pending(filename, operation_id)
            raise StorageError("injected failure after move")

        with mock.patch.object(
            self.storage,
            "trash_pending",
            side_effect=move_then_fail,
        ):
            with self.assertRaises(StorageFailed):
                self.lifecycle.cancel_submission(self.cancel_intent())
        self.assertEqual(self.submission().status, "pending")
        self.assertTrue((self.storage.pending_dir / self.pending_name()).exists())
        self.assertFalse(self.lifecycle_trash_path().exists())

    def test_failed_post_move_restore_leaves_cancelling_row_and_trash(self):
        real_trash_pending = self.storage.trash_pending

        def move_then_fail(filename, operation_id):
            real_trash_pending(filename, operation_id)
            raise StorageError("injected failure after move")

        with mock.patch.object(
            self.storage,
            "trash_pending",
            side_effect=move_then_fail,
        ), mock.patch.object(
            self.storage,
            "restore_pending",
            side_effect=StorageError("restore unavailable"),
        ):
            with self.assertRaises(StorageFailed):
                self.lifecycle.cancel_submission(self.cancel_intent())

        self.assertEqual(self.submission().status, "cancelling")
        self.assertFalse((self.storage.pending_dir / self.pending_name()).exists())
        self.assertTrue(self.lifecycle_trash_path().exists())

    def test_two_concurrent_acceptance_intents_leave_one_paper(self):
        ready = threading.Barrier(2)
        outcomes = []

        class BarrierIndexer(FakeRevisionIndexer):
            def enabled(inner):
                try:
                    ready.wait(timeout=5)
                except threading.BrokenBarrierError:
                    pass
                return False

        first = self.new_lifecycle(BarrierIndexer())
        second = self.new_lifecycle(BarrierIndexer())

        def accept(lifecycle):
            try:
                outcomes.append(lifecycle.review_submission(self.accept_intent()))
            except Exception as exc:
                outcomes.append(exc)

        threads = [threading.Thread(target=accept, args=(lifecycle,)) for lifecycle in (first, second)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=7)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(len(self.papers()), 1)
        self.assertEqual(len([outcome for outcome in outcomes if not isinstance(outcome, Exception)]), 2)
        self.assertEqual({outcome.paper_id for outcome in outcomes}, {self.papers()[0].id})
        self.assertEqual(sorted(outcome.replayed for outcome in outcomes), [False, True])

    def test_acceptance_reconstructs_winner_when_cleanup_wins_stage_race(self):
        second_storage = PaperStorage(
            self.storage.papers_dir,
            self.storage.pending_dir,
        )
        second = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False),
            storage=second_storage,
        )
        entered_stage = threading.Event()
        release_stage = threading.Event()
        outcomes = []
        errors = []
        real_stage = second_storage.stage_pending

        def stage_after_winner_cleanup(filename, operation_id):
            entered_stage.set()
            if not release_stage.wait(timeout=5):
                raise RuntimeError("acceptance race release timed out")
            return real_stage(filename, operation_id)

        def accept_second():
            try:
                outcomes.append(second.review_submission(self.accept_intent()))
            except Exception as exc:
                errors.append(exc)

        try:
            with mock.patch.object(
                second_storage,
                "stage_pending",
                side_effect=stage_after_winner_cleanup,
            ):
                thread = threading.Thread(target=accept_second)
                thread.start()
                self.assertTrue(entered_stage.wait(timeout=2))
                winner = self.lifecycle.review_submission(self.accept_intent())
                release_stage.set()
                thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(len(outcomes), 1)
            self.assertEqual(outcomes[0].paper_id, winner.paper_id)
            self.assertTrue(outcomes[0].replayed)
        finally:
            release_stage.set()
            second_storage.close()


if __name__ == "__main__":
    unittest.main()
