"""Lifecycle tests for hide-first, retryable Paper deletion."""

from __future__ import annotations

import io
import unittest
from datetime import timedelta

from sqlalchemy import event

from models import (
    PaperChunkModel,
    PaperFilenameAliasModel,
    PaperMetadataModel,
    PaperRevisionModel,
    PublishingJobModel,
    PublishingMigrationIssueModel,
    PublishingMigrationJournalModel,
    PublishingMigrationStateModel,
    RagIndexMetaModel,
    SubmissionModel,
)
from services.paper_identity import normalize_alias_key
from services.paper_storage import StorageError
from services.publishing_contracts import (
    AcceptSubmission,
    Actor,
    DeletePaper,
    DeletionState,
    DirectPublish,
    EditMetadata,
    Forbidden,
    JobLease,
    MetadataPatch,
    NormalizedPaperMetadata,
    NotFound,
    PdfUpload,
    PreparedChunk,
    PreparedRevisionIndex,
    RevisePdf,
    StaleVersion,
)
from tests.publishing_support import PublishingLifecycleTestCase


class _FailOnceDeleteStorage:
    """Delegate every storage operation except one injected delete failure."""

    def __init__(self, storage):
        self._storage = storage
        self.fail_delete_once = False

    def __getattr__(self, name):
        return getattr(self._storage, name)

    def delete_paper(self, paper_id, retained_legacy_filenames):
        if self.fail_delete_once:
            self.fail_delete_once = False
            raise StorageError("disk failed with bearer secret-token")
        return self._storage.delete_paper(paper_id, retained_legacy_filenames)


class _CommitFailingFactory:
    """Fail one selected commit without changing the underlying database."""

    def __init__(self, session_factory, failure_number):
        self._session_factory = session_factory
        self._failure_number = failure_number
        self.commits = 0

    def __call__(self):
        session = self._session_factory()
        real_commit = session.commit

        def commit():
            self.commits += 1
            if self.commits == self._failure_number:
                raise RuntimeError("injected final deletion commit failure")
            return real_commit()

        session.commit = commit
        return session


class PublishingDeleteTests(PublishingLifecycleTestCase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._raw_storage = self.storage
        self.storage = _FailOnceDeleteStorage(self._raw_storage)
        self.lifecycle = self.new_lifecycle(self.indexer, storage=self.storage)
        self._publication_number = 0

    def direct_intent(self, *, filename=None, key=None):
        self._publication_number += 1
        number = self._publication_number
        return DirectPublish(
            actor=Actor("contributor", 2),
            idempotency_key=key or f"delete-publish-{number}",
            metadata=NormalizedPaperMetadata(
                filename=filename or f"delete-paper-{number}.pdf",
                title=f"Delete Paper {number}",
                journal="Journal",
                category="science",
                language="en",
                author_name="Author",
            ),
            pdf=PdfUpload(
                "source.pdf",
                io.BytesIO(self.valid_pdf_bytes(f"delete-{number}")),
            ),
        )

    def publish(self, *, filename=None):
        return self.lifecycle.publish_direct(self.direct_intent(filename=filename))

    @staticmethod
    def delete_intent(published, *, role=2, expected_row_version=None):
        return DeletePaper(
            actor=Actor("contributor", role),
            paper_id=published.paper_id,
            expected_row_version=(
                published.row_version
                if expected_row_version is None
                else expected_row_version
            ),
        )

    def seed_submission(self, submission_id="delete-submission"):
        pending_filename = f"pending-{submission_id}.pdf"
        pending_path = self.storage.pending_dir / pending_filename
        pending_path.write_bytes(self.valid_pdf_bytes(submission_id))
        pending_path.chmod(0o600)
        with self.session_factory() as session:
            session.add(
                SubmissionModel(
                    id=submission_id,
                    pdf_filename=f"{submission_id}.pdf",
                    pending_filename=pending_filename,
                    title="Accepted Paper",
                    author_name="Reader Author",
                    author_email="reader@example.test",
                    author_school="Reader School",
                    status="pending",
                    submitted_at="2026-07-21",
                    abstract="Accepted abstract",
                    keywords="evidence",
                    journal="Submission Journal",
                    category="science",
                    language="en",
                    submitted_by="reader",
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
        return pending_filename

    def accept_submission(self, submission_id="delete-submission"):
        self.seed_submission(submission_id)
        intent = AcceptSubmission(
            actor=Actor("curator", 3),
            submission_id=submission_id,
            idempotency_key=f"accept-{submission_id}",
            metadata=NormalizedPaperMetadata(
                filename="accepted-original.pdf",
                title="Accepted Paper",
                journal="Submission Journal",
                category="science",
                language="en",
                author_name="Reader Author",
            ),
            pdf=PdfUpload("ignored.pdf", io.BytesIO(b"caller bytes are ignored")),
        )
        return self.lifecycle.review_submission(intent), intent

    def paper_or_none(self, paper_id):
        with self.session_factory() as session:
            return session.get(PaperMetadataModel, paper_id)

    def submission(self, submission_id):
        with self.session_factory() as session:
            return session.get(SubmissionModel, submission_id)

    def visible_papers(self):
        with self.session_factory() as session:
            return (
                session.query(PaperMetadataModel)
                .filter(PaperMetadataModel.lifecycle_state == "published")
                .order_by(PaperMetadataModel.id)
                .all()
            )

    def rows_for(self, model, paper_id):
        with self.session_factory() as session:
            return (
                session.query(model)
                .filter(model.paper_id == paper_id)
                .all()
            )

    def stamp(self):
        with self.session_factory() as session:
            row = session.get(RagIndexMetaModel, "chunks_version")
            return None if row is None else row.value

    def claim_delete_job(self, paper_id, *, token=None):
        token = token or "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        with self.session_factory() as session:
            job = (
                session.query(PublishingJobModel)
                .filter(
                    PublishingJobModel.paper_id == paper_id,
                    PublishingJobModel.kind == "delete_paper",
                )
                .one()
            )
            previous_updated_at = job.updated_at
            job.state = "running"
            job.attempts += 1
            job.lease_token = token
            job.lease_expires_at = self.now + timedelta(seconds=1800)
            job.updated_at = self.now
            session.commit()
            return JobLease(
                job_id=job.id,
                paper_id=paper_id,
                revision=0,
                kind="delete_paper",
                attempts=job.attempts,
                lease_token=token,
                lease_expires_at=job.lease_expires_at,
                created_at=job.created_at,
                previous_updated_at=previous_updated_at,
            )

    def seed_running_index_job(self, paper_id, revision):
        job_id = "11111111-2222-4333-8444-555555555555"
        token = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        with self.session_factory() as session:
            session.add(
                PublishingJobModel(
                    id=job_id,
                    kind="index_revision",
                    paper_id=paper_id,
                    revision_number=revision,
                    dedupe_key=f"index:{paper_id}:{revision}",
                    state="running",
                    attempts=1,
                    available_at=self.now,
                    lease_token=token,
                    lease_expires_at=self.now + timedelta(seconds=1800),
                    last_error=None,
                    created_at=self.now,
                    updated_at=self.now,
                )
            )
            session.commit()
        return JobLease(
            job_id=job_id,
            paper_id=paper_id,
            revision=revision,
            kind="index_revision",
            attempts=1,
            lease_token=token,
            lease_expires_at=self.now + timedelta(seconds=1800),
            created_at=self.now,
            previous_updated_at=self.now,
        )

    def test_delete_removes_every_paper_owned_artifact_but_keeps_submission(self):
        accepted, accept_intent = self.accept_submission()
        renamed = self.lifecycle.change_paper(
            EditMetadata(
                actor=Actor("contributor", 2),
                patch=MetadataPatch(
                    paper_id=accepted.paper_id,
                    expected_row_version=1,
                    changes=(("filename", "accepted-renamed.pdf"),),
                ),
            )
        )
        revised = self.lifecycle.change_paper(
            RevisePdf(
                actor=Actor("contributor", 2),
                paper_id=accepted.paper_id,
                expected_row_version=renamed.row_version,
                pdf=PdfUpload(
                    "replacement.pdf",
                    io.BytesIO(self.valid_pdf_bytes("replacement")),
                ),
            )
        )
        paper_id = accepted.paper_id
        original_flat = self.storage.papers_dir / "accepted-original.pdf"
        renamed_flat = self.storage.papers_dir / "accepted-renamed.pdf"
        for path, label in (
            (original_flat, "legacy-original"),
            (renamed_flat, "legacy-renamed"),
        ):
            path.write_bytes(self.valid_pdf_bytes(label))
            path.chmod(0o600)
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            session.add(
                PaperChunkModel(
                    filename=paper.filename,
                    paper_id=paper_id,
                    revision_number=paper.current_revision,
                    chunk_index=7,
                    content="owned chunk",
                    embedding_vec="[0.1]",
                    lang="en",
                )
            )
            session.add(
                PublishingMigrationJournalModel(
                    legacy_key="accepted-original.pdf",
                    paper_id=paper_id,
                    revision_number=1,
                    source_sha256="a" * 64,
                    source_size_bytes=123,
                    legacy_chunk_count=1,
                    legacy_chunk_fingerprint="b" * 64,
                    checkpoint="complete",
                    created_at=self.now,
                    updated_at=self.now,
                )
            )
            session.add(
                PublishingMigrationIssueModel(
                    id="99999999-9999-4999-8999-999999999999",
                    kind="submission_unmatched",
                    legacy_key="accepted-original.pdf",
                    paper_id=paper_id,
                    details="retained audit detail",
                    blocking=False,
                    created_at=self.now,
                    updated_at=self.now,
                )
            )
            session.add(
                PublishingMigrationStateModel(
                    name="inventory",
                    paper_count=1,
                    submission_count=1,
                    chunk_count=1,
                    vector_count=1,
                    ddl_phase="contract",
                    captured_at=self.now,
                )
            )
            session.commit()

        result = self.lifecycle.delete_paper(self.delete_intent(revised))

        self.assertEqual(result.state, DeletionState.DELETED)
        self.assertIsNone(self.paper_or_none(paper_id))
        for model in (
            PaperRevisionModel,
            PaperFilenameAliasModel,
            PaperChunkModel,
            PublishingJobModel,
            PublishingMigrationJournalModel,
            PublishingMigrationIssueModel,
        ):
            self.assertEqual(self.rows_for(model, paper_id), [])
        retained = self.submission("delete-submission")
        self.assertEqual(retained.status, "accepted")
        self.assertIsNone(retained.paper_id)
        self.assertEqual(retained.decision_idempotency_key, accept_intent.idempotency_key)
        with self.session_factory() as session:
            self.assertIsNotNone(session.get(PublishingMigrationStateModel, "inventory"))
        self.assertFalse(self.storage.revision_path(paper_id, 1).parent.exists())
        self.assertFalse(original_flat.exists())
        self.assertFalse(renamed_flat.exists())

        replay = self.lifecycle.review_submission(accept_intent)
        self.assertTrue(replay.accepted)
        self.assertTrue(replay.replayed)
        self.assertIsNone(replay.paper_id)
        self.assertEqual(self.papers(), [])

    def test_cleanup_failure_hides_paper_and_retry_completes_idempotently(self):
        published = self.publish()
        initial_stamp = self.stamp()
        intent = self.delete_intent(published)
        self.storage.fail_delete_once = True

        first = self.lifecycle.delete_paper(intent)

        self.assertEqual(first.state, DeletionState.DELETING)
        hidden = self.paper_or_none(published.paper_id)
        self.assertEqual(hidden.lifecycle_state, "deleting")
        self.assertEqual(hidden.row_version, published.row_version + 1)
        self.assertEqual(self.visible_papers(), [])
        aliases = self.rows_for(PaperFilenameAliasModel, published.paper_id)
        self.assertTrue(aliases)
        jobs = self.jobs(published.paper_id)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].kind, "delete_paper")
        self.assertEqual(jobs[0].state, "pending")
        self.assertIsNone(jobs[0].lease_token)
        self.assertNotIn("secret-token", jobs[0].last_error)
        self.assertEqual(self.stamp(), initial_stamp + 1)

        second = self.lifecycle.delete_paper(intent)
        second_job = self.jobs(published.paper_id)
        self.assertEqual(second.state, DeletionState.DELETING)
        self.assertEqual(len(second_job), 1)
        self.assertEqual(second_job[0].id, jobs[0].id)
        self.assertEqual(second_job[0].attempts, jobs[0].attempts)

        lease = self.claim_delete_job(published.paper_id)
        completed = self.lifecycle._run_delete_job(lease)
        replayed_private = self.lifecycle._run_delete_job(lease)

        self.assertEqual(completed.state, DeletionState.DELETED)
        self.assertEqual(replayed_private.state, DeletionState.DELETED)
        self.assertEqual(self.stamp(), initial_stamp + 2)
        with self.assertRaises(NotFound):
            self.lifecycle.delete_paper(intent)

    def test_stale_version_and_reader_are_rejected_before_hiding(self):
        published = self.publish()

        with self.assertRaises(StaleVersion) as stale:
            self.lifecycle.delete_paper(
                self.delete_intent(
                    published,
                    expected_row_version=published.row_version + 1,
                )
            )
        self.assertEqual(stale.exception.current_version, published.row_version)
        with self.assertRaises(Forbidden):
            self.lifecycle.delete_paper(self.delete_intent(published, role=1))

        self.assertEqual(self.paper_or_none(published.paper_id).lifecycle_state, "published")
        self.assertTrue(self.storage.revision_path(published.paper_id, 1).exists())
        self.assertEqual(self.jobs(published.paper_id), [])

    def test_delete_locks_paper_before_job_rows(self):
        published = self.publish()
        statements = []

        def capture(_connection, _cursor, statement, _parameters, _context, _many):
            normalized = " ".join(statement.lower().split())
            if normalized.startswith("select"):
                statements.append(normalized)

        event.listen(self.engine, "before_cursor_execute", capture)
        self.storage.fail_delete_once = True
        try:
            self.lifecycle.delete_paper(self.delete_intent(published))
        finally:
            event.remove(self.engine, "before_cursor_execute", capture)

        paper_select = next(
            index
            for index, statement in enumerate(statements)
            if " from papers_metadata " in statement
        )
        job_select = next(
            index
            for index, statement in enumerate(statements)
            if " from publishing_jobs " in statement
        )
        self.assertLess(paper_select, job_select)

    def test_obsolete_index_job_cannot_write_after_delete_marking(self):
        published = self.publish()
        old_lease = self.seed_running_index_job(published.paper_id, published.revision)
        self.storage.fail_delete_once = True

        result = self.lifecycle.delete_paper(self.delete_intent(published))
        wrote = self.lifecycle._complete_index(
            old_lease,
            PreparedRevisionIndex(
                paper_id=published.paper_id,
                revision=published.revision,
                chunks=(
                    PreparedChunk(
                        chunk_index=0,
                        content="must not appear",
                        embedding=(0.1,),
                        language="en",
                    ),
                ),
            ),
        )

        self.assertEqual(result.state, DeletionState.DELETING)
        self.assertFalse(wrote)
        self.assertEqual(self.rows_for(PaperChunkModel, published.paper_id), [])
        self.assertEqual(self.jobs(published.paper_id)[0].kind, "delete_paper")

    def test_stale_delete_lease_cannot_remove_hidden_paper(self):
        published = self.publish()
        self.storage.fail_delete_once = True
        self.lifecycle.delete_paper(self.delete_intent(published))
        current = self.claim_delete_job(published.paper_id)
        stale = JobLease(
            job_id=current.job_id,
            paper_id=current.paper_id,
            revision=0,
            kind="delete_paper",
            attempts=current.attempts,
            lease_token="eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            lease_expires_at=current.lease_expires_at,
            created_at=current.created_at,
            previous_updated_at=current.previous_updated_at,
        )

        progress = self.lifecycle._run_delete_job(stale)

        self.assertEqual(progress.state, DeletionState.DELETING)
        self.assertIsNotNone(self.paper_or_none(published.paper_id))
        self.assertTrue(self.storage.revision_path(published.paper_id, 1).exists())
        self.assertEqual(
            self.lifecycle._run_delete_job(current).state,
            DeletionState.DELETED,
        )

    def test_database_failure_after_storage_cleanup_is_retryable(self):
        published = self.publish()
        failing_factory = _CommitFailingFactory(self.session_factory, 2)
        failing_lifecycle = self.new_lifecycle(
            self.indexer,
            session_factory=failing_factory,
            storage=self.storage,
        )

        progress = failing_lifecycle.delete_paper(self.delete_intent(published))

        self.assertEqual(progress.state, DeletionState.DELETING)
        self.assertFalse(self.storage.revision_path(published.paper_id, 1).exists())
        self.assertEqual(self.paper_or_none(published.paper_id).lifecycle_state, "deleting")
        self.assertEqual(self.jobs(published.paper_id)[0].state, "pending")
        retry = self.claim_delete_job(published.paper_id)
        self.assertEqual(
            self.lifecycle._run_delete_job(retry).state,
            DeletionState.DELETED,
        )

    def test_delete_is_scoped_to_target_paper_and_global_audit(self):
        target = self.publish(filename="target.pdf")
        survivor = self.publish(filename="survivor.pdf")
        with self.session_factory() as session:
            session.add(
                PublishingMigrationStateModel(
                    name="global-audit",
                    paper_count=2,
                    submission_count=0,
                    chunk_count=0,
                    vector_count=0,
                    ddl_phase="contract",
                    captured_at=self.now,
                )
            )
            session.commit()

        self.lifecycle.delete_paper(self.delete_intent(target))

        self.assertIsNotNone(self.paper_or_none(survivor.paper_id))
        self.assertTrue(self.storage.revision_path(survivor.paper_id, 1).exists())
        self.assertIsNotNone(self.alias("survivor.pdf"))
        with self.session_factory() as session:
            self.assertIsNotNone(
                session.get(PublishingMigrationStateModel, "global-audit")
            )

    def test_unsafe_retained_alias_fails_closed_without_touching_outside_file(self):
        published = self.publish()
        outside = self.storage.papers_dir.parent / "outside.pdf"
        outside.write_bytes(self.valid_pdf_bytes("outside"))
        escaped = self.storage.papers_dir / "escaped.pdf"
        escaped.symlink_to(outside)
        with self.session_factory() as session:
            session.add(
                PaperFilenameAliasModel(
                    lookup_key=normalize_alias_key("escaped.pdf"),
                    filename="escaped.pdf",
                    paper_id=published.paper_id,
                    created_at=self.now,
                )
            )
            session.commit()

        progress = self.lifecycle.delete_paper(self.delete_intent(published))

        self.assertEqual(progress.state, DeletionState.DELETING)
        self.assertEqual(self.paper_or_none(published.paper_id).lifecycle_state, "deleting")
        self.assertTrue(outside.exists())
        self.assertTrue(escaped.is_symlink())


if __name__ == "__main__":
    unittest.main()
