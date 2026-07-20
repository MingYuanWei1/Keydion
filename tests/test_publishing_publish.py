import io
import threading
import unittest
from dataclasses import asdict
from datetime import timedelta
from unittest import mock

from models import (
    PaperChunkModel,
    PaperFilenameAliasModel,
    PaperMetadataModel,
    PaperRevisionModel,
    PublishingJobModel,
    RagIndexMetaModel,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from services.paper_storage import StorageError
from services.publishing_contracts import (
    Actor,
    DirectPublish,
    Forbidden,
    IdempotencyConflict,
    IndexDeadlineExceeded,
    IndexingState,
    InvalidInput,
    NormalizedPaperMetadata,
    PdfUpload,
    PersistenceFailed,
    PreparedChunk,
    PreparedRevisionIndex,
    StorageFailed,
)
from tests.publishing_support import FakeRevisionIndexer, PublishingLifecycleTestCase


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


class AliasRaceFactory(CommitFailingFactory):
    def __call__(self):
        session = self.session_factory()
        real_commit = session.commit

        def commit():
            self.commits += 1
            if self.commits != self.failure_number:
                return real_commit()
            session.rollback()
            now = session.info["race_now"]
            with self.session_factory() as competitor:
                owner_id = "00000000-0000-4000-8000-999999999999"
                owner = PaperMetadataModel(
                    id=owner_id,
                    filename="owner.pdf",
                    title="Owner",
                    language="en",
                    lifecycle_state="published",
                    current_revision=1,
                    row_version=1,
                    index_status="pending",
                )
                competitor.add(owner)
                competitor.flush()
                competitor.add(
                    PaperRevisionModel(
                        paper_id=owner_id,
                        revision_number=1,
                        sha256="0" * 64,
                        size_bytes=1,
                        created_at=now,
                        created_by="owner",
                    )
                )
                competitor.add(
                    PaperFilenameAliasModel(
                        lookup_key="paper.pdf",
                        filename="owner.pdf",
                        paper_id=owner_id,
                        created_at=now,
                    )
                )
                competitor.commit()
            raise IntegrityError("simulated alias race", {}, RuntimeError("duplicate"))

        session.info["race_now"] = self.race_now
        session.commit = commit
        return session


class ReservationInsertRaceFactory:
    """Replace the first reservation commit with a competing identical insert."""

    def __init__(self, session_factory):
        self.session_factory = session_factory
        self.injected = False

    def __call__(self):
        session = self.session_factory()
        real_commit = session.commit

        def commit():
            reservations = [
                row for row in session.new if isinstance(row, PaperMetadataModel)
            ]
            if self.injected or len(reservations) != 1:
                return real_commit()
            self.injected = True
            reservation = reservations[0]
            values = {
                column.name: getattr(reservation, column.name)
                for column in PaperMetadataModel.__table__.columns
            }
            session.rollback()
            with self.session_factory() as competitor:
                competitor.add(PaperMetadataModel(**values))
                competitor.commit()
            raise IntegrityError("simulated reservation race", {}, RuntimeError("duplicate"))

        session.commit = commit
        return session


class PublishingPublishTests(PublishingLifecycleTestCase, unittest.TestCase):
    def direct_intent(self, **metadata_changes):
        metadata = {
            "filename": "paper.pdf",
            "title": "Paper",
            "journal": "Journal",
            "category": "science",
            "language": "en",
            "keywords": "one, two",
            "abstract": "Abstract",
            "author_name": "Alice",
            "author_email": "alice@example.test",
            "author_school": "Example School",
            "published_at": "2026-07-21",
        }
        metadata.update(metadata_changes)
        return DirectPublish(
            actor=Actor(user_id="alice", role=2),
            idempotency_key="11111111-1111-4111-8111-111111111111",
            metadata=NormalizedPaperMetadata(**metadata),
            pdf=PdfUpload("source.pdf", io.BytesIO(self.valid_pdf_bytes())),
        )

    def with_key(self, intent, key):
        return DirectPublish(
            actor=intent.actor,
            idempotency_key=key,
            metadata=intent.metadata,
            pdf=intent.pdf,
        )

    def seed_hidden_reservation(self, *, key=None, expires_at=None, with_final=True):
        intent = self.direct_intent()
        if key is not None:
            intent = self.with_key(intent, key)
        paper_id = "00000000-0000-4000-8000-888888888888"
        staged = self.storage.stage(intent.pdf, "seed-hidden")
        payload_hash = self.lifecycle._payload_hash(intent, staged.source_sha256)
        prepared = self.storage.apply_metadata(
            staged,
            title=intent.metadata.title,
            author=intent.metadata.author_name,
        )
        if with_final:
            self.storage.promote(prepared, paper_id, 1)
        else:
            self.storage.discard_stage(prepared)
        with self.session_factory() as session:
            session.add(
                PaperMetadataModel(
                    id=paper_id,
                    **asdict(intent.metadata),
                    lifecycle_state="publishing",
                    current_revision=None,
                    row_version=0,
                    index_status="pending",
                    direct_idempotency_key=intent.idempotency_key,
                    direct_payload_hash=payload_hash,
                    reservation_expires_at=expires_at or self.now - timedelta(seconds=1),
                )
            )
            session.commit()
        return paper_id

    def assert_stale_index_completion_is_ignored(self, mutate):
        class MutatingIndexer(FakeRevisionIndexer):
            def prepare(inner, **kwargs):
                super(MutatingIndexer, inner).prepare(**kwargs)
                with self.session_factory() as session:
                    paper = session.get(PaperMetadataModel, kwargs["paper_id"])
                    job = session.query(PublishingJobModel).one()
                    session.add(
                        PaperChunkModel(
                            filename=paper.filename,
                            paper_id=paper.id,
                            revision_number=kwargs["revision_number"],
                            chunk_index=9,
                            content="old chunk",
                            embedding_vec="[0.0]",
                            lang="en",
                        )
                    )
                    mutate(session, paper, job)
                    session.commit()
                return PreparedRevisionIndex(
                    paper_id=kwargs["paper_id"],
                    revision=kwargs["revision_number"],
                    chunks=(PreparedChunk(0, "late chunk", (1.0,), "en"),),
                )

        self.lifecycle = self.new_lifecycle(MutatingIndexer())
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, outcome.paper_id)
            contents = [
                chunk.content
                for chunk in session.query(PaperChunkModel)
                .filter(PaperChunkModel.paper_id == outcome.paper_id)
                .order_by(PaperChunkModel.chunk_index)
            ]
            self.assertNotIn("late chunk", contents)
            self.assertEqual(
                session.get(RagIndexMetaModel, "chunks_version").value,
                1,
            )
            if paper is not None:
                self.assertEqual(paper.index_status, "pending")
                self.assertIsNone(paper.indexed_revision)
                self.assertIsNone(paper.index_error)
                self.assertEqual(contents, ["old chunk"])
            else:
                self.assertEqual(contents, [])

    def test_publish_direct_commits_record_pdf_alias_and_revision(self):
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.revision, 1)
        self.assertEqual(outcome.indexing.state, IndexingState.INDEXED)
        self.assertTrue(self.storage.revision_path(outcome.paper_id, 1).exists())
        self.assertEqual(self.paper(outcome.paper_id).lifecycle_state, "published")
        self.assertEqual(self.alias("paper.pdf").paper_id, outcome.paper_id)
        with self.session_factory() as session:
            revision = session.get(PaperRevisionModel, (outcome.paper_id, 1))
            self.assertGreater(revision.size_bytes, 100)

    def test_default_expiring_session_keeps_reservation_id_across_commit(self):
        default_factory = sessionmaker(bind=self.engine)
        self.assertTrue(default_factory.kw["expire_on_commit"])
        lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False),
            session_factory=default_factory,
        )

        outcome = lifecycle.publish_direct(self.direct_intent())

        self.assertEqual(
            outcome.paper_id,
            "00000000-0000-4000-8000-000000000002",
        )
        self.assertEqual(self.paper(outcome.paper_id).lifecycle_state, "published")

    def test_initial_index_attempt_has_exact_request_lease_and_deadline(self):
        observed = {}

        class InspectingIndexer(FakeRevisionIndexer):
            def prepare(inner, **kwargs):
                with self.session_factory() as session:
                    job = session.query(PublishingJobModel).one()
                    observed.update(
                        state=job.state,
                        attempts=job.attempts,
                        available_at=job.available_at,
                        lease_expires_at=job.lease_expires_at,
                        lease_token=job.lease_token,
                    )
                observed["deadline"] = kwargs["deadline"]
                return super(InspectingIndexer, inner).prepare(**kwargs)

        self.lifecycle = self.new_lifecycle(InspectingIndexer())
        self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(observed["state"], "running")
        self.assertEqual(observed["attempts"], 1)
        self.assertEqual(observed["available_at"], self.now)
        self.assertEqual(observed["lease_expires_at"], self.now + timedelta(seconds=1800))
        self.assertTrue(observed["lease_token"])
        self.assertEqual(observed["deadline"], self.monotonic_now + 45)

    def test_successful_index_replaces_chunks_and_bumps_version_atomically(self):
        class ChunkIndexer(FakeRevisionIndexer):
            def prepare(inner, **kwargs):
                super(ChunkIndexer, inner).prepare(**kwargs)
                return PreparedRevisionIndex(
                    paper_id=kwargs["paper_id"],
                    revision=kwargs["revision_number"],
                    chunks=(
                        PreparedChunk(
                            chunk_index=0,
                            content="indexed text",
                            embedding=(0.25, 0.75),
                            language=kwargs["language"],
                        ),
                    ),
                )

        self.lifecycle = self.new_lifecycle(ChunkIndexer())
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.INDEXED)
        with self.session_factory() as session:
            chunks = session.query(PaperChunkModel).all()
            self.assertEqual(
                [(chunk.paper_id, chunk.revision_number, chunk.content) for chunk in chunks],
                [(outcome.paper_id, 1, "indexed text")],
            )
            from models import RagIndexMetaModel

            self.assertEqual(session.get(RagIndexMetaModel, "chunks_version").value, 2)
            self.assertEqual(session.query(PublishingJobModel).count(), 0)

    def test_successful_index_replaces_preexisting_chunks(self):
        class ReplacingIndexer(FakeRevisionIndexer):
            def prepare(inner, **kwargs):
                with self.session_factory() as session:
                    session.add(
                        PaperChunkModel(
                            filename="paper.pdf",
                            paper_id=kwargs["paper_id"],
                            revision_number=kwargs["revision_number"],
                            chunk_index=9,
                            content="old chunk",
                            embedding_vec="[0.0]",
                            lang="en",
                        )
                    )
                    session.commit()
                return PreparedRevisionIndex(
                    paper_id=kwargs["paper_id"],
                    revision=kwargs["revision_number"],
                    chunks=(
                        PreparedChunk(0, "new chunk", (1.0,), "en"),
                    ),
                )

        self.lifecycle = self.new_lifecycle(ReplacingIndexer())
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        with self.session_factory() as session:
            chunks = session.query(PaperChunkModel).all()
            self.assertEqual(
                [(chunk.paper_id, chunk.revision_number, chunk.chunk_index, chunk.content)
                 for chunk in chunks],
                [(outcome.paper_id, 1, 0, "new chunk")],
            )

    def test_index_failure_keeps_paper_visible_and_schedules_retry(self):
        self.lifecycle = self.new_lifecycle(
            FakeRevisionIndexer(RuntimeError("provider down; token=secret"))
        )
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.FAILED)
        paper = self.paper(outcome.paper_id)
        self.assertEqual(paper.lifecycle_state, "published")
        self.assertEqual(paper.index_status, "failed")
        self.assertNotIn("secret", paper.index_error)
        jobs = self.jobs(outcome.paper_id)
        self.assertEqual(jobs[0].state, "pending")
        self.assertNotIn("secret", jobs[0].last_error)

    def test_same_idempotency_key_replays_original_paper(self):
        first = self.lifecycle.publish_direct(self.direct_intent())
        second = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(second.paper_id, first.paper_id)
        self.assertTrue(second.replayed)
        self.assertEqual(len(self.papers()), 1)
        self.assertEqual(self.staged_entries(), [])

    def test_same_key_with_different_payload_conflicts(self):
        self.lifecycle.publish_direct(self.direct_intent())
        with self.assertRaises(IdempotencyConflict):
            self.lifecycle.publish_direct(self.direct_intent(title="Different"))
        self.assertEqual(self.staged_entries(), [])

    def test_blank_or_unnormalized_idempotency_key_is_invalid(self):
        original = self.direct_intent()
        invalid = DirectPublish(
            actor=original.actor,
            idempotency_key=" unnormalized ",
            metadata=original.metadata,
            pdf=original.pdf,
        )
        with self.assertRaises(InvalidInput) as raised:
            self.lifecycle.publish_direct(invalid)
        self.assertIn("idempotency_key", raised.exception.field_errors)
        self.assertEqual(self.papers(), [])

    def test_request_deadline_failure_keeps_publication_and_job(self):
        self.lifecycle = self.new_lifecycle(FakeRevisionIndexer(IndexDeadlineExceeded()))
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.FAILED)
        self.assertEqual(self.jobs(outcome.paper_id)[0].state, "pending")

    def test_deadline_equality_is_exhausted_after_preparation(self):
        class EqualityIndexer(FakeRevisionIndexer):
            def prepare(inner, **kwargs):
                prepared = super(EqualityIndexer, inner).prepare(**kwargs)
                self.monotonic_now = kwargs["deadline"]
                return prepared

        self.lifecycle = self.new_lifecycle(EqualityIndexer())
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.FAILED)
        self.assertEqual(self.jobs(outcome.paper_id)[0].state, "pending")

    def test_index_persistence_failure_keeps_publication_and_releases_job(self):
        failing_factory = CommitFailingFactory(self.session_factory, failure_number=3)
        lifecycle = self.new_lifecycle(session_factory=failing_factory)
        outcome = lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.FAILED)
        self.assertEqual(self.paper(outcome.paper_id).lifecycle_state, "published")
        self.assertEqual(self.paper(outcome.paper_id).index_status, "failed")
        self.assertEqual(self.jobs(outcome.paper_id)[0].state, "pending")

    def test_disabled_rag_is_not_a_failure_and_creates_no_job(self):
        indexer = FakeRevisionIndexer(enabled=False)
        self.lifecycle = self.new_lifecycle(indexer)
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.NOT_REQUIRED)
        self.assertEqual(self.paper(outcome.paper_id).index_status, "pending")
        self.assertEqual(self.jobs(outcome.paper_id), [])
        self.assertEqual(indexer.calls, [])

    def test_reader_is_forbidden_before_storage_or_persistence(self):
        intent = self.direct_intent()
        intent = DirectPublish(
            actor=Actor(user_id="reader", role=1),
            idempotency_key=intent.idempotency_key,
            metadata=intent.metadata,
            pdf=intent.pdf,
        )
        with mock.patch.object(self.storage, "stage", wraps=self.storage.stage) as stage:
            with self.assertRaises(Forbidden):
                self.lifecycle.publish_direct(intent)
        stage.assert_not_called()
        self.assertEqual(self.papers(), [])

    def test_unknown_role_is_invalid_and_never_stages(self):
        intent = self.direct_intent()
        invalid = DirectPublish(
            actor=Actor(user_id="alice", role=4),
            idempotency_key=intent.idempotency_key,
            metadata=intent.metadata,
            pdf=intent.pdf,
        )
        with mock.patch.object(self.storage, "stage", wraps=self.storage.stage) as stage:
            with self.assertRaises(InvalidInput) as raised:
                self.lifecycle.publish_direct(invalid)
        self.assertEqual(raised.exception.field_errors, {"actor": "is invalid"})
        stage.assert_not_called()

    def test_actor_user_id_must_be_stripped_and_nonblank(self):
        intent = self.direct_intent()
        invalid = DirectPublish(
            actor=Actor(user_id=" alice ", role=2),
            idempotency_key=intent.idempotency_key,
            metadata=intent.metadata,
            pdf=intent.pdf,
        )
        with self.assertRaises(InvalidInput) as raised:
            self.lifecycle.publish_direct(invalid)
        self.assertEqual(raised.exception.field_errors, {"actor": "must identify a user"})

    def test_invalid_normalized_metadata_has_field_errors(self):
        with self.assertRaises(InvalidInput) as raised:
            self.lifecycle.publish_direct(self.direct_intent(title=" Paper ", filename="../x.pdf"))
        self.assertEqual(set(raised.exception.field_errors), {"filename", "title"})
        self.assertEqual(self.papers(), [])

    def test_non_metadata_record_is_reported_as_invalid_input(self):
        intent = self.direct_intent()
        invalid = DirectPublish(
            actor=intent.actor,
            idempotency_key=intent.idempotency_key,
            metadata="not metadata",
            pdf=intent.pdf,
        )
        with self.assertRaises(InvalidInput) as raised:
            self.lifecycle.publish_direct(invalid)
        self.assertEqual(raised.exception.field_errors, {"metadata": "is invalid"})

    def test_non_pdf_is_rejected_before_any_reservation_is_visible(self):
        intent = self.direct_intent()
        intent = DirectPublish(
            actor=intent.actor,
            idempotency_key=intent.idempotency_key,
            metadata=intent.metadata,
            pdf=PdfUpload("source.pdf", io.BytesIO(b"not a PDF")),
        )
        with self.assertRaises(StorageFailed):
            self.lifecycle.publish_direct(intent)
        self.assertEqual(self.papers(), [])

    def test_storage_failure_keeps_only_a_hidden_reservation(self):
        with mock.patch.object(
            self.storage, "promote", side_effect=StorageError("disk full")
        ):
            with self.assertRaises(StorageFailed):
                self.lifecycle.publish_direct(self.direct_intent())
        papers = self.papers()
        self.assertEqual(len(papers), 1)
        self.assertEqual(papers[0].lifecycle_state, "publishing")
        self.assertIsNone(papers[0].current_revision)
        self.assertFalse(self.storage.revision_path(papers[0].id, 1).exists())

    def test_replay_resumes_valid_hidden_reservation_with_same_uuid(self):
        real_promote = self.storage.promote
        with mock.patch.object(
            self.storage, "promote", side_effect=StorageError("temporary disk failure")
        ):
            with self.assertRaises(StorageFailed):
                self.lifecycle.publish_direct(self.direct_intent())
        reserved_id = self.papers()[0].id
        with mock.patch.object(self.storage, "promote", wraps=real_promote):
            outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.paper_id, reserved_id)
        self.assertFalse(outcome.replayed)
        self.assertEqual(self.paper(reserved_id).lifecycle_state, "published")

    def test_visibility_commit_failure_reconciles_unreferenced_final(self):
        failing_factory = CommitFailingFactory(self.session_factory, failure_number=2)
        lifecycle = self.new_lifecycle(session_factory=failing_factory)
        with self.assertRaises(PersistenceFailed):
            lifecycle.publish_direct(self.direct_intent())
        papers = self.papers()
        self.assertEqual(len(papers), 1)
        self.assertEqual(papers[0].lifecycle_state, "publishing")
        self.assertFalse(self.storage.revision_path(papers[0].id, 1).exists())
        with self.session_factory() as session:
            self.assertIsNone(session.get(PaperRevisionModel, (papers[0].id, 1)))

    def test_alias_normalization_blocks_a_second_visible_owner(self):
        self.lifecycle.publish_direct(self.direct_intent(filename="ＰＡＰＥＲ.PDF"))
        other = self.direct_intent(filename="paper.pdf")
        other = DirectPublish(
            actor=other.actor,
            idempotency_key="22222222-2222-4222-8222-222222222222",
            metadata=other.metadata,
            pdf=other.pdf,
        )
        from services.publishing_contracts import AliasConflict

        with self.assertRaises(AliasConflict):
            self.lifecycle.publish_direct(other)
        self.assertEqual(
            [paper.lifecycle_state for paper in self.papers()].count("published"), 1
        )
        self.assertEqual(self.staged_entries(), [])

    def test_visibility_alias_race_raises_alias_conflict_and_removes_candidate(self):
        from services.publishing_contracts import AliasConflict

        factory = AliasRaceFactory(self.session_factory, failure_number=2)
        factory.race_now = self.now
        lifecycle = self.new_lifecycle(session_factory=factory)
        with self.assertRaises(AliasConflict):
            lifecycle.publish_direct(self.direct_intent())
        candidates = [paper for paper in self.papers() if paper.filename == "paper.pdf"]
        self.assertEqual(candidates, [])
        self.assertEqual(self.alias("paper.pdf").filename, "owner.pdf")
        self.assertEqual(self.staged_entries(), [])

    def test_late_preparation_cannot_overwrite_a_newer_or_lost_lease(self):
        class LosingIndexer(FakeRevisionIndexer):
            def prepare(inner, **kwargs):
                prepared = super(LosingIndexer, inner).prepare(**kwargs)
                with self.session_factory() as session:
                    job = session.query(PublishingJobModel).one()
                    job.lease_token = "00000000-0000-4000-8000-999999999999"
                    job.state = "pending"
                    session.commit()
                return prepared

        self.lifecycle = self.new_lifecycle(LosingIndexer())
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.PENDING)
        self.assertEqual(self.paper(outcome.paper_id).index_status, "pending")
        self.assertEqual(self.jobs(outcome.paper_id)[0].state, "pending")

    def test_changed_lease_token_cannot_mutate_chunks_or_index_stamp(self):
        def change_token(_session, _paper, job):
            job.lease_token = "00000000-0000-4000-8000-999999999999"

        self.assert_stale_index_completion_is_ignored(change_token)

    def test_expired_lease_cannot_mutate_chunks_or_index_stamp(self):
        def expire(_session, _paper, job):
            job.lease_expires_at = self.now

        self.assert_stale_index_completion_is_ignored(expire)

    def test_missing_lease_token_cannot_mutate_chunks_or_index_stamp(self):
        def remove_token(_session, _paper, job):
            job.lease_token = None

        self.assert_stale_index_completion_is_ignored(remove_token)

    def test_missing_job_cannot_mutate_chunks_or_index_stamp(self):
        def remove_job(session, _paper, job):
            session.delete(job)

        self.assert_stale_index_completion_is_ignored(remove_job)

    def test_deleting_paper_cannot_mutate_chunks_or_index_stamp(self):
        def begin_deleting(_session, paper, _job):
            paper.lifecycle_state = "deleting"

        self.assert_stale_index_completion_is_ignored(begin_deleting)

    def test_superseded_revision_cannot_mutate_chunks_or_index_stamp(self):
        def supersede(_session, paper, _job):
            paper.current_revision = 2

        self.assert_stale_index_completion_is_ignored(supersede)

    def test_missing_paper_cannot_mutate_chunks_or_rag_stamp(self):
        def remove_paper(session, paper, _job):
            session.delete(paper)

        self.assert_stale_index_completion_is_ignored(remove_paper)

    def test_malformed_preparation_fails_and_releases_matching_lease(self):
        class WrongTargetIndexer(FakeRevisionIndexer):
            def prepare(inner, **kwargs):
                super(WrongTargetIndexer, inner).prepare(**kwargs)
                return PreparedRevisionIndex(
                    paper_id="00000000-0000-4000-8000-999999999999",
                    revision=kwargs["revision_number"],
                    chunks=(),
                )

        self.lifecycle = self.new_lifecycle(WrongTargetIndexer())
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.FAILED)
        self.assertEqual(self.paper(outcome.paper_id).index_status, "failed")
        self.assertEqual(self.jobs(outcome.paper_id)[0].state, "pending")

    def test_indexer_enabled_probe_failure_is_nonblocking_and_retryable(self):
        class BrokenProbeIndexer(FakeRevisionIndexer):
            def enabled(inner):
                raise RuntimeError("configuration source unavailable; token=secret")

        self.lifecycle = self.new_lifecycle(BrokenProbeIndexer())
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(outcome.indexing.state, IndexingState.FAILED)
        self.assertEqual(self.paper(outcome.paper_id).lifecycle_state, "published")
        self.assertEqual(self.jobs(outcome.paper_id)[0].state, "pending")
        self.assertNotIn("secret", self.paper(outcome.paper_id).index_error)
        self.assertEqual(self.staged_entries(), [])

    def test_metadata_rewrite_failure_discards_unconsumed_stage(self):
        with mock.patch.object(
            self.storage,
            "apply_metadata",
            side_effect=StorageError("metadata writer failed"),
        ):
            with self.assertRaises(StorageFailed):
                self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(self.staged_entries(), [])

    def test_reservation_failure_discards_unconsumed_stage(self):
        factory = CommitFailingFactory(self.session_factory, failure_number=1)
        lifecycle = self.new_lifecycle(session_factory=factory)
        with self.assertRaises(PersistenceFailed):
            lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(self.staged_entries(), [])

    def test_payload_hash_includes_all_normalized_fields_and_source_bytes(self):
        first = self.direct_intent()
        with mock.patch.object(self.storage, "promote", side_effect=StorageError("stop")):
            with self.assertRaises(StorageFailed):
                self.lifecycle.publish_direct(first)
        changed = self.direct_intent(ia_data='{"subject":"Math"}')
        with self.assertRaises(IdempotencyConflict):
            self.lifecycle.publish_direct(changed)

    def test_expired_hidden_reservation_is_replaced_and_old_final_removed(self):
        with mock.patch.object(self.storage, "promote", side_effect=StorageError("stop")):
            with self.assertRaises(StorageFailed):
                self.lifecycle.publish_direct(self.direct_intent())
        old = self.papers()[0]
        with self.session_factory() as session:
            row = session.get(PaperMetadataModel, old.id)
            row.reservation_expires_at = self.now - timedelta(seconds=1)
            session.commit()
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertNotEqual(outcome.paper_id, old.id)
        self.assertIsNone(self.paper(old.id))

    def test_expired_reservation_removes_real_orphan_final_before_row(self):
        old_id = self.seed_hidden_reservation()
        outcome = self.lifecycle.publish_direct(self.direct_intent())
        self.assertNotEqual(outcome.paper_id, old_id)
        self.assertIsNone(self.paper(old_id))
        self.assertFalse(self.storage.revision_path(old_id, 1).exists())

    def test_expired_cleanup_failure_retains_reservation_and_idempotency(self):
        old_id = self.seed_hidden_reservation()
        with mock.patch.object(
            self.storage,
            "delete_paper",
            side_effect=StorageError("cleanup unavailable"),
        ):
            with self.assertRaises(StorageFailed):
                self.lifecycle.publish_direct(self.direct_intent())
        retained = self.paper(old_id)
        self.assertIsNotNone(retained)
        self.assertEqual(retained.lifecycle_state, "publishing")
        self.assertTrue(self.storage.revision_path(old_id, 1).exists())

    def test_different_key_reconciles_expired_same_filename_reservation(self):
        old_id = self.seed_hidden_reservation(key="old-key")
        intent = self.with_key(self.direct_intent(), "new-key")
        outcome = self.lifecycle.publish_direct(intent)
        self.assertNotEqual(outcome.paper_id, old_id)
        self.assertIsNone(self.paper(old_id))
        self.assertFalse(self.storage.revision_path(old_id, 1).exists())

    def test_reservation_expiring_before_visibility_is_reconciled_not_published(self):
        class ExpiringProbe(FakeRevisionIndexer):
            def enabled(inner):
                self.now += timedelta(hours=2)
                return False

        self.lifecycle = self.new_lifecycle(ExpiringProbe())
        with self.assertRaises(PersistenceFailed):
            self.lifecycle.publish_direct(self.direct_intent())
        self.assertEqual(self.papers(), [])
        self.assertEqual(list(self.storage.papers_dir.glob("*/*.pdf")), [])

    def test_compensation_cannot_leave_sibling_visibility_without_final(self):
        ready = threading.Event()
        release = threading.Event()
        sibling_results = []

        class PausingProbe(FakeRevisionIndexer):
            def enabled(inner):
                ready.set()
                if not release.wait(timeout=5):
                    raise RuntimeError("test synchronization timed out")
                return False

        sibling = self.new_lifecycle(PausingProbe())

        def publish_sibling():
            try:
                sibling_results.append(sibling.publish_direct(self.direct_intent()))
            except Exception as exc:
                sibling_results.append(exc)

        thread = threading.Thread(target=publish_sibling)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        failing_factory = CommitFailingFactory(self.session_factory, failure_number=2)
        failing = self.new_lifecycle(
            FakeRevisionIndexer(enabled=False),
            session_factory=failing_factory,
        )
        with self.assertRaises(PersistenceFailed):
            failing.publish_direct(self.direct_intent())
        release.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(sibling_results), 1)
        self.assertIsInstance(sibling_results[0], StorageFailed)
        self.assertEqual(
            [paper for paper in self.papers() if paper.lifecycle_state == "published"],
            [],
        )
        hidden = self.papers()[0]
        self.assertFalse(self.storage.revision_path(hidden.id, 1).exists())

    def test_insert_race_reuses_winning_hidden_uuid_and_publishes_once(self):
        factory = ReservationInsertRaceFactory(self.session_factory)
        lifecycle = self.new_lifecycle(session_factory=factory)
        outcome = lifecycle.publish_direct(self.direct_intent())
        self.assertTrue(factory.injected)
        self.assertEqual(len(self.papers()), 1)
        self.assertEqual(self.papers()[0].id, outcome.paper_id)
        self.assertEqual(len(self.jobs(outcome.paper_id)), 0)


if __name__ == "__main__":
    unittest.main()
