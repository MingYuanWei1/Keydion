"""Lifecycle tests for immutable Paper revisions and metadata patches."""

from __future__ import annotations

import io
import unittest
from datetime import timedelta
from unittest import mock

from models import (
    PaperChunkModel,
    PaperFilenameAliasModel,
    PaperMetadataModel,
    PaperRevisionModel,
    PublishingJobModel,
)
from services.paper_identity import normalize_alias_key
from services.paper_storage import StorageError
from services.publishing_contracts import (
    Actor,
    AliasConflict,
    BulkEditMetadata,
    DirectPublish,
    EditMetadata,
    Forbidden,
    IndexingState,
    InvalidInput,
    MetadataPatch,
    NormalizedPaperMetadata,
    NotFound,
    PersistenceFailed,
    PdfUpload,
    RevisePdf,
    RestoreRevision,
    StaleVersion,
    StorageFailed,
)
from tests.publishing_support import FakeRevisionIndexer, PublishingLifecycleTestCase


class _CommitFailingFactory:
    def __init__(self, session_factory):
        self._session_factory = session_factory
        self._failed = False

    def __call__(self):
        session = self._session_factory()
        real_commit = session.commit

        def fail_first_commit():
            if not self._failed:
                self._failed = True
                raise RuntimeError("injected final visibility failure")
            return real_commit()

        session.commit = fail_first_commit
        return session


class PublishingRevisionTests(PublishingLifecycleTestCase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._publication_number = 0

    def _direct_intent(self, *, filename=None, idempotency_key=None):
        self._publication_number += 1
        number = self._publication_number
        return DirectPublish(
            actor=Actor("contributor", 2),
            idempotency_key=idempotency_key or f"publish-key-{number}",
            metadata=NormalizedPaperMetadata(
                filename=filename or f"paper-{number}.pdf",
                title=f"Paper {number}",
                journal="Journal",
                category="original-category",
                language="en",
                author_name="Author",
                ib_ee_data="old-ee",
                cp_data="old-cp",
                ia_data="old-ia",
            ),
            pdf=PdfUpload("source.pdf", io.BytesIO(self.valid_pdf_bytes(f"first-{number}"))),
        )

    def publish(self, **kwargs):
        return self.lifecycle.publish_direct(self._direct_intent(**kwargs))

    def revisions(self, paper_id):
        with self.session_factory() as session:
            return [
                revision.revision_number
                for revision in (
                    session.query(PaperRevisionModel)
                    .filter(PaperRevisionModel.paper_id == paper_id)
                    .order_by(PaperRevisionModel.revision_number)
                    .all()
                )
            ]

    def revision(self, paper_id, number):
        with self.session_factory() as session:
            return session.get(PaperRevisionModel, (paper_id, number))

    def edit_intent(self, published, **changes):
        return EditMetadata(
            actor=Actor("contributor", 2),
            patch=MetadataPatch(
                paper_id=published.paper_id,
                expected_row_version=published.row_version,
                changes=tuple(changes.items()),
            ),
        )

    @staticmethod
    def revise_intent(published, pdf, *, role=2):
        return RevisePdf(
            actor=Actor("contributor", role),
            paper_id=published.paper_id,
            expected_row_version=published.row_version,
            pdf=PdfUpload("replacement.pdf", io.BytesIO(pdf)),
        )

    @staticmethod
    def restore_intent(published, source_revision, *, role=2):
        return RestoreRevision(
            actor=Actor("contributor", role),
            paper_id=published.paper_id,
            expected_row_version=published.row_version,
            revision=source_revision,
        )

    @staticmethod
    def bulk_intent(*patches, role=3):
        return BulkEditMetadata(Actor("curator", role), tuple(patches))

    def paper_row(self, paper_id):
        with self.session_factory() as session:
            return session.get(PaperMetadataModel, paper_id)

    def visible_chunks(self, paper_id):
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            return (
                session.query(PaperChunkModel)
                .filter(
                    PaperChunkModel.paper_id == paper_id,
                    PaperChunkModel.revision_number == paper.current_revision,
                )
                .all()
            )

    def test_metadata_edit_does_not_rewrite_or_append_pdf(self):
        published = self.publish()
        original = self.storage.revision_path(published.paper_id, 1).read_bytes()

        changed = self.lifecycle.change_paper(self.edit_intent(published, title="New title"))

        self.assertEqual(changed.revision, 1)
        self.assertEqual(changed.row_version, 2)
        self.assertEqual(changed.indexing.state, IndexingState.NOT_REQUIRED)
        self.assertEqual(self.revisions(published.paper_id), [1])
        self.assertEqual(self.storage.revision_path(published.paper_id, 1).read_bytes(), original)
        self.assertEqual(self.paper_row(published.paper_id).title, "New title")

    def test_metadata_edit_retains_existing_normalized_alias(self):
        published = self.publish(filename="Ｐａｐｅｒ.pdf")

        changed = self.lifecycle.change_paper(self.edit_intent(published, filename="Renamed.pdf"))

        old_alias = self.alias("paper.pdf")
        new_alias = self.alias("renamed.pdf")
        self.assertEqual(changed.filename, "Renamed.pdf")
        self.assertEqual(old_alias.paper_id, published.paper_id)
        self.assertEqual(old_alias.filename, "Ｐａｐｅｒ.pdf")
        self.assertEqual(new_alias.paper_id, published.paper_id)
        self.assertEqual(new_alias.filename, "Renamed.pdf")

    def test_metadata_filename_change_rejects_normalized_alias_collision(self):
        owner = self.publish(filename="Ｐａｐｅｒ.pdf")
        edited = self.publish(filename="other.pdf")

        with self.assertRaises(AliasConflict):
            self.lifecycle.change_paper(self.edit_intent(edited, filename="paper.pdf"))

        self.assertEqual(self.paper_row(edited.paper_id).filename, "other.pdf")
        self.assertEqual(self.paper_row(edited.paper_id).row_version, 1)
        self.assertEqual(self.alias("paper.pdf").paper_id, owner.paper_id)

    def test_pdf_replacement_appends_revision_and_preserves_first(self):
        published = self.publish()
        first = self.storage.revision_path(published.paper_id, 1).read_bytes()

        changed = self.lifecycle.change_paper(
            self.revise_intent(published, self.valid_pdf_bytes("second"))
        )

        self.assertEqual(changed.revision, 2)
        self.assertEqual(changed.row_version, 2)
        self.assertTrue(self.storage.revision_path(published.paper_id, 1).exists())
        self.assertTrue(self.storage.revision_path(published.paper_id, 2).exists())
        self.assertEqual(self.storage.revision_path(published.paper_id, 1).read_bytes(), first)
        self.assertEqual(self.revisions(published.paper_id), [1, 2])

    def test_restoration_appends_instead_of_rewinding(self):
        published = self.publish()
        revised = self.lifecycle.change_paper(
            self.revise_intent(published, self.valid_pdf_bytes("second"))
        )

        changed = self.lifecycle.change_paper(self.restore_intent(revised, 1))

        self.assertEqual(changed.revision, 3)
        self.assertEqual(self.revisions(published.paper_id), [1, 2, 3])
        self.assertEqual(self.revision(published.paper_id, 3).restored_from_revision, 1)
        self.assertEqual(
            self.storage.revision_path(published.paper_id, 3).read_bytes(),
            self.storage.revision_path(published.paper_id, 1).read_bytes(),
        )

    def test_stale_row_version_cannot_overwrite(self):
        published = self.publish()
        self.lifecycle.change_paper(self.edit_intent(published, category="first"))

        with self.assertRaises(StaleVersion):
            self.lifecycle.change_paper(self.edit_intent(published, category="second"))

        self.assertEqual(self.paper_row(published.paper_id).category, "first")

    def test_unknown_restore_source_is_not_found(self):
        published = self.publish()

        with self.assertRaises(NotFound):
            self.lifecycle.change_paper(self.restore_intent(published, 99))

        self.assertEqual(self.revisions(published.paper_id), [1])

    def test_replacement_storage_failure_keeps_current_revision(self):
        published = self.publish()
        with mock.patch.object(self.storage, "promote", side_effect=StorageError("disk full")):
            with self.assertRaises(StorageFailed):
                self.lifecycle.change_paper(
                    self.revise_intent(published, self.valid_pdf_bytes("second"))
                )

        paper = self.paper_row(published.paper_id)
        self.assertEqual(paper.current_revision, 1)
        self.assertEqual(paper.row_version, 1)
        self.assertEqual(self.revisions(published.paper_id), [1])

    def test_replacement_visibility_failure_removes_only_unreferenced_new_file(self):
        published = self.publish()
        failing_lifecycle = self.new_lifecycle(
            self.indexer,
            session_factory=_CommitFailingFactory(self.session_factory),
        )

        with self.assertRaises(PersistenceFailed):
            failing_lifecycle.change_paper(
                self.revise_intent(published, self.valid_pdf_bytes("second"))
            )

        self.assertEqual(self.revisions(published.paper_id), [1])
        self.assertFalse(self.storage.revision_path(published.paper_id, 2).exists())
        self.assertTrue(self.storage.revision_path(published.paper_id, 1).exists())

    def test_disabled_indexing_appends_revision_without_a_job(self):
        published = self.publish()
        self.lifecycle = self.new_lifecycle(FakeRevisionIndexer(enabled=False))

        changed = self.lifecycle.change_paper(
            self.revise_intent(published, self.valid_pdf_bytes("second"))
        )

        self.assertEqual(changed.indexing.state, IndexingState.NOT_REQUIRED)
        self.assertEqual(self.jobs(published.paper_id), [])
        self.assertEqual(self.paper_row(published.paper_id).current_revision, 2)

    def test_reader_cannot_edit_replace_or_restore(self):
        published = self.publish()
        reader_edit = EditMetadata(
            actor=Actor("reader", 1),
            patch=MetadataPatch(published.paper_id, published.row_version, (("category", "new"),)),
        )
        for intent in (
            reader_edit,
            self.revise_intent(published, self.valid_pdf_bytes("second"), role=1),
            self.restore_intent(published, 1, role=1),
        ):
            with self.subTest(intent=type(intent).__name__), self.assertRaises(Forbidden):
                self.lifecycle.change_paper(intent)

    def test_replacement_index_failure_keeps_new_revision_current_and_retryable(self):
        published = self.publish()
        with self.session_factory() as session:
            session.add(
                PaperChunkModel(
                    filename=self.paper_row(published.paper_id).filename,
                    paper_id=published.paper_id,
                    revision_number=1,
                    chunk_index=0,
                    content="revision one",
                    embedding_vec="[0.0]",
                    lang="en",
                )
            )
            session.commit()
        self.lifecycle = self.new_lifecycle(FakeRevisionIndexer(RuntimeError("provider down")))

        changed = self.lifecycle.change_paper(
            self.revise_intent(published, self.valid_pdf_bytes("second"))
        )

        paper = self.paper_row(published.paper_id)
        self.assertEqual(changed.revision, 2)
        self.assertEqual(changed.indexing.state, IndexingState.FAILED)
        self.assertEqual(paper.lifecycle_state, "published")
        self.assertEqual(paper.current_revision, 2)
        self.assertEqual(paper.index_status, "failed")
        self.assertEqual(self.visible_chunks(published.paper_id), [])
        jobs = self.jobs(published.paper_id)
        self.assertEqual([(job.revision_number, job.state) for job in jobs], [(2, "pending")])

    def test_bulk_metadata_requires_curator(self):
        published = self.publish()
        patch = MetadataPatch(published.paper_id, published.row_version, (("category", "new"),))

        with self.assertRaises(Forbidden):
            self.lifecycle.change_many_metadata(self.bulk_intent(patch, role=2))

        self.assertEqual(self.paper_row(published.paper_id).category, "original-category")

    def test_bulk_metadata_stale_row_rolls_back_every_paper(self):
        first = self.publish()
        second = self.publish()
        first_patch = MetadataPatch(first.paper_id, first.row_version, (("category", "first-new"),))
        stale_second = MetadataPatch(second.paper_id, second.row_version + 1, (("category", "second-new"),))

        with self.assertRaises(StaleVersion):
            self.lifecycle.change_many_metadata(self.bulk_intent(first_patch, stale_second))

        self.assertEqual(self.paper_row(first.paper_id).category, "original-category")
        self.assertEqual(self.paper_row(first.paper_id).row_version, 1)
        self.assertEqual(self.paper_row(second.paper_id).category, "original-category")
        self.assertEqual(self.paper_row(second.paper_id).row_version, 1)

    def test_bulk_metadata_sorts_results_and_updates_only_approved_fields(self):
        first = self.publish()
        second = self.publish()
        original_revisions = {
            first.paper_id: self.revisions(first.paper_id),
            second.paper_id: self.revisions(second.paper_id),
        }
        patches = (
            MetadataPatch(
                second.paper_id,
                second.row_version,
                (("category", "second-category"), ("ib_ee_data", "second-ee")),
            ),
            MetadataPatch(
                first.paper_id,
                first.row_version,
                (("journal", "first-journal"), ("ia_data", "first-ia"), ("cp_data", "first-cp")),
            ),
        )

        changed = self.lifecycle.change_many_metadata(self.bulk_intent(*patches))

        self.assertEqual([item.paper_id for item in changed.papers], sorted(original_revisions))
        first_row = self.paper_row(first.paper_id)
        second_row = self.paper_row(second.paper_id)
        self.assertEqual((first_row.journal, first_row.ia_data, first_row.cp_data), ("first-journal", "first-ia", "first-cp"))
        self.assertEqual((second_row.category, second_row.ib_ee_data), ("second-category", "second-ee"))
        self.assertEqual((first_row.id, second_row.id), (first.paper_id, second.paper_id))
        self.assertEqual(first_row.row_version, 2)
        self.assertEqual(second_row.row_version, 2)
        self.assertEqual(self.revisions(first.paper_id), original_revisions[first.paper_id])
        self.assertEqual(self.revisions(second.paper_id), original_revisions[second.paper_id])
        self.assertEqual(self.jobs(first.paper_id), [])
        self.assertEqual(self.jobs(second.paper_id), [])

    def test_bulk_contract_rejects_filename_lifecycle_index_and_identity_fields(self):
        published = self.publish()
        for field in ("filename", "lifecycle_state", "index_status", "id", "current_revision"):
            with self.subTest(field=field), self.assertRaises(InvalidInput):
                self.bulk_intent(
                    MetadataPatch(
                        published.paper_id,
                        published.row_version,
                        ((field, "forbidden"),),
                    )
                )
