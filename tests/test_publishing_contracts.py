import io
import unittest
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

from services.paper_identity import normalize_alias_key
from services.publishing_time import utc_iso_z
from services.publishing_contracts import (
    Actor, BulkEditMetadata, DirectPublish, IndexingOutcome, IndexingState, JobLease,
    JobProgress, JobState,
    InvalidInput, MetadataPatch, NormalizedPaperMetadata, PdfUpload, PreparedChunk,
    PreparedRevisionIndex, Published,
)


class PublishingContractTests(unittest.TestCase):
    def test_alias_normalization_has_one_unicode_casefold_policy(self):
        self.assertEqual(normalize_alias_key("ＰＡＰＥＲ.PDF"), "paper.pdf")

    def test_direct_publish_is_an_explicit_frozen_intent(self):
        intent = DirectPublish(
            actor=Actor(user_id="alice", role=2),
            idempotency_key="11111111-1111-4111-8111-111111111111",
            metadata=NormalizedPaperMetadata(filename="paper.pdf", title="Paper"),
            pdf=PdfUpload(filename="paper.pdf", stream=io.BytesIO(b"%PDF-1.4\n")),
        )
        self.assertEqual(intent.metadata.filename, "paper.pdf")
        with self.assertRaises(FrozenInstanceError):
            intent.idempotency_key = "changed"

    def test_index_failure_is_part_of_a_successful_publication(self):
        outcome = Published(
            paper_id="22222222-2222-4222-8222-222222222222",
            filename="paper.pdf",
            revision=1,
            row_version=1,
            replayed=False,
            indexing=IndexingOutcome(
                state=IndexingState.FAILED,
                job_id="33333333-3333-4333-8333-333333333333",
                next_retry_at=datetime(2026, 7, 20, 12, 0),
            ),
        )
        self.assertEqual(outcome.indexing.state, IndexingState.FAILED)
        self.assertEqual(outcome.paper_id[14], "4")

    def test_database_utc_serialization_is_explicit(self):
        value = datetime(2026, 7, 20, 12, 0)
        self.assertEqual(utc_iso_z(value), "2026-07-20T12:00:00Z")

    def test_metadata_patch_rejects_fields_outside_the_editable_subset(self):
        with self.assertRaises(InvalidInput):
            MetadataPatch(
                paper_id="22222222-2222-4222-8222-222222222222",
                expected_row_version=1,
                changes=(("title", "Not allowed"),),
            )

    def test_metadata_patch_rejects_mutable_or_non_string_pairs(self):
        with self.assertRaises(InvalidInput):
            MetadataPatch(
                paper_id="22222222-2222-4222-8222-222222222222",
                expected_row_version=1,
                changes=(["journal", "Allowed but mutable"],),
            )
        with self.assertRaises(InvalidInput):
            MetadataPatch(
                paper_id="22222222-2222-4222-8222-222222222222",
                expected_row_version=1,
                changes=(("journal", 1),),
            )

    def test_prepared_chunk_rejects_mutable_embeddings(self):
        with self.assertRaises(InvalidInput):
            PreparedChunk(
                chunk_index=0,
                content="content",
                embedding=[0.5],
                language="en",
            )

    def test_prepared_revision_index_rejects_mutable_or_invalid_chunks(self):
        chunk = PreparedChunk(0, "content", (0.5,), "en")
        with self.assertRaises(InvalidInput):
            PreparedRevisionIndex("22222222-2222-4222-8222-222222222222", 1, [chunk])
        with self.assertRaises(InvalidInput):
            PreparedRevisionIndex("22222222-2222-4222-8222-222222222222", 1, ("not a chunk",))

    def test_bulk_edit_metadata_rejects_non_patch_members(self):
        with self.assertRaises(InvalidInput):
            BulkEditMetadata(Actor("curator", 3), ("not a patch",))

    def test_job_lease_rejects_aware_database_timestamps(self):
        aware = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
        for field in ("lease_expires_at", "created_at", "previous_updated_at"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                values = {
                    "lease_expires_at": datetime(2026, 7, 20, 12, 0),
                    "created_at": datetime(2026, 7, 20, 12, 0),
                    "previous_updated_at": datetime(2026, 7, 20, 12, 0),
                }
                values[field] = aware
                JobLease(
                    job_id="33333333-3333-4333-8333-333333333333",
                    paper_id="22222222-2222-4222-8222-222222222222",
                    revision=1,
                    kind="index",
                    attempts=1,
                    lease_token="lease",
                    **values,
                )

    def test_indexing_outcome_rejects_aware_next_retry_at(self):
        with self.assertRaises(ValueError):
            IndexingOutcome(
                state=IndexingState.PENDING,
                next_retry_at=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
            )

    def test_job_progress_rejects_aware_next_retry_at(self):
        with self.assertRaises(ValueError):
            JobProgress(
                job_id="33333333-3333-4333-8333-333333333333",
                paper_id="22222222-2222-4222-8222-222222222222",
                revision=1,
                state=JobState.PENDING,
                attempts=0,
                next_retry_at=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
            )
