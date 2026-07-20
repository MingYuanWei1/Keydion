import unittest

from sqlalchemy.dialects import mysql

from models import (
    PaperChunkModel, PaperFilenameAliasModel, PaperMetadataModel,
    PaperRevisionModel, PublishingJobModel, PublishingMigrationJournalModel,
    PublishingMigrationStateModel, SubmissionModel,
)


class PublishingModelTests(unittest.TestCase):
    def test_paper_identity_and_revision_columns_exist(self):
        self.assertTrue(PaperMetadataModel.id.primary_key)
        self.assertFalse(PaperMetadataModel.filename.primary_key)
        self.assertTrue(PaperMetadataModel.filename.unique)
        self.assertEqual(PaperMetadataModel.current_revision.nullable, True)
        self.assertEqual(PaperRevisionModel.__table__.primary_key.columns.keys(),
                         ["paper_id", "revision_number"])

    def test_chunk_identity_is_paper_and_revision(self):
        names = set(PaperChunkModel.__table__.columns.keys())
        self.assertIn("paper_id", names)
        self.assertIn("revision_number", names)
        self.assertIn("filename", names)  # compatibility-only until the later cleanup release

    def test_submission_persists_decision_history(self):
        names = set(SubmissionModel.__table__.columns.keys())
        self.assertTrue({"paper_id", "submitter_name", "reviewed_at", "reviewer",
                         "comment", "decision_idempotency_key",
                         "decision_payload_hash"}.issubset(names))

    def test_alias_and_job_dedupe_keys_are_unique(self):
        self.assertTrue(PaperFilenameAliasModel.lookup_key.primary_key)
        self.assertTrue(PublishingJobModel.dedupe_key.unique)

    def test_raw_filename_and_direct_idempotency_key_are_binary_on_mysql(self):
        dialect = mysql.dialect()
        for column_name in ("filename", "direct_idempotency_key"):
            column_type = PaperMetadataModel.__table__.c[column_name].type
            self.assertEqual(
                column_type.dialect_impl(dialect).collation,
                "utf8mb4_bin",
            )

    def test_migration_journal_has_stable_identity_and_checkpoint(self):
        self.assertTrue(PublishingMigrationJournalModel.legacy_key.primary_key)
        self.assertTrue(PublishingMigrationJournalModel.paper_id.unique)
        self.assertFalse(PublishingMigrationJournalModel.checkpoint.nullable)
        self.assertTrue(PublishingMigrationStateModel.name.primary_key)
