from __future__ import annotations

import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import db
from models import AttachmentChunkModel, AttachmentJobModel, BASE, ConversationModel
from services import attachment_jobs
from services.attachment_processing import (
    AttachmentProcessingError,
    extract_in_subprocess,
    preflight_docx,
)


def _docx(entries: dict[str, bytes], *, compression=zipfile.ZIP_DEFLATED) -> bytes:
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=compression) as archive:
        for name, content in entries.items():
            archive.writestr(name, content)
    return output.getvalue()


class DocxPreflightTests(unittest.TestCase):
    def test_accepts_minimal_safe_ooxml_shape(self):
        preflight_docx(_docx({
            "[Content_Types].xml": b"<Types/>",
            "word/document.xml": b"<document><p>safe</p></document>",
        }))

    def test_rejects_path_traversal_and_xml_entities(self):
        with self.assertRaises(AttachmentProcessingError):
            preflight_docx(_docx({
                "[Content_Types].xml": b"<Types/>",
                "word/document.xml": b"<document/>",
                "../secret.xml": b"x",
            }))
        with self.assertRaises(AttachmentProcessingError):
            preflight_docx(_docx({
                "[Content_Types].xml": b"<Types/>",
                "word/document.xml": b"<!DOCTYPE x [<!ENTITY y 'z'>]><x>&y;</x>",
            }))

    def test_rejects_extreme_compression_ratio(self):
        with self.assertRaises(AttachmentProcessingError):
            preflight_docx(_docx({
                "[Content_Types].xml": b"<Types/>",
                "word/document.xml": b"A" * (2 * 1024 * 1024),
            }))

    def test_plain_text_extraction_runs_out_of_process(self):
        self.assertEqual(
            extract_in_subprocess("notes.txt", "safe text".encode()),
            "safe text",
        )


class DurableAttachmentJobTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.engine = create_engine(
            f"sqlite:///{Path(self.temp.name) / 'attachments.sqlite'}"
        )
        self.addCleanup(self.engine.dispose)
        BASE.metadata.create_all(self.engine)
        self.factory = sessionmaker(bind=self.engine)
        self.old_factory = db._SESSION_LOCAL
        self.old_engine = db._ENGINE
        db._SESSION_LOCAL = self.factory
        db._ENGINE = self.engine
        self.addCleanup(setattr, db, "_SESSION_LOCAL", self.old_factory)
        self.addCleanup(setattr, db, "_ENGINE", self.old_engine)
        with self.factory.begin() as session:
            conversation = ConversationModel(
                serial="abc123",
                owner_key="owner",
                title="test",
                created_at="now",
                updated_at="now",
            )
            session.add(conversation)
            session.flush()
            self.conversation_id = conversation.id

    def test_job_is_durable_then_worker_persists_chunks(self):
        job_id = attachment_jobs.queue_attachment(
            self.conversation_id,
            "notes.txt",
            b"hello durable worker",
        )
        with self.factory() as session:
            job = session.get(AttachmentJobModel, job_id)
            self.assertEqual(job.state, "queued")
            self.assertEqual(job.payload, b"hello durable worker")

        with mock.patch.object(
            attachment_jobs,
            "extract_in_subprocess",
            return_value="hello durable worker",
        ), mock.patch.object(
            attachment_jobs.rag_index,
            "embed_texts",
            return_value=[[1.0, 0.0]],
        ), mock.patch.object(
            attachment_jobs.rag_index,
            "chunk_text",
            return_value=["hello durable worker"],
        ):
            self.assertTrue(attachment_jobs.run_one())

        with self.factory() as session:
            job = session.get(AttachmentJobModel, job_id)
            self.assertEqual(job.state, "succeeded")
            self.assertIsNone(job.payload)
            chunks = session.query(AttachmentChunkModel).all()
            self.assertEqual([chunk.content for chunk in chunks], ["hello durable worker"])

    def test_cancel_prevents_a_leased_job_from_persisting(self):
        attachment_jobs.queue_attachment(
            self.conversation_id,
            "notes.txt",
            b"hello",
        )
        claim = attachment_jobs._claim_one()
        self.assertIsNotNone(claim)
        attachment_jobs.cancel_attachment(self.conversation_id, "notes.txt")
        attachment_jobs._finish_success(claim, ["late"], [[1.0]])
        with self.factory() as session:
            self.assertEqual(session.query(AttachmentChunkModel).count(), 0)


if __name__ == "__main__":
    unittest.main()
