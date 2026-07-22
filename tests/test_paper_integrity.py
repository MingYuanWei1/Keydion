from __future__ import annotations

import io
import unittest

from models import PaperMetadataModel
from services.paper_integrity import scan_current_revisions
from services.paper_library import PaperLibrary
from services.publishing_contracts import (
    Actor,
    DirectPublish,
    NormalizedPaperMetadata,
    PdfUpload,
)
from tests.publishing_support import PublishingLifecycleTestCase


class PaperIntegrityTests(PublishingLifecycleTestCase, unittest.TestCase):
    def _publish(self):
        result = self.lifecycle.publish_direct(
            DirectPublish(
                actor=Actor("contributor", 2),
                metadata=NormalizedPaperMetadata(
                    filename="integrity.pdf",
                    title="Integrity",
                    journal="Journal",
                    category="science",
                    language="en",
                    keywords="integrity",
                    abstract="Integrity test.",
                    author_name="Author",
                    author_email="author@example.test",
                    author_school="School",
                    published_at="2026-07-23",
                    ib_ee_data="",
                    is_ib_sample="",
                    cp_data="",
                    is_anonymous="",
                    ia_data="",
                ),
                pdf=PdfUpload(
                    "integrity.pdf",
                    io.BytesIO(self.valid_pdf_bytes("integrity")),
                ),
                idempotency_key="integrity-test-key",
            )
        )
        return result.paper_id

    def test_publication_caches_verified_integrity(self):
        paper_id = self._publish()
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            self.assertEqual(paper.integrity_status, "verified")
            self.assertEqual(paper.integrity_checked_revision, 1)
            self.assertIsNotNone(paper.integrity_checked_at)

    def test_scanner_marks_and_hides_corrupted_current_revision(self):
        paper_id = self._publish()
        path = self.storage.revision_path(paper_id, 1)
        path.write_bytes(b"%PDF-truncated")
        path.chmod(0o600)

        result = scan_current_revisions(
            session_factory=self.session_factory,
            storage=self.storage,
        )

        self.assertEqual(result.corrupt, 1)
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            self.assertEqual(paper.integrity_status, "corrupt")
        library = PaperLibrary(
            session_factory=self.session_factory,
            storage=self.storage,
        )
        self.assertEqual(library.list_visible(), ())


if __name__ == "__main__":
    unittest.main()
