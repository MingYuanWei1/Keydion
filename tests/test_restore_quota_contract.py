"""Contract: restore/revise cannot duplicate immutable PDFs without bound.

Security-review finding [4]: each tiny restore POST copied the entire selected
PDF into a new immutable revision with no deduplication, revision cap, or
throttle — one large uploaded PDF could be duplicated indefinitely.

Enforced here:
1. Restoring a revision whose bytes are identical to the current revision is
   refused (it would duplicate identical content for zero benefit).
2. The total revision count per paper is capped.
3. The restore route is throttled per user.
"""
import io
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.publishing import MAX_REVISIONS_PER_PAPER
from services.publishing_contracts import (
    Actor,
    InvalidInput,
    LifecycleError,
    PdfUpload,
    RevisePdf,
    RestoreRevision,
)
from tests.publishing_support import PublishingLifecycleTestCase

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


class RestoreDuplicateSuppression(PublishingLifecycleTestCase, unittest.TestCase):
    def _publish(self, label="alpha"):
        intent = self._direct_intent(label)
        return self.lifecycle.publish_direct(intent)

    def _direct_intent(self, label):
        from services.publishing_contracts import DirectPublish, NormalizedPaperMetadata
        return DirectPublish(
            actor=Actor("contributor", 2),
            idempotency_key=f"key-{label}",
            metadata=NormalizedPaperMetadata(
                filename=f"{label}.pdf",
                title=f"Paper {label}",
                journal="Journal",
                category="cat",
                language="en",
                author_name="Author",
                ib_ee_data="", cp_data="", ia_data="",
            ),
            pdf=PdfUpload("source.pdf", io.BytesIO(self.valid_pdf_bytes(label))),
        )

    def _revise(self, published, label):
        return self.lifecycle.change_paper(RevisePdf(
            actor=Actor("contributor", 2),
            paper_id=published.paper_id,
            expected_row_version=published.row_version,
            pdf=PdfUpload("replacement.pdf", io.BytesIO(self.valid_pdf_bytes(label))),
        ))

    def _restore(self, published, revision):
        return self.lifecycle.change_paper(RestoreRevision(
            actor=Actor("contributor", 2),
            paper_id=published.paper_id,
            expected_row_version=published.row_version,
            revision=revision,
        ))

    def test_restore_of_content_identical_to_current_is_refused(self):
        published = self._publish("alpha")                 # rev1: alpha
        changed = self._revise(published, "beta")          # rev2: beta
        first = self._restore(changed, 1)                  # rev3: alpha again
        self.assertEqual(first.revision, 3)
        with self.assertRaises(LifecycleError):
            self._restore(first, 1)                        # alpha == current bytes

    def test_restore_of_different_content_still_works(self):
        published = self._publish("alpha")                 # rev1
        changed = self._revise(published, "beta")          # rev2
        restored = self._restore(changed, 1)               # rev3
        self.assertEqual(restored.revision, 3)


class RevisionCountCap(PublishingLifecycleTestCase, unittest.TestCase):
    def test_cap_is_small_and_positive(self):
        self.assertGreater(MAX_REVISIONS_PER_PAPER, 1)
        self.assertLessEqual(MAX_REVISIONS_PER_PAPER, 100)

    def test_append_beyond_cap_refused_before_copy(self):
        from services.publishing_contracts import DirectPublish, NormalizedPaperMetadata
        published = self.lifecycle.publish_direct(DirectPublish(
            actor=Actor("contributor", 2),
            idempotency_key="cap-key",
            metadata=NormalizedPaperMetadata(
                filename="cap.pdf", title="Cap", journal="Journal",
                category="cat", language="en", author_name="Author",
                ib_ee_data="", cp_data="", ia_data="",
            ),
            pdf=PdfUpload("source.pdf", io.BytesIO(self.valid_pdf_bytes("rev-1"))),
        ))
        current = published
        for number in range(2, MAX_REVISIONS_PER_PAPER + 1):
            current = self.lifecycle.change_paper(RevisePdf(
                actor=Actor("contributor", 2),
                paper_id=current.paper_id,
                expected_row_version=current.row_version,
                pdf=PdfUpload("replacement.pdf",
                              io.BytesIO(self.valid_pdf_bytes(f"rev-{number}"))),
            ))
        self.assertEqual(current.revision, MAX_REVISIONS_PER_PAPER)
        with self.assertRaises(InvalidInput):
            self.lifecycle.change_paper(RevisePdf(
                actor=Actor("contributor", 2),
                paper_id=current.paper_id,
                expected_row_version=current.row_version,
                pdf=PdfUpload("replacement.pdf",
                              io.BytesIO(self.valid_pdf_bytes("one-too-many"))),
            ))


class RestoreRouteThrottle(unittest.TestCase):
    def test_restore_route_is_rate_limited(self):
        src = support.source_of("paper_restore")
        self.assertIn("consume_rate_limit(", src,
                      "restore must be throttled per user")


if __name__ == "__main__":
    unittest.main()
