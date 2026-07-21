import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


def _before(src, guard, op):
    """Assert `guard` text appears before the first `op` text in src."""
    return guard in src and op in src and src.index(guard) < src.index(op)


class PathTraversalContractTest(unittest.TestCase):
    def test_paper_preview_contained_but_still_guest_reachable(self):
        src = support.source_of("paper_preview")
        self.assertIn("_current_paper_pdf(paper_id)", src)
        self.assertIn("verify_revision(", support.source_of("_verified_pdf"))
        self.assertIn("_opened_regular(", support.source_of("verify_revision"))
        self.assertNotIn("require_login", src, "paper_preview must stay guest-reachable")

    def test_preview_paper_contained(self):
        src = support.source_of("preview_paper")
        self.assertIn("_current_paper_pdf(paper_id)", src)
        self.assertIn("_canonical_id(paper_id)", support.source_of("current_pdf"))
        self.assertIn("verify_revision(", support.source_of("_verified_pdf"))

    def test_paper_delete_delegates_storage_mutation_to_lifecycle(self):
        src = support.source_of("paper_delete")
        self.assertIn("delete_paper(", src)
        self.assertNotIn("resolve_contained(", src)
        self.assertNotIn(".unlink", src)

    def test_paper_modify_delegates_storage_and_metadata_to_lifecycle(self):
        src = support.source_of("paper_modify")
        self.assertIn("change_paper(", src)
        self.assertNotIn("resolve_contained(", src)
        self.assertNotIn(".rename(", src)
        self.assertNotIn("set_pdf_metadata(", src)

    def test_lib_full_text_verifies_live_alias_and_current_storage_before_read(self):
        src = support.source_of("_lib_full_text")
        live = support.source_of("_live_paper_document")
        verified = support.source_of("_verified_pdf")

        self.assertTrue(_before(src, "_live_paper_document(", "db_session()"))
        self.assertIn("resolve_alias(filename)", live)
        self.assertIn("current_pdf(record.paper_id)", live)
        self.assertIn("verify_revision(", verified)

    def test_upload_validates_draft_id_ownership(self):
        src = support.source_of("upload")
        # draft_id must be ownership-checked before it becomes the pending sub_id
        self.assertIn("sub_id = draft_id", src)
        idx_use = src.index("sub_id = draft_id")
        guard = src[:idx_use]
        self.assertIn("_get_submission(draft_id)", guard,
                      "draft_id must be validated as an owned submission before use")
