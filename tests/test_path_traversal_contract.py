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
        self.assertIn("is_relative_to", src)
        self.assertIn(".resolve()", src)
        self.assertNotIn("require_login", src, "paper_preview must stay guest-reachable")

    def test_preview_paper_contained(self):
        src = support.source_of("preview_paper")
        self.assertIn("is_relative_to", src)

    def test_paper_delete_guard_precedes_unlink(self):
        src = support.source_of("paper_delete")
        self.assertTrue(_before(src, "is_relative_to", ".unlink"))

    def test_paper_modify_guard_precedes_fs_ops(self):
        src = support.source_of("paper_modify")
        self.assertIn("is_relative_to", src)
        self.assertTrue(src.index("is_relative_to") < src.index(".rename("))
        self.assertTrue(src.index("is_relative_to") < src.index("set_pdf_metadata("))

    def test_rag_paper_text_guards_before_read_bytes(self):
        src = support.source_of("_rag_paper_text")
        self.assertTrue(_before(src, "is_relative_to", ".read_bytes()"))

    def test_upload_validates_draft_id_ownership(self):
        src = support.source_of("upload")
        # draft_id must be ownership-checked before it becomes the pending sub_id
        self.assertIn("sub_id = draft_id", src)
        idx_use = src.index("sub_id = draft_id")
        guard = src[:idx_use]
        self.assertIn("_get_submission(draft_id)", guard,
                      "draft_id must be validated as an owned submission before use")
