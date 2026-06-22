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
        self.assertIn("resolve_contained(", src)
        self.assertNotIn("require_login", src, "paper_preview must stay guest-reachable")

    def test_preview_paper_contained(self):
        src = support.source_of("preview_paper")
        self.assertIn("resolve_contained(", src)

    def test_paper_delete_guard_precedes_unlink(self):
        src = support.source_of("paper_delete")
        self.assertTrue(_before(src, "resolve_contained(", ".unlink"))

    def test_paper_modify_guard_precedes_fs_ops(self):
        src = support.source_of("paper_modify")
        self.assertIn("resolve_contained(", src)
        self.assertTrue(src.index("resolve_contained(") < src.index(".rename("))
        self.assertTrue(src.index("resolve_contained(") < src.index("set_pdf_metadata("))

    def test_lib_full_text_neutralizes_traversal_before_read(self):
        # H5: the read_paper boundary (_lib_full_text) collapses the
        # model-supplied filename to a basename before the DB query / disk
        # fallback, so the dual-purpose indexer (_rag_paper_text) stays clean.
        src = support.source_of("_lib_full_text")
        self.assertIn("Path(filename).name", src)
        self.assertTrue(_before(src, "Path(filename).name", "_rag_paper_text("))

    def test_upload_validates_draft_id_ownership(self):
        src = support.source_of("upload")
        # draft_id must be ownership-checked before it becomes the pending sub_id
        self.assertIn("sub_id = draft_id", src)
        idx_use = src.index("sub_id = draft_id")
        guard = src[:idx_use]
        self.assertIn("_get_submission(draft_id)", guard,
                      "draft_id must be validated as an owned submission before use")
