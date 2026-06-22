import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import support

# Every sink that previously hand-rolled resolve()+is_relative_to() (or relied on
# send_from_directory alone) now routes through resolve_contained().
MIGRATED_SINKS = [
    "preview_paper", "paper_preview", "paper_file", "download",
    "paper_delete", "paper_modify", "papers_bulk_action",
    "pending_paper_file", "delete_submission", "review_reject",
    "my_submission_file", "upload", "_lib_full_text",
]


class ContainmentOneHomeContractTest(unittest.TestCase):
    def test_containment_idiom_lives_only_in_the_resolver(self):
        self.assertIn("is_relative_to", support.source_of("resolve_contained"))
        for fn in MIGRATED_SINKS:
            self.assertNotIn(
                "is_relative_to", support.source_of(fn),
                f"{fn} still hand-rolls containment; it must call resolve_contained()",
            )

    def test_every_migrated_sink_routes_through_the_resolver(self):
        for fn in MIGRATED_SINKS:
            self.assertIn(
                "resolve_contained(", support.source_of(fn),
                f"{fn} must route its path through resolve_contained()",
            )


if __name__ == "__main__":
    unittest.main()
