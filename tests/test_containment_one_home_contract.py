import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import support

# Filename/path sinks still route through the one shared containment resolver.
# Canonical UUID delivery routes are intentionally absent: PaperLibrary and
# PaperStorage validate their UUID/revision paths instead. The retired
# ``download`` endpoint is likewise absent; downloads use ``paper_file``.
MIGRATED_SINKS = [
    "pending_paper_file", "delete_submission",
    "my_submission_file", "upload",
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
