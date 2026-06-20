import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


class ReviewCuratorOnlyContractTest(unittest.TestCase):
    REVIEW_FNS = ("review_list", "review_detail", "review_accept",
                  "review_reject", "pending_paper_file")

    def test_all_review_endpoints_are_level_3(self):
        for fn in self.REVIEW_FNS:
            src = support.source_of(fn)
            self.assertIn("require_login(level=3)", src, f"{fn} must be curator-only")
            self.assertNotIn("require_login(level=2)", src, f"{fn} must not allow role 2")
