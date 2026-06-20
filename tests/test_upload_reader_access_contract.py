import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


class UploadReaderAccessContractTest(unittest.TestCase):
    def test_upload_admits_readers_for_submission(self):
        src = support.source_of("upload")
        self.assertRegex(src, r"require_login\(\s*level\s*=\s*1\s*\)",
                         "upload must stay level=1 so Readers can submit for review")

    def test_upload_keeps_dual_publish_and_submit_branches(self):
        src = support.source_of("upload")
        self.assertIn("role >= 2", src, "direct-publish branch must exist")
        self.assertTrue("_save_submission" in src or "_update_submission" in src,
                        "Reader pending-queue branch must exist")
