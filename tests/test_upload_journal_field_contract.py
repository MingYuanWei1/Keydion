"""Contract: the upload wizard offers an optional journal selector wired end-to-end."""
import unittest
from pathlib import Path

from tests.support import source_of

ROOT = Path(__file__).resolve().parents[1]


class UploadJournalFieldTest(unittest.TestCase):
    def test_boot_payload_includes_journals(self):
        src = source_of("_render_upload")
        self.assertIn('"journals": get_journal_names()', src)

    def test_wizard_js_renders_selects_and_serializes_journal(self):
        js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")
        self.assertIn("id=\"f-journal\"", js)        # rendered control
        self.assertIn("state.journal", js)            # tracked in state
        self.assertIn("add('journal', state.journal)", js)  # serialized to form


if __name__ == "__main__":
    unittest.main()
