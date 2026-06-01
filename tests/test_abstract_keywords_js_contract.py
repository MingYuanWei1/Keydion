import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class AbstractKeywordsJsContractTest(unittest.TestCase):
    """The wizard JS must wire the generate button to the new endpoint and
    apply the response to wizard state."""

    @classmethod
    def setUpClass(cls):
        cls.js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")

    def test_calls_endpoint(self):
        self.assertIn("/api/upload/generate-abstract-keywords", self.js)

    def test_has_handler(self):
        self.assertIn("runMetaAutofill", self.js)

    def test_has_button_id(self):
        self.assertIn("metaAutofillBtn", self.js)

    def test_gated_on_flag(self):
        self.assertIn("BOOT.llm_metadata_enabled", self.js)

    def test_sends_language(self):
        self.assertRegex(self.js, r"form\.append\(\s*['\"]language['\"]")

    def test_applies_response_to_state(self):
        self.assertIn("state.abstract", self.js)
        self.assertIn("state.keywords", self.js)

    def test_has_status_and_overwrite_guard(self):
        self.assertIn("metaAutofillStatus", self.js)
        self.assertIn("meta_autofill_overwrite", self.js)

    def test_no_standalone_picker_and_uses_step4_file(self):
        self.assertNotIn("metaAutofillFile", self.js)         # standalone input removed
        self.assertIn("uploadFormFile", self.js)              # reuses the File-step input
        self.assertIn("meta_autofill_no_file", self.js)       # prompt key when no file

    def test_applies_title_and_authors(self):
        self.assertIn("state.title", self.js)
        self.assertIn("data.title", self.js)
        self.assertIn("data.authors", self.js)

    def test_dirty_check_includes_title_and_authors(self):
        # isMetaDirty must consider title and author names, not just abstract/keywords.
        import re
        m = re.search(r"function isMetaDirty\(\)\s*\{.*?\n\s*\}", self.js, re.DOTALL)
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertIn("state.title", body)
        self.assertIn("authors", body)


if __name__ == "__main__":
    unittest.main()
