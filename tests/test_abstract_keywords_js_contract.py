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


if __name__ == "__main__":
    unittest.main()
