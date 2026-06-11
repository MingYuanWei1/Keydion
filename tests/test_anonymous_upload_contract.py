"""Contract tests for the anonymous-upload feature: wizard JS structure
and template guards. Pure text/regex tests — no DB required."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class WizardJsAnonymousContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")

    def _function_body(self, name):
        """Slice from a top-level (2-space indented) function to the next one."""
        start = self.js.find(f"function {name}(")
        self.assertNotEqual(start, -1, f"function {name} not found")
        end = self.js.find("\n  function ", start + 1)
        if end == -1:
            end = len(self.js)
        return self.js[start:end]

    def test_state_hydrates_is_anonymous_with_ib_sample_priority(self):
        self.assertIn("isAnonymous: !fd.is_ib_sample && !!fd.is_anonymous,", self.js)

    def test_authors_step_is_unconditional(self):
        body = self._function_body("getSteps")
        self.assertNotIn("isIbSample", body)
        self.assertIn("id: 'authors'", body)

    def test_metadata_step_no_longer_owns_ib_sample(self):
        self.assertNotIn("f-ibsample", self._function_body("renderMetadata"))
        self.assertNotIn("f-ibsample", self._function_body("bindMetadata"))

    def test_authors_step_renders_mode_choice(self):
        body = self._function_body("renderAuthors")
        self.assertIn("authorModeChoice", body)
        self.assertIn("upload_anonymous", body)
        self.assertIn("is_ib_sample", body)
        self.assertIn('name="f-author-mode"', self.js)

    def test_authors_step_binds_mode_choice(self):
        body = self._function_body("bindAuthors")
        self.assertIn("f-author-mode", body)
        self.assertIn("setAuthorMode", body)

    def test_serialization_sends_anonymous_flag_and_omits_authors(self):
        self.assertIn("add('is_anonymous', '1')", self.js)
        self.assertIn("if (!state.isIbSample && !state.isAnonymous) {", self.js)

    def test_review_step_summarises_anonymous_mode(self):
        self.assertIn("anonymous_skipped", self._function_body("renderReview"))


if __name__ == "__main__":
    unittest.main()
