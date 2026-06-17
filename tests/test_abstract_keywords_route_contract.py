import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

PATH = "/api/upload/generate-abstract-keywords"
FUNC = "api_generate_abstract_keywords"


class AbstractKeywordsRouteContractTest(unittest.TestCase):
    """The /api/upload/generate-abstract-keywords route must exist, require
    contributor (level=2) login, validate the PDF, forward the language, and
    delegate to generate_abstract_keywords. Body assertions are scoped to the
    route's own function (via AST) so neighbouring routes cannot satisfy them.
    """

    @classmethod
    def setUpClass(cls):
        cls.source = support.all_sources()
        cls.route_src = support.source_of(FUNC)

    # ── module-level wiring (whole-source) ──────────────────────────────
    def test_route_path_present(self):
        self.assertIn(f'"{PATH}"', self.source)

    def test_imports_generator(self):
        self.assertTrue(
            re.search(
                r"from\s+llm_metadata\s+import\s+[^\n]*generate_abstract_keywords",
                self.source,
            ),
            "generate_abstract_keywords must be imported from llm_metadata",
        )

    def test_boot_exposes_enabled_flag(self):
        self.assertIn("extract_assist_enabled", self.source)

    def test_boot_flag_is_role_gated(self):
        # The enabled flag must require contributor role (>=2) AND at least one
        # model that can drive extraction. generate_abstract_keywords is
        # vision-first (returns from the vision branch before the chat client),
        # so a vision-only config can extract — the gate includes BOTH
        # vision_enabled() and llm_enabled().
        line = next(l for l in self.source.splitlines() if '"extract_assist_enabled"' in l)
        self.assertIn("llm_client.llm_enabled()", line)
        self.assertIn("llm_client.vision_enabled()", line)
        self.assertIn("_role", line)

    def test_i18n_key_present(self):
        self.assertIn('"meta_autofill_btn"', self.source)

    # ── route body (scoped to the function) ─────────────────────────────
    def test_route_function_found(self):
        self.assertIsNotNone(self.route_src, f"{FUNC} not found in app.py")

    def test_route_uses_require_login_level_2(self):
        self.assertRegex(self.route_src, r"require_login\s*\(\s*level\s*=\s*2\s*\)")

    def test_route_returns_401_when_unauthorized(self):
        self.assertIn("401", self.route_src)

    def test_route_validates_pdf(self):
        self.assertIn('startswith(b"%PDF-")', self.route_src)
        self.assertIn(".pdf", self.route_src)

    def test_route_forwards_language(self):
        self.assertRegex(self.route_src, r'request\.form\.get\(\s*["\']language["\']')

    def test_route_calls_generator(self):
        self.assertIn("generate_abstract_keywords(", self.route_src)

    def test_route_catches_llm_error(self):
        self.assertIn("LLMMetadataError", self.route_src)


if __name__ == "__main__":
    unittest.main()
