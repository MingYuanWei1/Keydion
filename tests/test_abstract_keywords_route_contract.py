import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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
        cls.source = (ROOT / "app.py").read_text(encoding="utf-8")
        tree = ast.parse(cls.source)  # also a sanity check that app.py parses
        cls.route_src = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == FUNC:
                cls.route_src = ast.get_source_segment(cls.source, node)
                break

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
        self.assertIn("llm_metadata_enabled", self.source)

    def test_boot_flag_is_role_gated(self):
        # The enabled flag must require BOTH a configured key AND contributor
        # role (>=2), so role-1 users don't see a button that 401s.
        line = next(l for l in self.source.splitlines() if '"llm_metadata_enabled"' in l)
        self.assertIn("llm_client.llm_enabled()", line)
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
