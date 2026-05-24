import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class EeExtractRouteContractTest(unittest.TestCase):
    """The /api/upload/extract-ee-metadata route must:
       - exist
       - require contributor (level=2) login
       - delegate to extract_ee_metadata from ee_pdf_extractor
    """

    @classmethod
    def setUpClass(cls):
        cls.source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_route_path_present(self):
        self.assertIn(
            '"/api/upload/extract-ee-metadata"',
            self.source,
            "API route path not registered with @app.route",
        )

    def test_imports_extractor(self):
        # Either 'from ee_pdf_extractor import extract_ee_metadata' or
        # 'import ee_pdf_extractor' is acceptable.
        self.assertTrue(
            re.search(r"from\s+ee_pdf_extractor\s+import\s+[^\n]*extract_ee_metadata", self.source)
            or re.search(r"import\s+ee_pdf_extractor", self.source),
            "ee_pdf_extractor must be imported in app.py",
        )

    def test_route_uses_require_login_level_2(self):
        idx = self.source.index('"/api/upload/extract-ee-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertRegex(
            window,
            r"require_login\s*\(\s*level\s*=\s*2\s*\)",
            "route handler must call require_login(level=2)",
        )

    def test_route_calls_extractor(self):
        idx = self.source.index('"/api/upload/extract-ee-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertIn("extract_ee_metadata(", window, "route must call extract_ee_metadata(...)")

    def test_route_returns_json_on_extractor_error(self):
        idx = self.source.index('"/api/upload/extract-ee-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertIn("EePdfExtractionError", window, "route must catch EePdfExtractionError")


if __name__ == "__main__":
    unittest.main()
