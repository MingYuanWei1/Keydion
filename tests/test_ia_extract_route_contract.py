# tests/test_ia_extract_route_contract.py
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


class IaExtractRouteContractTest(unittest.TestCase):
    """The /api/upload/extract-ia-metadata route must:
       - exist
       - require contributor (level=2) login
       - delegate to generate_ia_scores from ia_metadata
       - catch IAMetadataError
    """

    @classmethod
    def setUpClass(cls):
        cls.source = support.all_sources()

    def test_route_path_present(self):
        self.assertIn(
            '"/api/upload/extract-ia-metadata"',
            self.source,
            "API route path not registered with @app.route",
        )

    def test_imports_extractor(self):
        # Either 'from ia_metadata import generate_ia_scores' or
        # 'import ia_metadata' is acceptable.
        self.assertTrue(
            re.search(r"from\s+ia_metadata\s+import\s+[^\n]*generate_ia_scores", self.source)
            or re.search(r"import\s+ia_metadata", self.source),
            "ia_metadata must be imported in routes/upload.py",
        )

    def test_route_uses_require_login_level_2(self):
        idx = self.source.index('"/api/upload/extract-ia-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertRegex(
            window,
            r"require_login\s*\(\s*level\s*=\s*2\s*\)",
            "route handler must call require_login(level=2)",
        )

    def test_route_calls_extractor(self):
        idx = self.source.index('"/api/upload/extract-ia-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertIn("generate_ia_scores(", window, "route must call generate_ia_scores(...)")

    def test_route_returns_json_on_extractor_error(self):
        idx = self.source.index('"/api/upload/extract-ia-metadata"')
        window = self.source[idx : idx + 4000]
        self.assertIn("IAMetadataError", window, "route must catch IAMetadataError")


if __name__ == "__main__":
    unittest.main()
