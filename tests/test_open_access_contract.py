import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class OpenAccessContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        return ""

    def test_open_access_flag_defined_from_env(self):
        self.assertIn("PAPERQUERY_OPEN_ACCESS", self.app_source)
        self.assertRegex(
            self.app_source,
            r'OPEN_ACCESS\s*=\s*os\.environ\.get\(\s*"PAPERQUERY_OPEN_ACCESS"\s*,\s*"0"\s*\)',
            "OPEN_ACCESS must read PAPERQUERY_OPEN_ACCESS and default to \"0\"",
        )

    def test_context_processor_exposes_open_access(self):
        src = self._function_source("inject_global_vars")
        self.assertIn('"open_access": OPEN_ACCESS', src)

    def test_paper_file_gate_is_conditional(self):
        src = self._function_source("paper_file")
        self.assertIn("if not OPEN_ACCESS:", src)
        self.assertIn("require_login()", src)

    def test_download_gate_is_conditional(self):
        src = self._function_source("download")
        self.assertIn("if not OPEN_ACCESS:", src)
        self.assertIn("require_login()", src)

    def test_preview_serves_full_pdf_when_open_access(self):
        src = self._function_source("preview_paper")
        self.assertIn("not is_guest or OPEN_ACCESS", src)
