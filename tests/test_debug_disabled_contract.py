import ast
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

ROOT = Path(__file__).resolve().parents[1]


class DebugDisabledContractTest(unittest.TestCase):
    def test_dockerfile_default_cmd_is_gunicorn(self):
        cmd_line = ""
        for line in (ROOT / "Dockerfile").read_text(encoding="utf-8").splitlines():
            if line.strip().startswith("CMD"):
                cmd_line = line
        self.assertIn("gunicorn", cmd_line)
        self.assertNotIn("--debug", cmd_line)

    def test_create_app_has_no_debug(self):
        src = support.source_of("create_app")
        self.assertNotIn("debug=True", src)
        self.assertNotIn("PROPAGATE_EXCEPTIONS", src)

    def test_main_block_has_no_debug_true(self):
        tree = ast.parse((ROOT / "app.py").read_text(encoding="utf-8"))
        bad = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "run":
                for kw in node.keywords:
                    if kw.arg == "debug" and isinstance(kw.value, ast.Constant) and kw.value.value is True:
                        bad.append(node.lineno)
        self.assertEqual([], bad, f"app.run(debug=True) at lines {bad}")
