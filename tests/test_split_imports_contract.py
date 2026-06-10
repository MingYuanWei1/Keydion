import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SplitImportContractTest(unittest.TestCase):
    """routes/ and services/ must never import app (acyclic import graph)."""

    def test_extracted_modules_never_import_app(self):
        for pkg in ("routes", "services"):
            pkg_dir = ROOT / pkg
            if not pkg_dir.is_dir():
                continue
            for path in sorted(pkg_dir.rglob("*.py")):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    names = []
                    if isinstance(node, ast.Import):
                        names = [alias.name for alias in node.names]
                    elif isinstance(node, ast.ImportFrom):
                        names = [node.module or ""]
                    for name in names:
                        self.assertFalse(
                            name == "app" or name.startswith("app."),
                            f"{path.name} imports {name!r} — forbidden by the split design",
                        )


if __name__ == "__main__":
    unittest.main()
