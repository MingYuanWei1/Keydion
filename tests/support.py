"""Source locator for contract tests.

Contract tests assert on the SOURCE of functions historically defined in
app.py. The split refactor moves functions into config/db/models and the
routes/ + services/ packages; this module finds a function wherever it
lives so tests never hardcode a file path.
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_LEAF_MODULES = ("app.py", "config.py", "db.py", "models.py")
_PACKAGES = ("routes", "services")

_cache = {}


def _iter_files():
    for name in _LEAF_MODULES:
        p = ROOT / name
        if p.exists():
            yield p
    for pkg in _PACKAGES:
        d = ROOT / pkg
        if d.is_dir():
            yield from sorted(d.glob("*.py"))


def _parsed(path):
    if path not in _cache:
        text = path.read_text(encoding="utf-8")
        _cache[path] = (text, ast.parse(text))
    return _cache[path]


def find_function(name):
    """Return (ast_node, file_source_text) for the uniquely-named function."""
    hits = []
    for path in _iter_files():
        text, tree = _parsed(path)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                hits.append((node, text, path))
    if not hits:
        raise AssertionError(f"function {name!r} not found in any source module")
    files = sorted({str(h[2]) for h in hits})
    if len(files) > 1:
        raise AssertionError(f"function {name!r} defined in multiple modules: {files}")
    node, text, _path = hits[0]
    return node, text


def source_of(name):
    """Source segment of the uniquely-named function, wherever it lives."""
    node, text = find_function(name)
    return ast.get_source_segment(text, node)


def find_class(name):
    """Return (ast_node, file_source_text) for the uniquely-named class."""
    hits = []
    for path in _iter_files():
        text, tree = _parsed(path)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == name:
                hits.append((node, text, path))
    if not hits:
        raise AssertionError(f"class {name!r} not found in any source module")
    node, text, _path = hits[0]
    return node, text


def all_sources():
    """Concatenated text of every Python source module (for whole-app scans)."""
    return "\n".join(_parsed(p)[0] for p in _iter_files())
