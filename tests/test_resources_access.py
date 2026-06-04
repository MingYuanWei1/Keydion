import importlib
import os
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_app():
    os.environ["PAPERQUERY_SECRET"] = "test-secret"
    os.environ.setdefault("PAPERQUERY_DATABASE_URL", "sqlite:///:memory:")
    import app as app_module
    importlib.reload(app_module)
    return app_module


class CanViewNodeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = _load_app()

    def test_guest_sees_only_reader_level(self):
        can = self.m._can_view_node
        self.assertTrue(can(1, 0))    # guest (OPEN_ACCESS) sees reader-level
        self.assertFalse(can(2, 0))   # guest cannot see contributor-restricted

    def test_logged_in_role_threshold(self):
        can = self.m._can_view_node
        self.assertFalse(can(2, 1))   # reader cannot see contributor folder
        self.assertTrue(can(2, 2))    # contributor can
        self.assertTrue(can(3, 3))    # curator sees everything

    def test_none_viewer_denied(self):
        self.assertFalse(self.m._can_view_node(1, None))
