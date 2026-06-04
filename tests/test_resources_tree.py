import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))   # import app.py regardless of the runner's cwd


def _reload_app_with_temp_db():
    """Reload app.py against a throwaway sqlite file + temp resources dir.

    A file-backed sqlite (not :memory:) is used so the multiple connections
    opened by db_session() share state.
    """
    tmp_db = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
    tmp_db.close()
    os.environ["PAPERQUERY_SECRET"] = "test-secret"
    os.environ["PAPERQUERY_DATABASE_URL"] = f"sqlite:///{tmp_db.name}"
    os.environ["PAPERQUERY_RESOURCES_DIR"] = tempfile.mkdtemp()
    import app as app_module
    importlib.reload(app_module)
    app_module.create_app()  # runs init_db() -> creates resource_nodes table
    return app_module, tmp_db.name


class ResourcesModelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m, cls.db_path = _reload_app_with_temp_db()

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls.db_path)
        except OSError:
            pass

    def test_model_and_constants_exist(self):
        m = self.m
        self.assertTrue(hasattr(m, "ResourceNode"))
        self.assertEqual(m.ResourceNode.__tablename__, "resource_nodes")
        for col in ("id", "parent_id", "node_type", "name", "stored_filename",
                    "original_filename", "mime_type", "size_bytes",
                    "description", "min_role", "created_at"):
            self.assertIn(col, m.ResourceNode.__table__.columns,
                          f"ResourceNode missing column {col}")
        self.assertTrue(str(m.RESOURCES_DIR))
        self.assertIn("application/pdf", m.RESOURCE_ALLOWED_EXTENSIONS.values())
        self.assertIn("application/pdf", m.PREVIEWABLE_MIMES)
