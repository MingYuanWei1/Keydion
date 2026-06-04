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


class ResourcesTreeBehaviorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m, cls.db_path = _reload_app_with_temp_db()
        cls.app = cls.m.create_app()   # reuses the engine init_db already set up

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls.db_path)
        except OSError:
            pass

    def setUp(self):
        m = self.m
        with m.db_session() as db:
            db.query(m.ResourceNode).delete()
            db.commit()
        # _() (gettext) inside helpers needs an app/request context.
        self.ctx = self.app.test_request_context()
        self.ctx.push()

    def tearDown(self):
        self.ctx.pop()

    def test_effective_min_role_walks_ancestors(self):
        m = self.m
        a = m.create_resource_folder(None, "A", 1, "")
        b = m.create_resource_folder(a, "B", 2, "")
        c = m.create_resource_folder(b, "C", 1, "")
        self.assertEqual(m.effective_min_role(a), 1)
        self.assertEqual(m.effective_min_role(b), 2)
        self.assertEqual(m.effective_min_role(c), 2)  # inherits B's restriction

    def test_breadcrumbs_root_to_node(self):
        m = self.m
        a = m.create_resource_folder(None, "A", 1, "")
        b = m.create_resource_folder(a, "B", 1, "")
        crumbs = m.resource_breadcrumbs(b)
        self.assertEqual([c["name"] for c in crumbs], ["A", "B"])

    def test_children_sorted_folders_first(self):
        m = self.m
        root_folder = m.create_resource_folder(None, "Zeta", 1, "")
        m.create_resource_folder(None, "Alpha", 1, "")
        kids = m.load_resource_children(None, viewer_role=3)
        self.assertEqual([k["name"] for k in kids], ["Alpha", "Zeta"])
        self.assertTrue(all(k["node_type"] == "folder" for k in kids))
        self.assertIsNotNone(root_folder)

    def test_children_hidden_from_lower_role(self):
        m = self.m
        m.create_resource_folder(None, "Public", 1, "")
        m.create_resource_folder(None, "Staff", 2, "")
        reader_view = m.load_resource_children(None, viewer_role=1)
        self.assertEqual([k["name"] for k in reader_view], ["Public"])
        admin_view = m.load_resource_children(None, viewer_role=3)
        self.assertEqual(sorted(k["name"] for k in admin_view), ["Public", "Staff"])

    def test_move_rejects_cycle(self):
        m = self.m
        a = m.create_resource_folder(None, "A", 1, "")
        b = m.create_resource_folder(a, "B", 1, "")
        ok, err = m.move_resource_node(a, b)          # A into its own child B
        self.assertFalse(ok)
        self.assertTrue(err)
        ok2, err2 = m.move_resource_node(b, None)     # B to root is fine
        self.assertTrue(ok2)
        self.assertIsNone(err2)

    def test_recursive_delete_removes_blobs(self):
        m = self.m
        folder = m.create_resource_folder(None, "Docs", 1, "")
        # Create a fake blob + file row by hand.
        blob = m.RESOURCES_DIR / "deadbeef00.pdf"
        blob.write_bytes(b"%PDF-1.4 test")
        with m.db_session() as db:
            db.add(m.ResourceNode(parent_id=folder, node_type="file", name="x",
                                  stored_filename="deadbeef00.pdf",
                                  original_filename="x.pdf", mime_type="application/pdf",
                                  size_bytes=12, min_role=1, created_at=""))
            db.commit()
        self.assertTrue(blob.exists())
        m.delete_resource_node(folder)
        self.assertFalse(blob.exists())
        with m.db_session() as db:
            self.assertEqual(db.query(m.ResourceNode).count(), 0)

    def test_save_resource_file_roundtrip_and_validation(self):
        from io import BytesIO
        from werkzeug.datastructures import FileStorage
        m = self.m
        folder = m.create_resource_folder(None, "Docs", 1, "")

        # (a) valid upload -> (id, None); blob written; size + mime recorded.
        data = b"%PDF-1.4 hello world"
        good = FileStorage(stream=BytesIO(data), filename="report.pdf")
        node_id, err = m.save_resource_file(folder, good, "Report", "", 1)
        self.assertIsNone(err)
        self.assertIsNotNone(node_id)
        node = m.get_resource_node(node_id)
        self.assertEqual(node["size_bytes"], len(data))
        self.assertEqual(node["mime_type"], "application/pdf")
        self.assertTrue((m.RESOURCES_DIR / node["stored_filename"]).exists())

        # (b) disallowed extension -> (None, message).
        bad = FileStorage(stream=BytesIO(b"x"), filename="evil.exe")
        bad_id, bad_err = m.save_resource_file(folder, bad, "", "", 1)
        self.assertIsNone(bad_id)
        self.assertTrue(bad_err)

        # (c) over the size cap -> rejected.
        orig_max = m.RESOURCE_MAX_BYTES
        m.RESOURCE_MAX_BYTES = 4
        try:
            big = FileStorage(stream=BytesIO(b"way too big"), filename="big.pdf")
            big_id, big_err = m.save_resource_file(folder, big, "", "", 1)
        finally:
            m.RESOURCE_MAX_BYTES = orig_max
        self.assertIsNone(big_id)
        self.assertTrue(big_err)
