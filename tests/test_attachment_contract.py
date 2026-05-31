# tests/test_attachment_contract.py
import os
import unittest

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import app as app_module


class ExtractTextFromUpload(unittest.TestCase):
    def test_txt_decodes_utf8(self):
        out = app_module.extract_text_from_upload("notes.txt", "héllo world".encode("utf-8"))
        self.assertIn("héllo world", out)

    def test_md_decodes_utf8(self):
        out = app_module.extract_text_from_upload("README.md", b"# Title\n\nBody text")
        self.assertIn("Body text", out)

    def test_unsupported_extension_raises(self):
        with self.assertRaises(ValueError):
            app_module.extract_text_from_upload("image.png", b"\x89PNG")


class AttachmentModelContract(unittest.TestCase):
    def test_model_exists_with_columns(self):
        model = getattr(app_module, "AttachmentChunkModel")
        self.assertEqual(model.__tablename__, "attachment_chunks")
        cols = set(model.__table__.columns.keys())
        for c in ("id", "conversation_id", "filename", "chunk_index", "content", "embedding"):
            self.assertIn(c, cols)

    def test_grounding_helper_exists(self):
        self.assertTrue(callable(getattr(app_module, "_attachment_grounding")))


from io import BytesIO
from unittest import mock


def _make_client():
    try:
        app = app_module.create_app()
    except Exception as exc:  # pragma: no cover - environment dependent
        msg = str(exc).lower()
        if "connect" in msg or "refused" in msg or "mysql" in msg or "2003" in msg:
            raise unittest.SkipTest("database unavailable: %s" % exc)
        raise
    app.config["TESTING"] = True
    return app.test_client()


class AttachEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = _make_client()

    def test_disabled_when_no_api_key(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LLM_API_KEY", None)
            resp = self.client.post(
                "/api/ask/attach",
                data={"conversation_id": "zzzzzz",
                      "file": (BytesIO(b"hello"), "a.txt")},
                content_type="multipart/form-data")
            self.assertEqual(resp.status_code, 503)

    def test_unknown_conversation_404(self):
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=False):
            resp = self.client.post(
                "/api/ask/attach",
                data={"conversation_id": "nope00",
                      "file": (BytesIO(b"hello"), "a.txt")},
                content_type="multipart/form-data")
            self.assertEqual(resp.status_code, 404)


if __name__ == "__main__":
    unittest.main()
