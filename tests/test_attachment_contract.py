# tests/test_attachment_contract.py
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

from flask import Flask

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

from app import create_app
from models import AttachmentChunkModel, ChatMessageModel
from services import auth
import services.ai as ask_module
from services.papers import extract_pdf_text, extract_text_from_upload

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import support


class ExtractTextFromUpload(unittest.TestCase):
    def test_txt_decodes_utf8(self):
        out = extract_text_from_upload("notes.txt", "héllo world".encode("utf-8"))
        self.assertIn("héllo world", out)

    def test_md_decodes_utf8(self):
        out = extract_text_from_upload("README.md", b"# Title\n\nBody text")
        self.assertIn("Body text", out)

    def test_unsupported_extension_raises(self):
        with self.assertRaises(ValueError):
            extract_text_from_upload("image.png", b"\x89PNG")

    def test_pdf_branch_delegates_to_pdf_text(self):
        from unittest import mock
        with mock.patch("pdf_text.extract_pdf_text", return_value="ocr or pypdf text") as ex:
            out = extract_text_from_upload("scan.pdf", b"%PDF-1.4 fake")
        # No vision_fallback kwarg: attachments must stay on bounded local OCR
        # (pypdf + Tesseract) — never opt user uploads into paid vision calls.
        ex.assert_called_once_with(b"%PDF-1.4 fake")
        self.assertEqual(out, "ocr or pypdf text")


class AttachmentNoVisionContract(unittest.TestCase):
    """Security: attachment extraction must never reach paid vision calls.

    Unbounded synchronous vision LLM calls on untrusted uploads are a
    DoS/cost-abuse vector (reverts commit 6b34d80). AST-pinned so the
    invariant holds regardless of mock plumbing or import style.
    """

    BANNED_TOKENS = ("vision_read", "vision_fallback", "vision_enabled",
                     "transcribe_pdf", "llm_client")

    def test_no_vision_names_in_extract_text_from_upload(self):
        src = support.source_of("extract_text_from_upload")
        for token in self.BANNED_TOKENS:
            self.assertNotIn(token, src)

    def test_single_bare_extract_pdf_text_call(self):
        import ast
        fn = ast.parse(support.source_of("extract_text_from_upload")).body[0]
        calls = [
            node for node in ast.walk(fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "extract_pdf_text"
        ]
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(calls[0].args), 1)
        self.assertEqual(calls[0].keywords, [])


class AttachmentModelContract(unittest.TestCase):
    def test_model_exists_with_columns(self):
        model = AttachmentChunkModel
        self.assertEqual(model.__tablename__, "attachment_chunks")
        cols = set(model.__table__.columns.keys())
        for c in ("id", "conversation_id", "filename", "chunk_index", "content", "embedding"):
            self.assertIn(c, cols)

    def test_grounding_helper_exists(self):
        self.assertTrue(callable(ask_module._attachment_grounding))


from io import BytesIO
from unittest import mock


def _make_client():
    try:
        app = create_app()
    except Exception as exc:  # pragma: no cover - environment dependent
        msg = str(exc).lower()
        if "connect" in msg or "refused" in msg or "mysql" in msg or "2003" in msg:
            raise unittest.SkipTest("database unavailable: %s" % exc)
        raise
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    return app.test_client()


def _authenticate(client, username):
    if auth.get_local_user(username) is None:
        auth.create_local_user(username, "test-password1", role="1")
    token, _ = auth.register_active_session(
        auth.ACCOUNT_LOCAL,
        username,
    )
    with client.session_transaction() as session:
        session["session_token"] = token


class AttachEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = _make_client()
        _authenticate(self.client, "attachment-contract-reader")

    def test_disabled_when_no_api_key(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LLM_API_KEY", None)
            resp = self.client.post(
                "/api/ai/attach",
                data={"conversation_id": "zzzzzz",
                      "file": (BytesIO(b"hello"), "a.txt")},
                content_type="multipart/form-data")
            self.assertEqual(resp.status_code, 503)

    def test_unknown_conversation_404(self):
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=False):
            resp = self.client.post(
                "/api/ai/attach",
                data={"conversation_id": "nope00",
                      "file": (BytesIO(b"hello"), "a.txt")},
                content_type="multipart/form-data")
            self.assertEqual(resp.status_code, 404)


class ConversationDeletePurges(unittest.TestCase):
    def test_delete_branch_purges_attachment_chunks(self):
        text = support.source_of("api_conversation_item")
        # the conversation DELETE branch must delete attachment chunks too
        self.assertIn("AttachmentChunkModel.conversation_id == conv.id", text)

    def test_api_ask_merges_attachment_grounding(self):
        text = support.source_of("api_ai")
        self.assertIn("_attachment_grounding(", text)


class RagPaperTextOcr(unittest.TestCase):
    def test_rag_paper_text_uses_pdf_text(self):
        from unittest import mock
        paper_id = "11111111-1111-4111-8111-111111111111"
        library = mock.Mock()
        library.current_pdf.return_value = SimpleNamespace(
            paper=SimpleNamespace(language="en"),
            path=Path("/safe/1.pdf"),
        )
        test_app = Flask(__name__)
        test_app.extensions["paper_library"] = library
        with test_app.app_context(), \
             mock.patch("pathlib.Path.read_bytes", return_value=b"%PDF-1.4 fake"), \
             mock.patch("pdf_text.extract_pdf_text", return_value="scanned paper text") as ex:
            out = ask_module._rag_paper_text(paper_id)
        library.current_pdf.assert_called_once_with(paper_id)
        ex.assert_called_once()
        self.assertEqual(out, "scanned paper text")

    def test_search_papers_stays_pypdf_only(self):
        # The live /search full-text fallback must NOT gain OCR (timeout risk).
        import inspect
        src = inspect.getsource(extract_pdf_text)  # the Path-based one
        self.assertIn("PdfReader", src)
        self.assertNotIn("pdf_text.", src)  # must not delegate to pdf_text module


class RagPaperTextDeadlineContract(unittest.TestCase):
    """Security: _rag_paper_text runs synchronously inside the /api/ai request
    handler (agentic read_paper fallback for chunk-less scanned papers). Its
    extraction must run under a deadline so one paper cannot hold a gunicorn
    worker or burn vision quota unboundedly."""

    def test_extract_pdf_text_call_is_deadline_bound(self):
        import ast
        fn = ast.parse(support.source_of("_rag_paper_text")).body[0]
        calls = [
            node for node in ast.walk(fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "extract_pdf_text"
        ]
        self.assertEqual(len(calls), 1)
        self.assertIn("deadline", {keyword.arg for keyword in calls[0].keywords})

    def test_vision_fallback_prebinds_deadline(self):
        import ast
        fn = ast.parse(support.source_of("_rag_paper_text")).body[0]
        calls = [
            node for node in ast.walk(fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "transcribe_pdf"
        ]
        self.assertEqual(len(calls), 1)
        self.assertIn("deadline", {keyword.arg for keyword in calls[0].keywords})

    def test_deadline_value_is_monotonic_plus_ask_timeout(self):
        # Presence-only pins would still pass with deadline=None, a
        # time.time() clock mixup, or an inflated constant — pin the actual
        # value (mirrors the publishing path in test_publishing_publish.py).
        from unittest import mock
        paper_id = "11111111-1111-4111-8111-111111111111"
        library = mock.Mock()
        library.current_pdf.return_value = SimpleNamespace(
            paper=SimpleNamespace(language="en"),
            path=Path("/safe/1.pdf"),
        )
        test_app = Flask(__name__)
        test_app.extensions["paper_library"] = library
        with test_app.app_context(), \
             mock.patch.object(ask_module.time, "monotonic", return_value=7.0), \
             mock.patch("pathlib.Path.read_bytes", return_value=b"%PDF-1.4 fake"), \
             mock.patch("pdf_text.extract_pdf_text", return_value="") as ex:
            ask_module._rag_paper_text(paper_id)
        self.assertEqual(
            ex.call_args.kwargs["deadline"],
            7.0 + ask_module.ASK_READ_PAPER_TIMEOUT)


class ConversationGetReturnsMessageAttachments(unittest.TestCase):
    def test_message_dict_includes_attachments(self):
        src = support.source_of("api_conversation_item")
        # the per-message dict built in the GET branch must carry attachments
        self.assertIn('"attachments": ', src)
        self.assertIn("m.attachments", src)


class AskPersistsMessageAttachments(unittest.TestCase):
    def test_reads_and_stores_message_attachments(self):
        src = support.source_of("api_ai")
        self.assertIn('data.get("message_attachments"', src)
        self.assertIn("attachments=", src)   # set on the user ChatMessageModel row


class MessageAttachmentsColumn(unittest.TestCase):
    def test_column_exists(self):
        cols = set(ChatMessageModel.__table__.columns.keys())
        self.assertIn("attachments", cols)

    def test_schema_verifier_exists(self):
        from models import ensure_schema_current
        self.assertTrue(callable(ensure_schema_current))


if __name__ == "__main__":
    unittest.main()
