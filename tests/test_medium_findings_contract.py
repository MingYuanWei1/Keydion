"""Contract + behavioral tests for the Medium-severity hardening cluster.

[6] login throttle keyed on the resolved account identity
[7] PDF structural budgets before synchronous parsing
[8] cumulative Reader-intake and news-image storage quotas
[9] anonymity scrubs the served PDF /Author and retires legacy aliases
[10] Microsoft step-up before first local password enrollment
[12] Ask owner/conversation/turn-wide budgets
"""
import io
import os
import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

import pdf_text
import services.submissions as submission_service
from services.publishing_contracts import (
    Actor,
    DirectPublish,
    EditMetadata,
    MetadataPatch,
    NormalizedPaperMetadata,
    PdfUpload,
)
from models import PaperMetadataModel, PaperRevisionModel
from tests.publishing_support import PublishingLifecycleTestCase


class LoginThrottleCanonicalKeyContract(unittest.TestCase):
    def test_ip_limit_runs_before_lookups_account_limit_after(self):
        src = support.source_of("login")
        self.assertLess(src.index("login.ip"), src.index("get_local_user_by_email"))
        self.assertLess(src.index("get_local_user_by_email"), src.index("login.account"))

    def test_account_key_is_the_resolved_principal(self):
        src = support.source_of("login")
        self.assertIn("f\"local:{user_record.get('username', '')}\"", src)
        self.assertIn("f\"ms:{ms_record.get('ms_id', '')}\"", src)
        self.assertIn("unicodedata.normalize(", src)

    def test_success_clears_account_but_never_the_ip_bucket(self):
        src = support.source_of("login")
        self.assertIn('clear_rate_limit("login.account"', src)
        self.assertNotIn('clear_rate_limit("login.ip"', src)


class PdfStructureBudgetContract(unittest.TestCase):
    @staticmethod
    def _text_pdf(page_texts):
        import fitz
        doc = fitz.open()
        for text in page_texts:
            page = doc.new_page()
            page.insert_text((72, 72), text)
        data = doc.tobytes()
        doc.close()
        return data

    def test_check_rejects_non_pdf_and_corrupt_bytes(self):
        for raw in (b"", b"not a pdf", b"%PDF-1.7 garbage without structure"):
            with self.subTest(raw=raw):
                with self.assertRaises(pdf_text.PdfStructureError):
                    pdf_text.check_pdf_bytes(raw)

    def test_check_accepts_a_real_pdf_and_reports_pages(self):
        raw = self._text_pdf(["one page"] * 2)
        self.assertEqual(pdf_text.check_pdf_bytes(raw), 2)

    def test_check_refuses_documents_over_the_page_budget(self):
        raw = self._text_pdf(["page"] * 4)
        with mock.patch.object(pdf_text, "MAX_PDF_PAGES", 3):
            with self.assertRaises(pdf_text.PdfStructureError):
                pdf_text.check_pdf_bytes(raw)

    def test_extraction_stops_at_the_page_budget(self):
        raw = self._text_pdf(["alpha " * 30, "beta " * 30])
        with mock.patch.object(pdf_text, "MIN_TEXT_CHARS", 0):
            out = pdf_text.extract_pdf_text(raw, max_pages=1, max_ocr_pages=0)
        self.assertIn("alpha", out)
        self.assertNotIn("beta", out)

    def test_intake_and_extraction_routes_run_the_structural_check(self):
        self.assertIn("check_pdf_bytes(", support.source_of("upload"))
        self.assertIn("check_pdf_bytes(", support.source_of("api_extract_ee_metadata"))
        self.assertIn("check_pdf_bytes(", support.source_of("api_generate_abstract_keywords"))
        self.assertIn("check_pdf_bytes(", support.source_of("api_extract_ia_metadata"))

    def test_lifecycle_staging_enforces_the_page_budget(self):
        import services.paper_storage as paper_storage
        self.assertIn("MAX_PDF_PAGES", support.source_of("_strict_pdf"))


class ReaderIntakeQuotaContract(unittest.TestCase):
    def test_upload_route_enforces_per_account_budgets(self):
        src = support.source_of("upload")
        self.assertIn("_submission_quota_error(", src)
        helper = support.source_of("_submission_quota_error")
        self.assertIn("MAX_ACTIVE_SUBMISSIONS_PER_USER", helper)
        self.assertIn("MAX_PENDING_BYTES_PER_USER", helper)

    def test_news_images_enforce_size_and_directory_budgets(self):
        src = support.source_of("news_upload_inline_image")
        self.assertIn("_news_image_rejection(", src)
        helper = support.source_of("_news_image_rejection")
        self.assertIn("NEWS_IMAGE_MAX_BYTES", helper)
        budget = support.source_of("_news_image_budget_exhausted")
        self.assertIn("NEWS_IMAGES_MAX_FILES", budget)
        self.assertIn("NEWS_IMAGES_MAX_TOTAL_BYTES", budget)

    def test_active_submission_usage_counts_rows_and_contained_bytes(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "ok.pdf").write_bytes(b"1234567")
            (root / "stray.pdf").write_bytes(b"999")
            rows = [(None,), ("ok.pdf",), ("../evil.pdf",)]

            class FakeQuery:
                def filter(self, *_args, **_kwargs):
                    return self

                def all(self):
                    return rows

            fake_db = mock.Mock()
            fake_db.query.return_value = FakeQuery()
            with mock.patch.object(submission_service, "db_session",
                                   return_value=nullcontext(fake_db)), \
                 mock.patch.object(submission_service, "PENDING_PAPERS_DIR", root):
                count, used = submission_service.active_submission_usage("reader@example.test")
        self.assertEqual(count, 3)
        self.assertEqual(used, 7)


class AnonymityScrubContract(PublishingLifecycleTestCase, unittest.TestCase):
    def _publish_named(self, label="alpha"):
        intent = DirectPublish(
            actor=Actor("contributor", 2),
            idempotency_key=f"key-{label}",
            metadata=NormalizedPaperMetadata(
                filename=f"author_{label}.pdf",
                title=f"Paper {label}",
                journal="Journal",
                category="cat",
                language="en",
                author_name="Alice Author",
                ib_ee_data="", cp_data="", ia_data="",
            ),
            pdf=PdfUpload("source.pdf", io.BytesIO(self.valid_pdf_bytes(label))),
        )
        return self.lifecycle.publish_direct(intent)

    def _edit(self, published, **changes):
        return self.lifecycle.change_paper(EditMetadata(
            actor=Actor("contributor", 2),
            patch=MetadataPatch(
                paper_id=published.paper_id,
                expected_row_version=published.row_version,
                changes=tuple(changes.items()),
            ),
        ))

    @staticmethod
    def _pdf_author(path):
        from pypdf import PdfReader
        with open(path, "rb") as handle:
            metadata = PdfReader(handle, strict=True).metadata
        return str(getattr(metadata, "author", "") or "").strip()

    def test_anonymize_scrubs_pdf_author_and_retires_legacy_alias(self):
        published = self._publish_named("alpha")
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, published.paper_id)
            self.assertEqual(paper.author_name, "Alice Author")
            revision = session.get(PaperRevisionModel, (paper.id, paper.current_revision))
        self.assertEqual(self._pdf_author(self.storage.open_revision(paper.id, 1)),
                         "Alice Author")
        self.assertIsNotNone(self.alias("author_alpha.pdf"))

        changed = self._edit(
            published,
            filename="alpha.pdf",
            is_anonymous="1",
            author_name="",
            author_email="",
            author_school="",
        )
        self.assertGreater(changed.revision, published.revision)
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, published.paper_id)
            self.assertEqual(paper.author_name, "")
            self.assertEqual(paper.current_revision, changed.revision)
        self.assertEqual(self._pdf_author(
                         self.storage.open_revision(paper.id, changed.revision)), "")
        self.assertIsNone(self.alias("author_alpha.pdf"))
        self.assertIsNotNone(self.alias("alpha.pdf"))


class MicrosoftStepUpContract(unittest.TestCase):
    def test_ms_session_start_records_a_recent_auth_marker(self):
        src = support.source_of("start_ms_session")
        self.assertIn("MS_RECENT_AUTH_SESSION_KEY", src)

    def test_first_password_requires_fresh_microsoft_login(self):
        src = support.source_of("change_password")
        self.assertIn("needs_ms_step_up", src)
        self.assertIn("_ms_recent_login_marker_valid(", src)
        self.assertIn("url_for(\"ms_login\")", src)
        self.assertIn("session.pop(MS_RECENT_AUTH_SESSION_KEY, None)", src)


class AskBudgetContract(unittest.TestCase):
    def test_turn_dispatch_is_capped_and_allowlisted(self):
        src = support.source_of("run_ask_turn")
        self.assertIn("allowed_tools", src)
        self.assertIn("MAX_TOOL_CALLS_PER_ROUND", src)
        self.assertIn("LIBRARY_TOOL_CALL_CAP", src)
        self.assertIn("MAX_TOOL_RESULT_CHARS", src)
        self.assertIn("name not in allowed_tools", src)

    def test_conversation_creation_has_an_owner_quota(self):
        src = support.source_of("api_conversations")
        self.assertIn("MAX_CONVERSATIONS_PER_OWNER", src)

    def test_ask_history_and_forced_sources_are_bounded(self):
        src = support.source_of("api_ai")
        self.assertIn("MAX_ASK_HISTORY_MESSAGES", src)
        self.assertIn("MAX_FORCED_PAPERS_PER_TURN", src)

    def test_history_messages_are_truncated_per_message(self):
        src = support.source_of("_ask_llm_messages")
        self.assertIn("MAX_ASK_HISTORY_MESSAGE_CHARS", src)


if __name__ == "__main__":
    unittest.main()
