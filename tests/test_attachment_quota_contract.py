"""Contract: attachment uploads cannot fan out into unbounded durable work.

Security-review finding [5]: each 5 MiB upload durably enqueued raw payloads
with no per-conversation/owner job or byte budgets, and one upload could fan
out into thousands of chunks and paid embedding calls.

Enforced here:
1. queue_attachment refuses work past per-conversation active-job and
   queued-byte budgets, and a per-owner aggregate queued-byte budget.
2. The processing path caps the chunk count per attachment.
3. The upload route reports quota refusals distinctly from parse errors.
"""
import sys
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import tests.publishing_support  # noqa: F401  registers sqlite VECTOR compile
from db import BASE
from models import ConversationModel
import services.attachment_jobs as aj


class AttachmentQueueSqliteBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = create_engine(f"sqlite:///{Path(self.tmp.name) / 'attach.sqlite'}")
        BASE.metadata.create_all(self.engine)
        factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.factory = factory

        @contextmanager
        def fake_db_session():
            session = factory()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        patch = mock.patch.object(aj, "db_session", fake_db_session)
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(self.engine.dispose)
        self.addCleanup(self.tmp.cleanup)

        with fake_db_session() as db:
            db.add(ConversationModel(id=1, serial="aaaaaa", owner_key="local:alice",
                                     title="c1", created_at="", updated_at=""))
            db.add(ConversationModel(id=2, serial="bbbbbb", owner_key="local:alice",
                                     title="c2", created_at="", updated_at=""))
            db.add(ConversationModel(id=3, serial="cccccc", owner_key="local:bob",
                                     title="c3", created_at="", updated_at=""))

    def queue(self, conv_id, name, payload=b"x"):
        return aj.queue_attachment(conv_id, name, payload)


class ConversationJobBudgets(AttachmentQueueSqliteBase):
    def test_active_job_count_capped_per_conversation(self):
        limit = aj.MAX_ACTIVE_JOBS_PER_CONVERSATION
        for i in range(limit):
            self.queue(1, f"file-{i}.txt")
        with self.assertRaises(aj.AttachmentQuotaExceeded):
            self.queue(1, "one-too-many.txt")

    def test_other_conversation_unaffected_by_count_cap(self):
        limit = aj.MAX_ACTIVE_JOBS_PER_CONVERSATION
        for i in range(limit):
            self.queue(1, f"file-{i}.txt")
        self.queue(3, "other-owner.txt")  # different owner entirely

    def test_superseded_jobs_do_not_count_against_cap(self):
        limit = aj.MAX_ACTIVE_JOBS_PER_CONVERSATION
        for i in range(limit):
            self.queue(1, "same-name.txt")  # each supersedes the previous
        # Still one active job for that name, so one more distinct name fits.
        self.queue(1, "different.txt")


class ConversationByteBudget(AttachmentQueueSqliteBase):
    def test_queued_bytes_capped_per_conversation(self):
        # Patch to small caps so the budget is exercised without large blobs.
        with mock.patch.object(aj, "MAX_QUEUED_BYTES_PER_CONVERSATION", 30), \
             mock.patch.object(aj, "MAX_QUEUED_BYTES_PER_OWNER", 10 ** 9):
            payload = b"x" * 10
            self.queue(1, "big-0.bin", payload)   # 10
            self.queue(1, "big-1.bin", payload)   # 20
            self.queue(1, "big-2.bin", payload)   # 30 (at cap)
            with self.assertRaises(aj.AttachmentQuotaExceeded):
                self.queue(1, "big-overflow.bin", payload)   # 40 > 30


class OwnerByteBudget(AttachmentQueueSqliteBase):
    def test_owner_aggregate_bytes_capped_across_conversations(self):
        # Conversation cap is generous; only the owner cap should trigger.
        with mock.patch.object(aj, "MAX_QUEUED_BYTES_PER_CONVERSATION", 100), \
             mock.patch.object(aj, "MAX_QUEUED_BYTES_PER_OWNER", 30):
            payload = b"x" * 10
            self.queue(1, "a.bin", payload)   # owner 10
            self.queue(2, "b.bin", payload)   # owner 20 (other conversation)
            self.queue(2, "c.bin", payload)   # owner 30 (at cap)
            with self.assertRaises(aj.AttachmentQuotaExceeded) as ctx:
                self.queue(1, "d.bin", payload)   # owner 40 > 30
            self.assertIn("account", str(ctx.exception))

    def test_other_owner_unaffected(self):
        with mock.patch.object(aj, "MAX_QUEUED_BYTES_PER_CONVERSATION", 100), \
             mock.patch.object(aj, "MAX_QUEUED_BYTES_PER_OWNER", 30):
            payload = b"x" * 10
            self.queue(1, "a.bin", payload)   # alice owner 10
            self.queue(2, "b.bin", payload)   # alice owner 20
            self.queue(2, "c.bin", payload)   # alice owner 30 (at her cap)
            # bob (conv 3) is under his own owner budget and unaffected.
            self.queue(3, "bob.bin", payload)


class ChunkCapContract(unittest.TestCase):
    def test_run_one_caps_chunk_count(self):
        # run_one exists in both attachment_jobs and publishing_jobs; pin the
        # attachment worker's copy explicitly.
        import inspect
        src = inspect.getsource(aj.run_one)
        self.assertIn("MAX_CHUNKS_PER_ATTACHMENT", src,
                      "attachment processing must cap chunk fan-out")

    def test_route_reports_quota_distinctly(self):
        src = support.source_of("api_ai_attach")
        self.assertIn("AttachmentQuotaExceeded", src)


if __name__ == "__main__":
    unittest.main()
