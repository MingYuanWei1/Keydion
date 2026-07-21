"""Real-MySQL publishing concurrency and transactional contracts.

Every test owns a randomly named disposable database derived only from the
explicit test-admin URL.  Importing or discovering this module never connects
to an ambient application database.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

# Capture only the caller's explicit test-admin setting before application
# configuration has an opportunity to load deployment dotenv files.
MYSQL_ADMIN_URL = os.environ.get("PAPERQUERY_TEST_MYSQL_ADMIN_URL")

from alembic import command
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import models
import rag_index
import services.ai as ai_module
from config import RAG_EMBED_DIM
from models import (
    PaperChunkModel,
    PaperMetadataModel,
    PaperRevisionModel,
    PublishingJobModel,
    RagIndexMetaModel,
)
from services.paper_storage import PaperStorage
from services.publishing import PublishingLifecycle
from services.publishing_contracts import (
    Actor,
    DeletePaper,
    DeletionState,
    JobLease,
    PreparedChunk,
    PreparedRevisionIndex,
)
from services.publishing_jobs import (
    claim_job_id,
    claim_one_due,
    job_status,
    release_failed_job,
)


_TEST_DATABASE_RE = re.compile(r"keydion_test_[0-9a-f]{32}\Z")
_NOW = datetime(2026, 7, 22, 8, 30, 15)
_PAPER_A = "11111111-1111-4111-8111-111111111111"
_PAPER_B = "22222222-2222-4222-8222-222222222222"


def _validated_admin_url(raw_url: str):
    url = make_url(raw_url)
    if url.get_backend_name() != "mysql":
        raise ValueError("PAPERQUERY_TEST_MYSQL_ADMIN_URL must use MySQL")
    if url.database and not _TEST_DATABASE_RE.fullmatch(url.database):
        raise ValueError("refusing a non-generated database in the MySQL admin URL")
    return url


class _Embeddings:
    def create(self, *, model, input, **_kwargs):
        vector = [1.0] + [0.0] * (RAG_EMBED_DIM - 1)
        data = [type("Embedding", (), {"embedding": vector})() for _ in input]
        return type("Response", (), {"data": data})()


class _EmbeddingClient:
    def __init__(self):
        self.embeddings = _Embeddings()


@unittest.skipUnless(
    MYSQL_ADMIN_URL,
    "PAPERQUERY_TEST_MYSQL_ADMIN_URL is absent; real MySQL concurrency test skipped",
)
class PublishingMySQLConcurrencyTests(unittest.TestCase):
    def setUp(self):
        admin_url = _validated_admin_url(MYSQL_ADMIN_URL)
        self.database_name = f"keydion_test_{uuid.uuid4().hex}"
        if not _TEST_DATABASE_RE.fullmatch(self.database_name):
            raise ValueError("refusing unsafe generated MySQL test database name")

        self.server_url = admin_url.set(database=None)
        self.database_url = admin_url.set(database=self.database_name)
        self.admin_engine = create_engine(self.server_url, pool_pre_ping=True)
        self.addCleanup(self.admin_engine.dispose)
        try:
            with self.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        f"CREATE DATABASE `{self.database_name}` "
                        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                    )
                )
        finally:
            # Register exact cleanup even if the driver reports an error after
            # the CREATE reached MySQL.
            self.addCleanup(self._drop_exact_database)

        rendered_url = self.database_url.render_as_string(hide_password=False)
        self.data_temp = tempfile.TemporaryDirectory(prefix="keydion-mysql-data-")
        self.paper_temp = tempfile.TemporaryDirectory(prefix="keydion-mysql-papers-")
        self.pending_temp = tempfile.TemporaryDirectory(prefix="keydion-mysql-pending-")
        self.addCleanup(self.pending_temp.cleanup)
        self.addCleanup(self.paper_temp.cleanup)
        self.addCleanup(self.data_temp.cleanup)
        self.data_dir = Path(self.data_temp.name)
        self.papers_dir = Path(self.paper_temp.name)
        self.pending_dir = Path(self.pending_temp.name)
        environment = mock.patch.dict(
            os.environ,
            {
                "PAPERQUERY_DATABASE_URL": rendered_url,
                "PAPERQUERY_TEST_MYSQL_URL": rendered_url,
                "PAPERQUERY_DATA_DIR": str(self.data_dir),
                "PAPERQUERY_UPLOAD_DIR": str(self.papers_dir),
            },
        )
        environment.start()
        self.addCleanup(environment.stop)

        # A bootstrap engine creates and stamps only the empty generated
        # database.  Worker engines are separate pools, so their sessions can
        # never share a DBAPI connection accidentally.
        self.engine = create_engine(self.database_url, pool_pre_ping=True)
        self.addCleanup(self.engine.dispose)
        models.ensure_schema_current(self.engine)
        self.worker_a_engine = create_engine(self.database_url, pool_pre_ping=True)
        self.worker_b_engine = create_engine(self.database_url, pool_pre_ping=True)
        self.addCleanup(self.worker_b_engine.dispose)
        self.addCleanup(self.worker_a_engine.dispose)
        self.worker_a = sessionmaker(
            bind=self.worker_a_engine,
            expire_on_commit=False,
        )
        self.worker_b = sessionmaker(
            bind=self.worker_b_engine,
            expire_on_commit=False,
        )

        self.storage_a = PaperStorage(self.papers_dir, self.pending_dir)
        self.storage_b = PaperStorage(self.papers_dir, self.pending_dir)
        self.addCleanup(self.storage_b.close)
        self.addCleanup(self.storage_a.close)

    def _drop_exact_database(self):
        if not _TEST_DATABASE_RE.fullmatch(self.database_name):
            raise ValueError("refusing unsafe MySQL cleanup target")
        for name in ("worker_a_engine", "worker_b_engine", "engine"):
            engine = getattr(self, name, None)
            if engine is not None:
                engine.dispose()
        with self.admin_engine.begin() as connection:
            connection.execute(
                text(f"DROP DATABASE IF EXISTS `{self.database_name}`")
            )

    def _seed_paper(
        self,
        paper_id: str,
        *,
        filename: str,
        current_revision: int = 1,
        revisions: tuple[int, ...] = (1,),
    ) -> None:
        with self.worker_a() as session:
            session.add(
                PaperMetadataModel(
                    id=paper_id,
                    filename=filename,
                    title=filename,
                    language="en",
                    lifecycle_state="published",
                    current_revision=current_revision,
                    row_version=1,
                    index_status="pending",
                )
            )
            for revision in revisions:
                session.add(
                    PaperRevisionModel(
                        paper_id=paper_id,
                        revision_number=revision,
                        sha256=str(revision) * 64,
                        size_bytes=revision,
                        created_at=_NOW,
                        created_by="mysql-test",
                    )
                )
            session.commit()

    def _seed_job(
        self,
        *,
        job_id: str,
        paper_id: str,
        state: str = "pending",
        attempts: int = 0,
        available_at: datetime = _NOW,
        lease_token: str | None = None,
        lease_expires_at: datetime | None = None,
        created_at: datetime | None = None,
    ) -> None:
        created_at = created_at or (_NOW - timedelta(minutes=1))
        with self.worker_a() as session:
            session.add(
                PublishingJobModel(
                    id=job_id,
                    kind="index_revision",
                    paper_id=paper_id,
                    revision_number=1,
                    dedupe_key=f"index:{paper_id}:1",
                    state=state,
                    attempts=attempts,
                    available_at=available_at,
                    lease_token=lease_token,
                    lease_expires_at=lease_expires_at,
                    created_at=created_at,
                    updated_at=created_at,
                )
            )
            session.commit()

    def _lifecycle(self, factory, storage, generated_uuid: str) -> PublishingLifecycle:
        return PublishingLifecycle(
            session_factory=factory,
            storage=storage,
            indexer=object(),
            clock=lambda: _NOW,
            monotonic_clock=lambda: 1.0,
            uuid_factory=lambda: generated_uuid,
            jitter=lambda: 0.0,
        )

    @staticmethod
    def _lease_token(number: int) -> str:
        return f"aaaaaaaa-aaaa-4aaa-8aaa-{number:012d}"

    @staticmethod
    def _db_session(factory):
        @contextmanager
        def managed():
            session = factory()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        return managed

    def test_skip_locked_claims_distinct_jobs_without_double_claiming(self):
        self._seed_paper(_PAPER_A, filename="a.pdf")
        self._seed_paper(_PAPER_B, filename="b.pdf")
        first_id = "00000000-0000-4000-8000-000000000001"
        second_id = "00000000-0000-4000-8000-000000000002"
        self._seed_job(job_id=first_id, paper_id=_PAPER_A)
        self._seed_job(job_id=second_id, paper_id=_PAPER_B)

        with self.worker_a_engine.connect() as first_connection, \
             self.worker_b_engine.connect() as second_connection:
            self.assertNotEqual(
                first_connection.execute(text("SELECT CONNECTION_ID()")).scalar_one(),
                second_connection.execute(text("SELECT CONNECTION_ID()")).scalar_one(),
            )

        first_locked = threading.Event()
        release_first = threading.Event()
        first_finished = threading.Event()
        second_finished = threading.Event()
        results = {}
        errors = []

        def paused_first_token():
            # _claim invokes its token factory only after SELECT ... FOR UPDATE
            # has returned the due row, while that row lock is still held and
            # before the job is mutated or committed.
            first_locked.set()
            if not release_first.wait(10):
                raise TimeoutError("first claim was not released")
            return self._lease_token(1)

        def claim_first_job():
            try:
                results["first"] = claim_one_due(
                    self.worker_a,
                    _NOW,
                    1800,
                    paused_first_token,
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                first_finished.set()

        def claim_second_job():
            try:
                results["second"] = claim_one_due(
                    self.worker_b,
                    _NOW,
                    1800,
                    lambda: self._lease_token(2),
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                second_finished.set()

        first_worker = threading.Thread(target=claim_first_job, daemon=True)
        second_worker = threading.Thread(target=claim_second_job, daemon=True)
        second_started = False
        locked_before_second = False
        first_remained_paused = False
        second_completed_while_locked = False
        try:
            first_worker.start()
            locked_before_second = first_locked.wait(5)
            first_remained_paused = (
                not first_finished.is_set() if locked_before_second else False
            )
            if locked_before_second:
                second_worker.start()
                second_started = True
            second_completed_while_locked = (
                second_finished.wait(5) if second_started else False
            )
        finally:
            release_first.set()
        first_worker.join(5)
        if second_started:
            second_worker.join(5)

        self.assertTrue(locked_before_second)
        self.assertTrue(first_remained_paused)
        self.assertTrue(
            second_completed_while_locked,
            "second production claim blocked instead of skipping the locked due row",
        )
        self.assertFalse(first_worker.is_alive())
        self.assertFalse(second_started and second_worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results["first"].job_id, first_id)
        self.assertEqual(results["second"].job_id, second_id)
        self.assertNotEqual(
            results["first"].lease_token,
            results["second"].lease_token,
        )
        self.assertIsNone(
            claim_one_due(
                self.worker_a,
                _NOW,
                1800,
                lambda: self._lease_token(3),
            )
        )
        with self.worker_a() as session:
            jobs = session.query(PublishingJobModel).order_by(
                PublishingJobModel.id
            ).all()
            self.assertEqual([(job.state, job.attempts) for job in jobs], [
                ("running", 1),
                ("running", 1),
            ])

    def test_expiry_reclaims_crashed_job_and_stale_token_cannot_release_it(self):
        self._seed_paper(_PAPER_A, filename="lease.pdf")
        job_id = "33333333-3333-4333-8333-333333333333"
        self._seed_job(job_id=job_id, paper_id=_PAPER_A)
        old = claim_job_id(
            self.worker_a,
            job_id,
            _NOW,
            1,
            lambda: self._lease_token(10),
        )
        reclaimed_at = _NOW + timedelta(seconds=2)
        reclaimed = claim_job_id(
            self.worker_b,
            job_id,
            reclaimed_at,
            1800,
            lambda: self._lease_token(11),
        )

        self.assertEqual(reclaimed.job_id, job_id)
        self.assertEqual(reclaimed.attempts, 2)
        self.assertIsNone(
            release_failed_job(
                self.worker_a,
                old,
                RuntimeError("stale worker"),
                reclaimed_at,
                jitter=0.0,
            )
        )
        with self.worker_b() as session:
            persisted = session.get(PublishingJobModel, job_id)
            self.assertEqual(persisted.state, "running")
            self.assertEqual(persisted.lease_token, reclaimed.lease_token)
            self.assertEqual(persisted.attempts, 2)

    def test_index_delete_race_leaves_no_orphan_chunks(self):
        self._seed_paper(_PAPER_A, filename="race.pdf")
        job_id = "44444444-4444-4444-8444-444444444444"
        self._seed_job(
            job_id=job_id,
            paper_id=_PAPER_A,
            state="running",
            attempts=1,
            lease_token=self._lease_token(20),
            lease_expires_at=_NOW + timedelta(minutes=30),
        )
        with self.worker_a() as session:
            job = session.get(PublishingJobModel, job_id)
            lease = JobLease(
                job_id=job.id,
                paper_id=job.paper_id,
                revision=job.revision_number,
                kind=job.kind,
                attempts=job.attempts,
                lease_token=job.lease_token,
                lease_expires_at=job.lease_expires_at,
                created_at=job.created_at,
                previous_updated_at=job.updated_at,
            )
        prepared = PreparedRevisionIndex(
            paper_id=_PAPER_A,
            revision=1,
            chunks=(
                PreparedChunk(
                    chunk_index=0,
                    content="racing index content",
                    embedding=(1.0,) + (0.0,) * (RAG_EMBED_DIM - 1),
                    language="en",
                ),
            ),
        )
        index_lifecycle = self._lifecycle(
            self.worker_a,
            self.storage_a,
            "55555555-5555-4555-8555-555555555555",
        )
        delete_lifecycle = self._lifecycle(
            self.worker_b,
            self.storage_b,
            "66666666-6666-4666-8666-666666666666",
        )
        index_paper_locked = threading.Event()
        release_index = threading.Event()
        delete_paper_lock_attempted = threading.Event()
        index_finished = threading.Event()
        delete_finished = threading.Event()
        outcomes = {}
        errors = []

        def is_paper_lock(statement):
            normalized = " ".join(statement.casefold().split())
            return (
                "from papers_metadata" in normalized
                and "for update" in normalized
            )

        def pause_after_index_paper_lock(
            _connection,
            _cursor,
            statement,
            _parameters,
            _context,
            _executemany,
        ):
            if is_paper_lock(statement) and not index_paper_locked.is_set():
                # The SELECT has completed, so InnoDB holds the Paper row lock.
                index_paper_locked.set()
                if not release_index.wait(10):
                    raise TimeoutError("index Paper lock was not released")

        def observe_delete_paper_lock_attempt(
            _connection,
            _cursor,
            statement,
            _parameters,
            _context,
            _executemany,
        ):
            if is_paper_lock(statement):
                delete_paper_lock_attempted.set()

        def complete_index():
            try:
                outcomes["index"] = index_lifecycle._complete_index(lease, prepared)
            except BaseException as exc:
                errors.append(exc)
            finally:
                index_finished.set()

        def delete_paper():
            try:
                outcomes["delete"] = delete_lifecycle.delete_paper(
                    DeletePaper(Actor("mysql-curator", 3), _PAPER_A, 1)
                )
            except BaseException as exc:
                errors.append(exc)
            finally:
                delete_finished.set()

        index_thread = threading.Thread(target=complete_index, daemon=True)
        delete_thread = threading.Thread(target=delete_paper, daemon=True)
        event.listen(
            self.worker_a_engine,
            "after_cursor_execute",
            pause_after_index_paper_lock,
        )
        event.listen(
            self.worker_b_engine,
            "before_cursor_execute",
            observe_delete_paper_lock_attempt,
        )
        delete_started = False
        index_locked_before_delete = False
        index_remained_paused = False
        delete_attempted_while_index_locked = False
        delete_remained_blocked = False
        try:
            index_thread.start()
            index_locked_before_delete = index_paper_locked.wait(5)
            index_remained_paused = (
                not index_finished.is_set() if index_locked_before_delete else False
            )
            if index_locked_before_delete:
                delete_thread.start()
                delete_started = True
            delete_attempted_while_index_locked = (
                delete_paper_lock_attempted.wait(5) if delete_started else False
            )
            delete_remained_blocked = (
                not delete_finished.is_set()
                if delete_attempted_while_index_locked
                else False
            )
        finally:
            release_index.set()
            index_thread.join(10)
            if delete_started:
                delete_thread.join(10)
            event.remove(
                self.worker_a_engine,
                "after_cursor_execute",
                pause_after_index_paper_lock,
            )
            event.remove(
                self.worker_b_engine,
                "before_cursor_execute",
                observe_delete_paper_lock_attempt,
            )

        self.assertTrue(index_locked_before_delete)
        self.assertTrue(index_remained_paused)
        self.assertTrue(delete_attempted_while_index_locked)
        self.assertTrue(delete_remained_blocked)
        self.assertFalse(index_thread.is_alive())
        self.assertFalse(delete_started and delete_thread.is_alive())
        self.assertTrue(index_finished.is_set())
        self.assertTrue(delete_finished.is_set())
        self.assertEqual(errors, [])
        self.assertIs(outcomes["index"], True)
        self.assertEqual(outcomes["delete"].state, DeletionState.DELETED)
        with self.worker_a() as session:
            self.assertIsNone(session.get(PaperMetadataModel, _PAPER_A))
            self.assertEqual(
                session.query(PaperChunkModel).filter_by(paper_id=_PAPER_A).count(),
                0,
            )
            self.assertEqual(
                session.query(PublishingJobModel).filter_by(paper_id=_PAPER_A).count(),
                0,
            )

    def test_revision_switch_between_snapshot_score_and_fetch_hides_old_content(self):
        self._seed_paper(
            _PAPER_A,
            filename="revision-race.pdf",
            revisions=(1, 2),
        )
        vector = json.dumps([1.0] + [0.0] * (RAG_EMBED_DIM - 1))
        with self.worker_a() as session:
            session.add(
                PaperChunkModel(
                    paper_id=_PAPER_A,
                    revision_number=1,
                    chunk_index=0,
                    content="old visible content",
                    embedding_vec=vector,
                    lang="en",
                )
            )
            session.commit()

        saved_dependencies = dict(rag_index._DEPS)
        saved_qvec = dict(rag_index._QVEC_CACHE)

        def restore_rag_state():
            rag_index._DEPS.clear()
            rag_index._DEPS.update(saved_dependencies)
            rag_index._QVEC_CACHE.clear()
            rag_index._QVEC_CACHE.update(saved_qvec)
            rag_index.invalidate_cache()

        self.addCleanup(restore_rag_state)
        db_session = self._db_session(self.worker_a)
        switched = threading.Event()

        def switch_then_fetch(ids):
            with self.worker_b() as session:
                paper = session.get(PaperMetadataModel, _PAPER_A)
                paper.current_revision = 2
                session.get(RagIndexMetaModel, "chunks_version").value += 1
                session.commit()
            switched.set()
            return ai_module._rag_fetch_chunks(ids)

        with mock.patch.object(ai_module, "db_session", db_session):
            rag_index.configure(
                build_embed_client=lambda: _EmbeddingClient(),
                embed_model=lambda: "mysql-test-embedding",
                embed_batch_size=lambda: 10,
                store_version=ai_module._rag_store_version,
                store_vectors=ai_module._rag_store_vectors,
                fetch_chunks=switch_then_fetch,
                fetch_papers=ai_module._rag_fetch_papers,
            )
            rag_index.invalidate_cache()
            hits = rag_index.retrieve("old", min_sim=0.0)

        self.assertTrue(switched.is_set())
        self.assertEqual(hits, [])

    def test_version_stamp_commit_and_rollback_are_transactional(self):
        lifecycle = self._lifecycle(
            self.worker_a,
            self.storage_a,
            "77777777-7777-4777-8777-777777777777",
        )
        transaction = self.worker_a()
        transaction.begin()
        lifecycle._bump_rag_version(transaction)
        transaction.flush()
        with self.worker_b() as observer:
            self.assertEqual(
                observer.get(RagIndexMetaModel, "chunks_version").value,
                0,
            )
        transaction.rollback()
        transaction.close()
        with self.worker_b() as observer:
            self.assertEqual(
                observer.get(RagIndexMetaModel, "chunks_version").value,
                0,
            )

        with self.worker_a() as committed:
            lifecycle._bump_rag_version(committed)
            committed.commit()
        with self.worker_b() as observer:
            self.assertEqual(
                observer.get(RagIndexMetaModel, "chunks_version").value,
                1,
            )

    def test_composite_revision_ownership_rejects_cross_paper_chunk(self):
        self._seed_paper(_PAPER_A, filename="owner.pdf", revisions=(1,))
        self._seed_paper(
            _PAPER_B,
            filename="other.pdf",
            current_revision=2,
            revisions=(2,),
        )
        session = self.worker_a()
        try:
            session.add(
                PaperChunkModel(
                    paper_id=_PAPER_A,
                    revision_number=2,
                    chunk_index=0,
                    content="wrong owner",
                    embedding_vec=json.dumps(
                        [1.0] + [0.0] * (RAG_EMBED_DIM - 1)
                    ),
                    lang="en",
                )
            )
            with self.assertRaises(IntegrityError):
                session.commit()
            session.rollback()
        finally:
            session.close()
        with self.worker_b() as observer:
            self.assertEqual(observer.query(PaperChunkModel).count(), 0)

    def test_fresh_mysql_schema_has_no_orm_drift(self):
        alembic_config = models._alembic_config()
        with self.engine.connect() as connection:
            alembic_config.attributes["connection"] = connection
            try:
                command.check(alembic_config)
            finally:
                alembic_config.attributes.pop("connection", None)

    def test_naive_utc_queue_timestamps_and_status_age_round_trip_exactly(self):
        self._seed_paper(_PAPER_A, filename="time.pdf")
        job_id = "88888888-8888-4888-8888-888888888888"
        available_at = _NOW - timedelta(seconds=10)
        created_at = _NOW - timedelta(seconds=91)
        self._seed_job(
            job_id=job_id,
            paper_id=_PAPER_A,
            available_at=available_at,
            created_at=created_at,
        )
        with self.worker_b() as observer:
            job = observer.get(PublishingJobModel, job_id)
            self.assertEqual(job.available_at, available_at)
            self.assertIsNone(job.available_at.tzinfo)
            self.assertEqual(job.created_at, created_at)
            self.assertIsNone(job.created_at.tzinfo)

        lease = claim_job_id(
            self.worker_a,
            job_id,
            _NOW,
            1800,
            lambda: self._lease_token(30),
        )
        self.assertEqual(lease.lease_expires_at, _NOW + timedelta(seconds=1800))
        self.assertIsNone(lease.lease_expires_at.tzinfo)
        with self.worker_b() as observer:
            persisted = observer.get(PublishingJobModel, job_id)
            self.assertEqual(
                persisted.lease_expires_at,
                _NOW + timedelta(seconds=1800),
            )
            self.assertIsNone(persisted.lease_expires_at.tzinfo)
        progress = release_failed_job(
            self.worker_b,
            lease,
            RuntimeError("retry"),
            _NOW,
            jitter=0.0,
        )
        self.assertEqual(progress.next_retry_at, _NOW + timedelta(seconds=30))
        self.assertIsNone(progress.next_retry_at.tzinfo)
        with self.worker_a() as observer:
            job = observer.get(PublishingJobModel, job_id)
            self.assertEqual(job.available_at, _NOW + timedelta(seconds=30))
            self.assertIsNone(job.available_at.tzinfo)
            self.assertIsNone(job.lease_expires_at)
            self.assertEqual(job.updated_at, _NOW)
            self.assertIsNone(job.updated_at.tzinfo)

        status = job_status(self.worker_b, _NOW + timedelta(seconds=45))
        self.assertEqual((status.pending, status.running), (1, 0))
        self.assertEqual(status.oldest_age_seconds, 136)


if __name__ == "__main__":
    unittest.main()
