from __future__ import annotations

import tempfile
import types
import unittest
from contextlib import contextmanager
from dataclasses import FrozenInstanceError
from datetime import timedelta
from pathlib import Path
from unittest import mock

import numpy as np
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

import rag_index
import services.ai as ai_module
import services.search as search_module
from db import BASE
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
    IndexDeadlineExceeded,
    JobLease,
    PreparedChunk,
    PreparedRevisionIndex,
)
from services.publishing_rag import StrictRagAdapter

# Register the SQLite compiler for the MySQL VECTOR type used by lifecycle tests.
from tests import publishing_support as _publishing_support  # noqa: F401,E402


PAPER_A = "11111111-1111-4111-8111-111111111111"
PAPER_B = "22222222-2222-4222-8222-222222222222"
PAPER_C = "33333333-3333-4333-8333-333333333333"


def _f32(values):
    return np.asarray(values, dtype="<f4").tobytes()


class _Embeddings:
    def __init__(self, *, error=None, calls=None):
        self.error = error
        self.calls = calls if calls is not None else []

    def create(self, *, model, input, **kwargs):
        self.calls.append((model, tuple(input), kwargs))
        if self.error is not None:
            raise self.error
        vectors = []
        for text in input:
            lowered = text.lower()
            vector = [1.0, 0.0] if "cold" in lowered else [0.0, 1.0]
            vectors.append(type("Embedding", (), {"embedding": vector})())
        return type("Response", (), {"data": vectors})()


class _Client:
    def __init__(self, *, error=None, calls=None):
        self.embeddings = _Embeddings(error=error, calls=calls)


class StrictRagAdapterTest(unittest.TestCase):
    def adapter(self, **overrides):
        dependencies = {
            "embedding_available": lambda: True,
            "extract_text": lambda _pdf, **_kwargs: "cold indexed text",
            "chunker": rag_index.chunk_text,
            "embed_texts": lambda texts, **_kwargs: [[1.0, 0.0] for _ in texts],
            "vision_available": lambda: False,
            "transcribe": lambda *_args, **_kwargs: "",
            "monotonic": lambda: 1.0,
        }
        dependencies.update(overrides)
        return StrictRagAdapter(**dependencies)

    def test_enabled_delegates_to_embedding_capability(self):
        self.assertFalse(
            self.adapter(embedding_available=lambda: False).enabled()
        )
        self.assertTrue(self.adapter().enabled())

    def test_prepare_is_revision_targeted_frozen_and_side_effect_free(self):
        calls = []

        def extract(pdf_bytes, **kwargs):
            calls.append(("extract", pdf_bytes, kwargs))
            return "cold indexed text"

        def embed(chunks, **kwargs):
            calls.append(("embed", tuple(chunks), kwargs))
            return [[1, 0] for _ in chunks]

        prepared = self.adapter(extract_text=extract, embed_texts=embed).prepare(
            paper_id=PAPER_A,
            revision_number=2,
            pdf_bytes=b"immutable revision bytes",
            language="en",
            deadline=45.0,
        )

        self.assertEqual(prepared.paper_id, PAPER_A)
        self.assertEqual(prepared.revision, 2)
        self.assertTrue(prepared.chunks)
        self.assertEqual(prepared.chunks[0].embedding, (1.0, 0.0))
        self.assertEqual(calls[0][0], "extract")
        self.assertTrue(calls[0][2]["strict"])
        self.assertEqual(calls[-1][0], "embed")
        with self.assertRaises(FrozenInstanceError):
            prepared.chunks[0].content = "mutated"

    def test_extraction_and_embedding_failures_propagate_strictly(self):
        extraction_error = RuntimeError("extract failed")
        with self.assertRaises(RuntimeError) as extraction:
            self.adapter(
                extract_text=mock.Mock(side_effect=extraction_error)
            ).prepare(
                paper_id=PAPER_A,
                revision_number=1,
                pdf_bytes=b"pdf",
                language="en",
                deadline=10.0,
            )
        self.assertIs(extraction.exception, extraction_error)

        embedding_error = RuntimeError("embed failed")
        with self.assertRaises(RuntimeError) as embedding:
            self.adapter(
                embed_texts=mock.Mock(side_effect=embedding_error)
            ).prepare(
                paper_id=PAPER_A,
                revision_number=1,
                pdf_bytes=b"pdf",
                language="en",
                deadline=10.0,
            )
        self.assertIs(embedding.exception, embedding_error)

    def test_stage_failure_that_consumes_deadline_becomes_deadline_error(self):
        for stage in ("extract", "embed"):
            with self.subTest(stage=stage):
                now = [1.0]
                failure = TimeoutError(f"{stage} timed out")

                def fail(*_args, **_kwargs):
                    now[0] = 10.0
                    raise failure

                overrides = {
                    "monotonic": lambda: now[0],
                    "extract_text": fail if stage == "extract" else (
                        lambda _pdf, **_kwargs: "cold indexed text"
                    ),
                    "embed_texts": fail if stage == "embed" else (
                        lambda texts, **_kwargs: [[1.0, 0.0] for _ in texts]
                    ),
                }
                with self.assertRaises(IndexDeadlineExceeded) as raised:
                    self.adapter(**overrides).prepare(
                        paper_id=PAPER_A,
                        revision_number=1,
                        pdf_bytes=b"pdf",
                        language="en",
                        deadline=10.0,
                    )
                self.assertIs(raised.exception.__cause__, failure)

    def test_empty_extraction_is_a_strict_failure(self):
        with self.assertRaisesRegex(ValueError, "indexable text"):
            self.adapter(extract_text=lambda _pdf, **_kwargs: "  ").prepare(
                paper_id=PAPER_A,
                revision_number=1,
                pdf_bytes=b"pdf",
                language="en",
                deadline=10.0,
            )

    def test_prepare_rejects_mutable_revision_bytes(self):
        with self.assertRaisesRegex(TypeError, "immutable bytes"):
            self.adapter().prepare(
                paper_id=PAPER_A,
                revision_number=1,
                pdf_bytes=bytearray(b"pdf"),
                language="en",
                deadline=10.0,
            )

    def test_embedding_result_count_and_values_are_validated(self):
        for vectors in (
            [],
            [[], [1.0]],
            [["not-a-number"]],
            [[float("nan"), 0.0]],
            [[float("inf"), 0.0]],
        ):
            with self.subTest(vectors=vectors):
                with self.assertRaisesRegex(ValueError, "embedding"):
                    self.adapter(
                        embed_texts=lambda _texts, **_kwargs: vectors
                    ).prepare(
                        paper_id=PAPER_A,
                        revision_number=1,
                        pdf_bytes=b"pdf",
                        language="en",
                        deadline=10.0,
                    )

    def test_deadline_exhaustion_before_and_between_stages(self):
        extraction = mock.Mock(return_value="cold indexed text")
        with self.assertRaises(IndexDeadlineExceeded):
            self.adapter(monotonic=lambda: 10.0, extract_text=extraction).prepare(
                paper_id=PAPER_A,
                revision_number=1,
                pdf_bytes=b"pdf",
                language="en",
                deadline=10.0,
            )
        extraction.assert_not_called()

        ticks = iter((1.0, 11.0))
        embedding = mock.Mock(return_value=[[1.0, 0.0]])
        with self.assertRaises(IndexDeadlineExceeded):
            self.adapter(
                monotonic=lambda: next(ticks),
                extract_text=lambda _pdf, **_kwargs: "cold indexed text",
                embed_texts=embedding,
            ).prepare(
                paper_id=PAPER_A,
                revision_number=1,
                pdf_bytes=b"pdf",
                language="en",
                deadline=10.0,
            )
        embedding.assert_not_called()

    def test_vision_fallback_receives_the_same_deadline(self):
        observed = {}

        def transcribe(pdf_bytes, **kwargs):
            observed.update(pdf_bytes=pdf_bytes, **kwargs)
            return "vision text"

        def extract(pdf_bytes, **kwargs):
            return kwargs["vision_fallback"](pdf_bytes, 50)

        prepared = self.adapter(
            extract_text=extract,
            vision_available=lambda: True,
            transcribe=transcribe,
        ).prepare(
            paper_id=PAPER_A,
            revision_number=1,
            pdf_bytes=b"pdf",
            language="zh",
            deadline=20.0,
        )

        self.assertTrue(prepared.chunks)
        self.assertEqual(observed["deadline"], 20.0)
        self.assertEqual(observed["language"], "zh")
        self.assertTrue(observed["strict"])


class RevisionVisibilityTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.engine = create_engine(f"sqlite:///{root / 'rag.sqlite'}")

        @event.listens_for(self.engine, "connect")
        def sqlite_functions(connection, _record):
            connection.create_function("STRING_TO_VECTOR", 1, lambda value: value)
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        BASE.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.session_factory() as session:
            session.add(RagIndexMetaModel(name="chunks_version", value=1))
            session.commit()
        self.storage = PaperStorage(root / "papers", root / "pending")
        self._saved_deps = dict(rag_index._DEPS)
        self._saved_qvec = dict(rag_index._QVEC_CACHE)

    def tearDown(self):
        rag_index._DEPS.clear()
        rag_index._DEPS.update(self._saved_deps)
        rag_index._QVEC_CACHE.clear()
        rag_index._QVEC_CACHE.update(self._saved_qvec)
        rag_index.invalidate_cache()
        self.storage.close()
        self.engine.dispose()
        self.tmp.cleanup()
        super().tearDown()

    @contextmanager
    def db_session(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def seed_paper(
        self,
        paper_id,
        *,
        filename,
        lifecycle_state="published",
        current_revision=1,
        revisions=(1,),
        chunks=(),
    ):
        with self.session_factory() as session:
            session.add(
                PaperMetadataModel(
                    id=paper_id,
                    filename=filename,
                    title=f"Title {filename}",
                    author_name="Author",
                    language="en",
                    lifecycle_state=lifecycle_state,
                    current_revision=current_revision,
                    row_version=1,
                    index_status="ready",
                    indexed_revision=current_revision,
                )
            )
            for revision in revisions:
                session.add(
                    PaperRevisionModel(
                        paper_id=paper_id,
                        revision_number=revision,
                        sha256=str(revision) * 64,
                        size_bytes=revision,
                        created_at=__import__("datetime").datetime(2026, 7, 21, 9, 30),
                        created_by="test",
                    )
                )
            session.flush()
            for index, (revision, content, vector) in enumerate(chunks):
                session.add(
                    PaperChunkModel(
                        filename=f"legacy-{paper_id}-{revision}.pdf",
                        paper_id=paper_id,
                        revision_number=revision,
                        chunk_index=index,
                        content=content,
                        embedding_vec=_f32(vector),
                        lang="en",
                    )
                )
            session.commit()

    @contextmanager
    def configured(self):
        with mock.patch.object(ai_module, "db_session", self.db_session), \
             mock.patch.object(search_module, "db_session", self.db_session), \
             mock.patch.object(
                 ai_module.llm_client,
                 "build_embed_client",
                 side_effect=lambda **_kwargs: _Client(),
             ), \
             mock.patch.object(
                 ai_module.llm_client,
                 "embed_model",
                 return_value="test-embedding",
             ):
            ai_module.configure_rag()
            rag_index._QVEC_CACHE.clear()
            rag_index.invalidate_cache()
            yield

    def set_paper_state(self, paper_id, *, state=None, current_revision=None, bump=True):
        with self.session_factory() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            if state is not None:
                paper.lifecycle_state = state
            if current_revision is not None:
                paper.current_revision = current_revision
            if bump:
                session.get(RagIndexMetaModel, "chunks_version").value += 1
            session.commit()

    def test_snapshot_and_fetch_return_only_current_visible_revision(self):
        self.seed_paper(
            PAPER_A,
            filename="visible.pdf",
            chunks=((1, "cold visible", [1.0, 0.0]),),
        )
        self.seed_paper(
            PAPER_B,
            filename="deleting.pdf",
            lifecycle_state="deleting",
            chunks=((1, "cold deleting", [1.0, 0.0]),),
        )
        self.seed_paper(
            PAPER_C,
            filename="revised.pdf",
            current_revision=2,
            revisions=(1, 2),
            chunks=(
                (1, "cold obsolete", [1.0, 0.0]),
                (2, "history current", [0.0, 1.0]),
            ),
        )

        with self.configured():
            vectors = ai_module._rag_store_vectors()
            self.assertEqual(
                {(row["paper_id"], row["revision_number"]) for row in vectors},
                {(PAPER_A, 1), (PAPER_C, 2)},
            )
            hits = rag_index.retrieve("cold", k=10, min_sim=0.0)

        self.assertEqual({hit["paper_id"] for hit in hits}, {PAPER_A, PAPER_C})
        by_id = {hit["paper_id"]: hit for hit in hits}
        self.assertEqual(by_id[PAPER_A]["filename"], "visible.pdf")
        self.assertEqual(by_id[PAPER_C]["revision_number"], 2)
        self.assertEqual(by_id[PAPER_C]["content"], "history current")
        self.assertNotIn("cold obsolete", {hit["content"] for hit in hits})

    def test_revision_switch_between_scoring_and_fetch_hides_old_chunk(self):
        self.seed_paper(
            PAPER_A,
            filename="switch.pdf",
            revisions=(1, 2),
            chunks=((1, "cold old revision", [1.0, 0.0]),),
        )

        with self.configured():
            original_fetch = ai_module._rag_fetch_chunks

            def switch_then_fetch(ids):
                self.set_paper_state(PAPER_A, current_revision=2)
                return original_fetch(ids)

            rag_index._DEPS["fetch_chunks"] = switch_then_fetch
            self.assertEqual(rag_index.retrieve("cold", min_sim=0.0), [])

    def test_cached_snapshot_after_stamp_failure_still_rechecks_visibility(self):
        self.seed_paper(
            PAPER_A,
            filename="cached.pdf",
            chunks=((1, "cold cached", [1.0, 0.0]),),
        )

        with self.configured():
            self.assertTrue(rag_index.retrieve("cold", min_sim=0.0))
            self.set_paper_state(PAPER_A, state="deleting")

            def stamp_failure():
                raise RuntimeError("stamp unavailable")

            rag_index._DEPS["store_version"] = stamp_failure
            self.assertEqual(rag_index.retrieve("cold", min_sim=0.0), [])

    def test_cached_snapshot_semantic_and_related_results_recheck_visibility(self):
        self.seed_paper(
            PAPER_A,
            filename="source.pdf",
            chunks=((1, "cold source", [1.0, 0.0]),),
        )
        self.seed_paper(
            PAPER_B,
            filename="candidate.pdf",
            chunks=((1, "cold candidate", [0.9, 0.1]),),
        )

        with self.configured():
            self.assertTrue(rag_index.search_papers_semantic("cold", min_sim=0.0))
            self.set_paper_state(PAPER_B, state="deleting")

            def stamp_failure():
                raise RuntimeError("stamp unavailable")

            rag_index._DEPS["store_version"] = stamp_failure
            semantic_ids = {
                paper_id
                for paper_id, _score in rag_index.search_papers_semantic(
                    "cold", min_sim=0.0
                )
            }
            related_ids = {
                paper_id
                for paper_id, _score in rag_index.related_papers(
                    PAPER_A, min_sim=0.0
                )
            }

        self.assertEqual(semantic_ids, {PAPER_A})
        self.assertEqual(related_ids, set())

    def test_filename_is_projected_at_fetch_not_cached_as_identity(self):
        self.seed_paper(
            PAPER_A,
            filename="before.pdf",
            chunks=((1, "cold body", [1.0, 0.0]),),
        )

        with self.configured():
            rag_index.warm()
            with self.session_factory() as session:
                session.get(PaperMetadataModel, PAPER_A).filename = "after.pdf"
                session.commit()
            hits = rag_index.retrieve("cold", min_sim=0.0)

        self.assertEqual(hits[0]["paper_id"], PAPER_A)
        self.assertEqual(hits[0]["filename"], "after.pdf")

    def test_paper_pooling_and_related_results_are_grouped_by_uuid(self):
        self.seed_paper(
            PAPER_A,
            filename="renamed.pdf",
            chunks=(
                (1, "cold one", [1.0, 0.0]),
                (1, "cold two", [0.9, 0.1]),
            ),
        )
        self.seed_paper(
            PAPER_B,
            filename="other.pdf",
            chunks=((1, "cold other", [0.8, 0.2]),),
        )

        with self.configured():
            semantic = rag_index.search_papers_semantic("cold", min_sim=0.0)
            related = rag_index.related_papers(PAPER_A, min_sim=0.0)

        self.assertEqual({paper_id for paper_id, _ in semantic}, {PAPER_A, PAPER_B})
        self.assertEqual([paper_id for paper_id, _ in related], [PAPER_B])

    def test_lexical_chunk_index_is_uuid_keyed_and_current_visible(self):
        self.seed_paper(
            PAPER_A,
            filename="visible.pdf",
            chunks=((1, "Current Visible Body", [1.0, 0.0]),),
        )
        self.seed_paper(
            PAPER_B,
            filename="hidden.pdf",
            lifecycle_state="deleting",
            chunks=((1, "Hidden Body", [1.0, 0.0]),),
        )
        self.seed_paper(
            PAPER_C,
            filename="revision.pdf",
            current_revision=2,
            revisions=(1, 2),
            chunks=(
                (1, "Obsolete Body", [1.0, 0.0]),
                (2, "New Body", [0.0, 1.0]),
            ),
        )

        with self.configured():
            fulltext = search_module._fulltext_index()

        self.assertEqual(
            fulltext,
            {
                PAPER_A: (1, "current visible body"),
                PAPER_C: (2, "new body"),
            },
        )


class CompletionAuthorityTest(RevisionVisibilityTest):
    def lifecycle(self):
        return PublishingLifecycle(
            session_factory=self.session_factory,
            storage=self.storage,
            indexer=mock.Mock(),
            clock=lambda: __import__("datetime").datetime(2026, 7, 21, 9, 30),
            monotonic_clock=lambda: 1.0,
            uuid_factory=lambda: "99999999-9999-4999-8999-999999999999",
            jitter=lambda: 0.0,
        )

    def running_job(self, paper_id, revision=1):
        now = __import__("datetime").datetime(2026, 7, 21, 9, 30)
        job_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        token = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        with self.session_factory() as session:
            session.add(
                PublishingJobModel(
                    id=job_id,
                    kind="index_revision",
                    paper_id=paper_id,
                    revision_number=revision,
                    dedupe_key=f"index:{paper_id}:{revision}",
                    state="running",
                    attempts=1,
                    available_at=now,
                    lease_token=token,
                    lease_expires_at=now + timedelta(minutes=30),
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()
        return JobLease(
            job_id=job_id,
            paper_id=paper_id,
            revision=revision,
            kind="index_revision",
            attempts=1,
            lease_token=token,
            lease_expires_at=now + timedelta(minutes=30),
            created_at=now,
            previous_updated_at=now,
        )

    def test_preparation_does_not_mutate_chunks_jobs_or_stamp(self):
        self.seed_paper(PAPER_A, filename="paper.pdf")
        self.running_job(PAPER_A)
        with self.session_factory() as session:
            before = (
                session.query(PaperChunkModel).count(),
                session.query(PublishingJobModel).count(),
                session.get(RagIndexMetaModel, "chunks_version").value,
            )

        prepared = StrictRagAdapter(
            embedding_available=lambda: True,
            extract_text=lambda _pdf, **_kwargs: "cold body",
            chunker=rag_index.chunk_text,
            embed_texts=lambda chunks, **_kwargs: [[1.0, 0.0] for _ in chunks],
            vision_available=lambda: False,
            transcribe=lambda *_args, **_kwargs: "",
            monotonic=lambda: 1.0,
        ).prepare(
            paper_id=PAPER_A,
            revision_number=1,
            pdf_bytes=b"immutable bytes",
            language="en",
            deadline=10.0,
        )

        self.assertTrue(prepared.chunks)
        with self.session_factory() as session:
            after = (
                session.query(PaperChunkModel).count(),
                session.query(PublishingJobModel).count(),
                session.get(RagIndexMetaModel, "chunks_version").value,
            )
        self.assertEqual(after, before)

    def test_successful_lifecycle_completion_writes_once_and_bumps_once(self):
        self.seed_paper(PAPER_A, filename="paper.pdf")
        lease = self.running_job(PAPER_A)
        with self.session_factory() as session:
            before = session.get(RagIndexMetaModel, "chunks_version").value

        wrote = self.lifecycle()._complete_index(
            lease,
            PreparedRevisionIndex(
                paper_id=PAPER_A,
                revision=1,
                chunks=(PreparedChunk(0, "indexed", (1.0, 0.0), "en"),),
            ),
        )

        self.assertTrue(wrote)
        with self.session_factory() as session:
            self.assertEqual(
                session.get(RagIndexMetaModel, "chunks_version").value,
                before + 1,
            )
            self.assertEqual(session.query(PaperChunkModel).one().paper_id, PAPER_A)
            self.assertIsNone(session.query(PaperChunkModel).one().filename)
            self.assertEqual(session.query(PublishingJobModel).count(), 0)

    def test_obsolete_prepared_revision_cannot_write_after_final_recheck(self):
        self.seed_paper(
            PAPER_A,
            filename="paper.pdf",
            revisions=(1, 2),
        )
        lease = self.running_job(PAPER_A)
        self.set_paper_state(PAPER_A, current_revision=2, bump=False)

        wrote = self.lifecycle()._complete_index(
            lease,
            PreparedRevisionIndex(
                paper_id=PAPER_A,
                revision=1,
                chunks=(PreparedChunk(0, "must not write", (1.0, 0.0), "en"),),
            ),
        )

        self.assertFalse(wrote)
        with self.session_factory() as session:
            self.assertEqual(session.query(PaperChunkModel).count(), 0)


class RebuildToolContractTest(unittest.TestCase):
    def test_build_tool_enqueues_through_lifecycle_only(self):
        source = (Path(__file__).parents[1] / "tools" / "build_embeddings.py").read_text()
        self.assertNotIn("import app", source)
        self.assertNotIn("rag_index.build_index", source)
        self.assertNotIn("glob(", source)
        self.assertIn("ensure_index_job", source)
        self.assertIn("recover_job", source)

    def test_build_tool_ensures_each_target_once_and_recovers_returned_job_once(self):
        from tools import build_embeddings

        lifecycle = mock.Mock()
        lifecycle.ensure_index_job.side_effect = (
            types.SimpleNamespace(job_id="job-a"),
            types.SimpleNamespace(job_id=None),
        )
        targets = [
            (PAPER_A, 1, "a.pdf"),
            (PAPER_B, 2, "b.pdf"),
        ]
        with mock.patch.object(
            build_embeddings.llm_client, "embedding_enabled", return_value=True
        ), mock.patch.object(build_embeddings, "init_db"), mock.patch.object(
            build_embeddings, "build_publishing_lifecycle", return_value=lifecycle
        ), mock.patch.object(
            build_embeddings, "_targets", return_value=targets
        ) as target_query, mock.patch.object(
            build_embeddings.sys, "argv", ["build_embeddings.py"]
        ):
            result = build_embeddings.main()

        self.assertEqual(result, 0)
        target_query.assert_called_once_with(rebuild=False)
        self.assertEqual(
            lifecycle.ensure_index_job.call_args_list,
            [mock.call(PAPER_A, 1), mock.call(PAPER_B, 2)],
        )
        lifecycle.recover_job.assert_called_once_with("job-a")

    def test_vector_migration_tool_does_not_import_application(self):
        source = (
            Path(__file__).parents[1] / "tools" / "migrate_chunk_vectors.py"
        ).read_text()
        self.assertNotIn("import app", source)
        for direct_write in (
            "UPDATE papers_chunks",
            "INSERT INTO papers_chunks",
            "DELETE FROM papers_chunks",
            "ALTER TABLE papers_chunks",
        ):
            self.assertNotIn(direct_write, source)


if __name__ == "__main__":
    unittest.main()
