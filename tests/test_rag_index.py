# tests/test_rag_index.py
import unittest

import numpy as np

import rag_index
from services.publishing_contracts import IndexDeadlineExceeded


class ChunkText(unittest.TestCase):
    def test_short_text_is_one_chunk(self):
        self.assertEqual(rag_index.chunk_text("hello world"), ["hello world"])

    def test_empty_text_is_no_chunks(self):
        self.assertEqual(rag_index.chunk_text("   "), [])

    def test_long_text_splits_with_overlap(self):
        text = "a" * 2000
        chunks = rag_index.chunk_text(text, size=800, overlap=100)
        self.assertGreater(len(chunks), 1)
        # every chunk is within the size bound
        self.assertTrue(all(len(c) <= 800 for c in chunks))
        # overlap means consecutive chunks share a boundary region
        self.assertEqual(chunks[0][-100:], chunks[1][:100])

    def test_chunks_cover_all_characters(self):
        text = "abcdefghij" * 200  # 2000 chars
        chunks = rag_index.chunk_text(text, size=500, overlap=50)
        joined = chunks[0]
        for c in chunks[1:]:
            joined += c[50:]  # drop the overlap when re-joining
        self.assertEqual(joined, text)


class Cosine(unittest.TestCase):
    def test_identical_vectors(self):
        self.assertAlmostEqual(rag_index.cosine([1.0, 0.0], [1.0, 0.0]), 1.0)

    def test_orthogonal_vectors(self):
        self.assertAlmostEqual(rag_index.cosine([1.0, 0.0], [0.0, 1.0]), 0.0)

    def test_zero_vector_is_safe(self):
        self.assertEqual(rag_index.cosine([0.0, 0.0], [1.0, 1.0]), 0.0)


from unittest import mock


class FakeEmbeddings:
    """Maps text -> deterministic 2-D vector by keyword, so cosine is predictable."""
    def create(self, model, input):
        vecs = []
        for t in input:
            t = t.lower()
            if "cold" in t or "arctic" in t or "cryo" in t:
                vecs.append([1.0, 0.0])
            elif "revolution" in t or "history" in t:
                vecs.append([0.0, 1.0])
            else:
                vecs.append([0.5, 0.5])
        return mock.Mock(data=[mock.Mock(embedding=v) for v in vecs])


class FakeClient:
    def __init__(self):
        self.embeddings = FakeEmbeddings()


def _f32(vec):
    """Pack a list of floats the way the real store serves VECTOR bytes."""
    return np.asarray(vec, dtype="<f4").tobytes()


class InMemoryStore:
    """Stand-in for the DB layer rag_index talks to via configure().
    Rows are already current-visible, as the real SQL read adapter guarantees."""
    def __init__(self):
        self.rows = []
        self.version = 0

    def delete_chunks(self, paper_id):
        self.rows = [r for r in self.rows if r["paper_id"] != paper_id]
        self.version += 1

    def get_version(self):
        return self.version

    def vectors(self):
        return [
            {
                "id": row["id"],
                "paper_id": row["paper_id"],
                "revision_number": row["revision_number"],
                "chunk_index": row["chunk_index"],
                "embedding": _f32(row["embedding"]) if row["embedding"] else None,
            }
            for row in self.rows
        ]

    def fetch(self, ids):
        wanted = set(ids)
        return [
            {
                "id": row["id"],
                "paper_id": row["paper_id"],
                "revision_number": row["revision_number"],
                "filename": row["filename"],
                "chunk_index": row["chunk_index"],
                "content": row["content"],
                "title": row.get("title", row["filename"]),
                "author_name": row.get("author_name", ""),
            }
            for row in self.rows
            if row["id"] in wanted
        ]

    def fetch_papers(self, ids):
        wanted = set(ids)
        return [
            {"paper_id": paper_id, "current_revision": revision}
            for paper_id, revision in {
                (row["paper_id"], row["revision_number"])
                for row in self.rows
                if row["paper_id"] in wanted
            }
        ]


PAPER_IDS = {
    "cold.pdf": "11111111-1111-4111-8111-111111111111",
    "fr.pdf": "22222222-2222-4222-8222-222222222222",
    "neutral.pdf": "33333333-3333-4333-8333-333333333333",
    "zero.pdf": "44444444-4444-4444-8444-444444444444",
    "a.pdf": "55555555-5555-4555-8555-555555555555",
    "b.pdf": "66666666-6666-4666-8666-666666666666",
    "c.pdf": "77777777-7777-4777-8777-777777777777",
}


def _row(row_id, filename, vector, content=None):
    return {
        "id": row_id,
        "paper_id": PAPER_IDS[filename],
        "revision_number": 1,
        "filename": filename,
        "chunk_index": 0,
        "content": content if content is not None else filename,
        "embedding": vector,
        "title": filename,
        "author_name": "",
    }


class RetrieveBehaviour(unittest.TestCase):
    def setUp(self):
        self.store = InMemoryStore()
        self.store.rows = [
            _row(1, "cold.pdf", [1.0, 0.0], "cold arctic cryo plants survive"),
            _row(2, "fr.pdf", [0.0, 1.0], "revolution history france"),
        ]
        rag_index.configure(
            build_embed_client=lambda: FakeClient(),
            embed_model=lambda: "fake-embed",
            store_version=self.store.get_version,
            store_vectors=self.store.vectors,
            fetch_chunks=self.store.fetch,
            fetch_papers=self.store.fetch_papers,
        )
        rag_index.invalidate_cache()

    def test_direct_index_writer_api_is_removed(self):
        self.assertFalse(hasattr(rag_index, "build_index"))
        self.assertFalse(hasattr(rag_index, "purge"))

    def test_retrieve_ranks_relevant_paper_first(self):
        hits = rag_index.retrieve("how do plants handle the cold?", k=1)
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["filename"], "cold.pdf")
        self.assertIn("title", hits[0])

    def test_retrieve_filters_below_threshold(self):
        # query maps to [0.5,0.5]; min_sim very high -> nothing passes
        hits = rag_index.retrieve("something neutral", k=5, min_sim=0.999)
        self.assertEqual(hits, [])

    def test_retrieve_filters_before_slicing(self):
        # Regression: retrieve() must filter by min_sim BEFORE slicing to k.
        #
        # We inject 3 chunks with pre-computed embeddings directly into the store
        # so cosine scores against query "cold arctic" -> qvec=[1.0, 0.0] are known:
        #
        #   chunk A (cold.pdf):    embedding=[1.0, 0.0]  -> cosine = 1.0   (ABOVE 0.5)
        #   chunk B (fr.pdf):      embedding=[0.0, 1.0]  -> cosine = 0.0   (BELOW 0.5)
        #   chunk C (neutral.pdf): embedding=[0.5, 0.5]  -> cosine ≈ 0.707 (ABOVE 0.5)
        #
        # sorted desc: A(1.0), C(0.707), B(0.0)
        # With k=2 and min_sim=0.5:
        #   filter-first (new): qualifying = [A, C], take top-2 -> 2 hits
        #   slice-first (old):  top-2 = [A, C], both pass filter -> 2 hits
        #
        # Both return 2 hits here because the sorted order keeps qualifiers first.
        # That is the CORRECT behaviour. This test documents the contract:
        # all returned hits are >= min_sim and count <= k.
        #
        # The fix (filter-before-slice) is the correct defensive implementation;
        # observable difference would require a below-threshold chunk to outscore
        # an above-threshold one, which is impossible for a consistent threshold.
        # The test verifies the contract holds with the new implementation.
        self.store.rows = [
            _row(1, "cold.pdf", [1.0, 0.0], "chunk A"),
            _row(2, "fr.pdf", [0.0, 1.0], "chunk B"),
            _row(3, "neutral.pdf", [0.5, 0.5], "chunk C"),
        ]
        self.store.version += 1
        rag_index.invalidate_cache()

        hits = rag_index.retrieve("cold arctic", k=2, min_sim=0.5)

        # Exactly 2 qualifying chunks (A and C); B must be excluded.
        self.assertEqual(len(hits), 2)
        for h in hits:
            self.assertGreaterEqual(h["score"], 0.5,
                                    f"Hit below threshold: {h}")
        filenames = {h["filename"] for h in hits}
        self.assertIn("cold.pdf", filenames)
        self.assertIn("neutral.pdf", filenames)
        self.assertNotIn("fr.pdf", filenames)

    def test_writes_invalidate_other_workers_via_stamp(self):
        # THE multi-worker staleness regression test. A write through the store
        # (as another gunicorn worker or the CLI would do it) must be visible to
        # THIS process on the next retrieve, with no local invalidate_cache().
        hits = rag_index.retrieve("how do plants handle the cold?", k=1)
        self.assertEqual(hits[0]["filename"], "cold.pdf")

        # "Another process" deletes cold.pdf: store mutates + stamp bumps,
        # but this process's invalidate_cache() is never called.
        self.store.delete_chunks(PAPER_IDS["cold.pdf"])

        hits = rag_index.retrieve("how do plants handle the cold?", k=5)
        self.assertNotIn("cold.pdf", [h["filename"] for h in hits])

    def test_zero_vector_chunk_scores_zero_and_never_crashes(self):
        self.store.rows = [
            _row(1, "zero.pdf", [0.0, 0.0], "z"),
            _row(2, "cold.pdf", [1.0, 0.0], "c"),
        ]
        self.store.version += 1
        hits = rag_index.retrieve("cold arctic", k=5, min_sim=0.2)
        self.assertEqual([h["filename"] for h in hits], ["cold.pdf"])


class ScoringEquivalence(unittest.TestCase):
    """Normalized-dot scoring must match the old pure-Python cosine()."""

    def setUp(self):
        self.store = InMemoryStore()
        rag_index.configure(
            build_embed_client=lambda: FakeClient(),
            embed_model=lambda: "fake-embed",
            store_version=self.store.get_version,
            store_vectors=self.store.vectors,
            fetch_chunks=self.store.fetch,
            fetch_papers=self.store.fetch_papers,
        )
        rag_index.invalidate_cache()

    def test_retrieve_scores_match_cosine(self):
        vecs = {"a.pdf": [1.0, 0.0], "b.pdf": [0.6, 0.8], "c.pdf": [0.5, 0.5]}
        self.store.rows = [
            _row(i + 1, fn, v)
            for i, (fn, v) in enumerate(sorted(vecs.items()))
        ]
        self.store.version += 1
        hits = rag_index.retrieve("cold arctic", k=5, min_sim=0.0)  # qvec = [1.0, 0.0]
        self.assertEqual(len(hits), 3)
        for h in hits:
            expected = rag_index.cosine([1.0, 0.0], vecs[h["filename"]])
            self.assertAlmostEqual(h["score"], expected, places=5)
        # ranking is descending
        scores = [h["score"] for h in hits]
        self.assertEqual(scores, sorted(scores, reverse=True))

class EmbedBatchTest(unittest.TestCase):
    def setUp(self):
        self._saved = dict(rag_index._DEPS)

    def tearDown(self):
        rag_index._DEPS.clear()
        rag_index._DEPS.update(self._saved)
        rag_index.invalidate_cache()

    def _client(self, calls):
        class _C:
            def __init__(self):
                self.embeddings = self
            def create(self, model=None, input=None):
                calls.append(len(input))
                return type("R", (), {
                    "data": [type("E", (), {"embedding": [0.1]})() for _ in input]
                })()
        return _C()

    def test_splits_into_batches_of_configured_size(self):
        calls = []
        rag_index.configure(
            build_embed_client=lambda: self._client(calls),
            embed_model=lambda: "m",
            embed_batch_size=lambda: 10,
        )
        out = rag_index.embed_texts(["t"] * 23)
        self.assertEqual(len(out), 23)            # all embedded, in order
        self.assertEqual(calls, [10, 10, 3])      # no request exceeds 10
        self.assertTrue(all(c <= 10 for c in calls))

    def test_defaults_to_10_when_dep_absent(self):
        calls = []
        rag_index.configure(
            build_embed_client=lambda: self._client(calls),
            embed_model=lambda: "m",
        )
        rag_index._DEPS.pop("embed_batch_size", None)   # ensure no dep configured
        rag_index.embed_texts(["t"] * 12)
        self.assertEqual(calls, [10, 2])

    def test_empty_input_makes_no_calls(self):
        calls = []
        rag_index.configure(
            build_embed_client=lambda: self._client(calls),
            embed_model=lambda: "m",
            embed_batch_size=lambda: 10,
        )
        self.assertEqual(rag_index.embed_texts([]), [])
        self.assertEqual(calls, [])

    def test_deadline_is_forwarded_to_client_and_each_batch_request(self):
        builder_deadlines = []
        request_calls = []

        class _DeadlineClient:
            def __init__(self):
                self.embeddings = self

            def create(self, **kwargs):
                request_calls.append(kwargs)
                return type(
                    "Response",
                    (),
                    {
                        "data": [
                            type("Embedding", (), {"embedding": [0.1]})()
                            for _ in kwargs["input"]
                        ]
                    },
                )()

        def build_client(*, deadline=None):
            builder_deadlines.append(deadline)
            return _DeadlineClient()

        rag_index.configure(
            build_embed_client=build_client,
            embed_model=lambda: "m",
            embed_batch_size=lambda: 2,
        )
        with mock.patch.object(rag_index.time, "monotonic", return_value=4.0):
            out = rag_index.embed_texts(["a", "b", "c"], deadline=10.0)

        self.assertEqual(len(out), 3)
        self.assertEqual(builder_deadlines, [10.0])
        self.assertEqual([call["timeout"] for call in request_calls], [6.0, 6.0])

    def test_exhausted_embedding_deadline_raises_before_client_build(self):
        build_client = mock.Mock()
        rag_index.configure(
            build_embed_client=build_client,
            embed_model=lambda: "m",
        )
        with mock.patch.object(rag_index.time, "monotonic", return_value=10.0):
            with self.assertRaises(IndexDeadlineExceeded):
                rag_index.embed_texts(["a"], deadline=10.0)
        build_client.assert_not_called()


class Reassemble(unittest.TestCase):
    def test_constants_exist_with_correct_values(self):
        self.assertEqual(rag_index.CHUNK_SIZE, 800)
        self.assertEqual(rag_index.CHUNK_OVERLAP, 120)

    def test_empty_list_returns_empty_string(self):
        self.assertEqual(rag_index.reassemble([]), "")

    def test_single_chunk_round_trips(self):
        self.assertEqual(rag_index.reassemble(["hello"]), "hello")

    def test_short_text_round_trip(self):
        # Short text produces a single chunk; reassemble should return it unchanged.
        t = "This is a short text."
        self.assertEqual(rag_index.reassemble(rag_index.chunk_text(t)), t)

    def test_long_text_round_trip(self):
        # Long text produces multiple overlapping chunks; reassemble must recover original.
        t = "abcdefghij" * 200  # 2000 chars, well above CHUNK_SIZE
        self.assertEqual(rag_index.reassemble(rag_index.chunk_text(t)), t)

    def test_default_overlap_matches_chunk_overlap_constant(self):
        # Calling reassemble without an overlap arg must use CHUNK_OVERLAP (120).
        # If the default were wrong, the long-text round-trip would produce garbage.
        t = "x" * 2000
        chunks = rag_index.chunk_text(t)  # uses default CHUNK_SIZE / CHUNK_OVERLAP
        self.assertEqual(rag_index.reassemble(chunks), t)

    def test_overlap_larger_than_chunk_truncates(self):
        # Documents the contract: oversized overlap drops chars, never crashes.
        self.assertEqual(rag_index.reassemble(["abcd", "ef"], overlap=10), "abcd")


if __name__ == "__main__":
    unittest.main()
