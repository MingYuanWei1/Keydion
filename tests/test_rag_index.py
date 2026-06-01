# tests/test_rag_index.py
import unittest

import rag_index


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


import json
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


class InMemoryStore:
    """Stand-in for the DB layer rag_index talks to via configure()."""
    def __init__(self):
        self.rows = []  # dicts: filename, chunk_index, content, embedding(list), lang

    def replace_chunks(self, filename, rows):
        self.rows = [r for r in self.rows if r["filename"] != filename] + rows

    def all_chunks(self):
        return list(self.rows)

    def delete_chunks(self, filename):
        self.rows = [r for r in self.rows if r["filename"] != filename]


class RetrieveBehaviour(unittest.TestCase):
    def setUp(self):
        self.store = InMemoryStore()
        self.papers = {
            "cold.pdf": {"filename": "cold.pdf", "title": "Cryoprotection in alpine flora",
                         "author_name": "Lee", "language": "en", "text": "cold arctic cryo plants survive"},
            "fr.pdf": {"filename": "fr.pdf", "title": "The French Revolution",
                       "author_name": "Marin", "language": "en", "text": "revolution history france"},
        }
        rag_index.configure(
            build_embed_client=lambda: FakeClient(),
            embed_model=lambda: "fake-embed",
            iter_papers=lambda: list(self.papers.values()),
            paper_text=lambda fn: self.papers[fn]["text"],
            store_replace=self.store.replace_chunks,
            store_all=self.store.all_chunks,
            store_delete=self.store.delete_chunks,
            paper_meta=lambda fn: self.papers.get(fn, {}),
        )
        rag_index.invalidate_cache()

    def test_build_index_creates_chunks(self):
        stats = rag_index.build_index()
        self.assertEqual(stats["papers"], 2)
        self.assertEqual(len(self.store.all_chunks()), 2)

    def test_retrieve_ranks_relevant_paper_first(self):
        rag_index.build_index()
        rag_index.invalidate_cache()
        hits = rag_index.retrieve("how do plants handle the cold?", k=1)
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["filename"], "cold.pdf")
        self.assertIn("title", hits[0])

    def test_retrieve_filters_below_threshold(self):
        rag_index.build_index()
        rag_index.invalidate_cache()
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
        self.papers["neutral.pdf"] = {
            "filename": "neutral.pdf", "title": "Neutral Paper",
            "author_name": "Smith", "language": "en", "text": "neutral content",
        }
        self.store.rows = [
            {"filename": "cold.pdf",    "chunk_index": 0, "content": "chunk A",
             "embedding": [1.0, 0.0], "lang": "en"},
            {"filename": "fr.pdf",      "chunk_index": 0, "content": "chunk B",
             "embedding": [0.0, 1.0], "lang": "en"},
            {"filename": "neutral.pdf", "chunk_index": 0, "content": "chunk C",
             "embedding": [0.5, 0.5], "lang": "en"},
        ]
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


class ResumeBehaviour(unittest.TestCase):
    def setUp(self):
        self.store = InMemoryStore()
        self.papers = {
            "a.pdf": {"filename": "a.pdf", "title": "A", "author_name": "x",
                      "language": "en", "text": "alpha content here"},
            "b.pdf": {"filename": "b.pdf", "title": "B", "author_name": "y",
                      "language": "en", "text": "beta content here"},
        }
        # a.pdf is already indexed (has a stored chunk).
        self.store.rows = [{"filename": "a.pdf", "chunk_index": 0,
                            "content": "alpha content here", "embedding": [0.5, 0.5],
                            "lang": "en"}]
        rag_index.configure(
            build_embed_client=lambda: FakeClient(),
            embed_model=lambda: "fake-embed",
            iter_papers=lambda: list(self.papers.values()),
            paper_text=lambda fn: self.papers[fn]["text"],
            store_replace=self.store.replace_chunks,
            store_all=self.store.all_chunks,
            store_delete=self.store.delete_chunks,
            paper_meta=lambda fn: self.papers.get(fn, {}),
            indexed_filenames=lambda: {r["filename"] for r in self.store.rows},
        )
        rag_index.invalidate_cache()

    def test_skip_existing_resumes(self):
        stats = rag_index.build_index(skip_existing=True)
        self.assertEqual(stats["skipped"], 1)       # a.pdf skipped
        self.assertEqual(stats["papers"], 1)        # only b.pdf indexed
        filenames = {r["filename"] for r in self.store.all_chunks()}
        self.assertIn("a.pdf", filenames)           # untouched
        self.assertIn("b.pdf", filenames)           # newly added

    def test_no_skip_reprocesses_all(self):
        stats = rag_index.build_index(skip_existing=False)
        self.assertEqual(stats.get("skipped", 0), 0)
        self.assertEqual(stats["papers"], 2)

    def test_progress_is_logged(self):
        with self.assertLogs("rag_index", level="INFO") as cm:
            rag_index.build_index()
        joined = "\n".join(cm.output)
        self.assertIn("[1/2]", joined)
        self.assertIn("[2/2]", joined)


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
