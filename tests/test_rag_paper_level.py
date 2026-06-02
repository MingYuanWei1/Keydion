# tests/test_rag_paper_level.py
import unittest
from unittest import mock

import rag_index


class FakeEmbeddings:
    """Maps query text -> deterministic 2-D vector so cosine is predictable."""
    def create(self, model, input):
        vecs = []
        for t in input:
            t = t.lower()
            if "cold" in t or "arctic" in t:
                vecs.append([1.0, 0.0])
            elif "revolution" in t or "history" in t:
                vecs.append([0.0, 1.0])
            else:
                vecs.append([0.5, 0.5])
        return mock.Mock(data=[mock.Mock(embedding=v) for v in vecs])


class FakeClient:
    def __init__(self):
        self.embeddings = FakeEmbeddings()


class PaperLevelBase(unittest.TestCase):
    def setUp(self):
        self._saved = dict(rag_index._DEPS)
        self.rows = []
        rag_index.configure(
            build_embed_client=lambda: FakeClient(),
            embed_model=lambda: "fake-embed",
            store_all=lambda: list(self.rows),
            paper_meta=lambda fn: {"title": fn, "author_name": ""},
        )
        rag_index._QVEC_CACHE.clear()
        rag_index.invalidate_cache()

    def tearDown(self):
        rag_index._DEPS.clear()
        rag_index._DEPS.update(self._saved)
        rag_index._QVEC_CACHE.clear()
        rag_index.invalidate_cache()


class PaperVectors(PaperLevelBase):
    def test_constants_exist(self):
        self.assertEqual(rag_index.PAPER_SEARCH_MIN_SIM, 0.25)
        self.assertEqual(rag_index.RELATED_MIN_SIM, 0.30)

    def test_pools_chunks_per_paper(self):
        self.rows = [
            {"filename": "cold.pdf", "chunk_index": 0, "content": "a", "embedding": [1.0, 0.0]},
            {"filename": "cold.pdf", "chunk_index": 1, "content": "b", "embedding": [0.0, 0.0]},
            {"filename": "fr.pdf",   "chunk_index": 0, "content": "c", "embedding": [0.0, 1.0]},
        ]
        rag_index.invalidate_cache()
        pv = rag_index.paper_vectors()
        self.assertEqual(set(pv), {"cold.pdf", "fr.pdf"})
        self.assertEqual(pv["cold.pdf"], [0.5, 0.0])   # mean of [1,0] and [0,0]
        self.assertEqual(pv["fr.pdf"], [0.0, 1.0])

    def test_paper_with_no_vectors_is_absent(self):
        self.rows = [
            {"filename": "empty.pdf", "chunk_index": 0, "content": "", "embedding": []},
            {"filename": "ok.pdf",    "chunk_index": 0, "content": "x", "embedding": [1.0, 0.0]},
        ]
        rag_index.invalidate_cache()
        pv = rag_index.paper_vectors()
        self.assertNotIn("empty.pdf", pv)
        self.assertIn("ok.pdf", pv)

    def test_invalidate_cache_clears_paper_vectors(self):
        self.rows = [{"filename": "a.pdf", "chunk_index": 0, "content": "x", "embedding": [1.0, 0.0]}]
        rag_index.invalidate_cache()
        rag_index.paper_vectors()
        self.assertIsNotNone(rag_index._PAPER_VECS)
        rag_index.invalidate_cache()
        self.assertIsNone(rag_index._PAPER_VECS)


if __name__ == "__main__":
    unittest.main()
