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


if __name__ == "__main__":
    unittest.main()
