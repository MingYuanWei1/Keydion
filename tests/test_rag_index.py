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


if __name__ == "__main__":
    unittest.main()
