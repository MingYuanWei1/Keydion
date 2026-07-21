import unittest
from unittest import mock

import numpy as np

import rag_index


PAPER_A = "11111111-1111-4111-8111-111111111111"
PAPER_B = "22222222-2222-4222-8222-222222222222"
PAPER_C = "33333333-3333-4333-8333-333333333333"
PAPER_EMPTY = "44444444-4444-4444-8444-444444444444"


class FakeEmbeddings:
    """Maps query text to deterministic 2-D vectors."""

    def create(self, model, input):
        vectors = []
        for text in input:
            lowered = text.lower()
            if "cold" in lowered or "arctic" in lowered:
                vectors.append([1.0, 0.0])
            elif "revolution" in lowered or "history" in lowered:
                vectors.append([0.0, 1.0])
            else:
                vectors.append([0.5, 0.5])
        return mock.Mock(data=[mock.Mock(embedding=value) for value in vectors])


class FakeClient:
    def __init__(self):
        self.embeddings = FakeEmbeddings()


def _f32(vector):
    return np.asarray(vector, dtype="<f4").tobytes()


def _row(paper_id, chunk_index, embedding):
    return {
        "paper_id": paper_id,
        "revision_number": 1,
        "chunk_index": chunk_index,
        "embedding": embedding,
    }


class PaperLevelBase(unittest.TestCase):
    def setUp(self):
        self._saved = dict(rag_index._DEPS)
        self.rows = []
        self.version = 0

        def vectors():
            return [
                {
                    "id": index + 1,
                    "paper_id": row["paper_id"],
                    "revision_number": row["revision_number"],
                    "chunk_index": row["chunk_index"],
                    "embedding": (
                        _f32(row["embedding"]) if row["embedding"] else None
                    ),
                }
                for index, row in enumerate(self.rows)
            ]

        def fetch_papers(ids):
            wanted = set(ids)
            return [
                {"paper_id": paper_id, "current_revision": 1}
                for paper_id in {
                    row["paper_id"] for row in self.rows
                    if row["paper_id"] in wanted
                }
            ]

        rag_index.configure(
            build_embed_client=lambda: FakeClient(),
            embed_model=lambda: "fake-embed",
            store_version=lambda: self.version,
            store_vectors=vectors,
            fetch_papers=fetch_papers,
        )
        rag_index._QVEC_CACHE.clear()
        rag_index.invalidate_cache()

    def tearDown(self):
        rag_index._DEPS.clear()
        rag_index._DEPS.update(self._saved)
        rag_index._QVEC_CACHE.clear()
        rag_index.invalidate_cache()


class PaperPooling(PaperLevelBase):
    def test_constants_exist(self):
        self.assertEqual(rag_index.PAPER_SEARCH_MIN_SIM, 0.25)
        self.assertEqual(rag_index.RELATED_MIN_SIM, 0.30)

    def test_pools_chunks_per_paper_uuid(self):
        self.rows = [
            _row(PAPER_A, 0, [1.0, 0.0]),
            _row(PAPER_A, 1, [0.0, 0.0]),
            _row(PAPER_B, 0, [0.5, 0.5]),
        ]
        rag_index.invalidate_cache()
        hits = rag_index.search_papers_semantic("cold arctic", min_sim=0.0)
        self.assertEqual(hits[0][0], PAPER_A)
        self.assertAlmostEqual(hits[0][1], 1.0, places=5)

    def test_paper_with_no_vectors_is_absent(self):
        self.rows = [
            _row(PAPER_EMPTY, 0, []),
            _row(PAPER_A, 0, [1.0, 0.0]),
        ]
        rag_index.invalidate_cache()
        identities = [
            paper_id
            for paper_id, _score in rag_index.search_papers_semantic(
                "cold", min_sim=0.0
            )
        ]
        self.assertNotIn(PAPER_EMPTY, identities)
        self.assertIn(PAPER_A, identities)
        self.assertEqual(rag_index.related_papers(PAPER_EMPTY), [])

    def test_invalidate_cache_clears_snapshot(self):
        self.rows = [_row(PAPER_A, 0, [1.0, 0.0])]
        rag_index.invalidate_cache()
        rag_index.search_papers_semantic("cold")
        self.assertIsNotNone(rag_index._SNAPSHOT)
        rag_index.invalidate_cache()
        self.assertIsNone(rag_index._SNAPSHOT)

    def test_version_bump_refreshes_without_local_invalidation(self):
        self.rows = [_row(PAPER_A, 0, [1.0, 0.0])]
        rag_index.invalidate_cache()
        self.assertEqual(
            [
                paper_id
                for paper_id, _score in rag_index.search_papers_semantic(
                    "cold", min_sim=0.0
                )
            ],
            [PAPER_A],
        )
        self.rows.append(_row(PAPER_B, 0, [0.9, 0.1]))
        self.version += 1
        identities = [
            paper_id
            for paper_id, _score in rag_index.search_papers_semantic(
                "cold", min_sim=0.0
            )
        ]
        self.assertIn(PAPER_B, identities)


class SearchPapersSemantic(PaperLevelBase):
    def setUp(self):
        super().setUp()
        self.rows = [
            _row(PAPER_A, 0, [1.0, 0.0]),
            _row(PAPER_B, 0, [0.0, 1.0]),
            _row(PAPER_C, 0, [0.5, 0.5]),
        ]
        rag_index.invalidate_cache()

    def test_ranks_relevant_paper_first(self):
        hits = rag_index.search_papers_semantic("cold arctic")
        self.assertEqual(hits[0][0], PAPER_A)
        self.assertNotIn(PAPER_B, [paper_id for paper_id, _score in hits])

    def test_threshold_excludes_low_scores(self):
        hits = rag_index.search_papers_semantic("cold", min_sim=0.99)
        self.assertEqual([paper_id for paper_id, _score in hits], [PAPER_A])

    def test_k_caps_results(self):
        self.assertEqual(
            len(rag_index.search_papers_semantic("neutral", min_sim=0.0, k=1)),
            1,
        )

    def test_zero_k_returns_no_results(self):
        self.assertEqual(
            rag_index.search_papers_semantic("neutral", min_sim=0.0, k=0),
            [],
        )

    def test_blank_query_returns_empty(self):
        self.assertEqual(rag_index.search_papers_semantic("   "), [])

    def test_empty_index_returns_empty(self):
        self.rows = []
        rag_index.invalidate_cache()
        self.assertEqual(rag_index.search_papers_semantic("cold"), [])

    def test_query_vector_is_memoized(self):
        calls = []
        original = rag_index.embed_texts

        def counting(texts):
            calls.append(tuple(texts))
            return original(texts)

        with mock.patch.object(rag_index, "embed_texts", counting):
            rag_index.search_papers_semantic("cold arctic")
            rag_index.search_papers_semantic("cold arctic")
        self.assertEqual(len(calls), 1)


class RelatedPapers(PaperLevelBase):
    def setUp(self):
        super().setUp()
        self.rows = [
            _row(PAPER_A, 0, [1.0, 0.0]),
            _row(PAPER_B, 0, [0.9, 0.1]),
            _row(PAPER_C, 0, [0.0, 1.0]),
        ]
        rag_index.invalidate_cache()

    def test_excludes_self_and_ranks_by_similarity(self):
        related = rag_index.related_papers(PAPER_A, k=5, min_sim=0.0)
        identities = [paper_id for paper_id, _score in related]
        self.assertNotIn(PAPER_A, identities)
        self.assertEqual(identities, [PAPER_B, PAPER_C])

    def test_threshold_filters(self):
        related = rag_index.related_papers(PAPER_A, min_sim=0.95)
        self.assertEqual([paper_id for paper_id, _score in related], [PAPER_B])

    def test_unembedded_paper_returns_empty(self):
        self.assertEqual(rag_index.related_papers(PAPER_EMPTY), [])

    def test_zero_k_returns_no_results(self):
        self.assertEqual(
            rag_index.related_papers(PAPER_A, min_sim=0.0, k=0),
            [],
        )


if __name__ == "__main__":
    unittest.main()
