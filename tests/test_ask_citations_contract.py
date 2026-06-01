# tests/test_ask_citations_contract.py
"""Contract: the Ask sources panel reflects papers the assistant actually used.

Two defects this guards against:
  A. retrieval is chunk-level, so one paper can occupy several top hits and get
     listed (and cited) as multiple sources -> dedupe by filename.
  B. every retrieved source was shown regardless of use -> show only the source
     numbers the answer references (half-width [1] and full-width 【1】).
"""
import os
import unittest

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import app as app_module


class DedupeHitsByPaper(unittest.TestCase):
    def test_collapses_multiple_chunks_of_same_paper(self):
        hits = [
            {"filename": "a.pdf", "title": "A", "author_name": "x", "content": "chunk1", "score": 0.9},
            {"filename": "a.pdf", "title": "A", "author_name": "x", "content": "chunk2", "score": 0.8},
            {"filename": "b.pdf", "title": "B", "author_name": "y", "content": "chunk3", "score": 0.7},
            {"filename": "a.pdf", "title": "A", "author_name": "x", "content": "chunk4", "score": 0.6},
        ]
        out = app_module._dedupe_hits_by_paper(hits)
        self.assertEqual([h["filename"] for h in out], ["a.pdf", "b.pdf"])

    def test_preserves_best_score_order(self):
        hits = [
            {"filename": "a.pdf", "title": "A", "content": "c1", "score": 0.9},
            {"filename": "b.pdf", "title": "B", "content": "c2", "score": 0.5},
            {"filename": "a.pdf", "title": "A", "content": "c3", "score": 0.4},
        ]
        out = app_module._dedupe_hits_by_paper(hits)
        self.assertEqual([h["filename"] for h in out], ["a.pdf", "b.pdf"])

    def test_merges_chunk_text_for_grounding(self):
        hits = [
            {"filename": "a.pdf", "title": "A", "content": "first", "score": 0.9},
            {"filename": "a.pdf", "title": "A", "content": "second", "score": 0.8},
        ]
        out = app_module._dedupe_hits_by_paper(hits)
        self.assertEqual(len(out), 1)
        self.assertIn("first", out[0]["content"])
        self.assertIn("second", out[0]["content"])

    def test_empty_is_empty(self):
        self.assertEqual(app_module._dedupe_hits_by_paper([]), [])


class CitedNumbers(unittest.TestCase):
    def test_halfwidth_brackets(self):
        self.assertEqual(app_module._cited_numbers("see [1] and also [3]."), {1, 3})

    def test_fullwidth_brackets_for_chinese(self):
        self.assertEqual(app_module._cited_numbers("根据【2】的研究。"), {2})

    def test_grouped_numbers(self):
        self.assertEqual(app_module._cited_numbers("evidence [1, 2] supports"), {1, 2})

    def test_no_citations_is_empty(self):
        self.assertEqual(app_module._cited_numbers("a plain answer with no refs"), set())

    def test_non_numeric_brackets_ignored(self):
        self.assertEqual(app_module._cited_numbers("[see note] not a ref"), set())


class FilterCited(unittest.TestCase):
    def test_keeps_only_referenced_sources(self):
        items = [{"n": 1, "title": "A"}, {"n": 2, "title": "B"}, {"n": 3, "title": "C"}]
        out = app_module._filter_cited(items, "supported by [1] and [3]")
        self.assertEqual([it["n"] for it in out], [1, 3])

    def test_unreferenced_sources_dropped(self):
        items = [{"n": 1, "title": "A"}, {"n": 2, "title": "B"}]
        out = app_module._filter_cited(items, "an answer that cites nothing")
        self.assertEqual(out, [])


if __name__ == "__main__":
    unittest.main()
