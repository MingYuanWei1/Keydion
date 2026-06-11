# tests/test_hybrid_search.py
import os
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module
import services.search as search_module


def _rec(filename, **kw):
    r = {"filename": filename, "title": "", "author_name": "", "keywords": "",
         "ib_ee_data": "", "cp_data": "", "published_at": ""}
    r.update(kw)
    return r


class QueryInMetadata(unittest.TestCase):
    def test_matches_title(self):
        self.assertTrue(app_module._query_in_metadata(_rec("a.pdf", title="Quantum Physics"), "quantum"))

    def test_matches_author(self):
        self.assertTrue(app_module._query_in_metadata(_rec("a.pdf", author_name="Jane Smith"), "smith"))

    def test_matches_keywords(self):
        self.assertTrue(app_module._query_in_metadata(_rec("a.pdf", keywords="climate, ocean"), "ocean"))

    def test_matches_ee_subjects(self):
        rec = _rec("a.pdf", ib_ee_data='{"core_subject": "Chemistry", "interdisciplinary_subject": ""}')
        self.assertTrue(app_module._query_in_metadata(rec, "chemistry"))

    def test_matches_cp_context(self):
        rec = _rec("a.pdf", cp_data='{"global_context": "Globalization", "action_types": ["advocacy"]}')
        self.assertTrue(app_module._query_in_metadata(rec, "advocacy"))

    def test_no_match(self):
        self.assertFalse(app_module._query_in_metadata(_rec("a.pdf", title="Biology"), "physics"))


class OrderHybridFilenames(unittest.TestCase):
    def test_three_tier_order(self):
        lexical = [
            _rec("m1.pdf", title="climate change"),   # metadata match for "climate"
            _rec("f1.pdf", title="unrelated"),          # lexical full-text only
        ]
        semantic = [("s1.pdf", 0.9), ("m1.pdf", 0.8)]
        ordered = app_module._order_hybrid_filenames(lexical, semantic, "climate")
        # tier1 metadata: m1 ; tier2 semantic-only: s1 ; tier3 full-text-only: f1
        self.assertEqual(ordered, ["m1.pdf", "s1.pdf", "f1.pdf"])

    def test_metadata_tier_sorted_by_semantic_score(self):
        lexical = [_rec("m_lo.pdf", title="climate a"), _rec("m_hi.pdf", title="climate b")]
        semantic = [("m_hi.pdf", 0.9), ("m_lo.pdf", 0.2)]
        ordered = app_module._order_hybrid_filenames(lexical, semantic, "climate")
        self.assertEqual(ordered, ["m_hi.pdf", "m_lo.pdf"])

    def test_no_semantic_keeps_lexical_order(self):
        lexical = [_rec("a.pdf", title="x"), _rec("b.pdf", title="y")]
        ordered = app_module._order_hybrid_filenames(lexical, [], "zzz")
        self.assertEqual(ordered, ["a.pdf", "b.pdf"])   # all tier3, original order


class HybridSearchRecords(unittest.TestCase):
    def test_falls_back_to_lexical_when_llm_disabled(self):
        lexical = [_rec("a.pdf")]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "llm_enabled", return_value=False):
            out = app_module._hybrid_search_records("anything")
        self.assertEqual(out, lexical)

    def test_short_query_skips_semantic(self):
        lexical = [_rec("a.pdf")]
        sem = mock.Mock()
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "llm_enabled", return_value=True), \
             mock.patch.object(app_module.rag_index, "search_papers_semantic", sem):
            out = app_module._hybrid_search_records("a")   # len 1 < MIN_SEMANTIC_QUERY_LEN
        sem.assert_not_called()
        self.assertEqual(out, lexical)

    def test_falls_back_to_lexical_on_semantic_error(self):
        lexical = [_rec("a.pdf")]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "llm_enabled", return_value=True), \
             mock.patch.object(app_module.rag_index, "search_papers_semantic",
                               side_effect=RuntimeError("boom")):
            out = app_module._hybrid_search_records("climate")
        self.assertEqual(out, lexical)

    def test_falls_back_when_no_semantic_hits(self):
        lexical = [_rec("a.pdf", title="x")]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "llm_enabled", return_value=True), \
             mock.patch.object(app_module.rag_index, "search_papers_semantic", return_value=[]):
            out = app_module._hybrid_search_records("climate")
        self.assertEqual(out, lexical)

    def test_merges_semantic_only_paper_after_metadata_match(self):
        lexical = [_rec("m1.pdf", title="climate")]            # metadata match
        index_rows = [_rec("m1.pdf", title="climate"), _rec("s1.pdf", title="semantic only")]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "llm_enabled", return_value=True), \
             mock.patch.object(app_module.rag_index, "search_papers_semantic",
                               return_value=[("s1.pdf", 0.9), ("m1.pdf", 0.8)]), \
             mock.patch.object(search_module, "load_paper_metadata", return_value=index_rows), \
             mock.patch.object(search_module, "build_paper_record",
                               side_effect=lambda fn, idx=None: _rec(fn, title="semantic only")), \
             mock.patch.object(search_module, "PAPERS_DIR", mock.MagicMock()):
            out = app_module._hybrid_search_records("climate")
        self.assertEqual([r["filename"] for r in out], ["m1.pdf", "s1.pdf"])


if __name__ == "__main__":
    unittest.main()
