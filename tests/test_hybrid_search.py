# tests/test_hybrid_search.py
import os
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module
import services.search as search_module

M1 = "11111111-1111-4111-8111-111111111111"
F1 = "22222222-2222-4222-8222-222222222222"
S1 = "33333333-3333-4333-8333-333333333333"
M2 = "44444444-4444-4444-8444-444444444444"


def _rec(filename, paper_id=M1, **kw):
    r = {"paper_id": paper_id, "current_revision": 1, "filename": filename,
         "title": "", "author_name": "", "keywords": "",
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
            _rec("m1.pdf", M1, title="climate change"),
            _rec("f1.pdf", F1, title="unrelated"),
        ]
        semantic = [(S1, 0.9), (M1, 0.8)]
        ordered = app_module._order_hybrid_filenames(lexical, semantic, "climate")
        self.assertEqual(ordered, [M1, S1, F1])

    def test_metadata_tier_sorted_by_semantic_score(self):
        lexical = [
            _rec("m_lo.pdf", M1, title="climate a"),
            _rec("m_hi.pdf", M2, title="climate b"),
        ]
        semantic = [(M2, 0.9), (M1, 0.2)]
        ordered = app_module._order_hybrid_filenames(lexical, semantic, "climate")
        self.assertEqual(ordered, [M2, M1])

    def test_no_semantic_keeps_lexical_order(self):
        lexical = [_rec("a.pdf", M1, title="x"), _rec("b.pdf", F1, title="y")]
        ordered = app_module._order_hybrid_filenames(lexical, [], "zzz")
        self.assertEqual(ordered, [M1, F1])


class HybridSearchRecords(unittest.TestCase):
    def test_falls_back_to_lexical_when_llm_disabled(self):
        lexical = [_rec("a.pdf")]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "embedding_enabled", return_value=False):
            out = app_module._hybrid_search_records("anything")
        self.assertEqual(out, lexical)

    def test_short_query_skips_semantic(self):
        lexical = [_rec("a.pdf")]
        sem = mock.Mock()
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "embedding_enabled", return_value=True), \
             mock.patch.object(app_module.rag_index, "search_papers_semantic", sem):
            out = app_module._hybrid_search_records("a")   # len 1 < MIN_SEMANTIC_QUERY_LEN
        sem.assert_not_called()
        self.assertEqual(out, lexical)

    def test_falls_back_to_lexical_on_semantic_error(self):
        lexical = [_rec("a.pdf")]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "embedding_enabled", return_value=True), \
             mock.patch.object(app_module.rag_index, "search_papers_semantic",
                               side_effect=RuntimeError("boom")):
            out = app_module._hybrid_search_records("climate")
        self.assertEqual(out, lexical)

    def test_falls_back_when_no_semantic_hits(self):
        lexical = [_rec("a.pdf", title="x")]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "embedding_enabled", return_value=True), \
             mock.patch.object(app_module.rag_index, "search_papers_semantic", return_value=[]):
            out = app_module._hybrid_search_records("climate")
        self.assertEqual(out, lexical)

    def test_merges_semantic_only_paper_after_metadata_match(self):
        lexical = [_rec("m1.pdf", M1, title="climate")]
        visible = [
            _rec("m1.pdf", M1, title="climate"),
            _rec("s1.pdf", S1, title="semantic only"),
        ]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "embedding_enabled", return_value=True), \
             mock.patch.object(app_module.rag_index, "search_papers_semantic",
                               return_value=[(S1, 0.9), (M1, 0.8)]), \
             mock.patch.object(search_module, "_visible_paper_records", return_value=visible):
            out = app_module._hybrid_search_records("climate")
        self.assertEqual([r["filename"] for r in out], ["m1.pdf", "s1.pdf"])

    def test_drops_semantic_only_paper_missing_from_fresh_visible_rows(self):
        lexical = [_rec("m1.pdf", M1, title="climate")]
        with mock.patch.object(search_module, "search_papers", return_value=lexical), \
             mock.patch.object(app_module.llm_client, "embedding_enabled", return_value=True), \
             mock.patch.object(
                 app_module.rag_index,
                 "search_papers_semantic",
                 return_value=[(S1, 0.9), (M1, 0.8)],
             ), \
             mock.patch.object(
                 search_module,
                 "_visible_paper_records",
                 return_value=[_rec("m1.pdf", M1, title="climate")],
             ):
            out = app_module._hybrid_search_records("climate")

        self.assertEqual([record["paper_id"] for record in out], [M1])


if __name__ == "__main__":
    unittest.main()
