"""Contract: public /search has admission control and never parses PDFs.

Security-review finding [1]: any anonymous query triggered corpus-wide work
(full chunk materialization, per-paper scan, on-demand PDF extraction for
unindexed revisions) plus a paid embedding call per distinct query, with no
rate, length, or work budget before the expensive work.

Enforced here:
1. The /search route rate-limits query work and caps query length BEFORE any
   corpus work.
2. search_papers() never extracts PDF text on the request path — unindexed
   papers match on metadata only until the async indexer catches up.
"""
import os
import sys
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

import services.search as search_module

PAPER_A = "11111111-1111-4111-8111-111111111111"


def _rec(filename, paper_id=PAPER_A, **kw):
    r = {"paper_id": paper_id, "current_revision": 2, "filename": filename,
         "title": "", "author_name": "", "keywords": "",
         "ib_ee_data": "", "cp_data": "", "published_at": ""}
    r.update(kw)
    return r


class SearchRouteAdmission(unittest.TestCase):
    def test_route_rate_limits_query_work(self):
        src = support.source_of("search")
        self.assertIn("consume_rate_limit(", src,
                      "/search must throttle query work before doing it")

    def test_route_caps_query_length(self):
        src = support.source_of("search")
        self.assertIn("MAX_SEARCH_QUERY_CHARS", src,
                      "/search must reject over-long queries before corpus work")

    def test_config_defines_search_budgets(self):
        import config
        self.assertGreater(config.MAX_SEARCH_QUERY_CHARS, 0)
        self.assertGreater(config.SEARCH_RATE_LIMIT, 0)
        self.assertGreater(config.SEARCH_RATE_WINDOW, 0)


class SearchNeverParsesPdfs(unittest.TestCase):
    def test_search_papers_has_no_extraction_sink(self):
        src = support.source_of("search_papers")
        self.assertNotIn("extract_pdf_text", src,
                         "public search must not parse PDFs on the request path")

    def test_unindexed_paper_matches_metadata_only(self):
        record = _rec("a.pdf")
        extract = mock.Mock(side_effect=AssertionError("must not extract on search"))
        with mock.patch.object(search_module, "_visible_paper_records",
                               return_value=[record]), \
             mock.patch.object(search_module, "_fulltext_index", return_value={}), \
             mock.patch.object(search_module, "extract_pdf_text", extract,
                               create=True):
            out = search_module.search_papers("bodyterm")
        self.assertEqual(out, [])
        extract.assert_not_called()

    def test_unindexed_paper_with_metadata_hit_still_matches(self):
        record = _rec("a.pdf", title="Mitochondria Survey")
        extract = mock.Mock(side_effect=AssertionError("must not extract on search"))
        with mock.patch.object(search_module, "_visible_paper_records",
                               return_value=[record]), \
             mock.patch.object(search_module, "_fulltext_index", return_value={}), \
             mock.patch.object(search_module, "extract_pdf_text", extract,
                               create=True):
            out = search_module.search_papers("mitochondria")
        self.assertEqual([r["filename"] for r in out], ["a.pdf"])
        extract.assert_not_called()


if __name__ == "__main__":
    unittest.main()
