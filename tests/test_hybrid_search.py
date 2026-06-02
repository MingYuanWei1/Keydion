# tests/test_hybrid_search.py
import os
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module


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


if __name__ == "__main__":
    unittest.main()
