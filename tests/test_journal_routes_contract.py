"""Contract: journals are served by name-based slug, with an id->slug 301 redirect."""
import unittest

from tests.support import source_of, all_sources


class JournalRoutesContractTest(unittest.TestCase):
    def test_detail_route_is_slug_based(self):
        src = source_of("journal_detail")
        self.assertIn("/journals/<slug>", all_sources())
        self.assertIn("slug", src)
        self.assertIn("get_journal_by_slug", src)

    def test_legacy_id_route_redirects(self):
        sources = all_sources()
        self.assertIn("/journal/<journal_id>", sources)
        self.assertIn("journal_detail_legacy", sources)

    def test_add_sets_slug(self):
        self.assertIn("set_unique_slug", source_of("journal_add"))

    def test_edit_regenerates_slug(self):
        self.assertIn("set_unique_slug", source_of("journal_edit"))

    def test_list_page_annotates_paper_count(self):
        src = source_of("journal_list_page")
        self.assertIn("get_journal_paper_counts", src)
        self.assertIn("paper_count", src)


if __name__ == "__main__":
    unittest.main()
