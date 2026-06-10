# tests/test_ask_citation_url_contract.py
"""Contract: Ask answer citations link to the human-facing preview page.

Clicking a cited source must land the user on the paper preview page
(`preview_paper` -> /preview/<file>), NOT on `paper_info` -> /paper/<file>/info.
`paper_info` is an admin-only (require_login level=3) JSON endpoint built for the
dashboard preview modal; the Ask feature is available to every signed-in user
(and to guests under OPEN_ACCESS), so a citation pointing there yields a 401 for
non-admins and a raw JSON dump for admins instead of the paper.
"""
import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


class AskCitationUrl(unittest.TestCase):
    def setUp(self):
        self.src = support.source_of("api_ask")

    def test_citations_link_to_preview_page(self):
        self.assertIn(
            'url_for("preview_paper"', self.src,
            "Ask citations must link to the preview page (preview_paper).",
        )

    def test_citations_do_not_link_to_admin_info_json(self):
        self.assertNotIn(
            "paper_info", self.src,
            "Ask citations must not link to paper_info (admin-only JSON endpoint).",
        )


if __name__ == "__main__":
    unittest.main()
