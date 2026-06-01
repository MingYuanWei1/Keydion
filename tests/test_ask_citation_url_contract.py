# tests/test_ask_citation_url_contract.py
"""Contract: Ask answer citations link to the human-facing preview page.

Clicking a cited source must land the user on the paper preview page
(`preview_paper` -> /preview/<file>), NOT on `paper_info` -> /paper/<file>/info.
`paper_info` is an admin-only (require_login level=3) JSON endpoint built for the
dashboard preview modal; the Ask feature is available to every signed-in user
(and to guests under OPEN_ACCESS), so a citation pointing there yields a 401 for
non-admins and a raw JSON dump for admins instead of the paper.
"""
import ast
import os
import unittest

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

APP_PY = os.path.join(os.path.dirname(__file__), "..", "app.py")


def _api_ask_source():
    with open(APP_PY, "r", encoding="utf-8") as fh:
        src = fh.read()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "api_ask":
            return ast.get_source_segment(src, node)
    raise AssertionError("api_ask view function not found in app.py")


class AskCitationUrl(unittest.TestCase):
    def setUp(self):
        self.src = _api_ask_source()

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
