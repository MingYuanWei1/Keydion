# tests/test_cite_library_contract.py
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import support


def _read(p):
    with open(os.path.join(ROOT, p), encoding="utf-8") as f:
        return f.read()


class CiteLibrary(unittest.TestCase):
    def test_paper_list_endpoint(self):
        src = support.all_sources()
        self.assertIn('@app.route("/api/ask/papers")', src)
        self.assertIn("def api_ask_papers(", src)

    def test_forced_grounding_helper(self):
        self.assertIn("def _forced_grounding(", support.all_sources())

    def test_modal_markup_present(self):
        html = _read("templates/ask.html")
        self.assertIn("kd-overlay", html)
        self.assertIn("kd-modal", html)


if __name__ == "__main__":
    unittest.main()
