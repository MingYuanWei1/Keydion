# tests/test_ask_entrypoint_gating_contract.py
import os
import re
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(path):
    with open(os.path.join(ROOT, path), encoding="utf-8") as f:
        return f.read()


class EntryPointGating(unittest.TestCase):
    def test_base_nav_link_gated_on_llm_enabled(self):
        html = _read("templates/_header.html")
        self.assertRegex(
            html,
            r"\{%\s*if\s+llm_enabled\s*%\}[^\n]*url_for\('ai'\)",
        )

    def test_landing_nav_link_gated_on_llm_enabled(self):
        html = _read("templates/_header.html")
        self.assertRegex(
            html,
            r"\{%\s*if\s+llm_enabled\s*%\}[^\n]*url_for\('ai'\)",
        )

    def test_landing_cta_button_gated_on_llm_enabled(self):
        html = _read("templates/landing.html")
        # The nav link now lives in _header.html; only the CTA remains gated in landing.
        gated = re.findall(r"\{%\s*if\s+llm_enabled\s*%\}[^\n]*url_for\('ai'\)", html)
        self.assertGreaterEqual(len(gated), 1)


if __name__ == "__main__":
    unittest.main()
