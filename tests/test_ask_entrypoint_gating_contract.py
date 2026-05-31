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
        html = _read("templates/base.html")
        # the ask_library nav link must sit inside an {% if llm_enabled %} block
        self.assertRegex(
            html,
            r"\{%\s*if\s+llm_enabled\s*%\}[^\n]*url_for\('ask_library'\)",
        )

    def test_landing_nav_link_gated_on_llm_enabled(self):
        html = _read("templates/landing.html")
        self.assertRegex(
            html,
            r"\{%\s*if\s+llm_enabled\s*%\}[^\n]*url_for\('ask_library'\)",
        )

    def test_landing_cta_button_gated_on_llm_enabled(self):
        html = _read("templates/landing.html")
        # there are two gated occurrences (nav link + CTA button); require >= 2
        gated = re.findall(r"\{%\s*if\s+llm_enabled\s*%\}[^\n]*ask_library", html)
        self.assertGreaterEqual(len(gated), 2)


if __name__ == "__main__":
    unittest.main()
