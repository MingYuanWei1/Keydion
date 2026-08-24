# tests/test_ask_prompt_contract.py
"""Contract: when no library sources are retrieved, the assistant may still
chat normally (greetings / identity / capability questions) instead of being
forced into a 'nothing found' reply. The 'no relevant papers' fallback is kept
only for genuine research/library questions.
"""
import os
import unittest

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import services.ai as app_module


class NoSourcesPrompt(unittest.TestCase):
    def _prompt(self, locale="en"):
        # empty hits + no web results -> the "no sources" branch
        return app_module._build_ask_prompt("Hello", [], locale)

    def test_introduces_identity_for_conversation(self):
        self.assertIn("introduce", self._prompt().lower())

    def test_keeps_research_fallback(self):
        self.assertIn("could not find", self._prompt().lower())

    def test_behaviour_is_conditional_not_forced(self):
        # Must branch on the kind of message rather than always replying not-found.
        self.assertIn("if", self._prompt().lower())

    def test_does_not_invent_sources(self):
        self.assertIn("not invent", self._prompt().lower())


class WithSourcesPromptUnchanged(unittest.TestCase):
    def test_sources_branch_still_lists_sources(self):
        hits = [{"title": "T", "author_name": "A", "content": "body"}]
        p = app_module._build_ask_prompt("q", hits, "en")
        self.assertIn("SOURCES:", p)
        self.assertIn("Cite", p)


if __name__ == "__main__":
    unittest.main()
