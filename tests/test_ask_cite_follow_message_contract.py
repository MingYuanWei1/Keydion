# tests/test_ask_cite_follow_message_contract.py
#
# Contract: a library citation must "follow the message" — when a message is
# sent, the cited-paper chip moves from the composer onto the sent message
# (below the bubble, like uploaded-file chips) and the composer clears. The
# cited paper stays as context for the rest of the conversation (the forced
# grounding set is the union of every message's cited papers).
import os
import unittest
from pathlib import Path

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import app as app_module

ROOT = Path(__file__).resolve().parents[1]


class AskCiteFollowMessageJs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = (ROOT / "static" / "js" / "ask.js").read_text(encoding="utf-8")

    def test_send_posts_message_papers(self):
        # the per-message cited papers go out under a new field
        self.assertIn("message_papers", self.js)

    def test_no_longer_posts_paper_filenames(self):
        # the always-resent composer set is gone
        self.assertNotIn("paper_filenames", self.js)

    def test_selected_paper_list_helper_exists(self):
        # a helper exposes selected papers as {filename, title} for display + posting
        self.assertIn("__selectedPaperList", self.js)

    def test_send_clears_selected_papers(self):
        # send() must empty the library-cite set so the composer doesn't keep it pinned
        self.assertIn("__clearSelected", self.js)

    def test_adduser_takes_papers_third_arg(self):
        self.assertRegex(self.js, r"function addUser\(\s*text\s*,\s*\w+\s*,\s*\w+")

    def test_adduser_renders_paper_chip_on_message(self):
        # a paper chip rendered as "sent" (i.e. on the message, not the composer)
        self.assertIn("kd-chip--paper kd-chip--sent", self.js)

    def test_reload_renders_message_papers(self):
        # reopening a conversation must restore each message's cited-paper chips
        self.assertIn("m.papers", self.js)


class CitedPapersColumn(unittest.TestCase):
    def test_column_exists(self):
        cols = set(app_module.ChatMessageModel.__table__.columns.keys())
        self.assertIn("cited_papers", cols)

    def test_migration_present(self):
        src = (Path(app_module.__file__).resolve().parent / "app.py").read_text(encoding="utf-8")
        self.assertIn("ALTER TABLE chat_messages ADD COLUMN cited_papers", src)


class AskPersistsCitedPapers(unittest.TestCase):
    def setUp(self):
        import inspect
        self.src = inspect.getsource(app_module.create_app)

    def test_reads_message_papers(self):
        self.assertIn('data.get("message_papers"', self.src)

    def test_stores_cited_papers_on_user_row(self):
        self.assertIn("cited_papers=", self.src)

    def test_forced_grounding_unions_across_conversation(self):
        # the forced set is built from cited_papers across the conversation's messages
        self.assertIn("ChatMessageModel.cited_papers", self.src)
        self.assertIn("_forced_grounding(", self.src)


class ConversationGetReturnsMessagePapers(unittest.TestCase):
    def test_message_dict_includes_papers(self):
        import inspect
        src = inspect.getsource(app_module.create_app)
        self.assertIn('"papers": ', src)
        self.assertIn("m.cited_papers", src)


if __name__ == "__main__":
    unittest.main()
