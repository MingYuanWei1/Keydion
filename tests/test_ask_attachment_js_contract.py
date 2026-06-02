import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class AskAttachmentJsContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = (ROOT / "static" / "js" / "ask.js").read_text(encoding="utf-8")

    def test_send_posts_message_attachments(self):
        self.assertIn("message_attachments", self.js)

    def test_send_captures_then_clears(self):
        # send() captures the pending set into sentAttachments, then clears
        # (uploads AND cited papers) + re-renders the composer.
        self.assertIn("sentAttachments", self.js)
        self.assertRegex(self.js, r"sentAttachments[\s\S]{0,400}window\.__attachedDocs = \{\}[\s\S]{0,200}renderChips\(\)")

    def test_adduser_takes_attachments(self):
        self.assertRegex(self.js, r"function addUser\(\s*text\s*,\s*\w+")

    def test_open_conversation_no_composer_restore(self):
        # attachments must NOT be re-injected into the composer on reload
        self.assertNotIn("j.attachments || []).forEach", self.js)

    def test_awaits_in_flight_uploads(self):
        self.assertIn("__attachUploads", self.js)
