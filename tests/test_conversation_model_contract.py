# tests/test_conversation_model_contract.py
import os
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(p):
    with open(os.path.join(ROOT, p), encoding="utf-8") as f:
        return f.read()


class ConversationModels(unittest.TestCase):
    def test_models_defined(self):
        src = _read("app.py")
        self.assertIn("class ConversationModel(BASE)", src)
        self.assertIn("class ChatMessageModel(BASE)", src)
        self.assertIn('__tablename__ = "conversations"', src)
        self.assertIn('__tablename__ = "chat_messages"', src)

    def test_crud_routes_present(self):
        src = _read("app.py")
        self.assertIn('@app.route("/api/conversations"', src)
        self.assertIn("def api_conversations(", src)

    def test_owner_key_helper(self):
        self.assertIn("def _ask_owner_key(", _read("app.py"))


if __name__ == "__main__":
    unittest.main()
