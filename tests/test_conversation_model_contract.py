# tests/test_conversation_model_contract.py
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import support


class ConversationModels(unittest.TestCase):
    def test_models_defined(self):
        src = support.all_sources()
        self.assertIn("class ConversationModel(BASE)", src)
        self.assertIn("class ChatMessageModel(BASE)", src)
        self.assertIn('__tablename__ = "conversations"', src)
        self.assertIn('__tablename__ = "chat_messages"', src)

    def test_crud_routes_present(self):
        src = support.all_sources()
        self.assertIn('@app.route("/api/conversations"', src)
        self.assertIn("def api_conversations(", src)

    def test_owner_key_helper(self):
        self.assertIn("def _ask_owner_key(", support.all_sources())

    def test_llm_messages_include_prior_conversation_context(self):
        src = support.all_sources()
        self.assertIn("def _ask_llm_messages(", src)
        self.assertIn("history_rows = (db.query(ChatMessageModel)", src)
        self.assertIn('{"role": row.role, "content": row.content}', src)
        self.assertIn('messages=[{"role": "system", "content": system}] + llm_messages', src)


if __name__ == "__main__":
    unittest.main()
