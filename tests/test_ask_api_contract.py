# tests/test_ask_api_contract.py
import os
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import app as app_module


class ApiAskValidation(unittest.TestCase):
    def setUp(self):
        self.app = app_module.create_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def test_disabled_when_no_api_key(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LLM_API_KEY", None)
            resp = self.client.post("/api/ask", json={"question": "hi", "mode": "flash"})
            self.assertEqual(resp.status_code, 503)
            self.assertIn("error", resp.get_json())

    def test_rejects_empty_question(self):
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=False):
            resp = self.client.post("/api/ask", json={"question": "   ", "mode": "flash"})
            self.assertEqual(resp.status_code, 400)

    def test_rejects_overlong_question(self):
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=False):
            resp = self.client.post("/api/ask", json={"question": "x" * 2001, "mode": "flash"})
            self.assertEqual(resp.status_code, 400)


if __name__ == "__main__":
    unittest.main()
