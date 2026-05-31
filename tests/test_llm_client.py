import os
import unittest
from unittest import mock

import llm_client


class LlmClientModelResolution(unittest.TestCase):
    def test_flash_defaults_when_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(llm_client.flash_model(), "gpt-4o-mini")

    def test_flash_uses_env(self):
        with mock.patch.dict(os.environ, {"LLM_DEFAULT_FLASH": "fast-x"}, clear=True):
            self.assertEqual(llm_client.flash_model(), "fast-x")

    def test_think_falls_back_to_flash_when_unset(self):
        with mock.patch.dict(os.environ, {"LLM_DEFAULT_FLASH": "fast-x"}, clear=True):
            self.assertEqual(llm_client.think_model(), "fast-x")

    def test_think_uses_env(self):
        with mock.patch.dict(os.environ, {"LLM_DEFAULT_THINK": "deep-y"}, clear=True):
            self.assertEqual(llm_client.think_model(), "deep-y")

    def test_embed_defaults(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(llm_client.embed_model(), "gemini-embedding-001")

    def test_llm_enabled_reflects_api_key(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(llm_client.llm_enabled())
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True):
            self.assertTrue(llm_client.llm_enabled())


class LlmClientEmbedConfig(unittest.TestCase):
    def test_embed_config_falls_back_to_chat_vars(self):
        env = {"LLM_API_KEY": "chatkey", "LLM_BASE_URL": "https://chat.example/v1"}
        with mock.patch.dict(os.environ, env, clear=True):
            key, base = llm_client._embed_credentials()
            self.assertEqual(key, "chatkey")
            self.assertEqual(base, "https://chat.example/v1")

    def test_embed_config_prefers_embed_vars(self):
        env = {
            "LLM_API_KEY": "chatkey",
            "LLM_BASE_URL": "https://chat.example/v1",
            "LLM_EMBED_API_KEY": "geminikey",
            "LLM_EMBED_BASE_URL": "https://gemini.example/v1beta/openai/",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            key, base = llm_client._embed_credentials()
            self.assertEqual(key, "geminikey")
            self.assertEqual(base, "https://gemini.example/v1beta/openai/")


if __name__ == "__main__":
    unittest.main()
