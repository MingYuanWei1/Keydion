import os
import sys
import types
import unittest
from unittest import mock

import llm_client
from services.publishing_contracts import IndexDeadlineExceeded


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

    def test_embed_batch_size_defaults_to_10(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(llm_client.embed_batch_size(), 10)

    def test_embed_batch_size_uses_env(self):
        with mock.patch.dict(os.environ, {"LLM_EMBED_BATCH": "64"}, clear=True):
            self.assertEqual(llm_client.embed_batch_size(), 64)

    def test_embed_batch_size_falls_back_on_garbage(self):
        with mock.patch.dict(os.environ, {"LLM_EMBED_BATCH": "not-a-number"}, clear=True):
            self.assertEqual(llm_client.embed_batch_size(), 10)

    def test_llm_enabled_reflects_api_key(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(llm_client.llm_enabled())
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True):
            self.assertTrue(llm_client.llm_enabled())

    def test_embedding_enabled_is_independent_and_uses_chat_fallback(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(llm_client.embedding_enabled())
        with mock.patch.dict(
            os.environ,
            {"LLM_EMBED_API_KEY": "embed-only"},
            clear=True,
        ):
            self.assertTrue(llm_client.embedding_enabled())
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "chat"}, clear=True):
            self.assertTrue(llm_client.embedding_enabled())


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

    def test_deadline_bound_client_uses_remaining_timeout_and_no_retries(self):
        calls = []

        def openai(**kwargs):
            calls.append(kwargs)
            return object()

        fake_module = types.SimpleNamespace(OpenAI=openai)
        with mock.patch.dict(sys.modules, {"openai": fake_module}), \
             mock.patch("time.monotonic", return_value=4.5):
            llm_client._new_client("key", "https://embed.example", deadline=10.0)

        self.assertEqual(
            calls,
            [
                {
                    "api_key": "key",
                    "base_url": "https://embed.example",
                    "timeout": 5.5,
                    "max_retries": 0,
                }
            ],
        )

    def test_exhausted_client_deadline_raises_before_construction(self):
        openai = mock.Mock()
        fake_module = types.SimpleNamespace(OpenAI=openai)
        with mock.patch.dict(sys.modules, {"openai": fake_module}), \
             mock.patch("time.monotonic", return_value=10.0):
            with self.assertRaises(IndexDeadlineExceeded):
                llm_client._new_client("key", None, deadline=10.0)
        openai.assert_not_called()

    def test_client_construction_failure_after_deadline_becomes_deadline_error(self):
        now = [1.0]
        failure = TimeoutError("client construction timed out")

        def openai(**_kwargs):
            now[0] = 10.0
            raise failure

        fake_module = types.SimpleNamespace(OpenAI=openai)
        with mock.patch.dict(sys.modules, {"openai": fake_module}), \
             mock.patch(
                 "time.monotonic", side_effect=lambda: now[0]
             ):
            with self.assertRaises(IndexDeadlineExceeded) as raised:
                llm_client._new_client("key", None, deadline=10.0)
        self.assertIs(raised.exception.__cause__, failure)

    def test_client_construction_failure_with_time_remaining_is_preserved(self):
        failure = RuntimeError("client construction failed early")
        fake_module = types.SimpleNamespace(OpenAI=mock.Mock(side_effect=failure))
        with mock.patch.dict(sys.modules, {"openai": fake_module}), \
             mock.patch("time.monotonic", return_value=1.0):
            with self.assertRaises(RuntimeError) as raised:
                llm_client._new_client("key", None, deadline=10.0)
        self.assertIs(raised.exception, failure)


class LlmClientVisionConfig(unittest.TestCase):
    def test_vision_model_defaults_empty_when_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(llm_client.vision_model(), "")

    def test_vision_model_uses_env(self):
        with mock.patch.dict(os.environ, {"LLM_VISION": "vqa-x"}, clear=True):
            self.assertEqual(llm_client.vision_model(), "vqa-x")

    def test_vision_credentials_fall_back_to_chat_vars(self):
        env = {"LLM_API_KEY": "chatkey", "LLM_BASE_URL": "https://chat.example/v1"}
        with mock.patch.dict(os.environ, env, clear=True):
            key, base = llm_client._vision_credentials()
            self.assertEqual(key, "chatkey")
            self.assertEqual(base, "https://chat.example/v1")

    def test_vision_credentials_prefer_vision_vars(self):
        env = {
            "LLM_API_KEY": "chatkey",
            "LLM_BASE_URL": "https://chat.example/v1",
            "LLM_VISION_API_KEY": "viskey",
            "LLM_VISION_BASE_URL": "https://vis.example/v1",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            key, base = llm_client._vision_credentials()
            self.assertEqual(key, "viskey")
            self.assertEqual(base, "https://vis.example/v1")

    def test_vision_enabled_false_without_model(self):
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True):
            self.assertFalse(llm_client.vision_enabled())

    def test_vision_enabled_false_without_key(self):
        with mock.patch.dict(os.environ, {"LLM_VISION": "vqa-x"}, clear=True):
            self.assertFalse(llm_client.vision_enabled())

    def test_vision_enabled_true_with_model_and_key(self):
        env = {"LLM_VISION": "vqa-x", "LLM_API_KEY": "k"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertTrue(llm_client.vision_enabled())


class VisionClientFallbackBounds(unittest.TestCase):
    """A vision client built without a deadline must still be finite.

    Vision calls run synchronously on user-visible request paths; the SDK
    default (600s reads x 2 retries, ~30 min) would let one slow provider
    response block a gunicorn worker and burn paid quota. The deadline path
    already bounds itself; this pins the no-deadline backstop and guards
    chat/embed clients against unintended change.
    """

    def _construct(self, build):
        calls = []

        def openai(**kwargs):
            calls.append(kwargs)
            return object()

        fake_module = types.SimpleNamespace(OpenAI=openai)
        with mock.patch.dict(sys.modules, {"openai": fake_module}):
            build()
        return calls[0]

    def test_vision_client_without_deadline_gets_finite_backstop(self):
        kwargs = self._construct(llm_client.build_vision_client)
        self.assertIn("timeout", kwargs)
        self.assertGreater(kwargs["timeout"], 0)
        self.assertLessEqual(kwargs["timeout"], 120)
        self.assertEqual(kwargs["max_retries"], 0)

    def test_vision_client_deadline_overrides_fallback(self):
        with mock.patch("time.monotonic", return_value=4.5):
            kwargs = self._construct(
                lambda: llm_client.build_vision_client(deadline=34.5))
        self.assertEqual(kwargs["timeout"], 30.0)
        self.assertEqual(kwargs["max_retries"], 0)

    def test_chat_and_embed_clients_keep_sdk_defaults(self):
        for build in (llm_client.build_client, llm_client.build_embed_client):
            kwargs = self._construct(build)
            self.assertNotIn("timeout", kwargs)
            self.assertNotIn("max_retries", kwargs)


class ParseJsonTest(unittest.TestCase):
    """llm_client._parse_json — JSON salvage for prose-wrapped model output."""

    def test_clean_json_object(self):
        self.assertEqual(
            llm_client._parse_json('{"a": 1, "b": [2, 3]}'), {"a": 1, "b": [2, 3]}
        )

    def test_json_wrapped_in_prose(self):
        content = 'Here is the assessment:\n{"criteria": [], "holistic_comment": "ok"}\nThanks!'
        self.assertEqual(
            llm_client._parse_json(content),
            {"criteria": [], "holistic_comment": "ok"},
        )

    def test_empty_string_returns_none(self):
        self.assertIsNone(llm_client._parse_json(""))

    def test_no_json_present_returns_none(self):
        self.assertIsNone(llm_client._parse_json("absolutely no json here"))

    def test_malformed_braces_return_none(self):
        self.assertIsNone(llm_client._parse_json("{not: valid, json"))


class _ChatClient:
    """Fake OpenAI client whose chat.completions.create records kwargs."""

    class _Resp:
        def __init__(self, content):
            class _Msg:
                pass
            class _Choice:
                pass
            msg = _Msg()
            msg.content = content
            choice = _Choice()
            choice.message = msg
            self.choices = [choice]

    def __init__(self, content="plain text", error=None):
        self.calls = []
        self._content = content
        self._error = error
        self.chat = self
        self.completions = self

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return _ChatClient._Resp(self._content)


class ChatInterfaceTest(unittest.TestCase):
    """The conversation seam: tier resolution, kwargs, errors, deadline authority."""

    MESSAGES = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]

    def test_chat_resolves_flash_tier_and_forwards_kwargs(self):
        client = _ChatClient(content="plain text")
        with mock.patch.dict(
            os.environ, {"LLM_API_KEY": "k", "LLM_DEFAULT_FLASH": "fast-x"}, clear=True
        ), mock.patch.object(llm_client, "build_client", return_value=client) as build:
            out = llm_client.chat(self.MESSAGES, tier="flash", temperature=0.2)
        self.assertEqual(out, "plain text")
        build.assert_called_once_with(deadline=None)
        self.assertEqual(client.calls[0]["model"], "fast-x")
        self.assertEqual(client.calls[0]["temperature"], 0.2)
        self.assertNotIn("response_format", client.calls[0])
        self.assertNotIn("timeout", client.calls[0])

    def test_chat_json_sets_json_mode(self):
        client = _ChatClient(content='{"a": 1}')
        with mock.patch.dict(
            os.environ, {"LLM_API_KEY": "k", "LLM_DEFAULT_THINK": "deep-y"}, clear=True
        ), mock.patch.object(llm_client, "build_client", return_value=client):
            out = llm_client.chat_json(self.MESSAGES, tier="think", temperature=0)
        self.assertEqual(out, {"a": 1})
        self.assertEqual(client.calls[0]["model"], "deep-y")
        self.assertEqual(client.calls[0]["response_format"], {"type": "json_object"})

    def test_vision_tier_uses_vision_client_and_model(self):
        client = _ChatClient(content='{"ok": true}')
        with mock.patch.dict(
            os.environ, {"LLM_VISION": "vqa-x", "LLM_API_KEY": "k"}, clear=True
        ), mock.patch.object(
            llm_client, "build_vision_client", return_value=client
        ) as build:
            out = llm_client.chat_json(self.MESSAGES, tier="vision", temperature=0.2)
        self.assertEqual(out, {"ok": True})
        build.assert_called_once_with(deadline=None)
        self.assertEqual(client.calls[0]["model"], "vqa-x")
        self.assertEqual(client.calls[0]["temperature"], 0.2)

    def test_unknown_tier_rejected(self):
        client = _ChatClient()
        with self.assertRaises(ValueError):
            llm_client.chat(self.MESSAGES, tier="bogus", client=client)

    def test_injected_client_skips_builder(self):
        client = _ChatClient(content="hi")
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(llm_client, "build_client") as build:
            out = llm_client.chat(self.MESSAGES, tier="flash", client=client)
        self.assertEqual(out, "hi")
        build.assert_not_called()

    def test_chat_unavailable_without_api_key(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(llm_client.LLMChatUnavailable) as ctx:
                llm_client.chat(self.MESSAGES, tier="flash")
        self.assertEqual(str(ctx.exception), "AI assist is not configured.")

    def test_chat_unavailable_when_openai_missing(self):
        def _raise_import(*_a, **_k):
            raise ImportError("openai")
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True), \
             mock.patch.object(llm_client, "build_client", side_effect=_raise_import):
            with self.assertRaises(llm_client.LLMChatUnavailable) as ctx:
                llm_client.chat(self.MESSAGES, tier="flash")
        self.assertEqual(str(ctx.exception), "openai package is not installed.")

    def test_provider_error_becomes_request_error_and_logs(self):
        failure = RuntimeError("rate limited")
        client = _ChatClient(error=failure)
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client):
            with self.assertLogs("llm_client", level="ERROR"):
                with self.assertRaises(llm_client.LLMChatRequestError) as ctx:
                    llm_client.chat(self.MESSAGES, tier="flash")
        self.assertIs(ctx.exception.__cause__, failure)

    def test_malformed_response_becomes_parse_error(self):
        class _EmptyChoicesClient:
            def __init__(self):
                self.chat = self
                self.completions = self

            def create(self, **_kwargs):
                class _Resp:
                    choices = []
                return _Resp()
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True), \
             mock.patch.object(llm_client, "build_client",
                               return_value=_EmptyChoicesClient()):
            with self.assertRaises(llm_client.LLMChatParseError):
                llm_client.chat(self.MESSAGES, tier="flash")

    def test_chat_json_salvages_prose_wrapped_json(self):
        client = _ChatClient(content='Sure!\n{"a": 2}\nthanks')
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client):
            self.assertEqual(llm_client.chat_json(self.MESSAGES, tier="flash"), {"a": 2})

    def test_chat_json_non_object_raises_parse_error(self):
        client = _ChatClient(content='["a", "b"]')
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client):
            with self.assertRaises(llm_client.LLMChatParseError):
                llm_client.chat_json(self.MESSAGES, tier="flash")

    def test_deadline_sets_request_timeout(self):
        client = _ChatClient(content="ok")
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client), \
             mock.patch("time.monotonic", return_value=4.0):
            llm_client.chat(self.MESSAGES, tier="flash", deadline=10.0)
        self.assertEqual(client.calls[0]["timeout"], 6.0)

    def test_provider_failure_after_deadline_becomes_deadline_error(self):
        now = [1.0]
        failure = RuntimeError("provider timed out")

        def fail(**_kwargs):
            now[0] = 10.0
            raise failure

        client = _ChatClient()
        client.create = fail
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client), \
             mock.patch("time.monotonic", side_effect=lambda: now[0]):
            with self.assertRaises(IndexDeadlineExceeded) as ctx:
                llm_client.chat(self.MESSAGES, tier="flash", deadline=10.0)
        self.assertIs(ctx.exception.__cause__, failure)

    def test_exhausted_deadline_raises_before_request(self):
        client = _ChatClient()
        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client), \
             mock.patch("time.monotonic", return_value=10.0):
            with self.assertRaises(IndexDeadlineExceeded):
                llm_client.chat(self.MESSAGES, tier="flash", deadline=10.0)
        self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
