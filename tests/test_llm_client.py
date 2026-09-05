import os
import sys
import types
import unittest
from unittest import mock

import llm_client
from services.publishing_contracts import IndexDeadlineExceeded


class LlmClientModelResolution(unittest.TestCase):
    def test_models_are_worker_purposes(self):
        with mock.patch.dict(os.environ, {"LLM_WORKER_EMBED_ID": "a" * 64}, clear=True):
            self.assertEqual(llm_client.flash_model(), "flash")
            self.assertEqual(llm_client.think_model(), "think")
            self.assertEqual(llm_client.embed_model(), "embed:" + "a" * 64)

    def test_embed_batch_size(self):
        for value, expected in (("10", 10), ("64", 64), ("bad", 10), ("0", 1)):
            with mock.patch.dict(os.environ, {"LLM_EMBED_BATCH": value}):
                self.assertEqual(llm_client.embed_batch_size(), expected)


class LlmClientConstruction(unittest.TestCase):
    def test_deadline_bound_client_uses_remaining_timeout_and_no_retries(self):
        calls = []

        def openai(**kwargs):
            calls.append(kwargs)
            return object()

        fake_module = types.SimpleNamespace(OpenAI=openai, DefaultHttpxClient=lambda **kw: "no-redirect-client")
        with mock.patch.dict(sys.modules, {"openai": fake_module}), \
             mock.patch("time.monotonic", return_value=4.5):
            llm_client._new_client("key", "https://embed.example", deadline=10.0)

        self.assertEqual(
            calls,
            [
                {
                    "api_key": "key",
                    "base_url": "https://embed.example",
                    "http_client": "no-redirect-client",
                    "default_headers": {"User-Agent": "Keydion/llm-worker"},
                    "timeout": 5.5,
                    "max_retries": 0,
                }
            ],
        )

    def test_exhausted_client_deadline_raises_before_construction(self):
        openai = mock.Mock()
        fake_module = types.SimpleNamespace(OpenAI=openai, DefaultHttpxClient=lambda **kw: "no-redirect-client")
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

        fake_module = types.SimpleNamespace(OpenAI=openai, DefaultHttpxClient=lambda **kw: "no-redirect-client")
        with mock.patch.dict(sys.modules, {"openai": fake_module}), \
             mock.patch(
                 "time.monotonic", side_effect=lambda: now[0]
             ):
            with self.assertRaises(IndexDeadlineExceeded) as raised:
                llm_client._new_client("key", None, deadline=10.0)
        self.assertIs(raised.exception.__cause__, failure)

    def test_client_construction_failure_with_time_remaining_is_preserved(self):
        failure = RuntimeError("client construction failed early")
        fake_module = types.SimpleNamespace(OpenAI=mock.Mock(side_effect=failure), DefaultHttpxClient=lambda **kw: "no-redirect-client")
        with mock.patch.dict(sys.modules, {"openai": fake_module}), \
             mock.patch("time.monotonic", return_value=1.0):
            with self.assertRaises(RuntimeError) as raised:
                llm_client._new_client("key", None, deadline=10.0)
        self.assertIs(raised.exception, failure)


class WorkerClientBounds(unittest.TestCase):
    def test_all_clients_are_bounded_and_use_only_worker_credentials(self):
        with mock.patch.dict(os.environ, {"LLM_WORKER_URL": "https://worker.example", "LLM_WORKER_TOKEN": "test"}, clear=True):
            for build in (llm_client.build_client, llm_client.build_embed_client, llm_client.build_vision_client):
                with build() as client:
                    self.assertEqual(client.timeout, 90)
                    self.assertEqual(client.max_retries, 0)
                    self.assertEqual(client.base_url.host, "worker.example")


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

    def setUp(self):
        capability = mock.patch("llm_worker.purpose_enabled", return_value=True)
        capability.start()
        self.addCleanup(capability.stop)

    MESSAGES = [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]

    def test_chat_resolves_flash_tier_and_forwards_kwargs(self):
        client = _ChatClient(content="plain text")
        with mock.patch.dict(
            os.environ, {}, clear=True
        ), mock.patch.object(llm_client, "build_client", return_value=client) as build:
            out = llm_client.chat(self.MESSAGES, tier="flash", temperature=0.2)
        self.assertEqual(out, "plain text")
        build.assert_called_once_with(deadline=None)
        self.assertEqual(client.calls[0]["model"], "flash")
        self.assertEqual(client.calls[0]["temperature"], 0.2)
        self.assertNotIn("response_format", client.calls[0])
        self.assertNotIn("timeout", client.calls[0])

    def test_chat_json_sets_json_mode(self):
        client = _ChatClient(content='{"a": 1}')
        with mock.patch.dict(
            os.environ, {}, clear=True
        ), mock.patch.object(llm_client, "build_client", return_value=client):
            out = llm_client.chat_json(self.MESSAGES, tier="think", temperature=0)
        self.assertEqual(out, {"a": 1})
        self.assertEqual(client.calls[0]["model"], "think")
        self.assertEqual(client.calls[0]["response_format"], {"type": "json_object"})

    def test_vision_tier_uses_vision_client_and_model(self):
        client = _ChatClient(content='{"ok": true}')
        with mock.patch.dict(
            os.environ, {}, clear=True
        ), mock.patch.object(
            llm_client, "build_vision_client", return_value=client
        ) as build:
            out = llm_client.chat_json(self.MESSAGES, tier="vision", temperature=0.2)
        self.assertEqual(out, {"ok": True})
        build.assert_called_once_with(deadline=None)
        self.assertEqual(client.calls[0]["model"], "vision")
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

    def test_chat_unavailable_without_worker_capability(self):
        with mock.patch("llm_worker.purpose_enabled", return_value=False):
            with self.assertRaises(llm_client.LLMChatUnavailable) as ctx:
                llm_client.chat(self.MESSAGES, tier="flash")
        self.assertEqual(str(ctx.exception), "AI assist is not configured.")

    def test_chat_unavailable_when_openai_missing(self):
        def _raise_import(*_a, **_k):
            raise ImportError("openai")
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(llm_client, "build_client", side_effect=_raise_import):
            with self.assertRaises(llm_client.LLMChatUnavailable) as ctx:
                llm_client.chat(self.MESSAGES, tier="flash")
        self.assertEqual(str(ctx.exception), "openai package is not installed.")

    def test_provider_error_becomes_request_error_and_logs(self):
        failure = RuntimeError("rate limited")
        client = _ChatClient(error=failure)
        with mock.patch.dict(os.environ, {}, clear=True), \
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
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(llm_client, "build_client",
                               return_value=_EmptyChoicesClient()):
            with self.assertRaises(llm_client.LLMChatParseError):
                llm_client.chat(self.MESSAGES, tier="flash")

    def test_chat_json_salvages_prose_wrapped_json(self):
        client = _ChatClient(content='Sure!\n{"a": 2}\nthanks')
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client):
            self.assertEqual(llm_client.chat_json(self.MESSAGES, tier="flash"), {"a": 2})

    def test_chat_json_non_object_raises_parse_error(self):
        client = _ChatClient(content='["a", "b"]')
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client):
            with self.assertRaises(llm_client.LLMChatParseError):
                llm_client.chat_json(self.MESSAGES, tier="flash")

    def test_deadline_sets_request_timeout(self):
        client = _ChatClient(content="ok")
        with mock.patch.dict(os.environ, {}, clear=True), \
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
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client), \
             mock.patch("time.monotonic", side_effect=lambda: now[0]):
            with self.assertRaises(IndexDeadlineExceeded) as ctx:
                llm_client.chat(self.MESSAGES, tier="flash", deadline=10.0)
        self.assertIs(ctx.exception.__cause__, failure)

    def test_exhausted_deadline_raises_before_request(self):
        client = _ChatClient()
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(llm_client, "build_client", return_value=client), \
             mock.patch("time.monotonic", return_value=10.0):
            with self.assertRaises(IndexDeadlineExceeded):
                llm_client.chat(self.MESSAGES, tier="flash", deadline=10.0)
        self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
