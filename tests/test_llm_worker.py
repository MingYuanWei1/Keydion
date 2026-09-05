"""Worker transport behavior with real SDK serialization and mocked HTTP only."""
import json
import os
import time
import unittest
from unittest.mock import patch

import httpx2 as httpx
import llm_client
import llm_worker
from services.publishing_contracts import IndexDeadlineExceeded

ENV = {
    "LLM_WORKER_URL": "https://worker.example",
    "LLM_WORKER_TOKEN": "worker-secret", "LLM_WORKER_EMBED_ID": "a" * 64,
    "LLM_API_KEY": "direct-secret", "LLM_BASE_URL": "https://direct.example",
    "LLM_EMBED_API_KEY": "direct-embed-secret", "LLM_VISION_API_KEY": "direct-vision-secret",
}


def capability_data():
    return {"purposes": {
        purpose: {"enabled": purpose != "flash", "model": purpose + "-model",
                  "embedding_id": "a" * 64, "dimensions": llm_worker.config.RAG_EMBED_DIM}
        for purpose in ("flash", "think", "vision", "embed")
    }}


class WorkerClientTest(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, ENV, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        llm_worker._cache.clear()

    def test_purpose_aliases_and_worker_credentials_for_every_client(self):
        for build, model in ((llm_client.build_client, llm_client.flash_model),
                             (llm_client.build_embed_client, llm_client.embed_model),
                             (llm_client.build_vision_client, lambda: "vision")):
            seen = []

            def respond(request):
                seen.append(request)
                if request.url.path.endswith("embeddings"):
                    return httpx.Response(200, json={"object": "list", "model": "real-model",
                        "data": [{"index": 0, "embedding": [0.1, 0.2], "object": "embedding"}]})
                return httpx.Response(200, json={"id": "answer", "model": "real-model",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}]})

            def transport(**kwargs):
                self.assertFalse(kwargs["follow_redirects"])
                return httpx.Client(transport=httpx.MockTransport(respond), **kwargs)

            with patch("openai.DefaultHttpxClient", side_effect=transport):
                with build() as client:
                    self.assertEqual(client.max_retries, 0)
                    if build is llm_client.build_embed_client:
                        result = client.embeddings.create(model=model(), input=["q"])
                        self.assertEqual(result.data[0].embedding, [0.1, 0.2])
                    else:
                        self.assertEqual(client.chat.completions.create(model=model(), messages=[{"role":"user", "content":"hi"}]).choices[0].message.content, "ok")
            self.assertEqual(seen[0].url.host, "worker.example")
            self.assertEqual(seen[0].headers["authorization"], "Bearer worker-secret")
            self.assertEqual(seen[0].headers["user-agent"], "Keydion/llm-worker")
            self.assertNotIn("direct-secret", str(seen[0].headers))
            self.assertEqual(json.loads(seen[0].content)["model"], model())
            if build is llm_client.build_embed_client:
                self.assertEqual(seen[0].headers["x-keydion-embed-dim"], str(llm_worker.config.RAG_EMBED_DIM))

    def test_streaming_sdk_preserves_tool_delta_and_content(self):
        chunks = [
            {"id":"a", "choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"t","type":"function","function":{"name":"search","arguments":"{}"}}]}}]},
            {"id":"a", "choices":[{"index":0,"delta":{"content":"answer"}}]},
        ]
        def respond(request):
            payload = json.loads(request.content)
            self.assertTrue(payload["stream"])
            self.assertEqual(payload["model"], "think")
            return httpx.Response(200, text="".join("data: " + json.dumps(c) + "\n\n" for c in chunks) + "data: [DONE]\n\n", headers={"content-type":"text/event-stream"})
        with patch("openai.DefaultHttpxClient", side_effect=lambda **kw: httpx.Client(transport=httpx.MockTransport(respond), **kw)):
            with llm_client.build_client() as client:
                stream = client.chat.completions.create(model=llm_client.think_model(), messages=[], stream=True)
                output = list(stream)
        self.assertEqual(output[0].choices[0].delta.tool_calls[0].function.name, "search")
        self.assertEqual(output[1].choices[0].delta.content, "answer")

    def test_redirect_does_not_send_worker_token_to_another_origin(self):
        seen = []
        def respond(request):
            seen.append(request.url.host)
            return httpx.Response(307, headers={"location":"https://other.example/v1/chat/completions"})
        with patch("openai.DefaultHttpxClient", side_effect=lambda **kw: httpx.Client(transport=httpx.MockTransport(respond), **kw)):
            with llm_client.build_client() as client:
                with self.assertRaises(Exception):
                    client.chat.completions.create(model="flash", messages=[])
        self.assertEqual(seen, ["worker.example"])

    def test_missing_worker_settings_never_fall_back_to_direct_keys(self):
        os.environ["LLM_WORKER_TOKEN"] = ""
        for build in (llm_client.build_client, llm_client.build_embed_client, llm_client.build_vision_client):
            with self.assertRaises(ValueError):
                build()
        self.assertFalse(llm_client.llm_enabled())
        self.assertFalse(llm_client.embedding_enabled())
        self.assertFalse(llm_client.vision_enabled())

    def test_deadline_is_still_authoritative(self):
        with self.assertRaises(IndexDeadlineExceeded):
            llm_client.build_client(deadline=time.monotonic() - 1)
        with llm_client.build_client(deadline=time.monotonic() + 3) as client:
            self.assertLessEqual(client.timeout, 3)
            self.assertEqual(client.max_retries, 0)

    def test_legacy_transport_switch_cannot_restore_direct_access(self):
        os.environ["LLM_TRANSPORT"] = "direct"
        with llm_client.build_client() as client:
            self.assertEqual(client.api_key, "worker-secret")
            self.assertEqual(client.base_url.host, "worker.example")
        self.assertEqual(llm_client.flash_model(), "flash")

    def test_capabilities_cache_expiry_and_fail_closed_without_cross_capability_gates(self):
        data = capability_data()
        calls = []
        def respond(request):
            calls.append(request)
            self.assertEqual(request.headers["authorization"], "Bearer worker-secret")
            return httpx.Response(200, json=data)
        client_type = httpx.Client
        with patch("llm_worker.httpx.Client", side_effect=lambda **kw: client_type(transport=httpx.MockTransport(respond), **kw)), patch("llm_worker.time.monotonic", return_value=1):
            self.assertTrue(llm_client.llm_enabled())  # think alone enables Ask
            self.assertTrue(llm_client.vision_enabled())
            self.assertTrue(llm_client.embedding_enabled())
            self.assertEqual(len(calls), 1)
            os.environ["LLM_WORKER_EMBED_ID"] = "b" * 64
            self.assertFalse(llm_client.embedding_enabled())
            self.assertTrue(llm_client.vision_enabled())
        with patch("llm_worker.httpx.Client", side_effect=httpx.ConnectError("unavailable")), patch("llm_worker.time.monotonic", return_value=20):
            self.assertFalse(llm_client.llm_enabled())
            self.assertFalse(llm_client.vision_enabled())
            self.assertFalse(llm_client.embedding_enabled())

    def test_url_validation_rejects_insecure_or_ambiguous_origins(self):
        for url in ("http://remote.example", "https://user:pass@worker.example", "https://worker.example/v1", "https://worker.example?token=x", "https://worker.example/#part"):
            os.environ["LLM_WORKER_URL"] = url
            with self.assertRaises(ValueError):
                llm_worker.credentials()
        os.environ["LLM_WORKER_URL"] = "http://localhost:8787"
        self.assertEqual(llm_worker.credentials()[1], "http://localhost:8787/v1")


class WorkerAdminTest(unittest.TestCase):
    def test_model_slot_writes_are_rejected(self):
        from flask import Flask
        from flask_babel import Babel
        from services import llm_admin
        app = Flask(__name__)
        Babel(app)
        with app.app_context(), patch.object(llm_admin, "_write_env") as write:
            for slot in ("text", "embed", "vision", "provider"):
                with self.assertRaises(llm_admin.LLMAdminError):
                    llm_admin.apply_slot({"slot": slot})
            write.assert_not_called()

    def test_worker_template_has_status_no_model_editor_and_retains_tavily(self):
        from tests.test_admin_models_contract import _render
        html = _render({"worker":True, "available":True, **capability_data(), "embedding_ready":False,
                        "env_mtime":1, "search_configured":True})
        self.assertIn("Model configuration is managed in Cloudflare.", html)
        self.assertIn("Embeddings are blocked", html)
        self.assertIn('id="workerSearchForm"', html)
        self.assertIn('method="post"', html)
        self.assertNotIn('id="providerModal"', html)
        self.assertNotIn('js/admin-models.js', html)
        self.assertNotIn("worker-secret", html)

    def test_tavily_save_does_not_rewrite_provider_configuration(self):
        from services import llm_admin
        with patch.dict(os.environ, ENV), patch.object(llm_admin, "_write_env") as write, \
             patch.object(llm_admin, "snapshot", return_value={}), \
             patch.object(llm_admin.version_service, "request_graceful_restart", return_value=False):
            result = llm_admin.apply_slot({"slot":"search", "api_key":"new-search-key"}, expected_env_mtime=1)
            self.assertTrue(result["ok"])
            write.assert_called_once_with({"WEB_SEARCH_API_KEY":"new-search-key"}, expected_mtime=1)


if __name__ == "__main__":
    unittest.main()
