"""Contract: provider credentials stay bound to their stored origin.

Security-review finding [3]: the admin probe combined a request-supplied base
URL with a STORED write-only key (exfiltrating it to an attacker origin), and
save/assign persisted endpoints with character validation only, letting
internal or cleartext origins receive stored credentials at runtime.

Rules enforced here:
1. A stored key is only ever probed against the endpoint it is stored for;
   changing the endpoint requires re-entering the key.
2. save_provider refuses non-public and non-HTTPS endpoints.
3. apply_slot refuses providers whose stored endpoint is non-public/cleartext
   (legacy registries created before the vetting existed).
"""
import json
import os
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import services.llm_admin as la


class LLMAdminOriginBindingBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env_path = Path(self.tmp.name) / ".env"
        self.env_path.write_text(
            "LLM_API_KEY=sk-slot\n"
            "LLM_BASE_URL=https://api.deepseek.com\n"
            "LLM_DEFAULT_FLASH=deepseek-v4-flash\n",
            encoding="utf-8",
        )
        self.json_path = Path(self.tmp.name) / "llm_providers.json"
        env_patch = unittest.mock.patch.dict(
            os.environ,
            {key: "" for key in (
                "LLM_API_KEY", "LLM_BASE_URL", "LLM_DEFAULT_FLASH", "LLM_DEFAULT_THINK",
                "LLM_EMBED_API_KEY", "LLM_EMBED_BASE_URL", "LLM_EMBED_MODEL",
                "LLM_VISION", "LLM_VISION_API_KEY", "LLM_VISION_BASE_URL",
                "WEB_SEARCH_PROVIDER", "WEB_SEARCH_API_KEY",
            )},
            clear=False)
        env_patch.start()
        self.addCleanup(env_patch.stop)
        for target in (la.active_env_path, la._registry_path):
            patch = unittest.mock.patch.object(
                la, target.__name__ if hasattr(target, "__name__") else target,
                return_value=self.env_path if target is la.active_env_path else self.json_path)
        patches = [
            unittest.mock.patch.object(la, "active_env_path", return_value=self.env_path),
            unittest.mock.patch.object(la, "_registry_path", return_value=self.json_path),
            unittest.mock.patch.object(la.version_service, "request_graceful_restart",
                                       return_value=False),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda: [os.environ.pop(k, None) for k in tuple(os.environ)
                                 if k.startswith("LLM_PROVIDER_")])

    def _seed_provider(self, base_url="https://api.deepseek.com"):
        """Create a provider with a stored key; return its id."""
        import web_search
        with unittest.mock.patch.object(web_search, "_resolve_public_ips",
                                        return_value=("93.184.216.34",)):
            result = la.save_provider(
                {"name": "DeepSeek", "base_url": base_url, "api_key": "sk-stored",
                 "models": [{"id": "deepseek-v4-flash", "role": "text"}]},
                expected_env_mtime=self.env_path.stat().st_mtime)
        return result["id"]


class ProbeOriginBinding(LLMAdminOriginBindingBase):
    def test_stored_key_never_travels_to_foreign_origin_provider_slot(self):
        pid = self._seed_provider()
        captured = []

        def capture(base_url, api_key, model):
            captured.append((base_url, api_key))
            return {"ok": True, "state": "online", "models": []}

        with unittest.mock.patch.object(la, "_probe_models", side_effect=capture):
            result = la.probe({"slot": "provider", "provider_id": pid,
                               "base_url": "https://attacker.example.com/v1",
                               "api_key": ""})
        for base_url, api_key in captured:
            self.assertNotEqual(
                (base_url, api_key),
                ("https://attacker.example.com/v1", "sk-stored"),
                "stored write-only key was sent to a request-supplied origin")
        self.assertFalse(result["ok"])

    def test_saved_pair_still_probes_without_overrides(self):
        pid = self._seed_provider()
        captured = []

        def capture(base_url, api_key, model):
            captured.append((base_url, api_key))
            return {"ok": True, "state": "online", "models": []}

        with unittest.mock.patch.object(la, "_probe_models", side_effect=capture):
            result = la.probe({"slot": "provider", "provider_id": pid})
        self.assertTrue(result["ok"])
        self.assertEqual(captured, [("https://api.deepseek.com", "sk-stored")])

    def test_operator_supplied_key_may_target_any_public_origin(self):
        pid = self._seed_provider()
        captured = []

        def capture(base_url, api_key, model):
            captured.append((base_url, api_key))
            return {"ok": True, "state": "online", "models": []}

        with unittest.mock.patch.object(la, "_probe_models", side_effect=capture):
            result = la.probe({"slot": "provider", "provider_id": pid,
                               "base_url": "https://other.example.com/v1",
                               "api_key": "sk-fresh"})
        self.assertTrue(result["ok"])
        self.assertEqual(captured, [("https://other.example.com/v1", "sk-fresh")])

    def test_stored_slot_key_never_travels_to_foreign_origin(self):
        captured = []

        def capture(base_url, api_key, model):
            captured.append((base_url, api_key))
            return {"ok": True, "state": "online", "models": []}

        with unittest.mock.patch.object(la, "_probe_models", side_effect=capture):
            result = la.probe({"slot": "text",
                               "base_url": "https://attacker.example.com/v1",
                               "api_key": ""})
        for base_url, api_key in captured:
            self.assertNotEqual((base_url, api_key),
                                ("https://attacker.example.com/v1", "sk-slot"))
        self.assertFalse(result["ok"])


class SaveProviderEndpointVetting(LLMAdminOriginBindingBase):
    def test_refuses_internal_endpoint(self):
        with self.assertRaises(la.LLMAdminError):
            la.save_provider({"name": "Internal", "base_url": "http://10.0.0.5/v1",
                              "models": []})

    def test_refuses_cleartext_public_endpoint(self):
        import web_search
        with unittest.mock.patch.object(web_search, "_resolve_public_ips",
                                        return_value=("93.184.216.34",)):
            with self.assertRaises(la.LLMAdminError):
                la.save_provider({"name": "Cleartext",
                                  "base_url": "http://api.example.com/v1",
                                  "models": []})

    def test_https_public_endpoint_still_saves(self):
        pid = self._seed_provider("https://api.deepseek.com")
        self.assertTrue(pid)


class ApplySlotVetsStoredEndpoint(LLMAdminOriginBindingBase):
    def test_refuses_legacy_internal_provider(self):
        registry = {
            "version": 2,
            "providers": [{
                "id": "legacy", "name": "Legacy Internal",
                "base_url": "http://10.0.0.5/v1",
                "key_var": "LLM_PROVIDER_LEGACY_API_KEY",
                "models": [{"id": "m1", "role": "text"}],
            }],
            "assignments": {"text": {}, "embed": {}, "vision": {}, "search": {}},
        }
        self.json_path.write_text(json.dumps(registry), encoding="utf-8")
        env_text = self.env_path.read_text(encoding="utf-8")
        self.env_path.write_text(env_text + "LLM_PROVIDER_LEGACY_API_KEY=sk-legacy\n",
                                 encoding="utf-8")
        before = self.env_path.read_text(encoding="utf-8")
        with self.assertRaises(la.LLMAdminError):
            la.apply_slot({"slot": "text", "provider_id": "legacy",
                           "flash": "m1", "think": ""},
                          expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertEqual(before, self.env_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
