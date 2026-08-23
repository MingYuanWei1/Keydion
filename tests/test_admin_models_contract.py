"""Contract: admin AI-models control panel.

Covers the sidebar nav entry, the level-3 gate on every route, the
template/JS DOM id contract, the partial-rendering convention, the write-only
key posture (no key material ever rendered or echoed), env-file injection
validation, provider registry CRUD (id derivation, assignment guard, legacy
key normalization, derived key pattern), the embedding dimension gate, and
the probe SSRF guard.
No DB required (AST source lookup + bare-Jinja render + tmp-file units).
"""
import os
import re
import stat
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

from tests.support import source_of

ROOT = Path(__file__).resolve().parents[1]

SNAP = {
    "env_file": ".env.prod",
    "env_mtime": 1770000000.123456,
    "json_mtime": 1770000000.223456,
    "embed_dim": 3072,
    "providers": [
        {"id": "deepseek", "name": "DeepSeek", "base_url": "https://api.deepseek.com",
         "key_set": True, "used_by": ["text"],
         "models": [{"id": "deepseek-v4-flash", "role": "text"},
                    {"id": "deepseek-v4-pro", "role": "multimodal"}]},
        {"id": "aliyun-dashscope", "name": "Aliyun DashScope",
         "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
         "key_set": True, "used_by": ["embed", "vision"],
         "models": [{"id": "qwen3.7-plus", "role": "multimodal"},
                    {"id": "text-embedding-v4", "role": "embedding"}]},
    ],
    "assignments": {
        "text": {"provider_id": "deepseek", "flash": "deepseek-v4-flash", "think": "deepseek-v4-pro"},
        "embed": {"provider_id": "aliyun-dashscope", "model": "text-embedding-v4"},
        "vision": {"mode": "dedicated", "provider_id": "aliyun-dashscope", "model": "qwen3.7-plus"},
        "search": {},
    },
    "slots": {
        "text": {"provider": "DeepSeek", "base_url": "https://api.deepseek.com",
                 "key_set": True, "flash": "deepseek-v4-flash",
                 "think": "deepseek-v4-pro", "think_follows_flash": False},
        "embed": {"provider": "Aliyun DashScope",
                  "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                  "key_set": True, "uses_text_key": False, "model": "text-embedding-v4"},
        "vision": {"mode": "dedicated", "provider": "Aliyun DashScope",
                   "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                   "key_set": True, "uses_text_key": False, "model": "qwen3.7-plus"},
        "search": {"provider": "Tavily", "key_set": True},
    },
    "features": {
        "ask": {"on": True, "model": "deepseek-v4-pro"},
        "semantic_search": {"on": True, "model": "text-embedding-v4"},
        "vision_first": {"on": True, "model": "qwen3.7-plus"},
        "web_access": {"on": True, "model": "Tavily"},
    },
}


def _render(snap):
    env = Environment(loader=FileSystemLoader(ROOT / "templates"))
    env.globals["csrf_token"] = lambda: ""
    return env.get_template("admin_models.html").render(
        _=lambda value, **kw: value % kw if kw else value,
        url_for=lambda endpoint, **kw: "/" + endpoint,
        get_flashed_messages=lambda with_categories=False: [],
        request=SimpleNamespace(full_path="/dashboard/admin/models", args={}),
        session={},
        current_locale="en",
        partial=True,
        user=SimpleNamespace(role="3"),
        snap=snap,
    )


class AdminModelsTemplateContract(unittest.TestCase):
    def test_partial_flag_first_line(self):
        src = (ROOT / "templates" / "admin_models.html").read_text(encoding="utf-8")
        self.assertEqual(
            src.splitlines()[0],
            '{% extends "_bare.html" if partial else "_dashboard_shell.html" %}',
        )

    def test_dom_ids_used_by_js_exist(self):
        html = _render(SNAP)
        js = (ROOT / "static" / "js" / "admin-models.js").read_text(encoding="utf-8")
        # admin-models.js routes lookups through an id() helper around
        # getElementById, so collect both call shapes.
        ids = set(re.findall(r'getElementById\("([^"]+)"\)', js))
        ids |= set(re.findall(r'\bid\("([^"]+)"\)', js))
        self.assertTrue(ids, "expected the JS to reference elements by id")
        for element_id in ids:
            with self.subTest(element_id=element_id):
                self.assertIn(f'id="{element_id}"', html)

    def test_endpoint_urls_wired(self):
        html = _render(SNAP)
        self.assertIn('data-probe-url="/admin_models_probe"', html)
        self.assertIn('data-save-url="/admin_models_save"', html)
        self.assertIn('data-provider-save-url="/admin_models_provider_save"', html)
        self.assertIn('data-provider-delete-url="/admin_models_provider_delete"', html)

    def test_uses_manage_page_idiom(self):
        html = _render(SNAP)
        for cls in ("kp-head", "kp-title", "kp-crumb", "kp-card", "kp-table", "kp-btn--primary"):
            self.assertIn(cls, html)

    def test_no_key_material_rendered(self):
        """Keys are write-only: configured slots show status text, never values,
        and password inputs never carry a value attribute."""
        html = _render(SNAP)
        self.assertIsNone(re.search(r"sk-[A-Za-z0-9]{8,}", html), "DeepSeek-style key leaked")
        self.assertIsNone(re.search(r"tvly-[A-Za-z0-9]{8,}", html), "Tavily key leaked")
        for field in re.finditer(r'<input[^>]*type="password"[^>]*>', html):
            self.assertNotIn("value=", field.group(0), field.group(0))
        self.assertIn("Configured", html)

    def test_capability_strip_pills_present(self):
        html = _render(SNAP)
        for pill in ("capAsk", "capSemantic", "capVision", "capWeb"):
            self.assertIn(f'id="{pill}"', html)
        self.assertIn("deepseek-v4-pro", html)
        self.assertIn("qwen3.7-plus", html)

    def test_provider_table_and_modal_present(self):
        html = _render(SNAP)
        self.assertIn('id="providersBody"', html)
        self.assertIn('id="providerModal"', html)
        self.assertIn('data-edit-provider="deepseek"', html)
        self.assertIn('data-delete-provider="deepseek"', html)

    def test_status_column_shows_connection_state(self):
        """The Status column is a live connection state (online/offline/error),
        rendered as Checking… until the JS probe lands."""
        html = _render(SNAP)
        self.assertIn("<th>Status</th>", html)
        self.assertIn('data-status-provider="deepseek"', html)
        self.assertIn("models-status--checking", html)

    def test_provider_selects_offer_all_providers_seeded(self):
        html = _render(SNAP)
        def options_of(select_id):
            block = re.search(r'<select[^>]*id="' + select_id + r'".*?</select>', html, re.S).group(0)
            return set(re.findall(r'value="([a-z0-9-]+)"', block))
        for select_id in ("textProviderSel", "embedProviderSel", "visionProviderSel"):
            self.assertEqual({"deepseek", "aliyun-dashscope"}, options_of(select_id))
        self.assertIn('id="textProviderSel" data-selected="deepseek"', html)

    def test_model_selects_present_and_slot_test_buttons_gone(self):
        """Models are picked from lists the JS fills per provider+role; testing
        lives on the provider editor, not the slot cards."""
        html = _render(SNAP)
        for select_id in ("textFlash", "textThink", "embedModel", "visionModel"):
            self.assertRegex(html, r'<select[^>]*id="' + select_id + r'"')
        for gone in ("textTestBtn", "embedTestBtn", "visionTestBtn"):
            self.assertNotIn(gone, html)
        self.assertIn('id="providerTestBtn"', html)  # Test stays on the provider editor
        self.assertIn('id="providerModelsBody"', html)
        self.assertIn('id="providerModelAddBtn"', html)
        self.assertNotIn("datalist", html)  # free-text model entry is gone

    def test_vision_three_state_options(self):
        html = _render(SNAP)
        for option in ('value="text"', 'value="dedicated"', 'value="disabled"'):
            self.assertIn(option, html)

    def test_embed_dimension_visible(self):
        html = _render(SNAP)
        self.assertIn("VECTOR(3072)", html)


class AdminModelsRouteContract(unittest.TestCase):
    ROUTES = ("admin_models", "admin_models_probe", "admin_models_save",
              "admin_models_provider_save", "admin_models_provider_delete")

    def test_route_functions_require_level_3(self):
        for name in self.ROUTES:
            with self.subTest(route=name):
                self.assertIn("require_login(level=3)", source_of(name))

    def test_route_functions_do_not_pass_partial(self):
        for name in self.ROUTES:
            with self.subTest(route=name):
                self.assertNotIn("partial=", source_of(name))

    def test_module_registered_in_routes_package(self):
        src = (ROOT / "routes" / "__init__.py").read_text(encoding="utf-8")
        self.assertIn("llm_admin.register_routes(app)", src)

    def test_config_makes_panel_keys_file_authoritative(self):
        """Without file-wins semantics for the panel's keys, a panel edit is
        masked by systemd's EnvironmentFile snapshot even after a restart —
        but ONLY the panel's keys; other env overrides must keep winning."""
        src = (ROOT / "config.py").read_text(encoding="utf-8")
        self.assertIn("PANEL_ENV_KEYS", src)
        self.assertIn("dotenv_values(_ENV_FILE)", src)
        self.assertIn('"WEB_SEARCH_API_KEY"', src)
        self.assertNotIn("load_dotenv(_ENV_FILE, override=True)", src)


class AdminModelsNavContract(unittest.TestCase):
    def test_admin_group_links_models_page_between_users_and_version(self):
        shell = (ROOT / "templates" / "_dashboard_shell.html").read_text(encoding="utf-8")
        self.assertIn("{{ url_for('admin_models') }}", shell)
        users_pos = shell.index("url_for('admin_users')")
        models_pos = shell.index("url_for('admin_models')")
        version_pos = shell.index("url_for('admin_version')")
        self.assertGreater(models_pos, users_pos)
        self.assertLess(models_pos, version_pos)
        self.assertIn('data-partial-href="{{ url_for(\'admin_models\') }}"', shell)


class LLMAdminEnvWriterContract(unittest.TestCase):
    """Real unit tests against a temp env file (no DB, no network)."""

    def setUp(self):
        import services.llm_admin as la
        self.la = la
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / ".env"
        self.path.write_text(
            "# comment line\n"
            "PAPERQUERY_SECRET=keep-me\n"
            'PAPERQUERY_DATABASE_URL="mysql+pymysql://user:pw@host/db"\n'
            "LLM_API_KEY=sk-existing\n"
            "\n"
            "WEB_SEARCH_PROVIDER=tavily\n",
            encoding="utf-8",
        )
        self._patch = unittest.mock.patch.object(la, "active_env_path", return_value=self.path)
        self._patch.start()
        self.addCleanup(self._patch.stop)
        self.addCleanup(self.tmp.cleanup)

    def mtime(self):
        return self.path.stat().st_mtime

    def test_unknown_lines_preserved(self):
        before = self.path.read_text(encoding="utf-8")
        self.la._write_env({"LLM_BASE_URL": "https://api.deepseek.com"}, expected_mtime=self.mtime())
        after = self.path.read_text(encoding="utf-8")
        self.assertIn("# comment line", after)
        self.assertIn("PAPERQUERY_SECRET=keep-me", after)
        self.assertIn('PAPERQUERY_DATABASE_URL="mysql+pymysql://user:pw@host/db"', after)
        self.assertIn("WEB_SEARCH_PROVIDER=tavily", after)
        self.assertIn("LLM_BASE_URL=https://api.deepseek.com", after)
        self.assertIn("LLM_API_KEY=sk-existing", after)
        self.assertEqual(len(after.splitlines()), len(before.splitlines()) + 1)

    def test_appends_missing_key_and_writes_empty_value(self):
        self.la._write_env({"LLM_VISION": ""}, expected_mtime=self.mtime())
        self.assertIn("LLM_VISION=\n", self.path.read_text(encoding="utf-8"))

    def test_updated_key_written_unquoted_and_duplicates_collapsed(self):
        self.path.write_text("LLM_API_KEY=old1\nLLM_API_KEY=old2\n", encoding="utf-8")
        self.la._write_env({"LLM_API_KEY": "sk-new"}, expected_mtime=self.mtime())
        self.assertEqual("LLM_API_KEY=sk-new\n", self.path.read_text(encoding="utf-8"))

    def test_write_is_atomic_with_mode_600(self):
        self.la._write_env({"LLM_API_KEY": "sk-new"}, expected_mtime=self.mtime())
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        leftovers = [p for p in self.path.parent.iterdir() if ".tmp-" in p.name]
        self.assertEqual([], leftovers)

    def test_derived_provider_key_pattern_allowed_arbitrary_refused(self):
        from services.llm_admin import LLMAdminError
        self.la._write_env({"LLM_PROVIDER_DEEPSEEK_API_KEY": "sk-p"}, expected_mtime=self.mtime())
        with self.assertRaises(LLMAdminError):
            self.la._write_env({"LLM_PROVIDER_EVIL_NAME(x)_API_KEY": "sk-p"})
        with self.assertRaises(LLMAdminError):
            self.la._write_env({"PAPERQUERY_SECRET": "evil"})

    def test_injection_values_refused(self):
        from services.llm_admin import LLMAdminError
        bad_values = [
            "abc\ndef",            # breakout into a new KEY=VALUE line
            "val # comment",       # inline comment divergence between parsers
            'va"lue', "va'lue",    # quoting divergence
            "va lue", " va",       # whitespace
            "va\x00lue",           # control characters
        ]
        for value in bad_values:
            with self.subTest(value=repr(value)):
                with self.assertRaises(LLMAdminError):
                    self.la._write_env({"LLM_API_KEY": value})

    def test_mtime_conflict_refused_without_writing(self):
        from services.llm_admin import LLMAdminConflict
        before = self.path.read_text(encoding="utf-8")
        stale = self.mtime() - 500
        with self.assertRaises(LLMAdminConflict):
            self.la._write_env({"LLM_API_KEY": "sk-new"}, expected_mtime=stale)
        self.assertEqual(before, self.path.read_text(encoding="utf-8"))


class LLMAdminRegistryContract(unittest.TestCase):
    """Provider registry: derivation, CRUD, key normalization."""

    ENV_TEXT = (
        "LLM_API_KEY=sk-text-secret\n"
        "LLM_BASE_URL=https://api.deepseek.com\n"
        "LLM_DEFAULT_FLASH=deepseek-v4-flash\n"
        "LLM_DEFAULT_THINK=\n"
        "LLM_EMBED_API_KEY=sk-embed-secret\n"
        "LLM_EMBED_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1\n"
        "LLM_EMBED_MODEL=text-embedding-v4\n"
        "LLM_VISION=qwen3.7-plus\n"
        "WEB_SEARCH_API_KEY=tvly-search-secret\n"
    )

    def setUp(self):
        import services.llm_admin as la
        self.la = la
        self.tmp = tempfile.TemporaryDirectory()
        self.env_path = Path(self.tmp.name) / ".env"
        self.env_path.write_text(self.ENV_TEXT, encoding="utf-8")
        self.json_path = Path(self.tmp.name) / "llm_providers.json"
        env_vars = {key: "" for key in (
            "LLM_API_KEY", "LLM_BASE_URL", "LLM_DEFAULT_FLASH", "LLM_DEFAULT_THINK",
            "LLM_EMBED_API_KEY", "LLM_EMBED_BASE_URL", "LLM_EMBED_MODEL",
            "LLM_VISION", "LLM_VISION_API_KEY", "LLM_VISION_BASE_URL",
            "WEB_SEARCH_PROVIDER", "WEB_SEARCH_API_KEY",
        )}
        env_patch = unittest.mock.patch.dict(os.environ, env_vars, clear=False)
        env_patch.start()
        self.addCleanup(env_patch.stop)
        patches = [
            unittest.mock.patch.object(la, "active_env_path", return_value=self.env_path),
            unittest.mock.patch.object(la, "_registry_path", return_value=self.json_path),
            unittest.mock.patch.object(la.version_service, "request_graceful_restart", return_value=False),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda: [os.environ.pop(k, None) for k in tuple(os.environ)
                                 if k.startswith("LLM_") or k.startswith("WEB_SEARCH_")])

    def test_derived_registry_from_env(self):
        reg, _ = self.la.load_registry()
        ids = {p["id"] for p in reg["providers"]}
        self.assertEqual({"deepseek", "aliyun-dashscope"}, ids)
        by_id = {p["id"]: p for p in reg["providers"]}
        self.assertEqual(by_id["deepseek"]["key_var"], "LLM_API_KEY")
        self.assertEqual(by_id["deepseek"]["models"],
                         [{"id": "deepseek-v4-flash", "role": "text"},
                          {"id": "qwen3.7-plus", "role": "multimodal"}])  # vision text-mode upgrade
        self.assertEqual(by_id["aliyun-dashscope"]["models"],
                         [{"id": "text-embedding-v4", "role": "embedding"}])
        self.assertEqual(reg["assignments"]["text"]["provider_id"], "deepseek")
        self.assertEqual(reg["assignments"]["embed"]["provider_id"], "aliyun-dashscope")
        self.assertEqual(reg["assignments"]["vision"]["mode"], "text")  # no dedicated creds

    def test_snapshot_derives_bounded_id_for_dynamic_aliyun_hostname(self):
        """First load must handle deployment-specific hosts longer than an id."""
        with self.env_path.open("a", encoding="utf-8") as env_file:
            env_file.write(
                "LLM_VISION_API_KEY=sk-vision-secret\n"
                "LLM_VISION_BASE_URL="
                "https://abc123def456gh789xy.cn-shanghai.pai-eas.aliyuncs.com/v1\n"
            )

        snap = self.la.snapshot()

        vision_id = snap["assignments"]["vision"]["provider_id"]
        self.assertEqual("abc123def456gh789xy-cn-ee7f9796", vision_id)
        self.assertRegex(vision_id, r"^[a-z0-9][a-z0-9-]{0,31}$")
        self.assertIn(vision_id, {provider["id"] for provider in snap["providers"]})

    def test_save_provider_validates_model_roles(self):
        from services.llm_admin import LLMAdminError
        with self.assertRaises(LLMAdminError):
            self.la.save_provider({"name": "Bad Role", "base_url": "https://x.example",
                                   "models": [{"id": "m1", "role": "vision"}]})
        with self.assertRaises(LLMAdminError):
            self.la.save_provider({"name": "Dup", "base_url": "https://x.example",
                                   "models": [{"id": "m1", "role": "text"},
                                              {"id": "m1", "role": "embedding"}]})

    def test_assign_rejects_wrong_model_role(self):
        from services.llm_admin import LLMAdminError
        result = self.la.save_provider(
            {"name": "Embed Only", "base_url": "https://embed.example", "api_key": "sk-e",
             "models": [{"id": "embed-m", "role": "embedding"}]},
            expected_env_mtime=self.env_path.stat().st_mtime)
        with self.assertRaises(LLMAdminError) as ctx:
            self.la.apply_slot({"slot": "text", "provider_id": result["id"], "flash": "embed-m"},
                               expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertIn("not configured", str(ctx.exception))
        with self.assertRaises(LLMAdminError):
            self.la.apply_slot({"slot": "vision", "mode": "dedicated",
                                "provider_id": result["id"], "model": "embed-m"},
                               expected_env_mtime=self.env_path.stat().st_mtime)

    def test_multimodal_model_serves_text_and_vision(self):
        result = self.la.save_provider(
            {"name": "Google Gemini", "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
             "api_key": "sk-g",
             "models": [{"id": "gemini-3.1-pro", "role": "multimodal"},
                        {"id": "text-embedding-001", "role": "embedding"}]},
            expected_env_mtime=self.env_path.stat().st_mtime)
        self.la.apply_slot({"slot": "text", "provider_id": result["id"],
                            "flash": "gemini-3.1-pro", "think": ""},
                           expected_env_mtime=self.env_path.stat().st_mtime)
        outcome = self.la.apply_slot({"slot": "vision", "mode": "text", "model": "gemini-3.1-pro"},
                                     expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertTrue(outcome["ok"])
        self.assertIn("LLM_VISION=gemini-3.1-pro",
                      self.env_path.read_text(encoding="utf-8"))

    def test_model_removal_refused_while_assigned(self):
        from services.llm_admin import LLMAdminError
        result = self.la.save_provider(
            {"name": "Open Router", "base_url": "https://openrouter.ai/api/v1", "api_key": "sk-or",
             "models": [{"id": "m1", "role": "text"}, {"id": "m2", "role": "text"}]},
            expected_env_mtime=self.env_path.stat().st_mtime)
        self.la.apply_slot({"slot": "text", "provider_id": result["id"], "flash": "m1", "think": ""},
                           expected_env_mtime=self.env_path.stat().st_mtime)
        with self.assertRaises(LLMAdminError) as ctx:
            self.la.save_provider({"id": result["id"], "name": "Open Router",
                                   "base_url": "https://openrouter.ai/api/v1",
                                   "models": [{"id": "m2", "role": "text"}]},
                                  expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertIn("reassign", str(ctx.exception))

    def test_derivation_uses_process_env_when_no_file(self):
        """A dev container injects LLM vars via compose env_file with no env
        file on disk; derivation must still see the real providers."""
        self.env_path.unlink()
        injected = {
            "LLM_API_KEY": "sk-text-secret",
            "LLM_BASE_URL": "https://api.deepseek.com",
            "LLM_DEFAULT_FLASH": "deepseek-v4-flash",
        }
        with unittest.mock.patch.dict(os.environ, injected, clear=False):
            reg, _ = self.la.load_registry()
        self.assertEqual({p["id"] for p in reg["providers"]}, {"deepseek"})
        self.assertEqual(reg["assignments"]["text"]["provider_id"], "deepseek")

    def test_snapshot_never_contains_key_material(self):
        blob = repr(self.la.snapshot())
        self.assertNotIn("sk-text-secret", blob)
        self.assertNotIn("sk-embed-secret", blob)
        self.assertNotIn("tvly-search-secret", blob)
        snap = self.la.snapshot()
        self.assertTrue(snap["providers"][0]["key_set"])
        self.assertTrue(snap["slots"]["search"]["key_set"])

    def test_create_provider_derives_id_and_key_var(self):
        result = self.la.save_provider(
            {"name": "Open Router", "base_url": "https://openrouter.ai/api/v1",
             "api_key": "sk-or-new",
             "models": [{"id": "m1", "role": "text"}]},
            expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertEqual("open-router", result["id"])
        env_text = self.env_path.read_text(encoding="utf-8")
        self.assertIn("LLM_PROVIDER_OPEN_ROUTER_API_KEY=sk-or-new", env_text)
        reg = __import__("json").loads(self.json_path.read_text(encoding="utf-8"))
        stored = next(p for p in reg["providers"] if p["id"] == "open-router")
        self.assertEqual("LLM_PROVIDER_OPEN_ROUTER_API_KEY", stored["key_var"])

    def test_update_provider_keeps_id(self):
        self.la.save_provider({"name": "DeepSeek", "base_url": "https://api.deepseek.com",
                               "models": [{"id": "deepseek-v4-flash", "role": "text"}]},
                              expected_env_mtime=self.env_path.stat().st_mtime)
        result = self.la.save_provider({"id": "deepseek", "name": "DeepSeek v4",
                                        "base_url": "https://api.deepseek.com",
                                        "models": [{"id": "deepseek-v4-flash", "role": "text"},
                                                   {"id": "qwen3.7-plus", "role": "multimodal"}]},
                                       expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertEqual("deepseek", result["id"])

    def test_editing_migrated_provider_moves_key_off_slot_var(self):
        """A derived provider reading LLM_API_KEY must get its own key on first
        edit, so a later slot change cannot silently swap its key."""
        result = self.la.save_provider({"id": "deepseek", "name": "DeepSeek",
                                        "base_url": "https://api.deepseek.com",
                                        "api_key": "sk-deepseek-own",
                                        "models": [{"id": "deepseek-v4-flash", "role": "text"},
                                                   {"id": "qwen3.7-plus", "role": "multimodal"}]},
                                       expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertEqual("deepseek", result["id"])
        reg = __import__("json").loads(self.json_path.read_text(encoding="utf-8"))
        stored = next(p for p in reg["providers"] if p["id"] == "deepseek")
        self.assertEqual("LLM_PROVIDER_DEEPSEEK_API_KEY", stored["key_var"])
        self.assertIn("LLM_PROVIDER_DEEPSEEK_API_KEY=sk-deepseek-own",
                      self.env_path.read_text(encoding="utf-8"))

    def test_delete_refuses_while_assigned(self):
        from services.llm_admin import LLMAdminError
        with self.assertRaises(LLMAdminError) as ctx:
            self.la.delete_provider({"id": "deepseek"})
        self.assertIn("assigned", str(ctx.exception))

    def test_delete_unassigned_clears_derived_key(self):
        self.la.save_provider({"name": "Open Router", "base_url": "https://openrouter.ai/api/v1",
                               "api_key": "sk-or",
                               "models": [{"id": "m1", "role": "text"}]},
                              expected_env_mtime=self.env_path.stat().st_mtime)
        self.la.delete_provider({"id": "open-router"})
        self.assertIn("LLM_PROVIDER_OPEN_ROUTER_API_KEY=",
                      self.env_path.read_text(encoding="utf-8"))
        reg = __import__("json").loads(self.json_path.read_text(encoding="utf-8"))
        self.assertNotIn("open-router", {p["id"] for p in reg["providers"]})


class LLMAdminApplyContract(unittest.TestCase):
    def setUp(self):
        import services.llm_admin as la
        self.la = la
        self.tmp = tempfile.TemporaryDirectory()
        self.env_path = Path(self.tmp.name) / ".env"
        self.env_path.write_text(
            "LLM_API_KEY=sk-text\n"
            "LLM_BASE_URL=https://api.deepseek.com\n"
            "LLM_DEFAULT_FLASH=deepseek-v4-flash\n"
            "LLM_EMBED_MODEL=text-embedding-v4\n"
            "LLM_EMBED_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1\n"
            "LLM_VISION=deepseek-v4-flash\n",
            encoding="utf-8",
        )
        self.json_path = Path(self.tmp.name) / "llm_providers.json"
        # Importing config loads the REAL env file into os.environ; blank every
        # slot/provider var so apply tests never see or copy real keys.
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
        patches = [
            unittest.mock.patch.object(la, "active_env_path", return_value=self.env_path),
            unittest.mock.patch.object(la, "_registry_path", return_value=self.json_path),
            unittest.mock.patch.object(la.version_service, "request_graceful_restart", return_value=False),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda: [os.environ.pop(k, None) for k in tuple(os.environ)
                                 if k.startswith("LLM_") or k.startswith("WEB_SEARCH_")])

    def _seed_provider(self):
        """Create a provider through the public API; return its (unique) id."""
        result = self.la.save_provider({"name": "Aliyun DashScope",
                                        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                                        "api_key": "sk-dash",
                                        "models": [{"id": "text-embedding-v4", "role": "embedding"},
                                                   {"id": "same-dim-embedding", "role": "embedding"}]},
                                       expected_env_mtime=self.env_path.stat().st_mtime)
        return result["id"]

    def test_text_save_resolves_provider_into_env_and_process_env(self):
        result = self.la.apply_slot({
            "slot": "text", "provider_id": "deepseek",
            "flash": "deepseek-v4-flash", "think": "",
        }, expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertTrue(result["ok"])
        self.assertFalse(result["satellite_notice"])  # text is web-only
        after = self.env_path.read_text(encoding="utf-8")
        self.assertIn("LLM_BASE_URL=https://api.deepseek.com", after)
        self.assertIn("LLM_DEFAULT_FLASH=deepseek-v4-flash", after)
        self.assertEqual(os.environ.get("LLM_DEFAULT_THINK"), "")
        # The registry records the provider link.
        reg = __import__("json").loads(self.json_path.read_text(encoding="utf-8"))
        self.assertEqual("deepseek", reg["assignments"]["text"]["provider_id"])

    def test_vision_text_mode_clears_dedicated_creds(self):
        result = self.la.apply_slot({
            "slot": "vision", "mode": "text", "model": "deepseek-v4-flash",
        }, expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertTrue(result["ok"])
        after = self.env_path.read_text(encoding="utf-8")
        self.assertIn("LLM_VISION=deepseek-v4-flash", after)
        self.assertIn("LLM_VISION_API_KEY=", after)
        self.assertIn("LLM_VISION_BASE_URL=", after)
        self.assertEqual(os.environ.get("LLM_VISION_API_KEY"), "")

    def test_vision_disable_empties_model(self):
        self.la.apply_slot({"slot": "vision", "mode": "disabled"},
                           expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertIn("LLM_VISION=\n", self.env_path.read_text(encoding="utf-8"))

    def test_unknown_provider_refused(self):
        from services.llm_admin import LLMAdminError
        with self.assertRaises(LLMAdminError):
            self.la.apply_slot({"slot": "text", "provider_id": "ghost", "flash": "m"},
                               expected_env_mtime=self.env_path.stat().st_mtime)

    def test_embed_dimension_mismatch_refused(self):
        from services.llm_admin import LLMAdminError
        pid = self._seed_provider()
        before = self.env_path.read_text(encoding="utf-8")
        with unittest.mock.patch.object(self.la, "probe",
                                        return_value={"ok": True, "dimension": 768,
                                                      "expected": 3072, "dimension_ok": False}):
            with self.assertRaises(LLMAdminError) as ctx:
                self.la.apply_slot({
                    "slot": "embed", "provider_id": pid, "model": "same-dim-embedding",
                }, expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertIn("migration", str(ctx.exception))
        self.assertEqual(before, self.env_path.read_text(encoding="utf-8"))

    def test_embed_dimension_match_writes(self):
        pid = self._seed_provider()
        with unittest.mock.patch.object(self.la, "probe",
                                        return_value={"ok": True, "dimension": 3072,
                                                      "expected": 3072, "dimension_ok": True}):
            result = self.la.apply_slot({
                "slot": "embed", "provider_id": pid, "model": "same-dim-embedding",
            }, expected_env_mtime=self.env_path.stat().st_mtime)
        self.assertTrue(result["ok"])
        self.assertTrue(result["satellite_notice"])  # embedding feeds the workers
        after = self.env_path.read_text(encoding="utf-8")
        self.assertIn("LLM_EMBED_MODEL=same-dim-embedding", after)
        # The provider's stored key is resolved into the slot variable server-side.
        self.assertIn("LLM_EMBED_API_KEY=sk-dash", after)

    def test_embed_probe_failure_refuses_save(self):
        from services.llm_admin import LLMAdminError
        pid = self._seed_provider()
        with unittest.mock.patch.object(self.la, "probe",
                                        return_value={"ok": False, "error": "Endpoint rejected the request: 401"}):
            with self.assertRaises(LLMAdminError):
                self.la.apply_slot({
                    "slot": "embed", "provider_id": pid, "model": "same-dim-embedding",
                }, expected_env_mtime=self.env_path.stat().st_mtime)


class ProbeStateContract(unittest.TestCase):
    """Provider probes classify failures: online / offline / error."""

    def _fake_openai(self, list_result=None, error=None):
        import types
        from openai import APIConnectionError
        def ctor(**kwargs):
            if error is not None:
                raise error
            return types.SimpleNamespace(models=types.SimpleNamespace(list=lambda: list_result))
        return types.SimpleNamespace(OpenAI=ctor, APIConnectionError=APIConnectionError)

    def test_online_state_on_success(self):
        import sys
        import types
        import services.llm_admin as la
        from unittest import mock
        resp = types.SimpleNamespace(data=[types.SimpleNamespace(id="m1")])
        with mock.patch.dict(sys.modules, {"openai": self._fake_openai(list_result=resp)}):
            result = la.probe({"slot": "provider", "base_url": "https://api.deepseek.com",
                               "api_key": "sk-x"})
        self.assertTrue(result["ok"])
        self.assertEqual(result["state"], "online")

    def test_connection_failure_is_offline(self):
        import sys
        import services.llm_admin as la
        from unittest import mock
        from openai import APIConnectionError
        failure = APIConnectionError(message="timeout", request=None)
        with mock.patch.dict(sys.modules, {"openai": self._fake_openai(error=failure)}):
            result = la.probe({"slot": "provider", "base_url": "https://api.deepseek.com",
                               "api_key": "sk-x"})
        self.assertFalse(result["ok"])
        self.assertEqual(result["state"], "offline")

    def test_rejection_is_error(self):
        import sys
        import services.llm_admin as la
        from unittest import mock
        with mock.patch.dict(sys.modules, {"openai": self._fake_openai(error=RuntimeError("401"))}):
            result = la.probe({"slot": "provider", "base_url": "https://api.deepseek.com",
                               "api_key": "sk-bad"})
        self.assertFalse(result["ok"])
        self.assertEqual(result["state"], "error")

    def test_missing_key_is_offline(self):
        import services.llm_admin as la
        result = la.probe({"slot": "provider", "base_url": "https://api.deepseek.com",
                           "api_key": ""})
        self.assertFalse(result["ok"])
        self.assertEqual(result["state"], "offline")


class ProbeSsrfGuardContract(unittest.TestCase):
    def test_internal_addresses_refused(self):
        import web_search
        for url in ("http://127.0.0.1:9/v1", "http://169.254.169.254/latest/meta-data",
                    "http://10.0.0.5/v1", "http://[::1]/v1"):
            with self.subTest(url=url):
                self.assertFalse(web_search.url_targets_public_host(url))

    def test_public_address_allowed(self):
        import unittest.mock as mock
        import web_search
        with mock.patch.object(web_search, "_resolve_public_ips",
                               return_value=("93.184.216.34",)):
            self.assertTrue(web_search.url_targets_public_host("https://api.deepseek.com"))

    def test_probe_refuses_internal_base_url(self):
        import services.llm_admin as la
        result = la.probe({"slot": "provider", "base_url": "http://169.254.169.254/v1",
                           "api_key": "sk-x", "model": "m"})
        self.assertFalse(result["ok"])
        self.assertIn("public address", result["error"])


if __name__ == "__main__":
    unittest.main()
