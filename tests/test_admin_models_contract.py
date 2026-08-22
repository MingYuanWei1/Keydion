"""Contract: admin AI-models control panel.

Covers the sidebar nav entry, the level-3 gate on every route, the
template/JS DOM id contract, the partial-rendering convention, the write-only
key posture (no key material ever rendered), env-file injection validation,
the embedding dimension gate, and the probe SSRF guard.
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
    "env_writable": True,
    "embed_dim": 3072,
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

    def test_probe_and_save_urls_wired(self):
        html = _render(SNAP)
        self.assertIn('data-probe-url="/admin_models_probe"', html)
        self.assertIn('data-save-url="/admin_models_save"', html)

    def test_no_key_material_rendered(self):
        """Keys are write-only: configured slots show status text, never values,
        and password inputs never carry a value attribute."""
        html = _render(SNAP)
        self.assertIsNone(re.search(r"sk-[A-Za-z0-9]{8,}", html), "DeepSeek-style key leaked")
        self.assertIsNone(re.search(r"tvly-[A-Za-z0-9]{8,}", html), "Tavily key leaked")
        for field in re.finditer(r'<input[^>]*type="password"[^>]*>', html):
            self.assertNotIn("value=", field.group(0), field.group(0))
        # Configured slots say so via status labels, not stored secrets.
        self.assertIn("Configured", html)

    def test_capability_strip_pills_present(self):
        html = _render(SNAP)
        for pill in ("capAsk", "capSemantic", "capVision", "capWeb"):
            self.assertIn(f'id="{pill}"', html)
        self.assertIn("deepseek-v4-pro", html)
        self.assertIn("qwen3.7-plus", html)

    def test_vision_three_state_options(self):
        html = _render(SNAP)
        for option in ('value="text"', 'value="dedicated"', 'value="disabled"'):
            self.assertIn(option, html)

    def test_embed_dimension_visible(self):
        html = _render(SNAP)
        self.assertIn("VECTOR(3072)", html)


class AdminModelsRouteContract(unittest.TestCase):
    def test_route_functions_require_level_3(self):
        for name in ("admin_models", "admin_models_probe", "admin_models_save"):
            with self.subTest(route=name):
                self.assertIn("require_login(level=3)", source_of(name))

    def test_route_functions_do_not_pass_partial(self):
        for name in ("admin_models", "admin_models_probe", "admin_models_save"):
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

    def test_unknown_lines_preserved_byte_identical(self):
        before = self.path.read_text(encoding="utf-8")
        self.la._write_env({"LLM_BASE_URL": "https://api.deepseek.com"}, expected_mtime=self.mtime())
        after = self.path.read_text(encoding="utf-8")
        self.assertIn("# comment line", after)
        self.assertIn("PAPERQUERY_SECRET=keep-me", after)
        self.assertIn('PAPERQUERY_DATABASE_URL="mysql+pymysql://user:pw@host/db"', after)
        self.assertIn("WEB_SEARCH_PROVIDER=tavily", after)
        self.assertIn("LLM_BASE_URL=https://api.deepseek.com", after)
        self.assertIn("LLM_API_KEY=sk-existing", after)
        # Nothing removed except the one updated key's old value.
        self.assertEqual(len(after.splitlines()), len(before.splitlines()) + 1)

    def test_appends_missing_key_and_writes_empty_value(self):
        self.la._write_env({"LLM_VISION": ""}, expected_mtime=self.mtime())
        after = self.path.read_text(encoding="utf-8")
        self.assertIn("LLM_VISION=\n", after)

    def test_updated_key_written_unquoted_and_duplicates_collapsed(self):
        self.path.write_text("LLM_API_KEY=old1\nLLM_API_KEY=old2\n", encoding="utf-8")
        self.la._write_env({"LLM_API_KEY": "sk-new"}, expected_mtime=self.mtime())
        after = self.path.read_text(encoding="utf-8")
        self.assertEqual(after, "LLM_API_KEY=sk-new\n")

    def test_write_is_atomic_with_mode_600(self):
        self.la._write_env({"LLM_API_KEY": "sk-new"}, expected_mtime=self.mtime())
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        leftovers = [p for p in self.path.parent.iterdir() if ".tmp-" in p.name]
        self.assertEqual([], leftovers)

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

    def test_non_whitelisted_key_refused(self):
        from services.llm_admin import LLMAdminError
        with self.assertRaises(LLMAdminError):
            self.la._write_env({"PAPERQUERY_SECRET": "evil"})

    def test_mtime_conflict_refused_without_writing(self):
        from services.llm_admin import LLMAdminConflict
        before = self.path.read_text(encoding="utf-8")
        stale = self.mtime() - 500
        with self.assertRaises(LLMAdminConflict):
            self.la._write_env({"LLM_API_KEY": "sk-new"}, expected_mtime=stale)
        self.assertEqual(before, self.path.read_text(encoding="utf-8"))


class LLMAdminSnapshotContract(unittest.TestCase):
    def setUp(self):
        import services.llm_admin as la
        self.la = la
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / ".env.prod"
        self.path.write_text(
            "LLM_API_KEY=sk-text-secret\n"
            "LLM_BASE_URL=https://api.deepseek.com\n"
            "LLM_DEFAULT_FLASH=deepseek-v4-flash\n"
            "LLM_DEFAULT_THINK=\n"
            "LLM_EMBED_MODEL=text-embedding-v4\n"
            "LLM_VISION=qwen3.7-plus\n"
            "WEB_SEARCH_API_KEY=tvly-search-secret\n",
            encoding="utf-8",
        )
        # Importing config loads the REAL env file into os.environ (override=True),
        # so blank every slot var to keep snapshot tests independent of the host.
        env = {key: "" for key in (
            "LLM_API_KEY", "LLM_BASE_URL", "LLM_DEFAULT_FLASH", "LLM_DEFAULT_THINK",
            "LLM_EMBED_API_KEY", "LLM_EMBED_BASE_URL", "LLM_EMBED_MODEL",
            "LLM_VISION", "LLM_VISION_API_KEY", "LLM_VISION_BASE_URL",
            "WEB_SEARCH_PROVIDER", "WEB_SEARCH_API_KEY",
        )}
        import unittest.mock as mock
        env_patch = mock.patch.dict(os.environ, env, clear=False)
        env_patch.start()
        self.addCleanup(env_patch.stop)
        self._patch = unittest.mock.patch.object(la, "active_env_path", return_value=self.path)
        self._patch.start()
        self.addCleanup(self._patch.stop)
        self.addCleanup(self.tmp.cleanup)

    def test_snapshot_never_contains_key_material(self):
        snap = self.la.snapshot()
        blob = repr(snap)
        self.assertNotIn("sk-text-secret", blob)
        self.assertNotIn("tvly-search-secret", blob)
        self.assertTrue(snap["slots"]["text"]["key_set"])
        self.assertTrue(snap["slots"]["search"]["key_set"])

    def test_vision_mode_and_defaults(self):
        snap = self.la.snapshot()
        # LLM_VISION set, dedicated creds unset → falls back to chat creds.
        self.assertEqual(snap["slots"]["vision"]["mode"], "text")
        self.assertTrue(snap["slots"]["vision"]["uses_text_key"])
        # Empty think follows flash.
        self.assertTrue(snap["slots"]["text"]["think_follows_flash"])
        self.assertEqual(snap["slots"]["text"]["think"], snap["slots"]["text"]["flash"])

    def test_vision_dedicated_when_own_creds_set(self):
        self.path.write_text("LLM_VISION=qwen\nLLM_VISION_API_KEY=sk-v\n", encoding="utf-8")
        snap = self.la.snapshot()
        self.assertEqual(snap["slots"]["vision"]["mode"], "dedicated")

    def test_vision_disabled_when_model_empty(self):
        self.path.write_text("LLM_VISION=\n", encoding="utf-8")
        snap = self.la.snapshot()
        self.assertEqual(snap["slots"]["vision"]["mode"], "disabled")
        self.assertFalse(snap["features"]["vision_first"]["on"])


class LLMAdminApplyContract(unittest.TestCase):
    def setUp(self):
        import services.llm_admin as la
        self.la = la
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / ".env"
        self.path.write_text(
            "LLM_API_KEY=sk-text\n"
            "LLM_BASE_URL=https://api.deepseek.com\n"
            "LLM_EMBED_MODEL=text-embedding-v4\n"
            "LLM_EMBED_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1\n"
            "LLM_VISION=qwen3.7-plus\n",
            encoding="utf-8",
        )
        import unittest.mock as mock
        patches = [
            mock.patch.object(la, "active_env_path", return_value=self.path),
            mock.patch.object(la.version_service, "request_graceful_restart", return_value=False),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        env_patch = mock.patch.dict(os.environ, {}, clear=False)
        env_patch.start()
        self.addCleanup(env_patch.stop)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(lambda: [os.environ.pop(k, None) for k in
                                 ("LLM_EMBED_MODEL", "LLM_VISION", "LLM_VISION_API_KEY",
                                  "LLM_VISION_BASE_URL", "LLM_DEFAULT_FLASH", "LLM_DEFAULT_THINK",
                                  "LLM_BASE_URL")])

    def test_text_save_updates_file_and_process_env(self):
        result = self.la.apply_slot({
            "slot": "text", "base_url": "https://api.deepseek.com",
            "flash": "deepseek-v4-flash", "think": "", "api_key": "",
        }, expected_mtime=self.path.stat().st_mtime)
        self.assertTrue(result["ok"])
        self.assertFalse(result["satellite_notice"])  # text is web-only
        self.assertIn("LLM_DEFAULT_FLASH=deepseek-v4-flash", self.path.read_text(encoding="utf-8"))
        self.assertEqual(os.environ.get("LLM_DEFAULT_FLASH"), "deepseek-v4-flash")
        self.assertEqual(os.environ.get("LLM_DEFAULT_THINK"), "")

    def test_vision_text_mode_clears_dedicated_creds(self):
        result = self.la.apply_slot({
            "slot": "vision", "mode": "text", "model": "deepseek-v4-pro",
        }, expected_mtime=self.path.stat().st_mtime)
        self.assertTrue(result["ok"])
        after = self.path.read_text(encoding="utf-8")
        self.assertIn("LLM_VISION=deepseek-v4-pro", after)
        self.assertIn("LLM_VISION_API_KEY=", after)
        self.assertIn("LLM_VISION_BASE_URL=", after)
        self.assertEqual(os.environ.get("LLM_VISION_API_KEY"), "")

    def test_vision_disable_empties_model(self):
        self.la.apply_slot({"slot": "vision", "mode": "disabled"},
                           expected_mtime=self.path.stat().st_mtime)
        self.assertIn("LLM_VISION=\n", self.path.read_text(encoding="utf-8"))

    def test_embed_dimension_mismatch_refused(self):
        from services.llm_admin import LLMAdminError
        import unittest.mock as mock
        before = self.path.read_text(encoding="utf-8")
        with mock.patch.object(self.la, "probe",
                               return_value={"ok": True, "dimension": 768,
                                             "expected": 3072, "dimension_ok": False}):
            with self.assertRaises(LLMAdminError) as ctx:
                self.la.apply_slot({
                    "slot": "embed", "model": "other-embedding",
                    "base_url": "", "api_key": "",
                }, expected_mtime=self.path.stat().st_mtime)
        self.assertIn("migration", str(ctx.exception))
        self.assertEqual(before, self.path.read_text(encoding="utf-8"))

    def test_embed_dimension_match_writes(self):
        import unittest.mock as mock
        with mock.patch.object(self.la, "probe",
                               return_value={"ok": True, "dimension": 3072,
                                             "expected": 3072, "dimension_ok": True}):
            result = self.la.apply_slot({
                "slot": "embed", "model": "same-dim-embedding",
                "base_url": "", "api_key": "",
            }, expected_mtime=self.path.stat().st_mtime)
        self.assertTrue(result["ok"])
        self.assertTrue(result["satellite_notice"])  # embedding feeds the workers
        self.assertIn("LLM_EMBED_MODEL=same-dim-embedding", self.path.read_text(encoding="utf-8"))

    def test_embed_probe_failure_refuses_save(self):
        from services.llm_admin import LLMAdminError
        import unittest.mock as mock
        with mock.patch.object(self.la, "probe",
                               return_value={"ok": False, "error": "Endpoint rejected the request: 401"}):
            with self.assertRaises(LLMAdminError):
                self.la.apply_slot({
                    "slot": "embed", "model": "new-model", "base_url": "", "api_key": "",
                }, expected_mtime=self.path.stat().st_mtime)


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
        result = la.probe({"slot": "text", "base_url": "http://169.254.169.254/v1",
                           "api_key": "sk-x", "model": "m"})
        self.assertFalse(result["ok"])
        self.assertIn("public address", result["error"])


if __name__ == "__main__":
    unittest.main()