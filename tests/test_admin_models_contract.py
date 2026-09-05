"""Worker model status, route authorization, and Tavily env-write contracts."""
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

SNAP = {"available": True, "purposes": {}, "embedding_ready": False,
        "env_mtime": 1, "search_configured": True}


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
    def test_partial_and_csrf_contract(self):
        src = (ROOT / "templates/admin_models.html").read_text()
        self.assertEqual(src.splitlines()[0], '{% extends "_bare.html" if partial else "_dashboard_shell.html" %}')
        html = _render(SNAP)
        self.assertIn('name="csrf_token"', html)
        self.assertNotIn('providerModal', html)
        js = (ROOT / "static/js/admin-models-worker.js").read_text()
        self.assertIn('X-CSRFToken', js)
        for element_id in re.findall(r'getElementById\("([^"]+)"\)', js):
            self.assertIn(f'id="{element_id}"', html)

    def test_retired_provider_routes_are_absent(self):
        from flask import Flask
        from routes.llm_admin import register_routes
        app = Flask(__name__)
        register_routes(app)
        with app.test_client() as client:
            for path in ("probe", "providers/save", "providers/delete"):
                self.assertEqual(client.post("/dashboard/admin/models/" + path).status_code, 404)


class AdminModelsRouteContract(unittest.TestCase):
    ROUTES = ("admin_models", "admin_models_save")

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
            "LLM_WORKER_TOKEN=worker-existing\n"
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
        self.la._write_env({"WEB_SEARCH_API_KEY": "search-key"}, expected_mtime=self.mtime())
        after = self.path.read_text(encoding="utf-8")
        self.assertIn("# comment line", after)
        self.assertIn("PAPERQUERY_SECRET=keep-me", after)
        self.assertIn('PAPERQUERY_DATABASE_URL="mysql+pymysql://user:pw@host/db"', after)
        self.assertIn("WEB_SEARCH_PROVIDER=tavily", after)
        self.assertIn("WEB_SEARCH_API_KEY=search-key", after)
        self.assertIn("LLM_WORKER_TOKEN=worker-existing", after)
        self.assertEqual(len(after.splitlines()), len(before.splitlines()) + 1)

    def test_appends_missing_key_and_writes_empty_value(self):
        self.la._write_env({"WEB_SEARCH_API_KEY": ""}, expected_mtime=self.mtime())
        self.assertIn("WEB_SEARCH_API_KEY=\n", self.path.read_text(encoding="utf-8"))

    def test_updated_key_written_unquoted_and_duplicates_collapsed(self):
        self.path.write_text("WEB_SEARCH_API_KEY=old1\nWEB_SEARCH_API_KEY=old2\n", encoding="utf-8")
        self.la._write_env({"WEB_SEARCH_API_KEY": "sk-new"}, expected_mtime=self.mtime())
        self.assertEqual("WEB_SEARCH_API_KEY=sk-new\n", self.path.read_text(encoding="utf-8"))

    def test_write_is_atomic_with_mode_600(self):
        self.la._write_env({"WEB_SEARCH_API_KEY": "sk-new"}, expected_mtime=self.mtime())
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        leftovers = [p for p in self.path.parent.iterdir() if ".tmp-" in p.name]
        self.assertEqual([], leftovers)

    def test_only_tavily_key_is_writable(self):
        for key in ("LLM_PROVIDER_GOOGLE_API_KEY", "LLM_WORKER_TOKEN", "LLM_API_KEY", "PAPERQUERY_SECRET"):
            with self.assertRaises(self.la.LLMAdminError):
                self.la._write_env({key: "evil"})

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
                    self.la._write_env({"WEB_SEARCH_API_KEY": value})

    def test_mtime_conflict_refused_without_writing(self):
        from services.llm_admin import LLMAdminConflict
        before = self.path.read_text(encoding="utf-8")
        stale = self.mtime() - 500
        with self.assertRaises(LLMAdminConflict):
            self.la._write_env({"WEB_SEARCH_API_KEY": "sk-new"}, expected_mtime=stale)
        self.assertEqual(before, self.path.read_text(encoding="utf-8"))
