"""Contract: admin Version & updates page.

Covers the sidebar nav entry, the level-3 gate on every route, the
template/JS DOM id contract, and the partial-rendering convention.
No DB required (AST source lookup + bare-Jinja render).
"""
import re
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

from tests.support import source_of

ROOT = Path(__file__).resolve().parents[1]

INFO = {
    "is_git": True,
    "branch": "main",
    "head_sha": "1bf8bfc971d7b2e58c5f2e3766fdb99dd5e721a0",
    "head_short": "1bf8bfc",
    "dirty": False,
    "status_lines": [],
    "upstream_sha": "abcdef1234567890abcdef1234567890abcdef12",
    "upstream_short": "abcdef1",
    "behind_count": 2,
    "commits": [
        {"sha": "abcdef1234567890abcdef1234567890abcdef12", "short": "abcdef1",
         "author": "Ann", "date": "2026-08-01", "subject": "feat: something new"},
        {"sha": "1234567890abcdef1234567890abcdef12345678", "short": "1234567",
         "author": "Bob", "date": "2026-08-02", "subject": "fix: something else"},
    ],
    "check_error": None,
    "last_update": {"status": "restarting", "previous_sha": "0000000",
                    "target_sha": "1bf8bfc", "finished_at": "2026-08-01T00:00:00+00:00"},
    "running_under_gunicorn": False,
    "update_running": False,
}


def _render(info):
    env = Environment(loader=FileSystemLoader(ROOT / "templates"))
    env.globals["csrf_token"] = lambda: ""
    return env.get_template("admin_version.html").render(
        _=lambda value, **kw: value % kw if kw else value,
        url_for=lambda endpoint, **kw: "/" + endpoint,
        get_flashed_messages=lambda with_categories=False: [],
        request=SimpleNamespace(full_path="/dashboard/admin/version", args={}),
        session={},
        current_locale="en",
        partial=True,
        user=SimpleNamespace(role="3"),
        info=info,
    )


class AdminVersionTemplateContract(unittest.TestCase):
    def test_partial_flag_first_line(self):
        src = (ROOT / "templates" / "admin_version.html").read_text(encoding="utf-8")
        self.assertEqual(
            src.splitlines()[0],
            '{% extends "_bare.html" if partial else "_dashboard_shell.html" %}',
        )

    def test_route_functions_require_level_3(self):
        for name in ("admin_version", "admin_version_update", "admin_version_status"):
            with self.subTest(route=name):
                self.assertIn("require_login(level=3)", source_of(name))

    def test_route_functions_do_not_pass_partial(self):
        # The partial flag comes from the inject_partial_flag context processor;
        # an explicit partial= kwarg would override it (nested-shell bug).
        for name in ("admin_version", "admin_version_update", "admin_version_status"):
            with self.subTest(route=name):
                self.assertNotIn("partial=", source_of(name))

    def test_dom_ids_used_by_js_exist(self):
        html = _render(INFO)
        js = (ROOT / "static" / "js" / "admin-version.js").read_text(encoding="utf-8")
        ids = set(re.findall(r'getElementById\("([^"]+)"\)', js))
        self.assertTrue(ids, "expected the JS to reference elements by id")
        for element_id in ids:
            with self.subTest(element_id=element_id):
                self.assertIn(f'id="{element_id}"', html)

    def test_update_and_status_urls_wired(self):
        html = _render(INFO)
        self.assertIn('data-update-url="/admin_version_update"', html)
        self.assertIn('data-status-url="/admin_version_status"', html)

    def test_update_enabled_when_behind_and_clean(self):
        html = _render(INFO)
        match = re.search(r'<button id="versionUpdateBtn"[^>]*>', html)
        self.assertIsNotNone(match)
        self.assertNotIn("disabled", match.group(0))

    def test_update_disabled_when_dirty(self):
        dirty = dict(INFO, dirty=True, status_lines=[" M app.py"])
        html = _render(dirty)
        match = re.search(r'<button id="versionUpdateBtn"[^>]*>', html)
        self.assertIn("disabled", match.group(0))
        self.assertIn(" M app.py", html)

    def test_update_disabled_when_up_to_date(self):
        current = dict(INFO, behind_count=0, commits=[], upstream_short="1bf8bfc")
        html = _render(current)
        match = re.search(r'<button id="versionUpdateBtn"[^>]*>', html)
        self.assertIn("disabled", match.group(0))
        self.assertIn("Up to date", html)

    def test_incoming_commits_listed(self):
        html = _render(INFO)
        self.assertIn("feat: something new", html)
        self.assertIn("fix: something else", html)
        self.assertIn("abcdef1", html)


class AdminVersionNavContract(unittest.TestCase):
    def test_admin_group_links_version_page(self):
        shell = (ROOT / "templates" / "_dashboard_shell.html").read_text(encoding="utf-8")
        # The Version link lives in the role>=3 Admin group, after Manage users.
        self.assertIn("{{ url_for('admin_version') }}", shell)
        manage_users_pos = shell.index("url_for('admin_users')")
        version_pos = shell.index("url_for('admin_version')")
        self.assertGreater(version_pos, manage_users_pos)
        self.assertIn('data-partial-href="{{ url_for(\'admin_version\') }}"', shell)


class AdminVersionNoGitContract(unittest.TestCase):
    """Deployments without a git binary (e.g. the dev container) must degrade
    to the 'not a git checkout' page state instead of 500-ing."""

    def test_git_helper_reports_missing_binary(self):
        from unittest import mock
        import services.version as v
        with mock.patch.object(v.shutil, "which", return_value=None):
            result = v._git("rev-parse", "HEAD")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("git is not installed", result.stderr)

    def test_snapshot_degrades_when_git_missing(self):
        from unittest import mock
        import services.version as v
        with mock.patch.object(v.shutil, "which", return_value=None):
            info = v.snapshot()
        self.assertFalse(info["is_git"])
        self.assertIn("git is not installed", info["check_error"])
        self.assertEqual(info["behind_count"], 0)

    def test_start_update_refused_when_git_missing(self):
        from unittest import mock
        import services.version as v
        with mock.patch.object(v.shutil, "which", return_value=None):
            ok, message = v.start_update()
        self.assertFalse(ok)
        self.assertIn("git is not installed", message)

    def test_probe_repo_surfaces_real_git_error(self):
        """The page must show the underlying git failure (e.g. dubious
        ownership), never a generic 'not a git checkout' mask."""
        import subprocess
        from unittest import mock
        import services.version as v
        stderr = (
            "fatal: detected dubious ownership in repository at '/Keydion'\n"
            "To add an exception, run: git config --global --add safe.directory /Keydion"
        )
        fake = subprocess.CompletedProcess(["git"], 128, "", stderr)
        with mock.patch.object(v.shutil, "which", return_value="/usr/bin/git"), \
             mock.patch.object(v.subprocess, "run", return_value=fake):
            is_git, detail = v.probe_repo()
        self.assertFalse(is_git)
        self.assertIn("dubious ownership", detail)
        self.assertIn("safe.directory", detail)

        with mock.patch.object(v.shutil, "which", return_value="/usr/bin/git"), \
             mock.patch.object(v.subprocess, "run", return_value=fake):
            info = v.snapshot()
        self.assertIn("dubious ownership", info["check_error"])


class AdminVersionServiceContract(unittest.TestCase):
    def test_service_refuses_dirty_tree_and_running_updates(self):
        src = source_of("start_update")
        self.assertIn("working_tree_status()", src)
        self.assertIn('_run["running"]', src)
        self.assertIn("--ff-only", source_of("_run_update"))

    def test_restart_uses_sighup_under_gunicorn_only(self):
        src = source_of("_schedule_restart")
        self.assertIn('"gunicorn" not in sys.modules', src)
        self.assertIn("signal.SIGHUP", src)
        self.assertIn("os.getppid()", src)


if __name__ == "__main__":
    unittest.main()
