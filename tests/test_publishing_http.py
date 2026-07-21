from __future__ import annotations

import ast
import importlib.util
import os
import stat
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from flask import Flask, get_flashed_messages, session

from routes.publishing_http import (
    actor_from_session,
    lifecycle_error_response,
    lifecycle_from_app,
)
from services.publishing_contracts import (
    Actor,
    AliasConflict,
    DecisionConflict,
    Forbidden,
    IdempotencyConflict,
    InvalidInput,
    NotFound,
    PersistenceFailed,
    StaleVersion,
    StorageFailed,
    SubmissionNotPending,
)


ROOT = Path(__file__).resolve().parents[1]


class PublishingHttpAdapterTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "publishing-http-test"

        @self.app.get("/papers")
        def safe_return():
            return "safe"

        @self.app.get("/papers/<paper_id>/edit")
        def safe_paper_return(paper_id):
            return paper_id

    def test_lifecycle_lookup_returns_exact_installed_fake_and_has_no_fallback(self):
        fake = mock.Mock(name="publishing lifecycle")
        self.app.extensions["publishing_lifecycle"] = fake
        with self.app.app_context():
            self.assertIs(lifecycle_from_app(), fake)

        self.app.extensions.pop("publishing_lifecycle")
        with self.app.app_context(), self.assertRaises(KeyError):
            lifecycle_from_app()

    def test_actor_maps_only_canonical_authenticated_session_values(self):
        for role in ("1", "2", "3"):
            with self.subTest(role=role), self.app.test_request_context("/"):
                session["user"] = {"username": "reader@example.test", "role": role}
                self.assertEqual(
                    actor_from_session(),
                    Actor(user_id="reader@example.test", role=int(role)),
                )

        malformed = (
            None,
            "not-a-mapping",
            {},
            {"username": "", "role": "1"},
            {"username": " " * 3, "role": "1"},
            {"username": " padded ", "role": "1"},
            {"username": "x" * 256, "role": "1"},
            {"username": "reader"},
            {"username": "reader", "role": True},
            {"username": "reader", "role": 2},
            {"username": "reader", "role": 2.0},
            {"username": "reader", "role": "2.0"},
            {"username": "reader", "role": " 2"},
            {"username": "reader", "role": "+2"},
            {"username": "reader", "role": "02"},
            {"username": "reader", "role": "4"},
        )
        for user in malformed:
            with self.subTest(user=user), self.app.test_request_context("/"):
                if user is not None:
                    session["user"] = user
                with self.assertRaises(Forbidden):
                    actor_from_session()

    def test_all_known_failures_have_fixed_json_status_and_code(self):
        cases = (
            (InvalidInput({"title": "is required"}), 422, "invalid_input"),
            (Forbidden("provider-secret"), 403, "forbidden"),
            (NotFound("database-secret"), 404, "not_found"),
            (StaleVersion(4), 409, "stale_version"),
            (SubmissionNotPending("provider-secret"), 409, "submission_not_pending"),
            (DecisionConflict("provider-secret"), 409, "decision_conflict"),
            (IdempotencyConflict("provider-secret"), 409, "idempotency_conflict"),
            (AliasConflict("secret-paper-name"), 409, "alias_conflict"),
            (StorageFailed("storage-secret"), 503, "storage_failed"),
            (PersistenceFailed("database-password=sentinel"), 503, "persistence_failed"),
        )
        for error, expected_status, expected_code in cases:
            error.__cause__ = RuntimeError("cause-secret=sentinel")
            error.provider_detail = "provider-detail=sentinel"
            with self.subTest(error=type(error).__name__), self.app.test_request_context(
                "/papers", method="POST", json={}
            ):
                response, status = lifecycle_error_response(
                    error,
                    redirect_endpoint="safe_return",
                )
                payload = response.get_json()

            self.assertEqual(status, expected_status)
            self.assertEqual(payload["error"]["code"], expected_code)
            self.assertIsInstance(payload["error"]["message"], str)
            serialized = response.get_data(as_text=True)
            self.assertNotIn("sentinel", serialized)
            self.assertNotIn("secret", serialized)

    def test_invalid_input_copies_field_errors_without_stringifying_objects(self):
        error = InvalidInput({"title": "is required", "language": "is unsupported"})
        with self.app.test_request_context("/papers", method="POST", json={}):
            response, status = lifecycle_error_response(
                error,
                redirect_endpoint="safe_return",
            )

        self.assertEqual(status, 422)
        self.assertEqual(
            response.get_json()["error"]["field_errors"],
            {"title": "is required", "language": "is unsupported"},
        )

    def test_unknown_programming_error_is_not_mislabeled(self):
        error = RuntimeError("programming-error-secret")
        with self.app.test_request_context("/papers", method="POST", json={}):
            with self.assertRaises(RuntimeError) as raised:
                lifecycle_error_response(error, redirect_endpoint="safe_return")
        self.assertIs(raised.exception, error)

    def test_stale_json_response_is_exact_and_redacted(self):
        error = StaleVersion(4)
        error.__cause__ = RuntimeError("database-password=sentinel")
        with self.app.test_request_context(
            "/edit", method="POST", json={"title": "changed"}
        ):
            response, status = lifecycle_error_response(
                error,
                redirect_endpoint="safe_return",
            )

        self.assertEqual(status, 409)
        self.assertEqual(
            response.get_json(),
            {
                "error": {
                    "code": "stale_version",
                    "message": (
                        "This paper changed while you were editing it. "
                        "Reload and try again."
                    ),
                    "current_version": 4,
                }
            },
        )
        self.assertNotIn("sentinel", response.get_data(as_text=True))

    def test_json_negotiation_requires_an_explicit_signal(self):
        cases = (
            ({"method": "POST", "json": {}}, True),
            ({"method": "POST", "headers": {"Accept": "application/json"}}, True),
            ({
                "method": "POST",
                "headers": {"Accept": "application/json;q=0.9, text/html;q=0.1"},
            }, True),
            ({
                "method": "POST",
                "data": {"title": "paper"},
                "content_type": "multipart/form-data",
                "headers": {"X-Requested-With": "XMLHttpRequest"},
            }, True),
            ({"method": "POST", "data": {"title": "paper"}}, False),
            ({"method": "POST", "headers": {"Accept": "*/*"}}, False),
            ({
                "method": "POST",
                "headers": {"Accept": "application/json, text/html"},
            }, False),
            ({
                "method": "POST",
                "headers": {"Accept": "application/json;q=0.1, text/html;q=0.9"},
            }, False),
        )
        for request_kwargs, expect_json in cases:
            with self.subTest(request_kwargs=request_kwargs), self.app.test_request_context(
                "/papers", **request_kwargs
            ):
                result = lifecycle_error_response(
                    Forbidden("do-not-leak"),
                    redirect_endpoint="safe_return",
                )
                if expect_json:
                    response, status = result
                    self.assertEqual(status, 403)
                    self.assertTrue(response.is_json)
                else:
                    self.assertEqual(result.status_code, 303)
                    self.assertEqual(result.headers["Location"], "/papers")

    def test_html_response_uses_fixed_flash_and_server_chosen_local_endpoint(self):
        error = StorageFailed("storage-secret")
        error.__cause__ = RuntimeError("provider-secret")
        with self.app.test_request_context(
            "/papers?next=https://evil.example/phish",
            method="POST",
            data={"next": "//evil.example/phish"},
            headers={"Referer": "https://evil.example/phish", "Accept": "text/html"},
        ):
            response = lifecycle_error_response(
                error,
                redirect_endpoint="safe_return",
            )
            flashed = get_flashed_messages(with_categories=True)

        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["Location"], "/papers")
        self.assertEqual(
            flashed,
            [("danger", "Publishing is temporarily unavailable. Please try again.")],
        )
        self.assertNotIn("evil.example", response.headers["Location"])
        self.assertNotIn("secret", repr(flashed))

    def test_contextual_redirect_uses_validated_encoded_route_values(self):
        unsafe_value = "paper name?next=evil.example"
        with self.app.test_request_context("/papers", method="POST"):
            response = lifecycle_error_response(
                Forbidden(),
                redirect_endpoint="safe_paper_return",
                redirect_values={"paper_id": unsafe_value},
            )

        self.assertEqual(response.status_code, 303)
        self.assertEqual(
            response.headers["Location"],
            "/papers/paper%20name%3Fnext=evil.example/edit",
        )

    def test_html_renderer_receives_only_sanitized_structured_failure(self):
        captured = []
        form_context = {"idempotency_key": "route-owned-key"}

        def rerender(error_payload, status):
            captured.append((error_payload, status, form_context.copy()))
            return "rerendered", status

        error = InvalidInput({"title": "is required"})
        error.__cause__ = RuntimeError("database-secret")
        with self.app.test_request_context("/papers", method="POST"):
            response = lifecycle_error_response(error, html_renderer=rerender)
            flashed = get_flashed_messages()

        self.assertEqual(response, ("rerendered", 422))
        self.assertEqual(
            captured,
            [(
                {
                    "code": "invalid_input",
                    "message": "Please correct the highlighted fields.",
                    "field_errors": {"title": "is required"},
                },
                422,
                {"idempotency_key": "route-owned-key"},
            )],
        )
        self.assertEqual(flashed, [])
        self.assertNotIn("secret", repr(captured))

    def test_absolute_string_and_special_values_cannot_select_redirect_target(self):
        with self.app.test_request_context("/papers", method="POST"):
            with self.assertRaises(ValueError):
                lifecycle_error_response(
                    Forbidden(),
                    redirect_endpoint="https://evil.example/phish",
                )
            with self.assertRaises(ValueError):
                lifecycle_error_response(
                    Forbidden(),
                    redirect_endpoint="safe_return",
                    redirect_values={"_external": True},
                )
            self.app.url_build_error_handlers.append(
                lambda _error, _endpoint, _values: "https://evil.example/phish"
            )
            with self.assertRaises(ValueError):
                lifecycle_error_response(
                    Forbidden(),
                    redirect_endpoint="missing_endpoint",
                )
            self.assertEqual(get_flashed_messages(), [])


class PublishingRootPreparationTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)

    @staticmethod
    def _prepare(path):
        from services.publishing_wiring import _prepare_private_storage_root

        return _prepare_private_storage_root(path)

    def test_fresh_root_is_created_with_exact_private_mode(self):
        from services import publishing_wiring

        papers = self.root / "papers"
        synced = []
        real_fsync = os.fsync

        def record_fsync(descriptor):
            info = os.fstat(descriptor)
            synced.append((info.st_dev, info.st_ino))
            return real_fsync(descriptor)

        with mock.patch.object(
            publishing_wiring.os,
            "fsync",
            side_effect=record_fsync,
        ):
            prepared = self._prepare(papers)

        self.assertEqual(prepared, papers.absolute())
        self.assertEqual(stat.S_IMODE(papers.stat().st_mode), 0o700)
        self.assertIn(
            (papers.stat().st_dev, papers.stat().st_ino),
            synced,
        )
        self.assertIn(
            (papers.parent.stat().st_dev, papers.parent.stat().st_ino),
            synced,
        )

    def test_blank_configured_paths_use_defaults_without_targeting_cwd(self):
        working_directory = self.root / "process-cwd"
        working_directory.mkdir(mode=0o755)
        working_directory.chmod(0o755)
        before_mode = stat.S_IMODE(working_directory.stat().st_mode)
        env = os.environ.copy()
        env.update(
            {
                "KEYDION_TEST_ROOT": str(ROOT),
                "PAPERQUERY_DATA_DIR": "",
                "PAPERQUERY_UPLOAD_DIR": "",
                "PAPERQUERY_RESOURCES_DIR": "",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        script = """
import os
import sys
from pathlib import Path

sys.path.insert(0, os.environ["KEYDION_TEST_ROOT"])
import config

assert config.DATA_DIR == config.BASE_DIR / "data", config.DATA_DIR
assert config.PAPERS_DIR == config.BASE_DIR / "papers", config.PAPERS_DIR
assert config.RESOURCES_DIR == config.BASE_DIR / "resource_files", config.RESOURCES_DIR
assert config.PAPERS_DIR != Path.cwd()
"""

        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=working_directory,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(stat.S_IMODE(working_directory.stat().st_mode), before_mode)

    def test_existing_app_owned_root_is_tightened_through_descriptor(self):
        papers = self.root / "papers"
        papers.mkdir(mode=0o755)
        papers.chmod(0o755)

        self._prepare(papers)

        self.assertEqual(stat.S_IMODE(papers.stat().st_mode), 0o700)

    def test_symlink_and_non_directory_are_rejected_without_mutation(self):
        from services.paper_storage import StorageError

        outside = self.root / "outside"
        outside.mkdir(mode=0o755)
        outside.chmod(0o755)
        link = self.root / "papers-link"
        link.symlink_to(outside, target_is_directory=True)
        regular_file = self.root / "not-a-directory"
        regular_file.write_text("do not alter", encoding="utf-8")
        file_mode = stat.S_IMODE(regular_file.stat().st_mode)

        with self.assertRaises(StorageError):
            self._prepare(link)
        with self.assertRaises(StorageError):
            self._prepare(regular_file)

        self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o755)
        self.assertEqual(regular_file.read_text(encoding="utf-8"), "do not alter")
        self.assertEqual(stat.S_IMODE(regular_file.stat().st_mode), file_mode)

    def test_foreign_owner_is_rejected_before_permissions_change(self):
        from services import publishing_wiring
        from services.paper_storage import StorageError

        papers = self.root / "papers"
        papers.mkdir(mode=0o755)
        papers.chmod(0o755)
        with mock.patch.object(
            publishing_wiring.os,
            "getuid",
            return_value=os.getuid() + 1,
        ):
            with self.assertRaises(StorageError):
                self._prepare(papers)

        self.assertEqual(stat.S_IMODE(papers.stat().st_mode), 0o755)

    def test_path_replacement_is_rejected_before_either_entry_is_tightened(self):
        from services import publishing_wiring
        from services.paper_storage import StorageError

        papers = self.root / "papers"
        moved = self.root / "moved-original"
        papers.mkdir(mode=0o755)
        papers.chmod(0o755)

        def replace_opened_path(path, _descriptor):
            path.rename(moved)
            path.mkdir(mode=0o755)
            path.chmod(0o755)

        with mock.patch.object(
            publishing_wiring,
            "_after_private_storage_root_opened",
            side_effect=replace_opened_path,
        ):
            with self.assertRaises(StorageError):
                self._prepare(papers)

        self.assertEqual(stat.S_IMODE(papers.stat().st_mode), 0o755)
        self.assertEqual(stat.S_IMODE(moved.stat().st_mode), 0o755)

    def test_configured_symlink_spelling_reaches_safety_check_unchanged(self):
        outside = self.root / "outside"
        outside.mkdir(mode=0o755)
        outside.chmod(0o755)
        configured = self.root / "configured-papers"
        configured.symlink_to(outside, target_is_directory=True)
        env = os.environ.copy()
        env.update(
            {
                "PAPERQUERY_UPLOAD_DIR": str(configured),
                "PAPERQUERY_DATA_DIR": str(self.root / "data"),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        script = """
import os
from pathlib import Path
import config
from services.paper_storage import StorageError
from services.publishing_wiring import _prepare_private_storage_root

expected = Path(os.path.abspath(os.environ["PAPERQUERY_UPLOAD_DIR"]))
assert config.PAPERS_DIR == expected, (config.PAPERS_DIR, expected)
try:
    _prepare_private_storage_root(config.PAPERS_DIR)
except StorageError:
    pass
else:
    raise AssertionError("configured symlink was accepted")
"""

        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o755)


class PublishingConstructionTest(unittest.TestCase):
    def test_same_or_nested_roots_are_rejected_before_any_mode_changes(self):
        from services import publishing_wiring
        from services.paper_storage import StorageError

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases = (
                ("same", root / "same", root / "same"),
                (
                    "papers-parent",
                    root / "papers-parent",
                    root / "papers-parent" / "pending",
                ),
                (
                    "pending-parent",
                    root / "pending-parent" / "papers",
                    root / "pending-parent",
                ),
            )
            for name, papers, pending in cases:
                with self.subTest(name=name):
                    papers.mkdir(parents=True, exist_ok=True)
                    pending.mkdir(parents=True, exist_ok=True)
                    for path in {papers, pending}:
                        path.chmod(0o755)
                    before = {
                        path: stat.S_IMODE(path.stat().st_mode)
                        for path in {papers, pending}
                    }

                    with mock.patch("config.PAPERS_DIR", papers), mock.patch(
                        "config.PENDING_PAPERS_DIR", pending
                    ):
                        with self.assertRaises(StorageError):
                            publishing_wiring._build_lifecycle(
                                mock.sentinel.session_factory
                            )

                    self.assertEqual(
                        {
                            path: stat.S_IMODE(path.stat().st_mode)
                            for path in {papers, pending}
                        },
                        before,
                    )

    def test_lifecycle_prepares_both_roots_before_constructing_storage(self):
        from services import publishing_wiring

        papers = Path("configured-papers")
        pending = Path("configured-pending")
        prepared_papers = Path("prepared-papers")
        prepared_pending = Path("prepared-pending")
        storage = mock.sentinel.storage
        lifecycle = mock.sentinel.lifecycle
        session_factory = mock.sentinel.session_factory

        with mock.patch("config.PAPERS_DIR", papers), mock.patch(
            "config.PENDING_PAPERS_DIR", pending
        ), mock.patch.object(
            publishing_wiring,
            "_prepare_private_storage_root",
            side_effect=(prepared_papers, prepared_pending),
        ) as prepare, mock.patch.object(
            publishing_wiring,
            "PaperStorage",
            return_value=storage,
        ) as storage_factory, mock.patch.object(
            publishing_wiring,
            "StrictRagAdapter",
            return_value=mock.sentinel.indexer,
        ), mock.patch.object(
            publishing_wiring,
            "PublishingLifecycle",
            return_value=lifecycle,
        ):
            result = publishing_wiring._build_lifecycle(session_factory)

        self.assertIs(result, lifecycle)
        self.assertEqual(prepare.call_args_list, [mock.call(papers), mock.call(pending)])
        storage_factory.assert_called_once_with(prepared_papers, prepared_pending)

    def test_app_factory_starts_with_private_roots_and_one_lifecycle_per_app(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = os.environ.copy()
            env.update(
                {
                    "PAPERQUERY_SECRET": "publishing-factory-test",
                    "PAPERQUERY_COOKIE_SECURE": "0",
                    "PAPERQUERY_DATABASE_URL": f"sqlite:///{root / 'app.sqlite'}",
                    "PAPERQUERY_DATA_DIR": str(root / "data"),
                    "PAPERQUERY_UPLOAD_DIR": str(root / "papers"),
                    "PYTHONDONTWRITEBYTECODE": "1",
                }
            )
            script = """
import stat
from unittest import mock

import app as app_module
from config import PAPERS_DIR, PENDING_PAPERS_DIR
from services import publishing_wiring

assert "publishing_lifecycle" in app_module.app.extensions
assert stat.S_IMODE(PAPERS_DIR.stat().st_mode) == 0o700
assert stat.S_IMODE(PENDING_PAPERS_DIR.stat().st_mode) == 0o700

events = []
lifecycles = []
def build_lifecycle():
    events.append("build")
    lifecycle = object()
    lifecycles.append(lifecycle)
    return lifecycle

with mock.patch.object(app_module, "init_db", side_effect=lambda: events.append("db")), \
     mock.patch.object(app_module, "configure_rag", side_effect=lambda: events.append("rag")), \
     mock.patch.object(publishing_wiring, "build_publishing_lifecycle", side_effect=build_lifecycle):
    first = app_module.create_app()
    second = app_module.create_app()

assert events == ["db", "rag", "build", "db", "rag", "build"], events
assert first.extensions["publishing_lifecycle"] is lifecycles[0]
assert second.extensions["publishing_lifecycle"] is lifecycles[1]
assert lifecycles[0] is not lifecycles[1]
"""

            completed = subprocess.run(
                [sys.executable, "-c", script],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_web_factory_has_no_worker_or_thread_and_no_default_root_mkdir(self):
        source = (ROOT / "app.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        create_app = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "create_app"
        )
        create_source = ast.get_source_segment(source, create_app)

        self.assertNotIn("PAPERS_DIR.mkdir", create_source)
        self.assertNotIn("PENDING_PAPERS_DIR.mkdir", create_source)
        self.assertNotIn("build_publishing_worker", create_source)
        self.assertNotIn("Thread", create_source)
        self.assertNotIn(".start(", create_source)
        self.assertIn('app.extensions["publishing_lifecycle"]', create_source)


class GunicornPublishingSafetyTest(unittest.TestCase):
    @staticmethod
    def _load_config():
        path = ROOT / "gunicorn.conf.py"
        spec = importlib.util.spec_from_file_location("gunicorn_task12_test", path)
        if spec is None or spec.loader is None:
            raise AssertionError("could not load gunicorn configuration")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_web_app_is_not_preloaded_and_post_fork_reuses_served_app(self):
        source = (ROOT / "gunicorn.conf.py").read_text(encoding="utf-8")
        module = self._load_config()

        self.assertIs(module.preload_app, False)
        post_fork_source = ast.get_source_segment(
            source,
            next(
                node
                for node in ast.parse(source).body
                if isinstance(node, ast.FunctionDef) and node.name == "post_fork"
            ),
        )
        self.assertNotIn("create_app", post_fork_source)
        self.assertNotIn("build_publishing_worker", post_fork_source)
        self.assertNotIn("Thread", post_fork_source)
        self.assertIn("app_module.app.app_context()", post_fork_source)

    def test_production_entrypoints_serve_the_module_app_singleton(self):
        for relative_path in (
            "run_prod.sh",
            "Dockerfile",
            "docker-compose.prod.yml",
        ):
            with self.subTest(path=relative_path):
                source = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertNotIn("app:create_app()", source)
                self.assertIn("app:app", source)

    def test_post_fork_disposes_inherited_engine_then_warms_served_app(self):
        module = self._load_config()
        events = []

        class Context:
            def __enter__(self):
                events.append("context-enter")

            def __exit__(self, *_args):
                events.append("context-exit")

        engine = types.SimpleNamespace(dispose=lambda: events.append("dispose"))
        fake_db = types.ModuleType("db")
        fake_db.get_engine = lambda: engine
        fake_app = types.ModuleType("app")
        fake_app.app = types.SimpleNamespace(
            app_context=lambda: (events.append("served-app-context") or Context())
        )
        fake_app.rag_index = types.SimpleNamespace(warm=lambda: events.append("warm"))
        worker = types.SimpleNamespace(
            log=types.SimpleNamespace(exception=lambda *_args: events.append("logged"))
        )

        with mock.patch.dict(sys.modules, {"db": fake_db, "app": fake_app}):
            module.post_fork(mock.sentinel.server, worker)

        self.assertEqual(
            events,
            ["dispose", "served-app-context", "context-enter", "warm", "context-exit"],
        )


if __name__ == "__main__":
    unittest.main()
