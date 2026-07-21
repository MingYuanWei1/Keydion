"""Standalone publishing-worker command behavior and import-boundary tests."""

from __future__ import annotations

import contextlib
import io
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.publishing_jobs import JobStatus
from tools import publishing_worker


class _FakeWorker:
    def __init__(self):
        self.calls: list[str] = []
        self.poll_seconds = 5
        self._run_hook = None
        self._reconcile_hook = None

    def status(self):
        self.calls.append("status")
        return JobStatus(pending=2, running=1, oldest_age_seconds=91)

    def reconcile(self):
        self.calls.append("reconcile")
        if self._reconcile_hook is not None:
            return self._reconcile_hook()
        return 0

    def run_one(self):
        self.calls.append("run_one")
        if self._run_hook is not None:
            return self._run_hook()
        return None


class PublishingWorkerCliTests(unittest.TestCase):
    def run_main(self, argv, worker):
        stdout = io.StringIO()
        with mock.patch.object(publishing_worker, "build_worker", return_value=worker):
            with contextlib.redirect_stdout(stdout):
                code = publishing_worker.main(argv)
        return code, stdout.getvalue()

    def test_once_reconciles_attempts_at_most_one_job_and_exits_zero(self):
        worker = _FakeWorker()
        code, stdout = self.run_main(["--once"], worker)
        self.assertEqual(code, 0)
        self.assertEqual(stdout, "")
        self.assertEqual(worker.calls, ["reconcile", "run_one"])

    def test_once_still_attempts_job_when_reconciliation_raises(self):
        worker = _FakeWorker()
        worker._reconcile_hook = lambda: (_ for _ in ()).throw(
            RuntimeError("Bearer do-not-log")
        )
        code, _ = self.run_main(["--once"], worker)
        self.assertEqual(code, 0)
        self.assertEqual(worker.calls, ["reconcile", "run_one"])

    def test_status_is_exact_and_never_claims_or_reconciles(self):
        worker = _FakeWorker()
        code, stdout = self.run_main(["--status"], worker)
        self.assertEqual(code, 0)
        self.assertEqual(stdout, "pending=2 running=1 oldest_age_seconds=91\n")
        self.assertEqual(worker.calls, ["status"])

    def test_status_prints_none_for_empty_queue_age(self):
        worker = _FakeWorker()
        worker.status = mock.Mock(
            return_value=JobStatus(pending=0, running=0, oldest_age_seconds=None)
        )
        code, stdout = self.run_main(["--status"], worker)
        self.assertEqual(code, 0)
        self.assertEqual(stdout, "pending=0 running=0 oldest_age_seconds=none\n")

    def test_default_loop_uses_five_second_poll_interval(self):
        worker = _FakeWorker()
        handlers = {}
        sleeps = []

        def install(sig, handler):
            handlers[sig] = handler

        def sleep(seconds):
            sleeps.append(seconds)
            handlers[signal.SIGTERM](signal.SIGTERM, None)

        with mock.patch.object(publishing_worker, "build_worker", return_value=worker):
            with mock.patch.object(publishing_worker.signal, "signal", side_effect=install):
                with mock.patch.object(publishing_worker.time, "sleep", side_effect=sleep):
                    self.assertEqual(publishing_worker.main([]), 0)
        self.assertEqual(sleeps, [5])
        self.assertEqual(worker.calls, ["reconcile", "run_one"])

    def test_sigterm_during_current_job_finishes_it_then_claims_no_more(self):
        worker = _FakeWorker()
        handlers = {}

        def install(sig, handler):
            handlers[sig] = handler

        def finish_current_job():
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return None

        worker._run_hook = finish_current_job
        with mock.patch.object(publishing_worker, "build_worker", return_value=worker):
            with mock.patch.object(publishing_worker.signal, "signal", side_effect=install):
                with mock.patch.object(publishing_worker.time, "sleep") as sleep:
                    self.assertEqual(publishing_worker.main([]), 0)
        self.assertEqual(worker.calls, ["reconcile", "run_one"])
        sleep.assert_not_called()

    def test_sigterm_during_reconciliation_exits_before_claiming(self):
        worker = _FakeWorker()
        handlers = {}

        def install(sig, handler):
            handlers[sig] = handler

        def finish_reconcile():
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return 0

        worker._reconcile_hook = finish_reconcile
        with mock.patch.object(publishing_worker, "build_worker", return_value=worker):
            with mock.patch.object(publishing_worker.signal, "signal", side_effect=install):
                with mock.patch.object(publishing_worker.time, "sleep") as sleep:
                    self.assertEqual(publishing_worker.main([]), 0)
        self.assertEqual(worker.calls, ["reconcile"])
        sleep.assert_not_called()

    def test_module_import_graph_never_loads_app(self):
        root = Path(__file__).resolve().parents[1]
        script = (
            "import sys; import tools.publishing_worker; "
            "assert 'app' not in sys.modules"
        )
        with tempfile.TemporaryDirectory() as directory:
            env = os.environ.copy()
            env["PYTHONPATH"] = str(root)
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=directory,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_module_help_resolves_from_non_repository_working_directory(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            env = os.environ.copy()
            env["PYTHONPATH"] = str(root)
            result = subprocess.run(
                [sys.executable, "-m", "tools.publishing_worker", "--help"],
                cwd=directory,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--once", result.stdout)
        self.assertIn("--status", result.stdout)

    def test_config_exposes_exact_worker_environment_names_and_defaults(self):
        source = (Path(__file__).resolve().parents[1] / "config.py").read_text()
        self.assertIn("PAPERQUERY_PUBLISHING_WORKER_POLL_SECONDS", source)
        self.assertIn("PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS", source)
        self.assertIn("PAPERQUERY_PUBLISHING_RESERVATION_GRACE_SECONDS", source)
        self.assertIn('"5"', source)
        self.assertIn('"1800"', source)
        self.assertIn('"3600"', source)


if __name__ == "__main__":
    unittest.main()
