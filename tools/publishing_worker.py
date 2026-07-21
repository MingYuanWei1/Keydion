"""Poll durable Paper lifecycle work without importing the Flask application."""

from __future__ import annotations

import argparse
import logging
import signal
import time

from services.publishing_jobs import redact_job_error
from services.publishing_wiring import build_publishing_worker


_LOG = logging.getLogger(__name__)


def build_worker():
    return build_publishing_worker()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Paper publishing worker")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="run one polling pass")
    mode.add_argument("--status", action="store_true", help="show queue status")
    return parser


def _safe_worker_call(label: str, callback):
    try:
        return callback()
    except Exception as exc:
        _LOG.warning(
            "publishing worker operation failed operation=%s error=%s",
            label,
            redact_job_error(exc),
        )
        return None


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    worker = build_worker()

    if args.status:
        status = worker.status()
        oldest = (
            "none"
            if status.oldest_age_seconds is None
            else str(status.oldest_age_seconds)
        )
        print(
            f"pending={status.pending} running={status.running} "
            f"oldest_age_seconds={oldest}"
        )
        return 0

    if args.once:
        _safe_worker_call("reconcile", worker.reconcile)
        _safe_worker_call("run_one", worker.run_one)
        return 0

    stop_requested = False

    def request_stop(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    while not stop_requested:
        _safe_worker_call("reconcile", worker.reconcile)
        if stop_requested:
            break
        _safe_worker_call("run_one", worker.run_one)
        if stop_requested:
            break
        time.sleep(worker.poll_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
