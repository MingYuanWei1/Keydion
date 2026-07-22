"""Poll durable attachment extraction and embedding jobs."""

from __future__ import annotations

import argparse
import signal
import time

from db import get_session_factory
from services.attachment_jobs import queue_status, run_one


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--status", action="store_true")
    args = parser.parse_args(argv)
    get_session_factory()  # verification-only schema initialization
    if args.status:
        queued, running = queue_status()
        print(f"queued={queued} running={running}")
        return 0
    if args.once:
        run_one()
        return 0

    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        if not run_one():
            time.sleep(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
