"""Self-update service for the admin Version page.

Version identity is commit-based: the deployed git HEAD SHA is compared
against origin/<UPSTREAM_BRANCH>. Applying an update fast-forwards the
checkout, reinstalls pinned dependencies when requirements.lock changed,
then restarts the process: under gunicorn the master receives SIGHUP so
gracefully-replaced workers re-import the new code (gunicorn.conf.py runs
with preload_app=False); under the Flask dev server the werkzeug reloader
restarts on the pulled file changes by itself.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import config

UPSTREAM_REMOTE = "origin"
UPSTREAM_BRANCH = "main"
STATE_FILENAME = "version_update_state.json"
LOCK_PATH_NAME = "requirements.lock"
RESTART_DELAY_SECONDS = 1.5

_lock = threading.Lock()
_run = {
    "running": False,
    "phase": "idle",
    "target_sha": None,
    "previous_sha": None,
    "started_at": None,
    "error": None,
    "deps_installed": False,
    "log": [],
}


def _state_path() -> Path:
    return Path(config.DATA_DIR) / STATE_FILENAME


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _git(*args, timeout=120) -> subprocess.CompletedProcess:
    if shutil.which("git") is None:
        # Deployments without a git binary (e.g. the dev container) are not
        # git checkouts; degrade instead of 500-ing the admin page.
        return subprocess.CompletedProcess(["git", *args], 127, "", "git is not installed")
    try:
        return subprocess.run(
            ["git", *args],
            cwd=str(config.BASE_DIR),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        # Never let a hung git command 500 the admin page or the update thread.
        return subprocess.CompletedProcess(
            ["git", *args], 124, "", f"git {args[0]} timed out after {timeout}s"
        )
    except OSError as exc:
        return subprocess.CompletedProcess(["git", *args], 127, "", str(exc))


def _log(message: str) -> None:
    _run["log"].append(f"[{_now()}] {message}")
    # Keep the tail bounded; the status endpoint ships this to the browser.
    if len(_run["log"]) > 200:
        del _run["log"][:-200]


def probe_repo():
    """Return (is_git, detail).

    When the repo is unusable, detail carries the actual git error so
    ownership/permission problems are distinguishable from a plain
    non-checkout instead of being masked by a generic message.
    """
    if shutil.which("git") is None:
        return False, "git is not installed on this host"
    result = _git("rev-parse", "--is-inside-work-tree", timeout=10)
    if result.returncode == 0:
        return True, ""
    detail = (result.stderr or result.stdout).strip()
    if not detail:
        detail = f"{config.BASE_DIR} is not a git checkout"
    return False, detail[:500]


def repo_root_is_git() -> bool:
    return probe_repo()[0]


def current_sha():
    result = _git("rev-parse", "HEAD", timeout=10)
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def current_branch() -> str:
    result = _git("rev-parse", "--abbrev-ref", "HEAD", timeout=10)
    return result.stdout.strip() if result.returncode == 0 else ""


def working_tree_status():
    result = _git("status", "--porcelain", timeout=30)
    if result.returncode != 0:
        return []
    return [line for line in result.stdout.splitlines() if line.strip()]


def fetch_upstream():
    result = _git("fetch", UPSTREAM_REMOTE, UPSTREAM_BRANCH, "--quiet", timeout=180)
    if result.returncode != 0:
        return False, (result.stderr or result.stdout).strip()
    return True, ""


def upstream_sha():
    result = _git("rev-parse", f"{UPSTREAM_REMOTE}/{UPSTREAM_BRANCH}", timeout=10)
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def commits_between(old_sha, new_sha):
    result = _git(
        "log",
        "--pretty=format:%H%x1f%an%x1f%ad%x1f%s",
        "--date=short",
        f"{old_sha}..{new_sha}",
        timeout=30,
    )
    if result.returncode != 0:
        return []
    commits = []
    for line in result.stdout.splitlines():
        sha, sep, rest = line.partition("\x1f")
        if not sep:
            continue
        author, sep, rest = rest.partition("\x1f")
        if not sep:
            continue
        date, sep, subject = rest.partition("\x1f")
        if not sep:
            continue
        commits.append(
            {"sha": sha, "short": sha[:7], "author": author, "date": date, "subject": subject}
        )
    return commits


def files_changed(old_sha, new_sha):
    result = _git("diff", "--name-only", f"{old_sha}..{new_sha}", timeout=30)
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _read_state():
    try:
        return json.loads(_state_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_state(state: dict) -> None:
    try:
        path = _state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except OSError:
        pass  # the status file is best-effort; never fail an update over it


def snapshot() -> dict:
    """Everything the Version page needs, gathered in one pass."""
    is_git, repo_detail = probe_repo()
    info = {
        "is_git": is_git,
        "branch": "",
        "head_sha": None,
        "head_short": "",
        "dirty": False,
        "status_lines": [],
        "upstream_sha": None,
        "upstream_short": "",
        "behind_count": 0,
        "commits": [],
        "check_error": None,
        "last_update": _read_state(),
        "running_under_gunicorn": "gunicorn" in sys.modules,
        "update_running": _run["running"],
    }
    if not info["is_git"]:
        info["check_error"] = repo_detail
        return info
    info["branch"] = current_branch()
    info["head_sha"] = current_sha()
    info["head_short"] = (info["head_sha"] or "")[:7]
    info["status_lines"] = working_tree_status()
    info["dirty"] = bool(info["status_lines"])
    ok, err = fetch_upstream()
    if not ok:
        info["check_error"] = err
        return info
    info["upstream_sha"] = upstream_sha()
    info["upstream_short"] = (info["upstream_sha"] or "")[:7]
    if info["head_sha"] and info["upstream_sha"] and info["head_sha"] != info["upstream_sha"]:
        info["commits"] = commits_between(info["head_sha"], info["upstream_sha"])
        info["behind_count"] = len(info["commits"])
    return info


def update_status() -> dict:
    """Live status for the JS poller; survives restarts via the state file."""
    state = _read_state()
    head = current_sha() if repo_root_is_git() else None
    status = {
        "running": _run["running"],
        "phase": _run["phase"],
        "error": _run["error"],
        "target_sha": _run["target_sha"],
        "previous_sha": _run["previous_sha"],
        "deps_installed": _run["deps_installed"],
        "log": list(_run["log"]),
        "head_sha": head,
        "last_update": state,
    }
    if not _run["running"] and state and head and state.get("status") == "restarting":
        # In-memory run state does not survive a restart; derive the outcome.
        status["phase"] = "complete" if state.get("target_sha") == head else "incomplete"
    return status


def start_update():
    """Validate preconditions and launch the background update.

    Returns (ok, message); message is a user-facing reason when ok is False.
    """
    with _lock:
        if _run["running"]:
            return False, "An update is already running."
        is_git, repo_detail = probe_repo()
        if not is_git:
            return False, repo_detail
        head = current_sha()
        if not head:
            return False, "Unable to read the current revision."
        if working_tree_status():
            return False, "The working tree has uncommitted changes."
        ok, err = fetch_upstream()
        if not ok:
            return False, f"Fetch failed: {err}"
        target = upstream_sha()
        if not target:
            return False, "Unable to read the upstream revision."
        if target == head:
            return False, "Already up to date."
        _run.update(
            running=True,
            phase="starting",
            target_sha=target,
            previous_sha=head,
            started_at=_now(),
            error=None,
            deps_installed=False,
            log=[],
        )
        _write_state(
            {
                "status": "running",
                "previous_sha": head,
                "target_sha": target,
                "started_at": _run["started_at"],
            }
        )
    thread = threading.Thread(target=_run_update, args=(head, target), name="version-update", daemon=True)
    thread.start()
    return True, ""


def _run_update(previous_sha: str, target_sha: str) -> None:
    try:
        _run["phase"] = "updating"
        _log(f"Fast-forwarding {previous_sha[:7]} -> {target_sha[:7]}")
        result = _git("merge", "--ff-only", f"{UPSTREAM_REMOTE}/{UPSTREAM_BRANCH}", timeout=120)
        if result.returncode != 0:
            raise RuntimeError(f"git merge --ff-only failed: {(result.stderr or result.stdout).strip()}")
        head_now = current_sha() or ""
        if head_now != target_sha:
            raise RuntimeError(f"Checkout is at {head_now[:7]}, expected {target_sha[:7]}")
        if LOCK_PATH_NAME in files_changed(previous_sha, target_sha):
            _run["phase"] = "installing-dependencies"
            _log(f"{LOCK_PATH_NAME} changed - reinstalling pinned dependencies")
            pip = subprocess.run(
                [sys.executable, "-m", "pip", "install", "--require-hashes", "--quiet",
                 "-r", str(config.BASE_DIR / LOCK_PATH_NAME)],
                cwd=str(config.BASE_DIR),
                capture_output=True,
                text=True,
                timeout=1800,
            )
            if pip.returncode != 0:
                tail = (pip.stderr or pip.stdout).strip().splitlines()
                raise RuntimeError("pip install failed: " + (tail[-1] if tail else "unknown error"))
            _run["deps_installed"] = True
            _log("Dependencies installed")
        else:
            _log(f"{LOCK_PATH_NAME} unchanged - skipping dependency install")
        _run["phase"] = "restarting"
        _write_state(
            {
                "status": "restarting",
                "previous_sha": previous_sha,
                "target_sha": target_sha,
                "started_at": _run["started_at"],
                "finished_at": _now(),
                "deps_installed": _run["deps_installed"],
            }
        )
        _log("Update applied - restarting the app")
        _schedule_restart()
    except Exception as exc:  # noqa: BLE001 - the worker must record and exit cleanly
        _run["phase"] = "failed"
        _run["error"] = str(exc)
        _log(f"FAILED: {exc}")
        _write_state(
            {
                "status": "failed",
                "previous_sha": previous_sha,
                "target_sha": target_sha,
                "started_at": _run["started_at"],
                "finished_at": _now(),
                "error": str(exc),
                "deps_installed": _run["deps_installed"],
            }
        )
    finally:
        _run["running"] = False


def _schedule_restart() -> None:
    if "gunicorn" not in sys.modules:
        # Flask dev: the werkzeug reloader restarts on the pulled file changes.
        # A deps-only update needs a manual dev-server restart in that case.
        return

    def _signal_master():
        time.sleep(RESTART_DELAY_SECONDS)
        try:
            os.kill(os.getppid(), signal.SIGHUP)
        except OSError:
            pass

    threading.Thread(target=_signal_master, name="version-restart", daemon=True).start()
