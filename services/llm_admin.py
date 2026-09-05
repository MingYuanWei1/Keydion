"""Read-only Worker model status and write-only Tavily credential settings."""
from __future__ import annotations

import os
import re
import threading
from pathlib import Path

from flask_babel import gettext as _

import config
import llm_worker
import web_search
from services import version as version_service

_WRITABLE_KEYS = frozenset({"WEB_SEARCH_API_KEY"})
_VALUE_RE = re.compile(r"^[A-Za-z0-9._:/~@+-]{1,512}$")
_write_lock = threading.Lock()


class LLMAdminError(Exception):
    """A control-panel request failed; the message is user-facing."""


class LLMAdminConflict(LLMAdminError):
    """The environment file changed; reload and retry."""


def active_env_path() -> Path:
    """The env file config.py loads (.env.prod preferred)."""
    prod = config.BASE_DIR / ".env.prod"
    return prod if prod.exists() else config.BASE_DIR / ".env"


def _line_key(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    key, sep, _ = stripped.partition("=")
    if not sep:
        return None
    return key.strip()


def _validate_value(key: str, value: str) -> None:
    if value and not _VALUE_RE.match(value):
        raise LLMAdminError(
            f"{key}: value contains characters the env file cannot store "
            "safely (letters, digits and . _ : / ~ @ + - only)."
        )


def _writable(name: str) -> bool:
    return name in _WRITABLE_KEYS


def _write_env(updates: dict[str, str], expected_mtime: float | None = None) -> float:
    """Rewrite only the named keys in the active env file; return its new mtime.

    Line-preserving: unknown lines, comments, and quoting are byte-identical
    after the write. Atomic: temp file + os.replace, mode 0600. Refuses names
    outside the whitelist and values outside the safe charset.
    """
    if not updates:
        raise LLMAdminError("Nothing to save.")
    if not all(_writable(name) for name in updates):
        raise LLMAdminError("Refusing to write keys outside the web-search set.")
    for key, value in updates.items():
        _validate_value(key, value)

    path = active_env_path()
    with _write_lock:
        try:
            text = path.read_text(encoding="utf-8")
            current_mtime = path.stat().st_mtime
        except OSError as exc:
            raise LLMAdminError(f"Cannot read {path.name}: {exc}") from exc
        if expected_mtime is not None and abs(current_mtime - expected_mtime) > 0.001:
            raise LLMAdminConflict(
                f"{path.name} changed since this page loaded — reload the page and save again."
            )

        lines = text.splitlines(keepends=True)
        written: set[str] = set()
        out: list[str] = []
        for line in lines:
            key = _line_key(line)
            if key is not None and key in updates:
                if key in written:
                    continue  # drop duplicate definitions of an updated key
                ending = "\r\n" if line.endswith("\r\n") else ("\n" if line.endswith("\n") else "")
                out.append(f"{key}={updates[key]}{ending}")
                written.add(key)
            else:
                out.append(line)
        pending = {k: v for k, v in updates.items() if k not in written}
        if pending:
            if out and not out[-1].endswith("\n"):
                out[-1] += "\n"
            for key in sorted(pending):
                out.append(f"{key}={pending[key]}\n")

        tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write("".join(out))
                fh.flush()
                os.fsync(fh.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, path)
        except OSError as exc:
            try:
                tmp.unlink()
            except OSError:
                pass
            raise LLMAdminError(f"Cannot write {path.name}: {exc}") from exc
        try:
            return path.stat().st_mtime
        except OSError as exc:
            raise LLMAdminError(f"Cannot stat {path.name}: {exc}") from exc


def snapshot() -> dict:
    """Model configuration and readiness; never expose credentials."""
    status = llm_worker.capabilities()
    path = active_env_path()
    return {
        **status,
        "embedding_ready": llm_worker.purpose_enabled("embed"),
        "embed_dim": config.RAG_EMBED_DIM,
        "search_configured": web_search.web_search_enabled(),
        "env_mtime": path.stat().st_mtime if path.exists() else 0,
    }


def apply_slot(payload: dict, expected_env_mtime: float | None = None) -> dict:
    """The only editable application setting is the Tavily key."""
    if payload.get("slot") != "search":
        raise LLMAdminError(_("Model configuration is managed in Cloudflare."))
    api_key = (payload.get("api_key") or "").strip()
    updates = {"WEB_SEARCH_API_KEY": api_key}
    _write_env(updates, expected_mtime=expected_env_mtime)
    os.environ.update(updates)
    restarted = version_service.request_graceful_restart()
    return {"ok": True, "restarted": restarted, "snap": snapshot()}
