"""Admin AI-models control panel service.

Reads and edits the ACTIVE env file (.env.prod preferred, mirroring config.py)
for the four LLM provider slots: Text (chat flash/think), Embedding, Vision,
and Web search. API keys are write-only: every read path returns booleans,
never key values — a key, once saved, is never rendered or echoed again.

Write safety: only the whitelisted LLM_*/WEB_SEARCH_* keys are ever written,
and values must match a strict charset (no newlines, control characters,
whitespace, quotes, or '#') so a value can never break out into a new
KEY=VALUE line for either parser that reads this file (python-dotenv here,
systemd EnvironmentFile= at boot). See the plan doc for the full rationale.
"""
from __future__ import annotations

import os
import re
import threading
from pathlib import Path
from urllib.parse import urlsplit

import config
import web_search
from services import version as version_service

_WRITABLE_KEYS = frozenset({
    "LLM_API_KEY", "LLM_BASE_URL", "LLM_DEFAULT_FLASH", "LLM_DEFAULT_THINK",
    "LLM_EMBED_API_KEY", "LLM_EMBED_BASE_URL", "LLM_EMBED_MODEL",
    "LLM_VISION", "LLM_VISION_API_KEY", "LLM_VISION_BASE_URL",
    "WEB_SEARCH_PROVIDER", "WEB_SEARCH_API_KEY",
})

# Values that parse identically for python-dotenv and systemd EnvironmentFile=:
# no quoting is ever needed, so no value can alter line structure.
_VALUE_RE = re.compile(r"^[A-Za-z0-9._:/~@+-]{1,512}$")

_PROBE_TIMEOUT_SECONDS = 15
_SLOTS = ("text", "embed", "vision", "search")
# Slots consumed by the publishing / attachment worker units, which only an
# operator can restart; the panel reports this after saving.
_SATELLITE_SLOTS = frozenset({"embed", "vision", "search"})

_PROVIDER_LABELS = {
    "api.deepseek.com": "DeepSeek",
    "api.openai.com": "OpenAI",
    "generativelanguage.googleapis.com": "Google Gemini",
    "dashscope.aliyuncs.com": "Aliyun DashScope",
    "api.anthropic.com": "Anthropic",
}

_write_lock = threading.Lock()


class LLMAdminError(Exception):
    """A control-panel request failed; the message is user-facing."""


class LLMAdminConflict(LLMAdminError):
    """The env file changed underneath the admin; reload and retry."""


# ── env file access ──────────────────────────────────────────────────────────

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


def _parse_env_file(text: str) -> dict[str, str]:
    values = {}
    for line in text.splitlines():
        key = _line_key(line)
        if key is None:
            continue
        value = line.strip().partition("=")[2].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def _saved_values() -> dict[str, str]:
    try:
        text = active_env_path().read_text(encoding="utf-8")
    except OSError:
        return {}
    return _parse_env_file(text)


def _validate_value(key: str, value: str) -> None:
    if value and not _VALUE_RE.match(value):
        raise LLMAdminError(
            f"{key}: value contains characters the env file cannot store "
            "safely (letters, digits and . _ : / ~ @ + - only)."
        )


def _write_env(updates: dict[str, str], expected_mtime: float | None = None) -> float:
    """Rewrite only the named keys in the active env file; return its new mtime.

    Line-preserving: unknown lines, comments, and quoting are byte-identical
    after the write. Atomic: temp file + os.replace, mode 0600. Refuses keys
    outside the whitelist and values outside the safe charset.
    """
    if not updates:
        raise LLMAdminError("Nothing to save.")
    if not set(updates) <= _WRITABLE_KEYS:
        raise LLMAdminError("Refusing to write keys outside the LLM / web-search set.")
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
        pending = dict(updates)
        written: set[str] = set()
        out: list[str] = []
        for line in lines:
            key = _line_key(line)
            if key is not None and key in updates:
                if key in written:
                    continue  # drop duplicate definitions of an updated key
                ending = "\r\n" if line.endswith("\r\n") else ("\n" if line.endswith("\n") else "")
                out.append(f"{key}={pending.pop(key)}{ending}")
                written.add(key)
            else:
                out.append(line)
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


# ── snapshot (redacted — booleans only, never key material) ─────────────────

def _provider_label(base_url: str) -> str:
    host = (urlsplit(base_url).hostname or "") if base_url else ""
    if not host:
        return "OpenAI (default endpoint)"
    return _PROVIDER_LABELS.get(host, host)


def snapshot() -> dict:
    """Everything the AI-models page needs; keys appear only as booleans."""
    path = active_env_path()
    raw_file = _saved_values()

    def val(key: str) -> str:
        return (raw_file.get(key) or os.environ.get(key) or "").strip()

    text_key = val("LLM_API_KEY")
    embed_key = val("LLM_EMBED_API_KEY") or text_key
    vision_key = val("LLM_VISION_API_KEY") or text_key
    flash = val("LLM_DEFAULT_FLASH") or "gpt-4o-mini"
    think = val("LLM_DEFAULT_THINK") or flash
    embed_model = val("LLM_EMBED_MODEL") or "gemini-embedding-001"
    vision_model = val("LLM_VISION")

    vision_dedicated = bool(val("LLM_VISION_API_KEY") or val("LLM_VISION_BASE_URL"))
    if not vision_model:
        vision_mode = "disabled"
    elif vision_dedicated:
        vision_mode = "dedicated"
    else:
        vision_mode = "text"

    try:
        mtime = path.stat().st_mtime
        writable = os.access(path, os.W_OK)
    except OSError:
        mtime = 0.0
        writable = False

    return {
        "env_file": path.name,
        "env_mtime": mtime,
        "env_writable": writable,
        "embed_dim": config.RAG_EMBED_DIM,
        "slots": {
            "text": {
                "provider": _provider_label(val("LLM_BASE_URL")),
                "base_url": val("LLM_BASE_URL"),
                "key_set": bool(text_key),
                "flash": flash,
                "think": think,
                "think_follows_flash": not val("LLM_DEFAULT_THINK"),
            },
            "embed": {
                "provider": _provider_label(val("LLM_EMBED_BASE_URL")),
                "base_url": val("LLM_EMBED_BASE_URL"),
                "key_set": bool(val("LLM_EMBED_API_KEY")),
                "uses_text_key": not val("LLM_EMBED_API_KEY"),
                "model": embed_model,
            },
            "vision": {
                "mode": vision_mode,
                "provider": _provider_label(val("LLM_VISION_BASE_URL") or val("LLM_BASE_URL"))
                if vision_mode == "dedicated" else _provider_label(val("LLM_BASE_URL")),
                "base_url": val("LLM_VISION_BASE_URL"),
                "key_set": bool(val("LLM_VISION_API_KEY")),
                "uses_text_key": vision_mode == "text",
                "model": vision_model,
            },
            "search": {
                "provider": "Tavily",
                "key_set": bool(val("WEB_SEARCH_API_KEY")),
            },
        },
        # Capability strip: the user-visible feature each slot powers, with the
        # same enablement semantics llm_client applies at call time.
        "features": {
            "ask": {"on": bool(text_key), "model": think},
            "semantic_search": {"on": bool(embed_key), "model": embed_model},
            "vision_first": {"on": bool(vision_model) and bool(vision_key), "model": vision_model},
            "web_access": {"on": bool(val("WEB_SEARCH_API_KEY")), "model": "Tavily"},
        },
    }


# ── probe (validates form values against the live endpoint; never persists) ─

def _effective(payload: dict, field: str, saved_key: str) -> str:
    return (payload.get(field) or "").strip() or _saved_values().get(saved_key, "")


def _vetted_base_url(base_url: str) -> str:
    base_url = (base_url or "").strip()
    if not _VALUE_RE.match(base_url):
        raise LLMAdminError("Base URL is missing or contains unsafe characters.")
    if not web_search.url_targets_public_host(base_url):
        raise LLMAdminError("That endpoint is not a public address — internal hosts cannot be probed.")


def _probe_fail(message: str) -> dict:
    return {"ok": False, "error": message[:400]}


def probe(payload: dict) -> dict:
    """One validation call with the form's current (possibly unsaved) values.

    Falls back to the saved key/base URL when the form leaves them blank, so
    Test works without retyping a stored key. Keys are used once and dropped.
    """
    try:
        slot = (payload.get("slot") or "").strip()
        if slot not in _SLOTS:
            return _probe_fail("Unknown slot.")

        if slot == "search":
            return _probe_tavily(_effective(payload, "api_key", "WEB_SEARCH_API_KEY"))
        if slot == "embed":
            model = (payload.get("model") or "").strip() or _saved_values().get("LLM_EMBED_MODEL", "")
            if not model:
                return _probe_fail("Enter an embedding model first.")
            return _probe_embedding(
                _effective(payload, "base_url", "LLM_EMBED_BASE_URL"),
                _effective(payload, "api_key", "LLM_EMBED_API_KEY") or os.environ.get("LLM_API_KEY", ""),
                model,
            )
        saved_key = "LLM_VISION_API_KEY" if slot == "vision" else "LLM_API_KEY"
        return _probe_models(
            _effective(payload, "base_url", "LLM_VISION_BASE_URL" if slot == "vision" else "LLM_BASE_URL"),
            _effective(payload, "api_key", saved_key) or os.environ.get("LLM_API_KEY", ""),
            (payload.get("model") or "").strip(),
        )
    except LLMAdminError as exc:
        return _probe_fail(str(exc))
    except Exception as exc:  # noqa: BLE001 — surface any provider failure safely
        return _probe_fail(f"Probe failed: {exc}")


def _probe_models(base_url: str, api_key: str, model: str) -> dict:
    _vetted_base_url(base_url)
    if not api_key:
        return _probe_fail("No API key configured for this slot.")
    try:
        from openai import OpenAI  # lazy, matching llm_client
    except Exception as exc:
        return _probe_fail(f"openai package unavailable: {exc}")
    try:
        client = OpenAI(api_key=api_key, base_url=base_url or None,
                        timeout=_PROBE_TIMEOUT_SECONDS, max_retries=0)
        resp = client.models.list()
    except Exception as exc:  # noqa: BLE001 — provider/auth/network errors
        return _probe_fail(f"Endpoint rejected the request: {exc}")
    ids = sorted({m.id for m in resp.data if getattr(m, "id", None)})[:200]
    return {
        "ok": True,
        "models": ids,
        "model_listed": (model in ids) if model else None,
    }


def _probe_embedding(base_url: str, api_key: str, model: str) -> dict:
    _vetted_base_url(base_url)
    if not api_key:
        return _probe_fail("No API key configured for this slot.")
    try:
        from openai import OpenAI
    except Exception as exc:
        return _probe_fail(f"openai package unavailable: {exc}")
    try:
        client = OpenAI(api_key=api_key, base_url=base_url or None,
                        timeout=_PROBE_TIMEOUT_SECONDS, max_retries=0)
        resp = client.embeddings.create(model=model, input=["dimension probe"])
        dimension = len(resp.data[0].embedding)
    except Exception as exc:  # noqa: BLE001
        return _probe_fail(f"Embedding request failed: {exc}")
    return {
        "ok": True,
        "dimension": dimension,
        "expected": config.RAG_EMBED_DIM,
        "dimension_ok": dimension == config.RAG_EMBED_DIM,
    }


def _probe_tavily(api_key: str) -> dict:
    if not api_key:
        return _probe_fail("No Tavily key configured.")
    import requests
    try:
        resp = requests.post(
            "https://api.tavily.com/search",
            json={"api_key": api_key, "query": "connectivity check", "max_results": 1,
                  "search_depth": "basic"},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return _probe_fail(f"Tavily rejected the request: {exc}")
    return {"ok": True}


# ── save (validate → gate → write → apply in-process → recycle web workers) ─

def apply_slot(payload: dict, expected_mtime: float | None = None) -> dict:
    """Persist one card's form values. Raises LLMAdminError/Conflict on refusal."""
    slot = (payload.get("slot") or "").strip()
    if slot not in _SLOTS:
        raise LLMAdminError("Unknown slot.")
    saved = _saved_values()
    updates = _build_updates(slot, payload, saved)

    if slot == "embed" and updates.get("LLM_EMBED_MODEL", saved.get("LLM_EMBED_MODEL", "")) != saved.get("LLM_EMBED_MODEL", ""):
        _gate_embedding_dimension(payload, updates)

    _write_env(updates, expected_mtime=expected_mtime)
    # Mirror the file into this worker so the change is live before any restart.
    os.environ.update(updates)
    restarted = version_service.request_graceful_restart()
    return {
        "ok": True,
        "restarted": restarted,
        "satellite_notice": slot in _SATELLITE_SLOTS,
        "snap": snapshot(),
    }


def _build_updates(slot: str, payload: dict, saved: dict) -> dict[str, str]:
    def field(name: str) -> str:
        value = (payload.get(name) or "").strip()
        _validate_value(name.upper(), value)
        return value

    if slot == "text":
        flash = field("flash")
        if not flash:
            raise LLMAdminError("A Flash model is required.")
        field("think")  # charset-check even when it follows Flash
        updates = {
            "LLM_BASE_URL": field("base_url"),
            "LLM_DEFAULT_FLASH": flash,
            "LLM_DEFAULT_THINK": field("think"),
        }
        api_key = field("api_key")
        if api_key:
            updates["LLM_API_KEY"] = api_key
        return updates

    if slot == "embed":
        model = field("model")
        if not model:
            raise LLMAdminError("An embedding model is required.")
        updates = {"LLM_EMBED_BASE_URL": field("base_url"), "LLM_EMBED_MODEL": model}
        api_key = field("api_key")
        if api_key:
            updates["LLM_EMBED_API_KEY"] = api_key
        return updates

    if slot == "vision":
        mode = field("mode")
        if mode == "disabled":
            return {"LLM_VISION": "", "LLM_VISION_API_KEY": "", "LLM_VISION_BASE_URL": ""}
        model = field("model")
        if not model:
            raise LLMAdminError("A vision model is required unless vision is disabled.")
        if mode == "text":
            # Requirement: use the text provider — dedicated creds must be
            # cleared so llm_client falls back to the chat credentials.
            return {"LLM_VISION": model, "LLM_VISION_API_KEY": "", "LLM_VISION_BASE_URL": ""}
        if mode == "dedicated":
            updates = {"LLM_VISION": model, "LLM_VISION_BASE_URL": field("base_url")}
            api_key = field("api_key")
            if api_key:
                updates["LLM_VISION_API_KEY"] = api_key
            return updates
        raise LLMAdminError("Unknown vision mode.")

    # slot == "search"
    api_key = field("api_key")
    return {"WEB_SEARCH_API_KEY": api_key}  # empty clears → web toggle hidden


def _gate_embedding_dimension(payload: dict, updates: dict) -> None:
    """Refuse an embedding-model change unless a live probe matches RAG_EMBED_DIM."""
    result = probe({
        "slot": "embed",
        "base_url": payload.get("base_url") or "",
        "api_key": payload.get("api_key") or "",
        "model": updates.get("LLM_EMBED_MODEL", ""),
    })
    if not result.get("ok"):
        raise LLMAdminError(
            "Cannot verify the new embedding model, so the change was refused: "
            + result.get("error", "probe failed")
        )
    if not result.get("dimension_ok"):
        raise LLMAdminError(
            f"The new embedding model outputs {result.get('dimension')}-d vectors, but the "
            f"database column is VECTOR({result.get('expected')}). Switching requires an "
            "Alembic migration plus a full re-index — an operator action, not a panel save."
        )
