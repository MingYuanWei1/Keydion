"""Admin AI-models control panel service.

Two layers:

- The PROVIDER REGISTRY (data/llm_providers.json) holds named providers —
  display name, base URL, an editable model list — and which provider each
  slot (Text flash/think, Embedding, Vision) is assigned to. Non-secret data.
- The ENV FILE (.env.prod preferred, mirroring config.py) stays the runtime
  source of truth llm_client reads. A provider's API key lives there under a
  derived name (LLM_PROVIDER_<ID>_API_KEY); saving a slot assignment resolves
  provider → base URL + key and writes the flat LLM_* variables exactly as
  before. llm_client is untouched by the registry.

API keys are write-only everywhere: a key, once saved, is never rendered or
echoed — only booleans. Copying a provider key into a slot variable happens
file-to-file server side.

Write safety: only whitelisted or pattern-validated variable names are ever
written, and values must match a strict charset (no newlines, control
characters, whitespace, quotes, or '#') so a value can never break out into a
new KEY=VALUE line for either parser that reads this file (python-dotenv
here, systemd EnvironmentFile= at boot).
"""
from __future__ import annotations

import json
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
# Provider keys use derived names (dashes in the id become underscores); the
# pattern, not the browser, names them.
_PROVIDER_KEY_RE = re.compile(r"^LLM_PROVIDER_[A-Z0-9_-]{1,32}_API_KEY$")
# Migrated providers may still read a slot variable until first save
# normalizes them onto their own key.
_LEGACY_KEY_VARS = frozenset({"LLM_API_KEY", "LLM_EMBED_API_KEY", "LLM_VISION_API_KEY"})

# Values that parse identically for python-dotenv and systemd EnvironmentFile=:
# no quoting is ever needed, so no value can alter line structure.
_VALUE_RE = re.compile(r"^[A-Za-z0-9._:/~@+-]{1,512}$")
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")

_PROBE_TIMEOUT_SECONDS = 15
_SLOTS = ("text", "embed", "vision", "search")
# Role each MODEL plays; "multimodal" covers text+vision in one model.
_MODEL_ROLES = ("text", "multimodal", "embedding")
# Which model roles each slot may use.
_SLOT_MODEL_ROLES = {
    "text": ("text", "multimodal"),
    "embed": ("embedding",),
    "vision": ("multimodal",),
}
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

_REGISTRY_VERSION = 2  # v2: models are [{"id", "role"}] with per-model roles
_REGISTRY_FILENAME = "llm_providers.json"

_write_lock = threading.Lock()


class LLMAdminError(Exception):
    """A control-panel request failed; the message is user-facing."""


class LLMAdminConflict(LLMAdminError):
    """A managed file changed underneath the admin; reload and retry."""


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


def _env_value(key_var: str) -> str:
    return (_saved_values().get(key_var) or os.environ.get(key_var) or "").strip()


def _validate_value(key: str, value: str) -> None:
    if value and not _VALUE_RE.match(value):
        raise LLMAdminError(
            f"{key}: value contains characters the env file cannot store "
            "safely (letters, digits and . _ : / ~ @ + - only)."
        )


def _writable(name: str) -> bool:
    return name in _WRITABLE_KEYS or bool(_PROVIDER_KEY_RE.match(name))


def _write_env(updates: dict[str, str], expected_mtime: float | None = None) -> float:
    """Rewrite only the named keys in the active env file; return its new mtime.

    Line-preserving: unknown lines, comments, and quoting are byte-identical
    after the write. Atomic: temp file + os.replace, mode 0600. Refuses names
    outside the whitelist/pattern and values outside the safe charset.
    """
    if not updates:
        raise LLMAdminError("Nothing to save.")
    if not all(_writable(name) for name in updates):
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


def _atomic_write_json(path: Path, payload: dict, expected_mtime: float | None) -> float:
    """Atomic tmp+replace JSON write under the same lock discipline as env."""
    with _write_lock:
        if expected_mtime is not None:
            try:
                current = path.stat().st_mtime
            except OSError:
                current = 0.0
            if abs(current - expected_mtime) > 0.001:
                raise LLMAdminConflict(
                    f"{path.name} changed since this page loaded — reload the page and save again."
                )
        tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=False)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
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


# ── provider registry ────────────────────────────────────────────────────────

def _registry_path() -> Path:
    return config.DATA_DIR / _REGISTRY_FILENAME


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    if not _SLUG_RE.match(slug):
        raise LLMAdminError("Could not derive a valid provider id from that name.")
    return slug


def _provider_key_var(pid: str) -> str:
    return f"LLM_PROVIDER_{pid.upper().replace('-', '_')}_API_KEY"


def _host_of(base_url: str) -> str:
    return (urlsplit(base_url).hostname or "") if base_url else ""


def _label_for(base_url: str) -> str:
    host = _host_of(base_url)
    return _PROVIDER_LABELS.get(host, host or "OpenAI (default endpoint)")


def _unique_slug(base: str, taken: set[str]) -> str:
    slug, n = base, 2
    while slug in taken:
        suffix = f"-{n}"
        if len(base) + len(suffix) > 32:
            raise LLMAdminError("Could not derive a unique provider id; choose another name.")
        slug = base + suffix
        n += 1
    return slug


def _derive_registry(env: dict[str, str]) -> dict:
    """First-run registry derived from the current env — no writes happen here;
    legacy slot keys are normalized onto provider keys on first save."""
    providers: list[dict] = []
    by_base: dict[str, str] = {}

    def provider_for(base_url: str, key_var: str, model: str, model_role: str) -> str | None:
        if not base_url and not _env_value(key_var):
            return None
        base_url = base_url or ""
        pid = by_base.get(base_url)
        if pid is None:
            seed = _slugify(_PROVIDER_LABELS.get(_host_of(base_url), _host_of(base_url) or key_var.lower()))
            pid = _unique_slug(seed, set(by_base.values()))
            by_base[base_url] = pid
            providers.append({"id": pid, "name": _label_for(base_url), "base_url": base_url,
                              "key_var": key_var, "models": []})
        if model:
            entry = next(p for p in providers if p["id"] == pid)
            _upsert_model(entry, model, model_role)
        return pid

    text_pid = provider_for(env.get("LLM_BASE_URL", ""), "LLM_API_KEY",
                            env.get("LLM_DEFAULT_FLASH", ""), "text")
    flash = env.get("LLM_DEFAULT_FLASH") or "gpt-4o-mini"
    think = env.get("LLM_DEFAULT_THINK", "")
    if think and text_pid:
        provider_for(env.get("LLM_BASE_URL", ""), "LLM_API_KEY", think, "text")
    embed_pid = provider_for(env.get("LLM_EMBED_BASE_URL", ""), "LLM_EMBED_API_KEY",
                             env.get("LLM_EMBED_MODEL", ""), "embedding")
    vision_dedicated = bool(env.get("LLM_VISION_API_KEY") or env.get("LLM_VISION_BASE_URL"))
    vision_model = env.get("LLM_VISION", "")
    if vision_dedicated and vision_model:
        vision_pid = provider_for(env.get("LLM_VISION_BASE_URL", ""), "LLM_VISION_API_KEY",
                                  vision_model, "multimodal")
    elif vision_model and text_pid:
        # Vision runs on the text provider's credentials, so that model is
        # multimodal — upgrade it if it was seeded as a plain text model.
        entry = next(p for p in providers if p["id"] == text_pid)
        _upsert_model(entry, vision_model, "multimodal")
        vision_pid = None  # same as the Text provider
    else:
        vision_pid = None

    return {
        "version": _REGISTRY_VERSION,
        "providers": providers,
        "assignments": {
            "text": {"provider_id": text_pid, "flash": flash, "think": think},
            "embed": {"provider_id": embed_pid, "model": env.get("LLM_EMBED_MODEL", "")},
            "vision": {
                "mode": "dedicated" if vision_dedicated and vision_model else ("text" if vision_model else "disabled"),
                "provider_id": vision_pid if (vision_dedicated and vision_model) else None,
                "model": vision_model,
            },
            "search": {},
        },
    }


def _models_of(provider: dict) -> list[dict]:
    """A provider's models as [{"id", "role"}]; a v1 string list is upgraded
    with the text role."""
    models = provider.get("models")
    if not isinstance(models, list):
        return []
    out = []
    for model in models:
        if isinstance(model, dict):
            role = model.get("role") if model.get("role") in _MODEL_ROLES else "text"
            out.append({"id": str(model.get("id", "")), "role": role})
        elif model:
            out.append({"id": str(model), "role": "text"})
    return [m for m in out if m["id"]]


def _upsert_model(provider: dict, model_id: str, role: str) -> None:
    """Add a model with its role, or upgrade an existing model's role when the
    new role is strictly stronger (text → multimodal)."""
    models = provider.setdefault("models", [])
    for model in models if all(isinstance(m, dict) for m in models) else []:
        if model.get("id") == model_id:
            if model.get("role") == "text" and role == "multimodal":
                model["role"] = "multimodal"
            return
    for model in models:
        if model == model_id:  # v1 string entry
            models[models.index(model)] = {"id": model_id, "role": role}
            return
    models.append({"id": model_id, "role": role})


def _find_model(provider: dict, model_id: str, allowed_roles) -> dict | None:
    for model in _models_of(provider):
        if model["id"] == model_id and model["role"] in allowed_roles:
            return model
    return None


def _effective_env_values() -> dict[str, str]:
    """File values with the live process environment layered on top — the
    environment is what llm_client actually reads, and dev containers inject
    LLM vars via compose env_file without shipping an env file on disk."""
    values = _saved_values()
    for key, value in os.environ.items():
        if value and _writable(key):
            values[key] = value
    return values


def load_registry() -> tuple[dict, float]:
    """The registry plus its mtime; derives from env when absent/unreadable."""
    path = _registry_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        mtime = path.stat().st_mtime
    except (OSError, ValueError):
        raw, mtime = None, 0.0
    if not isinstance(raw, dict) or "providers" not in raw or "assignments" not in raw:
        raw = _derive_registry(_effective_env_values())
    return raw, mtime


def _provider_by_id(reg: dict, pid: str) -> dict | None:
    return next((p for p in reg["providers"] if p["id"] == pid), None)


def _normalize_provider_keys(reg: dict) -> dict[str, str]:
    """Env updates moving migrated providers off legacy slot keys onto their
    own derived key, so later slot changes cannot clobber each other."""
    updates: dict[str, str] = {}
    for provider in reg["providers"]:
        if provider.get("key_var") in _LEGACY_KEY_VARS:
            value = _env_value(provider["key_var"])
            new_var = _provider_key_var(provider["id"])
            if value and new_var != provider["key_var"]:
                updates[new_var] = value
                provider["key_var"] = new_var
    return updates


def _parse_models(payload: dict) -> list[dict]:
    """A provider's models with each model's role: text, multimodal, embedding."""
    models: list[dict] = []
    seen: set[str] = set()
    for model in (payload.get("models") or []):
        model_id = ((model or {}).get("id") or "").strip()
        role = ((model or {}).get("role") or "").strip().lower()
        if not model_id:
            continue
        _validate_value("Model", model_id)
        if role not in _MODEL_ROLES:
            raise LLMAdminError(
                "Unknown model role '" + role + "' — roles are: " + ", ".join(_MODEL_ROLES) + "."
            )
        if model_id in seen:
            raise LLMAdminError(f"Model {model_id} is listed twice.")
        seen.add(model_id)
        models.append({"id": model_id, "role": role})
    return models


def save_provider(payload: dict, expected_env_mtime: float | None = None,
                  expected_json_mtime: float | None = None) -> dict:
    """Create or update one provider. `id` in the payload means update (the id
    itself is immutable); absence means create, deriving the id from the name."""
    reg, _ = load_registry()
    name = (payload.get("name") or "").strip()
    if not name or len(name) > 64 or any(ord(c) < 32 for c in name):
        raise LLMAdminError("Provider name must be 1-64 characters.")
    base_url = (payload.get("base_url") or "").strip()
    _validate_value("Base URL", base_url)
    models = _parse_models(payload)

    env_updates: dict[str, str] = {}
    pid = (payload.get("id") or "").strip()
    if pid:
        provider = _provider_by_id(reg, pid)
        if provider is None:
            raise LLMAdminError("Unknown provider.")
        removed = {m["id"] for m in _models_of(provider)} - {m["id"] for m in models}
        _refuse_assigned_models(reg, removed)
        provider.update(name=name, base_url=base_url, models=models)
    else:
        pid = _unique_slug(_slugify(name), {p["id"] for p in reg["providers"]})
        reg["providers"].append({"id": pid, "name": name, "base_url": base_url,
                                 "key_var": _provider_key_var(pid), "models": models})

    api_key = (payload.get("api_key") or "").strip()
    if api_key:
        _validate_value("API key", api_key)
        provider = _provider_by_id(reg, pid)
        if provider.get("key_var") in _LEGACY_KEY_VARS:
            # First edit of a migrated provider: move it onto its own key.
            provider["key_var"] = _provider_key_var(pid)
        env_updates[provider["key_var"]] = api_key

    env_updates.update(_normalize_provider_keys(reg))
    env_mtime = _write_env(env_updates, expected_mtime=expected_env_mtime) if env_updates else None
    json_mtime = _atomic_write_json(_registry_path(), reg, expected_json_mtime)
    return {"ok": True, "id": pid, "env_mtime": env_mtime, "json_mtime": json_mtime,
            "snap": snapshot()}


def _refuse_assigned_models(reg: dict, removed_model_ids: set[str]) -> None:
    """Refuse removing models a slot still uses — same posture as delete."""
    if not removed_model_ids:
        return
    a = reg["assignments"]
    uses = []
    for model_id in sorted(removed_model_ids):
        if a.get("text", {}).get("flash") == model_id or a.get("text", {}).get("think") == model_id:
            uses.append(f"text ({model_id})")
        if a.get("embed", {}).get("model") == model_id:
            uses.append(f"embedding ({model_id})")
        if a.get("vision", {}).get("model") == model_id:
            uses.append(f"vision ({model_id})")
    if uses:
        raise LLMAdminError(
            "Still assigned: " + ", ".join(uses)
            + " — reassign those slots before removing the model."
        )


def delete_provider(payload: dict, expected_env_mtime: float | None = None,
                    expected_json_mtime: float | None = None) -> dict:
    reg, _ = load_registry()
    pid = (payload.get("id") or "").strip()
    provider = _provider_by_id(reg, pid)
    if provider is None:
        raise LLMAdminError("Unknown provider.")
    assigned = [slot for slot, a in reg["assignments"].items()
                if a.get("provider_id") == pid]
    if assigned:
        raise LLMAdminError(
            "That provider is assigned to " + ", ".join(sorted(assigned))
            + " — reassign those slots before deleting it."
        )
    reg["providers"] = [p for p in reg["providers"] if p["id"] != pid]
    env_updates = _normalize_provider_keys(reg)
    if _PROVIDER_KEY_RE.match(provider.get("key_var", "")):
        env_updates[provider["key_var"]] = ""  # clear the orphaned key
    if env_updates:
        _write_env(env_updates, expected_mtime=expected_env_mtime)
    json_mtime = _atomic_write_json(_registry_path(), reg, expected_json_mtime)
    return {"ok": True, "json_mtime": json_mtime, "snap": snapshot()}


# ── snapshot (redacted — booleans only, never key material) ─────────────────

def _provider_public(reg: dict, provider: dict) -> dict:
    return {
        "id": provider["id"],
        "name": provider["name"],
        "base_url": provider.get("base_url", ""),
        "key_set": bool(_env_value(provider.get("key_var", ""))),
        "models": _models_of(provider),
        "used_by": sorted(slot for slot, a in reg["assignments"].items()
                          if a.get("provider_id") == provider["id"]),
    }


def snapshot() -> dict:
    """Everything the AI-models page needs; keys appear only as booleans."""
    path = active_env_path()
    raw_file = _saved_values()
    reg, json_mtime = load_registry()

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
    except OSError:
        mtime = 0.0

    assignments = reg["assignments"]
    return {
        "env_file": path.name,
        "env_mtime": mtime,
        "json_mtime": json_mtime,
        "embed_dim": config.RAG_EMBED_DIM,
        "providers": [_provider_public(reg, p) for p in reg["providers"]],
        "assignments": assignments,
        "slots": {
            "text": {
                "provider": _label_for(val("LLM_BASE_URL")),
                "base_url": val("LLM_BASE_URL"),
                "key_set": bool(text_key),
                "flash": flash,
                "think": think,
                "think_follows_flash": not val("LLM_DEFAULT_THINK"),
            },
            "embed": {
                "provider": _label_for(val("LLM_EMBED_BASE_URL")),
                "base_url": val("LLM_EMBED_BASE_URL"),
                "key_set": bool(val("LLM_EMBED_API_KEY")),
                "uses_text_key": not val("LLM_EMBED_API_KEY"),
                "model": embed_model,
            },
            "vision": {
                "mode": vision_mode,
                "provider": _label_for(val("LLM_VISION_BASE_URL") or val("LLM_BASE_URL"))
                if vision_mode == "dedicated" else _label_for(val("LLM_BASE_URL")),
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

def _provider_credentials(pid: str) -> tuple[str, str]:
    reg, _ = load_registry()
    provider = _provider_by_id(reg, pid or "")
    if provider is None:
        raise LLMAdminError("Unknown provider.")
    return provider.get("base_url", ""), _env_value(provider.get("key_var", ""))


def _vetted_base_url(base_url: str) -> None:
    if base_url and not _VALUE_RE.match(base_url):
        raise LLMAdminError("Base URL contains unsafe characters.")
    if base_url and not web_search.url_targets_public_host(base_url):
        raise LLMAdminError("That endpoint is not a public address — internal hosts cannot be probed.")


def _probe_fail(message: str, state: str = "error") -> dict:
    return {"ok": False, "state": state, "error": message[:400]}


def _classify_failure(exc: Exception) -> tuple[str, str]:
    """(state, message) — offline means the endpoint could not be reached at
    all; error means it answered but rejected the request."""
    try:
        from openai import APIConnectionError
        if isinstance(exc, APIConnectionError):
            return "offline", f"Cannot reach the endpoint: {exc}"
    except Exception:  # noqa: BLE001 — openai unavailable; keep generic state
        pass
    return "error", f"Endpoint rejected the request: {exc}"


def probe(payload: dict) -> dict:
    """One validation call with the form's current (possibly unsaved) values.

    A provider_id makes the server fall back to that provider's stored base
    URL and key, so Test works without retyping anything. Keys are used once
    and dropped.
    """
    try:
        slot = (payload.get("slot") or "").strip()
        if slot not in _SLOTS and slot != "provider":
            return _probe_fail("Unknown slot.")
        if slot == "provider":
            pid = (payload.get("provider_id") or "").strip()
            base_url = (payload.get("base_url") or "").strip()
            if pid:
                saved_base, saved_key = _provider_credentials(pid)
                base_url = base_url or saved_base
            else:
                saved_key = ""
            api_key = (payload.get("api_key") or "").strip() or saved_key
            return _probe_models(base_url, api_key, (payload.get("model") or "").strip())

        if slot == "search":
            return _probe_tavily(_effective(payload, "api_key", "WEB_SEARCH_API_KEY"))
        if slot == "embed":
            model = (payload.get("model") or "").strip() or _saved_values().get("LLM_EMBED_MODEL", "")
            base_url, api_key = _slot_credentials(payload, slot)
            if not model:
                return _probe_fail("Enter an embedding model first.")
            return _probe_embedding(base_url, api_key, model)
        base_url, api_key = _slot_credentials(payload, slot)
        return _probe_models(base_url, api_key, (payload.get("model") or "").strip())
    except LLMAdminError as exc:
        return _probe_fail(str(exc))
    except Exception as exc:  # noqa: BLE001 — surface any provider failure safely
        return _probe_fail(f"Probe failed: {exc}")


def _slot_credentials(payload: dict, slot: str) -> tuple[str, str]:
    """Effective base URL + key for a slot probe: form values first, then the
    payload's provider_id, then the saved slot variables."""
    pid = (payload.get("provider_id") or "").strip()
    base_url = (payload.get("base_url") or "").strip()
    api_key = (payload.get("api_key") or "").strip()
    if pid:
        saved_base, saved_key = _provider_credentials(pid)
        base_url = base_url or saved_base
        api_key = api_key or saved_key
        return base_url, api_key
    if slot == "embed":
        fallback_base, fallback_key = "LLM_EMBED_BASE_URL", "LLM_EMBED_API_KEY"
    elif slot == "vision":
        fallback_base, fallback_key = "LLM_VISION_BASE_URL", "LLM_VISION_API_KEY"
    else:
        fallback_base, fallback_key = "LLM_BASE_URL", "LLM_API_KEY"
    base_url = base_url or _saved_values().get(fallback_base, "")
    api_key = api_key or _env_value(fallback_key) or os.environ.get("LLM_API_KEY", "")
    return base_url, api_key


def _effective(payload: dict, field: str, saved_key: str) -> str:
    return (payload.get(field) or "").strip() or _env_value(saved_key)


def _probe_models(base_url: str, api_key: str, model: str) -> dict:
    _vetted_base_url(base_url)
    if not api_key:
        return _probe_fail("No API key configured for this slot.", "offline")
    try:
        from openai import OpenAI  # lazy, matching llm_client
    except Exception as exc:
        return _probe_fail(f"openai package unavailable: {exc}")
    try:
        client = OpenAI(api_key=api_key, base_url=base_url or None,
                        timeout=_PROBE_TIMEOUT_SECONDS, max_retries=0)
        resp = client.models.list()
    except Exception as exc:  # noqa: BLE001 — provider/auth/network errors
        state, message = _classify_failure(exc)
        return _probe_fail(message, state)
    ids = sorted({m.id for m in resp.data if getattr(m, "id", None)})[:200]
    return {
        "ok": True,
        "state": "online",
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


# ── save (validate → gate → write env → mirror to registry → recycle) ───────

def apply_slot(payload: dict, expected_env_mtime: float | None = None,
               expected_json_mtime: float | None = None) -> dict:
    """Persist one slot's assignment. The registry records the provider link;
    the env file receives the resolved flat variables llm_client reads."""
    slot = (payload.get("slot") or "").strip()
    if slot not in _SLOTS:
        raise LLMAdminError("Unknown slot.")
    reg, _ = load_registry()
    saved = _saved_values()
    updates, assignment = _build_updates(slot, payload, reg, saved)

    if slot == "embed" and updates.get("LLM_EMBED_MODEL", saved.get("LLM_EMBED_MODEL", "")) != saved.get("LLM_EMBED_MODEL", ""):
        _gate_embedding_dimension(payload, updates, reg)

    env_updates = dict(updates)
    env_updates.update(_normalize_provider_keys(reg))
    _write_env(env_updates, expected_mtime=expected_env_mtime)
    reg["assignments"][slot] = assignment
    json_mtime = _atomic_write_json(_registry_path(), reg, expected_json_mtime)
    # Mirror the file into this worker so the change is live before any restart.
    os.environ.update(env_updates)
    restarted = version_service.request_graceful_restart()
    return {
        "ok": True,
        "restarted": restarted,
        "satellite_notice": slot in _SATELLITE_SLOTS,
        "json_mtime": json_mtime,
        "snap": snapshot(),
    }


def _build_updates(slot: str, payload: dict, reg: dict, saved: dict) -> tuple[dict, dict]:
    def field(name: str) -> str:
        value = (payload.get(name) or "").strip()
        _validate_value(name, value)
        return value

    def require_provider(pid: str) -> dict:
        provider = _provider_by_id(reg, pid)
        if provider is None:
            raise LLMAdminError("Choose a provider first (save it if it is new).")
        return provider

    def require_model(provider: dict, model_id: str, slot_name: str) -> str:
        """The model must be one the provider lists, with a role the slot uses."""
        if not model_id:
            raise LLMAdminError(f"Choose a model for the {slot_name} slot.")
        allowed = _SLOT_MODEL_ROLES[slot_name]
        if _find_model(provider, model_id, allowed) is None:
            raise LLMAdminError(
                f"{model_id} is not configured on that provider for the {slot_name} slot "
                "(roles: " + "/".join(allowed) + ") — add it on the provider first."
            )
        return model_id

    if slot == "text":
        provider = require_provider(field("provider_id"))
        flash = require_model(provider, field("flash"), "text")
        think = field("think")
        if think:
            require_model(provider, think, "text")  # charset + role check when not following Flash
        updates = {
            "LLM_BASE_URL": provider.get("base_url", ""),
            "LLM_API_KEY": _env_value(provider.get("key_var", "")),
            "LLM_DEFAULT_FLASH": flash,
            "LLM_DEFAULT_THINK": think,
        }
        return updates, {"provider_id": provider["id"], "flash": flash, "think": think}

    if slot == "embed":
        provider = require_provider(field("provider_id"))
        model = require_model(provider, field("model"), "embed")
        updates = {
            "LLM_EMBED_BASE_URL": provider.get("base_url", ""),
            "LLM_EMBED_API_KEY": _env_value(provider.get("key_var", "")),
            "LLM_EMBED_MODEL": model,
        }
        return updates, {"provider_id": provider["id"], "model": model}

    if slot == "vision":
        mode = field("mode")
        if mode == "disabled":
            updates = {"LLM_VISION": "", "LLM_VISION_API_KEY": "", "LLM_VISION_BASE_URL": ""}
            return updates, {"mode": "disabled", "provider_id": None, "model": ""}
        model = field("model")
        if not model:
            raise LLMAdminError("A vision model is required unless vision is disabled.")
        if mode == "text":
            # Same as the Text provider: dedicated creds must be cleared so
            # llm_client falls back to the chat credentials, and the model must
            # be a multimodal model on the provider the Text slot uses.
            text_pid = (reg["assignments"].get("text") or {}).get("provider_id") or ""
            provider = require_provider(text_pid)
            model = require_model(provider, model, "vision")
            updates = {"LLM_VISION": model, "LLM_VISION_API_KEY": "", "LLM_VISION_BASE_URL": ""}
            return updates, {"mode": "text", "provider_id": None, "model": model}
        if mode == "dedicated":
            provider = require_provider(field("provider_id"))
            model = require_model(provider, model, "vision")
            updates = {
                "LLM_VISION": model,
                "LLM_VISION_BASE_URL": provider.get("base_url", ""),
                "LLM_VISION_API_KEY": _env_value(provider.get("key_var", "")),
            }
            return updates, {"mode": "dedicated", "provider_id": provider["id"], "model": model}
        raise LLMAdminError("Unknown vision mode.")

    # slot == "search"
    api_key = field("api_key")
    return {"WEB_SEARCH_API_KEY": api_key}, {}  # empty clears → web toggle hidden


def _gate_embedding_dimension(payload: dict, updates: dict, reg: dict) -> None:
    """Refuse an embedding-model change unless a live probe matches RAG_EMBED_DIM."""
    result = probe({
        "slot": "embed",
        "provider_id": payload.get("provider_id") or "",
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
