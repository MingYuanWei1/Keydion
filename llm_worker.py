"""Worker connection and short-lived capability discovery; no provider secrets."""

import config  # Load the selected environment before reading it.
import json
import os
import re
import threading
import time
from urllib.parse import urlsplit

import httpx

_cache = {}
_lock = threading.Lock()
CAPABILITY_TTL = 15.0


def enabled() -> bool:
    """Existing installations stay direct until explicitly switched."""
    mode = os.environ.get("LLM_TRANSPORT", "direct").strip()
    if mode not in {"direct", "worker"}:
        raise ValueError("LLM_TRANSPORT must be direct or worker")
    return mode == "worker"


def credentials() -> tuple[str, str]:
    base = os.environ.get("LLM_WORKER_URL", "").strip().rstrip("/")
    token = os.environ.get("LLM_WORKER_TOKEN", "").strip()
    parts = urlsplit(base)
    local = parts.scheme == "http" and parts.hostname in {"localhost", "127.0.0.1", "::1"}
    if (not token or not parts.hostname or (parts.scheme != "https" and not local)
            or parts.username or parts.password or parts.query or parts.fragment
            or parts.path not in {"", "/"}):
        raise ValueError("Configure LLM_WORKER_URL as an HTTPS origin and LLM_WORKER_TOKEN")
    return token, base + "/v1"


def embedding_id() -> str:
    return os.environ.get("LLM_WORKER_EMBED_ID", "").strip()


def capabilities(*, refresh=False) -> dict:
    """Fail closed, including after an expired successful discovery.

    Cache by connection so changing the configured token cannot reuse an old
    authenticated result. The status endpoint checks configuration, not inference.
    """
    try:
        token, base = credentials()
    except ValueError:
        return {"available": False, "purposes": {}}
    key = (base, token)
    with _lock:
        cached = _cache.get(key)
        if not refresh and cached and time.monotonic() < cached[0]:
            return cached[1]
        result = {"available": False, "purposes": {}}
        try:
            with httpx.Client(timeout=2.0, follow_redirects=False) as client:
                with client.stream("GET", base + "/capabilities", headers={
                    "Authorization": f"Bearer {token}",
                }) as response:
                    response.raise_for_status()
                    raw = bytearray()
                    for chunk in response.iter_bytes():
                        raw.extend(chunk)
                        if len(raw) > 32768:
                            raise ValueError("Capability response too large")
            data = json.loads(raw)
            purposes = {}
            for name in ("flash", "think", "vision", "embed"):
                entry = data["purposes"][name]
                purposes[name] = {
                    "enabled": entry.get("enabled") is True,
                    "model": str(entry.get("model", ""))[:256],
                    "embedding_id": str(entry.get("embedding_id", ""))[:64],
                    "dimensions": entry.get("dimensions"),
                }
            result = {"available": True, "purposes": purposes}
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
            pass
        _cache.clear()
        _cache[key] = (time.monotonic() + CAPABILITY_TTL, result)
        return result


def purpose_enabled(purpose: str) -> bool:
    entry = capabilities()["purposes"].get(purpose, {})
    if purpose == "embed":
        pin = embedding_id()
        return bool(entry.get("enabled") and re.fullmatch(r"[a-f0-9]{64}", pin)
                    and entry.get("embedding_id") == pin
                    and entry.get("dimensions") == config.RAG_EMBED_DIM)
    return entry.get("enabled", False)
