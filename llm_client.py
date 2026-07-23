"""Central LLM client + model resolution for the Keydion app.

Chat models use LLM_API_KEY / LLM_BASE_URL. Embeddings can use a SEPARATE
provider (e.g. Gemini's OpenAI-compatible endpoint) via LLM_EMBED_API_KEY /
LLM_EMBED_BASE_URL, falling back to the chat credentials when unset.
"""

from __future__ import annotations

import os
import time

from services.publishing_contracts import IndexDeadlineExceeded


def flash_model() -> str:
    return os.environ.get("LLM_DEFAULT_FLASH") or "gpt-4o-mini"


def think_model() -> str:
    return os.environ.get("LLM_DEFAULT_THINK") or flash_model()


def embed_model() -> str:
    return os.environ.get("LLM_EMBED_MODEL") or "gemini-embedding-001"


def vision_model() -> str:
    return os.environ.get("LLM_VISION") or ""


def embed_batch_size() -> int:
    """Max inputs per embeddings request. Some providers cap this (DashScope: 10);
    OpenAI allows far more. Tune via LLM_EMBED_BATCH. Defaults to a safe 10."""
    try:
        return max(1, int(os.environ.get("LLM_EMBED_BATCH", "10")))
    except ValueError:
        return 10


def llm_enabled() -> bool:
    return bool(os.environ.get("LLM_API_KEY"))


def embedding_enabled() -> bool:
    """Whether either the dedicated or fallback chat key can embed text."""
    return bool(_embed_credentials()[0])


def _embed_credentials() -> tuple[str, str | None]:
    """(api_key, base_url) for the embedding provider, falling back to chat vars."""
    api_key = os.environ.get("LLM_EMBED_API_KEY") or os.environ.get("LLM_API_KEY") or ""
    base_url = os.environ.get("LLM_EMBED_BASE_URL") or os.environ.get("LLM_BASE_URL") or None
    return api_key, base_url


def _vision_credentials() -> tuple[str, str | None]:
    """(api_key, base_url) for the vision provider, falling back to chat vars."""
    api_key = os.environ.get("LLM_VISION_API_KEY") or os.environ.get("LLM_API_KEY") or ""
    base_url = os.environ.get("LLM_VISION_BASE_URL") or os.environ.get("LLM_BASE_URL") or None
    return api_key, base_url


def _remaining_timeout(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    remaining = max(float(deadline) - time.monotonic(), 0.0)
    if remaining == 0.0:
        raise IndexDeadlineExceeded()
    return remaining


def _raise_deadline_if_expired(deadline: float | None, error: Exception) -> None:
    if deadline is not None and time.monotonic() >= float(deadline):
        raise IndexDeadlineExceeded() from error


def _new_client(api_key: str, base_url, *, deadline: float | None = None,
                fallback_timeout: float | None = None,
                fallback_max_retries: int | None = None):
    _remaining_timeout(deadline)
    try:
        from openai import OpenAI  # imported lazily so import errors surface at call time
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    kwargs = {"api_key": api_key, "base_url": base_url}
    if deadline is not None:
        kwargs.update(timeout=_remaining_timeout(deadline), max_retries=0)
    else:
        if fallback_timeout is not None:
            kwargs["timeout"] = fallback_timeout
        if fallback_max_retries is not None:
            kwargs["max_retries"] = fallback_max_retries
    try:
        client = OpenAI(**kwargs)
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    _remaining_timeout(deadline)
    return client


def build_client(*, deadline: float | None = None):
    """OpenAI-compatible CHAT client from LLM_API_KEY / LLM_BASE_URL."""
    api_key = os.environ.get("LLM_API_KEY") or ""
    base_url = os.environ.get("LLM_BASE_URL") or None
    return _new_client(api_key, base_url, deadline=deadline)


def build_embed_client(*, deadline: float | None = None):
    """Embedding client from LLM_EMBED_* (fallback to chat vars)."""
    api_key, base_url = _embed_credentials()
    return _new_client(api_key, base_url, deadline=deadline)


# Vision calls run synchronously on user-visible paths; when a caller omits
# the deadline, still cap the wait (the SDK default is 600s reads x 2
# retries, ~30 min of blocked worker) and never auto-retry.
VISION_FALLBACK_TIMEOUT = 90.0
VISION_FALLBACK_MAX_RETRIES = 0


def build_vision_client(*, deadline: float | None = None):
    """Vision client from LLM_VISION_* (fallback to chat vars)."""
    api_key, base_url = _vision_credentials()
    return _new_client(api_key, base_url, deadline=deadline,
                       fallback_timeout=VISION_FALLBACK_TIMEOUT,
                       fallback_max_retries=VISION_FALLBACK_MAX_RETRIES)


def vision_enabled() -> bool:
    return bool(vision_model()) and bool(_vision_credentials()[0])
