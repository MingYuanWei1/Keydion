"""Central LLM client + model resolution for the Keydion app.

Chat models use LLM_API_KEY / LLM_BASE_URL. Embeddings can use a SEPARATE
provider (e.g. Gemini's OpenAI-compatible endpoint) via LLM_EMBED_API_KEY /
LLM_EMBED_BASE_URL, falling back to the chat credentials when unset.
"""

from __future__ import annotations

import os


def flash_model() -> str:
    return os.environ.get("LLM_DEFAULT_FLASH") or "gpt-4o-mini"


def think_model() -> str:
    return os.environ.get("LLM_DEFAULT_THINK") or flash_model()


def embed_model() -> str:
    return os.environ.get("LLM_EMBED_MODEL") or "gemini-embedding-001"


def embed_batch_size() -> int:
    """Max inputs per embeddings request. Some providers cap this (DashScope: 10);
    OpenAI allows far more. Tune via LLM_EMBED_BATCH. Defaults to a safe 10."""
    try:
        return max(1, int(os.environ.get("LLM_EMBED_BATCH", "10")))
    except ValueError:
        return 10


def llm_enabled() -> bool:
    return bool(os.environ.get("LLM_API_KEY"))


def _embed_credentials() -> tuple[str, str | None]:
    """(api_key, base_url) for the embedding provider, falling back to chat vars."""
    api_key = os.environ.get("LLM_EMBED_API_KEY") or os.environ.get("LLM_API_KEY") or ""
    base_url = os.environ.get("LLM_EMBED_BASE_URL") or os.environ.get("LLM_BASE_URL") or None
    return api_key, base_url


def _new_client(api_key: str, base_url):
    from openai import OpenAI  # imported lazily so import errors surface at call time
    return OpenAI(api_key=api_key, base_url=base_url)


def build_client():
    """OpenAI-compatible CHAT client from LLM_API_KEY / LLM_BASE_URL."""
    api_key = os.environ.get("LLM_API_KEY") or ""
    base_url = os.environ.get("LLM_BASE_URL") or None
    return _new_client(api_key, base_url)


def build_embed_client():
    """Embedding client from LLM_EMBED_* (fallback to chat vars)."""
    api_key, base_url = _embed_credentials()
    return _new_client(api_key, base_url)
