"""Central LLM client + model resolution for the Keydion app.

Chat models use LLM_API_KEY / LLM_BASE_URL. Embeddings can use a SEPARATE
provider (e.g. Gemini's OpenAI-compatible endpoint) via LLM_EMBED_API_KEY /
LLM_EMBED_BASE_URL, falling back to the chat credentials when unset.

The conversation interface — chat() / chat_json() — is the seam every
non-streaming caller crosses: model tier, client construction, the provider
wire shape, response unwrap, JSON salvage, and deadline authority all live
behind it. Streaming callers (the Ask turn) build their own client and parse
chunks themselves.
"""

from __future__ import annotations

import json
import logging
import os
import re

from services.publishing_contracts import (
    IndexDeadlineExceeded,
    raise_deadline_if_expired as _raise_deadline_if_expired,
    remaining_timeout as _remaining_timeout,
)

_log = logging.getLogger(__name__)


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


# ── conversation interface ───────────────────────────────────────────────────
#
# One seam for every one-shot LLM call. Callers hand over fully assembled
# messages (plain system/user strings, or content-parts lists for vision) and
# receive text or a parsed dict; everything provider-shaped stays below.

class LLMChatError(Exception):
    """A chat completion failed behind llm_client's conversation interface."""


class LLMChatUnavailable(LLMChatError):
    """No API key configured, or the openai package is not installed."""


class LLMChatRequestError(LLMChatError):
    """The provider request failed (an exhausted deadline already won)."""


class LLMChatParseError(LLMChatError):
    """The response was unreadable, or not the requested JSON object."""


def _tier_model(tier: str) -> str:
    if tier == "flash":
        return flash_model()
    if tier == "think":
        return think_model()
    if tier == "vision":
        return vision_model()
    raise ValueError(f"unknown chat tier: {tier!r}")


def _tier_client(tier: str, deadline: float | None):
    if tier == "vision":
        try:
            return build_vision_client(deadline=deadline)
        except IndexDeadlineExceeded:
            raise
        except Exception as exc:
            _raise_deadline_if_expired(deadline, exc)
            raise
    if not llm_enabled():
        raise LLMChatUnavailable("AI assist is not configured.")
    try:
        return build_client(deadline=deadline)
    except ImportError as exc:  # openai not installed
        raise LLMChatUnavailable("openai package is not installed.") from exc
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise


def chat(messages, *, tier: str, temperature: float = 0.0,
         deadline: float | None = None, response_format=None, client=None) -> str:
    """One-shot chat completion; returns the response text.

    Deadline authority: an exhausted deadline raises IndexDeadlineExceeded —
    including when the provider error that surfaces was caused by it. Every
    other failure raises the LLMChat* family for the caller to translate.
    """
    if client is None:
        client = _tier_client(tier, deadline)
    request = dict(model=_tier_model(tier), temperature=temperature, messages=messages)
    if response_format is not None:
        request["response_format"] = response_format
    if deadline is not None:
        request["timeout"] = _remaining_timeout(deadline)
    try:
        resp = client.chat.completions.create(**request)
        _remaining_timeout(deadline)
    except IndexDeadlineExceeded:
        raise
    except Exception as exc:  # network/auth/rate-limit from any provider
        _raise_deadline_if_expired(deadline, exc)
        # Log full detail server-side; never echo provider internals to the client.
        _log.exception("LLM chat request failed")
        raise LLMChatRequestError("AI request failed — please try again later.") from exc
    try:
        return (resp.choices[0].message.content or "").strip()
    except (AttributeError, IndexError, TypeError) as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise LLMChatParseError("The AI response could not be parsed.") from exc


def _parse_json(content: str):
    """Parse JSON, tolerating a model that wraps it in prose. Returns dict or None."""
    if not content:
        return None
    try:
        return json.loads(content)
    except (ValueError, TypeError):
        pass
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except ValueError:
        return None


def chat_json(messages, *, tier: str, temperature: float = 0.0,
              deadline: float | None = None, client=None) -> dict:
    """One-shot chat completion in JSON mode; returns the parsed JSON object."""
    text = chat(messages, tier=tier, temperature=temperature, deadline=deadline,
                response_format={"type": "json_object"}, client=client)
    data = _parse_json(text)
    _remaining_timeout(deadline)
    if not isinstance(data, dict):  # valid JSON can still be a list / str / number
        raise LLMChatParseError("The AI response could not be parsed.")
    return data
