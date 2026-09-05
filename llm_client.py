"""Central LLM client + model resolution for the Keydion app.

All model requests send purpose aliases to Cloudflare using one server credential.

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

import config
import llm_worker

from services.publishing_contracts import (
    IndexDeadlineExceeded,
    raise_deadline_if_expired as _raise_deadline_if_expired,
    remaining_timeout as _remaining_timeout,
)

_log = logging.getLogger(__name__)


def flash_model() -> str:
    return "flash"


def think_model() -> str:
    return "think"


def embed_model() -> str:
    return "embed:" + llm_worker.embedding_id()


def vision_model() -> str:
    return "vision" if vision_enabled() else ""


def embed_batch_size() -> int:
    """Maximum inputs per Worker embedding request; defaults to 10."""
    try:
        return max(1, int(os.environ.get("LLM_EMBED_BATCH", "10")))
    except ValueError:
        return 10


def llm_enabled() -> bool:
    return llm_worker.purpose_enabled("flash") or llm_worker.purpose_enabled("think")


def embedding_enabled() -> bool:
    """Embedding capability requires a matching index identity and dimensions."""
    return llm_worker.purpose_enabled("embed")


def _new_client(api_key: str, base_url, *, deadline: float | None = None,
                default_headers=None):
    _remaining_timeout(deadline)
    try:
        from openai import OpenAI  # imported lazily so import errors surface at call time
    except Exception as exc:
        _raise_deadline_if_expired(deadline, exc)
        raise
    kwargs = {"api_key": api_key, "base_url": base_url}
    # The shared token belongs only to the Worker; never follow SDK redirects.
    from openai import DefaultHttpxClient
    kwargs.update(http_client=DefaultHttpxClient(follow_redirects=False),
                  timeout=90.0, max_retries=0,
                  default_headers={"User-Agent": "Keydion/llm-worker", **(default_headers or {})})
    if deadline is not None:
        kwargs.update(timeout=_remaining_timeout(deadline), max_retries=0)
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
    """Chat client authenticated only to the configured Worker."""
    return _new_client(*llm_worker.credentials(), deadline=deadline)


def build_embed_client(*, deadline: float | None = None):
    """Embedding client with the server's expected vector dimensions."""
    return _new_client(*llm_worker.credentials(), deadline=deadline, default_headers={
        "X-Keydion-Embed-Dim": str(config.RAG_EMBED_DIM),
    })


def build_vision_client(*, deadline: float | None = None):
    """Vision client using the same bounded Worker transport."""
    return _new_client(*llm_worker.credentials(), deadline=deadline)


def vision_enabled() -> bool:
    return llm_worker.purpose_enabled("vision")


# ── conversation interface ───────────────────────────────────────────────────
#
# One seam for every one-shot LLM call. Callers hand over fully assembled
# messages (plain system/user strings, or content-parts lists for vision) and
# receive text or a parsed dict; everything provider-shaped stays below.

class LLMChatError(Exception):
    """A chat completion failed behind llm_client's conversation interface."""


class LLMChatUnavailable(LLMChatError):
    """Worker capability unavailable, or the openai package is not installed."""


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
