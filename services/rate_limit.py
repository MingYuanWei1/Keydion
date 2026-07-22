"""Shared database-backed rate limiting for credentials and expensive work."""

from __future__ import annotations

import hashlib
import hmac
import logging
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy.exc import IntegrityError

from db import db_session
from models import RateLimitBucketModel


_LOG = logging.getLogger(__name__)
_SCOPE = re.compile(r"[a-z0-9][a-z0-9_.:-]{0,63}\Z")


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after: int
    count: int


def _secret() -> bytes:
    value = (
        os.environ.get("PAPERQUERY_RATE_LIMIT_SECRET")
        or os.environ.get("PAPERQUERY_SECRET")
        or ""
    )
    if not value and os.environ.get("PAPERQUERY_ALLOW_DEV_SECRET", "").strip().lower() in {
        "1", "true", "yes", "on",
    }:
        value = "dev-secret-key"
    if not value:
        raise RuntimeError(
            "PAPERQUERY_SECRET or PAPERQUERY_RATE_LIMIT_SECRET is required for rate limiting"
        )
    return value.encode("utf-8")


def _key_hash(scope: str, key: str) -> str:
    return hmac.new(
        _secret(),
        f"{scope}\0{key}".encode("utf-8", "surrogatepass"),
        hashlib.sha256,
    ).hexdigest()


def _validate(scope: str, key: str, limit: int, window_seconds: int) -> None:
    if not isinstance(scope, str) or _SCOPE.fullmatch(scope) is None:
        raise ValueError("invalid rate-limit scope")
    if not isinstance(key, str) or not key:
        raise ValueError("rate-limit key is required")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
        raise ValueError("rate-limit limit must be a non-negative integer")
    if (
        isinstance(window_seconds, bool)
        or not isinstance(window_seconds, int)
        or window_seconds < 1
    ):
        raise ValueError("rate-limit window must be a positive integer")


def consume(
    scope: str,
    key: str,
    *,
    limit: int,
    window_seconds: int,
    base_block_seconds: int = 0,
    max_block_seconds: int = 3600,
    now: datetime | None = None,
) -> RateLimitDecision:
    """Consume one shared bucket allowance and return a fail-closed decision."""
    _validate(scope, key, limit, window_seconds)
    checked_at = now or datetime.utcnow()
    digest = _key_hash(scope, key)
    window = timedelta(seconds=window_seconds)
    with db_session() as database:
        # Opportunistic expiry keeps attacker-controlled key cardinality bounded.
        database.query(RateLimitBucketModel).filter(
            RateLimitBucketModel.expires_at <= checked_at
        ).delete(synchronize_session=False)
        bucket = (
            database.query(RateLimitBucketModel)
            .filter(
                RateLimitBucketModel.scope == scope,
                RateLimitBucketModel.key_hash == digest,
            )
            .with_for_update()
            .one_or_none()
        )
        if bucket is None:
            bucket = RateLimitBucketModel(
                scope=scope,
                key_hash=digest,
                window_started_at=checked_at,
                count=0,
                blocked_until=None,
                expires_at=checked_at + window,
            )
            database.add(bucket)
            try:
                database.flush()
            except IntegrityError:
                # A concurrent creator won.  Retrying the whole operation is
                # safer than attempting to reuse a failed transaction.
                database.rollback()
                return consume(
                    scope,
                    key,
                    limit=limit,
                    window_seconds=window_seconds,
                    base_block_seconds=base_block_seconds,
                    max_block_seconds=max_block_seconds,
                    now=checked_at,
                )
        if checked_at - bucket.window_started_at >= window:
            bucket.window_started_at = checked_at
            bucket.count = 0
            bucket.blocked_until = None
        if bucket.blocked_until is not None and bucket.blocked_until > checked_at:
            retry_after = max(
                1,
                math.ceil((bucket.blocked_until - checked_at).total_seconds()),
            )
            bucket.expires_at = max(bucket.expires_at, bucket.blocked_until)
            return RateLimitDecision(False, retry_after, bucket.count)

        bucket.count += 1
        window_end = bucket.window_started_at + window
        bucket.expires_at = window_end
        if bucket.count <= limit:
            return RateLimitDecision(True, 0, bucket.count)

        if base_block_seconds > 0:
            exponent = min(max(bucket.count - limit - 1, 0), 16)
            delay = min(base_block_seconds * (2 ** exponent), max_block_seconds)
            bucket.blocked_until = checked_at + timedelta(seconds=delay)
            bucket.expires_at = max(window_end, bucket.blocked_until)
            retry_after = delay
        else:
            retry_after = max(1, math.ceil((window_end - checked_at).total_seconds()))
        _LOG.warning("rate limit exceeded scope=%s retry_after=%s", scope, retry_after)
        return RateLimitDecision(False, retry_after, bucket.count)


def clear(scope: str, key: str) -> None:
    _validate(scope, key, 0, 1)
    digest = _key_hash(scope, key)
    with db_session() as database:
        database.query(RateLimitBucketModel).filter(
            RateLimitBucketModel.scope == scope,
            RateLimitBucketModel.key_hash == digest,
        ).delete(synchronize_session=False)
