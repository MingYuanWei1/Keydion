"""Flask-only translation helpers for the publishing lifecycle."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlsplit

from flask import current_app, flash, jsonify, redirect, request, url_for

from services.auth import get_active_user

from services.publishing_contracts import (
    Actor,
    AliasConflict,
    DecisionConflict,
    Forbidden,
    IdempotencyConflict,
    InvalidInput,
    NotFound,
    PersistenceFailed,
    StaleVersion,
    StorageFailed,
    SubmissionNotPending,
)


def _l(message: str) -> str:
    """Mark a fixed response for catalog extraction without eager translation."""
    return message


_STALE_MESSAGE = _l(
    "This paper changed while you were editing it. Reload and try again."
)
_UNAVAILABLE_MESSAGE = "Publishing is temporarily unavailable. Please try again."
_ERROR_SPECS = (
    (InvalidInput, 422, "invalid_input", "Please correct the highlighted fields."),
    (Forbidden, 403, "forbidden", "You do not have permission to perform this action."),
    (NotFound, 404, "not_found", "The requested publishing record was not found."),
    (StaleVersion, 409, "stale_version", _STALE_MESSAGE),
    (
        SubmissionNotPending,
        409,
        "submission_not_pending",
        _l("Only a pending submission can be cancelled."),
    ),
    (
        DecisionConflict,
        409,
        "decision_conflict",
        "This submission decision conflicts with a newer change.",
    ),
    (
        IdempotencyConflict,
        409,
        "idempotency_conflict",
        "This request conflicts with an earlier publishing request.",
    ),
    (
        AliasConflict,
        409,
        "alias_conflict",
        "A paper with that public filename already exists.",
    ),
    (StorageFailed, 503, "storage_failed", _UNAVAILABLE_MESSAGE),
    (PersistenceFailed, 503, "persistence_failed", _UNAVAILABLE_MESSAGE),
)


def lifecycle_from_app():
    """Return the one lifecycle installed by the application factory."""
    return current_app.extensions["publishing_lifecycle"]


def actor_from_session() -> Actor:
    """Map the request's server-hydrated account into a domain Actor.

    Authentication and live-token validation remain the calling route's job;
    this adapter deliberately neither queries users nor refreshes role state.
    """
    user = get_active_user()
    if not isinstance(user, Mapping):
        raise Forbidden()
    username = user.get("username")
    role = user.get("role")
    if (
        type(username) is not str
        or not username
        or username != username.strip()
        or len(username) > 255
        or type(role) is not str
        or role not in {"1", "2", "3"}
    ):
        raise Forbidden()
    return Actor(user_id=username, role=int(role))


def _wants_json_response() -> bool:
    if request.is_json:
        return True
    if request.headers.get("X-Requested-With", "").casefold() == "xmlhttprequest":
        return True
    accepted = request.accept_mimetypes
    json_quality = accepted["application/json"]
    html_quality = accepted["text/html"]
    return json_quality > 0 and json_quality > html_quality


def _error_spec(error):
    for error_type, status, code, message in _ERROR_SPECS:
        if isinstance(error, error_type):
            return status, code, message
    raise error


def _renderer_payload(payload: dict[str, object]) -> dict[str, object]:
    copied = dict(payload)
    field_errors = copied.get("field_errors")
    if isinstance(field_errors, dict):
        copied["field_errors"] = dict(field_errors)
    return copied


def _local_redirect_target(
    endpoint: str | None,
    values: Mapping[str, str | int] | None,
) -> str:
    if (
        type(endpoint) is not str
        or not endpoint
        or endpoint.startswith(("/", "\\"))
        or "://" in endpoint
    ):
        raise ValueError("redirect endpoint must be a local Flask endpoint")
    copied_values: dict[str, str | int] = {}
    if values is not None:
        if not isinstance(values, Mapping):
            raise ValueError("redirect values must be a mapping")
        for key, value in values.items():
            if (
                type(key) is not str
                or not key
                or key.startswith("_")
                or type(value) not in (str, int)
            ):
                raise ValueError("redirect values are invalid")
            copied_values[key] = value
    target = url_for(endpoint, **copied_values)
    parsed = urlsplit(target)
    if (
        parsed.scheme
        or parsed.netloc
        or not target.startswith("/")
        or target.startswith("//")
        or "\\" in target
    ):
        raise ValueError("redirect target must remain local")
    return target


def lifecycle_error_response(
    error,
    *,
    redirect_endpoint: str | None = None,
    redirect_values: Mapping[str, str | int] | None = None,
    html_renderer=None,
):
    """Translate a known lifecycle failure without exposing its details.

    HTML callers may either provide a server-selected endpoint plus validated
    route values, or a trusted renderer accepting ``(sanitized_error, status)``.
    """
    status, code, message = _error_spec(error)
    message = str(message)
    payload = {
        "code": code,
        "message": message,
    }
    if isinstance(error, InvalidInput):
        payload["field_errors"] = {
            key: value
            for key, value in error.field_errors.items()
            if type(key) is str and type(value) is str
        }
    elif (
        isinstance(error, StaleVersion)
        and type(error.current_version) is int
        and error.current_version > 0
    ):
        payload["current_version"] = error.current_version
    if _wants_json_response():
        return jsonify(error=payload), status
    if html_renderer is not None:
        if not callable(html_renderer):
            raise ValueError("HTML renderer must be callable")
        return html_renderer(_renderer_payload(payload), status)
    target = _local_redirect_target(redirect_endpoint, redirect_values)
    flash(message, "danger")
    return redirect(target, code=303)
