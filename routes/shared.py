"""HTTP helpers shared across domain route modules."""
from flask import request


def is_partial_request():
    """True when the request carries X-Partial-Content: 1.

    Used by routes to render either the full base.html shell or just the
    inner content block via _bare.html, so the dashboard can fetch a route
    and swap its content into the main panel.
    """
    return request.headers.get("X-Partial-Content") == "1"
