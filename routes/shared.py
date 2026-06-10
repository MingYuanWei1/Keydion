"""HTTP helpers shared across domain route modules."""
import math
from typing import Dict, List, Optional

from flask import request


def is_partial_request():
    """True when the request carries X-Partial-Content: 1.

    Used by routes to render either the full base.html shell or just the
    inner content block via _bare.html, so the dashboard can fetch a route
    and swap its content into the main panel.
    """
    return request.headers.get("X-Partial-Content") == "1"


def paginate_records(records: List[Dict[str, str]], page: int, per_page: int = 20) -> Dict[str, Optional[int]]:
    total = len(records)
    total_pages = max(1, math.ceil(total / per_page)) if total else 1
    current_page = max(1, min(page, total_pages))
    start = (current_page - 1) * per_page
    end = start + per_page
    items = records[start:end]
    return {
        "items": items,
        "total": total,
        "page": current_page,
        "pages": total_pages,
        "per_page": per_page,
        "has_prev": current_page > 1,
        "has_next": current_page < total_pages,
        "prev_page": current_page - 1 if current_page > 1 else None,
        "next_page": current_page + 1 if current_page < total_pages else None,
    }
