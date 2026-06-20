"""Guides: storage, sanitization, and category helpers."""
import json
import re
from datetime import datetime
from html.parser import HTMLParser as _HTMLParser

import bleach

from config import (
    GUIDE_CATEGORIES_JSON,
    GUIDE_FIELDS,
    _DEFAULT_GUIDE_CATEGORIES,
)
from db import db_session
from models import GuideModel


# ==================== GUIDE HELPERS ====================

GUIDE_ALLOWED_TAGS = [
    "h1", "h2", "h3", "h4", "p", "strong", "em", "u", "s",
    "ul", "ol", "li", "a", "img", "blockquote", "code", "pre",
    "br", "hr", "span", "div",
]
GUIDE_ALLOWED_ATTRS = {
    "a": ["href", "title", "target", "rel"],
    "img": ["src", "alt", "width", "height", "class"],
    "span": ["class"],
    "div": ["class"],
    "p": ["class"],
}
GUIDE_ALLOWED_PROTOCOLS = ["http", "https"]

# Tags whose entire content (not just the tag itself) must be removed.
_GUIDE_CONTENT_STRIP_TAGS = {"script", "style", "iframe", "object", "embed", "form"}


class _ContentStripper(_HTMLParser):
    """Pre-pass that drops both the tags and their inner text for dangerous elements."""

    def __init__(self):
        super().__init__(convert_charrefs=False)
        self._skip_depth = 0
        self._parts = []

    def handle_starttag(self, tag, attrs):
        if tag in _GUIDE_CONTENT_STRIP_TAGS:
            self._skip_depth += 1
        else:
            self._parts.append(self.get_starttag_text())

    def handle_endtag(self, tag):
        if tag in _GUIDE_CONTENT_STRIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
        elif not self._skip_depth:
            self._parts.append(f"</{tag}>")

    def handle_data(self, data):
        if not self._skip_depth:
            self._parts.append(data)

    def handle_entityref(self, name):
        if not self._skip_depth:
            self._parts.append(f"&{name};")

    def handle_charref(self, name):
        if not self._skip_depth:
            self._parts.append(f"&#{name};")

    def get_result(self):
        return "".join(self._parts)


def _sanitize_guide_html(html: str) -> str:
    """Strip dangerous HTML from a Quill body before storing it."""
    if not html:
        return ""
    # Phase 1: remove content of dangerous tags (script, style, etc.)
    stripper = _ContentStripper()
    stripper.feed(html)
    pre_cleaned = stripper.get_result()
    # Phase 2: use bleach to enforce tag/attribute/protocol allowlists
    cleaned = bleach.clean(
        pre_cleaned,
        tags=GUIDE_ALLOWED_TAGS,
        attributes=GUIDE_ALLOWED_ATTRS,
        protocols=GUIDE_ALLOWED_PROTOCOLS,
        strip=True,
    )
    # Phase 3: force rel="noopener noreferrer" on anchors that open a new
    # target (prevents reverse tabnabbing); preserve any existing rel tokens.
    return _harden_anchor_rels(cleaned)


def _harden_anchor_rels(html: str) -> str:
    """Add noopener/noreferrer to any <a target=...> while keeping existing rel tokens."""
    if "<a" not in html:
        return html
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    changed = False
    for anchor in soup.find_all("a"):
        if not anchor.get("target"):
            continue
        tokens = anchor.get("rel") or []
        for required in ("noopener", "noreferrer"):
            if required not in tokens:
                tokens.append(required)
                changed = True
        anchor["rel"] = tokens
    return str(soup) if changed else html


def _read_guide_form(form) -> dict:
    """Parse a guide POST form into the canonical form_data dict.

    Called from admin_guide_publish (which then validates and persists) and
    admin_guide_preview (which renders the article template without persisting).
    Slug normalization stays in admin_guide_publish since preview tolerates
    a blank or invalid slug.
    """
    return {
        "slug": form.get("slug", "").strip(),
        "category": form.get("category", "").strip(),
        "sort_order": form.get("sort_order", "100").strip() or "100",
        "published": form.get("published") in ("1", "on", "true", "yes"),
        "title_en": form.get("title_en", "").strip(),
        "title_zh": form.get("title_zh", "").strip(),
        "summary_en": form.get("summary_en", "").strip(),
        "summary_zh": form.get("summary_zh", "").strip(),
        "body_en": form.get("body_en", "").strip(),
        "body_zh": form.get("body_zh", "").strip(),
    }


def _load_guide_categories() -> list:
    """Load guide categories from JSON file, seeding from defaults if needed."""
    if GUIDE_CATEGORIES_JSON.exists():
        try:
            return json.loads(GUIDE_CATEGORIES_JSON.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    _save_guide_categories(_DEFAULT_GUIDE_CATEGORIES)
    return list(_DEFAULT_GUIDE_CATEGORIES)


def _save_guide_categories(cats: list) -> None:
    GUIDE_CATEGORIES_JSON.parent.mkdir(parents=True, exist_ok=True)
    GUIDE_CATEGORIES_JSON.write_text(
        json.dumps(cats, ensure_ascii=False, indent=2), encoding="utf-8"
    )


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(text: str) -> str:
    """Lowercase ASCII slug, dash-separated, max 120 chars."""
    if not text:
        return ""
    slug = _SLUG_RE.sub("-", text.lower()).strip("-")
    return slug[:120] or "guide"


def _guide_to_dict(g) -> dict:
    out = {}
    for field in GUIDE_FIELDS:
        val = getattr(g, field)
        if val is None and field not in ("id", "sort_order", "published"):
            val = ""
        out[field] = val
    return out


def _group_guides_for_index(all_guides: list, categories_in_order: list) -> list:
    """Group published guides for the public /guides index.

    Known categories appear first in the order from guide_categories.json,
    then any other named categories alphabetically, then guides with no
    category last. Uncategorized guides MUST still appear — they were
    previously dropped, leaving published-but-uncategorized guides reachable
    by direct URL yet invisible on the index.
    """
    seen = set()
    grouped = []
    for cat in categories_in_order:
        items = [g for g in all_guides if g.get("category") == cat]
        if items:
            grouped.append((cat, items))
            seen.add(cat)
    extras = {}
    uncategorized = []
    for g in all_guides:
        cat = g.get("category") or ""
        if not cat:
            uncategorized.append(g)
        elif cat not in seen:
            extras.setdefault(cat, []).append(g)
    for cat in sorted(extras):
        grouped.append((cat, extras[cat]))
    if uncategorized:
        grouped.append(("", uncategorized))
    return grouped


def load_guides(published_only: bool = True) -> list:
    """Return all guides, ordered by category then sort_order ascending."""
    with db_session() as db:
        q = db.query(GuideModel)
        if published_only:
            q = q.filter(GuideModel.published == True)  # noqa: E712
        guides = q.all()
        rows = [_guide_to_dict(g) for g in guides]
        rows.sort(key=lambda r: (r.get("category") or "", r.get("sort_order") or 0))
        return rows


def get_guide_by_slug(slug: str):
    with db_session() as db:
        g = db.query(GuideModel).filter_by(slug=slug).first()
        return _guide_to_dict(g) if g else None


def get_guide(guide_id: int):
    with db_session() as db:
        g = db.query(GuideModel).filter_by(id=guide_id).first()
        return _guide_to_dict(g) if g else None


def save_guide(data: dict) -> int:
    """Insert a new guide and return its id."""
    now = datetime.utcnow().isoformat()
    payload = {f: data.get(f, "") for f in GUIDE_FIELDS if f != "id"}
    payload["created_at"] = now
    payload["updated_at"] = now
    payload["body_en"] = _sanitize_guide_html(payload.get("body_en", ""))
    payload["body_zh"] = _sanitize_guide_html(payload.get("body_zh", ""))
    raw_pub = payload.get("published")
    if isinstance(raw_pub, str):
        payload["published"] = raw_pub.strip().lower() in ("1", "true", "yes", "on")
    else:
        payload["published"] = bool(raw_pub)
    raw_sort = payload.get("sort_order")
    payload["sort_order"] = int(raw_sort) if raw_sort not in (None, "") else 100
    with db_session() as db:
        g = GuideModel(**payload)
        db.add(g)
        db.commit()
        return g.id


def update_guide(guide_id: int, data: dict) -> bool:
    with db_session() as db:
        g = db.query(GuideModel).filter_by(id=guide_id).first()
        if not g:
            return False
        for field in GUIDE_FIELDS:
            if field in ("id", "created_at"):
                continue
            if field not in data:
                continue
            value = data[field]
            if field in ("body_en", "body_zh"):
                value = _sanitize_guide_html(value or "")
            elif field == "published":
                if isinstance(value, str):
                    value = value.strip().lower() in ("1", "true", "yes", "on")
                else:
                    value = bool(value)
            elif field == "sort_order":
                value = int(value) if value not in (None, "") else 100
            setattr(g, field, value)
        g.updated_at = datetime.utcnow().isoformat()
        db.commit()
        return True


def delete_guide(guide_id: int) -> bool:
    with db_session() as db:
        g = db.query(GuideModel).filter_by(id=guide_id).first()
        if not g:
            return False
        db.delete(g)
        db.commit()
        return True


def slug_exists(slug: str, exclude_id: int = 0) -> bool:
    with db_session() as db:
        q = db.query(GuideModel).filter_by(slug=slug)
        if exclude_id:
            q = q.filter(GuideModel.id != exclude_id)
        return db.query(q.exists()).scalar()
