"""Academic journals: DB-backed storage helpers."""
from __future__ import annotations

import re

from db import db_session
from models import JournalModel


# ==================== JOURNALS HELPERS ====================

def load_journals() -> list:
    """Load journals as list of dicts."""
    with db_session() as db:
        journals = db.query(JournalModel).all()
        return [{
            "id": j.id,
            "name": j.name,
            "slug": j.slug,
            "cover_image": j.cover_image,
            "introduction": j.introduction,
            "created_at": j.created_at,
        } for j in journals]


def save_journals(journals: list) -> None:
    with db_session() as db:
        db.query(JournalModel).delete()
        for j in journals:
            db.add(JournalModel(
                id=j.get("id"),
                name=j.get("name"),
                slug=j.get("slug"),
                cover_image=j.get("cover_image"),
                introduction=j.get("introduction"),
                created_at=j.get("created_at"),
            ))
        db.commit()


def get_journal_by_id(journal_id: str) -> dict | None:
    for j in load_journals():
        if j.get("id") == journal_id:
            return j
    return None


def get_journal_by_slug(slug: str) -> dict | None:
    for j in load_journals():
        if j.get("slug") == slug:
            return j
    return None


def get_journal_names() -> list:
    """Return a flat list of journal names for dropdowns."""
    return [j["name"] for j in load_journals()]


def get_journal_id_map() -> dict:
    """Return a dict mapping journal name -> journal id."""
    return {j["name"]: j["id"] for j in load_journals()}


def get_journal_slug_map() -> dict:
    """Return a dict mapping journal name -> journal slug."""
    return {j["name"]: j["slug"] for j in load_journals() if j.get("slug")}


# ---------- Slug generation ----------

def slugify(name: str) -> str:
    """Name -> URL slug: collapse whitespace to '_', keep [A-Za-z0-9_-],
    drop everything else, preserve case. Non-ASCII names yield ''."""
    s = (name or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_-]", "", s)
    return s.strip("_-")


def _unique_slug(base: str, existing) -> str:
    """Return `base`, or `base_2`/`base_3`/... if it collides with `existing`."""
    base = base or ""
    if base not in existing:
        return base
    i = 2
    while f"{base}_{i}" in existing:
        i += 1
    return f"{base}_{i}"


def set_unique_slug(name: str, exclude_id: str | None = None, fallback: str = "journal") -> str:
    """Compute a unique slug for `name`, ignoring the journal `exclude_id`."""
    base = slugify(name) or fallback
    existing = {j["slug"] for j in load_journals()
                if j.get("slug") and j.get("id") != exclude_id}
    return _unique_slug(base, existing)


def ensure_journal_slugs() -> None:
    """Backfill slugs for any journals missing one (migration helper)."""
    journals = load_journals()
    existing = {j["slug"] for j in journals if j.get("slug")}
    changed = False
    for j in journals:
        if not j.get("slug"):
            base = slugify(j.get("name") or "") or (j.get("id") or "journal")
            s = _unique_slug(base, existing)
            j["slug"] = s
            existing.add(s)
            changed = True
    if changed:
        save_journals(journals)


def get_journal_paper_counts() -> dict:
    """Return a dict mapping journal name -> number of published papers."""
    from services.papers import load_paper_metadata
    counts: dict = {}
    for row in load_paper_metadata():
        jn = (row.get("journal") or "").strip()
        if jn:
            counts[jn] = counts.get(jn, 0) + 1
    return counts


def get_recent_journals(limit: int = 4) -> list:
    """Most-recently-created journals, each annotated with `paper_count`."""
    journals = sorted(load_journals(),
                      key=lambda j: j.get("created_at") or "", reverse=True)[:limit]
    counts = get_journal_paper_counts()
    for j in journals:
        j["paper_count"] = counts.get(j["name"], 0)
    return journals
