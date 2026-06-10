"""Academic journals: DB-backed storage helpers."""
from __future__ import annotations

from db import db_session
from models import JournalModel


# ==================== JOURNALS HELPERS ====================

def load_journals() -> list:
    """Load journals as list of dicts from JSON."""
    with db_session() as db:
        journals = db.query(JournalModel).all()
        return [{
            "id": j.id,
            "name": j.name,
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
                cover_image=j.get("cover_image"),
                introduction=j.get("introduction"),
                created_at=j.get("created_at")
            ))
        db.commit()


def get_journal_by_id(journal_id: str) -> dict | None:
    for j in load_journals():
        if j.get("id") == journal_id:
            return j
    return None


def get_journal_names() -> list:
    """Return a flat list of journal names for dropdowns."""
    return [j["name"] for j in load_journals()]


def get_journal_id_map() -> dict:
    """Return a dict mapping journal name -> journal id."""
    return {j["name"]: j["id"] for j in load_journals()}
