"""Canonical identity helpers for Paper IDs and filename aliases."""
import unicodedata
import uuid


def normalize_alias_key(filename: str) -> str:
    """Return the single canonical comparison key for a filename alias."""
    return unicodedata.normalize("NFKC", filename).casefold()


def validate_paper_id(value: str) -> str:
    """Validate a UUID Paper ID and return its canonical string form."""
    return str(uuid.UUID(value))
