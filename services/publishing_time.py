"""Naive-UTC values for database persistence."""
from datetime import datetime, timezone


def utc_now_db() -> datetime:
    """Return the current UTC wall clock as the project's naive DB value."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def require_db_utc(value: datetime) -> datetime:
    """Reject aware values: database timestamps are always naive UTC."""
    if value.tzinfo is not None and value.utcoffset() is not None:
        raise ValueError("database UTC datetimes must be naive")
    return value


def utc_iso_z(value: datetime) -> str:
    """Serialize a validated naive-UTC database timestamp for external use."""
    return f"{require_db_utc(value).isoformat()}Z"
