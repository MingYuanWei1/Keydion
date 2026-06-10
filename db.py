"""Database plumbing: engine, session factory, declarative base.

init_db() (in models.py) populates _ENGINE/_SESSION_LOCAL; nothing here
connects at import time.
"""
import os
from contextlib import contextmanager

from sqlalchemy.orm import declarative_base

import config  # noqa: F401  -- ensures .env/.env.prod loaded before reading env

DB_URL = os.environ.get("PAPERQUERY_DATABASE_URL")
BASE = declarative_base()
_ENGINE = None
_SESSION_LOCAL = None


def get_engine():
    """Current engine (None before init_db). Used by gunicorn.conf.py post_fork."""
    return _ENGINE


@contextmanager
def db_session():
    if _SESSION_LOCAL is None:
        from models import init_db  # deferred: models imports db (BASE) at top level
        init_db()
    session = _SESSION_LOCAL()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
