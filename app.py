from __future__ import annotations
import os
from dotenv import load_dotenv
load_dotenv()

import base64
import binascii
import bleach

import hashlib
import hmac
import json
import math
import os
import secrets
import shutil
from contextlib import contextmanager
from datetime import datetime, timedelta
from html.parser import HTMLParser as _HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
from urllib.parse import urlparse

import re

import msal
import requests
from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, String, Text, Unicode, UnicodeText, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from flask import (
    Flask,
    Response,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    session,
    stream_with_context,
    url_for,
)
from flask_babel import Babel, gettext as _, get_locale, lazy_gettext as _l
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.utils import secure_filename
from ee_pdf_extractor import extract_ee_metadata, EePdfExtractionError
from llm_metadata import generate_abstract_keywords, LLMMetadataError
import llm_client
import rag_index
import web_search


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("PAPERQUERY_DATA_DIR", BASE_DIR / "data")).resolve()
PAPERS_DIR = Path(os.environ.get("PAPERQUERY_UPLOAD_DIR", BASE_DIR / "papers")).resolve()
LOCAL_USER_FIELDS = ["username", "password", "registration_date", "expiry_date", "role", "email", "first_name", "last_name", "school"]
NEWS_FIELDS = ["id", "title", "category", "abstract", "body", "author", "image_url", "published_at", "status"]
GUIDE_FIELDS = [
    "id", "slug", "category", "sort_order", "published",
    "title_en", "title_zh", "summary_en", "summary_zh",
    "body_en", "body_zh", "created_at", "updated_at",
]
GUIDE_CATEGORIES_JSON = DATA_DIR / "guide_categories.json"
_DEFAULT_GUIDE_CATEGORIES = [
    "Getting Started", "Account", "Submissions", "News", "Other",
]
_DEFAULT_NEWS_CATEGORIES = [
    "活动回顾", "期刊发布", "讲座预告", "成果展示",
    "公告通知", "学术动态", "社团新闻", "其他",
]
CATEGORIES_JSON = DATA_DIR / "news_categories.json"
JOURNALS_JSON = DATA_DIR / "paper_journals.json"
_DEFAULT_PAPER_CATEGORIES = ["literature", "natural-science", "social-science", "humanities"]
_DEFAULT_PAPER_JOURNALS: list = []
PENDING_PAPERS_DIR = DATA_DIR / "pending_papers"


def load_categories() -> list:
    """Load categories from JSON file, seeding from defaults if needed."""
    if CATEGORIES_JSON.exists():
        try:
            return json.loads(CATEGORIES_JSON.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    save_categories(_DEFAULT_NEWS_CATEGORIES)
    return list(_DEFAULT_NEWS_CATEGORIES)


def save_categories(cats: list) -> None:
    CATEGORIES_JSON.parent.mkdir(parents=True, exist_ok=True)
    CATEGORIES_JSON.write_text(json.dumps(cats, ensure_ascii=False, indent=2), encoding="utf-8")


def load_paper_categories() -> list:
    """Load paper subject categories from JSON."""
    path = DATA_DIR / "paper_categories.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    save_paper_categories(_DEFAULT_PAPER_CATEGORIES)
    return list(_DEFAULT_PAPER_CATEGORIES)


def save_paper_categories(cats: list) -> None:
    path = DATA_DIR / "paper_categories.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cats, ensure_ascii=False, indent=2), encoding="utf-8")


# ---- IB EE Subject helpers ----

_EE_SUBJECTS_PATH = DATA_DIR / "ee_subjects.json"

_EE_SUBJECTS_DEFAULT = {
    "groups": [
        {
            "id": 1,
            "name": "Group 1: Studies in Language and Literature",
            "subjects": [
                "Language A: Literature",
                "Language A: Language and Literature",
                "Literature and Performance"
            ]
        },
        {
            "id": 2,
            "name": "Group 2: Language Acquisition",
            "subjects": [
                "Language B",
                "Language ab initio",
                "Classical Languages"
            ]
        },
        {
            "id": 3,
            "name": "Group 3: Individuals and Societies",
            "subjects": [
                "Business Management",
                "Economics",
                "Geography",
                "Global Politics",
                "History",
                "Information Technology in a Global Society",
                "Philosophy",
                "Psychology",
                "Social and Cultural Anthropology",
                "World Religions"
            ]
        },
        {
            "id": 4,
            "name": "Group 4: Sciences",
            "subjects": [
                "Biology",
                "Chemistry",
                "Computer Science",
                "Design Technology",
                "Environmental Systems and Societies",
                "Physics",
                "Sports, Exercise and Health Science"
            ]
        },
        {
            "id": 5,
            "name": "Group 5: Mathematics",
            "subjects": [
                "Mathematics: Analysis and Approaches",
                "Mathematics: Applications and Interpretation"
            ]
        },
        {
            "id": 6,
            "name": "Group 6: The Arts",
            "subjects": [
                "Dance",
                "Film",
                "Music",
                "Theatre",
                "Visual Arts"
            ]
        }
    ],
    "interdisciplinary_subjects": [
        "Environmental Systems and Societies",
        "Literature and Performance",
        "World Studies"
    ]
}


def load_ee_subjects() -> dict:
    """Load IB EE subject groups from JSON, seeding defaults if needed."""
    if _EE_SUBJECTS_PATH.exists():
        try:
            return json.loads(_EE_SUBJECTS_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    save_ee_subjects(_EE_SUBJECTS_DEFAULT)
    return dict(_EE_SUBJECTS_DEFAULT)


def save_ee_subjects(data: dict) -> None:
    """Save IB EE subject groups to JSON."""
    _EE_SUBJECTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _EE_SUBJECTS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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

JOURNAL_COVERS_DIR = BASE_DIR / "static" / "uploads" / "journal_covers"
ALLOWED_EXTENSIONS = {"pdf"}
ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
NEWS_IMAGES_DIR = BASE_DIR / "static" / "uploads" / "news"
GUIDE_IMAGES_DIR = BASE_DIR / "static" / "uploads" / "guides"
GUIDE_IMAGE_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
MAX_SEARCH_RESULTS = 20
PASSWORD_SCHEME = "pbkdf2_sha256"
SUPPORTED_LOCALES = ("en", "zh")
SESSION_TIMEOUT_SECONDS = int(os.environ.get("PAPERQUERY_SESSION_TIMEOUT", "3600"))
OPEN_ACCESS = os.environ.get("PAPERQUERY_OPEN_ACCESS", "0").strip().lower() in ("1", "true", "yes", "on")
SESSION_TIMEOUT = timedelta(seconds=SESSION_TIMEOUT_SECONDS)
METADATA_FIELDS = ["filename", "title", "journal", "category", "language", "keywords", "abstract", "author_name", "author_email", "author_school", "published_at", "ib_ee_data", "is_ib_sample", "cp_data"]
MS_USER_FIELDS = [
    "ms_id",
    "tenant_id",
    "email",
    "display_name",
    "first_name",
    "last_name",
    "school",
    "grade",
    "role",
    "password",
    "created_at",
    "updated_at",
]
MS_CLIENT_ID = os.environ.get("PAPERQUERY_MS_CLIENT_ID")
MS_CLIENT_SECRET = os.environ.get("PAPERQUERY_MS_CLIENT_SECRET")
MS_REDIRECT_URI = os.environ.get("PAPERQUERY_MS_REDIRECT_URI", "http://127.0.0.1:5000/auth/callback")
MS_AUTHORITY = os.environ.get("PAPERQUERY_MS_AUTHORITY", "https://login.microsoftonline.com/common")
MS_SCOPES = ["User.Read"]
MS_GRAPH_ME_URL = "https://graph.microsoft.com/v1.0/me"
DB_URL = os.environ.get("PAPERQUERY_DATABASE_URL")
BASE = declarative_base()
_ENGINE = None
_SESSION_LOCAL = None
ROLE_OPTIONS = [
    ("1", "Reader"),
    ("2", "Moderator"),
    ("3", "Admin"),
]

_MISSING_FIELD_MESSAGES = {
    "title": _l("Please enter the paper title"),
    "category": _l("Please select a subject category"),
    "language": _l("Please select a language"),
    "keywords": _l("Please enter keywords"),
    "abstract": _l("Please enter the abstract"),
    "author_name": _l("Please enter the author name"),
    "author_email": _l("Please enter the contact email"),
    "author_school": _l("Please enter the school name"),
}

# CP Paper (MYP Community Project) constants
IB_EE_CRITERIA_DEFS = [
    ("A", "Framework for the essay", 6),
    ("B", "Knowledge and understanding", 6),
    ("C", "Analysis and line of argument", 6),
    ("D", "Discussion and evaluation", 8),
    ("E", "Reflection", 4),
]
CP_GLOBAL_CONTEXTS = [
    "Identities and Relationships",
    "Orientation in Space and Time",
    "Personal and Cultural Expression",
    "Scientific and Technical Innovation",
    "Globalization and Sustainability",
    "Fairness and Development",
]
CP_ACTION_TYPES = ["Direct Service", "Indirect Service", "Research", "Advocacy"]
CP_CRITERIA_DEFS = [
    ("A", "Investigating", 8),
    ("B", "Planning", 8),
    ("C", "Taking Action", 8),
    ("D", "Reflecting", 8),
]


def _form_int(form, name: str) -> int:
    raw = form.get(name, "").strip()
    return int(raw) if raw.isdigit() else 0


def _build_safe_paper_filename(title: str, author: str = "") -> str:
    """Return a safe PDF filename from a title (+ optional author).

    Caps title at 120 chars and author at 50 chars so the result stays well
    under the filesystem's NAME_MAX (255 bytes on ext4) even after a UUID
    prefix is later prepended for pending uploads. EE research questions
    routinely exceed 255 chars, which would otherwise trigger ENAMETOOLONG.
    """
    safe_title = secure_filename(title or "")[:120]
    safe_author = secure_filename(author or "")[:50]
    if safe_title and safe_author:
        return f"{safe_title}_{safe_author}.pdf"
    if safe_title:
        return f"{safe_title}.pdf"
    if safe_author:
        return f"{safe_author}.pdf"
    return f"{uuid4().hex[:12]}.pdf"


def build_ib_ee_data_from_form(form) -> str:
    criteria = {}
    for letter, label, max_mark in IB_EE_CRITERIA_DEFS:
        criteria[letter] = {
            "label": label,
            "max": max_mark,
            "score": _form_int(form, f"ib_crit_{letter}_score"),
            "comment": form.get(f"ib_crit_{letter}_comment", "").strip(),
        }
    total_score = sum(criterion["score"] for criterion in criteria.values())
    return json.dumps(
        {
            "is_ib_ee": True,
            "core_subject": form.get("ib_ee_core_subject", "").strip(),
            "interdisciplinary_subject": form.get("ib_ee_interdisciplinary_subject", "").strip(),
            "total_grade_letter": form.get("ib_total_grade_letter", "").strip(),
            "total_grade_number": str(total_score),
            "criteria": criteria,
            "holistic_comment": form.get("ib_holistic_comment", "").strip(),
        },
        ensure_ascii=False,
    )


def build_cp_data_from_form(form) -> str:
    criteria = {}
    for letter, label, max_mark in CP_CRITERIA_DEFS:
        criteria[letter] = {
            "label": label,
            "max": max_mark,
            "score": _form_int(form, f"cp_crit_{letter}_score"),
            "comment": form.get(f"cp_crit_{letter}_comment", "").strip(),
        }
    total_score = int(round(sum(criteria[c]["score"] for c in ["A", "B", "C", "D"]) / 4.0))
    return json.dumps(
        {
            "is_cp_paper": True,
            "global_context": form.get("cp_global_context", "").strip(),
            "action_types": form.getlist("cp_action_type"),
            "criteria": criteria,
            "total_score": total_score,
        },
        ensure_ascii=False,
    )


def parse_ib_ee_data_for_form(json_str) -> dict:
    """Flatten ib_ee_data JSON back into form-style keys for draft hydration.

    Returns {} for missing/invalid input so callers can safely .update() the result.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    out = {
        "is_ib_ee": "1",
        "ib_ee_core_subject": data.get("core_subject", ""),
        "ib_ee_interdisciplinary_subject": data.get("interdisciplinary_subject", ""),
        "ib_holistic_comment": data.get("holistic_comment", ""),
    }
    for letter, criterion in (data.get("criteria") or {}).items():
        out[f"ib_crit_{letter}_score"] = str(criterion.get("score", ""))
        out[f"ib_crit_{letter}_comment"] = criterion.get("comment", "")
    return out


def parse_cp_data_for_form(json_str) -> dict:
    """Flatten cp_data JSON back into form-style keys for draft hydration.

    Returns {} for missing/invalid input.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    out = {
        "is_cp_paper": "1",
        "cp_global_context": data.get("global_context", ""),
        "cp_action_types": data.get("action_types") or [],
    }
    for letter, criterion in (data.get("criteria") or {}).items():
        out[f"cp_crit_{letter}_score"] = str(criterion.get("score", ""))
    return out

def _is_ee_paper(record: dict) -> bool:
    raw = record.get("ib_ee_data", "")
    if not raw:
        return False
    try:
        return bool(json.loads(raw).get("is_ib_ee"))
    except (json.JSONDecodeError, TypeError):
        return False


def _is_cp_paper(record: dict) -> bool:
    raw = record.get("cp_data", "")
    if not raw:
        return False
    try:
        return bool(json.loads(raw).get("is_cp_paper"))
    except (json.JSONDecodeError, TypeError):
        return False


def _matches_ee_subject(record: dict, subject: str) -> bool:
    raw = record.get("ib_ee_data", "")
    if not raw:
        return False
    try:
        ib = json.loads(raw)
        s = subject.lower()
        return s in (ib.get("core_subject", "") + " " + ib.get("interdisciplinary_subject", "")).lower()
    except (json.JSONDecodeError, TypeError):
        return False


def _matches_cp_context(record: dict, context: str) -> bool:
    raw = record.get("cp_data", "")
    if not raw:
        return False
    try:
        cp = json.loads(raw)
        return context.lower() in (cp.get("global_context", "")).lower()
    except (json.JSONDecodeError, TypeError):
        return False


def _get_ee_subjects_list() -> list:
    """Return a flat sorted list of all EE subjects."""
    data = load_ee_subjects()
    subjects = set()
    for group in data.get("groups", []):
        for s in group.get("subjects", []):
            subjects.add(s.strip())
    return sorted(subjects)


babel = Babel()
ROLE_LABELS = {
    1: _l("Reader - View & Download"),
    2: _l("Contributor - Upload Enabled"),
    3: _l("Curator - Full Access"),
}
LANGUAGE_NAMES = {
    "en": _l("English"),
    "zh": _l("Chinese"),
}

class LocalUser(BASE):
    __tablename__ = "local_users"
    username = Column(Unicode(255), primary_key=True)
    password = Column(Unicode(255), nullable=False)
    registration_date = Column(Date)
    expiry_date = Column(Date)
    role = Column(Unicode(10), nullable=False)
    email = Column(Unicode(255))
    first_name = Column(Unicode(255))
    last_name = Column(Unicode(255))
    school = Column(Unicode(255))


class MsUser(BASE):
    __tablename__ = "ms_users"
    ms_id = Column(Unicode(255), primary_key=True)
    tenant_id = Column(Unicode(255))
    email = Column(Unicode(255))
    display_name = Column(Unicode(255))
    first_name = Column(Unicode(255))
    last_name = Column(Unicode(255))
    school = Column(Unicode(255))
    grade = Column(Unicode(255))
    role = Column(Unicode(10))
    password = Column(Unicode(255))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class JournalModel(BASE):
    __tablename__ = "journals"
    id = Column(Unicode(255), primary_key=True)
    name = Column(Unicode(255))
    cover_image = Column(Unicode(255))
    introduction = Column(UnicodeText)
    created_at = Column(Unicode(255))

class PaperMetadataModel(BASE):
    __tablename__ = "papers_metadata"
    filename = Column(Unicode(255), primary_key=True)
    title = Column(Unicode(255))
    journal = Column(Unicode(255))
    category = Column(Unicode(255))
    language = Column(Unicode(255))
    keywords = Column(UnicodeText)
    abstract = Column(UnicodeText)
    author_name = Column(Unicode(255))
    author_email = Column(Unicode(255))
    author_school = Column(Unicode(255))
    published_at = Column(Unicode(255))
    ib_ee_data = Column(UnicodeText)
    is_ib_sample = Column(Unicode(10))
    cp_data = Column(UnicodeText)


class PaperChunkModel(BASE):
    __tablename__ = "papers_chunks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(Unicode(255), index=True)
    chunk_index = Column(Integer)
    content = Column(UnicodeText)
    embedding = Column(UnicodeText)   # JSON-encoded list[float]
    lang = Column(Unicode(10))


class ConversationModel(BASE):
    __tablename__ = "conversations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    serial = Column(Unicode(6), unique=True, index=True)
    owner_key = Column(Unicode(64), index=True)
    title = Column(Unicode(255))
    created_at = Column(Unicode(40))
    updated_at = Column(Unicode(40))


class ChatMessageModel(BASE):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(Integer, index=True)
    role = Column(Unicode(16))          # "user" | "assistant"
    content = Column(UnicodeText)
    citations = Column(UnicodeText)     # JSON-encoded list
    created_at = Column(Unicode(40))


class AttachmentChunkModel(BASE):
    __tablename__ = "attachment_chunks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(Integer, index=True)
    filename = Column(Unicode(255))
    chunk_index = Column(Integer)
    content = Column(UnicodeText)
    embedding = Column(UnicodeText)   # JSON-encoded list[float]
    created_at = Column(Unicode(40))


class NewsArticleModel(BASE):
    __tablename__ = "news_articles"
    id = Column(Unicode(255), primary_key=True)
    title = Column(Unicode(255))
    category = Column(Unicode(255))
    abstract = Column(UnicodeText)
    body = Column(UnicodeText)
    author = Column(Unicode(255))
    image_url = Column(Unicode(255))
    published_at = Column(Unicode(255))
    status = Column(Unicode(20), default="published")


class GuideModel(BASE):
    __tablename__ = "guides"
    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(Unicode(120), unique=True, index=True, nullable=False)
    category = Column(Unicode(80), default="")
    sort_order = Column(Integer, default=100)
    published = Column(Boolean, default=False)
    title_en = Column(Unicode(200), default="")
    title_zh = Column(Unicode(200), default="")
    summary_en = Column(Unicode(300), default="")
    summary_zh = Column(Unicode(300), default="")
    body_en = Column(UnicodeText, default="")
    body_zh = Column(UnicodeText, default="")
    created_at = Column(Unicode(40), default="")
    updated_at = Column(Unicode(40), default="")


class SubmissionModel(BASE):
    __tablename__ = "submissions"
    id = Column(Unicode(255), primary_key=True)
    pdf_filename = Column(Unicode(255))
    pending_filename = Column(Unicode(255))
    title = Column(Unicode(255))
    author_name = Column(Unicode(255))
    author_email = Column(Unicode(255))
    author_school = Column(Unicode(255))
    status = Column(Unicode(50))
    submitted_at = Column(Unicode(255))
    feedback = Column(UnicodeText)
    abstract = Column(UnicodeText)
    keywords = Column(UnicodeText)
    journal = Column(Unicode(255))
    category = Column(Unicode(255))
    language = Column(Unicode(255))
    submitted_by = Column(Unicode(255))
    original_filename = Column(Unicode(255))
    ib_ee_data = Column(UnicodeText)
    is_ib_sample = Column(Unicode(10))
    cp_data = Column(UnicodeText)


class SessionModel(BASE):
    __tablename__ = "sessions"
    username = Column(Unicode(255), primary_key=True)
    token = Column(Unicode(255))
    last_seen = Column(Unicode(255))



def init_db() -> None:
    global _ENGINE, _SESSION_LOCAL
    if _ENGINE is None:
        _ENGINE = create_engine(DB_URL, pool_pre_ping=True, pool_recycle=3600)
        _SESSION_LOCAL = sessionmaker(bind=_ENGINE)
        BASE.metadata.create_all(_ENGINE)
        # Migrate: add password column to ms_users if it doesn't exist
        try:
            with _ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE ms_users ADD COLUMN password VARCHAR(255) NULL"))
                conn.commit()
        except Exception:
            pass  # Column already exists
        # Migrate: add serial column to conversations if it doesn't exist
        try:
            with _ENGINE.connect() as conn:
                from sqlalchemy import text
                import secrets
                try:
                    conn.execute(text("ALTER TABLE conversations ADD COLUMN serial VARCHAR(6)"))
                    conn.commit()
                except Exception:
                    pass
                try:
                    conn.execute(text("CREATE UNIQUE INDEX ix_conversations_serial ON conversations(serial)"))
                    conn.commit()
                except Exception:
                    pass
                
                rows = conn.execute(text("SELECT id FROM conversations WHERE serial IS NULL")).fetchall()
                for row in rows:
                    serial = secrets.token_urlsafe(5)[:6]
                    conn.execute(text("UPDATE conversations SET serial = :s WHERE id = :id"), {"s": serial, "id": row[0]})
                conn.commit()
        except Exception:
            pass
        # Migrate: add is_ib_sample column to papers_metadata if it doesn't exist
        try:
            with _ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE papers_metadata ADD COLUMN is_ib_sample VARCHAR(10) DEFAULT ''"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add is_ib_sample column to submissions if it doesn't exist
        try:
            with _ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE submissions ADD COLUMN is_ib_sample VARCHAR(10) DEFAULT ''"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add cp_data column to papers_metadata if it doesn't exist
        try:
            with _ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE papers_metadata ADD COLUMN cp_data TEXT"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add cp_data column to submissions if it doesn't exist
        try:
            with _ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE submissions ADD COLUMN cp_data TEXT"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add status column to news_articles if it doesn't exist
        try:
            with _ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE news_articles ADD COLUMN status VARCHAR(20) DEFAULT 'published'"))
                conn.execute(text("UPDATE news_articles SET status = 'published' WHERE status IS NULL OR status = ''"))
                conn.commit()
        except Exception:
            pass


@contextmanager
def db_session():
    if _SESSION_LOCAL is None:
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


def select_locale() -> str:
    preferred = session.get("language")
    if preferred in SUPPORTED_LOCALES:
        return preferred
    match = request.accept_languages.best_match(SUPPORTED_LOCALES)
    return match or SUPPORTED_LOCALES[0]


def create_app() -> Flask:
    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    app.config.update(
        SECRET_KEY=os.environ.get("PAPERQUERY_SECRET", "dev-secret-key"),
        PERMANENT_SESSION_LIFETIME=timedelta(days=365),
        UPLOAD_FOLDER=str(PAPERS_DIR),
        BABEL_DEFAULT_LOCALE="en",
        BABEL_DEFAULT_TIMEZONE="UTC",
        BABEL_SUPPORTED_LOCALES=",".join(SUPPORTED_LOCALES),
        MAX_CONTENT_LENGTH=int(os.environ.get("PAPERQUERY_MAX_UPLOAD_MB", "50")) * 1024 * 1024,
    )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PAPERS_DIR.mkdir(parents=True, exist_ok=True)
    PENDING_PAPERS_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    configure_rag()
    babel.init_app(app, locale_selector=select_locale)

    @app.context_processor
    def inject_helpers():
        def role_label(level: int) -> str:
            return str(ROLE_LABELS.get(level, ROLE_LABELS[1]))

        locale_code = str(get_locale())
        language_options = [
            {
                "code": code,
                "label": str(LANGUAGE_NAMES[code]),
                "active": code == locale_code,
            }
            for code in SUPPORTED_LOCALES
        ]
        active_language = next((option for option in language_options if option["active"]), language_options[0])

        return {
            "role_label": role_label,
            "languages": language_options,
            "current_locale": locale_code,
            "current_language_label": active_language["label"],
        }

    def is_partial_request():
        """True when the request carries X-Partial-Content: 1.

        Used by routes to render either the full base.html shell or just the
        inner content block via _bare.html, so the dashboard can fetch a route
        and swap its content into the main panel.
        """
        return request.headers.get("X-Partial-Content") == "1"

    def require_ask_api_access():
        if OPEN_ACCESS or get_active_user():
            return None
        return jsonify({"error": str(_("Please sign in first."))}), 401

    @app.context_processor
    def inject_partial_flag():
        return {"partial": is_partial_request()}

    @app.context_processor
    def inject_global_vars():
        """Inject global variables into all templates."""
        return {
            "current_year": datetime.utcnow().year,
            "site_name": "Keydion",
            "ms_enabled": is_ms_configured(),
            "open_access": OPEN_ACCESS,
            "llm_enabled": llm_client.llm_enabled(),
        }

    # ---- Template filter: parse block-based article body ----
    @app.template_filter("parse_body_blocks")
    def parse_body_blocks(body_text: str):
        """Parse article body into content blocks.

        Accepts a JSON array of blocks or plain text (backward compat).
        Each block: {"type": "text", "content": "..."}
                 or {"type": "image", "url": "...", "caption": "..."}
        """
        if not body_text or not body_text.strip():
            return []
        try:
            parsed = json.loads(body_text)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        # Fallback: treat plain text as paragraphs
        return [{"type": "text", "content": p.strip()} for p in body_text.split("\n") if p.strip()]

    @app.template_filter("from_json")
    def from_json_filter(value):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None

    @app.route("/")
    def index():
        user = session.get("user")
        token = session.get("session_token")
        if user and token:
            if not refresh_session(user.get("username", ""), token):
                session.clear()
        latest_news = load_news_articles(status="published")[:4]
        return render_template("landing.html", ms_enabled=is_ms_configured(), latest_news=latest_news)

    @app.route("/ask")
    @app.route("/ask/<serial>")
    def ask_library(serial=None):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        
        if serial:
            owner = _ask_owner_key()
            with db_session() as db:
                conv = db.query(ConversationModel).filter(
                    ConversationModel.serial == serial,
                    ConversationModel.owner_key == owner).first()
                if not conv:
                    return redirect(url_for("ask_library"))

        suggestions = [
            _("What does the research say about climate adaptation in plants?"),
            _("Summarize recent Extended Essays in economics."),
            _("Find papers about machine learning in healthcare."),
        ]
        boot = {
            "ask_url": url_for("ask_library"),
            "api_url": url_for("api_ask"),
            "enabled": llm_client.llm_enabled(),
            "web_enabled": web_search.web_search_enabled(),
            "i18n": {
                "title": "Keydion AI",
                "empty_title": "Keydion AI",
                "empty_sub": _("Ask a question and I'll answer from the published library, with citations."),
                "placeholder": _("Message Keydion AI…"),
                "flash": _("Flash"),
                "thinking": _("Thinking"),
                "send": _("Send"),
                "sources": _("Cited from your library"),
                "copy": _("Copy"),
                "regenerate": _("Regenerate"),
                "rename": _("Rename"),
                "today": _("Today"),
                "yesterday": _("Yesterday"),
                "previous_7_days": _("Previous 7 days"),
                "older": _("Older"),
                "no_conversations_match": _("No conversations match."),
                "thinking_state": _("Thinking…"),
                "error": _("Something went wrong. Please try again."),
                "disabled": _("AI assistant is not configured."),
                "no_sources": _("No matching papers were found in the library."),
                "searched_web": _("Searched the web"),
                "selected": _("selected"),
                "select_hint": _("Select papers to attach as citations"),
                "preview_abstract_label": _("Abstract"),
                "preview_no_abstract": _("No abstract available."),
                "preview_hint": _("Hover a paper to preview its abstract before citing."),
            },
            "active_serial": serial,
        }
        return render_template(
            "ask.html",
            partial=is_partial_request(),
            llm_enabled=llm_client.llm_enabled(),
            web_enabled=web_search.web_search_enabled(),
            ms_enabled=is_ms_configured(),
            suggestions=suggestions,
            ask_boot=boot,
        )

    @app.route("/faq")
    def faq():
        return render_template("FAQ.html")

    @app.route("/login", methods=["GET", "POST"])
    def login():
        # Redirect already-logged-in users away
        if session.get("user") and session.get("session_token"):
            return redirect(url_for("index"))
        if request.method == "POST":
            email = request.form.get("email", "").strip()
            password = request.form.get("password", "").strip()

            # 1. Try local user by email
            user_record = get_local_user_by_email(email)
            if not user_record:
                # 2. Try local user by username (for admin accounts like "admin")
                user_record = get_local_user(email)

            if user_record:
                user = authenticate(user_record.get("username", ""), password)
                if user:
                    allowed, warning = ensure_login_available(user["username"])
                    if not allowed:
                        flash(warning, "warning")
                        return redirect(url_for("index", login=1))
                    display = user_record.get("first_name", "") or user_record.get("email", "") or user["username"]
                    saved_next = session.get("next") or request.form.get("next", "")
                    start_local_session(
                        user,
                        display_name=display,
                        email=user_record.get("email", ""),
                    )
                    flash(_("Welcome back, %(username)s!", username=display), "success")
                    return redirect(saved_next or url_for("index"))
            else:
                # 3. Try MS user by email (if they have set a password)
                ms_record = get_ms_user_by_email(email)
                if ms_record and ms_record.get("password"):
                    if verify_password(password, ms_record["password"]):
                        allowed, warning = ensure_login_available(ms_record["ms_id"])
                        if not allowed:
                            flash(warning, "warning")
                            return redirect(url_for("index", login=1))
                        saved_next = session.get("next") or request.form.get("next", "")
                        start_ms_session(ms_record)
                        display = ms_record.get("display_name", "") or ms_record.get("email", "")
                        flash(_("Welcome back, %(username)s!", username=display), "success")
                        return redirect(saved_next or url_for("index"))

            flash(_("Invalid email or password"), "danger")
            return redirect(url_for("index", login=1))

        if not is_ms_configured():
            flash(_("Microsoft sign-in is not configured. Please contact the administrator."), "warning")
        return redirect(url_for("index", login=1))

    @app.route("/register", methods=["GET", "POST"])
    def register():
        flash(_("Email-based registration is disabled. Please sign in with Microsoft."), "warning")
        return redirect(url_for("login"))

    @app.route("/auth/login")
    def ms_login():
        if session.get("user") and session.get("session_token"):
            return redirect(url_for("index"))
        if not is_ms_configured():
            flash(_("Microsoft sign-in is not configured. Please contact the administrator."), "danger")
            return redirect(url_for("login"))
        state = uuid4().hex
        session["ms_state"] = state
        session["ms_next"] = request.args.get("next", "")
        auth_url = build_msal_app().get_authorization_request_url(
            MS_SCOPES,
            state=state,
            redirect_uri=MS_REDIRECT_URI,
            prompt="select_account",
        )
        return redirect(auth_url)

    @app.route("/auth/callback")
    def ms_callback():
        if not is_ms_configured():
            flash(_("Microsoft sign-in is not configured. Please contact the administrator."), "danger")
            return redirect(url_for("login"))
        if request.args.get("state") != session.get("ms_state"):
            flash(_("Login session expired. Please try again."), "warning")
            return redirect(url_for("login"))

        error = request.args.get("error")
        if error:
            description = request.args.get("error_description", error)
            flash(_("Microsoft sign-in failed: %(reason)s", reason=description), "danger")
            return redirect(url_for("login"))

        code = request.args.get("code")
        if not code:
            flash(_("Microsoft sign-in failed. Please try again."), "danger")
            return redirect(url_for("login"))

        result = build_msal_app().acquire_token_by_authorization_code(
            code,
            scopes=MS_SCOPES,
            redirect_uri=MS_REDIRECT_URI,
        )
        if "access_token" not in result:
            message = result.get("error_description") or "Token exchange failed."
            flash(_("Microsoft sign-in failed: %(reason)s", reason=message), "danger")
            return redirect(url_for("login"))

        profile = fetch_ms_profile(result)
        if not profile.get("ms_id"):
            flash(_("Microsoft sign-in did not return a valid profile."), "danger")
            return redirect(url_for("login"))

        allowed, warning = ensure_login_available(profile["ms_id"])
        if not allowed:
            flash(warning, "warning")
            return redirect(url_for("login"))

        user_record = upsert_ms_user(profile)
        saved_next = session.get("next")
        start_ms_session(user_record)
        if saved_next:
            session["next"] = saved_next

        if not is_profile_complete(user_record):
            return redirect(url_for("profile_setup"))
        next_url = session.pop("next", None)
        return redirect(next_url or url_for("index"))

    @app.route("/logout")
    def logout():
        language = session.get("language")
        username = session.get("user", {}).get("username", "")
        # 强制释放会话，不检查 token 匹配
        if username:
            force_release_session(username)
        session.clear()
        if language:
            session["language"] = language
        flash(_("Signed out successfully."), "info")
        return redirect(url_for("index"))

    @app.route("/profile/setup", methods=["GET", "POST"])
    def profile_setup():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        ms_id = user.get("ms_id") or user.get("username", "")
        record = get_ms_user(ms_id)
        if not record:
            flash(_("Unable to load your profile. Please sign in again."), "warning")
            return redirect(url_for("logout"))

        if request.method == "POST":
            first_name = request.form.get("first_name", "").strip()
            last_name = request.form.get("last_name", "").strip()

            if not first_name or not last_name:
                flash(_("Please enter your first and last name."), "warning")
            else:
                updated = update_ms_user(
                    ms_id,
                    {
                        "first_name": first_name,
                        "last_name": last_name,
                    },
                )
                if updated:
                    session["user"]["first_name"] = updated.get("first_name", "")
                    session["user"]["last_name"] = updated.get("last_name", "")
                    # Prefer user-entered name over MS display_name
                    entered_name = f"{updated.get('first_name', '').strip()} {updated.get('last_name', '').strip()}".strip()
                    session["user"]["display_name"] = entered_name or session["user"].get("display_name", "")
                flash(_("Profile saved successfully."), "success")
                next_url = session.pop("next", None)
                return redirect(next_url or url_for("index"))

        return render_template(
            "profile_setup.html",
            profile=record,
        )


    @app.route("/dashboard/account/change-password", methods=["GET", "POST"])
    def change_password():
        user = require_login()
        if not user:
            return redirect(url_for("login"))

        is_ms_user = not user.get("is_local", True)
        ms_id = user.get("ms_id") or user.get("username", "")

        # Determine if the user already has a password set. MS-only users may
        # have arrived via Microsoft sign-in without ever setting a local
        # password — in that case current-password verification is skipped.
        ms_record = get_ms_user(ms_id) if is_ms_user else None
        has_password = True
        if is_ms_user:
            has_password = bool(ms_record and ms_record.get("password"))

        if request.method == "POST":
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "").strip()
            confirm_password = request.form.get("confirm_password", "").strip()

            if has_password:
                if is_ms_user:
                    stored_hash = (ms_record or {}).get("password", "")
                else:
                    local_record = get_local_user(user.get("username", "")) or {}
                    stored_hash = local_record.get("password", "")
                if not stored_hash or not verify_password(current_password, stored_hash):
                    flash(_("Current password is incorrect."), "danger")
                    return redirect(url_for("change_password"))

            if not new_password:
                flash(_("Please enter a new password."), "warning")
                return redirect(url_for("change_password"))
            if new_password != confirm_password:
                flash(_("Passwords do not match."), "warning")
                return redirect(url_for("change_password"))
            if len(new_password) < 6:
                flash(_("Password must be at least 6 characters."), "warning")
                return redirect(url_for("change_password"))

            has_alpha = any(c.isalpha() for c in new_password)
            has_digit = any(c.isdigit() for c in new_password)
            if not (has_alpha and has_digit):
                flash(_("Password must contain both letters and numbers."), "warning")
                return redirect(url_for("change_password"))

            if has_password and new_password == current_password:
                flash(
                    _("New password must be different from your current password."),
                    "warning",
                )
                return redirect(url_for("change_password"))

            if is_ms_user:
                success = update_ms_user_password(ms_id, new_password)
            else:
                success = update_local_user_password(user.get("username", ""), new_password)

            if success:
                flash(_("Password updated successfully."), "success")
            else:
                flash(_("Unable to update password."), "danger")
            return redirect(url_for("change_password"))

        return render_template("change_password.html", user=user, has_password=has_password)

    @app.route("/account/change-password", endpoint="change_password_legacy")
    def change_password_legacy():
        return redirect(url_for("change_password"), code=301)

    @app.route("/dashboard/admin/users")
    def admin_users():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        local_users = load_users()
        ms_users = load_ms_users()

        return render_template(
            "admin_users.html",
            local_users=local_users,
            ms_users=ms_users,
            role_options=ROLE_OPTIONS,
        )

    @app.route("/dashboard/admin/users/roles", methods=["POST"], endpoint="admin_users_roles")
    def admin_bulk_update_roles():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        local_usernames = request.form.getlist("local_username")
        local_roles = request.form.getlist("local_role")
        for username, role in zip(local_usernames, local_roles):
            update_local_user_role(username, role)

        ms_ids = request.form.getlist("ms_id")
        ms_roles = request.form.getlist("ms_role")
        for ms_id, role in zip(ms_ids, ms_roles):
            update_ms_user_role(ms_id, role)

        flash(_("Role updates saved."), "success")
        return redirect(url_for("admin_users"))

    @app.route("/dashboard/admin/users/add", methods=["POST"], endpoint="admin_users_add")
    def admin_add_local_user():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        role = request.form.get("role", "1")
        if not username or not password:
            flash(_("Username and password are required."), "warning")
            return redirect(url_for("admin_users"))
        if get_local_user(username):
            flash(_("That username is already taken."), "warning")
            return redirect(url_for("admin_users"))
        create_local_user(username, password, role=role)
        flash(_("Local user created."), "success")
        return redirect(url_for("admin_users"))

    @app.route("/dashboard/admin/users/<path:username>/role", methods=["POST"], endpoint="admin_user_role")
    def admin_update_local_role(username: str):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        role = request.form.get("role", "1")
        if update_local_user_role(username, role):
            flash(_("Role updated."), "success")
        else:
            flash(_("Unable to update role."), "warning")
        return redirect(url_for("admin_users"))

    @app.route("/dashboard/admin/users/<path:username>/reset-password", methods=["POST"], endpoint="admin_user_reset_password")
    def admin_reset_password(username: str):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        new_password = request.form.get("password", "").strip()
        if not new_password:
            flash(_("Password is required."), "warning")
            return redirect(url_for("admin_users"))
        if update_local_user_password(username, new_password):
            flash(_("Password reset successfully."), "success")
        else:
            flash(_("Unable to reset password."), "warning")
        return redirect(url_for("admin_users"))

    @app.route("/dashboard/admin/users/<path:username>/delete", methods=["POST"], endpoint="admin_user_delete")
    def admin_delete_local_user(username: str):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        if delete_local_user(username):
            flash(_("Local user deleted."), "success")
        else:
            flash(_("Unable to delete user."), "warning")
        return redirect(url_for("admin_users"))

    @app.route("/admin/users", endpoint="admin_users_legacy")
    def admin_users_legacy():
        return redirect(url_for("admin_users"), code=301)

    @app.route("/dashboard/admin/ms-users/<path:ms_id>/role", methods=["POST"], endpoint="admin_ms_user_role")
    def admin_update_ms_role(ms_id: str):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        role = request.form.get("role", "1")
        if update_ms_user_role(ms_id, role):
            flash(_("Role updated."), "success")
        else:
            flash(_("Unable to update role."), "warning")
        return redirect(url_for("admin_users"))

    @app.route("/dashboard/admin/ms-users/<path:ms_id>/delete", methods=["POST"], endpoint="admin_ms_user_delete")
    def admin_delete_ms_user(ms_id: str):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        if delete_ms_user(ms_id):
            flash(_("Microsoft user deleted."), "success")
        else:
            flash(_("Unable to delete Microsoft user."), "warning")
        return redirect(url_for("admin_users"))

    @app.route("/dashboard/admin/ms-users/<path:ms_id>/set-password", methods=["POST"], endpoint="admin_ms_user_set_password")
    def admin_set_ms_password(ms_id: str):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        new_password = request.form.get("password", "").strip()
        if not new_password:
            flash(_("Password is required."), "warning")
            return redirect(url_for("admin_users"))
        if update_ms_user_password(ms_id, new_password):
            flash(_("Password set successfully."), "success")
        else:
            flash(_("Unable to set password."), "warning")
        return redirect(url_for("admin_users"))

    @app.route("/dashboard")
    def dashboard():
        user = require_login()
        if not user:
            return redirect(url_for("login"))

        try:
            role = int(user.get("role", "1"))
        except (TypeError, ValueError):
            role = 1

        stats: Dict[str, object] = {}

        if role >= 2:
            with db_session() as db:
                pending_subs = db.query(SubmissionModel).filter_by(status="pending").all()
                stats["pending_reviews"] = len(pending_subs)
                # Oldest submission delta — submitted_at is a Unicode ISO string.
                oldest_days = None
                for sub in pending_subs:
                    ts = (sub.submitted_at or "").strip()
                    if not ts:
                        continue
                    try:
                        dt = datetime.fromisoformat(ts)
                        days = (datetime.utcnow() - dt).days
                    except (ValueError, TypeError):
                        continue
                    if oldest_days is None or days > oldest_days:
                        oldest_days = days
                if oldest_days is not None and oldest_days > 0:
                    stats["pending_oldest_label"] = _("oldest %(n)d days ago") % {"n": oldest_days}

                stats["published_news"] = db.query(NewsArticleModel).filter_by(status="published").count()
                stats["pending_news"] = db.query(NewsArticleModel).filter_by(status="pending").count()

        if role >= 3:
            with db_session() as db:
                stats["papers_in_library"] = db.query(PaperMetadataModel).count()
                # "+N this month" delta via string-prefix comparison on the YYYY-MM portion.
                current_prefix = datetime.utcnow().strftime("%Y-%m")
                new_this_month = (
                    db.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.published_at.like(f"{current_prefix}%"))
                    .count()
                )
                if new_this_month:
                    stats["papers_delta_label"] = _("+%(n)d this month") % {"n": new_this_month}

        if is_partial_request():
            return render_template(
                "_dashboard/overview.html",
                user=user,
                dashboard_stats=stats,
            )

        return render_template(
            "dashboard.html",
            user=user,
            dashboard_stats=stats,
        )

    @app.route("/advanced-search")
    def advanced_search():
        user = get_active_user()
        is_guest = user is None
        journals = load_journals()
        return render_template("advanced_search.html", user=user, journals=journals,
                               ee_subjects_list=_get_ee_subjects_list(), cp_contexts=CP_GLOBAL_CONTEXTS)

    @app.route("/search", methods=["GET", "POST"])
    def search():
        user = get_active_user()
        is_guest = user is None

        if request.method == "POST":
            query_value = request.form.get("query", "").strip()
            if not query_value:
                flash(_("Enter a keyword to search."), "warning")
                return redirect(url_for("search"))
            return redirect(url_for("search", q=query_value))

        query = request.args.get("q", "").strip()
        category_filter = request.args.get("category", "").strip()
        language_filter = request.args.get("language", "").strip()
        date_filter = request.args.get("date", "").strip()
        author_filter = request.args.get("author", "").strip().lower()
        title_filter = request.args.get("title", "").strip().lower()
        start_year = request.args.get("start_year", "").strip()
        end_year = request.args.get("end_year", "").strip()
        journal_filters = request.args.getlist("journal[]")
        paper_type_filter = request.args.get("paper_type", "").strip()
        ee_subject_filter = request.args.get("ee_subject", "").strip()
        cp_context_filter = request.args.get("cp_context", "").strip()

        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1

        per_page = 20
        filtered = bool(query) or bool(category_filter) or bool(language_filter) or bool(date_filter) or bool(author_filter) or bool(title_filter) or bool(start_year) or bool(end_year) or bool(journal_filters) or bool(paper_type_filter) or bool(ee_subject_filter) or bool(cp_context_filter)
        
        # Only run full text search if 'q' is actually present
        record_pool = search_papers(query) if bool(query) else gather_paper_records()

        # Apply additional filters
        if category_filter:
            record_pool = [r for r in record_pool if r.get("category") == category_filter]
        if language_filter:
            record_pool = [r for r in record_pool if r.get("language") == language_filter]
        if date_filter:
            # Simple substring match for date filter (e.g., '2023', '2023-10')
            record_pool = [r for r in record_pool if (r.get("published_at") or "").startswith(date_filter)]
            
        if author_filter:
            record_pool = [r for r in record_pool if author_filter in (r.get("author_name") or "").lower()]
        if title_filter:
            record_pool = [r for r in record_pool if title_filter in (r.get("title") or "").lower() or title_filter in r.get("filename", "").lower()]
            
        if start_year:
            record_pool = [r for r in record_pool if str(r.get("published_at") or "")[:4] >= start_year]
        if end_year:
            record_pool = [r for r in record_pool if str(r.get("published_at") or "")[:4] <= end_year]
            
        if journal_filters:
            record_pool = [r for r in record_pool if r.get("journal") in journal_filters]

        if paper_type_filter:
            if paper_type_filter == "ee":
                record_pool = [r for r in record_pool if _is_ee_paper(r)]
            elif paper_type_filter == "cp":
                record_pool = [r for r in record_pool if _is_cp_paper(r)]
            elif paper_type_filter == "independent":
                record_pool = [r for r in record_pool if not _is_ee_paper(r) and not _is_cp_paper(r)]

        if ee_subject_filter:
            record_pool = [r for r in record_pool if _matches_ee_subject(r, ee_subject_filter)]

        if cp_context_filter:
            record_pool = [r for r in record_pool if _matches_cp_context(r, cp_context_filter)]

        if filtered and not record_pool:
            flash(_("No matching papers found."), "info")

        pagination = paginate_records(record_pool, page, per_page)
        
        for p in pagination["items"]:
            if p.get("author_school"):
                unique_schools = []
                for s in p["author_school"].split(","):
                    s_clean = s.strip()
                    if s_clean and s_clean not in unique_schools:
                        unique_schools.append(s_clean)
                p["author_school_deduped"] = ", ".join(unique_schools) if unique_schools else p["author_school"]
            # Parse paper type for display (CP > EE > Independent Research)
            p["paper_type"] = "Independent Research"
            raw_cp = p.get("cp_data", "")
            if raw_cp:
                try:
                    cp_info = json.loads(raw_cp)
                    if cp_info.get("is_cp_paper"):
                        p["paper_type"] = "Community Project"
                        p["cp_global_context"] = cp_info.get("global_context", "")
                        p["cp_action_types"] = cp_info.get("action_types", [])
                        p["cp_total_score"] = cp_info.get("total_score", 0)
                except (json.JSONDecodeError, TypeError):
                    pass
            raw_ib = p.get("ib_ee_data", "")
            if raw_ib and p["paper_type"] == "Independent Research":
                try:
                    ib_info = json.loads(raw_ib)
                    if ib_info.get("is_ib_ee"):
                        p["paper_type"] = "Extended Essay"
                        p["ee_core_subject"] = ib_info.get("core_subject", "")
                        p["ee_interdisciplinary_subject"] = ib_info.get("interdisciplinary_subject", "")
                except (json.JSONDecodeError, TypeError):
                    pass

        return render_template(
            "search.html",
            user=user,
            query=query,
            category_filter=category_filter,
            language_filter=language_filter,
            date_filter=date_filter,
            paper_type_filter=paper_type_filter,
            ee_subject_filter=ee_subject_filter,
            ee_subjects_list=_get_ee_subjects_list(),
            cp_context_filter=cp_context_filter,
            cp_contexts=CP_GLOBAL_CONTEXTS,
            filtered=filtered,
            records=pagination["items"],
            pagination=pagination,
            is_guest=is_guest,
            total_matches=len(record_pool),
            paper_categories=load_paper_categories(),
            journal_id_map=get_journal_id_map(),
        )

    def _render_upload(user, form_data, draft_id):
        """Render upload.html with the wizard_boot context the JS needs."""
        try:
            _role = int(user.get("role", "1"))
        except (TypeError, ValueError):
            _role = 1
        wizard_boot = {
            "submit_url": url_for("upload"),
            "draft_id": draft_id or "",
            "form_data": form_data,
            "paper_categories": load_paper_categories(),
            "ee_subjects": load_ee_subjects(),
            "cp_global_contexts": CP_GLOBAL_CONTEXTS,
            "cp_action_types": CP_ACTION_TYPES,
            "user_key": user.get("username", ""),
            "llm_metadata_enabled": llm_client.llm_enabled() and _role >= 2,
            "i18n": {
                "step_name_type": _("Paper Type"),
                "step_name_metadata": _("Metadata"),
                "step_name_authors": _("Authors"),
                "step_name_file": _("File"),
                "step_name_review": _("Review"),
                "step_label": _("Step %(n)s"),
                "submit_paper": _("Submit Paper"),
                "continue": _("Continue →"),
                "back": _("← Back"),
                "save_draft": _("Save Draft"),
                "choose_paper_type": _("Choose paper type"),
                "what_kind": _("What kind of paper are you submitting?"),
                "what_kind_sub": _("The fields you'll be asked for next depend on this. You can come back and change it before submitting."),
                "type_tag_standard": _("Independent Research"),
                "type_title_standard": _("Standard Paper"),
                "type_body_standard": _("A self-directed research paper, conference paper, or article that is not part of the IB Diploma framework."),
                "type_meta_standard": _("Title · authors · abstract · subject"),
                "type_tag_ee": _("IB Diploma"),
                "type_title_ee": _("Extended Essay (EE)"),
                "type_body_ee": _("A 4,000-word IB Diploma research essay with structured criterion scores (A–E) and an EE subject from the six IB subject groups."),
                "type_meta_ee": _("Research Question · EE subject · criterion scores A–E"),
                "type_tag_cp": _("IB Diploma"),
                "type_title_cp": _("Community Project (CP)"),
                "type_body_cp": _("An IB MYP Community Project graded against Criteria A–D, with a Global Context and a chosen type of action."),
                "type_meta_cp": _("Title · Global Context · type of action · criteria A–D"),
                "research_question": _("Research Question"),
                "paper_title": _("Paper Title"),
                "research_question_ph": _("e.g. To what extent did monetary policy contribute to the 2008 financial crisis?"),
                "paper_title_ph": _("Enter the complete paper title"),
                "tell_us_ee": _("Tell us about your essay"),
                "tell_us_cp": _("Tell us about your community project"),
                "tell_us_std": _("Tell us about your paper"),
                "metadata_sub_ib": _("IB grading information and bibliographic details for the submission."),
                "metadata_sub_std": _("Bibliographic information that will appear on the public paper page."),
                "paper_details": _("Paper details"),
                "bibliographic": _("Bibliographic"),
                "language": _("Language"),
                "english": _("English"),
                "chinese": _("Chinese"),
                "subject_category": _("Subject Category"),
                "choose_category": _("Choose a subject category…"),
                "keywords": _("Keywords"),
                "add_another": _("Add another…"),
                "keyword_ph": _("Type a keyword and press Enter"),
                "keyword_hint": _("Press Enter or comma to add. Aim for 3–6 keywords."),
                "added": _("added"),
                "abstract": _("Abstract"),
                "abstract_ph": _("Briefly describe your research background, methods, and conclusions…"),
                "abstract_hint": _("A short summary that appears in search results."),
                "is_ib_sample": _("This is an IB Sample Paper"),
                "is_ib_sample_hint": _("Sample papers are reference essays without an identified author. Checking this will skip the Authors step."),
                "crit_ee_A": _("Framework for the essay"),
                "crit_ee_B": _("Knowledge and understanding"),
                "crit_ee_C": _("Analysis and line of argument"),
                "crit_ee_D": _("Discussion and evaluation"),
                "crit_ee_E": _("Reflection"),
                "ee_subject": _("EE Subject"),
                "core_subject": _("Core Subject"),
                "select_core": _("Select a core subject…"),
                "inter_subject": _("Interdisciplinary"),
                "optional": _("Optional"),
                "select_inter": _("Optional — select if applicable…"),
                "crit_scores": _("Criterion Scores"),
                "crit": _("Crit."),
                "criterion": _("Criterion"),
                "score": _("Score"),
                "overall_grade": _("Overall Grade"),
                "overall_ee_sub": _("Calculated server-side from the criteria above"),
                "crit_comments": _("Criterion Commentaries"),
                "include_comments": _("Include commentaries for all criteria"),
                "include_comments_hint": _("Provide short remarks on each criterion plus an optional overall holistic commentary."),
                "crit_comment_ph": _("Commentary for Criterion %(k)s…"),
                "holistic_comment": _("Holistic Commentary"),
                "holistic_ph": _("An overall holistic commentary for the essay…"),
                "crit_cp_A": _("Investigating"),
                "crit_cp_B": _("Planning"),
                "crit_cp_C": _("Taking Action"),
                "crit_cp_D": _("Reflecting"),
                "global_context": _("Global Context"),
                "select_global": _("Select a Global Context…"),
                "global_contexts": _("Global Contexts"),
                "type_of_action": _("Type of Action"),
                "overall_cp_sub": _("Mean of the four criterion scores, rounded"),
                "author_info": _("Author information"),
                "who_wrote": _("Who wrote this?"),
                "authors_sub": _("The first author's contact details are required. Add co-authors as needed."),
                "name": _("Name"),
                "email": _("Email"),
                "school": _("School / Institution"),
                "remove_author": _("Remove author"),
                "add_author": _("+ Add another author"),
                "file_upload": _("File upload"),
                "upload_pdf": _("Upload your PDF"),
                "upload_pdf_sub": _("Submit a single PDF, up to 50 MB. You can change this before publishing."),
                "no_file_chosen": _("No file chosen"),
                "pdf_only_single": _("PDF only · single file"),
                "replace_file": _("Replace"),
                "choose_file": _("Choose file"),
                "file_save_hint": _("If you'd like to come back to this later, click Save Draft below — your form will be restored next time you visit."),
                "paper_type": _("Paper Type"),
                "first_author": _("First author (name, email, school)"),
                "ee_core": _("EE core subject"),
                "ee_score_x": _("EE criterion score %(k)s"),
                "cp_global": _("Global context"),
                "cp_action_label": _("Type of action"),
                "cp_score_x": _("CP criterion score %(k)s"),
                "pdf_file": _("PDF file"),
                "type_standard": _("Independent Research Paper"),
                "type_ee": _("IB Extended Essay"),
                "type_cp": _("IB Community Project"),
                "review_submit": _("Review & submit"),
                "almost_there": _("Almost there — review your submission"),
                "review_sub": _("Make sure everything looks right. You can jump back to any section to make changes."),
                "missing_fields_one": _("1 field still needs attention"),
                "missing_fields_many": _("%(n)s fields still need attention"),
                "go_to": _("go to %(step)s"),
                "everything_filled": _("Everything required is filled in."),
                "submit_cta": _("Click Submit Paper below to send your submission for review."),
                "edit": _("Edit"),
                "type": _("Type"),
                "metadata_title": _("Metadata"),
                "research_q_short": _("Research Q."),
                "title_short": _("Title"),
                "not_provided": _("Not provided"),
                "not_chosen": _("Not chosen"),
                "subject": _("Subject"),
                "none": _("None"),
                "not_written": _("Not written"),
                "ib_sample": _("IB Sample"),
                "yes_skipped": _("Yes — author info skipped"),
                "no": _("No"),
                "authors": _("Authors"),
                "author": _("Author"),
                "file": _("File"),
                "no_file_uploaded": _("No file uploaded"),
                "ee_details": _("EE Details"),
                "total": _("Total"),
                "cp_details": _("CP Details"),
                "none_selected": _("None selected"),
                "avg_grade": _("Avg. Grade"),
                "saving": _("Saving…"),
                "draft_saved_at": _("Draft saved · %(time)s"),
                "restore_banner_title": _("Unsaved changes from earlier"),
                "restore_banner_body": _("Your last session in this browser had changes you didn't save. Restore them?"),
                "discard_btn": _("Discard"),
                "restore_btn": _("Restore"),
                "search": _("Search…"),
                "no_matches": _("No matches"),
                "ee_autofill_btn": _("Auto-fill from commentary PDF"),
                "ee_autofill_extracting": _("Extracting…"),
                "ee_autofill_ok": _("Extracted all fields."),
                "ee_autofill_partial": _("Extracted %(filled)s of %(total)s fields."),
                "ee_autofill_error": _("Auto-fill failed — try again or fill manually."),
                "ee_autofill_overwrite": _("Replace your existing EE entries with values from the PDF?"),
                "meta_autofill_btn": _("Generate abstract & keywords from PDF"),
                "meta_autofill_extracting": _("Generating…"),
                "meta_autofill_ok": _("Generated abstract and keywords."),
                "meta_autofill_error": _("Generation failed — try again or fill manually."),
                "meta_autofill_overwrite": _("Replace your existing abstract and keywords with AI-generated ones?"),
            },
        }
        return render_template("upload.html",
            user=user,
            form_data=form_data,
            journals=get_journal_names(),
            paper_categories=load_paper_categories(),
            ee_subjects=load_ee_subjects(),
            cp_global_contexts=CP_GLOBAL_CONTEXTS,
            cp_action_types=CP_ACTION_TYPES,
            draft_id=draft_id,
            wizard_boot=wizard_boot,
        )

    @app.route("/dashboard/upload", methods=["GET", "POST"])
    def upload():
        user = require_login(level=1)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        today = datetime.utcnow().date().isoformat()
        draft_id = request.args.get("draft", "")

        # If editing an existing draft, pre-fill form data
        if request.method == "GET" and draft_id:
            draft = _get_submission(draft_id)
            if draft and draft.get("status") == "draft" and draft.get("submitter") == user.get("username", ""):
                form_data = {
                    "title": draft.get("title", ""),
                    "journal": draft.get("journal", ""),
                    "category": draft.get("category", ""),
                    "language": draft.get("language", ""),
                    "keywords": draft.get("keywords", ""),
                    "abstract": draft.get("abstract", ""),
                    "author_name": draft.get("author_name", ""),
                    "author_email": draft.get("author_email", ""),
                    "author_school": draft.get("author_school", ""),
                    "is_ib_sample": draft.get("is_ib_sample", ""),
                    "ib_ee_data": draft.get("ib_ee_data", ""),
                    "cp_data": draft.get("cp_data", ""),
                    "published_at": today,
                }
                # Hydrate EE/CP fieldsets so the wizard can repopulate them.
                form_data.update(parse_ib_ee_data_for_form(draft.get("ib_ee_data", "")))
                form_data.update(parse_cp_data_for_form(draft.get("cp_data", "")))
                return _render_upload(user, form_data, draft_id)

        raw_names = request.form.getlist("author_name")
        raw_emails = request.form.getlist("author_email")
        raw_schools = request.form.getlist("author_school")

        is_ib_sample = request.form.get("is_ib_sample") == "1"

        if is_ib_sample:
            author_names = ["IB SAMPLE"]
            author_emails = [""]
            author_schools = [""]
        else:
            author_names = []
            author_emails = []
            author_schools = []
            for i, name in enumerate(raw_names):
                if name.strip():
                    author_names.append(name.strip())
                    author_emails.append(raw_emails[i].strip() if i < len(raw_emails) else "")
                    author_schools.append(raw_schools[i].strip() if i < len(raw_schools) else "")


        form_data = {
            "title": request.form.get("title", "").strip(),
            "journal": request.form.get("journal", "").strip(),
            "category": request.form.get("category", "").strip(),
            "language": request.form.get("language", "").strip(),
            "keywords": request.form.get("keywords", "").strip(),
            "abstract": request.form.get("abstract", "").strip(),
            "author_name": ", ".join(author_names),
            "author_email": ", ".join(author_emails),
            "author_school": ", ".join(author_schools),
            "published_at": today,
            "is_ib_sample": "1" if is_ib_sample else "",
        }

        # ---- IB EE data processing ----
        is_ib_ee = request.form.get("is_ib_ee") == "1"
        if is_ib_ee:
            form_data["ib_ee_data"] = build_ib_ee_data_from_form(request.form)
            form_data["is_ib_ee"] = "1"
        else:
            form_data["ib_ee_data"] = ""

        # ---- CP Paper data processing ----
        is_cp_paper = request.form.get("is_cp_paper") == "1"
        if is_cp_paper:
            form_data["cp_data"] = build_cp_data_from_form(request.form)
            form_data["is_cp_paper"] = "1"
        else:
            form_data["cp_data"] = ""

        if request.method == "POST":
            # Handle "Save as Draft"
            draft_id = request.form.get("draft_id", "").strip()
            if "save_draft" in request.form:
                if not form_data["title"]:
                    flash(_("Please enter at least a paper title to save a draft."), "warning")
                    return _render_upload(user, form_data, draft_id)
                # Format keywords
                if form_data["keywords"]:
                    form_data["keywords"] = ", ".join(
                        [kw.strip() for kw in form_data["keywords"].split(",") if kw.strip()]
                    )
                now = datetime.utcnow().isoformat()
                if draft_id:
                    # Update existing draft
                    _update_submission(draft_id, {
                        "title": form_data["title"],
                        "journal": form_data["journal"],
                        "category": form_data["category"],
                        "language": form_data["language"],
                        "keywords": form_data["keywords"],
                        "abstract": form_data["abstract"],
                        "author_name": form_data["author_name"],
                        "author_email": form_data["author_email"],
                        "author_school": form_data["author_school"],
                        "is_ib_sample": form_data.get("is_ib_sample", ""),
                        "ib_ee_data": form_data.get("ib_ee_data", ""),
                        "cp_data": form_data.get("cp_data", ""),
                        "submitted_at": now,
                    })
                else:
                    # Create new draft
                    sub_id = uuid4().hex[:12]
                    submission = {
                        "id": sub_id,
                        "pdf_filename": "",
                        "pending_filename": "",
                        "submitter": user.get("username", ""),
                        "submitter_name": user.get("display_name", "") or user.get("first_name", "") or user.get("username", ""),
                        "status": "draft",
                        "submitted_at": now,
                        "reviewed_at": "",
                        "reviewer": "",
                        "comment": "",
                        "title": form_data["title"],
                        "journal": form_data["journal"],
                        "category": form_data["category"],
                        "language": form_data["language"],
                        "keywords": form_data["keywords"],
                        "abstract": form_data["abstract"],
                        "author_name": form_data["author_name"],
                        "author_email": form_data["author_email"],
                        "author_school": form_data["author_school"],
                        "is_ib_sample": form_data.get("is_ib_sample", ""),
                        "ib_ee_data": form_data.get("ib_ee_data", ""),
                        "cp_data": form_data.get("cp_data", ""),
                    }
                    _save_submission(submission)
                flash(_("Draft saved successfully."), "success")
                return redirect(url_for("my_submissions"))

            # Per-type required-field cascade. Keywords/abstract apply to Standard
            # papers only; author fields are skipped for IB Sample submissions.
            required = ["title", "category", "language"]
            if not (is_ib_ee or is_cp_paper):
                required += ["keywords", "abstract"]
            if not is_ib_sample:
                required += ["author_name", "author_email", "author_school"]

            for field in required:
                if not form_data.get(field):
                    flash(_MISSING_FIELD_MESSAGES[field], "danger")
                    return _render_upload(user, form_data, draft_id)

            if is_ib_ee and is_cp_paper:
                flash(_("A paper cannot be both an Extended Essay and a CP Paper."), "danger")
                return _render_upload(user, form_data, draft_id)
            if is_ib_ee:
                ib_data = json.loads(form_data["ib_ee_data"])
                if not ib_data.get("core_subject"):
                    flash(_("Please select an EE core subject."), "danger")
                    return _render_upload(user, form_data, draft_id)
            if is_cp_paper:
                cp_data = json.loads(form_data["cp_data"])
                if not cp_data.get("global_context"):
                    flash(_("Please select a Global Context."), "danger")
                    return _render_upload(user, form_data, draft_id)
                if not cp_data.get("action_types"):
                    flash(_("Please select at least one Type of Action."), "danger")
                    return _render_upload(user, form_data, draft_id)

            # 格式化关键词
            if form_data["keywords"]:
                form_data["keywords"] = ", ".join(
                    [kw.strip() for kw in form_data["keywords"].split(",") if kw.strip()]
                )

            file = request.files.get("paper")
            if not file or file.filename == "":
                flash(_("Please select a file to upload"), "warning")
            else:
                original_filename = secure_filename(file.filename)
                if not original_filename:
                    original_filename = f"{uuid4().hex[:8]}.pdf"
                if not allowed_file(original_filename):
                    flash(_("Only PDF files are supported"), "danger")
                else:
                    # Build a safe filename: try title+author first, fall back to UUID
                    filename = _build_safe_paper_filename(
                        form_data["title"], form_data["author_name"]
                    )
                    role = int(user.get("role", "1"))
                    if role >= 2:
                        # Moderator / Admin: publish directly
                        save_path = PAPERS_DIR / filename
                        if save_path.exists():
                            flash(_("A file with this name already exists"), "warning")
                        else:
                            file.save(save_path)
                            set_pdf_metadata(save_path, form_data["title"], form_data["author_name"])
                            upsert_paper_metadata(
                                filename,
                                {
                                    "title": form_data["title"],
                                    "journal": form_data["journal"],
                                    "category": form_data["category"],
                                    "language": form_data["language"],
                                    "keywords": form_data["keywords"],
                                    "abstract": form_data["abstract"],
                                    "author_name": form_data["author_name"],
                                    "author_email": form_data["author_email"],
                                    "author_school": form_data["author_school"],
                                    "published_at": form_data["published_at"],
                                    "is_ib_sample": form_data.get("is_ib_sample", ""),
                                    "ib_ee_data": form_data.get("ib_ee_data", ""),
                                    "cp_data": form_data.get("cp_data", ""),
                                },
                            )
                            flash(_("Paper %(filename)s uploaded successfully!", filename=filename), "success")
                            return redirect(url_for("upload"))
                    else:
                        # Reader: save to pending review queue
                        if draft_id:
                            sub_id = draft_id
                        else:
                            sub_id = uuid4().hex[:12]
                        pending_filename = f"{sub_id}_{filename}"
                        pending_path = PENDING_PAPERS_DIR / pending_filename
                        file.save(pending_path)
                        set_pdf_metadata(pending_path, form_data["title"], form_data["author_name"])
                        if draft_id:
                            _update_submission(draft_id, {
                                "pdf_filename": filename,
                                "pending_filename": pending_filename,
                                "status": "pending",
                                "submitted_at": datetime.utcnow().isoformat(),
                                "title": form_data["title"],
                                "journal": form_data["journal"],
                                "category": form_data["category"],
                                "language": form_data["language"],
                                "keywords": form_data["keywords"],
                                "abstract": form_data["abstract"],
                                "author_name": form_data["author_name"],
                                "author_email": form_data["author_email"],
                                "author_school": form_data["author_school"],
                                "is_ib_sample": form_data.get("is_ib_sample", ""),
                                "ib_ee_data": form_data.get("ib_ee_data", ""),
                                "cp_data": form_data.get("cp_data", ""),
                            })
                        else:
                            submission = {
                                "id": sub_id,
                                "pdf_filename": filename,
                                "pending_filename": pending_filename,
                                "submitter": user.get("username", ""),
                                "submitter_name": user.get("display_name", "") or user.get("first_name", "") or user.get("username", ""),
                                "status": "pending",
                                "submitted_at": datetime.utcnow().isoformat(),
                                "reviewed_at": "",
                                "reviewer": "",
                                "comment": "",
                                "title": form_data["title"],
                                "journal": form_data["journal"],
                                "category": form_data["category"],
                                "language": form_data["language"],
                                "keywords": form_data["keywords"],
                                "abstract": form_data["abstract"],
                                "author_name": form_data["author_name"],
                                "author_email": form_data["author_email"],
                                "author_school": form_data["author_school"],
                                "is_ib_sample": form_data.get("is_ib_sample", ""),
                                "ib_ee_data": form_data.get("ib_ee_data", ""),
                                "cp_data": form_data.get("cp_data", ""),
                            }
                            _save_submission(submission)
                        return redirect(url_for("upload_success", title=form_data["title"]))

        return _render_upload(user, form_data, request.args.get("draft", ""))

    @app.route("/dashboard/upload/success")
    def upload_success():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        title = request.args.get("title", "")
        submitted_at = datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S")
        return render_template("upload_success.html", user=user, title=title, submitted_at=submitted_at)

    @app.route("/upload", endpoint="upload_legacy")
    def upload_legacy():
        return redirect(url_for("upload"), code=301)

    @app.route("/upload/success", endpoint="upload_success_legacy")
    def upload_success_legacy():
        return redirect(url_for("upload_success"), code=301)

    @app.route("/dashboard/manage")
    def manage():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        # Build list of papers with metadata
        meta_rows = load_paper_metadata()
        meta_map = {r["filename"]: r for r in meta_rows}

        pdf_files = sorted(p.name for p in PAPERS_DIR.glob("*.pdf"))
        papers = []
        for fname in pdf_files:
            m = meta_map.get(fname, {})
            papers.append({
                "filename": fname,
                "title": m.get("title", "") or fname,
                "category": m.get("category", ""),
                "keywords": m.get("keywords", ""),
                "abstract": m.get("abstract", ""),
                "author_name": m.get("author_name", ""),
                "author_email": m.get("author_email", ""),
                "author_school": m.get("author_school", ""),
                "published_at": m.get("published_at", ""),
            })

        return render_template("delete.html", user=user, papers=papers)

    @app.route("/manage", endpoint="manage_legacy")
    def manage_legacy():
        return redirect(url_for("manage"), code=301)

    @app.route("/paper/<path:filename>/info")
    def paper_info(filename):
        """Return paper metadata as JSON for the preview modal."""
        user = require_login(level=3)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        meta_rows = load_paper_metadata()
        meta = {}
        for r in meta_rows:
            if r.get("filename") == filename:
                meta = r
                break
        return jsonify({
            "filename": filename,
            "title": meta.get("title", "") or filename,
            "category": meta.get("category", ""),
            "keywords": meta.get("keywords", ""),
            "abstract": meta.get("abstract", ""),
            "author_name": meta.get("author_name", ""),
            "author_email": meta.get("author_email", ""),
            "author_school": meta.get("author_school", ""),
            "published_at": meta.get("published_at", ""),
            "pdf_url": url_for("paper_file", filename=filename),
        })

    @app.route("/dashboard/paper/<path:filename>/modify", methods=["GET", "POST"])
    def paper_modify(filename):
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        paper_path = PAPERS_DIR / filename
        if not paper_path.exists():
            flash(_("Paper not found."), "warning")
            return redirect(url_for("manage"))

        meta_rows = load_paper_metadata()
        meta = {}
        for r in meta_rows:
            if r.get("filename") == filename:
                meta = r
                break

        def parsed_authors_from_meta(meta_row):
            names = meta_row.get("author_name", "").split(", ")
            emails = meta_row.get("author_email", "").split(", ")
            schools = meta_row.get("author_school", "").split(", ")

            parsed = []
            for i, name in enumerate(names):
                if name.strip():
                    parsed.append({
                        "name": name.strip(),
                        "email": emails[i].strip() if i < len(emails) else "",
                        "school": schools[i].strip() if i < len(schools) else ""
                    })
            if not parsed:
                parsed = [{"name": "", "email": "", "school": ""}]
            return parsed

        def render_modify_form(meta_row):
            return render_template(
                "paper_modify.html",
                user=user,
                filename=filename,
                meta=meta_row,
                parsed_authors=parsed_authors_from_meta(meta_row),
                categories=load_paper_categories(),
                journals=get_journal_names(),
                ee_subjects=load_ee_subjects(),
                cp_global_contexts=CP_GLOBAL_CONTEXTS,
                cp_action_types=CP_ACTION_TYPES,
                ib_criteria_defs=IB_EE_CRITERIA_DEFS,
                cp_criteria_defs=CP_CRITERIA_DEFS,
            )

        if request.method == "POST":
            title = request.form.get("title", "").strip()

            raw_names = request.form.getlist("author_name")
            raw_emails = request.form.getlist("author_email")
            raw_schools = request.form.getlist("author_school")

            is_ib_sample = request.form.get("is_ib_sample") == "1"
            is_ib_ee = request.form.get("is_ib_ee") == "1"
            is_cp_paper = request.form.get("is_cp_paper") == "1"
            ib_ee_data = build_ib_ee_data_from_form(request.form) if is_ib_ee else ""
            cp_data = build_cp_data_from_form(request.form) if is_cp_paper else ""

            if is_ib_sample:
                author_names = ["IB SAMPLE"]
                author_emails = [""]
                author_schools = [""]
            else:
                author_names = []
                author_emails = []
                author_schools = []
                for i, name in enumerate(raw_names):
                    if name.strip():
                        author_names.append(name.strip())
                        author_emails.append(raw_emails[i].strip() if i < len(raw_emails) else "")
                        author_schools.append(raw_schools[i].strip() if i < len(raw_schools) else "")

            final_author_name = ", ".join(author_names)
            final_author_email = ", ".join(author_emails)
            final_author_school = ", ".join(author_schools)

            form_meta = {
                **meta,
                "title": title,
                "journal": request.form.get("journal", "").strip(),
                "category": request.form.get("category", "").strip(),
                "language": request.form.get("language", "").strip(),
                "keywords": request.form.get("keywords", "").strip(),
                "abstract": request.form.get("abstract", "").strip(),
                "author_name": final_author_name,
                "author_email": final_author_email,
                "author_school": final_author_school,
                "published_at": meta.get("published_at", ""),
                "is_ib_sample": "1" if is_ib_sample else "",
                "ib_ee_data": ib_ee_data,
                "cp_data": cp_data,
            }

            if is_ib_ee and is_cp_paper:
                flash(_("A paper cannot be both an Extended Essay and a CP Paper."), "danger")
                return render_modify_form(form_meta)
            if is_ib_ee and not request.form.get("ib_ee_core_subject", "").strip():
                flash(_("Please select an EE core subject."), "danger")
                return render_modify_form(form_meta)
            if is_cp_paper and not request.form.get("cp_global_context", "").strip():
                flash(_("Please select a Global Context."), "danger")
                return render_modify_form(form_meta)
            if is_cp_paper and not request.form.getlist("cp_action_type"):
                flash(_("Please select at least one Type of Action."), "danger")
                return render_modify_form(form_meta)

            # We use the raw first author for the filename
            primary_author = author_names[0] if author_names else "author"
            new_filename = _build_safe_paper_filename(title, primary_author)
            if new_filename != filename:
                new_paper_path = PAPERS_DIR / new_filename
                if new_paper_path.exists():
                    flash(_("A file with the new name already exists, unable to rename."), "warning")
                    return redirect(url_for("paper_modify", filename=filename))
                else:
                    paper_path.rename(new_paper_path)
                    remove_paper_metadata(filename)
                    filename = new_filename

            set_pdf_metadata(PAPERS_DIR / filename, title, final_author_name)

            upsert_paper_metadata(filename, {
                "title": title,
                "journal": request.form.get("journal", "").strip(),
                "category": request.form.get("category", "").strip(),
                "language": request.form.get("language", "").strip(),
                "keywords": request.form.get("keywords", "").strip(),
                "abstract": request.form.get("abstract", "").strip(),
                "author_name": final_author_name,
                "author_email": final_author_email,
                "author_school": final_author_school,
                "published_at": meta.get("published_at", ""),
                "is_ib_sample": "1" if is_ib_sample else "",
                "ib_ee_data": ib_ee_data,
                "cp_data": cp_data,
            })
            flash(_("Paper information updated."), "success")
            return redirect(url_for("manage"))

        return render_modify_form(meta)

    @app.route("/paper/<path:filename>/modify", endpoint="paper_modify_legacy")
    def paper_modify_legacy(filename):
        return redirect(url_for("paper_modify", filename=filename), code=301)

    @app.route("/dashboard/paper/<path:filename>/delete", methods=["POST"])
    def paper_delete(filename):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))

        paper_path = PAPERS_DIR / filename
        if not paper_path.exists():
            flash(_("Paper not found."), "warning")
            return redirect(url_for("manage"))

        remove_paper_metadata(filename)
        paper_path.unlink(missing_ok=True)
        flash(_("Deleted %(filename)s.", filename=filename), "success")
        try:
            rag_index.purge(filename)
        except Exception:
            app.logger.exception("Failed to purge chunks for deleted paper")
        return redirect(url_for("manage"))

    @app.route("/set-language/<locale_code>")
    def set_language(locale_code: str):
        if locale_code not in SUPPORTED_LOCALES:
            flash(_("Language not supported."), "warning")
        else:
            session["language"] = locale_code
        if session.get("user") and session.get("session_token"):
            refresh_session(session["user"].get("username", ""), session.get("session_token"))
        next_url = request.args.get("next")
        if not next_url or not next_url.startswith("/"):
            referrer = request.referrer
            if referrer:
                parsed = urlparse(referrer)
                if parsed.path:
                    next_url = parsed.path
        if not next_url or not next_url.startswith("/"):
            destination = "dashboard" if session.get("user") else "login"
            next_url = url_for(destination)
        return redirect(next_url)

    @app.route("/preview/<path:filename>")
    def preview_paper(filename: str):
        user = get_active_user()
        is_guest = user is None
        pdf_path = PAPERS_DIR / filename
        if not pdf_path.exists():
            flash(_("Paper not found."), "danger")
            return redirect(url_for("search"))
        paper = build_paper_record(filename)
        source_query = request.args.get("q", "").strip()
        source_page = request.args.get("page", "").strip()
        related_papers = []
        if paper.get("category"):
            all_papers = gather_paper_records()
            related_papers = [
                p for p in all_papers
                if p.get("category") == paper.get("category") and p.get("filename") != filename
            ][:5]

        pdf_url = url_for("paper_file", filename=filename) if (not is_guest or OPEN_ACCESS) else url_for("paper_preview", filename=filename)
        
        # Parse authors
        names = paper.get("author_name", "").split(", ")
        emails = paper.get("author_email", "").split(", ")
        schools = paper.get("author_school", "").split(", ")
        parsed_authors = []
        for i, name in enumerate(names):
            if name.strip():
                parsed_authors.append({
                    "name": name.strip(),
                    "email": emails[i].strip() if i < len(emails) else "",
                    "school": schools[i].strip() if i < len(schools) else ""
                })
        
        # Deduplicate schools
        unique_schools = []
        for s in schools:
            s_clean = s.strip()
            if s_clean and s_clean not in unique_schools:
                unique_schools.append(s_clean)
        unique_schools_str = ", ".join(unique_schools) if unique_schools else ""

        # Parse IB EE data if present
        ib_ee_info = None
        raw_ib = paper.get("ib_ee_data", "")
        if raw_ib:
            try:
                ib_ee_info = json.loads(raw_ib)
            except (json.JSONDecodeError, TypeError):
                pass

        # Parse CP data if present
        cp_info = None
        raw_cp = paper.get("cp_data", "")
        if raw_cp:
            try:
                cp_info = json.loads(raw_cp)
            except (json.JSONDecodeError, TypeError):
                pass

        return render_template(
            "preview.html",
            user=user,
            paper=paper,
            parsed_authors=parsed_authors,
            unique_schools_str=unique_schools_str,
            related_papers=related_papers,
            source_query=source_query,
            source_page=source_page,
            is_guest=is_guest,
            pdf_url=pdf_url,
            journal_id_map=get_journal_id_map(),
            ib_ee_info=ib_ee_info,
            cp_info=cp_info,
        )

    @app.route("/papers/preview/<path:filename>")
    def paper_preview(filename: str):
        pdf_path = PAPERS_DIR / filename
        if not pdf_path.exists():
            abort(404)
        preview_stream = build_preview_pdf(pdf_path, max_pages=2)
        return send_file(preview_stream, mimetype="application/pdf", download_name=filename)

    @app.route("/papers/raw/<path:filename>")
    def paper_file(filename: str):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        pdf_path = PAPERS_DIR / filename
        if not pdf_path.exists():
            abort(404)
        return send_from_directory(PAPERS_DIR, filename, as_attachment=False)

    @app.route("/papers/<path:filename>")
    def download(filename: str):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        return send_from_directory(PAPERS_DIR, filename, as_attachment=True)

    # ==================== NEWS ROUTES ====================

    @app.route("/news")
    def news_list():
        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        per_page = 15
        all_articles = load_news_articles(status="published")
        pagination = paginate_records(all_articles, page, per_page)
        recent = all_articles[:6]
        return render_template(
            "news.html",
            articles=pagination["items"],
            pagination=pagination,
            recent=recent,
        )

    @app.route("/dashboard/news/upload-inline-image", methods=["POST"])
    def news_upload_inline_image():
        """AJAX endpoint: upload an image for the block editor and return its URL."""
        user = require_login(level=2)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        img_file = request.files.get("file")
        if not img_file or not img_file.filename:
            return jsonify({"error": "No file provided"}), 400
        img_ext = img_file.filename.rsplit(".", 1)[-1].lower() if "." in img_file.filename else ""
        if img_ext not in ALLOWED_IMAGE_EXTENSIONS:
            return jsonify({"error": "Invalid image format"}), 400
        NEWS_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        unique_name = f"{uuid4().hex[:12]}_{secure_filename(img_file.filename)}"
        img_file.save(NEWS_IMAGES_DIR / unique_name)
        img_url = url_for("static", filename=f"uploads/news/{unique_name}")
        return jsonify({"url": img_url})

    @app.route("/api/upload/extract-ee-metadata", methods=["POST"])
    def api_extract_ee_metadata():
        user = require_login(level=2)
        if not user:
            return jsonify({"error": str(_("Unauthorized"))}), 401

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        if not upload.filename.lower().endswith(".pdf"):
            return jsonify({"error": str(_("File must be a PDF"))}), 400

        raw = upload.read()
        if not raw.startswith(b"%PDF-"):
            return jsonify({"error": str(_("File is not a valid PDF"))}), 400

        try:
            result = extract_ee_metadata(raw)
        except EePdfExtractionError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 200

    @app.route("/api/conversations", methods=["GET", "POST"])
    def api_conversations():
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        owner = _ask_owner_key()
        if request.method == "POST":
            import secrets
            now = datetime.utcnow().isoformat()
            with db_session() as db:
                serial = secrets.token_urlsafe(5)[:6]
                conv = ConversationModel(owner_key=owner,
                                         serial=serial,
                                         title=str(_("New conversation")),
                                         created_at=now, updated_at=now)
                db.add(conv)
                db.flush()
                cid = conv.serial
            return jsonify({"id": cid, "title": str(_("New conversation"))}), 201
        with db_session() as db:
            rows = (db.query(ConversationModel)
                      .filter(ConversationModel.owner_key == owner)
                      .order_by(ConversationModel.updated_at.desc()).all())
            items = [{"id": r.serial, "title": r.title, "updated_at": r.updated_at} for r in rows]
        return jsonify({"conversations": items})

    @app.route("/api/conversations/<string:serial>", methods=["GET", "PATCH", "DELETE"])
    def api_conversation_item(serial):
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        owner = _ask_owner_key()
        with db_session() as db:
            conv = db.query(ConversationModel).filter(
                ConversationModel.serial == serial,
                ConversationModel.owner_key == owner).first()
            if not conv:
                return jsonify({"error": str(_("Not found"))}), 404
            if request.method == "DELETE":
                db.query(ChatMessageModel).filter(
                    ChatMessageModel.conversation_id == conv.id).delete()
                db.query(AttachmentChunkModel).filter(
                    AttachmentChunkModel.conversation_id == conv.id).delete()
                db.delete(conv)
                return jsonify({"ok": True})
            if request.method == "PATCH":
                data = request.get_json(silent=True) or {}
                title = (data.get("title") or "").strip()
                if title:
                    conv.title = title[:255]
                return jsonify({"ok": True, "title": conv.title})
            # GET messages
            msgs = (db.query(ChatMessageModel)
                      .filter(ChatMessageModel.conversation_id == conv.id)
                      .order_by(ChatMessageModel.id.asc()).all())
            out = []
            for m in msgs:
                try:
                    cites = json.loads(m.citations) if m.citations else []
                except (ValueError, TypeError):
                    cites = []
                out.append({"role": m.role, "content": m.content, "citations": cites})
            att = (db.query(AttachmentChunkModel.filename)
                     .filter(AttachmentChunkModel.conversation_id == conv.id)
                     .distinct().all())
            attachments = [a[0] for a in att]
            return jsonify({"title": conv.title, "messages": out,
                            "attachments": attachments})

    @app.route("/api/ask/papers")
    def api_ask_papers():
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        q = (request.args.get("q") or "").strip()
        records = search_papers(q) if q else gather_paper_records()
        items = [{
            "filename": r["filename"],
            "title": r.get("title") or r["filename"],
            "authors": r.get("author_name", ""),
            "category": r.get("category", ""),
            "abstract": (r.get("abstract") or "")[:400],
        } for r in records[:50]]
        return jsonify({"papers": items})

    @app.route("/api/ask/attach", methods=["POST", "DELETE"])
    def api_ask_attach():
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        if not llm_client.llm_enabled():
            return jsonify({"error": str(_("AI assistant is not configured."))}), 503
        owner = _ask_owner_key()
        conv_serial = (request.values.get("conversation_id") or "").strip()
        with db_session() as db:
            conv = db.query(ConversationModel).filter(
                ConversationModel.serial == conv_serial,
                ConversationModel.owner_key == owner).first()
            conv_id = conv.id if conv else None
        if conv_id is None:
            return jsonify({"error": str(_("Conversation not found."))}), 404

        if request.method == "DELETE":
            fname = (request.values.get("filename") or "").strip()
            with db_session() as db:
                db.query(AttachmentChunkModel).filter(
                    AttachmentChunkModel.conversation_id == conv_id,
                    AttachmentChunkModel.filename == fname).delete()
            return jsonify({"ok": True})

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        name = upload.filename
        if not name.lower().endswith((".pdf", ".docx", ".txt", ".md")):
            return jsonify({"error": str(_("Unsupported file type. Use PDF, DOCX, TXT, or Markdown."))}), 400
        raw = upload.read()
        if len(raw) > MAX_ATTACH_BYTES:
            return jsonify({"error": str(_("File is too large (max 5 MB)."))}), 400
        try:
            text = extract_text_from_upload(name, raw)
        except Exception:
            app.logger.exception("attachment extraction failed")
            return jsonify({"error": str(_("Could not read the file."))}), 400
        chunks = rag_index.chunk_text(text)
        if not chunks:
            return jsonify({"error": str(_("No readable text found in the file."))}), 400
        try:
            vectors = rag_index.embed_texts(chunks)
        except Exception:
            app.logger.exception("attachment embedding failed")
            return jsonify({"error": str(_("Something went wrong. Please try again."))}), 502
        display = name[:255]
        now = datetime.utcnow().isoformat()
        with db_session() as db:
            db.query(AttachmentChunkModel).filter(
                AttachmentChunkModel.conversation_id == conv_id,
                AttachmentChunkModel.filename == display).delete()
            for i, ch in enumerate(chunks):
                db.add(AttachmentChunkModel(
                    conversation_id=conv_id, filename=display, chunk_index=i,
                    content=ch, embedding=json.dumps(vectors[i]), created_at=now))
        return jsonify({"ok": True, "filename": display, "chunks": len(chunks)})

    @app.route("/api/ask", methods=["POST"])
    def api_ask():
        blocked = require_ask_api_access()
        if blocked:
            return blocked
        if not llm_client.llm_enabled():
            return jsonify({"error": str(_("AI assistant is not configured."))}), 503

        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "?").split(",")[0].strip()
        if not _ask_rate_ok(ip):
            return jsonify({"error": str(_("Too many requests — please slow down."))}), 429

        data = request.get_json(silent=True) or {}
        question = (data.get("question") or "").strip()
        mode = data.get("mode") if data.get("mode") in ("flash", "think") else "flash"
        forced = data.get("paper_filenames") or []   # Phase 3 (ignored if empty)
        web_on = bool(data.get("web"))
        if not question:
            return jsonify({"error": str(_("Please enter a question."))}), 400
        if len(question) > MAX_QUESTION_CHARS:
            return jsonify({"error": str(_("Your question is too long."))}), 400

        conv_serial = data.get("conversation_id")
        owner = _ask_owner_key()
        history_rows = []
        db_conv_id = None
        if conv_serial is not None:
            with db_session() as db:
                conv = db.query(ConversationModel).filter(
                    ConversationModel.serial == conv_serial,
                    ConversationModel.owner_key == owner).first()
                if conv:
                    db_conv_id = conv.id
                    now = datetime.utcnow().isoformat()
                    db.add(ChatMessageModel(conversation_id=db_conv_id, role="user",
                                            content=question, citations="",
                                            created_at=now))
                    # title the conversation from its first question
                    if conv.title == str(_("New conversation")):
                        conv.title = question[:60]
                    conv.updated_at = now
                    db.flush()
                    history_rows = (db.query(ChatMessageModel)
                                      .filter(ChatMessageModel.conversation_id == db_conv_id)
                                      .order_by(ChatMessageModel.id.asc()).all())
                    history_rows = [{"role": row.role, "content": row.content} for row in history_rows]
                else:
                    conv_serial = None
        llm_messages = _ask_llm_messages(question, history_rows)

        locale_code = str(get_locale() or "en")

        # Retrieve grounding: attached docs first (highest priority), then forced
        # papers (Phase 3) or automatic retrieval, capped to a shared budget.
        try:
            attach_hits = _attachment_grounding(question, db_conv_id)
            if forced:
                lib_hits = _forced_grounding(question, forced)
            else:
                lib_hits = rag_index.retrieve(question)
            hits = (attach_hits + lib_hits)[:6]
        except Exception:
            app.logger.exception("retrieval failed")
            hits = []

        model = llm_client.think_model() if mode == "think" else llm_client.flash_model()
        citations = [
            {"n": i + 1, "filename": h["filename"], "title": h["title"],
             "authors": h.get("author_name", ""),
             "url": (None if h.get("is_attachment")
                     else url_for("paper_info", filename=h["filename"]))}
            for i, h in enumerate(hits)
        ]
        web_results = []
        if web_on and web_search.web_search_enabled():
            try:
                web_results = web_search.web_search(question)
            except Exception:
                app.logger.exception("web search failed")
                web_results = []
        system = _build_ask_prompt(question, hits, locale_code, web_results)
        web_items = [
            {"n": len(hits) + j + 1, "title": w["title"], "url": w["url"]}
            for j, w in enumerate(web_results)
        ]

        def generate():
            import json as _json
            full = []
            try:
                client = llm_client.build_client()
                stream = client.chat.completions.create(
                    model=model, temperature=0.2, stream=True,
                    messages=[{"role": "system", "content": system}] + llm_messages,
                )
                for chunk in stream:
                    delta = ""
                    try:
                        delta = chunk.choices[0].delta.content or ""
                    except (AttributeError, IndexError):
                        delta = ""
                    if delta:
                        full.append(delta)
                        yield "data: " + _json.dumps({"type": "token", "text": delta}) + "\n\n"
                yield "data: " + _json.dumps({"type": "citations", "items": citations}) + "\n\n"
                if web_items:
                    yield "data: " + _json.dumps({"type": "web", "items": web_items}) + "\n\n"
                if db_conv_id is not None:
                    try:
                        with db_session() as db:
                            conv = db.query(ConversationModel).filter(
                                ConversationModel.id == db_conv_id,
                                ConversationModel.owner_key == owner).first()
                            if conv:
                                db.add(ChatMessageModel(
                                    conversation_id=db_conv_id, role="assistant",
                                    content="".join(full),
                                    citations=_json.dumps(citations),
                                    created_at=datetime.utcnow().isoformat()))
                    except Exception:
                        app.logger.exception("failed to persist assistant message")
                yield "data: " + _json.dumps({"type": "done"}) + "\n\n"
            except Exception:
                app.logger.exception("LLM stream failed")
                yield "data: " + _json.dumps({"type": "error",
                       "message": str(_("Something went wrong. Please try again."))}) + "\n\n"

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    @app.route("/api/upload/generate-abstract-keywords", methods=["POST"])
    def api_generate_abstract_keywords():
        user = require_login(level=2)
        if not user:
            return jsonify({"error": str(_("Unauthorized"))}), 401

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        if not upload.filename.lower().endswith(".pdf"):
            return jsonify({"error": str(_("File must be a PDF"))}), 400

        raw = upload.read()
        if not raw.startswith(b"%PDF-"):
            return jsonify({"error": str(_("File is not a valid PDF"))}), 400

        language = request.form.get("language", "en")
        try:
            result = generate_abstract_keywords(raw, language)
        except LLMMetadataError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 200

    @app.route("/dashboard/news/publish", methods=["GET", "POST"])
    def news_publish():
        user = require_login(level=2)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        display_name = user.get("display_name") or user.get("username", "")
        form_data = {
            "title": request.form.get("title", "").strip(),
            "category": request.form.get("category", "").strip(),
            "abstract": request.form.get("abstract", "").strip(),
            "body": request.form.get("body", "").strip(),
            "author": request.form.get("author", "").strip() or display_name,
            "image_url": "",
            "status": "",
        }

        if request.method == "POST":
            action = request.form.get("action", "publish")
            is_draft = action == "draft"

            # Drafts require only a title; publish requires the full set.
            if not form_data["title"]:
                flash(_("Please enter a title."), "warning")
            elif not is_draft and not form_data["category"]:
                flash(_("Please select a category."), "warning")
            elif not is_draft and not form_data["abstract"]:
                flash(_("Please enter an abstract."), "warning")
            elif not is_draft and not form_data["body"]:
                flash(_("Please write the article body."), "warning")
            else:
                article_id = uuid4().hex[:12]
                image_url = ""
                cover_file = request.files.get("cover_image")
                if cover_file and cover_file.filename:
                    img_ext = cover_file.filename.rsplit(".", 1)[-1].lower() if "." in cover_file.filename else ""
                    if img_ext in ALLOWED_IMAGE_EXTENSIONS:
                        NEWS_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
                        safe_name = f"{article_id}_{secure_filename(cover_file.filename)}"
                        cover_file.save(NEWS_IMAGES_DIR / safe_name)
                        image_url = url_for("static", filename=f"uploads/news/{safe_name}")
                    else:
                        flash(_("Cover image must be PNG, JPG, GIF, or WebP."), "warning")
                        return render_template(
                            "news_publish.html",
                            form_data=form_data,
                            categories=load_categories(),
                            editing=False,
                            user=user,
                        )
                article = {
                    "id": article_id,
                    "title": form_data["title"],
                    "category": form_data["category"],
                    "abstract": form_data["abstract"],
                    "body": form_data["body"],
                    "author": form_data["author"],
                    "image_url": image_url,
                    "published_at": "" if is_draft else datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                    "status": "pending" if is_draft else "published",
                }
                save_news_article(article)
                if is_draft:
                    flash(_("Draft saved."), "success")
                else:
                    flash(_("Article published successfully."), "success")
                return redirect(url_for("news_manage"))

        return render_template(
            "news_publish.html",
            form_data=form_data,
            categories=load_categories(),
            editing=False,
            user=user,
        )

    @app.route("/dashboard/news/<news_id>/edit", methods=["GET", "POST"])
    def news_edit(news_id: str):
        user = require_login(level=2)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        article = get_news_article(news_id)
        if not article:
            flash(_("Article not found."), "warning")
            return redirect(url_for("news_list"))

        form_data = {
            "title": article["title"],
            "category": article["category"],
            "abstract": article["abstract"],
            "body": article["body"],
            "author": article["author"],
            "image_url": article["image_url"],
            "status": article.get("status", "published"),
        }

        if request.method == "POST":
            action = request.form.get("action", "publish")
            is_draft = action == "draft"
            form_data = {
                "title": request.form.get("title", "").strip(),
                "category": request.form.get("category", "").strip(),
                "abstract": request.form.get("abstract", "").strip(),
                "body": request.form.get("body", "").strip(),
                "author": request.form.get("author", "").strip(),
                "image_url": article["image_url"],
                "status": article.get("status", "published"),
            }
            if not form_data["title"]:
                flash(_("Please enter a title."), "warning")
            elif not is_draft and not form_data["category"]:
                flash(_("Please select a category."), "warning")
            elif not is_draft and not form_data["abstract"]:
                flash(_("Please enter an abstract."), "warning")
            elif not is_draft and not form_data["body"]:
                flash(_("Please write the article body."), "warning")
            else:
                cover_file = request.files.get("cover_image")
                if cover_file and cover_file.filename:
                    img_ext = cover_file.filename.rsplit(".", 1)[-1].lower() if "." in cover_file.filename else ""
                    if img_ext in ALLOWED_IMAGE_EXTENSIONS:
                        NEWS_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
                        safe_name = f"{news_id}_{secure_filename(cover_file.filename)}"
                        cover_file.save(NEWS_IMAGES_DIR / safe_name)
                        form_data["image_url"] = url_for("static", filename=f"uploads/news/{safe_name}")
                    else:
                        flash(_("Cover image must be PNG, JPG, GIF, or WebP."), "warning")
                        return render_template(
                            "news_publish.html",
                            form_data=form_data,
                            categories=load_categories(),
                            editing=True,
                            user=user,
                        )
                if request.form.get("remove_image") == "1":
                    form_data["image_url"] = ""
                form_data["status"] = "pending" if is_draft else "published"
                update_news_article(news_id, form_data)
                if is_draft:
                    flash(_("Draft saved."), "success")
                else:
                    flash(_("Article updated."), "success")
                return redirect(url_for("news_manage"))

        return render_template(
            "news_publish.html",
            form_data=form_data,
            categories=load_categories(),
            editing=True,
            user=user,
        )

    @app.route("/dashboard/news/<news_id>/delete", methods=["POST"])
    def news_delete(news_id: str):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        if delete_news_article(news_id):
            flash(_("Article deleted."), "success")
        else:
            flash(_("Article not found."), "warning")
        return redirect(url_for("news_manage"))

    @app.route("/dashboard/news/manage")
    def news_manage():
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        articles = load_news_articles()
        return render_template("news_manage.html", articles=articles, user=user, categories=load_categories())

    # ---------- Category management API ----------
    @app.route("/dashboard/news/categories/add", methods=["POST"], endpoint="news_categories_add")
    def news_category_add():
        user = require_login(level=2)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Category name is required."))), 400
        cats = load_categories()
        if name in cats:
            return jsonify(error=str(_("Category already exists."))), 409
        cats.append(name)
        save_categories(cats)
        return jsonify(categories=cats)

    @app.route("/dashboard/news/categories/rename", methods=["POST"], endpoint="news_categories_rename")
    def news_category_rename():
        user = require_login(level=2)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.json or {}
        old_name = data.get("old_name", "").strip()
        new_name = data.get("new_name", "").strip()
        if not old_name or not new_name:
            return jsonify(error=str(_("Both old and new names are required."))), 400
        cats = load_categories()
        if old_name not in cats:
            return jsonify(error=str(_("Category not found."))), 404
        if new_name in cats:
            return jsonify(error=str(_("A category with that name already exists."))), 409
        cats[cats.index(old_name)] = new_name
        save_categories(cats)
        # Also update existing articles that use the old category name
        articles = load_news_articles()
        changed = False
        for art in articles:
            if art.get("category") == old_name:
                art["category"] = new_name
                changed = True
        if changed:
            with db_session() as db:
                for art in articles:
                    if art.get("category") == new_name:
                        db_art = db.query(NewsArticleModel).filter_by(id=art.get("id")).first()
                        if db_art:
                            db_art.category = new_name
                db.commit()
        return jsonify(categories=cats)

    @app.route("/dashboard/news/categories/delete", methods=["POST"], endpoint="news_categories_delete")
    def news_category_delete():
        user = require_login(level=2)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Category name is required."))), 400
        cats = load_categories()
        if name not in cats:
            return jsonify(error=str(_("Category not found."))), 404
        cats.remove(name)
        save_categories(cats)
        return jsonify(categories=cats)

    @app.route("/dashboard/news/bulk_action", methods=["POST"], endpoint="news_bulk_action")
    def news_bulk_action():
        user = require_login(level=2)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.get_json(silent=True) or {}
        ids = [str(x) for x in (data.get("ids") or [])]
        op = data.get("op")
        if op not in {"publish", "unpublish", "delete"}:
            return jsonify(error="bad op"), 400
        affected = 0
        with db_session() as db:
            rows = db.query(NewsArticleModel).filter(NewsArticleModel.id.in_(ids)).all()
            for r in rows:
                if op == "publish":
                    r.status = "published"
                    if not r.published_at:
                        r.published_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                elif op == "unpublish":
                    r.status = "pending"
                elif op == "delete":
                    db.delete(r)
                affected += 1
            db.commit()
        return jsonify(ok=True, affected=affected)

    # ---------- Legacy redirects (curator news routes) ----------
    @app.route("/news/publish", endpoint="news_publish_legacy")
    def news_publish_legacy():
        return redirect(url_for("news_publish"), code=301)

    @app.route("/news/<news_id>/edit", endpoint="news_edit_legacy")
    def news_edit_legacy(news_id):
        return redirect(url_for("news_edit", news_id=news_id), code=301)

    @app.route("/news/manage", endpoint="news_manage_legacy")
    def news_manage_legacy():
        return redirect(url_for("news_manage"), code=301)

    # ==================== GUIDE ROUTES ====================

    @app.route("/guides")
    def guides():
        all_guides = load_guides(published_only=True)
        # Group by category, preserving the order from guide_categories.json,
        # then any unknown categories at the end.
        categories_in_order = _load_guide_categories()
        seen = set()
        grouped = []
        for cat in categories_in_order:
            items = [g for g in all_guides if g.get("category") == cat]
            if items:
                grouped.append((cat, items))
                seen.add(cat)
        # Any leftover categories not in the JSON list
        extras = {}
        for g in all_guides:
            cat = g.get("category") or ""
            if cat and cat not in seen:
                extras.setdefault(cat, []).append(g)
        for cat in sorted(extras):
            grouped.append((cat, extras[cat]))
        return render_template("guides.html", grouped=grouped, total=len(all_guides))

    @app.route("/guides/<slug>")
    def guide_article(slug):
        guide = get_guide_by_slug(slug)
        if not guide or not guide.get("published"):
            abort(404)
        # Compute prev/next from the same ordered list the index uses.
        flat = load_guides(published_only=True)
        idx = next((i for i, g in enumerate(flat) if g.get("slug") == slug), -1)
        prev_guide = flat[idx - 1] if idx > 0 else None
        next_guide = flat[idx + 1] if 0 <= idx < len(flat) - 1 else None
        return render_template(
            "guide_article.html",
            guide=guide,
            prev_guide=prev_guide,
            next_guide=next_guide,
            preview_mode=False,
        )

    @app.route("/dashboard/admin/guides/upload-image", methods=["POST"], endpoint="admin_guides_upload_image")
    def admin_guide_upload_image():
        user = require_login(level=3)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        img_file = request.files.get("file")
        if not img_file or not img_file.filename:
            return jsonify({"error": "No file provided"}), 400
        img_file.stream.seek(0, 2)  # seek to end to measure size
        size = img_file.stream.tell()
        img_file.stream.seek(0)
        if size > GUIDE_IMAGE_MAX_BYTES:
            return jsonify({"error": "File too large"}), 400
        ext = img_file.filename.rsplit(".", 1)[-1].lower() if "." in img_file.filename else ""
        if ext not in ALLOWED_IMAGE_EXTENSIONS:
            return jsonify({"error": "Invalid image format"}), 400
        GUIDE_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        unique_name = f"{uuid4().hex[:12]}_{secure_filename(img_file.filename)}"
        img_file.save(GUIDE_IMAGES_DIR / unique_name)
        img_url = url_for("static", filename=f"uploads/guides/{unique_name}")
        return jsonify({"url": img_url})

    @app.route("/dashboard/admin/guides/new", methods=["GET", "POST"], endpoint="admin_guide_new")
    @app.route("/dashboard/admin/guides/<int:guide_id>/edit", methods=["GET", "POST"], endpoint="admin_guide_edit")
    def admin_guide_publish(guide_id: int = None):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))

        editing = guide_id is not None
        guide = get_guide(guide_id) if editing else None
        if editing and not guide:
            flash(_("Guide not found."), "warning")
            return redirect(url_for("admin_guides_manage"))

        form_data = {
            "slug": (guide or {}).get("slug", ""),
            "category": (guide or {}).get("category", ""),
            "sort_order": (guide or {}).get("sort_order", 100),
            "published": bool((guide or {}).get("published", False)),
            "title_en": (guide or {}).get("title_en", ""),
            "title_zh": (guide or {}).get("title_zh", ""),
            "summary_en": (guide or {}).get("summary_en", ""),
            "summary_zh": (guide or {}).get("summary_zh", ""),
            "body_en": (guide or {}).get("body_en", ""),
            "body_zh": (guide or {}).get("body_zh", ""),
        }

        if request.method == "POST":
            form_data = _read_guide_form(request.form)
            # Auto-generate slug if blank
            if not form_data["slug"]:
                form_data["slug"] = _slugify(form_data["title_en"] or form_data["title_zh"])
            else:
                form_data["slug"] = _slugify(form_data["slug"])

            error = None
            if not form_data["title_en"] and not form_data["title_zh"]:
                error = _("Please enter a title in at least one language.")
            elif not form_data["slug"]:
                error = _("Please enter a slug.")
            elif slug_exists(form_data["slug"], exclude_id=guide_id or 0):
                error = _("That slug is already taken. Pick another.")

            if error:
                flash(error, "warning")
            else:
                if editing:
                    update_guide(guide_id, form_data)
                    flash(_("Guide updated."), "success")
                else:
                    save_guide(form_data)
                    flash(_("Guide published."), "success")
                return redirect(url_for("admin_guides_manage"))

        return render_template(
            "guide_publish.html",
            form_data=form_data,
            categories=_load_guide_categories(),
            editing=editing,
            guide_id=guide_id,
            user=user,
        )

    @app.route("/dashboard/admin/guides")
    def admin_guides_manage():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        guides = load_guides(published_only=False)
        return render_template("guide_manage.html", guides=guides, user=user)

    @app.route("/dashboard/admin/guides/<int:guide_id>/delete", methods=["POST"])
    def admin_guide_delete(guide_id: int):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        if delete_guide(guide_id):
            flash(_("Guide deleted."), "success")
        else:
            flash(_("Guide not found."), "warning")
        return redirect(url_for("admin_guides_manage"))

    @app.route("/dashboard/admin/guides/reorder", methods=["POST"], endpoint="admin_guides_reorder")
    def admin_guides_reorder():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.get_json(silent=True) or {}
        items = data.get("items") or []
        with db_session() as db:
            for it in items:
                try:
                    gid = int(it.get("id"))
                except (TypeError, ValueError):
                    continue
                g = db.query(GuideModel).filter_by(id=gid).first()
                if not g:
                    continue
                try:
                    g.sort_order = int(it.get("sort_order"))
                except (TypeError, ValueError):
                    pass
                if "category" in it:
                    g.category = (it.get("category") or "").strip()
                g.updated_at = datetime.utcnow().isoformat()
            db.commit()
        return jsonify(ok=True)

    @app.route("/dashboard/admin/guides/<int:guide_id>/toggle", methods=["POST"], endpoint="admin_guide_toggle_published")
    def admin_guide_toggle_published(guide_id: int):
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.get_json(silent=True) or {}
        with db_session() as db:
            g = db.query(GuideModel).filter_by(id=guide_id).first()
            if not g:
                return jsonify(error="not found"), 404
            if "published" in data:
                g.published = bool(data["published"])
            else:
                g.published = not bool(g.published)
            g.updated_at = datetime.utcnow().isoformat()
            new_state = bool(g.published)
            db.commit()
        return jsonify(ok=True, published=new_state)

    @app.route("/dashboard/admin/guides/preview", methods=["POST"], endpoint="admin_guide_preview")
    def admin_guide_preview():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        data = _read_guide_form(request.form)
        # Sanitize bodies the same way the persisted save path would, so the
        # preview reflects exactly what would end up in the DB.
        data["body_en"] = _sanitize_guide_html(data.get("body_en", ""))
        data["body_zh"] = _sanitize_guide_html(data.get("body_zh", ""))
        guide = {
            "slug": data["slug"] or "preview",
            "category": data["category"],
            "title_en": data["title_en"],
            "title_zh": data["title_zh"],
            "summary_en": data["summary_en"],
            "summary_zh": data["summary_zh"],
            "body_en": data["body_en"],
            "body_zh": data["body_zh"],
            "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
            "published": data["published"],
        }
        return render_template("guide_article.html",
            guide=guide,
            prev_guide=None,
            next_guide=None,
            preview_mode=True,
        )

    @app.route("/admin/guides", endpoint="admin_guides_manage_legacy")
    def admin_guides_manage_legacy():
        return redirect(url_for("admin_guides_manage"), code=301)

    @app.route("/admin/guides/new", endpoint="admin_guide_new_legacy")
    def admin_guide_new_legacy():
        return redirect(url_for("admin_guide_new"), code=301)

    @app.route("/admin/guides/<int:guide_id>/edit", endpoint="admin_guide_edit_legacy")
    def admin_guide_edit_legacy(guide_id):
        return redirect(url_for("admin_guide_edit", guide_id=guide_id), code=301)

    # ---------- Paper categories & journals management ----------
    @app.route("/dashboard/admin/paper-manage")
    def paper_manage():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        return render_template("paper_manage.html", user=user,
                               paper_categories=load_paper_categories(),
                               journals=load_journals(),
                               ee_subjects=load_ee_subjects(), cp_global_contexts=CP_GLOBAL_CONTEXTS, cp_action_types=CP_ACTION_TYPES)

    @app.route("/admin/paper-manage", endpoint="paper_manage_legacy")
    def paper_manage_legacy():
        return redirect(url_for("paper_manage"), code=301)

    @app.route("/dashboard/admin/paper-categories/add", methods=["POST"], endpoint="admin_paper_categories_add")
    def paper_category_add():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Category name is required."))), 400
        cats = load_paper_categories()
        if name in cats:
            return jsonify(error=str(_("Category already exists."))), 409
        cats.append(name)
        save_paper_categories(cats)
        return jsonify(items=cats)

    @app.route("/dashboard/admin/paper-categories/rename", methods=["POST"], endpoint="admin_paper_categories_rename")
    def paper_category_rename():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.json or {}
        old_name = data.get("old_name", "").strip()
        new_name = data.get("new_name", "").strip()
        if not old_name or not new_name:
            return jsonify(error=str(_("Both old and new names are required."))), 400
        cats = load_paper_categories()
        if old_name not in cats:
            return jsonify(error=str(_("Category not found."))), 404
        if new_name in cats:
            return jsonify(error=str(_("A category with that name already exists."))), 409
        cats[cats.index(old_name)] = new_name
        save_paper_categories(cats)
        # Also update existing papers that use the old category name
        meta_rows = load_paper_metadata()
        changed = False
        for row in meta_rows:
            if row.get("category") == old_name:
                row["category"] = new_name
                changed = True
        if changed:
            save_paper_metadata(meta_rows)
        return jsonify(items=cats)

    @app.route("/dashboard/admin/paper-categories/delete", methods=["POST"], endpoint="admin_paper_categories_delete")
    def paper_category_delete():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Category name is required."))), 400
        cats = load_paper_categories()
        if name not in cats:
            return jsonify(error=str(_("Category not found."))), 404
        cats.remove(name)
        save_paper_categories(cats)
        return jsonify(items=cats)

    @app.route("/dashboard/admin/ee-subjects/add", methods=["POST"], endpoint="admin_ee_subjects_add")
    def ee_subject_add():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.json or {}
        group_id = data.get("group_id")
        name = data.get("name", "").strip()
        if not group_id or not name:
            return jsonify(error=str(_("Group ID and subject name are required."))), 400
        subjects_data = load_ee_subjects()
        for group in subjects_data.get("groups", []):
            if group["id"] == group_id:
                if name in group["subjects"]:
                    return jsonify(error=str(_("Subject already exists in this group."))), 409
                group["subjects"].append(name)
                save_ee_subjects(subjects_data)
                return jsonify(groups=subjects_data["groups"])
        return jsonify(error=str(_("Group not found."))), 404

    @app.route("/dashboard/admin/ee-subjects/delete", methods=["POST"], endpoint="admin_ee_subjects_delete")
    def ee_subject_delete():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.json or {}
        group_id = data.get("group_id")
        name = data.get("name", "").strip()
        if not group_id or not name:
            return jsonify(error=str(_("Group ID and subject name are required."))), 400
        subjects_data = load_ee_subjects()
        for group in subjects_data.get("groups", []):
            if group["id"] == group_id:
                if name not in group["subjects"]:
                    return jsonify(error=str(_("Subject not found in this group."))), 404
                group["subjects"].remove(name)
                if name in subjects_data.get("interdisciplinary_subjects", []):
                    subjects_data["interdisciplinary_subjects"].remove(name)
                save_ee_subjects(subjects_data)
                return jsonify(groups=subjects_data["groups"])
        return jsonify(error=str(_("Group not found."))), 404

    @app.route("/dashboard/admin/journals/add", methods=["POST"], endpoint="admin_journals_add")
    def journal_add():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Journal name is required."))), 400
        journals = load_journals()
        existing_names = [j["name"] for j in journals]
        if name in existing_names:
            return jsonify(error=str(_("Journal already exists."))), 409
        from uuid import uuid4
        new_journal = {
            "id": uuid4().hex[:12],
            "name": name,
            "cover_image": "",
            "introduction": "",
            "created_at": datetime.utcnow().date().isoformat(),
        }
        journals.append(new_journal)
        save_journals(journals)
        return jsonify(items=journals)

    @app.route("/dashboard/admin/journals/delete", methods=["POST"], endpoint="admin_journals_delete")
    def journal_delete():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        journal_id = (request.json or {}).get("id", "").strip()
        if not journal_id:
            return jsonify(error=str(_("Journal ID is required."))), 400
        journals = load_journals()
        journal = next((j for j in journals if j["id"] == journal_id), None)
        if not journal:
            return jsonify(error=str(_("Journal not found."))), 404
        # Clear journal field from papers
        old_name = journal["name"]
        meta_rows = load_paper_metadata()
        changed = False
        for row in meta_rows:
            if row.get("journal") == old_name:
                row["journal"] = ""
                changed = True
        if changed:
            save_paper_metadata(meta_rows)
        journals = [j for j in journals if j["id"] != journal_id]
        save_journals(journals)
        return jsonify(items=journals)

    @app.route("/dashboard/admin/journal/<journal_id>/edit", methods=["GET", "POST"], endpoint="admin_journal_edit")
    def journal_edit(journal_id):
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        journal = get_journal_by_id(journal_id)
        if not journal:
            flash(_("Journal not found."), "warning")
            return redirect(url_for("paper_manage"))

        if request.method == "POST":
            old_name = journal["name"]
            new_name = request.form.get("name", "").strip()
            introduction = request.form.get("introduction", "").strip()

            if not new_name:
                flash(_("Journal name is required."), "danger")
                return redirect(url_for("admin_journal_edit", journal_id=journal_id))

            journals = load_journals()
            for j in journals:
                if j["id"] == journal_id:
                    j["name"] = new_name
                    j["introduction"] = introduction

                    # Handle cover image upload
                    cover_file = request.files.get("cover_image")
                    if cover_file and cover_file.filename:
                        ext = cover_file.filename.rsplit(".", 1)[-1].lower()
                        if ext in ALLOWED_IMAGE_EXTENSIONS:
                            JOURNAL_COVERS_DIR.mkdir(parents=True, exist_ok=True)
                            cover_filename = f"journal_{journal_id}.{ext}"
                            cover_file.save(JOURNAL_COVERS_DIR / cover_filename)
                            j["cover_image"] = cover_filename
                    break

            save_journals(journals)

            # Update paper metadata if name changed
            if old_name != new_name:
                meta_rows = load_paper_metadata()
                changed = False
                for row in meta_rows:
                    if row.get("journal") == old_name:
                        row["journal"] = new_name
                        changed = True
                if changed:
                    save_paper_metadata(meta_rows)

            flash(_("Journal updated."), "success")
            return redirect(url_for("admin_journal_edit", journal_id=journal_id))

        # GET: load papers belonging to this journal
        all_papers = gather_paper_records()
        journal_papers = [p for p in all_papers if p.get("journal") == journal["name"]]
        journal_papers.sort(key=lambda r: r.get("published_at") or "", reverse=True)

        return render_template("journal_edit.html", user=user, journal=journal, papers=journal_papers)

    @app.route("/admin/journal/<journal_id>/edit", endpoint="admin_journal_edit_legacy")
    def admin_journal_edit_legacy(journal_id):
        return redirect(url_for("admin_journal_edit", journal_id=journal_id), code=301)

    # ---------- Public journal pages ----------
    @app.route("/journals")
    def journal_list_page():
        journals = load_journals()
        return render_template("journal_list.html", journals=journals)

    @app.route("/journal/<journal_id>")
    def journal_detail(journal_id):
        journal = get_journal_by_id(journal_id)
        if not journal:
            flash(_("Journal not found."), "warning")
            return redirect(url_for("journal_list_page"))
        # Get papers in this journal
        all_papers = gather_paper_records()
        journal_papers = [p for p in all_papers if p.get("journal") == journal["name"]]
        journal_papers.sort(key=lambda r: r.get("published_at") or "", reverse=True)

        user = get_active_user()
        is_guest = user is None
        return render_template("journal_detail.html", journal=journal, papers=journal_papers, user=user, is_guest=is_guest)

    @app.route("/news/<news_id>")
    def news_detail(news_id: str):
        article = get_news_article(news_id)
        if not article:
            flash(_("Article not found."), "warning")
            return redirect(url_for("news_list"))
        if article.get("status") == "pending":
            viewer = get_active_user()
            try:
                viewer_role = int(viewer.get("role", "1")) if viewer else 0
            except (TypeError, ValueError):
                viewer_role = 0
            if viewer_role < 2:
                flash(_("Article not found."), "warning")
                return redirect(url_for("news_list"))
        all_articles = load_news_articles(status="published")
        related = [a for a in all_articles if a.get("id") != news_id][:3]
        return render_template("news_article.html", article=article, related=related)

    # ---- Submission review helpers ----

    def _load_submissions():
        with db_session() as db:
            subs = db.query(SubmissionModel).all()
            return [{
                "id": s.id,
                "pdf_filename": s.pdf_filename,
                "pending_filename": s.pending_filename,
                "title": s.title,
                "author_name": s.author_name,
                "author_email": s.author_email,
                "author_school": s.author_school,
                "status": s.status,
                "submitted_at": s.submitted_at,
                "feedback": s.feedback,
                "abstract": s.abstract,
                "keywords": s.keywords,
                "journal": s.journal,
                "category": s.category,
                "language": s.language,
                "submitter": s.submitted_by,
                "original_filename": s.original_filename,
                "ib_ee_data": s.ib_ee_data,
                "is_ib_sample": s.is_ib_sample,
                "cp_data": s.cp_data,
            } for s in subs]

    def _write_submissions(subs):
        with db_session() as db:
            db.query(SubmissionModel).delete()
            for s in subs:
                db.add(SubmissionModel(
                    id=s.get("id"),
                    pdf_filename=s.get("pdf_filename"),
                    pending_filename=s.get("pending_filename"),
                    title=s.get("title"),
                    author_name=s.get("author_name"),
                    author_email=s.get("author_email"),
                    author_school=s.get("author_school"),
                    status=s.get("status"),
                    submitted_at=s.get("submitted_at"),
                    feedback=s.get("feedback"),
                    abstract=s.get("abstract"),
                    keywords=s.get("keywords"),
                    journal=s.get("journal"),
                    category=s.get("category"),
                    language=s.get("language"),
                    submitted_by=s.get("submitter"),
                    original_filename=s.get("original_filename"),
                    ib_ee_data=s.get("ib_ee_data"),
                    is_ib_sample=s.get("is_ib_sample"),
                    cp_data=s.get("cp_data"),
                ))
            db.commit()

    def _save_submission(sub):
        subs = _load_submissions()
        subs.append(sub)
        _write_submissions(subs)

    def _get_submission(sub_id):
        for s in _load_submissions():
            if s.get("id") == sub_id:
                return s
        return None

    def _update_submission(sub_id, updates):
        subs = _load_submissions()
        for s in subs:
            if s.get("id") == sub_id:
                s.update(updates)
                _write_submissions(subs)
                return s
        return None

    # ---- Submission review routes ----

    @app.route("/dashboard/my-submissions")
    def my_submissions():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        username = user.get("username", "")
        subs = [s for s in _load_submissions() if s.get("submitter") == username]
        subs.sort(key=lambda s: s.get("submitted_at", ""), reverse=True)
        return render_template("my_submissions.html", user=user, submissions=subs)

    @app.route("/dashboard/my-submissions/<sub_id>/delete", methods=["POST"], endpoint="my_submission_delete")
    def delete_submission(sub_id):
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        username = user.get("username", "")
        sub = _get_submission(sub_id)
        if not sub or sub.get("submitter") != username:
            flash(_("Submission not found."), "warning")
            return redirect(url_for("my_submissions"))
        # Remove pending PDF file if it exists
        pending_file = sub.get("pending_filename", "")
        if pending_file:
            pending_path = PENDING_PAPERS_DIR / pending_file
            if pending_path.exists():
                pending_path.unlink()
        # Remove submission record
        subs = _load_submissions()
        subs = [s for s in subs if s.get("id") != sub_id]
        _write_submissions(subs)
        flash(_("Submission deleted."), "success")
        return redirect(url_for("my_submissions"))

    @app.route("/dashboard/my-submissions/<sub_id>", endpoint="my_submission_view")
    def submission_detail(sub_id):
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("submitter") != user.get("username", ""):
            flash(_("Submission not found."), "warning")
            return redirect(url_for("my_submissions"))

        # Determine PDF URL based on status
        pdf_url = None
        if sub.get("status") == "pending":
            pending_path = PENDING_PAPERS_DIR / sub.get("pending_filename", "")
            if pending_path.exists():
                pdf_url = url_for("my_submission_file", sub_id=sub_id)
        elif sub.get("status") == "accepted":
            filename = sub.get("filename", "")
            publish_path = PAPERS_DIR / filename
            if not publish_path.exists():
                # Try with sub_id prefix (collision avoidance)
                filename = f"{sub_id}_{sub.get('filename', '')}"
                publish_path = PAPERS_DIR / filename
            if publish_path.exists():
                pdf_url = url_for("paper_file", filename=filename)
        # rejected: file deleted, pdf_url stays None

        return render_template("submission_detail.html", user=user, submission=sub, pdf_url=pdf_url)

    @app.route("/dashboard/my-submissions/<sub_id>/file")
    def my_submission_file(sub_id):
        """Serve a pending paper file to the submitter only."""
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("submitter") != user.get("username", ""):
            abort(403)
        pending_filename = sub.get("pending_filename", "")
        return send_from_directory(str(PENDING_PAPERS_DIR), pending_filename)

    @app.route("/my-submissions", endpoint="my_submissions_legacy")
    def my_submissions_legacy():
        return redirect(url_for("my_submissions"), code=301)

    @app.route("/my-submissions/<sub_id>", endpoint="my_submission_view_legacy")
    def my_submission_view_legacy(sub_id):
        return redirect(url_for("my_submission_view", sub_id=sub_id), code=301)

    @app.route("/my-submissions/<sub_id>/file", endpoint="my_submission_file_legacy")
    def my_submission_file_legacy(sub_id):
        return redirect(url_for("my_submission_file", sub_id=sub_id), code=301)

    @app.route("/dashboard/review")
    def review_list():
        user = require_login(level=2)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        status_filter = request.args.get("status", "pending")
        subs = _load_submissions()
        if status_filter == "pending":
            subs = [s for s in subs if s.get("status") == "pending"]
        elif status_filter == "accepted":
            subs = [s for s in subs if s.get("status") == "accepted"]
        elif status_filter == "rejected":
            subs = [s for s in subs if s.get("status") == "rejected"]
        subs.sort(key=lambda s: s.get("submitted_at", ""), reverse=True)
        return render_template("review_list.html", user=user, submissions=subs, status_filter=status_filter)

    @app.route("/dashboard/review/<sub_id>", endpoint="review_paper")
    def review_detail(sub_id):
        user = require_login(level=2)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        sub = _get_submission(sub_id)
        if not sub:
            flash(_("Submission not found."), "warning")
            return redirect(url_for("review_list"))
        pdf_url = url_for("pending_paper_file", filename=sub.get("pending_filename", ""))
        return render_template("review_paper.html", user=user, submission=sub, pdf_url=pdf_url)

    @app.route("/dashboard/review/<sub_id>/accept", methods=["POST"])
    def review_accept(sub_id):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("status") != "pending":
            flash(_("Submission not found or already reviewed."), "warning")
            return redirect(url_for("review_list"))

        # Move file from pending to published
        pending_path = PENDING_PAPERS_DIR / sub.get("pending_filename", "")
        filename = sub.get("pdf_filename") or sub.get("filename")
        if not filename:
            filename = _build_safe_paper_filename(
                sub.get("title", "paper"), sub.get("author_name", "author")
            )
        publish_path = PAPERS_DIR / filename
        if publish_path.exists():
            # Add sub_id prefix to avoid collision
            filename = f"{sub_id}_{filename}"
            publish_path = PAPERS_DIR / filename

        if pending_path.exists():
            shutil.move(str(pending_path), str(publish_path))

        # Save paper metadata
        today = datetime.utcnow().date().isoformat()
        upsert_paper_metadata(
            filename,
            {
                "title": sub.get("title", ""),
                "journal": sub.get("journal", ""),
                "category": sub.get("category", ""),
                "language": sub.get("language", ""),
                "keywords": sub.get("keywords", ""),
                "abstract": sub.get("abstract", ""),
                "author_name": sub.get("author_name", ""),
                "author_email": sub.get("author_email", ""),
                "author_school": sub.get("author_school", ""),
                "published_at": today,
                "ib_ee_data": sub.get("ib_ee_data", ""),
                "is_ib_sample": sub.get("is_ib_sample", ""),
                "cp_data": sub.get("cp_data", ""),
            },
        )

        reviewer_name = user.get("display_name", "") or user.get("first_name", "") or user.get("username", "")
        _update_submission(sub_id, {
            "status": "accepted",
            "reviewed_at": datetime.utcnow().isoformat(),
            "reviewer": reviewer_name,
        })
        flash(_("Paper accepted and published."), "success")
        try:
            if llm_client.llm_enabled():
                rag_index.build_index([filename])
        except Exception:
            app.logger.exception("Failed to index accepted paper")
        return redirect(url_for("review_list"))

    @app.route("/dashboard/review/<sub_id>/reject", methods=["POST"])
    def review_reject(sub_id):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        sub = _get_submission(sub_id)
        if not sub or sub.get("status") != "pending":
            flash(_("Submission not found or already reviewed."), "warning")
            return redirect(url_for("review_list"))

        comment = request.form.get("comment", "").strip()

        # Remove the pending file
        pending_path = PENDING_PAPERS_DIR / sub.get("pending_filename", "")
        if pending_path.exists():
            pending_path.unlink()

        reviewer_name = user.get("display_name", "") or user.get("first_name", "") or user.get("username", "")
        _update_submission(sub_id, {
            "status": "rejected",
            "reviewed_at": datetime.utcnow().isoformat(),
            "reviewer": reviewer_name,
            "comment": comment,
        })
        flash(_("Paper rejected."), "info")
        return redirect(url_for("review_list"))

    @app.route("/review", endpoint="review_list_legacy")
    def review_list_legacy():
        return redirect(url_for("review_list"), code=301)

    @app.route("/review/<sub_id>", endpoint="review_paper_legacy")
    def review_paper_legacy(sub_id):
        return redirect(url_for("review_paper", sub_id=sub_id), code=301)

    @app.route("/pending-papers/<path:filename>")
    def pending_paper_file(filename):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        return send_from_directory(str(PENDING_PAPERS_DIR), filename)

    return app


def load_users() -> List[Dict[str, str]]:
    with db_session() as db:
        users = db.query(LocalUser).order_by(LocalUser.username.asc()).all()
        return [
            {
                "username": user.username,
                "password": user.password,
                "registration_date": user.registration_date.isoformat() if user.registration_date else "",
                "expiry_date": user.expiry_date.isoformat() if user.expiry_date else "",
                "role": user.role,
                "email": user.email or "",
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "school": user.school or "",
            }
            for user in users
        ]


def get_local_user(username: str) -> Optional[Dict[str, str]]:
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return None
        return {
            "username": user.username,
            "password": user.password,
            "registration_date": user.registration_date.isoformat() if user.registration_date else "",
            "expiry_date": user.expiry_date.isoformat() if user.expiry_date else "",
            "role": user.role,
            "email": user.email or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
        }


def get_local_user_by_email(email: str) -> Optional[Dict[str, str]]:
    """Look up a local user by email address."""
    if not email:
        return None
    with db_session() as db:
        user = db.query(LocalUser).filter(LocalUser.email == email).first()
        if not user:
            return None
        return {
            "username": user.username,
            "password": user.password,
            "registration_date": user.registration_date.isoformat() if user.registration_date else "",
            "expiry_date": user.expiry_date.isoformat() if user.expiry_date else "",
            "role": user.role,
            "email": user.email or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
        }


def hash_password(password: str) -> str:
    iterations = int(os.environ.get("PAPERQUERY_PBKDF_ITERATIONS", "260000"))
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    salt_b64 = base64.b64encode(salt).decode("ascii")
    digest_b64 = base64.b64encode(digest).decode("ascii")
    return f"{PASSWORD_SCHEME}${iterations}${salt_b64}${digest_b64}"


def create_local_user(
    username: str,
    password: str,
    role: str = "1",
    email: str = "",
    first_name: str = "",
    last_name: str = "",
    school: str = "",
) -> Dict[str, str]:
    with db_session() as db:
        if db.get(LocalUser, username):
            raise ValueError("Username already exists.")
        record = LocalUser(
            username=username,
            password=hash_password(password),
            registration_date=datetime.utcnow().date(),
            expiry_date=None,
            role=role,
            email=email,
            first_name=first_name,
            last_name=last_name,
            school=school,
        )
        db.add(record)
        return {
            "username": record.username,
            "password": record.password,
            "registration_date": record.registration_date.isoformat() if record.registration_date else "",
            "expiry_date": "",
            "role": record.role,
            "email": record.email or "",
            "first_name": record.first_name or "",
            "last_name": record.last_name or "",
            "school": record.school or "",
        }


def update_local_user_role(username: str, role: str) -> bool:
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return False
        user.role = role
        return True


def update_local_user_password(username: str, password: str) -> bool:
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return False
        user.password = hash_password(password)
        return True


def delete_local_user(username: str) -> bool:
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return False
        db.delete(user)
        return True


def authenticate(username: str, password: str) -> Optional[Dict[str, str]]:
    today = datetime.utcnow().date()
    for user in load_users():
        if user.get("username") != username:
            continue
        encoded = user.get("password", "")
        if not encoded:
            continue
        if not verify_password(password, encoded):
            continue
        expiry_str = user.get("expiry_date")
        if expiry_str:
            try:
                expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
            except ValueError:
                expiry_date = None
            if expiry_date and expiry_date < today:
                return None
        return {
            "username": user.get("username", ""),
            "role": user.get("role", "1"),
            "registered_at": user.get("registration_date", ""),
            "expiry_date": expiry_str or "",
        }
    return None


def load_active_local_user(username: str) -> Optional[Dict[str, str]]:
    record = get_local_user(username)
    if not record:
        return None
    expiry_str = record.get("expiry_date", "")
    if expiry_str:
        try:
            expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        except ValueError:
            expiry_date = None
        if expiry_date and expiry_date < datetime.utcnow().date():
            return None
    return {
        "username": record.get("username", ""),
        "role": record.get("role", "1"),
        "registered_at": record.get("registration_date", ""),
        "expiry_date": expiry_str or "",
    }


def start_local_session(
    user: Dict[str, str],
    *,
    ms_id: str = "",
    display_name: str = "",
    email: str = "",
) -> None:
    preferred_lang = session.get("language")
    session.clear()
    if preferred_lang:
        session["language"] = preferred_lang
    token = register_active_session(user["username"])
    session["user"] = {
        "username": user.get("username", ""),
        "role": user.get("role", "1"),
        "registered_at": user.get("registered_at", ""),
        "expiry_date": user.get("expiry_date", ""),
        "ms_id": ms_id,
        "display_name": display_name,
        "email": email,
        "is_local": True,
    }
    session["session_token"] = token


def start_ms_session(ms_user: Dict[str, str], *, linked_username: str = "") -> None:
    preferred_lang = session.get("language")
    session.clear()
    if preferred_lang:
        session["language"] = preferred_lang
    token = register_active_session(ms_user.get("ms_id", ""))
    session_user = build_session_user(ms_user)
    session_user["is_local"] = False
    session_user["linked_username"] = linked_username
    session["user"] = session_user
    session["session_token"] = token


def is_ms_configured() -> bool:
    return bool(MS_CLIENT_ID and MS_CLIENT_SECRET)


def build_msal_app() -> msal.ConfidentialClientApplication:
    return msal.ConfidentialClientApplication(
        MS_CLIENT_ID,
        authority=MS_AUTHORITY,
        client_credential=MS_CLIENT_SECRET,
    )


def fetch_ms_profile(token_result: Dict[str, str]) -> Dict[str, str]:
    claims = token_result.get("id_token_claims") or {}
    profile: Dict[str, str] = {
        "ms_id": claims.get("oid") or claims.get("sub") or "",
        "tenant_id": claims.get("tid") or "",
        "email": claims.get("preferred_username") or claims.get("email") or "",
        "display_name": claims.get("name") or "",
        "role": "1",
    }

    access_token = token_result.get("access_token")
    if access_token:
        try:
            response = requests.get(
                MS_GRAPH_ME_URL,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10,
            )
            if response.ok:
                payload = response.json()
                profile["ms_id"] = profile["ms_id"] or payload.get("id", "")
                profile["display_name"] = payload.get("displayName") or profile["display_name"]
                profile["email"] = payload.get("mail") or payload.get("userPrincipalName") or profile["email"]
        except requests.RequestException:
            pass

    return profile


def build_session_user(record: Dict[str, str]) -> Dict[str, str]:
    email = record.get("email", "")
    username = record.get("ms_id", "")
    # Prefer user-entered first/last name over MS-provided display_name
    first = record.get("first_name", "").strip()
    last = record.get("last_name", "").strip()
    if first or last:
        display_name = f"{first} {last}".strip()
    else:
        display_name = (record.get("display_name", "") or "").strip()
    return {
        "username": username,
        "ms_id": record.get("ms_id", ""),
        "email": email,
        "display_name": display_name,
        "first_name": record.get("first_name", ""),
        "last_name": record.get("last_name", ""),
        "role": record.get("role", "1") or "1",
    }


def load_ms_users() -> List[Dict[str, str]]:
    with db_session() as db:
        users = db.query(MsUser).order_by(MsUser.ms_id.asc()).all()
        return [
            {
                "ms_id": user.ms_id,
                "tenant_id": user.tenant_id or "",
                "email": user.email or "",
                "display_name": user.display_name or "",
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "school": user.school or "",
                "grade": user.grade or "",
                "role": user.role or "1",
                "created_at": user.created_at.isoformat() if user.created_at else "",
                "updated_at": user.updated_at.isoformat() if user.updated_at else "",
            }
            for user in users
        ]



def get_ms_user(ms_id: str) -> Optional[Dict[str, str]]:
    if not ms_id:
        return None
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return None
        return {
            "ms_id": user.ms_id,
            "tenant_id": user.tenant_id or "",
            "email": user.email or "",
            "display_name": user.display_name or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
            "grade": user.grade or "",
            "role": user.role or "1",
            "password": user.password or "",
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def get_ms_user_by_email(email: str) -> Optional[Dict[str, str]]:
    """Look up a Microsoft user by email address."""
    if not email:
        return None
    with db_session() as db:
        user = db.query(MsUser).filter(MsUser.email == email).first()
        if not user:
            return None
        return {
            "ms_id": user.ms_id,
            "tenant_id": user.tenant_id or "",
            "email": user.email or "",
            "display_name": user.display_name or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
            "grade": user.grade or "",
            "role": user.role or "1",
            "password": user.password or "",
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def update_ms_user_password(ms_id: str, password: str) -> bool:
    """Set or update the password for an MS user."""
    hashed = hash_password(password)
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return False
        user.password = hashed
        user.updated_at = datetime.utcnow()
        return True


def upsert_ms_user(profile: Dict[str, str]) -> Dict[str, str]:
    ms_id = profile.get("ms_id", "")
    now = datetime.utcnow()
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            user = MsUser(ms_id=ms_id, created_at=now)
            db.add(user)
        user.tenant_id = profile.get("tenant_id", "") or user.tenant_id
        user.email = profile.get("email", "") or user.email
        user.display_name = profile.get("display_name", "") or user.display_name
        user.role = user.role or "1"
        user.updated_at = now
        return {
            "ms_id": user.ms_id,
            "tenant_id": user.tenant_id or "",
            "email": user.email or "",
            "display_name": user.display_name or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
            "grade": user.grade or "",
            "role": user.role or "1",
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def update_ms_user(ms_id: str, updates: Dict[str, str]) -> Optional[Dict[str, str]]:
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return None
        for key, value in updates.items():
            if key in MS_USER_FIELDS:
                setattr(user, key, value)
        if user.first_name or user.last_name:
            user.display_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
        user.updated_at = datetime.utcnow()
        return {
            "ms_id": user.ms_id,
            "tenant_id": user.tenant_id or "",
            "email": user.email or "",
            "display_name": user.display_name or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
            "grade": user.grade or "",
            "role": user.role or "1",
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def update_ms_user_role(ms_id: str, role: str) -> bool:
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return False
        user.role = role
        user.updated_at = datetime.utcnow()
        return True


def delete_ms_user(ms_id: str) -> bool:
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return False
        db.delete(user)
        return True


def is_profile_complete(record: Dict[str, str]) -> bool:
    return bool(
        record.get("first_name")
        and record.get("last_name")
    )


def get_active_user() -> Optional[Dict[str, str]]:
    user = session.get("user")
    if not user:
        return None
    username = user.get("username", "")
    token = session.get("session_token")
    if not username or not token:
        session.clear()
        return None
    if not refresh_session(username, token):
        session.clear()
        return None
    return user


def require_login(level: int = 1) -> Optional[Dict[str, str]]:
    user = session.get("user")
    
    def _fail_login(msg):
        if request.method == "GET":
            session["next"] = request.url
        flash(msg, "warning")
        return None

    if not user:
        return _fail_login(_("Please sign in first."))
    username = user.get("username", "")
    token = session.get("session_token")
    if not username or not token:
        session.clear()
        return _fail_login(_("Session expired. Please sign in again."))
    if not refresh_session(username, token):
        session.clear()
        return _fail_login(_("Session timed out. Please sign in again."))
    try:
        role = int(user.get("role", "1"))
    except ValueError:
        role = 1
    if role < level:
        flash(_("You do not have access to that action."), "danger")
        return None
    return user




def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, iterations_raw, salt_b64, hash_b64 = encoded.split("$", 3)
    except ValueError:
        return False
    if scheme != PASSWORD_SCHEME:
        return False
    try:
        iterations = int(iterations_raw)
    except ValueError:
        return False
    try:
        salt = base64.b64decode(salt_b64)
        stored_hash = base64.b64decode(hash_b64)
    except (ValueError, binascii.Error, TypeError):
        return False

    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
        dklen=len(stored_hash),
    )
    return hmac.compare_digest(dk, stored_hash)


def search_papers(keyword: str) -> List[Dict[str, str]]:
    metadata_index = {row["filename"]: row for row in load_paper_metadata()}
    matches: List[Dict[str, str]] = []
    normalized = keyword.lower()

    for pdf_path in PAPERS_DIR.glob("*.pdf"):
        record = build_paper_record(pdf_path.name, metadata_index)
        
        # Check metadata first: title, author, keys
        title_str = (record.get("title") or "").lower()
        author_str = (record.get("author_name") or "").lower()
        keywords_str = (record.get("keywords") or "").lower()

        # Also search EE subjects and CP global context
        ee_subjects_str = ""
        raw_ib = record.get("ib_ee_data", "")
        if raw_ib:
            try:
                ib = json.loads(raw_ib)
                ee_subjects_str = (ib.get("core_subject", "") + " " + ib.get("interdisciplinary_subject", "")).lower()
            except (json.JSONDecodeError, TypeError):
                pass
        cp_context_str = ""
        raw_cp = record.get("cp_data", "")
        if raw_cp:
            try:
                cp = json.loads(raw_cp)
                cp_context_str = (cp.get("global_context", "") + " " + " ".join(cp.get("action_types", []))).lower()
            except (json.JSONDecodeError, TypeError):
                pass

        if normalized in title_str or normalized in author_str or normalized in keywords_str or normalized in ee_subjects_str or normalized in cp_context_str:
            matches.append(record)
            continue

        # Fall back to full-text PDF search
        try:
            text = extract_pdf_text(pdf_path)
            if normalized in text.lower():
                matches.append(record)
        except Exception as exc:  # pragma: no cover - logging placeholder
            print(f"Failed to read {pdf_path.name}: {exc}")
            continue

    matches.sort(key=lambda row: row.get("published_at") or "", reverse=True)
    return matches[:MAX_SEARCH_RESULTS]


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        from PyPDF2 import PdfReader
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("PyPDF2 is required for PDF search.") from exc

    reader = PdfReader(str(pdf_path))
    text_parts: List[str] = []
    for page in reader.pages:
        text_parts.append(page.extract_text() or "")
    return "\n".join(text_parts)


def extract_text_from_upload(filename: str, raw: bytes) -> str:
    """Extract plain text from an uploaded attachment by extension.

    Supports PDF (PyPDF2), DOCX (python-docx), and TXT/MD (utf-8). Raises
    ValueError for anything else.
    """
    name = (filename or "").lower()
    if name.endswith(".pdf"):
        from PyPDF2 import PdfReader
        reader = PdfReader(BytesIO(raw))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    if name.endswith(".docx"):
        from docx import Document
        doc = Document(BytesIO(raw))
        return "\n".join(p.text for p in doc.paragraphs)
    if name.endswith((".txt", ".md")):
        return raw.decode("utf-8", "ignore")
    raise ValueError("unsupported file type")


def set_pdf_metadata(pdf_path: Path, title: str, author: str) -> None:
    try:
        from PyPDF2 import PdfReader, PdfWriter
    except ImportError as exc:
        print(f"PyPDF2 not installed, unable to set metadata: {exc}")
        return

    try:
        reader = PdfReader(str(pdf_path))
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)

        metadata = reader.metadata or {}
        new_metadata = dict(metadata)
        if title:
            new_metadata["/Title"] = title
        if author:
            new_metadata["/Author"] = author

        writer.add_metadata(new_metadata)

        with open(pdf_path, "wb") as f:
            writer.write(f)
    except Exception as exc:
        print(f"Failed to update PDF metadata for {pdf_path}: {exc}")


def build_preview_pdf(pdf_path: Path, *, max_pages: int = 2) -> BytesIO:
    try:
        from PyPDF2 import PdfReader, PdfWriter
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("PyPDF2 is required for PDF previews.") from exc

    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()
    for page in reader.pages[:max_pages]:
        writer.add_page(page)
    buffer = BytesIO()
    writer.write(buffer)
    buffer.seek(0)
    return buffer


def load_paper_metadata() -> List[Dict[str, str]]:
    with db_session() as db:
        papers = db.query(PaperMetadataModel).all()
        return [{field: (getattr(p, field) or "") for field in METADATA_FIELDS} for p in papers]


def save_paper_metadata(rows: List[Dict[str, str]]) -> None:
    with db_session() as db:
        db.query(PaperMetadataModel).delete()
        for r in rows:
            db.add(PaperMetadataModel(**{field: r.get(field, "") for field in METADATA_FIELDS}))
        db.commit()


def build_paper_record(filename: str, metadata_index: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, str]:
    if metadata_index is None:
        metadata_index = {row["filename"]: row for row in load_paper_metadata()}
    record = {field: "" for field in METADATA_FIELDS}
    record["filename"] = filename
    data = metadata_index.get(filename)
    if data:
        for field in METADATA_FIELDS:
            if field in data and data[field] is not None:
                record[field] = data[field]
    if not record["title"]:
        record["title"] = Path(filename).stem
    return record


def gather_paper_records() -> List[Dict[str, str]]:
    metadata_rows = load_paper_metadata()
    metadata_index = {row["filename"]: row for row in metadata_rows}
    records: List[Dict[str, str]] = []
    for pdf_path in sorted(PAPERS_DIR.glob("*.pdf"), key=lambda item: item.name.lower()):
        records.append(build_paper_record(pdf_path.name, metadata_index))
    records.sort(key=lambda row: (row.get("published_at") or "", row.get("title") or row["filename"]), reverse=True)
    return records


def _rag_iter_papers():
    index = {row["filename"]: row for row in load_paper_metadata()}
    return [build_paper_record(p.name, index) for p in PAPERS_DIR.glob("*.pdf")]


def _rag_paper_text(filename):
    return extract_pdf_text(PAPERS_DIR / filename)


def _rag_paper_meta(filename):
    return build_paper_record(filename)


def _rag_store_replace(filename, rows):
    with db_session() as db:
        db.query(PaperChunkModel).filter(PaperChunkModel.filename == filename).delete()
        for r in rows:
            db.add(PaperChunkModel(
                filename=r["filename"],
                chunk_index=r["chunk_index"],
                content=r["content"],
                embedding=json.dumps(r["embedding"]),
                lang=r.get("lang", ""),
            ))


def _rag_store_all():
    with db_session() as db:
        out = []
        for row in db.query(PaperChunkModel).all():
            try:
                vec = json.loads(row.embedding) if row.embedding else []
            except (ValueError, TypeError):
                vec = []
            out.append({"filename": row.filename, "chunk_index": row.chunk_index,
                        "content": row.content, "embedding": vec})
        return out


def _rag_store_delete(filename):
    with db_session() as db:
        db.query(PaperChunkModel).filter(PaperChunkModel.filename == filename).delete()


def configure_rag():
    rag_index.configure(
        build_embed_client=llm_client.build_embed_client,
        embed_model=llm_client.embed_model,
        iter_papers=_rag_iter_papers,
        paper_text=_rag_paper_text,
        paper_meta=_rag_paper_meta,
        store_replace=_rag_store_replace,
        store_all=_rag_store_all,
        store_delete=_rag_store_delete,
    )


MAX_QUESTION_CHARS = 2000
MAX_ATTACH_BYTES = 5 * 1024 * 1024   # 5 MB cap on ad-hoc attachments
_ASK_HITS: dict = {}   # ip -> list[timestamp]; best-effort per worker
ASK_RATE_LIMIT = 20    # requests
ASK_RATE_WINDOW = 60   # seconds


def _ask_rate_ok(ip: str) -> bool:
    import time
    now = time.time()
    hits = [t for t in _ASK_HITS.get(ip, []) if now - t < ASK_RATE_WINDOW]
    if len(hits) >= ASK_RATE_LIMIT:
        _ASK_HITS[ip] = hits
        return False
    hits.append(now)
    _ASK_HITS[ip] = hits
    return True


def _build_ask_prompt(question, hits, locale_code, web_results=None):
    lang = "Chinese" if locale_code == "zh" else "English"
    web_results = web_results or []
    blocks = [
        f"[{i + 1}] {h['title']} — {h.get('author_name', '')}\n{h['content']}"
        for i, h in enumerate(hits)
    ]
    offset = len(hits)
    for j, w in enumerate(web_results):
        blocks.append(f"[{offset + j + 1}] (web) {w['title']}\n{w.get('content', '')}")
    if blocks:
        sources = "\n\n".join(blocks)
        system = (
            "You are Keydion's library assistant. Answer the question using ONLY the "
            "numbered sources below. Cite claims with bracketed numbers like [1]. "
            "Sources marked (web) come from a live web search; all others are library "
            f"papers. Answer in {lang}. If the sources do not contain the answer, say "
            "you could not find it.\n\nSOURCES:\n" + sources
        )
    else:
        system = (
            "You are Keydion's library assistant. You found no relevant papers in the "
            f"library for this question. Answer in {lang}, briefly explain that nothing "
            "relevant was found, and invite the user to rephrase. Do not invent sources."
        )
    return system


def _ask_llm_messages(question, history_rows):
    messages = []
    for row in history_rows or []:
        if isinstance(row, dict):
            raw_role = row.get("role")
            raw_content = row.get("content")
        else:
            raw_role = row.role
            raw_content = row.content
        role = raw_role if raw_role in ("user", "assistant") else ""
        content = (raw_content or "").strip()
        if role and content:
            messages.append({"role": role, "content": content})
    if not messages or messages[-1] != {"role": "user", "content": question}:
        messages.append({"role": "user", "content": question})
    return messages


def _forced_grounding(question, filenames):
    """Ground on user-selected papers: score their stored chunks against the question."""
    chunks = []
    with db_session() as db:
        rows = (db.query(PaperChunkModel)
                  .filter(PaperChunkModel.filename.in_(filenames)).all())
        for r in rows:
            try:
                vec = json.loads(r.embedding) if r.embedding else []
            except (ValueError, TypeError):
                vec = []
            chunks.append((r.filename, r.chunk_index, r.content, vec))
    if not chunks:
        return []
    qvec = rag_index.embed_texts([question])[0]
    scored = []
    for filename, idx, content, vec in chunks:
        scored.append((rag_index.cosine(qvec, vec), filename, content))
    scored.sort(key=lambda t: t[0], reverse=True)
    min_sim = 0.20
    qualifying = [t for t in scored if t[0] >= min_sim]
    # If no chunk meets the threshold, fall back to the single best chunk so that
    # explicitly selected papers always contribute at least one grounding snippet.
    candidates = qualifying[:6] if qualifying else scored[:1]
    hits = []
    for score, filename, content in candidates:
        meta = build_paper_record(filename)
        hits.append({"filename": filename, "content": content, "score": score,
                     "title": meta.get("title", filename),
                     "author_name": meta.get("author_name", "")})
    return hits


def _attachment_grounding(question, conv_db_id):
    """Ground on documents attached to this conversation (transient scope)."""
    if conv_db_id is None:
        return []
    rows_data = []
    with db_session() as db:
        rows = (db.query(AttachmentChunkModel)
                  .filter(AttachmentChunkModel.conversation_id == conv_db_id).all())
        for r in rows:
            try:
                vec = json.loads(r.embedding) if r.embedding else []
            except (ValueError, TypeError):
                vec = []
            rows_data.append((r.filename, r.chunk_index, r.content, vec))
    if not rows_data:
        return []
    qvec = rag_index.embed_texts([question])[0]
    scored = []
    for filename, idx, content, vec in rows_data:
        scored.append((rag_index.cosine(qvec, vec), filename, content))
    scored.sort(key=lambda t: t[0], reverse=True)
    hits = []
    for score, filename, content in scored[:4]:
        hits.append({"filename": filename, "content": content, "score": score,
                     "title": filename, "author_name": str(_("Attached document")),
                     "is_attachment": True})
    return hits


def _ask_owner_key() -> str:
    """Stable per-browser key for owning conversations without a login."""
    session.permanent = True
    key = session.get("ask_owner")
    if not key:
        import uuid
        key = uuid.uuid4().hex
        session["ask_owner"] = key
    return key


def upsert_paper_metadata(filename: str, data: Dict[str, str]) -> None:
    rows = load_paper_metadata()
    updated = False
    for row in rows:
        if row.get("filename") == filename:
            for field in METADATA_FIELDS:
                if field == "filename":
                    continue
                row[field] = data.get(field, row.get(field, ""))
            updated = True
            break
    if not updated:
        new_row = {field: "" for field in METADATA_FIELDS}
        new_row["filename"] = filename
        for field in METADATA_FIELDS:
            if field != "filename":
                new_row[field] = data.get(field, "")
        rows.append(new_row)
    save_paper_metadata(rows)


def remove_paper_metadata(filename: str) -> None:
    rows = load_paper_metadata()
    filtered = [row for row in rows if row.get("filename") != filename]
    if len(filtered) != len(rows):
        save_paper_metadata(filtered)


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


def load_sessions() -> Dict[str, Dict[str, str]]:
    with db_session() as db:
        rows = db.query(SessionModel).all()
        return {r.username: {"token": r.token or "", "last_seen": r.last_seen or ""} for r in rows}


def is_session_expired(entry: Dict[str, str]) -> bool:
    last_seen = entry.get("last_seen")
    if not last_seen:
        return True
    try:
        timestamp = datetime.fromisoformat(last_seen)
    except ValueError:
        return True
    return datetime.utcnow() - timestamp > SESSION_TIMEOUT


def ensure_login_available(username: str) -> Tuple[bool, str]:
    sessions = load_sessions()
    entry = sessions.get(username)
    if not entry:
        return True, ""
    if is_session_expired(entry):
        with db_session() as db:
            db.query(SessionModel).filter(SessionModel.username == username).delete()
            db.commit()
        return True, ""
    minutes = max(1, SESSION_TIMEOUT_SECONDS // 60)
    return False, _(
        "This account is already signed in. Please sign out from the other session or wait %(minutes)d minutes.",
        minutes=minutes,
    )


def register_active_session(username: str) -> str:
    token = uuid4().hex
    now = datetime.utcnow().isoformat()
    with db_session() as db:
        existing = db.query(SessionModel).filter(SessionModel.username == username).first()
        if existing:
            existing.token = token
            existing.last_seen = now
        else:
            db.add(SessionModel(username=username, token=token, last_seen=now))
        db.commit()
    return token


def release_active_session(username: str, token: Optional[str]) -> None:
    if not username:
        return
    with db_session() as db:
        entry = db.query(SessionModel).filter(SessionModel.username == username).first()
        if entry and (token is None or entry.token == token):
            db.delete(entry)
            db.commit()


def force_release_session(username: str) -> None:
    if not username:
        return
    with db_session() as db:
        db.query(SessionModel).filter(SessionModel.username == username).delete()
        db.commit()


def refresh_session(username: str, token: str) -> bool:
    with db_session() as db:
        entry = db.query(SessionModel).filter(SessionModel.username == username).first()
        if not entry or entry.token != token or is_session_expired({
            "token": entry.token or "",
            "last_seen": entry.last_seen or "",
        }):
            if entry:
                db.delete(entry)
                db.commit()
            return False
        entry.last_seen = datetime.utcnow().isoformat()
        db.commit()
        return True

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
    return bleach.clean(
        pre_cleaned,
        tags=GUIDE_ALLOWED_TAGS,
        attributes=GUIDE_ALLOWED_ATTRS,
        protocols=GUIDE_ALLOWED_PROTOCOLS,
        strip=True,
    )


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
        "published": form.get("published") == "1",
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


# ==================== NEWS HELPERS ====================

def load_news_articles(status: Optional[str] = None) -> List[Dict[str, str]]:
    """Return news articles sorted by published_at descending.

    When status is provided, only articles with that status are returned.
    With status=None (default), all articles are returned regardless of status.
    """
    with db_session() as db:
        query = db.query(NewsArticleModel)
        if status:
            query = query.filter_by(status=status)
        articles = query.all()
        rows = [{field: (getattr(a, field) or "") for field in NEWS_FIELDS} for a in articles]
        rows.sort(key=lambda r: r.get("published_at", ""), reverse=True)
        return rows


def get_news_article(article_id: str) -> Optional[Dict[str, str]]:
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            return {field: (getattr(article, field) or "") for field in NEWS_FIELDS}
        return None


def save_news_article(article: Dict[str, str]) -> None:
    with db_session() as db:
        db.add(NewsArticleModel(**{field: article.get(field, "") for field in NEWS_FIELDS}))
        db.commit()


def update_news_article(article_id: str, data: Dict[str, str]) -> bool:
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            for field in NEWS_FIELDS:
                if field in ("id", "published_at"):
                    continue
                if field in data:
                    setattr(article, field, data[field])
            # When transitioning from draft to published, refresh published_at.
            if data.get("status") == "published" and (article.published_at is None or article.published_at == ""):
                article.published_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            db.commit()
            return True
        return False


def delete_news_article(article_id: str) -> bool:
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            db.delete(article)
            db.commit()
            return True
        return False


app = create_app()


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
