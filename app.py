from __future__ import annotations
import os
from dotenv import load_dotenv
load_dotenv()

import base64
import binascii
import csv
import hashlib
import hmac
import json
import math
import os
import secrets
import shutil
from contextlib import contextmanager
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
from urllib.parse import urlparse

import msal
import requests
from sqlalchemy import Column, Date, DateTime, ForeignKey, String, Text, Unicode, UnicodeText, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from flask import (
    Flask,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    session,
    url_for,
)
from flask_babel import Babel, gettext as _, get_locale, lazy_gettext as _l
from werkzeug.utils import secure_filename


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("PAPERQUERY_DATA_DIR", BASE_DIR / "data")).resolve()
PAPERS_DIR = Path(os.environ.get("PAPERQUERY_UPLOAD_DIR", BASE_DIR / "papers")).resolve()
USERS_CSV_ENV = os.environ.get("PAPERQUERY_USERS_CSV")
USERS_CSV = Path(USERS_CSV_ENV).resolve() if USERS_CSV_ENV else DATA_DIR / "users.csv"
LOCAL_USER_FIELDS = ["username", "password", "registration_date", "expiry_date", "role", "email", "first_name", "last_name", "school"]
METADATA_CSV = DATA_DIR / "papers_metadata.csv"
MS_USERS_CSV = DATA_DIR / "ms_users.csv"
ACCOUNT_LINKS_CSV = DATA_DIR / "account_links.csv"
ACCOUNT_LINK_FIELDS = ["username", "ms_id", "linked_at"]
NEWS_CSV = DATA_DIR / "news.csv"
NEWS_FIELDS = ["id", "title", "category", "abstract", "body", "author", "image_url", "published_at"]
_DEFAULT_NEWS_CATEGORIES = [
    "活动回顾", "期刊发布", "讲座预告", "成果展示",
    "公告通知", "学术动态", "社团新闻", "其他",
]
CATEGORIES_JSON = DATA_DIR / "news_categories.json"
JOURNALS_JSON = DATA_DIR / "paper_journals.json"
_DEFAULT_PAPER_CATEGORIES = ["literature", "natural-science", "social-science", "humanities"]
_DEFAULT_PAPER_JOURNALS: list = []
SUBMISSIONS_JSON = DATA_DIR / "submissions.json"
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


def load_journals() -> list:
    """Load journals as list of dicts from JSON."""
    if not db_enabled():
        if JOURNALS_JSON.exists():
            try:
                data = json.loads(JOURNALS_JSON.read_text(encoding="utf-8"))
                # Migration: convert old string-based list to rich objects
                if data and isinstance(data[0], str):
                    from uuid import uuid4
                    migrated = []
                    for name in data:
                        migrated.append({
                            "id": uuid4().hex[:12],
                            "name": name,
                            "cover_image": "",
                            "introduction": "",
                            "created_at": datetime.utcnow().date().isoformat(),
                        })
                    save_journals(migrated)
                    return migrated
                return data
            except (json.JSONDecodeError, OSError):
                pass
        save_journals([])
        return []
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
    if not db_enabled():
        JOURNALS_JSON.parent.mkdir(parents=True, exist_ok=True)
        JOURNALS_JSON.write_text(json.dumps(journals, ensure_ascii=False, indent=2), encoding="utf-8")
        return

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
MAX_SEARCH_RESULTS = 20
PASSWORD_SCHEME = "pbkdf2_sha256"
SUPPORTED_LOCALES = ("en", "zh")
SESSION_FILE = DATA_DIR / "active_sessions.json"
SESSION_TIMEOUT_SECONDS = int(os.environ.get("PAPERQUERY_SESSION_TIMEOUT", "600"))
SESSION_TIMEOUT = timedelta(seconds=SESSION_TIMEOUT_SECONDS)
METADATA_FIELDS = ["filename", "title", "journal", "category", "language", "keywords", "abstract", "author_name", "author_email", "author_school", "published_at"]
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
    "created_at",
    "updated_at",
]
MS_CLIENT_ID = os.environ.get("PAPERQUERY_MS_CLIENT_ID")
MS_CLIENT_SECRET = os.environ.get("PAPERQUERY_MS_CLIENT_SECRET")
MS_REDIRECT_URI = os.environ.get("PAPERQUERY_MS_REDIRECT_URI", "http://127.0.0.1:5000/auth/callback")
MS_AUTHORITY = os.environ.get("PAPERQUERY_MS_AUTHORITY", "https://login.microsoftonline.com/common")
MS_SCOPES = ["User.Read"]
MS_GRAPH_ME_URL = "https://graph.microsoft.com/v1.0/me"
USE_CSV = os.environ.get("PAPERQUERY_USE_CSV", "").strip().lower() in ("1", "true", "yes")
DB_URL = os.environ.get("PAPERQUERY_DATABASE_URL")
BASE = declarative_base()
_ENGINE = None
_SESSION_LOCAL = None
ROLE_OPTIONS = [
    ("1", "Reader"),
    ("2", "Moderator"),
    ("3", "Admin"),
]

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



def db_enabled() -> bool:
    return bool(DB_URL) and not USE_CSV


def ensure_csv_file(path: Path, fieldnames: List[str]) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()


def read_csv_rows(path: Path, fieldnames: List[str]) -> List[Dict[str, str]]:
    ensure_csv_file(path, fieldnames)
    with path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = []
        for row in reader:
            rows.append({field: row.get(field, "") or "" for field in fieldnames})
        return rows


def write_csv_rows(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") or "" for field in fieldnames})


def ensure_user_csv() -> None:
    ensure_csv_file(USERS_CSV, LOCAL_USER_FIELDS)


def ensure_ms_users_csv() -> None:
    ensure_csv_file(MS_USERS_CSV, MS_USER_FIELDS)


def ensure_account_links_csv() -> None:
    ensure_csv_file(ACCOUNT_LINKS_CSV, ACCOUNT_LINK_FIELDS)


def ensure_news_csv() -> None:
    ensure_csv_file(NEWS_CSV, NEWS_FIELDS)


def init_db() -> None:
    if not db_enabled():
        return
    global _ENGINE, _SESSION_LOCAL
    if _ENGINE is None:
        _ENGINE = create_engine(DB_URL, pool_pre_ping=True)
        _SESSION_LOCAL = sessionmaker(bind=_ENGINE)
        BASE.metadata.create_all(_ENGINE)


@contextmanager
def db_session():
    if not db_enabled():
        raise RuntimeError("Database is disabled or not configured.")
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
    app.config.update(
        SECRET_KEY=os.environ.get("PAPERQUERY_SECRET", "dev-secret-key"),
        UPLOAD_FOLDER=str(PAPERS_DIR),
        BABEL_DEFAULT_LOCALE="en",
        BABEL_DEFAULT_TIMEZONE="UTC",
        BABEL_SUPPORTED_LOCALES=",".join(SUPPORTED_LOCALES),
    )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PAPERS_DIR.mkdir(parents=True, exist_ok=True)
    PENDING_PAPERS_DIR.mkdir(parents=True, exist_ok=True)
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not SESSION_FILE.exists():
        SESSION_FILE.write_text("{}", encoding="utf-8")
    ensure_metadata_file()
    if db_enabled():
        init_db()
    else:
        ensure_user_csv()
        ensure_ms_users_csv()
        ensure_account_links_csv()
    ensure_news_csv()
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

    @app.context_processor
    def inject_global_vars():
        """Inject global variables into all templates."""
        return {
            "current_year": datetime.utcnow().year,
            "site_name": "Keydion",
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

    @app.route("/")
    def index():
        user = session.get("user")
        token = session.get("session_token")
        if user and token:
            if not refresh_session(user.get("username", ""), token):
                session.clear()
        latest_news = load_news_articles()[:4]
        return render_template("landing.html", ms_enabled=is_ms_configured(), latest_news=latest_news)

    @app.route("/faq")
    def faq():
        return render_template("FAQ.html")

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            email = request.form.get("email", "").strip()
            password = request.form.get("password", "").strip()
            # Look up by email first, then fall back to username for legacy accounts
            user_record = get_local_user_by_email(email)
            if not user_record:
                user_record = get_local_user(email)
            if user_record:
                user = authenticate(user_record.get("username", ""), password)
            else:
                user = None
            if user:
                allowed, warning = ensure_login_available(user["username"])
                if not allowed:
                    flash(warning, "warning")
                    return render_template("login.html", ms_enabled=is_ms_configured())
                display = user_record.get("first_name", "") or user_record.get("email", "") or user["username"]
                start_local_session(
                    user,
                    display_name=display,
                    email=user_record.get("email", ""),
                )
                flash(_("Welcome back, %(username)s!", username=display), "success")
                return redirect(url_for("index"))
            flash(_("Invalid email or password"), "danger")
            return render_template("login.html", ms_enabled=is_ms_configured())

        if not is_ms_configured():
            flash(_("Microsoft sign-in is not configured. Please contact the administrator."), "warning")
        return render_template("login.html", ms_enabled=is_ms_configured())

    @app.route("/admin/login", methods=["GET", "POST"])
    def admin_login():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            user = authenticate(username, password)
            if user:
                allowed, warning = ensure_login_available(user["username"])
                if not allowed:
                    flash(warning, "warning")
                    return render_template("admin_login.html")
                start_local_session(user)
                flash(_("Welcome back, %(username)s!", username=user["username"]), "success")
                return redirect(url_for("index"))
            flash(_("Invalid username or password"), "danger")
            return render_template("admin_login.html")
        return render_template("admin_login.html")

    @app.route("/register", methods=["GET", "POST"])
    def register():
        if request.method == "POST":
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "").strip()
            confirm_password = request.form.get("confirm_password", "").strip()
            first_name = request.form.get("first_name", "").strip()
            last_name = request.form.get("last_name", "").strip()

            if not email or not password or not first_name or not last_name:
                flash(_("Email, password, first name, and last name are required."), "warning")
                return render_template("register.html",
                    email=email, first_name=first_name, last_name=last_name)
            if password != confirm_password:
                flash(_("Passwords do not match."), "warning")
                return render_template("register.html",
                    email=email, first_name=first_name, last_name=last_name)
            if get_local_user_by_email(email) or get_local_user(email):
                flash(_("An account with this email already exists."), "warning")
                return render_template("register.html",
                    email=email, first_name=first_name, last_name=last_name)

            try:
                create_local_user(
                    username=email,
                    password=password,
                    role="1",
                    email=email,
                    first_name=first_name,
                    last_name=last_name,
                    school="",
                )
            except ValueError:
                flash(_("An account with this email already exists."), "warning")
                return render_template("register.html",
                    email=email, first_name=first_name, last_name=last_name)

            user = authenticate(email, password)
            if user:
                start_local_session(user, display_name=first_name, email=email)
                flash(_("Account created successfully. Welcome, %(name)s!", name=first_name), "success")
                return redirect(url_for("index"))
            flash(_("Account created. Please sign in."), "success")
            return redirect(url_for("login"))

        return render_template("register.html",
            email="", first_name="", last_name="")

    @app.route("/auth/login")
    def ms_login():
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
        start_ms_session(user_record)

        if not is_profile_complete(user_record):
            return redirect(url_for("profile_setup"))
        destination = url_for("index")
        return redirect(destination)

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
                    session["user"]["display_name"] = updated.get("display_name") or session["user"].get("display_name", "")
                flash(_("Profile saved successfully."), "success")
                return redirect(url_for("index"))

        return render_template(
            "profile_setup.html",
            profile=record,
        )



    @app.route("/admin/users")
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

    @app.route("/admin/users/roles", methods=["POST"])
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

    @app.route("/admin/users/add", methods=["POST"])
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

    @app.route("/admin/users/<path:username>/role", methods=["POST"])
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

    @app.route("/admin/users/<path:username>/reset-password", methods=["POST"])
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

    @app.route("/admin/users/<path:username>/delete", methods=["POST"])
    def admin_delete_local_user(username: str):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        if delete_local_user(username):
            flash(_("Local user deleted."), "success")
        else:
            flash(_("Unable to delete user."), "warning")
        return redirect(url_for("admin_users"))

    @app.route("/admin/ms-users/<path:ms_id>/role", methods=["POST"])
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

    @app.route("/admin/ms-users/<path:ms_id>/delete", methods=["POST"])
    def admin_delete_ms_user(ms_id: str):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        if delete_ms_user(ms_id):
            flash(_("Microsoft user deleted."), "success")
        else:
            flash(_("Unable to delete Microsoft user."), "warning")
        return redirect(url_for("admin_users"))

    @app.route("/dashboard")
    def dashboard():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        return render_template("dashboard.html", user=user)

    @app.route("/advanced-search")
    def advanced_search():
        user = get_active_user()
        is_guest = user is None
        journals = load_journals()
        return render_template("advanced_search.html", user=user, journals=journals)

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

        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1

        per_page = 20
        filtered = bool(query) or bool(category_filter) or bool(language_filter) or bool(date_filter) or bool(author_filter) or bool(title_filter) or bool(start_year) or bool(end_year) or bool(journal_filters)
        
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

        if filtered and not record_pool:
            flash(_("No matching papers found."), "info")

        pagination = paginate_records(record_pool, page, per_page)

        return render_template(
            "search.html",
            user=user,
            query=query,
            category_filter=category_filter,
            language_filter=language_filter,
            date_filter=date_filter,
            filtered=filtered,
            records=pagination["items"],
            pagination=pagination,
            is_guest=is_guest,
            total_matches=len(record_pool),
            paper_categories=load_paper_categories(),
            journal_id_map=get_journal_id_map(),
        )

    @app.route("/upload", methods=["GET", "POST"])
    def upload():
        user = require_login(level=1)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        today = datetime.utcnow().date().isoformat()
        author_names = [n.strip() for n in request.form.getlist("author_name") if n.strip()]
        author_emails = [e.strip() for e in request.form.getlist("author_email") if e.strip()]
        author_schools = [s.strip() for s in request.form.getlist("author_school") if s.strip()]

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
        }

        if request.method == "POST":
            # 验证必填字段
            if not form_data["title"]:
                flash(_("Please enter the paper title"), "danger")
                return render_template("upload.html", user=user, form_data=form_data)
            if not form_data["category"]:
                flash(_("Please select a subject category"), "danger")
                return render_template("upload.html", user=user, form_data=form_data)
            if not form_data["language"]:
                flash(_("Please select a language"), "danger")
                return render_template("upload.html", user=user, form_data=form_data)
            if not form_data["keywords"]:
                flash(_("Please enter keywords"), "danger")
                return render_template("upload.html", user=user, form_data=form_data)
            if not form_data["abstract"]:
                flash(_("Please enter the abstract"), "danger")
                return render_template("upload.html", user=user, form_data=form_data)
            if not form_data["author_name"]:
                flash(_("Please enter the author name"), "danger")
                return render_template("upload.html", user=user, form_data=form_data)
            if not form_data["author_email"]:
                flash(_("Please enter the contact email"), "danger")
                return render_template("upload.html", user=user, form_data=form_data)
            if not form_data["author_school"]:
                flash(_("Please enter the school name"), "danger")
                return render_template("upload.html", user=user, form_data=form_data)

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
                if not allowed_file(original_filename):
                    flash(_("Only PDF files are supported"), "danger")
                else:
                    filename = secure_filename(f"{form_data['title']}_{form_data['author_name']}.pdf")
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
                                },
                            )
                            flash(_("Paper %(filename)s uploaded successfully!", filename=filename), "success")
                            return redirect(url_for("upload"))
                    else:
                        # Reader: save to pending review queue
                        sub_id = uuid4().hex[:12]
                        pending_filename = f"{sub_id}_{filename}"
                        pending_path = PENDING_PAPERS_DIR / pending_filename
                        file.save(pending_path)
                        set_pdf_metadata(pending_path, form_data["title"], form_data["author_name"])
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
                        }
                        _save_submission(submission)
                        return redirect(url_for("upload_success", title=form_data["title"]))

        return render_template("upload.html", user=user, form_data=form_data, journals=get_journal_names(), paper_categories=load_paper_categories())

    @app.route("/upload/success")
    def upload_success():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        title = request.args.get("title", "")
        submitted_at = datetime.utcnow().strftime("%Y.%m.%d %H:%M:%S")
        return render_template("upload_success.html", user=user, title=title, submitted_at=submitted_at)

    @app.route("/delete")
    def delete():
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

    @app.route("/paper/<path:filename>/modify", methods=["GET", "POST"])
    def paper_modify(filename):
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        paper_path = PAPERS_DIR / filename
        if not paper_path.exists():
            flash(_("Paper not found."), "warning")
            return redirect(url_for("delete"))

        meta_rows = load_paper_metadata()
        meta = {}
        for r in meta_rows:
            if r.get("filename") == filename:
                meta = r
                break

        if request.method == "POST":
            title = request.form.get("title", "").strip()
            author_name = request.form.get("author_name", "").strip()

            new_filename = secure_filename(f"{title}_{author_name}.pdf")
            if new_filename != filename:
                new_paper_path = PAPERS_DIR / new_filename
                if new_paper_path.exists():
                    flash(_("A file with the new name already exists, unable to rename."), "warning")
                    return redirect(url_for("paper_modify", filename=filename))
                else:
                    paper_path.rename(new_paper_path)
                    remove_paper_metadata(filename)
                    filename = new_filename

            set_pdf_metadata(PAPERS_DIR / filename, title, author_name)

            upsert_paper_metadata(filename, {
                "title": title,
                "journal": request.form.get("journal", "").strip(),
                "category": request.form.get("category", "").strip(),
                "language": request.form.get("language", "").strip(),
                "keywords": request.form.get("keywords", "").strip(),
                "abstract": request.form.get("abstract", "").strip(),
                "author_name": author_name,
                "author_email": request.form.get("author_email", "").strip(),
                "author_school": request.form.get("author_school", "").strip(),
            })
            flash(_("Paper information updated."), "success")
            return redirect(url_for("delete"))

        return render_template("paper_modify.html", user=user, filename=filename, meta=meta,
                               categories=load_paper_categories(), journals=get_journal_names())

    @app.route("/paper/<path:filename>/delete", methods=["POST"])
    def paper_delete(filename):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))

        paper_path = PAPERS_DIR / filename
        if not paper_path.exists():
            flash(_("Paper not found."), "warning")
            return redirect(url_for("delete"))

        remove_paper_metadata(filename)
        paper_path.unlink(missing_ok=True)
        flash(_("Deleted %(filename)s.", filename=filename), "success")
        return redirect(url_for("delete"))

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

        pdf_url = url_for("paper_preview", filename=filename) if is_guest else url_for("paper_file", filename=filename)
        return render_template(
            "preview.html",
            user=user,
            paper=paper,
            related_papers=related_papers,
            source_query=source_query,
            source_page=source_page,
            is_guest=is_guest,
            pdf_url=pdf_url,
            journal_id_map=get_journal_id_map(),
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
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        pdf_path = PAPERS_DIR / filename
        if not pdf_path.exists():
            abort(404)
        return send_from_directory(PAPERS_DIR, filename, as_attachment=False)

    @app.route("/papers/<path:filename>")
    def download(filename: str):
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
        all_articles = load_news_articles()
        pagination = paginate_records(all_articles, page, per_page)
        recent = all_articles[:6]
        return render_template(
            "news.html",
            articles=pagination["items"],
            pagination=pagination,
            recent=recent,
        )

    @app.route("/news/upload-inline-image", methods=["POST"])
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

    @app.route("/news/publish", methods=["GET", "POST"])
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
        }

        if request.method == "POST":
            if not form_data["title"]:
                flash(_("Please enter a title."), "warning")
            elif not form_data["category"]:
                flash(_("Please select a category."), "warning")
            elif not form_data["abstract"]:
                flash(_("Please enter an abstract."), "warning")
            elif not form_data["body"]:
                flash(_("Please write the article body."), "warning")
            else:
                article_id = uuid4().hex[:12]
                # Handle cover image upload
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
                        )
                article = {
                    "id": article_id,
                    "title": form_data["title"],
                    "category": form_data["category"],
                    "abstract": form_data["abstract"],
                    "body": form_data["body"],
                    "author": form_data["author"],
                    "image_url": image_url,
                    "published_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                }
                save_news_article(article)
                flash(_("Article published successfully."), "success")
                return redirect(url_for("news_list"))

        return render_template(
            "news_publish.html",
            form_data=form_data,
            categories=load_categories(),
            editing=False,
        )

    @app.route("/news/<news_id>/edit", methods=["GET", "POST"])
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
        }

        if request.method == "POST":
            form_data = {
                "title": request.form.get("title", "").strip(),
                "category": request.form.get("category", "").strip(),
                "abstract": request.form.get("abstract", "").strip(),
                "body": request.form.get("body", "").strip(),
                "author": request.form.get("author", "").strip(),
                "image_url": article["image_url"],  # keep existing by default
            }
            if not form_data["title"]:
                flash(_("Please enter a title."), "warning")
            elif not form_data["category"]:
                flash(_("Please select a category."), "warning")
            elif not form_data["abstract"]:
                flash(_("Please enter an abstract."), "warning")
            elif not form_data["body"]:
                flash(_("Please write the article body."), "warning")
            else:
                # Handle cover image upload
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
                        )
                # Check if user wants to remove existing image
                if request.form.get("remove_image") == "1":
                    form_data["image_url"] = ""
                update_news_article(news_id, form_data)
                flash(_("Article updated."), "success")
                return redirect(url_for("news_list"))

        return render_template(
            "news_publish.html",
            form_data=form_data,
            categories=load_categories(),
            editing=True,
        )

    @app.route("/news/<news_id>/delete", methods=["POST"])
    def news_delete(news_id: str):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        if delete_news_article(news_id):
            flash(_("Article deleted."), "success")
        else:
            flash(_("Article not found."), "warning")
        return redirect(url_for("news_list"))

    @app.route("/news/manage")
    def news_manage():
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        articles = load_news_articles()
        return render_template("news_manage.html", articles=articles, user=user, categories=load_categories())

    # ---------- Category management API ----------
    @app.route("/news/categories/add", methods=["POST"])
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

    @app.route("/news/categories/rename", methods=["POST"])
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
            if not db_enabled():
                write_csv_rows(NEWS_CSV, NEWS_FIELDS, articles)
            else:
                with db_session() as db:
                    for art in articles:
                        if art.get("category") == new_name:
                            db_art = db.query(NewsArticleModel).filter_by(id=art.get("id")).first()
                            if db_art:
                                db_art.category = new_name
                    db.commit()
        return jsonify(categories=cats)

    @app.route("/news/categories/delete", methods=["POST"])
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

    # ---------- Paper categories & journals management ----------
    @app.route("/admin/paper-manage")
    def paper_manage():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        return render_template("paper_manage.html", user=user,
                               paper_categories=load_paper_categories(),
                               journals=load_journals())

    @app.route("/admin/paper-categories/add", methods=["POST"])
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

    @app.route("/admin/paper-categories/rename", methods=["POST"])
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

    @app.route("/admin/paper-categories/delete", methods=["POST"])
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

    @app.route("/admin/journals/add", methods=["POST"])
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

    @app.route("/admin/journals/delete", methods=["POST"])
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

    @app.route("/admin/journal/<journal_id>/edit", methods=["GET", "POST"])
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
                return redirect(url_for("journal_edit", journal_id=journal_id))

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
            return redirect(url_for("journal_edit", journal_id=journal_id))

        # GET: load papers belonging to this journal
        all_papers = gather_paper_records()
        journal_papers = [p for p in all_papers if p.get("journal") == journal["name"]]
        journal_papers.sort(key=lambda r: r.get("published_at") or "", reverse=True)

        return render_template("journal_edit.html", user=user, journal=journal, papers=journal_papers)

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
        all_articles = load_news_articles()
        related = [a for a in all_articles if a.get("id") != news_id][:3]
        return render_template("news_article.html", article=article, related=related)

    # ---- Submission review helpers ----

    def _load_submissions():
        if not db_enabled():
            if not SUBMISSIONS_JSON.exists():
                return []
            try:
                return json.loads(SUBMISSIONS_JSON.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return []
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
                "original_filename": s.original_filename
            } for s in subs]

    def _write_submissions(subs):
        if not db_enabled():
            SUBMISSIONS_JSON.parent.mkdir(parents=True, exist_ok=True)
            SUBMISSIONS_JSON.write_text(json.dumps(subs, ensure_ascii=False, indent=2), encoding="utf-8")
            return
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
                    original_filename=s.get("original_filename")
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

    @app.route("/my-submissions")
    def my_submissions():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        username = user.get("username", "")
        subs = [s for s in _load_submissions() if s.get("submitter") == username]
        subs.sort(key=lambda s: s.get("submitted_at", ""), reverse=True)
        return render_template("my_submissions.html", user=user, submissions=subs)

    @app.route("/my-submissions/<sub_id>")
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

    @app.route("/my-submissions/<sub_id>/file")
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

    @app.route("/review")
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

    @app.route("/review/<sub_id>")
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

    @app.route("/review/<sub_id>/accept", methods=["POST"])
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
            filename = secure_filename(f"{sub.get('title', 'paper')}_{sub.get('author_name', 'author')}.pdf")
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
                "category": sub.get("category", ""),
                "keywords": sub.get("keywords", ""),
                "abstract": sub.get("abstract", ""),
                "author_name": sub.get("author_name", ""),
                "author_email": sub.get("author_email", ""),
                "author_school": sub.get("author_school", ""),
                "published_at": today,
            },
        )

        reviewer_name = user.get("display_name", "") or user.get("first_name", "") or user.get("username", "")
        _update_submission(sub_id, {
            "status": "accepted",
            "reviewed_at": datetime.utcnow().isoformat(),
            "reviewer": reviewer_name,
        })
        flash(_("Paper accepted and published."), "success")
        return redirect(url_for("review_list"))

    @app.route("/review/<sub_id>/reject", methods=["POST"])
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

    @app.route("/pending-papers/<path:filename>")
    def pending_paper_file(filename):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        return send_from_directory(str(PENDING_PAPERS_DIR), filename)

    return app


def load_users() -> List[Dict[str, str]]:
    if not db_enabled():
        return read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS)
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
    if not db_enabled():
        for row in read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS):
            if row.get("username") == username:
                return row
        return None
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
    if not db_enabled():
        for row in read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS):
            if row.get("email", "").lower() == email.lower():
                return row
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
    if not db_enabled():
        rows = read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS)
        if any(row.get("username") == username for row in rows):
            raise ValueError("Username already exists.")
        record = {
            "username": username,
            "password": hash_password(password),
            "registration_date": datetime.utcnow().date().isoformat(),
            "expiry_date": "",
            "role": role,
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "school": school,
        }
        rows.append(record)
        write_csv_rows(USERS_CSV, LOCAL_USER_FIELDS, rows)
        return record
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
    if not db_enabled():
        rows = read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS)
        updated = False
        for row in rows:
            if row.get("username") == username:
                row["role"] = role
                updated = True
                break
        if updated:
            write_csv_rows(USERS_CSV, LOCAL_USER_FIELDS, rows)
        return updated
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return False
        user.role = role
        return True


def update_local_user_password(username: str, password: str) -> bool:
    if not db_enabled():
        rows = read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS)
        updated = False
        for row in rows:
            if row.get("username") == username:
                row["password"] = hash_password(password)
                updated = True
                break
        if updated:
            write_csv_rows(USERS_CSV, LOCAL_USER_FIELDS, rows)
        return updated
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return False
        user.password = hash_password(password)
        return True


def delete_local_user(username: str) -> bool:
    if not db_enabled():
        rows = read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS)
        new_rows = [row for row in rows if row.get("username") != username]
        if len(new_rows) == len(rows):
            return False
        write_csv_rows(USERS_CSV, LOCAL_USER_FIELDS, new_rows)
        return True
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
    display_name = (record.get("display_name", "") or "").strip()
    if not display_name:
        display_name = f"{record.get('first_name', '').strip()} {record.get('last_name', '').strip()}".strip()
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
    if not db_enabled():
        return read_csv_rows(MS_USERS_CSV, MS_USER_FIELDS)
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
    if not db_enabled():
        for row in read_csv_rows(MS_USERS_CSV, MS_USER_FIELDS):
            if row.get("ms_id") == ms_id:
                return row
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
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def upsert_ms_user(profile: Dict[str, str]) -> Dict[str, str]:
    ms_id = profile.get("ms_id", "")
    now = datetime.utcnow()
    if not db_enabled():
        rows = read_csv_rows(MS_USERS_CSV, MS_USER_FIELDS)
        record = next((row for row in rows if row.get("ms_id") == ms_id), None)
        if not record:
            record = {
                "ms_id": ms_id,
                "tenant_id": profile.get("tenant_id", ""),
                "email": profile.get("email", ""),
                "display_name": profile.get("display_name", ""),
                "first_name": profile.get("first_name", ""),
                "last_name": profile.get("last_name", ""),
                "school": profile.get("school", ""),
                "grade": profile.get("grade", ""),
                "role": profile.get("role", "1") or "1",
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }
            rows.append(record)
        else:
            record["tenant_id"] = profile.get("tenant_id", "") or record.get("tenant_id", "")
            record["email"] = profile.get("email", "") or record.get("email", "")
            record["display_name"] = profile.get("display_name", "") or record.get("display_name", "")
            record["role"] = record.get("role", "1") or "1"
            record["updated_at"] = now.isoformat()
        write_csv_rows(MS_USERS_CSV, MS_USER_FIELDS, rows)
        return record
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
    if not db_enabled():
        rows = read_csv_rows(MS_USERS_CSV, MS_USER_FIELDS)
        record = next((row for row in rows if row.get("ms_id") == ms_id), None)
        if not record:
            return None
        for key, value in updates.items():
            if key in MS_USER_FIELDS:
                record[key] = value
        if record.get("first_name") or record.get("last_name"):
            record["display_name"] = f"{(record.get('first_name') or '').strip()} {(record.get('last_name') or '').strip()}".strip()
        record["updated_at"] = datetime.utcnow().isoformat()
        write_csv_rows(MS_USERS_CSV, MS_USER_FIELDS, rows)
        return record
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
    if not db_enabled():
        rows = read_csv_rows(MS_USERS_CSV, MS_USER_FIELDS)
        updated = False
        for row in rows:
            if row.get("ms_id") == ms_id:
                row["role"] = role
                row["updated_at"] = datetime.utcnow().isoformat()
                updated = True
                break
        if updated:
            write_csv_rows(MS_USERS_CSV, MS_USER_FIELDS, rows)
        return updated
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return False
        user.role = role
        user.updated_at = datetime.utcnow()
        return True


def delete_ms_user(ms_id: str) -> bool:
    if not db_enabled():
        rows = read_csv_rows(MS_USERS_CSV, MS_USER_FIELDS)
        new_rows = [row for row in rows if row.get("ms_id") != ms_id]
        if len(new_rows) == len(rows):
            return False
        write_csv_rows(MS_USERS_CSV, MS_USER_FIELDS, new_rows)
        return True
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
    if not user:
        flash(_("Please sign in first."), "warning")
        return None
    username = user.get("username", "")
    token = session.get("session_token")
    if not username or not token:
        session.clear()
        flash(_("Session expired. Please sign in again."), "warning")
        return None
    if not refresh_session(username, token):
        session.clear()
        flash(_("Session timed out. Please sign in again."), "warning")
        return None
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
        try:
            text = extract_pdf_text(pdf_path)
        except Exception as exc:  # pragma: no cover - logging placeholder
            print(f"Failed to read {pdf_path.name}: {exc}")
            continue
        if normalized in text.lower():
            matches.append(build_paper_record(pdf_path.name, metadata_index))

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


def ensure_metadata_file() -> None:
    if not METADATA_CSV.exists():
        METADATA_CSV.parent.mkdir(parents=True, exist_ok=True)
        with METADATA_CSV.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=METADATA_FIELDS)
            writer.writeheader()


def load_paper_metadata() -> List[Dict[str, str]]:
    if not db_enabled():
        ensure_metadata_file()
        with METADATA_CSV.open(newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            rows: List[Dict[str, str]] = []
            for raw_row in reader:
                normalized = {field: (raw_row.get(field, "") or "").strip() for field in METADATA_FIELDS}
                rows.append(normalized)
            return rows
    with db_session() as db:
        papers = db.query(PaperMetadataModel).all()
        return [{field: (getattr(p, field) or "") for field in METADATA_FIELDS} for p in papers]


def save_paper_metadata(rows: List[Dict[str, str]]) -> None:
    if not db_enabled():
        ensure_metadata_file()
        with METADATA_CSV.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=METADATA_FIELDS)
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in METADATA_FIELDS})
        return
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
    if not SESSION_FILE.exists():
        return {}
    try:
        return json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_sessions(data: Dict[str, Dict[str, str]]) -> None:
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    SESSION_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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
        sessions.pop(username, None)
        save_sessions(sessions)
        return True, ""
    minutes = max(1, SESSION_TIMEOUT_SECONDS // 60)
    return False, _(
        "This account is already signed in. Please sign out from the other session or wait %(minutes)d minutes.",
        minutes=minutes,
    )


def register_active_session(username: str) -> str:
    sessions = load_sessions()
    token = uuid4().hex
    sessions[username] = {
        "token": token,
        "last_seen": datetime.utcnow().isoformat(),
    }
    save_sessions(sessions)
    return token


def release_active_session(username: str, token: Optional[str]) -> None:
    if not username:
        return
    sessions = load_sessions()
    entry = sessions.get(username)
    if entry and (token is None or entry.get("token") == token):
        sessions.pop(username, None)
        save_sessions(sessions)


def force_release_session(username: str) -> None:
    """强制释放用户会话，不检查 token"""
    if not username:
        return
    sessions = load_sessions()
    if username in sessions:
        sessions.pop(username, None)
        save_sessions(sessions)


def refresh_session(username: str, token: str) -> bool:
    sessions = load_sessions()
    entry = sessions.get(username)
    if not entry or entry.get("token") != token or is_session_expired(entry):
        sessions.pop(username, None)
        save_sessions(sessions)
        return False
    entry["last_seen"] = datetime.utcnow().isoformat()
    sessions[username] = entry
    save_sessions(sessions)
    return True

# ==================== NEWS HELPERS ====================

def load_news_articles() -> List[Dict[str, str]]:
    """Return all news articles sorted by published_at descending."""
    if not db_enabled():
        rows = read_csv_rows(NEWS_CSV, NEWS_FIELDS)
        rows.sort(key=lambda r: r.get("published_at", ""), reverse=True)
        return rows
    with db_session() as db:
        articles = db.query(NewsArticleModel).all()
        rows = [{field: (getattr(a, field) or "") for field in NEWS_FIELDS} for a in articles]
        rows.sort(key=lambda r: r.get("published_at", ""), reverse=True)
        return rows


def get_news_article(article_id: str) -> Optional[Dict[str, str]]:
    if not db_enabled():
        for row in read_csv_rows(NEWS_CSV, NEWS_FIELDS):
            if row.get("id") == article_id:
                return row
        return None
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            return {field: (getattr(article, field) or "") for field in NEWS_FIELDS}
        return None


def save_news_article(article: Dict[str, str]) -> None:
    if not db_enabled():
        rows = read_csv_rows(NEWS_CSV, NEWS_FIELDS)
        rows.append(article)
        write_csv_rows(NEWS_CSV, NEWS_FIELDS, rows)
        return
    with db_session() as db:
        db.add(NewsArticleModel(**{field: article.get(field, "") for field in NEWS_FIELDS}))
        db.commit()


def update_news_article(article_id: str, data: Dict[str, str]) -> bool:
    if not db_enabled():
        rows = read_csv_rows(NEWS_CSV, NEWS_FIELDS)
        for row in rows:
            if row.get("id") == article_id:
                for field in NEWS_FIELDS:
                    if field in ("id", "published_at"):
                        continue
                    if field in data:
                        row[field] = data[field]
                write_csv_rows(NEWS_CSV, NEWS_FIELDS, rows)
                return True
        return False
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            for field in NEWS_FIELDS:
                if field in ("id", "published_at"):
                    continue
                if field in data:
                    setattr(article, field, data[field])
            db.commit()
            return True
        return False


def delete_news_article(article_id: str) -> bool:
    if not db_enabled():
        rows = read_csv_rows(NEWS_CSV, NEWS_FIELDS)
        filtered = [r for r in rows if r.get("id") != article_id]
        if len(filtered) != len(rows):
            write_csv_rows(NEWS_CSV, NEWS_FIELDS, filtered)
            return True
        return False
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
