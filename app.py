from __future__ import annotations

import csv
import json
import os
import base64
import binascii
import hashlib
import hmac
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
from urllib.parse import urlparse

from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
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
ALLOWED_EXTENSIONS = {"pdf"}
MAX_SEARCH_RESULTS = 20
PASSWORD_SCHEME = "pbkdf2_sha256"
SUPPORTED_LOCALES = ("en", "zh")
SESSION_FILE = DATA_DIR / "active_sessions.json"
SESSION_TIMEOUT_SECONDS = int(os.environ.get("PAPERQUERY_SESSION_TIMEOUT", "600"))
SESSION_TIMEOUT = timedelta(seconds=SESSION_TIMEOUT_SECONDS)

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
        MAX_CONTENT_LENGTH=32 * 1024 * 1024,  # 32 MB upload limit
        BABEL_DEFAULT_LOCALE="en",
        BABEL_DEFAULT_TIMEZONE="UTC",
        BABEL_SUPPORTED_LOCALES=",".join(SUPPORTED_LOCALES),
    )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PAPERS_DIR.mkdir(parents=True, exist_ok=True)
    USERS_CSV.parent.mkdir(parents=True, exist_ok=True)
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not SESSION_FILE.exists():
        SESSION_FILE.write_text("{}", encoding="utf-8")
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

    @app.route("/")
    def index():
        if session.get("user"):
            return redirect(url_for("dashboard"))
        return redirect(url_for("login"))

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            user = authenticate(username, password)
            if user:
                allowed, warning = ensure_login_available(user["username"])
                if not allowed:
                    flash(warning, "warning")
                    return render_template("login.html")
                preferred_lang = session.get("language")
                session.clear()
                token = register_active_session(user["username"])
                if preferred_lang:
                    session["language"] = preferred_lang
                session["user"] = user
                session["session_token"] = token
                flash(_("Welcome back, %(username)s!", username=user["username"]), "success")
                return redirect(url_for("dashboard"))
            flash(_("Invalid credentials or access expired."), "danger")
        return render_template("login.html")

    @app.route("/logout")
    def logout():
        language = session.get("language")
        release_active_session(session.get("user", {}).get("username", ""), session.get("session_token"))
        session.clear()
        if language:
            session["language"] = language
        flash(_("Signed out successfully."), "info")
        return redirect(url_for("login"))

    @app.route("/dashboard")
    def dashboard():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        return render_template("dashboard.html", user=user)

    @app.route("/search", methods=["GET", "POST"])
    def search():
        user = require_login()
        if not user:
            return redirect(url_for("login"))

        query = ""
        results: List[Dict[str, str]] = []

        if request.method == "POST":
            query = request.form.get("query", "").strip()
            if not query:
                flash(_("Enter a keyword to search."), "warning")
            else:
                results = search_papers(query)
                if not results:
                    flash(_("No matching papers found."), "info")

        return render_template("search.html", user=user, query=query, results=results)

    @app.route("/upload", methods=["GET", "POST"])
    def upload():
        user = require_login(level=2)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        if request.method == "POST":
            file = request.files.get("paper")
            if not file or file.filename == "":
                flash(_("Select a PDF file to upload."), "warning")
            else:
                filename = secure_filename(file.filename)
                if not allowed_file(filename):
                    flash(_("Only PDF files are accepted."), "danger")
                else:
                    save_path = PAPERS_DIR / filename
                    if save_path.exists():
                        flash(_("A paper with this filename already exists. Rename your file and try again."), "warning")
                    else:
                        file.save(save_path)
                        flash(_("Uploaded %(filename)s.", filename=filename), "success")
                        return redirect(url_for("upload"))

        return render_template("upload.html", user=user)

    @app.route("/delete", methods=["GET", "POST"])
    def delete():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        available = sorted(p.name for p in PAPERS_DIR.glob("*.pdf"))

        if request.method == "POST":
            filename = request.form.get("filename")
            confirm = request.form.get("confirm")
            if filename not in available:
                flash(_("Select a valid paper to delete."), "danger")
            elif confirm != filename:
                flash(_("Type the filename exactly to confirm deletion."), "warning")
            else:
                (PAPERS_DIR / filename).unlink(missing_ok=True)
                flash(_("Deleted %(filename)s.", filename=filename), "success")
                return redirect(url_for("delete"))

        return render_template("delete.html", user=user, papers=available)

    @app.route("/set-language/<locale_code>")
    def set_language(locale_code: str):
        if locale_code not in SUPPORTED_LOCALES:
            flash(_("Language not supported."), "warning")
        else:
            session["language"] = locale_code
            flash(
                _("Language switched to %(language)s.", language=str(LANGUAGE_NAMES[locale_code])),
                "success",
            )
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

    @app.route("/papers/<path:filename>")
    def download(filename: str):
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        return send_from_directory(PAPERS_DIR, filename, as_attachment=True)

    return app


def load_users() -> List[Dict[str, str]]:
    if not USERS_CSV.exists():
        return []

    with USERS_CSV.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]


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
    matches: List[Dict[str, str]] = []
    normalized = keyword.lower()

    for pdf_path in PAPERS_DIR.glob("*.pdf"):
        try:
            text = extract_pdf_text(pdf_path)
        except Exception as exc:  # pragma: no cover - logging placeholder
            print(f"Failed to read {pdf_path.name}: {exc}")
            continue
        if normalized in text.lower():
            matches.append(
                {
                    "title": pdf_path.stem,
                    "filename": pdf_path.name,
                }
            )

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


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
