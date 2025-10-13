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
METADATA_CSV = DATA_DIR / "papers_metadata.csv"
ALLOWED_EXTENSIONS = {"pdf"}
MAX_SEARCH_RESULTS = 20
PASSWORD_SCHEME = "pbkdf2_sha256"
SUPPORTED_LOCALES = ("en", "zh")
SESSION_FILE = DATA_DIR / "active_sessions.json"
SESSION_TIMEOUT_SECONDS = int(os.environ.get("PAPERQUERY_SESSION_TIMEOUT", "600"))
SESSION_TIMEOUT = timedelta(seconds=SESSION_TIMEOUT_SECONDS)
METADATA_FIELDS = ["filename", "title", "authors", "keywords", "organization", "published_at"]

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
    ensure_metadata_file()
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
        filtered = False
        records = gather_paper_records()

        if request.method == "POST":
            query = request.form.get("query", "").strip()
            if not query:
                flash(_("Enter a keyword to search."), "warning")
            else:
                filtered = True
                records = search_papers(query)
                if not records:
                    flash(_("No matching papers found."), "info")

        return render_template("search.html", user=user, query=query, records=records, filtered=filtered)

    @app.route("/upload", methods=["GET", "POST"])
    def upload():
        user = require_login(level=2)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        today = datetime.utcnow().date().isoformat()
        form_data = {
            "title": request.form.get("title", "").strip(),
            "authors": request.form.get("authors", "").strip(),
            "keywords": request.form.get("keywords", "").strip(),
            "organization": request.form.get("organization", "").strip(),
            "published_at": request.form.get("published_at", "").strip() or today,
        }

        if request.method != "POST":
            form_data["published_at"] = today

        if request.method == "POST":
            if not form_data["title"]:
                flash(_("Title is required."), "danger")
                return render_template("upload.html", user=user, form_data=form_data)
            if not form_data["authors"]:
                flash(_("Author field is required."), "danger")
                return render_template("upload.html", user=user, form_data=form_data)

            form_data["authors"] = ", ".join(
                [author.strip() for author in form_data["authors"].split(",") if author.strip()]
            )
            if form_data["keywords"]:
                form_data["keywords"] = ", ".join(
                    [kw.strip() for kw in form_data["keywords"].split(",") if kw.strip()]
                )

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
                        upsert_paper_metadata(
                            filename,
                            {
                                "title": form_data["title"],
                                "authors": form_data["authors"],
                                "keywords": form_data["keywords"],
                                "organization": form_data["organization"],
                                "published_at": form_data["published_at"],
                            },
                        )
                        flash(_("Uploaded %(filename)s.", filename=filename), "success")
                        return redirect(url_for("upload"))

        return render_template("upload.html", user=user, form_data=form_data)

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
                remove_paper_metadata(filename)
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


def ensure_metadata_file() -> None:
    if not METADATA_CSV.exists():
        METADATA_CSV.parent.mkdir(parents=True, exist_ok=True)
        with METADATA_CSV.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=METADATA_FIELDS)
            writer.writeheader()


def load_paper_metadata() -> List[Dict[str, str]]:
    ensure_metadata_file()
    with METADATA_CSV.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        rows: List[Dict[str, str]] = []
        for raw_row in reader:
            normalized = {field: (raw_row.get(field, "") or "").strip() for field in METADATA_FIELDS}
            rows.append(normalized)
        return rows


def save_paper_metadata(rows: List[Dict[str, str]]) -> None:
    ensure_metadata_file()
    with METADATA_CSV.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=METADATA_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in METADATA_FIELDS})


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
