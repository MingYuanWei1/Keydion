from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Dict
from uuid import uuid4
from urllib.parse import urljoin, urlparse

from flask import (
    Flask,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_babel import Babel, gettext as _, get_locale
from werkzeug.middleware.proxy_fix import ProxyFix
import llm_client
import rag_index  # noqa: F401  -- gunicorn post_fork pre-warms app_module.rag_index
import web_search  # noqa: F401  -- tests patch app_module.web_search.web_search

# ---- Back-compat re-exports (split refactor) -------------------------------
# Names moved out of app.py during the split. Kept importable as app.<name>
# for the contract tests, tools/, and not-yet-moved code in this file.
from config import (  # noqa: F401
    BASE_DIR, DATA_DIR, PAPERS_DIR, LOCAL_USER_FIELDS, NEWS_FIELDS, GUIDE_FIELDS,
    GUIDE_CATEGORIES_JSON, _DEFAULT_GUIDE_CATEGORIES, _DEFAULT_NEWS_CATEGORIES,
    CATEGORIES_JSON, JOURNALS_JSON, _DEFAULT_PAPER_CATEGORIES,
    _EE_SUBJECTS_PATH, _EE_SUBJECTS_DEFAULT, JOURNAL_COVERS_DIR, ALLOWED_EXTENSIONS,
    ALLOWED_IMAGE_EXTENSIONS, NEWS_IMAGES_DIR, GUIDE_IMAGES_DIR, GUIDE_IMAGE_MAX_BYTES,
    RESOURCES_DIR, RESOURCE_ALLOWED_EXTENSIONS, PREVIEWABLE_MIMES, RESOURCE_MAX_BYTES,
    MAX_SEARCH_RESULTS, MIN_SEMANTIC_QUERY_LEN, PASSWORD_SCHEME, SUPPORTED_LOCALES,
    SESSION_TIMEOUT_SECONDS, OPEN_ACCESS, SESSION_TIMEOUT, METADATA_FIELDS,
    MS_USER_FIELDS, MS_CLIENT_ID, MS_CLIENT_SECRET, MS_REDIRECT_URI, MS_AUTHORITY,
    MS_SCOPES, MS_GRAPH_ME_URL, ROLE_OPTIONS, _MISSING_FIELD_MESSAGES,
    IB_EE_CRITERIA_DEFS, CP_GLOBAL_CONTEXTS, CP_ACTION_TYPES, CP_CRITERIA_DEFS,
    ROLE_LABELS, LANGUAGE_NAMES,
)
import db
# Reload shim: several tests importlib.reload() this module with a swapped
# PAPERQUERY_DATABASE_URL to get an isolated DB. Before the split these
# globals lived here, so a reload reset them; replicate that by re-reading
# the URL and dropping the engine so the next init_db() rebuilds it.
# (No-op on first import: DB_URL is unchanged and _ENGINE is already None.
# Grep tests/ for importlib.reload before removing.)
db.DB_URL = os.environ.get("PAPERQUERY_DATABASE_URL")
db._ENGINE = None
db._SESSION_LOCAL = None
from db import BASE, DB_URL, db_session, get_engine  # noqa: F401
from models import (  # noqa: F401
    LocalUser, MsUser, JournalModel, PaperMetadataModel, PaperChunkModel,
    ConversationModel, ChatMessageModel, AttachmentChunkModel, NewsArticleModel,
    GuideModel, ResourceNode, SubmissionModel, SessionModel, init_db,
)
from routes.shared import is_partial_request  # noqa: F401
from services.auth import (  # noqa: F401
    load_users, get_local_user, get_local_user_by_email, hash_password,
    create_local_user, update_local_user_role, update_local_user_password,
    delete_local_user, authenticate, load_active_local_user, start_local_session,
    start_ms_session, is_ms_configured, build_msal_app, fetch_ms_profile,
    build_session_user, load_ms_users, get_ms_user, get_ms_user_by_email,
    update_ms_user_password, upsert_ms_user, update_ms_user, update_ms_user_role,
    delete_ms_user, is_profile_complete, get_active_user, require_login,
    verify_password, load_sessions, is_session_expired, ensure_login_available,
    register_active_session, release_active_session, force_release_session,
    refresh_session,
)
from services.resources import (  # noqa: F401
    _can_view_node, _resource_viewer_role,
    get_resource_node, effective_min_role, resource_breadcrumbs,
    load_resource_children, load_resource_folder_tree,
    create_resource_folder, save_resource_file, update_resource_node,
    move_resource_node, delete_resource_node, slugify_resource_name,
    resource_name_is_valid, resource_slug_conflict, resolve_resource_path,
    resource_breadcrumb_paths,
)
from services.guides import (  # noqa: F401
    _sanitize_guide_html, _read_guide_form, _group_guides_for_index,
)
from services.news import (  # noqa: F401
    load_news_articles,
)
from services.journals import (  # noqa: F401
    load_journals,
    get_journal_names, get_journal_id_map,
    get_recent_journals,
)
from services.papers import (  # noqa: F401
    load_paper_metadata, save_paper_metadata,
    load_paper_categories, save_paper_categories,
    _get_ee_subjects_list, _build_safe_paper_filename,
    build_ib_ee_data_from_form, build_cp_data_from_form,
    parse_ib_ee_data_for_form, parse_cp_data_for_form,
    _is_ee_paper, _is_cp_paper, _matches_ee_subject, _matches_cp_context,
    allowed_file, extract_pdf_text, extract_text_from_upload,
    set_pdf_metadata, build_preview_pdf,
)
from services.search import (  # noqa: F401
    _query_in_metadata, _fulltext_index, search_papers,
    _order_hybrid_filenames, _hybrid_search_records,
)
from services.submissions import (  # noqa: F401
    _load_submissions, _write_submissions, _save_submission,
    _get_submission, _update_submission,
)
from services.ai import (  # noqa: F401
    configure_rag, _index_ocr_langs, _rag_paper_text,
    _lib_full_text, _lib_search, _lib_paper_meta, _lib_paper_url,
    _build_library_deps, _build_agentic_ask_prompt, _tool_status_text,
    _build_ask_prompt, _dedupe_hits_by_paper, _cited_numbers, _filter_cited,
    _attachment_grounding, _attachment_filenames, MAX_TOOL_ROUNDS,
)


babel = Babel()


def select_locale() -> str:
    preferred = session.get("language")
    if preferred in SUPPORTED_LOCALES:
        return preferred
    match = request.accept_languages.best_match(SUPPORTED_LOCALES)
    return match or SUPPORTED_LOCALES[0]


def _is_safe_redirect_target(target: str) -> bool:
    """SEC-10: allow only same-host / relative redirect targets for login `next`.

    Reject backslashes and network-path references ('//host', '////host', '/\\host')
    that browsers normalize to a foreign origin, plus control characters — these
    pass a naive netloc check but escape the origin when emitted as a raw Location.
    """
    if not target:
        return False
    target = target.strip()
    if not target or "\\" in target or target.startswith("//"):
        return False
    if any(ord(c) < 0x20 for c in target):
        return False
    host_url = request.host_url
    test = urlparse(urljoin(host_url, target))
    return test.scheme in ("http", "https") and test.netloc == urlparse(host_url).netloc


def create_app() -> Flask:
    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
    # SEC-09: never silently run with the insecure default secret. Local dev opts
    # in via PAPERQUERY_ALLOW_DEV_SECRET=1 (set by start_local.sh).
    secret = os.environ.get("PAPERQUERY_SECRET", "")
    allow_dev_secret = os.environ.get("PAPERQUERY_ALLOW_DEV_SECRET", "").strip().lower() in ("1", "true", "yes", "on")
    if (not secret or secret == "dev-secret-key") and not allow_dev_secret:
        raise RuntimeError(
            "PAPERQUERY_SECRET is unset or the insecure 'dev-secret-key' default. "
            "Set a strong PAPERQUERY_SECRET, or set PAPERQUERY_ALLOW_DEV_SECRET=1 for local development."
        )
    app.config.update(
        SECRET_KEY=secret or "dev-secret-key",
        PERMANENT_SESSION_LIFETIME=timedelta(seconds=SESSION_TIMEOUT_SECONDS),
        UPLOAD_FOLDER=str(PAPERS_DIR),
        BABEL_DEFAULT_LOCALE="en",
        BABEL_DEFAULT_TIMEZONE="UTC",
        BABEL_SUPPORTED_LOCALES=",".join(SUPPORTED_LOCALES),
        MAX_CONTENT_LENGTH=int(os.environ.get("PAPERQUERY_MAX_UPLOAD_MB", "50")) * 1024 * 1024,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SECURE=os.environ.get("PAPERQUERY_COOKIE_SECURE", "1").strip().lower() in ("1", "true", "yes", "on"),
    )

    @app.after_request
    def _set_security_headers(resp):
        # Defense-in-depth response headers. CSP is Report-Only so existing inline
        # handlers/fonts keep working; tune via browser console reports before enforcing.
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        resp.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
        resp.headers.setdefault(
            "Content-Security-Policy-Report-Only",
            "default-src 'self'; script-src 'self'; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com; img-src 'self' data: https://images.unsplash.com; "
            "connect-src 'self'; frame-ancestors 'self'; base-uri 'self'; form-action 'self'"
        )
        if request.is_secure:
            resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        return resp

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RESOURCES_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    configure_rag()
    from services import publishing_wiring
    app.extensions["publishing_lifecycle"] = (
        publishing_wiring.build_publishing_lifecycle()
    )
    babel.init_app(app, locale_selector=select_locale)

    from flask_wtf import CSRFProtect
    CSRFProtect(app)

    @app.context_processor
    def _inject_csrf():
        from flask_wtf.csrf import generate_csrf
        return {"csrf_token": generate_csrf}

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
        return render_template("landing.html", ms_enabled=is_ms_configured(),
                               latest_news=latest_news, recent_journals=get_recent_journals(4))

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
                    return redirect(saved_next if _is_safe_redirect_target(saved_next) else url_for("index"))
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
                        return redirect(saved_next if _is_safe_redirect_target(saved_next) else url_for("index"))

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
        expected_state = session.pop("ms_state", None)
        if not expected_state or request.args.get("state") != expected_state:
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
        return redirect(next_url if _is_safe_redirect_target(next_url) else url_for("index"))

    def _do_logout():
        language = session.get("language")
        username = session.get("user", {}).get("username", "")
        # 强制释放会话，不检查 token 匹配
        if username:
            force_release_session(username)
        session.clear()
        if language:
            session["language"] = language

    @app.route("/logout", methods=["POST"])
    def logout():
        _do_logout()
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
            _do_logout()
            return redirect(url_for("login"))

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
                return redirect(next_url if _is_safe_redirect_target(next_url) else url_for("index"))

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
            user=user,
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

    from routes import register_all
    register_all(app)

    return app


app = create_app()


if __name__ == "__main__":
    import os as _os
    app.run(
        host=_os.environ.get("HOST", "127.0.0.1"),
        port=int(_os.environ.get("PORT", "5000")),
        debug=_os.environ.get("FLASK_DEBUG", "").strip().lower() in ("1", "true", "yes", "on"),
    )
