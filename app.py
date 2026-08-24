from __future__ import annotations

import json
import os
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Dict
from uuid import uuid4
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

from flask import (
    Flask,
    flash,
    g,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_babel import Babel, gettext as _, get_locale
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.middleware.proxy_fix import ProxyFix
import llm_client
from config import (
    DATA_DIR, PAPERS_DIR, RESOURCES_DIR, SUPPORTED_LOCALES,
    SESSION_TIMEOUT_SECONDS, OPEN_ACCESS, MS_REDIRECT_URI, MS_SCOPES,
    ROLE_OPTIONS, ROLE_LABELS, LANGUAGE_NAMES, MS_STEP_UP_WINDOW_SECONDS,
)
from db import db_session
from models import (
    PaperMetadataModel, NewsArticleModel, SubmissionModel, init_db,
)
from routes.shared import is_partial_request
from services.auth import (
    load_users, get_local_user, get_local_user_by_email,
    create_local_user, update_local_user_role, update_local_user_password,
    delete_local_user, authenticate, start_local_session,
    start_ms_session, is_ms_configured, build_msal_app, fetch_ms_profile,
    load_ms_users, get_ms_user, get_ms_user_by_email,
    update_ms_user_password, upsert_ms_user, update_ms_user, update_ms_user_role,
    delete_ms_user, is_profile_complete, get_active_user, require_login,
    verify_password, password_validation_error, create_oauth_login_attempt,
    consume_oauth_login_attempt, release_active_session,
    MS_RECENT_AUTH_SESSION_KEY,
)
from services.session_cookie import AuthExpirySessionInterface
from services.rate_limit import clear as clear_rate_limit
from services.rate_limit import consume as consume_rate_limit
from services.news import (
    load_news_articles,
    news_body_html,
)
from services.journals import get_recent_journals
from services.ai import configure_rag


babel = Babel()


def select_locale() -> str:
    preferred = session.get("language")
    if preferred in SUPPORTED_LOCALES:
        return preferred
    match = request.accept_languages.best_match(SUPPORTED_LOCALES)
    return match or SUPPORTED_LOCALES[0]


def _safe_redirect_path(target: str) -> str | None:
    """Return a normalized same-origin relative path/query, or ``None``.

    Validation examines repeated percent-decoding so encoded slashes,
    backslashes, and controls cannot become a network-path redirect after a
    proxy or browser performs another normalization pass.
    """
    if not isinstance(target, str):
        return None
    candidate = target.strip()
    if not candidate:
        return None
    decoded = candidate
    for _ in range(3):
        if (
            not decoded
            or decoded.startswith("//")
            or "\\" in decoded
            or any(ord(character) < 0x20 or ord(character) == 0x7F for character in decoded)
        ):
            return None
        next_decoded = unquote(decoded)
        if next_decoded == decoded:
            break
        decoded = next_decoded
    host = urlsplit(request.host_url)
    resolved = urlsplit(urljoin(request.host_url, candidate))
    if resolved.scheme not in ("http", "https") or resolved.netloc != host.netloc:
        return None
    path = resolved.path or "/"
    decoded_path = unquote(path)
    if not path.startswith("/") or path.startswith("//") or decoded_path.startswith("//"):
        return None
    if "\\" in path or "\\" in decoded_path:
        return None
    return urlunsplit(("", "", path, resolved.query, ""))


def _is_safe_redirect_target(target: str) -> bool:
    return _safe_redirect_path(target) is not None


def create_app() -> Flask:
    app = Flask(__name__)
    app.session_interface = AuthExpirySessionInterface()
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
        if request.path == "/auth/callback":
            resp.headers["Referrer-Policy"] = "no-referrer"
        else:
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
    publishing_services = publishing_wiring.build_publishing_services()
    app.extensions["publishing_services"] = publishing_services
    app.extensions["publishing_lifecycle"] = publishing_services.lifecycle
    app.extensions["paper_library"] = publishing_services.library
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
            "current_user": get_active_user(),
        }

    app.add_template_filter(news_body_html)

    @app.template_filter("from_json")
    def from_json_filter(value):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None

    def _too_many_requests(retry_after: int):
        response = make_response(str(_("Too many requests — please slow down.")), 429)
        response.headers["Retry-After"] = str(max(int(retry_after), 1))
        return response

    def _consume_request_limit(scope: str, key: str, **policy):
        decision = consume_rate_limit(scope, key, **policy)
        return None if decision.allowed else _too_many_requests(decision.retry_after)

    def _ms_recent_login_marker_valid(ms_id: str) -> bool:
        """Fresh-Microsoft-login proof for step-up actions (age-bounded; the
        caller consumes the marker when the sensitive change is applied)."""
        raw = str(session.get(MS_RECENT_AUTH_SESSION_KEY, ""))
        marker_ms_id, _sep, stamp_raw = raw.rpartition(":")
        if not marker_ms_id or marker_ms_id != ms_id:
            return False
        try:
            stamp = int(stamp_raw)
        except (TypeError, ValueError):
            return False
        age = int(datetime.now(timezone.utc).timestamp()) - stamp
        return 0 <= age <= MS_STEP_UP_WINDOW_SECONDS

    @app.route("/")
    def index():
        user = get_active_user()
        latest_news = load_news_articles(status="published")[:4]
        return render_template("landing.html", ms_enabled=is_ms_configured(),
                               latest_news=latest_news,
                               recent_journals=get_recent_journals(
                                   4,
                                   library=app.extensions["paper_library"],
                               ))

    @app.route("/faq")
    def faq():
        return render_template("FAQ.html")

    @app.route("/login", methods=["GET", "POST"])
    def login():
        # Redirect already-logged-in users away
        if get_active_user() is not None:
            return redirect(url_for("index"))
        if request.method == "POST":
            email = request.form.get("email", "").strip()
            password = request.form.get("password", "").strip()
            remember = request.form.get("remember_me") == "1"
            login_ip_key = request.remote_addr or "unknown"
            limited = _consume_request_limit(
                "login.ip",
                login_ip_key,
                limit=20,
                window_seconds=300,
                base_block_seconds=2,
                max_block_seconds=900,
            )
            if limited is not None:
                return limited

            # 1. Try local user by email
            user_record = get_local_user_by_email(email)
            if not user_record:
                # 2. Try local user by username (for admin accounts like "admin")
                user_record = get_local_user(email)
            ms_record = None if user_record else get_ms_user_by_email(email)

            # Throttle per RESOLVED account identity, not per submitted text:
            # the documented production collation (utf8mb4_unicode_ci) matches
            # case/accent variants, so identifiers that resolve to the same
            # account must share one bucket (security finding: collation-
            # equivalent identifiers bypassed the per-account throttle).
            # Unknown identifiers fall back to the NFKC-casefolded text.
            if user_record:
                login_account_key = f"local:{user_record.get('username', '')}"
            elif ms_record:
                login_account_key = f"ms:{ms_record.get('ms_id', '')}"
            else:
                login_account_key = (
                    unicodedata.normalize("NFKC", email).casefold() or "empty"
                )
            limited = _consume_request_limit(
                "login.account",
                login_account_key,
                limit=8,
                window_seconds=300,
                base_block_seconds=2,
                max_block_seconds=900,
            )
            if limited is not None:
                return limited

            if user_record:
                user = authenticate(user_record.get("username", ""), password)
                if user:
                    display = user_record.get("first_name", "") or user_record.get("email", "") or user["username"]
                    saved_next = session.get("next") or request.form.get("next", "")
                    try:
                        start_local_session(
                            user,
                            display_name=display,
                            email=user_record.get("email", ""),
                            remember=remember,
                        )
                    except SQLAlchemyError:
                        app.logger.exception("Unable to create local login session")
                        flash(_("Unable to sign in. Please try again."), "danger")
                        return redirect(url_for("index", login=1))
                    flash(_("Welcome back, %(username)s!", username=display), "success")
                    # Only the account bucket is cleared on success: a login
                    # with any one valid credential must not reset the shared
                    # IP failure state, or a low-role attacker could launder
                    # guesses against other accounts (security finding).
                    clear_rate_limit("login.account", login_account_key)
                    return redirect(_safe_redirect_path(saved_next) or url_for("index"))
            elif ms_record and ms_record.get("password"):
                # 3. MS user with a password set
                if verify_password(password, ms_record["password"]):
                    saved_next = session.get("next") or request.form.get("next", "")
                    try:
                        start_ms_session(ms_record, remember=remember)
                    except SQLAlchemyError:
                        app.logger.exception("Unable to create Microsoft password login session")
                        flash(_("Unable to sign in. Please try again."), "danger")
                        return redirect(url_for("index", login=1))
                    display = ms_record.get("display_name", "") or ms_record.get("email", "")
                    flash(_("Welcome back, %(username)s!", username=display), "success")
                    clear_rate_limit("login.account", login_account_key)
                    return redirect(_safe_redirect_path(saved_next) or url_for("index"))

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
        if get_active_user() is not None:
            return redirect(url_for("index"))
        if not is_ms_configured():
            flash(_("Microsoft sign-in is not configured. Please contact the administrator."), "danger")
            return redirect(url_for("login"))
        limited = _consume_request_limit(
            "oauth.start.ip",
            request.remote_addr or "unknown",
            limit=10,
            window_seconds=300,
            base_block_seconds=5,
            max_block_seconds=900,
        )
        if limited is not None:
            return limited
        state = uuid4().hex
        requested_next = request.args.get("next")
        if requested_next is None:
            requested_next = session.get("next", "")
        from flask_wtf.csrf import generate_csrf
        generate_csrf()
        create_oauth_login_attempt(
            state,
            str(session.get("csrf_token") or ""),
            next_url=requested_next or "",
            remember=request.args.get("remember_me") == "1",
        )
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
        limited = _consume_request_limit(
            "oauth.callback.ip",
            request.remote_addr or "unknown",
            limit=20,
            window_seconds=300,
            base_block_seconds=5,
            max_block_seconds=900,
        )
        if limited is not None:
            return limited
        from flask_wtf.csrf import generate_csrf
        generate_csrf()
        attempt = consume_oauth_login_attempt(
            request.args.get("state", ""),
            str(session.get("csrf_token") or ""),
        )
        if attempt is None:
            flash(_("Login session expired. Please try again."), "warning")
            return redirect(url_for("login"))
        saved_next = attempt["next_url"]
        remember = bool(attempt["remember"])

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

        user_record = upsert_ms_user(profile)
        try:
            start_ms_session(user_record, remember=remember)
        except SQLAlchemyError:
            app.logger.exception("Unable to create Microsoft OAuth login session")
            flash(_("Unable to sign in. Please try again."), "danger")
            return redirect(url_for("login"))

        if not is_profile_complete(user_record):
            safe_next = _safe_redirect_path(saved_next)
            if safe_next:
                session["next"] = safe_next
            return redirect(url_for("profile_setup"))
        return redirect(
            _safe_redirect_path(saved_next) or url_for("index")
        )

    def _do_logout():
        language = session.get("language")
        token = session.get("session_token", "")
        release_active_session(token)
        session.clear()
        g.keydion_current_user = None
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
                    g.pop("keydion_current_user", None)
                flash(_("Profile saved successfully."), "success")
                next_url = session.pop("next", None)
                return redirect(_safe_redirect_path(next_url) or url_for("index"))

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
            # Enrolling a FIRST local password on a Microsoft-only account is a
            # step-up action: it converts a session into a durable credential.
            # Require proof of a recent Microsoft login so a stolen or
            # script-controlled session cannot enroll its own password
            # (security finding: password enrollment without reauthentication).
            needs_ms_step_up = is_ms_user and not has_password
            if needs_ms_step_up:
                if not is_ms_configured():
                    flash(_("Password setup is not available. Please contact the administrator."), "danger")
                    return redirect(url_for("index"))
                if not _ms_recent_login_marker_valid(ms_id):
                    flash(_("For your security, sign in with Microsoft again before setting your first password."), "warning")
                    session["next"] = url_for("change_password")
                    return redirect(url_for("ms_login"))

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

            password_error = password_validation_error(new_password)
            if password_error == "required":
                flash(_("Please enter a new password."), "warning")
                return redirect(url_for("change_password"))
            if new_password != confirm_password:
                flash(_("Passwords do not match."), "warning")
                return redirect(url_for("change_password"))
            if password_error == "too_short":
                flash(_("Password must be at least 6 characters."), "warning")
                return redirect(url_for("change_password"))

            if password_error in ("missing_letter", "missing_digit"):
                flash(_("Password must contain both letters and numbers."), "warning")
                return redirect(url_for("change_password"))

            if has_password and new_password == current_password:
                flash(
                    _("New password must be different from your current password."),
                    "warning",
                )
                return redirect(url_for("change_password"))

            if needs_ms_step_up:
                # Consume the single-use marker at the moment the durable
                # credential is actually installed.
                session.pop(MS_RECENT_AUTH_SESSION_KEY, None)
            if is_ms_user:
                success = update_ms_user_password(ms_id, new_password)
            else:
                success = update_local_user_password(user.get("username", ""), new_password)

            if success:
                language = session.get("language")
                session.clear()
                if language:
                    session["language"] = language
                flash(_("Password updated. Please sign in again."), "success")
                return redirect(url_for("index", login=1))
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
        if not username or password_validation_error(password) is not None:
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
        if password_validation_error(new_password) is not None:
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
        if password_validation_error(new_password) is not None:
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

    @app.route("/set-language/<locale_code>", methods=["POST"])
    def set_language(locale_code: str):
        if locale_code not in SUPPORTED_LOCALES:
            flash(_("Language not supported."), "warning")
        else:
            session["language"] = locale_code
        current = get_active_user()
        next_url = _safe_redirect_path(request.form.get("next", ""))
        if next_url is None and request.referrer:
            next_url = _safe_redirect_path(request.referrer)
        if next_url is None:
            destination = "dashboard" if current is not None else "login"
            next_url = url_for(destination)
        return redirect(next_url)

    from routes import register_all
    register_all(app)

    return app


if __name__ == "__main__":
    import os as _os
    create_app().run(
        host=_os.environ.get("HOST", "127.0.0.1"),
        port=int(_os.environ.get("PORT", "5000")),
        debug=_os.environ.get("FLASK_DEBUG", "").strip().lower() in ("1", "true", "yes", "on"),
    )
