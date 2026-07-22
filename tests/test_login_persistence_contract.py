"""Contracts for remembered-login cookie expiry and route persistence choices."""
import ast
import sys
import textwrap
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

from flask import Flask, flash, redirect, request, session, url_for
from flask.sessions import SecureCookieSession
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

from services.session_cookie import AuthExpirySessionInterface


class AuthExpirySessionInterfaceTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "test-secret"
        self.app.permanent_session_lifetime = timedelta(hours=1)
        self.interface = AuthExpirySessionInterface()

    def test_auth_deadline_overrides_global_permanent_lifetime(self):
        deadline = datetime(2026, 7, 29, tzinfo=timezone.utc)
        flask_session = SecureCookieSession({
            "_permanent": True,
            "auth_expires_at": int(deadline.timestamp()),
        })
        self.assertEqual(
            self.interface.get_expiration_time(self.app, flask_session),
            deadline,
        )

    def test_unrelated_permanent_session_keeps_global_lifetime(self):
        flask_session = SecureCookieSession({"_permanent": True, "ask_owner": "abc"})
        before = datetime.now(timezone.utc)
        expiry = self.interface.get_expiration_time(self.app, flask_session)
        after = datetime.now(timezone.utc)
        self.assertGreaterEqual(expiry, before + timedelta(hours=1))
        self.assertLessEqual(expiry, after + timedelta(hours=1))
        self.assertNotIn("auth_expires_at", flask_session)


class RememberRouteSourceContractTest(unittest.TestCase):
    def test_both_password_login_starters_receive_normalized_remember_flag(self):
        view, source = support.find_function("login")
        view_source = ast.get_source_segment(source, view)
        self.assertIn('request.form.get("remember_me") == "1"', view_source)

        for starter_name in ("start_local_session", "start_ms_session"):
            calls = [
                node
                for node in ast.walk(view)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == starter_name
            ]
            self.assertEqual(len(calls), 1, starter_name)
            remember_keywords = [
                keyword.value
                for keyword in calls[0].keywords
                if keyword.arg == "remember"
            ]
            self.assertEqual(len(remember_keywords), 1, starter_name)
            self.assertIsInstance(remember_keywords[0], ast.Name, starter_name)
            self.assertEqual(remember_keywords[0].id, "remember", starter_name)

    def test_oauth_round_trip_consumes_pending_flag(self):
        login_source = support.source_of("ms_login")
        callback_source = support.source_of("ms_callback")
        self.assertIn("create_oauth_login_attempt", login_source)
        self.assertIn("consume_oauth_login_attempt", callback_source)
        self.assertNotIn('session["ms_remember"]', login_source)
        self.assertIn("remember=remember", callback_source)

    def test_session_creation_failures_return_generic_feedback(self):
        for function_name in ("login", "ms_callback"):
            source = support.source_of(function_name)
            self.assertIn("except SQLAlchemyError", source)
            self.assertIn("Unable to sign in. Please try again.", source)


class MicrosoftNextRoundTripTest(unittest.TestCase):
    class _MsalApp:
        def get_authorization_request_url(self, *_args, **_kwargs):
            return "/microsoft-provider"

        def acquire_token_by_authorization_code(self, *_args, **_kwargs):
            return {"access_token": "access-token"}

    @staticmethod
    def _app(*, profile_complete=True):
        app = Flask(__name__)
        app.secret_key = "test-secret"
        attempts = {}
        app.extensions["oauth_attempts"] = attempts
        app.extensions["last_oauth_state"] = None

        @app.route("/")
        def index():
            return "index"

        @app.route("/login")
        def login():
            return "login"

        @app.route("/profile/setup")
        def profile_setup():
            return "profile"

        def start_ms_session(_record, *, remember=False):
            session.clear()
            session["session_token"] = "fresh-session-token"
            session["remember_seen"] = remember

        def create_oauth_login_attempt(
            state, browser_binding, *, next_url, remember
        ):
            attempts[state] = {
                "binding": browser_binding,
                "next_url": next_url,
                "remember": remember,
            }
            app.extensions["last_oauth_state"] = state

        def consume_oauth_login_attempt(state, browser_binding):
            attempt = attempts.get(state)
            if attempt is None or attempt["binding"] != browser_binding:
                return None
            attempts.pop(state)
            return {
                "next_url": attempt["next_url"],
                "remember": attempt["remember"],
            }

        namespace = {
            "SQLAlchemyError": SQLAlchemyError,
            "MS_REDIRECT_URI": "https://app.example/auth/callback",
            "MS_SCOPES": ["User.Read"],
            "_": lambda message, **values: message % values if values else message,
            "app": app,
            "build_msal_app": lambda: MicrosoftNextRoundTripTest._MsalApp(),
            "create_oauth_login_attempt": create_oauth_login_attempt,
            "consume_oauth_login_attempt": consume_oauth_login_attempt,
            "fetch_ms_profile": lambda _result: {"ms_id": "microsoft-alice"},
            "flash": flash,
            "is_ms_configured": lambda: True,
            "is_profile_complete": lambda _record: profile_complete,
            "get_active_user": lambda: None,
            "_consume_request_limit": lambda *_args, **_kwargs: None,
            "redirect": redirect,
            "request": request,
            "session": session,
            "start_ms_session": start_ms_session,
            "upsert_ms_user": lambda profile: dict(profile),
            "url_for": url_for,
            "urljoin": urljoin,
            "unquote": unquote,
            "urlsplit": urlsplit,
            "urlunsplit": urlunsplit,
            "uuid4": __import__("uuid").uuid4,
        }
        for function_name in (
            "_safe_redirect_path",
            "_is_safe_redirect_target",
            "ms_login",
            "ms_callback",
        ):
            exec(textwrap.dedent(support.source_of(function_name)), namespace)
        app.add_url_rule("/auth/login", view_func=namespace["ms_login"])
        app.add_url_rule("/auth/callback", view_func=namespace["ms_callback"])
        return app

    @staticmethod
    def _state(client):
        return client.application.extensions["last_oauth_state"]

    def test_safe_target_round_trip_survives_session_reset(self):
        client = self._app().test_client()
        response = client.get("/auth/login", query_string={"next": "/dashboard"})
        self.assertEqual(response.location, "/microsoft-provider")
        state = self._state(client)
        self.assertEqual(
            client.application.extensions["oauth_attempts"][state]["next_url"],
            "/dashboard",
        )

        response = client.get(
            "/auth/callback",
            query_string={"state": state, "code": "oauth-code"},
        )

        self.assertEqual(response.location, "/dashboard")
        with client.session_transaction() as browser_session:
            self.assertNotIn("ms_next", browser_session)

    def test_callback_consumes_target_before_failure_paths(self):
        client = self._app().test_client()
        client.get("/auth/login", query_string={"next": "/dashboard"})

        response = client.get(
            "/auth/callback",
            query_string={"state": "wrong-state", "code": "oauth-code"},
        )

        self.assertEqual(response.location, "/login")
        with client.session_transaction() as browser_session:
            self.assertNotIn("ms_next", browser_session)

    def test_missing_explicit_target_retains_protected_route_target(self):
        client = self._app().test_client()
        with client.session_transaction() as browser_session:
            browser_session["next"] = "/protected-route"

        client.get("/auth/login")
        state = self._state(client)
        self.assertEqual(
            client.application.extensions["oauth_attempts"][state]["next_url"],
            "/protected-route",
        )

    def test_incomplete_profile_keeps_only_a_validated_target(self):
        for target, expected_next in (
            ("/protected-route", "/protected-route"),
            ("//evil.example/phish", None),
        ):
            with self.subTest(target=target):
                client = self._app(profile_complete=False).test_client()
                client.get("/auth/login", query_string={"next": target})
                state = self._state(client)

                response = client.get(
                    "/auth/callback",
                    query_string={"state": state, "code": "oauth-code"},
                )

                self.assertEqual(response.location, "/profile/setup")
                with client.session_transaction() as browser_session:
                    self.assertEqual(browser_session.get("next"), expected_next)

    def test_complete_profile_rejects_unsafe_target(self):
        client = self._app().test_client()
        client.get(
            "/auth/login",
            query_string={"next": "//evil.example/phish"},
        )
        state = self._state(client)

        response = client.get(
            "/auth/callback",
            query_string={"state": state, "code": "oauth-code"},
        )

        self.assertEqual(response.location, "/")
