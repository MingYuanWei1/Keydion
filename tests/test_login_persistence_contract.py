"""Contracts for remembered-login cookie expiry and route persistence choices."""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask
from flask.sessions import SecureCookieSession

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
    def test_password_login_passes_normalized_remember_flag(self):
        source = support.source_of("login")
        self.assertIn('request.form.get("remember_me") == "1"', source)
        self.assertIn("remember=remember", source)

    def test_oauth_round_trip_consumes_pending_flag(self):
        login_source = support.source_of("ms_login")
        callback_source = support.source_of("ms_callback")
        self.assertIn('session["ms_remember"]', login_source)
        self.assertIn('session.pop("ms_remember"', callback_source)
        self.assertIn("remember=remember", callback_source)

    def test_session_creation_failures_return_generic_feedback(self):
        for function_name in ("login", "ms_callback"):
            source = support.source_of(function_name)
            self.assertIn("except SQLAlchemyError", source)
            self.assertIn("Unable to sign in. Please try again.", source)
