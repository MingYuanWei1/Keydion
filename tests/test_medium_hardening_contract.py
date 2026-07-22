"""Contract + behavioral tests for the Medium-severity hardening cluster:
SEC-08 (session lifetime), SEC-09 (dev-secret fail-fast), SEC-10 (open redirect),
SEC-11 (OAuth state), and HTTP security headers.
"""
import os
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module
from services.session_cookie import AuthExpirySessionInterface


class MediumHardeningContractTest(unittest.TestCase):
    def setUp(self):
        fake_services = types.SimpleNamespace(
            lifecycle=mock.Mock(),
            library=mock.Mock(),
        )
        with (
            mock.patch.object(app_module, "init_db"),
            mock.patch.object(app_module, "configure_rag"),
            mock.patch(
                "services.publishing_wiring.build_publishing_services",
                return_value=fake_services,
            ),
        ):
            self.app = app_module.create_app()

    # --- SEC-08: session cookie lifetime shortened to the server token timeout ---
    def test_session_lifetime_matches_server_timeout(self):
        src = support.source_of("create_app")
        self.assertIn("PERMANENT_SESSION_LIFETIME=timedelta(seconds=SESSION_TIMEOUT_SECONDS)", src)
        self.assertNotIn("timedelta(days=365)", src)

    def test_create_app_installs_auth_expiry_session_interface(self):
        self.assertIsInstance(self.app.session_interface, AuthExpirySessionInterface)

    def test_remembered_session_carries_absolute_cookie_deadline(self):
        src = support.source_of("_start_browser_session")
        self.assertIn("AUTH_EXPIRES_AT_KEY", src)
        self.assertIn('AUTH_EXPIRES_AT_KEY = "auth_expires_at"', support.all_sources())

    # --- SEC-09: fail-fast on the insecure default secret ---
    def test_secret_guard_present_in_source(self):
        src = support.source_of("create_app")
        self.assertIn("PAPERQUERY_ALLOW_DEV_SECRET", src)
        self.assertIn("raise RuntimeError", src)
        self.assertIn("dev-secret-key", src)

    def test_create_app_raises_on_weak_secret_without_optin(self):
        with mock.patch.dict(os.environ, {"PAPERQUERY_SECRET": "", "PAPERQUERY_ALLOW_DEV_SECRET": ""}, clear=False):
            with self.assertRaises(RuntimeError):
                app_module.create_app()

    def test_create_app_raises_on_dev_default_without_optin(self):
        with mock.patch.dict(os.environ, {"PAPERQUERY_SECRET": "dev-secret-key", "PAPERQUERY_ALLOW_DEV_SECRET": ""}, clear=False):
            with self.assertRaises(RuntimeError):
                app_module.create_app()

    # --- SEC-10: open redirect on login `next` ---
    def test_login_uses_safe_redirect_guard(self):
        src = support.source_of("login")
        self.assertIn("_safe_redirect_path(saved_next)", src)

    def test_is_safe_redirect_target_rejects_external(self):
        with self.app.test_request_context("/", base_url="http://localhost/"):
            # legitimate same-origin targets
            self.assertTrue(app_module._is_safe_redirect_target("/dashboard"))
            self.assertTrue(app_module._is_safe_redirect_target("/search?q=x"))
            self.assertTrue(app_module._is_safe_redirect_target("http://localhost/dashboard"))
            # external / bypass vectors must all be rejected
            for bad in (
                "",
                "https://evil.example/phish",
                "//evil.example/phish",
                "////evil.example",          # network-path the browser collapses to a host
                "/\\evil.example",            # backslash that browsers normalize to '//'
                "/\tevil",                    # control char
                "http://localhost\\@evil.example",
                "/%2f%2fevil.example",
                "/%5cevil.example",
                "/%255cevil.example",
                "/%0devil",
            ):
                self.assertFalse(
                    app_module._is_safe_redirect_target(bad),
                    f"should reject redirect target {bad!r}",
                )

    # --- SEC-11: OAuth callback rejects a missing/mismatched state ---
    def test_oauth_callback_requires_state(self):
        src = support.source_of("ms_callback")
        self.assertIn("consume_oauth_login_attempt", src)
        self.assertIn("attempt is None", src)
        self.assertNotIn('session["ms_state"]', support.source_of("ms_login"))

    # --- HTTP security headers on every response ---
    def test_security_headers_present(self):
        client = self.app.test_client()
        resp = client.get("/__no_such_route__")  # 404 still passes through after_request
        self.assertEqual(resp.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(resp.headers.get("X-Frame-Options"), "SAMEORIGIN")
        self.assertIn("Referrer-Policy", resp.headers)
        self.assertIn("Permissions-Policy", resp.headers)
        self.assertIn("Content-Security-Policy-Report-Only", resp.headers)


if __name__ == "__main__":
    unittest.main()
