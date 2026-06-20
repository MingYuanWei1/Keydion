"""Contract + behavioral tests for the Medium-severity hardening cluster:
SEC-08 (session lifetime), SEC-09 (dev-secret fail-fast), SEC-10 (open redirect),
SEC-11 (OAuth state), and HTTP security headers.
"""
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
import app as app_module


class MediumHardeningContractTest(unittest.TestCase):
    # --- SEC-08: session cookie lifetime shortened to the server token timeout ---
    def test_session_lifetime_matches_server_timeout(self):
        src = support.source_of("create_app")
        self.assertIn("PERMANENT_SESSION_LIFETIME=timedelta(seconds=SESSION_TIMEOUT_SECONDS)", src)
        self.assertNotIn("timedelta(days=365)", src)

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
        self.assertIn("_is_safe_redirect_target(saved_next)", src)

    def test_is_safe_redirect_target_rejects_external(self):
        with app_module.app.test_request_context("/", base_url="http://localhost/"):
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
            ):
                self.assertFalse(
                    app_module._is_safe_redirect_target(bad),
                    f"should reject redirect target {bad!r}",
                )

    # --- SEC-11: OAuth callback rejects a missing/mismatched state ---
    def test_oauth_callback_requires_state(self):
        src = support.source_of("ms_callback")
        self.assertIn('session.pop("ms_state"', src)
        self.assertIn("not expected_state", src)

    # --- HTTP security headers on every response ---
    def test_security_headers_present(self):
        client = app_module.app.test_client()
        resp = client.get("/__no_such_route__")  # 404 still passes through after_request
        self.assertEqual(resp.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(resp.headers.get("X-Frame-Options"), "DENY")
        self.assertIn("Referrer-Policy", resp.headers)
        self.assertIn("Permissions-Policy", resp.headers)
        self.assertIn("Content-Security-Policy-Report-Only", resp.headers)


if __name__ == "__main__":
    unittest.main()
