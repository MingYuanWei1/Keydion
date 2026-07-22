import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

from flask import Flask, session as flask_session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import db
from models import LocalUser, MsUser, OAuthLoginAttemptModel, SessionModel
from services import auth


class AuthSessionServiceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.addCleanup(self.engine.dispose)
        LocalUser.__table__.create(self.engine)
        MsUser.__table__.create(self.engine)
        SessionModel.__table__.create(self.engine)
        OAuthLoginAttemptModel.__table__.create(self.engine)
        self.session_factory = sessionmaker(bind=self.engine)
        original_factory = db._SESSION_LOCAL
        db._SESSION_LOCAL = self.session_factory
        self.addCleanup(setattr, db, "_SESSION_LOCAL", original_factory)
        self.now = datetime(2026, 7, 22, 12, 0, 0)
        with db.db_session() as session:
            session.add(LocalUser(
                username="alice",
                password=auth.hash_password("old-password-1"),
                registration_date=date(2026, 1, 1),
                role="1",
                email="alice@example.test",
            ))
            session.add(LocalUser(
                username="bob",
                password=auth.hash_password("sibling-password-1"),
                registration_date=date(2026, 1, 1),
                role="1",
                email="bob@example.test",
            ))
            session.add(MsUser(
                ms_id="microsoft-alice",
                password=auth.hash_password("old-password-1"),
                role="1",
                email="microsoft-alice@example.test",
                created_at=self.now,
                updated_at=self.now,
            ))
            session.add(MsUser(
                ms_id="microsoft-bob",
                password=auth.hash_password("sibling-password-1"),
                role="1",
                email="microsoft-bob@example.test",
                created_at=self.now,
                updated_at=self.now,
            ))

    def _sibling_token(self, account_type):
        account_id = (
            "bob" if account_type == auth.ACCOUNT_LOCAL else "microsoft-bob"
        )
        token, _ = auth.register_active_session(
            account_type,
            account_id,
            now=self.now,
        )
        return account_id, token

    def _assert_sibling_token_active(self, account_type, account_id, token):
        user = {
            "username": account_id,
            "role": "1",
            "is_local": account_type == auth.ACCOUNT_LOCAL,
        }
        if account_type == auth.ACCOUNT_MICROSOFT:
            user["ms_id"] = account_id
        self.assertIsNotNone(auth.refresh_session(user, token, now=self.now))

    def test_two_tokens_for_one_account_validate_independently(self):
        first, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        second, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        user = {"username": "alice", "role": "1", "is_local": True}

        self.assertNotEqual(first, second)
        self.assertIsNotNone(auth.refresh_session(user, first, now=self.now))
        self.assertIsNotNone(auth.refresh_session(user, second, now=self.now))

    def test_device_logout_releases_only_the_matching_token(self):
        first, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        second, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )

        self.assertTrue(auth.release_active_session(auth.ACCOUNT_LOCAL, "alice", first))
        self.assertFalse(auth.release_active_session(auth.ACCOUNT_LOCAL, "alice", first))
        self.assertIsNotNone(
            auth.refresh_session(
                {"username": "alice", "role": "1", "is_local": True},
                second,
                now=self.now,
            )
        )

    def test_normal_session_expires_at_the_inactivity_boundary(self):
        token, expires_at = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        self.assertIsNone(expires_at)
        user = {"username": "alice", "role": "1", "is_local": True}

        self.assertIsNotNone(
            auth.refresh_session(
                user,
                token,
                now=self.now + auth.SESSION_TIMEOUT - timedelta(microseconds=1),
            )
        )
        self.assertIsNone(
            auth.refresh_session(
                user,
                token,
                now=self.now + auth.SESSION_TIMEOUT * 2,
            )
        )

        boundary_token, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        self.assertIsNone(
            auth.refresh_session(
                user,
                boundary_token,
                now=self.now + auth.SESSION_TIMEOUT,
            )
        )

    def test_remembered_session_has_a_fixed_seven_day_boundary(self):
        token, expires_at = auth.register_active_session(
            auth.ACCOUNT_LOCAL,
            "alice",
            remember=True,
            now=self.now,
        )
        self.assertEqual(expires_at, self.now + timedelta(days=7))
        user = {"username": "alice", "role": "1", "is_local": True}
        self.assertIsNotNone(
            auth.refresh_session(
                user,
                token,
                now=expires_at - timedelta(microseconds=1),
            )
        )
        self.assertIsNone(auth.refresh_session(user, token, now=expires_at))

    def test_browser_and_database_remembered_deadlines_match(self):
        app = Flask(__name__)
        app.secret_key = "test-secret"
        user = {
            "username": "alice",
            "role": "1",
            "registered_at": "2026-01-01",
            "expiry_date": "",
        }
        with app.test_request_context("/"):
            with mock.patch.object(auth, "datetime") as clock:
                clock.utcnow.return_value = self.now
                auth.start_local_session(user, remember=True)
            token = flask_session["session_token"]
            browser_deadline = flask_session["auth_expires_at"]
            self.assertTrue(flask_session.permanent)
        with db.db_session() as database_session:
            database_deadline = database_session.get(SessionModel, token).expires_at
        self.assertEqual(
            browser_deadline,
            int(database_deadline.replace(tzinfo=timezone.utc).timestamp()),
        )

    def test_normal_browser_session_is_not_persistent(self):
        app = Flask(__name__)
        app.secret_key = "test-secret"
        user = {
            "username": "alice",
            "role": "1",
            "registered_at": "2026-01-01",
            "expiry_date": "",
        }
        with app.test_request_context("/"):
            auth.start_local_session(user, remember=False)
            self.assertFalse(flask_session.permanent)
            self.assertNotIn("auth_expires_at", flask_session)

    def test_account_has_no_active_session_cap(self):
        tokens = {
            auth.register_active_session(
                auth.ACCOUNT_LOCAL,
                "alice",
                now=self.now,
            )[0]
            for _ in range(25)
        }
        self.assertEqual(len(tokens), 25)
        with db.db_session() as session:
            self.assertEqual(session.query(SessionModel).count(), 25)

    def test_cookie_identity_cannot_override_token_identity(self):
        token, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        wrong_user = {"username": "different", "role": "1", "is_local": True}
        refreshed = auth.refresh_session(wrong_user, token, now=self.now)
        self.assertEqual(refreshed["username"], "alice")

    def test_browser_session_contains_no_identity_or_profile_fields(self):
        app = Flask(__name__)
        app.secret_key = "test-secret"
        user = {
            "username": "alice",
            "role": "3",
            "registered_at": "2026-01-01",
            "expiry_date": "",
        }
        with app.test_request_context("/"):
            auth.start_local_session(
                user,
                display_name="Alice Example",
                email="alice@example.test",
                remember=True,
            )
            keys = set(flask_session)
            self.assertNotIn("user", keys)
            self.assertNotIn("email", keys)
            self.assertNotIn("role", keys)
            self.assertNotIn("username", keys)
            self.assertEqual(
                keys,
                {"_permanent", "auth_expires_at", "session_token"},
            )

    def test_oauth_attempt_is_browser_bound_one_use_server_state(self):
        auth.create_oauth_login_attempt(
            "state-token",
            "browser-a",
            next_url="/dashboard",
            remember=True,
            now=self.now,
        )
        self.assertIsNone(
            auth.consume_oauth_login_attempt(
                "state-token",
                "browser-b",
                now=self.now,
            )
        )
        self.assertEqual(
            auth.consume_oauth_login_attempt(
                "state-token",
                "browser-a",
                now=self.now,
            ),
            {"next_url": "/dashboard", "remember": True},
        )
        self.assertIsNone(
            auth.consume_oauth_login_attempt(
                "state-token",
                "browser-a",
                now=self.now,
            )
        )

    def test_expired_oauth_attempt_is_rejected_and_purged(self):
        auth.create_oauth_login_attempt(
            "expired-state",
            "browser-a",
            next_url="/dashboard",
            remember=False,
            now=self.now,
        )
        self.assertIsNone(
            auth.consume_oauth_login_attempt(
                "expired-state",
                "browser-a",
                now=self.now + auth.OAUTH_ATTEMPT_LIFETIME,
            )
        )
        with db.db_session() as database:
            self.assertEqual(database.query(OAuthLoginAttemptModel).count(), 0)

    def test_password_update_revokes_every_local_session(self):
        sibling_id, sibling_token = self._sibling_token(auth.ACCOUNT_LOCAL)
        first, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        second, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )

        self.assertTrue(auth.update_local_user_password("alice", "new-password-1"))
        user = {"username": "alice", "role": "1", "is_local": True}
        self.assertIsNone(auth.refresh_session(user, first, now=self.now))
        self.assertIsNone(auth.refresh_session(user, second, now=self.now))
        self._assert_sibling_token_active(
            auth.ACCOUNT_LOCAL, sibling_id, sibling_token
        )

    def test_account_deletion_revokes_every_local_session(self):
        sibling_id, sibling_token = self._sibling_token(auth.ACCOUNT_LOCAL)
        first, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        second, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )

        self.assertTrue(auth.delete_local_user("alice"))
        with db.db_session() as session:
            self.assertEqual(session.query(SessionModel).count(), 1)
        user = {"username": "alice", "role": "1", "is_local": True}
        self.assertIsNone(auth.refresh_session(user, first, now=self.now))
        self.assertIsNone(auth.refresh_session(user, second, now=self.now))
        self._assert_sibling_token_active(
            auth.ACCOUNT_LOCAL, sibling_id, sibling_token
        )

    def test_expired_local_account_revokes_all_tokens(self):
        sibling_id, sibling_token = self._sibling_token(auth.ACCOUNT_LOCAL)
        first, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        second, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        with db.db_session() as session:
            session.get(LocalUser, "alice").expiry_date = self.now.date() - timedelta(days=1)

        user = {"username": "alice", "role": "1", "is_local": True}
        self.assertIsNone(auth.refresh_session(user, first, now=self.now))
        self.assertIsNone(auth.refresh_session(user, second, now=self.now))
        with db.db_session() as session:
            self.assertEqual(session.query(SessionModel).count(), 1)
        self._assert_sibling_token_active(
            auth.ACCOUNT_LOCAL, sibling_id, sibling_token
        )

    def test_explicit_account_revocation_preserves_sibling_token(self):
        sibling_id, sibling_token = self._sibling_token(auth.ACCOUNT_LOCAL)
        first, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        second, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )

        self.assertEqual(
            auth.revoke_account_sessions(auth.ACCOUNT_LOCAL, "alice"),
            2,
        )

        user = {"username": "alice", "role": "1", "is_local": True}
        self.assertIsNone(auth.refresh_session(user, first, now=self.now))
        self.assertIsNone(auth.refresh_session(user, second, now=self.now))
        self._assert_sibling_token_active(
            auth.ACCOUNT_LOCAL, sibling_id, sibling_token
        )

    def test_current_database_role_replaces_cookie_role(self):
        token, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        with db.db_session() as session:
            session.get(LocalUser, "alice").role = "3"

        refreshed = auth.refresh_session(
            {"username": "alice", "role": "1", "is_local": True},
            token,
            now=self.now,
        )
        self.assertEqual(refreshed["role"], "3")

    def test_password_update_revokes_every_microsoft_session(self):
        sibling_id, sibling_token = self._sibling_token(auth.ACCOUNT_MICROSOFT)
        first, _ = auth.register_active_session(
            auth.ACCOUNT_MICROSOFT, "microsoft-alice", now=self.now
        )
        second, _ = auth.register_active_session(
            auth.ACCOUNT_MICROSOFT, "microsoft-alice", now=self.now
        )

        self.assertTrue(auth.update_ms_user_password("microsoft-alice", "new-password-1"))
        user = {"ms_id": "microsoft-alice", "role": "1", "is_local": False}
        self.assertIsNone(auth.refresh_session(user, first, now=self.now))
        self.assertIsNone(auth.refresh_session(user, second, now=self.now))
        self._assert_sibling_token_active(
            auth.ACCOUNT_MICROSOFT, sibling_id, sibling_token
        )

    def test_account_deletion_revokes_every_microsoft_session(self):
        sibling_id, sibling_token = self._sibling_token(auth.ACCOUNT_MICROSOFT)
        first, _ = auth.register_active_session(
            auth.ACCOUNT_MICROSOFT, "microsoft-alice", now=self.now
        )
        second, _ = auth.register_active_session(
            auth.ACCOUNT_MICROSOFT, "microsoft-alice", now=self.now
        )

        self.assertTrue(auth.delete_ms_user("microsoft-alice"))
        with db.db_session() as session:
            self.assertEqual(session.query(SessionModel).count(), 1)
        user = {"ms_id": "microsoft-alice", "role": "1", "is_local": False}
        self.assertIsNone(auth.refresh_session(user, first, now=self.now))
        self.assertIsNone(auth.refresh_session(user, second, now=self.now))
        self._assert_sibling_token_active(
            auth.ACCOUNT_MICROSOFT, sibling_id, sibling_token
        )
