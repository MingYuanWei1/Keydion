import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

from flask import Flask, session as flask_session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import db
from models import LocalUser, MsUser, SessionModel
from services import auth


class AuthSessionServiceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.addCleanup(self.engine.dispose)
        LocalUser.__table__.create(self.engine)
        MsUser.__table__.create(self.engine)
        SessionModel.__table__.create(self.engine)
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

    def test_token_cannot_be_reused_with_another_identity(self):
        token, _ = auth.register_active_session(
            auth.ACCOUNT_LOCAL, "alice", now=self.now
        )
        wrong_user = {"username": "different", "role": "1", "is_local": True}
        self.assertIsNone(auth.refresh_session(wrong_user, token, now=self.now))
