import os
import unittest
from datetime import datetime, timedelta
from unittest import mock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import db
from models import RateLimitBucketModel
from services import rate_limit


class SharedRateLimitTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.addCleanup(self.engine.dispose)
        RateLimitBucketModel.__table__.create(self.engine)
        self.factory = sessionmaker(bind=self.engine)
        original = db._SESSION_LOCAL
        db._SESSION_LOCAL = self.factory
        self.addCleanup(setattr, db, "_SESSION_LOCAL", original)
        self.env = mock.patch.dict(
            os.environ,
            {"PAPERQUERY_RATE_LIMIT_SECRET": "rate-limit-test-secret"},
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.now = datetime(2026, 7, 23, 0, 0, 0)

    def test_allowance_is_shared_in_database_and_then_denied(self):
        first = rate_limit.consume(
            "ask.ip", "203.0.113.10", limit=2, window_seconds=60, now=self.now
        )
        second = rate_limit.consume(
            "ask.ip", "203.0.113.10", limit=2, window_seconds=60, now=self.now
        )
        third = rate_limit.consume(
            "ask.ip", "203.0.113.10", limit=2, window_seconds=60, now=self.now
        )
        self.assertTrue(first.allowed)
        self.assertTrue(second.allowed)
        self.assertFalse(third.allowed)
        self.assertEqual(third.retry_after, 60)

    def test_exponential_block_and_window_expiry(self):
        self.assertTrue(rate_limit.consume(
            "login.account", "alice@example.test", limit=1,
            window_seconds=60, base_block_seconds=2, now=self.now,
        ).allowed)
        denied = rate_limit.consume(
            "login.account", "alice@example.test", limit=1,
            window_seconds=60, base_block_seconds=2, now=self.now,
        )
        self.assertFalse(denied.allowed)
        self.assertEqual(denied.retry_after, 2)
        reset = rate_limit.consume(
            "login.account", "alice@example.test", limit=1,
            window_seconds=60, base_block_seconds=2,
            now=self.now + timedelta(seconds=61),
        )
        self.assertTrue(reset.allowed)

    def test_raw_identifier_is_not_persisted_and_clear_removes_bucket(self):
        raw = "alice@example.test"
        rate_limit.consume(
            "login.account", raw, limit=3, window_seconds=60, now=self.now
        )
        with db.db_session() as database:
            bucket = database.query(RateLimitBucketModel).one()
            self.assertNotEqual(bucket.key_hash, raw)
            self.assertEqual(len(bucket.key_hash), 64)
        rate_limit.clear("login.account", raw)
        with db.db_session() as database:
            self.assertEqual(database.query(RateLimitBucketModel).count(), 0)


if __name__ == "__main__":
    unittest.main()
