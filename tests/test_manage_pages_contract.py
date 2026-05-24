import json
import os
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _build_app():
    """Build a fresh Flask app against an in-memory sqlite DB."""
    os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
    os.environ["PAPERQUERY_DATABASE_URL"] = "sqlite:///:memory:"
    import importlib
    import sys
    sys.path.insert(0, str(ROOT))
    import app as app_module
    importlib.reload(app_module)
    app = app_module.create_app()
    return app, app_module


def _login_as(client, app_module, level):
    """Insert a local user at the given level and stash a session token."""
    with app_module.db_session() as db:
        # Remove any prior fixture user/session (tests share the app/DB at class scope).
        existing = db.get(app_module.LocalUser, f"u{level}")
        if existing is not None:
            db.delete(existing)
        db.query(app_module.SessionModel).filter(
            app_module.SessionModel.username == f"u{level}"
        ).delete()
        db.commit()
        u = app_module.LocalUser(
            username=f"u{level}",
            password=app_module.hash_password("pw"),
            role=str(level),
            first_name=f"User",
            last_name=f"{level}",
        )
        db.add(u)
        db.commit()
    # The login form field is named "email"; the route falls back to looking
    # up by username if no email matches, so we pass the username in that field.
    resp = client.post("/login", data={"email": f"u{level}", "password": "pw"},
                       follow_redirects=False)
    # Login redirects on both success AND failure, so verify the session was
    # actually populated (success path sets a session_token).
    assert resp.status_code in (302, 303), f"login failed: {resp.status_code} {resp.data!r}"
    with client.session_transaction() as s:
        assert s.get("session_token"), f"login did not set session_token: session={dict(s)}"


class NewsBulkActionEndpointTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app, cls.app_module = _build_app()

    def _seed_articles(self, ids_and_status):
        with self.app_module.db_session() as db:
            for nid, status in ids_and_status:
                db.add(self.app_module.NewsArticleModel(
                    id=nid, title=f"T-{nid}", status=status,
                    abstract="", body="", author="a", category="c",
                    image_url="", published_at="" if status == "pending" else "2026-01-01",
                ))
            db.commit()

    def test_route_is_registered(self):
        rules = [r.rule for r in self.app.url_map.iter_rules()
                 if r.endpoint == "news_bulk_action"]
        self.assertEqual(rules, ["/dashboard/news/bulk_action"])

    def test_publish_op_sets_status_published_and_stamps_published_at(self):
        self._seed_articles([("art-pub-1", "pending")])
        client = self.app.test_client()
        _login_as(client, self.app_module, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["art-pub-1"], "op": "publish"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["affected"], 1)
        with self.app_module.db_session() as db:
            row = db.query(self.app_module.NewsArticleModel).filter_by(id="art-pub-1").first()
            self.assertEqual(row.status, "published")
            self.assertTrue(row.published_at)  # stamped

    def test_unpublish_op_sets_status_pending(self):
        self._seed_articles([("art-unp-1", "published")])
        client = self.app.test_client()
        _login_as(client, self.app_module, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["art-unp-1"], "op": "unpublish"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        with self.app_module.db_session() as db:
            row = db.query(self.app_module.NewsArticleModel).filter_by(id="art-unp-1").first()
            self.assertEqual(row.status, "pending")

    def test_delete_op_removes_rows(self):
        self._seed_articles([("art-del-1", "published"), ("art-del-2", "pending")])
        client = self.app.test_client()
        _login_as(client, self.app_module, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["art-del-1", "art-del-2"], "op": "delete"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        with self.app_module.db_session() as db:
            remaining = db.query(self.app_module.NewsArticleModel).filter(
                self.app_module.NewsArticleModel.id.in_(["art-del-1", "art-del-2"])).count()
            self.assertEqual(remaining, 0)

    def test_bad_op_returns_400(self):
        client = self.app.test_client()
        _login_as(client, self.app_module, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["x"], "op": "garbage"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 400)

    def test_unauthenticated_returns_401(self):
        client = self.app.test_client()  # no login
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["x"], "op": "publish"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 401)


if __name__ == "__main__":
    unittest.main()
