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

    def test_publish_op_does_not_restamp_already_published_article(self):
        # An article that's already published keeps its original published_at
        # when re-published via bulk action (idempotent re-publish).
        original_ts = "2024-01-15 09:00"
        with self.app_module.db_session() as db:
            db.add(self.app_module.NewsArticleModel(
                id="art-already-pub", title="T", status="published",
                abstract="", body="", author="a", category="c",
                image_url="", published_at=original_ts,
            ))
            db.commit()
        client = self.app.test_client()
        _login_as(client, self.app_module, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["art-already-pub"], "op": "publish"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        with self.app_module.db_session() as db:
            row = db.query(self.app_module.NewsArticleModel).filter_by(id="art-already-pub").first()
            self.assertEqual(row.published_at, original_ts)
            self.assertEqual(row.status, "published")


class GuideReorderEndpointTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app, cls.app_module = _build_app()

    def _seed_guides(self, rows):
        ids = []
        with self.app_module.db_session() as db:
            for slug, category, sort_order in rows:
                g = self.app_module.GuideModel(
                    slug=slug, category=category, sort_order=sort_order,
                    published=False, title_en=slug, title_zh="", summary_en="",
                    summary_zh="", body_en="", body_zh="", created_at="", updated_at="",
                )
                db.add(g)
            db.commit()
            for slug, _c, _s in rows:
                g = db.query(self.app_module.GuideModel).filter_by(slug=slug).first()
                ids.append(g.id)
        return ids

    def test_route_is_registered(self):
        rules = [r.rule for r in self.app.url_map.iter_rules()
                 if r.endpoint == "admin_guides_reorder"]
        self.assertEqual(rules, ["/dashboard/admin/guides/reorder"])

    def test_reorder_updates_sort_order_and_category(self):
        ids = self._seed_guides([("g-a", "cat1", 10), ("g-b", "cat1", 20)])
        client = self.app.test_client()
        _login_as(client, self.app_module, level=3)
        payload = {"items": [
            {"id": ids[0], "sort_order": 2, "category": "cat2"},
            {"id": ids[1], "sort_order": 1, "category": "cat1"},
        ]}
        resp = client.post("/dashboard/admin/guides/reorder",
                           data=json.dumps(payload),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json()["ok"])
        with self.app_module.db_session() as db:
            a = db.query(self.app_module.GuideModel).filter_by(id=ids[0]).first()
            b = db.query(self.app_module.GuideModel).filter_by(id=ids[1]).first()
            self.assertEqual(a.sort_order, 2)
            self.assertEqual(a.category, "cat2")
            self.assertEqual(b.sort_order, 1)
            self.assertEqual(b.category, "cat1")

    def test_unknown_id_is_skipped_silently(self):
        client = self.app.test_client()
        _login_as(client, self.app_module, level=3)
        resp = client.post("/dashboard/admin/guides/reorder",
                           data=json.dumps({"items": [{"id": 999999, "sort_order": 1}]}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json()["ok"])

    def test_level_2_user_is_forbidden(self):
        client = self.app.test_client()
        _login_as(client, self.app_module, level=2)
        resp = client.post("/dashboard/admin/guides/reorder",
                           data=json.dumps({"items": []}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 401)


class GuideTogglePublishedEndpointTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app, cls.app_module = _build_app()

    def _seed_guide(self, slug, published):
        with self.app_module.db_session() as db:
            g = self.app_module.GuideModel(
                slug=slug, category="c", sort_order=10,
                published=published, title_en=slug, title_zh="",
                summary_en="", summary_zh="", body_en="", body_zh="",
                created_at="", updated_at="",
            )
            db.add(g)
            db.commit()
            return db.query(self.app_module.GuideModel).filter_by(slug=slug).first().id

    def test_route_is_registered(self):
        rules = [r.rule for r in self.app.url_map.iter_rules()
                 if r.endpoint == "admin_guide_toggle_published"]
        self.assertEqual(rules, ["/dashboard/admin/guides/<int:guide_id>/toggle"])

    def test_toggle_with_explicit_published_true_publishes(self):
        gid = self._seed_guide("toggle-1", published=False)
        client = self.app.test_client()
        _login_as(client, self.app_module, level=3)
        resp = client.post(f"/dashboard/admin/guides/{gid}/toggle",
                           data=json.dumps({"published": True}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["published"])
        with self.app_module.db_session() as db:
            row = db.query(self.app_module.GuideModel).filter_by(id=gid).first()
            self.assertTrue(row.published)

    def test_toggle_without_body_flips_current_state(self):
        gid = self._seed_guide("toggle-2", published=True)
        client = self.app.test_client()
        _login_as(client, self.app_module, level=3)
        resp = client.post(f"/dashboard/admin/guides/{gid}/toggle",
                           data="",
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.get_json()["published"])

    def test_unknown_guide_returns_404(self):
        client = self.app.test_client()
        _login_as(client, self.app_module, level=3)
        resp = client.post("/dashboard/admin/guides/9999999/toggle",
                           data=json.dumps({}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 404)

    def test_level_2_user_is_forbidden(self):
        gid = self._seed_guide("toggle-3", published=False)
        client = self.app.test_client()
        _login_as(client, self.app_module, level=2)
        resp = client.post(f"/dashboard/admin/guides/{gid}/toggle",
                           data=json.dumps({"published": True}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 401)


if __name__ == "__main__":
    unittest.main()
