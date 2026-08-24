import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from db import db_session
from models import GuideModel, LocalUser, NewsArticleModel, SessionModel
from services.auth import hash_password
from services.papers import load_ee_subjects, load_ia_subjects

ROOT = Path(__file__).resolve().parents[1]


def _build_app():
    """Build a fresh Flask app against an in-memory sqlite DB."""
    os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
    handle = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
    handle.close()
    os.environ["PAPERQUERY_DATABASE_URL"] = f"sqlite:///{handle.name}"
    import importlib
    import sys
    sys.path.insert(0, str(ROOT))
    import app as app_module
    import db
    import models
    from sqlalchemy import create_engine

    bootstrap_engine = create_engine(os.environ["PAPERQUERY_DATABASE_URL"])
    try:
        models.bootstrap_empty_database(bootstrap_engine)
    finally:
        bootstrap_engine.dispose()
    db.DB_URL = os.environ["PAPERQUERY_DATABASE_URL"]
    db._ENGINE = None
    db._SESSION_LOCAL = None
    importlib.reload(app_module)
    app = app_module.create_app()
    app.config["WTF_CSRF_ENABLED"] = False
    return app


def _login_as(client, level):
    """Insert a local user at the given level and stash a session token."""
    with db_session() as db:
        # Remove any prior fixture user/session (tests share the app/DB at class scope).
        existing = db.get(LocalUser, f"u{level}")
        if existing is not None:
            db.delete(existing)
        db.query(SessionModel).filter(
            SessionModel.account_type == "local",
            SessionModel.account_id == f"u{level}",
        ).delete()
        db.commit()
        u = LocalUser(
            username=f"u{level}",
            password=hash_password("pw"),
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
        cls.app = _build_app()

    def _seed_articles(self, ids_and_status):
        with db_session() as db:
            for nid, status in ids_and_status:
                db.add(NewsArticleModel(
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
        _login_as(client, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["art-pub-1"], "op": "publish"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["affected"], 1)
        with db_session() as db:
            row = db.query(NewsArticleModel).filter_by(id="art-pub-1").first()
            self.assertEqual(row.status, "published")
            self.assertTrue(row.published_at)  # stamped

    def test_unpublish_op_sets_status_pending(self):
        self._seed_articles([("art-unp-1", "published")])
        client = self.app.test_client()
        _login_as(client, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["art-unp-1"], "op": "unpublish"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        with db_session() as db:
            row = db.query(NewsArticleModel).filter_by(id="art-unp-1").first()
            self.assertEqual(row.status, "pending")

    def test_delete_op_removes_rows(self):
        self._seed_articles([("art-del-1", "published"), ("art-del-2", "pending")])
        client = self.app.test_client()
        _login_as(client, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["art-del-1", "art-del-2"], "op": "delete"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        with db_session() as db:
            remaining = db.query(NewsArticleModel).filter(
                NewsArticleModel.id.in_(["art-del-1", "art-del-2"])).count()
            self.assertEqual(remaining, 0)

    def test_bad_op_returns_400(self):
        client = self.app.test_client()
        _login_as(client, level=2)
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
        with db_session() as db:
            db.add(NewsArticleModel(
                id="art-already-pub", title="T", status="published",
                abstract="", body="", author="a", category="c",
                image_url="", published_at=original_ts,
            ))
            db.commit()
        client = self.app.test_client()
        _login_as(client, level=2)
        resp = client.post("/dashboard/news/bulk_action",
                           data=json.dumps({"ids": ["art-already-pub"], "op": "publish"}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        with db_session() as db:
            row = db.query(NewsArticleModel).filter_by(id="art-already-pub").first()
            self.assertEqual(row.published_at, original_ts)
            self.assertEqual(row.status, "published")


class NewsPublishingWorkflowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _build_app()

    def test_quill_body_is_sanitized_persisted_and_rendered(self):
        client = self.app.test_client()
        _login_as(client, level=2)
        response = client.post(
            "/dashboard/news/publish",
            data={
                "title": "Quill article",
                "category": "News",
                "abstract": "Summary",
                "author": "Editor",
                "body": '<p>Hello <strong>reader</strong></p><script>bad()</script>',
                "action": "publish",
            },
        )
        self.assertEqual(response.status_code, 302)
        with db_session() as db:
            row = db.query(NewsArticleModel).filter_by(title="Quill article").one()
            article_id = row.id
            self.assertIn("<strong>reader</strong>", row.body)
            self.assertNotIn("bad()", row.body)
        rendered = client.get(f"/news/{article_id}")
        self.assertIn(b"<strong>reader</strong>", rendered.data)

    def test_incomplete_article_saves_as_draft_but_cannot_publish(self):
        client = self.app.test_client()
        _login_as(client, level=2)
        draft = client.post(
            "/dashboard/news/publish",
            data={"title": "Incomplete draft", "action": "draft"},
        )
        self.assertEqual(draft.status_code, 302)
        with db_session() as db:
            row = db.query(NewsArticleModel).filter_by(title="Incomplete draft").one()
            self.assertEqual(row.status, "pending")
            self.assertEqual(row.published_at, "")

        rejected = client.post(
            "/dashboard/news/publish",
            data={"title": "Incomplete publish", "action": "publish"},
        )
        self.assertEqual(rejected.status_code, 200)
        with db_session() as db:
            self.assertIsNone(
                db.query(NewsArticleModel).filter_by(title="Incomplete publish").first()
            )

    def test_draft_is_hidden_from_readers_but_visible_to_editors(self):
        with db_session() as db:
            db.add(NewsArticleModel(
                id="private-draft", title="Private draft", category="", abstract="",
                body="", author="", image_url="", published_at="", status="pending",
            ))
            db.commit()
        reader = self.app.test_client()
        _login_as(reader, level=1)
        hidden = reader.get("/news/private-draft")
        self.assertEqual(hidden.status_code, 302)
        self.assertTrue(hidden.headers["Location"].endswith("/news"))
        self.assertNotIn(b"Private draft", reader.get("/news").data)

        editor = self.app.test_client()
        _login_as(editor, level=2)
        self.assertEqual(editor.get("/news/private-draft").status_code, 200)

    def test_legacy_blocks_render_and_open_in_quill_editor(self):
        legacy = json.dumps([
            {"type": "text", "content": "<p>Legacy body</p>"},
            {"type": "divider"},
        ])
        with db_session() as db:
            db.add(NewsArticleModel(
                id="legacy-news", title="Legacy", category="News", abstract="Summary",
                body=legacy, author="Editor", image_url="", published_at="2026-01-01",
                status="published",
            ))
            db.commit()
        client = self.app.test_client()
        _login_as(client, level=2)
        rendered = client.get("/news/legacy-news")
        self.assertIn(b"Legacy body", rendered.data)
        editor = client.get("/dashboard/news/legacy-news/edit")
        self.assertIn(b"vendor/quill/quill.min.js", editor.data)
        self.assertIn(b"Legacy body", editor.data)
        self.assertNotIn(b'"type": "text"', editor.data)


class GuideReorderEndpointTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _build_app()

    def _seed_guides(self, rows):
        ids = []
        with db_session() as db:
            for slug, category, sort_order in rows:
                g = GuideModel(
                    slug=slug, category=category, sort_order=sort_order,
                    published=False, title_en=slug, title_zh="", summary_en="",
                    summary_zh="", body_en="", body_zh="", created_at="", updated_at="",
                )
                db.add(g)
            db.commit()
            for slug, _c, _s in rows:
                g = db.query(GuideModel).filter_by(slug=slug).first()
                ids.append(g.id)
        return ids

    def test_route_is_registered(self):
        rules = [r.rule for r in self.app.url_map.iter_rules()
                 if r.endpoint == "admin_guides_reorder"]
        self.assertEqual(rules, ["/dashboard/admin/guides/reorder"])

    def test_reorder_updates_sort_order_and_category(self):
        ids = self._seed_guides([("g-a", "cat1", 10), ("g-b", "cat1", 20)])
        client = self.app.test_client()
        _login_as(client, level=3)
        payload = {"items": [
            {"id": ids[0], "sort_order": 2, "category": "cat2"},
            {"id": ids[1], "sort_order": 1, "category": "cat1"},
        ]}
        resp = client.post("/dashboard/admin/guides/reorder",
                           data=json.dumps(payload),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json()["ok"])
        with db_session() as db:
            a = db.query(GuideModel).filter_by(id=ids[0]).first()
            b = db.query(GuideModel).filter_by(id=ids[1]).first()
            self.assertEqual(a.sort_order, 2)
            self.assertEqual(a.category, "cat2")
            self.assertEqual(b.sort_order, 1)
            self.assertEqual(b.category, "cat1")

    def test_unknown_id_is_skipped_silently(self):
        client = self.app.test_client()
        _login_as(client, level=3)
        resp = client.post("/dashboard/admin/guides/reorder",
                           data=json.dumps({"items": [{"id": 999999, "sort_order": 1}]}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json()["ok"])

    def test_level_2_user_is_forbidden(self):
        client = self.app.test_client()
        _login_as(client, level=2)
        resp = client.post("/dashboard/admin/guides/reorder",
                           data=json.dumps({"items": []}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 401)


class GuideTogglePublishedEndpointTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _build_app()

    def _seed_guide(self, slug, published):
        with db_session() as db:
            g = GuideModel(
                slug=slug, category="c", sort_order=10,
                published=published, title_en=slug, title_zh="",
                summary_en="", summary_zh="", body_en="", body_zh="",
                created_at="", updated_at="",
            )
            db.add(g)
            db.commit()
            return db.query(GuideModel).filter_by(slug=slug).first().id

    def test_route_is_registered(self):
        rules = [r.rule for r in self.app.url_map.iter_rules()
                 if r.endpoint == "admin_guide_toggle_published"]
        self.assertEqual(rules, ["/dashboard/admin/guides/<int:guide_id>/toggle"])

    def test_toggle_with_explicit_published_true_publishes(self):
        gid = self._seed_guide("toggle-1", published=False)
        client = self.app.test_client()
        _login_as(client, level=3)
        resp = client.post(f"/dashboard/admin/guides/{gid}/toggle",
                           data=json.dumps({"published": True}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["published"])
        with db_session() as db:
            row = db.query(GuideModel).filter_by(id=gid).first()
            self.assertTrue(row.published)

    def test_toggle_without_body_flips_current_state(self):
        gid = self._seed_guide("toggle-2", published=True)
        client = self.app.test_client()
        _login_as(client, level=3)
        resp = client.post(f"/dashboard/admin/guides/{gid}/toggle",
                           data="",
                           content_type="application/json")
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.get_json()["published"])

    def test_unknown_guide_returns_404(self):
        client = self.app.test_client()
        _login_as(client, level=3)
        resp = client.post("/dashboard/admin/guides/9999999/toggle",
                           data=json.dumps({}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 404)

    def test_level_2_user_is_forbidden(self):
        gid = self._seed_guide("toggle-3", published=False)
        client = self.app.test_client()
        _login_as(client, level=2)
        resp = client.post(f"/dashboard/admin/guides/{gid}/toggle",
                           data=json.dumps({"published": True}),
                           content_type="application/json")
        self.assertEqual(resp.status_code, 401)


class CurriculumManagePagesRenderTest(unittest.TestCase):
    """Live-render the EE/IA subjects manage pages through real Flask-Babel.

    The DOM/AST contract tests render templates with a stubbed gettext, so they
    miss server-side gettext failures. A real render catches them — e.g. a
    `{{ _('…%(x)s…') }}` call with no args raises KeyError during `rv % {}`.
    Regression guard for the IA `critSummary` crash.
    """

    @classmethod
    def setUpClass(cls):
        cls.app = _build_app()

    def test_ia_subjects_manage_renders(self):
        client = self.app.test_client()
        _login_as(client, level=3)
        resp = client.get("/dashboard/admin/ia-subjects")
        self.assertEqual(resp.status_code, 200, resp.data[:400])
        self.assertIn(b"iaData", resp.data)

    def test_ee_subjects_manage_renders(self):
        client = self.app.test_client()
        _login_as(client, level=3)
        resp = client.get("/dashboard/admin/ee-subjects")
        self.assertEqual(resp.status_code, 200, resp.data[:400])
        self.assertIn(b"eeData", resp.data)

    def test_reordered_subject_payloads_are_persisted(self):
        client = self.app.test_client()
        _login_as(client, level=3)
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch("services.papers._EE_SUBJECTS_PATH", Path(directory) / "ee.json"), \
             mock.patch("services.papers._IA_SUBJECTS_PATH", Path(directory) / "ia.json"):
            cases = (
                (
                    "/dashboard/admin/ee-subjects/save",
                    {"groups": [
                        {"id": 2, "name": "Second", "subjects": [
                            {"name": "Beta", "original_name": None, "interdisciplinary": False},
                        ]},
                        {"id": 1, "name": "First", "subjects": [
                            {"name": "Alpha", "original_name": None, "interdisciplinary": False},
                        ]},
                    ]},
                    load_ee_subjects,
                ),
                (
                    "/dashboard/admin/ia-subjects/save",
                    {"groups": [
                        {"id": 2, "name": "Second", "subjects": [
                            {"name": "Beta", "original_name": None, "criteria": []},
                        ]},
                        {"id": 1, "name": "First", "subjects": [
                            {"name": "Alpha", "original_name": None, "criteria": []},
                        ]},
                    ]},
                    load_ia_subjects,
                ),
            )
            for url, payload, load in cases:
                with self.subTest(url=url):
                    response = client.post(url, json=payload)
                    self.assertEqual(response.status_code, 200, response.data[:400])
                    self.assertEqual(
                        [group["name"] for group in load()["groups"]],
                        ["Second", "First"],
                    )


if __name__ == "__main__":
    unittest.main()
