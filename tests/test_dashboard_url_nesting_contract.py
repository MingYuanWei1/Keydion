"""Contract: every curator-side endpoint lives under /dashboard/* and every
moved endpoint has a GET-only legacy redirect at its old path.

The endpoint -> (new_path, legacy_path) map below is the single source of
truth for the URL-nesting refactor. Each task that moves a route family
flips entries from xfail to pass."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Endpoint name -> (expected new URL pattern, old URL pattern for redirect).
# old=None means POST-only route, no legacy redirect needed.
MOVED_ROUTES = {
    # Task 4: workspace
    "upload":                          ("/dashboard/upload",                          "/upload"),
    "upload_success":                  ("/dashboard/upload/success",                  "/upload/success"),
    "my_submissions":                  ("/dashboard/my-submissions",                  "/my-submissions"),
    "my_submission_view":              ("/dashboard/my-submissions/<sub_id>",         "/my-submissions/<sub_id>"),
    "my_submission_file":              ("/dashboard/my-submissions/<sub_id>/file",    "/my-submissions/<sub_id>/file"),
    "my_submission_delete":            ("/dashboard/my-submissions/<sub_id>/delete",  None),

    # Task 5: collection management
    "paper_manage":                    ("/dashboard/admin/papers",                    "/admin/papers"),
    "paper_modify":                    ("/dashboard/paper/<path:filename>/modify",    "/paper/<path:filename>/modify"),
    "paper_delete":                    ("/dashboard/paper/<path:filename>/delete",    None),
    "ee_subjects_manage":              ("/dashboard/admin/ee-subjects",               "/admin/categories"),
    "admin_paper_categories_add":      ("/dashboard/admin/paper-categories/add",      None),
    "admin_paper_categories_rename":   ("/dashboard/admin/paper-categories/rename",   None),
    "admin_paper_categories_delete":   ("/dashboard/admin/paper-categories/delete",   None),
    "admin_ee_subjects_save":          ("/dashboard/admin/ee-subjects/save",          None),
    "admin_journals_add":              ("/dashboard/admin/journals/add",              None),
    "admin_journals_delete":           ("/dashboard/admin/journals/delete",           None),
    "admin_journal_edit":              ("/dashboard/admin/journal/<journal_id>/edit", "/admin/journal/<journal_id>/edit"),
    "admin_journals_manage":           ("/dashboard/admin/journals",                  None),
    "admin_journal_papers":            ("/dashboard/admin/journal/<journal_id>/papers", None),

    # Task 6: review
    "review_list":                     ("/dashboard/review",                          "/review"),
    "review_paper":                    ("/dashboard/review/<sub_id>",                 "/review/<sub_id>"),
    "review_accept":                   ("/dashboard/review/<sub_id>/accept",          None),
    "review_reject":                   ("/dashboard/review/<sub_id>/reject",          None),

    # Task 7: news editor
    "news_publish":                    ("/dashboard/news/publish",                    "/news/publish"),
    "news_edit":                       ("/dashboard/news/<news_id>/edit",             "/news/<news_id>/edit"),
    "news_delete":                     ("/dashboard/news/<news_id>/delete",           None),
    "news_manage":                     ("/dashboard/news/manage",                     "/news/manage"),
    "news_categories_add":             ("/dashboard/news/categories/add",             None),
    "news_categories_rename":          ("/dashboard/news/categories/rename",          None),
    "news_categories_delete":          ("/dashboard/news/categories/delete",          None),
    "news_upload_inline_image":        ("/dashboard/news/upload-inline-image",        None),

    # Task 8: admin (users + guides)
    "admin_users":                     ("/dashboard/admin/users",                     "/admin/users"),
    "admin_users_roles":               ("/dashboard/admin/users/roles",               None),
    "admin_users_add":                 ("/dashboard/admin/users/add",                 None),
    "admin_user_role":                 ("/dashboard/admin/users/<path:username>/role",            None),
    "admin_user_reset_password":       ("/dashboard/admin/users/<path:username>/reset-password",  None),
    "admin_user_delete":               ("/dashboard/admin/users/<path:username>/delete",          None),
    "admin_ms_user_role":              ("/dashboard/admin/ms-users/<path:ms_id>/role",            None),
    "admin_ms_user_delete":            ("/dashboard/admin/ms-users/<path:ms_id>/delete",          None),
    "admin_ms_user_set_password":      ("/dashboard/admin/ms-users/<path:ms_id>/set-password",    None),
    "admin_guides_manage":             ("/dashboard/admin/guides",                    "/admin/guides"),
    "admin_guide_new":                 ("/dashboard/admin/guides/new",                "/admin/guides/new"),
    "admin_guide_edit":                ("/dashboard/admin/guides/<int:guide_id>/edit", "/admin/guides/<int:guide_id>/edit"),
    "admin_guide_delete":              ("/dashboard/admin/guides/<int:guide_id>/delete", None),
    "admin_guides_upload_image":       ("/dashboard/admin/guides/upload-image",       None),

    # Task 10: academic resources
    "admin_resources_manage":          ("/dashboard/admin/resources",                 "/admin/resources"),
    "admin_resources_folder_new":      ("/dashboard/admin/resources/folder",          None),
    "admin_resources_upload":          ("/dashboard/admin/resources/upload",          None),
    "admin_resources_edit":            ("/dashboard/admin/resources/<int:node_id>/edit",   None),
    "admin_resources_move":            ("/dashboard/admin/resources/<int:node_id>/move",   None),
    "admin_resources_delete":          ("/dashboard/admin/resources/<int:node_id>/delete", None),

    # Task 9: account
    "change_password":                 ("/dashboard/account/change-password",         "/account/change-password"),
}


def _build_app():
    """Construct the Flask app once; the test reads its url_map only."""
    import os
    os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
    os.environ.setdefault("PAPERQUERY_DATABASE_URL", "sqlite:///:memory:")
    import importlib
    import app as app_module
    importlib.reload(app_module)
    return app_module.create_app()


class DashboardUrlNestingContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _build_app()
        cls.rules_by_endpoint = {}
        for rule in cls.app.url_map.iter_rules():
            cls.rules_by_endpoint.setdefault(rule.endpoint, []).append(rule.rule)

    def test_every_moved_endpoint_is_under_dashboard(self):
        missing = []
        for endpoint, (new_path, _old) in MOVED_ROUTES.items():
            rules = self.rules_by_endpoint.get(endpoint, [])
            if not any(r == new_path for r in rules):
                missing.append(f"{endpoint}: expected {new_path!r}, got {rules!r}")
        self.assertEqual(missing, [], "Endpoints not at expected /dashboard/ paths:\n" + "\n".join(missing))

    def test_legacy_redirect_endpoints_exist_for_get_routes(self):
        # Each moved endpoint with a non-None old_path must have a paired
        # *_legacy endpoint serving the old URL.
        missing = []
        for endpoint, (_new, old_path) in MOVED_ROUTES.items():
            if old_path is None:
                continue
            legacy_endpoint = f"{endpoint}_legacy"
            rules = self.rules_by_endpoint.get(legacy_endpoint, [])
            if not any(r == old_path for r in rules):
                missing.append(f"{legacy_endpoint}: expected {old_path!r}, got {rules!r}")
        self.assertEqual(missing, [], "Legacy redirect endpoints missing:\n" + "\n".join(missing))

    def test_legacy_routes_actually_redirect(self):
        client = self.app.test_client()
        skipped = []
        for endpoint, (new_path, old_path) in MOVED_ROUTES.items():
            if old_path is None:
                continue
            # Substitute fake values for path parameters so the URL is concrete.
            concrete = (old_path
                        .replace("<sub_id>", "abc")
                        .replace("<news_id>", "abc")
                        .replace("<path:filename>", "x.pdf")
                        .replace("<path:username>", "alice")
                        .replace("<path:ms_id>", "ms-1")
                        .replace("<journal_id>", "j1")
                        .replace("<int:guide_id>", "1"))
            resp = client.get(concrete, follow_redirects=False)
            if resp.status_code not in (301, 302, 308):
                skipped.append(f"{old_path} -> got {resp.status_code}, expected redirect")
                continue
            # Verify the redirect target is the /dashboard/* sibling, not e.g.
            # the login wall (302 to /login passes status-only checks vacuously).
            location = resp.headers.get("Location", "")
            if "/dashboard" not in location:
                skipped.append(f"{old_path} -> Location {location!r} does not contain /dashboard")
        self.assertEqual(skipped, [], "Legacy routes didn't redirect:\n" + "\n".join(skipped))


if __name__ == "__main__":
    unittest.main()
