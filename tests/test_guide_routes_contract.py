import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GuideRoutesContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)
        cls.landing = (ROOT / "templates" / "landing.html").read_text(encoding="utf-8")

    def _route_decorators(self, func_name):
        """Return list of @app.route decorator argument dicts for a given function."""
        decorators = []
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                for dec in node.decorator_list:
                    if isinstance(dec, ast.Call) and getattr(dec.func, "attr", "") == "route":
                        path = dec.args[0].value if dec.args else None
                        methods = []
                        for kw in dec.keywords:
                            if kw.arg == "methods" and isinstance(kw.value, ast.List):
                                methods = [e.value for e in kw.value.elts]
                        decorators.append({"path": path, "methods": methods or ["GET"]})
        return decorators

    def _function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        return ""

    def test_public_routes_exist(self):
        self.assertEqual(self._route_decorators("guides"), [{"path": "/guides", "methods": ["GET"]}])
        self.assertEqual(
            self._route_decorators("guide_article"),
            [{"path": "/guides/<slug>", "methods": ["GET"]}],
        )

    def test_admin_manage_route(self):
        decs = self._route_decorators("admin_guides_manage")
        self.assertEqual(decs, [{"path": "/dashboard/admin/guides", "methods": ["GET"]}])
        self.assertIn("require_login(level=3)", self._function_source("admin_guides_manage"))

    def test_admin_publish_route_handles_new_and_edit(self):
        decs = self._route_decorators("admin_guide_publish")
        paths = {(d["path"], tuple(sorted(d["methods"]))) for d in decs}
        self.assertIn(("/dashboard/admin/guides/new", ("GET", "POST")), paths)
        self.assertIn(("/dashboard/admin/guides/<int:guide_id>/edit", ("GET", "POST")), paths)
        self.assertIn("require_login(level=3)", self._function_source("admin_guide_publish"))

    def test_admin_delete_route(self):
        decs = self._route_decorators("admin_guide_delete")
        self.assertEqual(
            decs,
            [{"path": "/dashboard/admin/guides/<int:guide_id>/delete", "methods": ["POST"]}],
        )
        self.assertIn("require_login(level=3)", self._function_source("admin_guide_delete"))

    def test_admin_upload_image_route(self):
        decs = self._route_decorators("admin_guide_upload_image")
        self.assertEqual(
            decs,
            [{"path": "/dashboard/admin/guides/upload-image", "methods": ["POST"]}],
        )
        src = self._function_source("admin_guide_upload_image")
        self.assertIn("require_login(level=3)", src)
        self.assertIn("ALLOWED_IMAGE_EXTENSIONS", src)

    def test_landing_footer_links_to_guides(self):
        self.assertIn("url_for('guides')", self.landing)
        self.assertNotIn(
            '<li><a href="#">{{ _(\'Submission Guide\') }}</a></li>',
            self.landing,
        )

    def test_read_guide_form_helper_exists(self):
        # A helper that maps request.form -> the canonical guide form dict.
        self.assertIn("def _read_guide_form(", self.app_source,
                      "expected module-level helper _read_guide_form(form)")

    def test_admin_guide_publish_uses_read_guide_form(self):
        src = self._function_source("admin_guide_publish")
        self.assertIn("_read_guide_form(request.form)", src)

    def test_admin_guide_preview_route(self):
        decs = self._route_decorators("admin_guide_preview")
        self.assertEqual(
            decs,
            [{"path": "/dashboard/admin/guides/preview", "methods": ["POST"]}],
        )
        src = self._function_source("admin_guide_preview")
        self.assertIn("require_login(level=3)", src)
        self.assertIn("_read_guide_form(request.form)", src)
        self.assertIn('render_template("guide_article.html"', src)
        self.assertIn("preview_mode=True", src)

    def test_guide_article_passes_prev_next(self):
        src = self._function_source("guide_article")
        self.assertIn("prev_guide=", src)
        self.assertIn("next_guide=", src)
        self.assertIn("preview_mode=False", src)


if __name__ == "__main__":
    unittest.main()
