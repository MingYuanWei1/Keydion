import importlib
import os
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_app():
    os.environ["PAPERQUERY_SECRET"] = "test-secret"
    os.environ.setdefault("PAPERQUERY_DATABASE_URL", "sqlite:///:memory:")
    import app as app_module
    importlib.reload(app_module)
    return app_module


class CanViewNodeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = _load_app()

    def test_guest_sees_only_reader_level(self):
        can = self.m._can_view_node
        self.assertTrue(can(1, 0))    # guest (OPEN_ACCESS) sees reader-level
        self.assertFalse(can(2, 0))   # guest cannot see contributor-restricted

    def test_logged_in_role_threshold(self):
        can = self.m._can_view_node
        self.assertFalse(can(2, 1))   # reader cannot see contributor folder
        self.assertTrue(can(2, 2))    # contributor can
        self.assertTrue(can(3, 3))    # curator sees everything

    def test_none_viewer_denied(self):
        self.assertFalse(self.m._can_view_node(1, None))


import ast
from jinja2 import Environment, FileSystemLoader
from types import SimpleNamespace


class ResourcesRouteSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.src)

    def _func(self, name):
        for node in ast.walk(self.tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.src, node)
        return ""

    def test_browse_route_checks_viewer_and_effective_role(self):
        s = self._func("resources")
        self.assertIn("_resource_viewer_role()", s)
        self.assertIn("effective_min_role", s)
        self.assertIn("_can_view_node", s)

    def test_file_serving_routes_enforce_access(self):
        for fn in ("resource_file", "resource_download"):
            s = self._func(fn)
            self.assertIn("resolve_viewable_file", s, fn)
        resolver = self._func("resolve_viewable_file")
        self.assertIn("_resource_viewer_role()", resolver)
        self.assertIn("_can_view_node", resolver)


class ResourcesTemplateRenderTest(unittest.TestCase):
    def _render(self, children):
        env = Environment(loader=FileSystemLoader(str(ROOT / "templates")))
        env.filters.setdefault("filesizeformat", lambda v, binary=False: f"{v}B")
        template = env.get_template("resources.html")
        return template.render(
            _=lambda value, **kw: value % kw if kw else value,
            url_for=lambda endpoint, **kw: "/" + endpoint + (("/" + str(list(kw.values())[0])) if kw else ""),
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/resources", args={}, headers={}),
            session={}, current_locale="en", current_year=2026, ms_enabled=False,
            open_access=True, llm_enabled=False, partial=False,
            current=None, breadcrumbs=[], children=children,
        )

    def test_preview_only_for_previewable_files(self):
        html = self._render([
            {"id": 1, "node_type": "file", "name": "a.pdf", "size_bytes": 10,
             "description": "", "is_previewable": True},
            {"id": 2, "node_type": "file", "name": "b.docx", "size_bytes": 20,
             "description": "", "is_previewable": False},
        ])
        self.assertEqual(html.count(">Preview<"), 1)   # only the PDF
        self.assertEqual(html.count(">Download<"), 2)  # both files


class LandingFooterLinkTest(unittest.TestCase):
    def test_footer_links_to_resources_not_external(self):
        text = (ROOT / "templates" / "landing.html").read_text(encoding="utf-8")
        self.assertIn("_('Academic Resources')", text)
        self.assertIn("url_for('resources')", text)
        self.assertNotIn("destiny.huijiaedu.org", text)
