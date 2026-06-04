import re
import unittest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


class ResourceManageDomContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template_text = (ROOT / "templates" / "resource_manage.html").read_text(encoding="utf-8")
        env = Environment(loader=FileSystemLoader(str(ROOT / "templates")))
        env.filters.setdefault("filesizeformat", lambda v, binary=False: f"{v}B")
        template = env.get_template("resource_manage.html")
        cls.html = template.render(
            _=lambda value, **kw: value % kw if kw else value,
            pgettext=lambda ctx, value, **kw: value % kw if kw else value,
            url_for=lambda endpoint, **kw: "/" + endpoint,
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/dashboard/admin/resources", args={}, headers={}),
            session={"user": {"role": "3"}}, current_locale="en", current_year=2026,
            partial=True, user={"role": "3"}, current=None, breadcrumbs=[],
            parent_id=None,
            children=[
                {"id": 1, "node_type": "folder", "name": "F", "description": "",
                 "min_role": 1, "size_bytes": None, "is_previewable": False},
                {"id": 2, "node_type": "file", "name": "x.pdf", "description": "",
                 "min_role": 2, "size_bytes": 99, "is_previewable": True},
            ],
            move_targets=[{"id": 1, "name": "F", "depth": 0}],
        )

    def test_every_getElementById_has_matching_id(self):
        referenced = set(re.findall(r"getElementById\(['\"]([\w-]+)['\"]\)", self.template_text))
        present = set(re.findall(r'id="([\w-]+)"', self.html))
        missing = referenced - present
        self.assertEqual(missing, set(),
                         f"getElementById ids with no matching element: {missing}")

    def test_core_controls_present(self):
        for el_id in ("newFolderModal", "uploadModal", "editModal", "moveModal"):
            self.assertIn(f'id="{el_id}"', self.html)
