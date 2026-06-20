"""Contract: journal_manage.html exposes the IDs/attributes its JS depends on."""
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

ROOT = Path(__file__).resolve().parents[1]


class JournalManageTemplateTest(unittest.TestCase):
    def render(self, journals):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"))
        env.globals["csrf_token"] = lambda: ""
        return env.get_template("journal_manage.html").render(
            _=lambda value, **kw: value % kw if kw else value,
            url_for=lambda endpoint, **kw: f"/{endpoint}",
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/dashboard/admin/journals", args={}),
            session={},
            current_locale="en",
            partial=True,
            user=SimpleNamespace(role="3"),
            journals=journals,
        )

    def test_add_controls_and_endpoints_present(self):
        html = self.render([])
        self.assertIn('id="journalNameInput"', html)
        self.assertIn('id="journalAddBtn"', html)
        self.assertIn("admin_journals_add", html)
        self.assertIn("admin_journals_delete", html)

    def test_rows_carry_delete_hook_and_slug(self):
        html = self.render([
            {"id": "j1", "name": "IB EE", "slug": "IB_EE",
             "cover_image": "", "introduction": "Intro", "paper_count": 3},
        ])
        self.assertIn('data-journal-delete="j1"', html)
        self.assertIn("/journals/IB_EE", html)
        self.assertIn("admin_journal_edit", html)


if __name__ == "__main__":
    unittest.main()
