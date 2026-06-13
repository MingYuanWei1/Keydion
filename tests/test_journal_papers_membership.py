"""Contract: the journal edit page batch-manages paper membership (add + remove)."""
import unittest
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader

from tests.support import source_of, all_sources

ROOT = Path(__file__).resolve().parents[1]


class JournalPapersMembershipTest(unittest.TestCase):
    def test_membership_endpoint_exists(self):
        sources = all_sources()
        self.assertIn("/dashboard/admin/journal/<journal_id>/papers", sources)
        self.assertIn("admin_journal_papers", sources)

    def test_endpoint_sets_and_clears_journal(self):
        src = source_of("journal_papers")
        self.assertIn("filenames", src)
        self.assertIn("save_paper_metadata", src)

    def render_edit(self, all_papers, member_filenames):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"))
        return env.get_template("journal_edit.html").render(
            _=lambda value, **kw: value % kw if kw else value,
            url_for=lambda endpoint, **kw: f"/{endpoint}",
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/edit", args={}),
            session={}, current_locale="en", partial=True,
            user=SimpleNamespace(role="3"),
            journal={"id": "j1", "name": "IB EE", "slug": "IB_EE",
                     "cover_image": "", "introduction": "", "created_at": "2026-01-01"},
            papers=[p for p in all_papers if p["filename"] in member_filenames],
            all_papers=all_papers,
            journal_paper_filenames=member_filenames,
        )

    def test_picker_renders_all_papers_with_member_checked(self):
        html = self.render_edit(
            all_papers=[
                {"filename": "a.pdf", "title": "Alpha", "author_name": "Ann", "journal": "IB EE"},
                {"filename": "b.pdf", "title": "Beta", "author_name": "Bob", "journal": ""},
            ],
            member_filenames=["a.pdf"],
        )
        self.assertIn('id="journalPaperSearch"', html)
        self.assertIn('id="journalPapersSaveBtn"', html)
        self.assertIn('data-filename="a.pdf"', html)
        self.assertIn('data-filename="b.pdf"', html)
        # the current member is pre-checked (attributes sit between data-filename and checked)
        self.assertRegex(html, r'data-filename="a.pdf"[^>]*checked')


if __name__ == "__main__":
    unittest.main()
