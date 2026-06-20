import sys
import unittest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

ROOT = Path(__file__).resolve().parents[1]


class OpenAccessContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = support.all_sources()

    def _function_source(self, name):
        return support.source_of(name)

    def test_open_access_flag_defined_from_env(self):
        self.assertIn("PAPERQUERY_OPEN_ACCESS", self.app_source)
        self.assertRegex(
            self.app_source,
            r'OPEN_ACCESS\s*=\s*os\.environ\.get\(\s*"PAPERQUERY_OPEN_ACCESS"\s*,\s*"0"\s*\)',
            "OPEN_ACCESS must read PAPERQUERY_OPEN_ACCESS and default to \"0\"",
        )

    def test_context_processor_exposes_open_access(self):
        src = self._function_source("inject_global_vars")
        self.assertIn('"open_access": OPEN_ACCESS', src)

    def test_paper_file_gate_is_conditional(self):
        src = self._function_source("paper_file")
        self.assertIn("if not OPEN_ACCESS:", src)
        self.assertIn("require_login()", src)

    def test_download_gate_is_conditional(self):
        src = self._function_source("download")
        self.assertIn("if not OPEN_ACCESS:", src)
        self.assertIn("require_login()", src)

    def test_preview_serves_full_pdf_when_open_access(self):
        src = self._function_source("preview_paper")
        self.assertIn("not is_guest or OPEN_ACCESS", src)

    def test_preview_template_gates_buttons_and_banner(self):
        text = (ROOT / "templates" / "preview.html").read_text(encoding="utf-8")
        self.assertEqual(
            text.count("{% if is_guest and not open_access %}"),
            2,
            "preview.html must gate both the button row and the banner on open_access",
        )
        self.assertNotIn("{% if is_guest %}", text)

    def test_search_template_gates_download(self):
        text = (ROOT / "templates" / "search.html").read_text(encoding="utf-8")
        self.assertIn("{% if is_guest and not open_access %}", text)
        self.assertNotIn("{% if is_guest %}", text)

    def _render_search(self, is_guest, open_access):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"))
        env.globals["csrf_token"] = lambda: ""
        template = env.get_template("search.html")
        records = [{
            "filename": "p.pdf", "title": "T", "category": "History",
            "author_name": "Jane", "author_school": "S",
            "published_at": "2026-05-21", "abstract": "", "is_ib_sample": "",
        }]
        return template.render(
            _=lambda value, **kwargs: value % kwargs if kwargs else value,
            ngettext=lambda s, p, n, **k: s if n == 1 else p,
            url_for=lambda endpoint, **kwargs: f"/{endpoint}",
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/search", args={}),
            session={}, current_locale="en", current_year=2026, ms_enabled=False,
            user=None, query="", category_filter="", language_filter="",
            date_filter="", paper_type_filter="", ee_subject_filter="",
            cp_context_filter="", ee_subjects_list=[], cp_contexts=[],
            filtered=False, records=records,
            pagination=SimpleNamespace(page=1, pages=1, has_prev=False, has_next=False),
            is_guest=is_guest, open_access=open_access, total_matches=1,
            paper_categories=[], journal_id_map={},
        )

    def test_search_shows_download_for_guest_when_open(self):
        html = self._render_search(is_guest=True, open_access=True)
        self.assertNotIn("Sign in to Download", html)
        self.assertIn("/download", html)

    def test_search_shows_signin_for_guest_when_closed(self):
        html = self._render_search(is_guest=True, open_access=False)
        self.assertIn("Sign in to Download", html)
