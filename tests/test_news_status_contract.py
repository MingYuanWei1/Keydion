import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class NewsStatusSchemaContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_news_fields_includes_status(self):
        # NEWS_FIELDS is the canonical list used by load/save helpers.
        self.assertIn('"status"', self._find_assignment_source("NEWS_FIELDS"))

    def test_news_article_model_has_status_column(self):
        model_src = self._find_class_source("NewsArticleModel")
        self.assertIn("status = Column", model_src)
        # Default must be 'published' so existing rows remain visible.
        self.assertIn('default="published"', model_src)

    def test_init_db_runs_idempotent_status_migration(self):
        init_db = self._find_function_source("init_db")
        self.assertIn("ALTER TABLE news_articles ADD COLUMN status", init_db)

    def _find_assignment_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == name:
                        return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find assignment for {name}")

    def _find_class_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.ClassDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find class {name}")

    def _find_function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find function {name}")


class LoadNewsArticlesFilterContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_load_news_articles_accepts_status_filter(self):
        fn = None
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == "load_news_articles":
                fn = node
                break
        self.assertIsNotNone(fn, "load_news_articles not found")
        arg_names = [a.arg for a in fn.args.args]
        self.assertIn("status", arg_names)
        src = ast.get_source_segment(self.app_source, fn)
        self.assertIn("filter_by(status=", src)


class PublicNewsViewsFilterContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _find_function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find function {name}")

    def test_news_list_filters_to_published(self):
        src = self._find_function_source("news_list")
        self.assertIn('load_news_articles(status="published")', src.replace("'", '"'))

    def test_news_detail_blocks_drafts_from_non_editors(self):
        src = self._find_function_source("news_detail")
        # Must check status and require role >= 2 (editor/admin) to view drafts.
        self.assertIn('"status"', src.replace("'", '"'))
        self.assertIn("pending", src)

    def test_news_detail_related_list_filters_to_published(self):
        # The related-articles sidebar on the detail page must not leak drafts.
        src = self._find_function_source("news_detail")
        self.assertIn('load_news_articles(status="published")', src.replace("'", '"'))

    def test_landing_index_filters_latest_news_to_published(self):
        # The landing route is index(); it pulls latest_news from load_news_articles.
        src = self._find_function_source("index")
        self.assertIn('load_news_articles(status="published")', src.replace("'", '"'))


class NewsPublishActionContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _find_function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find function {name}")

    def test_news_publish_reads_action_and_sets_status(self):
        src = self._find_function_source("news_publish")
        self.assertIn('request.form.get("action"', src)
        self.assertIn('"pending"', src)
        self.assertIn('"published"', src)

    def test_news_publish_allows_draft_with_only_title(self):
        # Drafts skip the abstract/category/body validation.
        src = self._find_function_source("news_publish")
        self.assertIn("action == \"draft\"", src.replace("'", '"'))

    def test_news_edit_reads_action_and_sets_status(self):
        src = self._find_function_source("news_edit")
        self.assertIn('request.form.get("action"', src)
        self.assertIn('"pending"', src)
        self.assertIn('"published"', src)

    def test_update_news_article_can_update_status(self):
        src = self._find_function_source("update_news_article")
        # status must not be in the skip list.
        self.assertNotRegex(src, r"if field in \([^)]*['\"]status['\"]")

    def test_draft_save_leaves_published_at_empty(self):
        # Drafts must not record a publish timestamp; published_at is stamped
        # only on actual publish so the displayed date is accurate.
        src = self._find_function_source("news_publish")
        self.assertIn('"published_at": "" if is_draft else', src)


class NewsPublishTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = (ROOT / "templates" / "news_publish.html").read_text(encoding="utf-8")

    def test_template_has_save_as_draft_button(self):
        self.assertIn('name="action" value="draft"', self.template)

    def test_template_has_publish_action_button(self):
        self.assertIn('name="action" value="publish"', self.template)

    def test_ctrl_enter_triggers_publish_button(self):
        # The Ctrl/Cmd+Enter shortcut must click the publish button (not the draft button),
        # since requestSubmit() without a specific button omits the action field.
        self.assertIn("btnPublish", self.template)
        self.assertIn("getElementById('btnPublish').click()", self.template)

    def test_publish_button_precedes_draft_in_dom(self):
        # Browsers default-submit the first <button type="submit"> when Enter is
        # pressed inside a text input. Publish must be first in DOM order so
        # accidental Enter does not silently save as draft. Visual order is
        # preserved via flexbox `order:` on the buttons.
        publish_idx = self.template.find('id="btnPublish"')
        draft_idx = self.template.find('id="btnDraft"')
        self.assertGreater(publish_idx, 0, "btnPublish not found")
        self.assertGreater(draft_idx, 0, "btnDraft not found")
        self.assertLess(publish_idx, draft_idx,
                        "Publish button must appear before Draft button in DOM "
                        "so Enter inside a text field does not save as draft.")


class NewsManageTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = (ROOT / "templates" / "news_manage.html").read_text(encoding="utf-8")

    def test_table_has_status_header(self):
        # Must show a Status column header.
        self.assertRegex(self.template, r"<th[^>]*>\s*\{\{\s*_\(\s*'Status'\s*\)\s*\}\}\s*</th>")

    def test_table_renders_status_pill(self):
        # Body must reference item.status and render distinct pills for published vs pending.
        self.assertIn("item.status", self.template)
        self.assertIn("'pending'", self.template)
        self.assertIn("'published'", self.template)


if __name__ == "__main__":
    unittest.main()
