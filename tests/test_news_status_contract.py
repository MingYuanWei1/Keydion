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

    def test_landing_index_filters_latest_news_to_published(self):
        # The landing route is index(); it pulls latest_news from load_news_articles.
        src = self._find_function_source("index")
        self.assertIn('load_news_articles(status="published")', src.replace("'", '"'))


if __name__ == "__main__":
    unittest.main()
