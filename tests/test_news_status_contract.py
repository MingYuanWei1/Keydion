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


if __name__ == "__main__":
    unittest.main()
