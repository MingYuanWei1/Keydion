import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class GuideTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.publish_tpl = (ROOT / "templates" / "guide_publish.html").read_text(encoding="utf-8")
        cls.manage_tpl = (ROOT / "templates" / "guide_manage.html").read_text(encoding="utf-8")
        cls.index_tpl = (ROOT / "templates" / "guides.html").read_text(encoding="utf-8")
        cls.article_tpl = (ROOT / "templates" / "guide_article.html").read_text(encoding="utf-8")

    def test_publish_form_has_all_body_fields(self):
        # Per-language title/summary inputs
        for name in ("title_en", "title_zh", "summary_en", "summary_zh"):
            self.assertIn(f'name="{name}"', self.publish_tpl,
                          f"publish template missing input name={name}")
        # Hidden body fields populated by Quill
        self.assertIn('name="body_en"', self.publish_tpl)
        self.assertIn('name="body_zh"', self.publish_tpl)
        self.assertIn('id="bodyEnField"', self.publish_tpl)
        self.assertIn('id="bodyZhField"', self.publish_tpl)
        # Metadata
        for name in ("slug", "category", "sort_order", "published"):
            self.assertIn(f'name="{name}"', self.publish_tpl)

    def test_publish_template_loads_quill(self):
        self.assertIn("vendor/quill/quill.snow.css", self.publish_tpl)
        self.assertIn("vendor/quill/quill.min.js", self.publish_tpl)
        self.assertIn("new Quill(", self.publish_tpl)

    def test_publish_template_wires_image_upload(self):
        self.assertIn("admin_guides_upload_image", self.publish_tpl)

    def test_manage_template_links_to_new_and_edit(self):
        self.assertIn("admin_guide_new", self.manage_tpl)
        self.assertIn("admin_guide_edit", self.manage_tpl)
        self.assertIn("admin_guide_delete", self.manage_tpl)

    def test_index_template_links_to_articles_by_slug(self):
        self.assertIn("guide_article", self.index_tpl)
        self.assertIn("slug=g.slug", self.index_tpl)

    def test_index_template_uses_new_design(self):
        self.assertIn("kd-page", self.index_tpl)
        self.assertIn("kd-eyebrow", self.index_tpl)
        self.assertIn("kd-h-display", self.index_tpl)
        self.assertIn("kd-lede", self.index_tpl)
        self.assertIn("kd-cat-row", self.index_tpl)
        self.assertIn("kd-cat-label", self.index_tpl)
        self.assertIn("kd-guide-list", self.index_tpl)
        self.assertIn("kd-guide-item", self.index_tpl)
        self.assertIn("kd-guide-num", self.index_tpl)
        # Two-digit padded counter format used somewhere
        self.assertIn("'%02d'", self.index_tpl)
        # Empty-state message preserved
        self.assertIn("No guides published yet", self.index_tpl)

    def test_article_template_renders_body_safe(self):
        # body is sanitized server-side, so `| safe` is correct here
        self.assertIn("| safe", self.article_tpl)
        self.assertIn("guide.body_en", self.article_tpl)
        self.assertIn("guide.body_zh", self.article_tpl)


if __name__ == "__main__":
    unittest.main()
