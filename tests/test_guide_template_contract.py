import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
READER_LEDE = (
    "Discover and read research, ask questions about it, and submit your own "
    "work for review."
)
OLD_CONTRIBUTOR_LEDE = "How to upload, publish, and curate work on Keydion."
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
        # Quill CSS still loaded from the template; init moves to the JS module.
        self.assertIn("vendor/quill/quill.snow.css", self.publish_tpl)
        self.assertIn("vendor/quill/quill.min.js", self.publish_tpl)
        self.assertIn("js/guides-editor.js", self.publish_tpl)

    def test_publish_template_wires_image_upload(self):
        # Image upload endpoint is referenced from the publish template (so the
        # JS module can read it as a data-* attribute) AND used in the JS module.
        self.assertIn("admin_guides_upload_image", self.publish_tpl)
        js_path = ROOT / "static" / "js" / "guides-editor.js"
        js = js_path.read_text(encoding="utf-8") if js_path.exists() else ""
        self.assertIn("admin_guides_upload_image", js + self.publish_tpl)

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

    def test_index_introduction_is_reader_only(self):
        self.assertIn(READER_LEDE, self.index_tpl)
        self.assertNotIn(OLD_CONTRIBUTOR_LEDE, self.index_tpl)

    def test_article_template_renders_body_safe(self):
        # body is sanitized server-side, so `| safe` is correct here
        self.assertIn("| safe", self.article_tpl)
        self.assertIn("guide.body_en", self.article_tpl)
        self.assertIn("guide.body_zh", self.article_tpl)

    def test_article_template_uses_new_design(self):
        self.assertIn("kd-page", self.article_tpl)
        self.assertIn("kd-back", self.article_tpl)
        self.assertIn("kd-h-page", self.article_tpl)
        self.assertIn("kd-article-meta", self.article_tpl)
        self.assertIn("kd-cat-pill", self.article_tpl)
        self.assertIn('article class="kd-body"', self.article_tpl)
        self.assertIn("kd-prevnext", self.article_tpl)
        # prev/next rendered, optional via if/else
        self.assertIn("prev_guide", self.article_tpl)
        self.assertIn("next_guide", self.article_tpl)
        # preview banner shown when preview_mode is true
        self.assertIn("preview_mode", self.article_tpl)
        self.assertIn("Preview", self.article_tpl)

    def test_publish_template_uses_dashboard_shell(self):
        self.assertIn('extends "_dashboard_shell.html"', self.publish_tpl)
        self.assertIn("{% block panel %}", self.publish_tpl)

    def test_publish_template_has_new_form_structure(self):
        # Editor cards per language
        self.assertIn('data-lang="en"', self.publish_tpl)
        self.assertIn('data-lang="zh"', self.publish_tpl)
        # Slug prefix decoration
        self.assertIn("kd-input-prefix", self.publish_tpl)
        # Custom published toggle
        self.assertIn("kd-toggle", self.publish_tpl)
        # Sticky form footer with dirty state
        self.assertIn("kd-form-footer", self.publish_tpl)
        self.assertIn("data-dirty-state", self.publish_tpl)
        # Preview and delete buttons
        self.assertIn("data-preview-guide", self.publish_tpl)
        # Status pill
        self.assertIn("data-status", self.publish_tpl)
        # URL exposed for JS image upload
        self.assertIn("data-upload-image-url", self.publish_tpl)

    def test_publish_template_keeps_field_names(self):
        # Backward compat: the POST handler must still see these names
        for name in ("slug", "category", "sort_order", "published",
                     "title_en", "title_zh", "summary_en", "summary_zh",
                     "body_en", "body_zh"):
            self.assertIn(f'name="{name}"', self.publish_tpl)


if __name__ == "__main__":
    unittest.main()
