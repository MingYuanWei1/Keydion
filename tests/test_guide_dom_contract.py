import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GuideDomContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        js_path = ROOT / "static" / "js" / "guides-editor.js"
        cls.js = js_path.read_text(encoding="utf-8") if js_path.exists() else ""
        cls.publish_tpl = (ROOT / "templates" / "guide_publish.html").read_text(encoding="utf-8")

    def test_js_module_exists(self):
        self.assertTrue(self.js, "static/js/guides-editor.js must exist")

    def test_js_referenced_element_ids_exist_in_template(self):
        ids = set(re.findall(r"getElementById\(['\"]([\w-]+)['\"]\)", self.js))
        ids |= set(re.findall(r"querySelector\(['\"]#([\w-]+)['\"]\)", self.js))
        for el_id in ids:
            self.assertIn(f'id="{el_id}"', self.publish_tpl,
                          f"JS references #{el_id} but template has no id={el_id}")

    def test_js_referenced_data_attrs_exist_in_template(self):
        attrs = set(re.findall(r"\[data-([\w-]+)", self.js))
        for attr in attrs:
            self.assertIn(f"data-{attr}", self.publish_tpl,
                          f"JS references [data-{attr}] but template has no such attribute")

    def test_js_registers_callout_blot(self):
        self.assertIn("CalloutBlot", self.js)
        self.assertIn("blotName = 'callout'", self.js.replace('"', "'"))
        self.assertIn("kd-callout", self.js)
        # Toolbar handler for callout
        self.assertIn("'callout'", self.js)

    def test_js_registers_figure_blot(self):
        self.assertIn("FigureBlot", self.js)
        self.assertIn("blotName = 'figure'", self.js.replace('"', "'"))
        self.assertIn("kd-fig", self.js)
        self.assertIn("kd-fig-img", self.js)
        self.assertIn("kd-fig-caption", self.js)

    def test_js_wires_status_pill(self):
        self.assertIn("data-status-label", self.js)
        # Status states surface as text in the JS for translation/observability
        for phrase in ("All fields filled", "Title missing",
                       "Summary missing", "Body missing"):
            self.assertIn(phrase, self.js)

    def test_js_wires_dirty_tracker(self):
        self.assertIn("data-dirty-state", self.js)
        self.assertIn("beforeunload", self.js)
        for phrase in ("All changes saved", "Unsaved changes"):
            self.assertIn(phrase, self.js)

    def test_js_wires_toggle_preview_delete(self):
        self.assertIn("data-toggle-published", self.js)
        self.assertIn("data-preview-guide", self.js)
        self.assertIn("data-delete-guide", self.js)
        self.assertIn("publishedCheck", self.js)
        self.assertIn("deleteGuideForm", self.js)


if __name__ == "__main__":
    unittest.main()
