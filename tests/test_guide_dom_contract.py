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


if __name__ == "__main__":
    unittest.main()
