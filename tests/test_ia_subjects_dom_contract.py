"""Contract: the IA-subjects template exposes every static anchor id that
ia-subjects.js looks up by getElementById, and uses the partial extends."""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ANCHORS = ["iaData", "iaI18n", "iaGroups", "iaAddGroup", "iaSaveBar",
           "iaSaveSummary", "iaSave", "iaDiscard", "iaConflicts", "iaToast"]


class IaSubjectsDomContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tpl = (ROOT / "templates" / "ia_subjects_manage.html").read_text(encoding="utf-8")
        cls.js = (ROOT / "static" / "js" / "ia-subjects.js").read_text(encoding="utf-8")

    def test_anchor_ids_present_in_template(self):
        for a in ANCHORS:
            self.assertIn('id="%s"' % a, self.tpl, "template missing id %r" % a)

    def test_js_getElementById_ids_all_anchored(self):
        ids = set(re.findall(r"getElementById\(['\"]([\w-]+)['\"]\)", self.js))
        for i in ids:
            self.assertIn('id="%s"' % i, self.tpl, "JS reads #%s not present in template" % i)

    def test_partial_extends_and_loads_js(self):
        self.assertEqual(
            self.tpl.splitlines()[0].strip(),
            '{% extends "_bare.html" if partial else "_dashboard_shell.html" %}')
        self.assertIn("js/ia-subjects.js", self.tpl)


if __name__ == "__main__":
    unittest.main()
