# tests/test_related_i18n.py
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NEW = "No related texts found."
OLD = "No related texts found in this category."


def _msgstr_for(po_path, msgid):
    text = po_path.read_text(encoding="utf-8")
    m = re.search(r'msgid "' + re.escape(msgid) + r'"\nmsgstr "([^"]*)"', text)
    return m.group(1) if m else None


class RelatedStringI18n(unittest.TestCase):
    def test_template_uses_new_string(self):
        html = (ROOT / "templates" / "preview.html").read_text(encoding="utf-8")
        self.assertIn(NEW, html)
        self.assertNotIn(OLD, html)

    def test_zh_has_translation(self):
        s = _msgstr_for(ROOT / "translations" / "zh" / "LC_MESSAGES" / "messages.po", NEW)
        self.assertTrue(s, f"missing zh translation for {NEW!r}")

    def test_en_has_entry(self):
        s = _msgstr_for(ROOT / "translations" / "en" / "LC_MESSAGES" / "messages.po", NEW)
        self.assertTrue(s, f"missing en entry for {NEW!r}")


if __name__ == "__main__":
    unittest.main()
