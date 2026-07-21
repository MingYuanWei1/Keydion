"""Catalog contract for publishing-lifecycle feedback.

The source and compiled catalogs are parsed with Babel so this contract checks
active messages and their real metadata rather than matching PO file text.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from babel.messages import mofile, pofile


ROOT = Path(__file__).resolve().parents[1]
POT_PATH = ROOT / "messages.pot"
PO_PATHS = {
    "en": ROOT / "translations" / "en" / "LC_MESSAGES" / "messages.po",
    "zh": ROOT / "translations" / "zh" / "LC_MESSAGES" / "messages.po",
}
MO_PATHS = {locale: path.with_suffix(".mo") for locale, path in PO_PATHS.items()}

APPROVED_TRANSLATIONS = {
    "%(paper_name)s uploaded successfully, but RAG indexing failed.": (
        "%(paper_name)s 已上传成功，但 RAG 索引失败。"
    ),
    "%(paper_name)s published successfully, but RAG indexing failed.": (
        "%(paper_name)s 已发布成功，但 RAG 索引失败。"
    ),
    "Submission cancelled.": "投稿已取消。",
    "Only a pending submission can be cancelled.": "只能取消待审核的投稿。",
    "Published paper unavailable": "已发布的论文不可用",
    "This paper changed while you were editing it. Reload and try again.": (
        "您编辑期间该论文已发生更改。请重新加载后再试。"
    ),
    "Paper revision %(revision)s published.": "论文修订版 %(revision)s 已发布。",
    "Revision %(revision)s restored as revision %(new_revision)s.": (
        "修订版 %(revision)s 已恢复为修订版 %(new_revision)s。"
    ),
    "Paper deletion is still in progress. The paper is no longer accessible.": (
        "论文删除仍在进行中。该论文已无法访问。"
    ),
    "Deleted %(paper_name)s.": "已删除 %(paper_name)s。",
    "Replace PDF (optional)": "替换 PDF（可选）",
    "Revision history": "修订历史",
    "Current revision": "当前修订版",
    "Restore this revision? A new revision will be created.": (
        "要恢复此修订版吗？系统将创建一个新的修订版。"
    ),
}

PYTHON_PLACEHOLDER = re.compile(
    r"%\(([^)]+)\)[#0\- +]*(?:\d+|\*)?(?:\.(?:\d+|\*))?[diouxXeEfFgGcrsa]"
)


def _read_po(path: Path):
    with path.open("r", encoding="utf-8") as catalog_file:
        return pofile.read_po(catalog_file)


def _read_mo(path: Path):
    with path.open("rb") as catalog_file:
        return mofile.read_mo(catalog_file)


def _placeholders(message: str) -> set[str]:
    return set(PYTHON_PLACEHOLDER.findall(message))


class PublishingLifecycleI18nContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pot = _read_po(POT_PATH)
        cls.po_catalogs = {locale: _read_po(path) for locale, path in PO_PATHS.items()}
        cls.mo_catalogs = {locale: _read_mo(path) for locale, path in MO_PATHS.items()}

    def test_required_msgids_are_active_in_source_catalogs(self):
        catalogs = {"POT": self.pot, **self.po_catalogs}
        for catalog_name, catalog in catalogs.items():
            with self.subTest(catalog=catalog_name):
                missing = [
                    msgid for msgid in APPROVED_TRANSLATIONS if catalog.get(msgid) is None
                ]
                self.assertEqual([], missing)

    def test_required_entries_are_not_fuzzy(self):
        catalogs = {"POT": self.pot, **self.po_catalogs}
        for catalog_name, catalog in catalogs.items():
            for msgid in APPROVED_TRANSLATIONS:
                with self.subTest(catalog=catalog_name, msgid=msgid):
                    message = catalog.get(msgid)
                    self.assertIsNotNone(message)
                    self.assertNotIn("fuzzy", message.flags)

    def test_english_translations_resolve_safely_to_the_msgid(self):
        catalog = self.po_catalogs["en"]
        for msgid in APPROVED_TRANSLATIONS:
            with self.subTest(msgid=msgid):
                message = catalog.get(msgid)
                self.assertIsNotNone(message)
                self.assertEqual(msgid, message.string)

    def test_chinese_translations_match_the_approved_text(self):
        catalog = self.po_catalogs["zh"]
        for msgid, expected in APPROVED_TRANSLATIONS.items():
            with self.subTest(msgid=msgid):
                message = catalog.get(msgid)
                self.assertIsNotNone(message)
                self.assertTrue(message.string)
                self.assertEqual(expected, message.string)

    def test_translations_preserve_python_placeholder_sets(self):
        for locale, catalog in self.po_catalogs.items():
            for msgid in APPROVED_TRANSLATIONS:
                with self.subTest(locale=locale, msgid=msgid):
                    message = catalog.get(msgid)
                    self.assertIsNotNone(message)
                    self.assertEqual(_placeholders(msgid), _placeholders(message.string))

    def test_compiled_catalogs_contain_the_usable_translations(self):
        expected_by_locale = {
            "en": {msgid: msgid for msgid in APPROVED_TRANSLATIONS},
            "zh": APPROVED_TRANSLATIONS,
        }
        for locale, expected_translations in expected_by_locale.items():
            catalog = self.mo_catalogs[locale]
            for msgid, expected in expected_translations.items():
                with self.subTest(locale=locale, msgid=msgid):
                    message = catalog.get(msgid)
                    self.assertIsNotNone(message)
                    self.assertTrue(message.string)
                    self.assertEqual(expected, message.string)
                    self.assertEqual(_placeholders(msgid), _placeholders(message.string))


if __name__ == "__main__":
    unittest.main()
