# tests/test_en_msgstr_correctness_contract.py
"""Contract: EN locale msgstr values must equal their msgid.

For the English locale, every msgstr must either be empty (untranslated/fuzzy)
or must equal the msgid exactly — a non-empty msgstr that differs from its msgid
is a mistranslation (wrong borrowed string from an unrelated entry).

This test specifically guards four historically mis-wired entries that caused
user-visible confusion:
- "Uploading… %(pct)s%" had msgstr "Upload" (truncated, lost the % placeholder)
- "Upload failed. Please check your connection and try again." had an auth error msgstr
- "Upload failed. Please try again." had the same auth error msgstr
- "Network error. Please try again." had a session-expiry msgstr
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EN_PO = ROOT / "translations" / "en" / "LC_MESSAGES" / "messages.po"


def _entries(po_path):
    """Parse a .po file into a list of (msgid, msgstr) string pairs.

    Only includes entries where both msgid and msgstr are non-empty single-line
    strings (multi-line / plural forms are skipped — they are not the target).
    Fuzzy entries ARE included because a non-empty fuzzy msgstr is still served.
    """
    text = po_path.read_text(encoding="utf-8")
    entries = []
    for block in re.split(r"\n\n+", text):
        mid = re.search(r'^msgid "(.+)"$', block, re.MULTILINE)
        mstr = re.search(r'^msgstr "(.+)"$', block, re.MULTILINE)
        if mid and mstr:
            entries.append((mid.group(1), mstr.group(1)))
    return entries


class EnMsgstrCorrectnessTest(unittest.TestCase):
    """For the EN catalog a non-empty msgstr must equal its msgid."""

    @classmethod
    def setUpClass(cls):
        cls.entries = _entries(EN_PO)

    def _assert_msgstr(self, msgid, expected_msgstr):
        """Assert that the given msgid has exactly the expected msgstr."""
        found = [(mid, mstr) for mid, mstr in self.entries if mid == msgid]
        self.assertTrue(found, f"msgid {msgid!r} not found in EN catalog")
        _, actual = found[0]
        self.assertEqual(
            actual,
            expected_msgstr,
            f"EN msgid {msgid!r}: expected msgstr {expected_msgstr!r}, got {actual!r}",
        )

    def test_uploading_progress_msgstr_contains_placeholder(self):
        """Uploading progress string must keep the %(pct)s placeholder."""
        self._assert_msgstr(
            "Uploading… %(pct)s%",
            "Uploading… %(pct)s%",
        )

    def test_upload_failed_connection_error_msgstr(self):
        """Network-failure upload error must not show auth config message."""
        self._assert_msgstr(
            "Upload failed. Please check your connection and try again.",
            "Upload failed. Please check your connection and try again.",
        )

    def test_upload_failed_retry_msgstr(self):
        """Server-side upload rejection must not show auth config message."""
        self._assert_msgstr(
            "Upload failed. Please try again.",
            "Upload failed. Please try again.",
        )

    def test_network_error_msgstr_not_session_expired(self):
        """Paper-manage dashboard network error must not show session-expiry message."""
        self._assert_msgstr(
            "Network error. Please try again.",
            "Network error. Please try again.",
        )


if __name__ == "__main__":
    unittest.main()
