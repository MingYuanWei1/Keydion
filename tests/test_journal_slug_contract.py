"""Contract: journal slug generation is name-based, case-preserving, and unique."""
import unittest

from services.journals import slugify, _unique_slug


class SlugifyTest(unittest.TestCase):
    def test_spaces_become_underscores_case_preserved(self):
        self.assertEqual(slugify("IB EE"), "IB_EE")

    def test_punctuation_dropped(self):
        self.assertEqual(slugify("A/B: Test!"), "AB_Test")

    def test_collapses_whitespace_runs(self):
        self.assertEqual(slugify("  Youth   Research  "), "Youth_Research")

    def test_non_ascii_yields_empty(self):
        self.assertEqual(slugify("中文期刊"), "")

    def test_keeps_existing_hyphens_and_digits(self):
        self.assertEqual(slugify("Vol-2 2025"), "Vol-2_2025")


class UniqueSlugTest(unittest.TestCase):
    def test_returns_base_when_free(self):
        self.assertEqual(_unique_slug("IB_EE", set()), "IB_EE")

    def test_suffixes_on_collision(self):
        self.assertEqual(_unique_slug("IB_EE", {"IB_EE"}), "IB_EE_2")

    def test_skips_taken_suffixes(self):
        self.assertEqual(_unique_slug("IB_EE", {"IB_EE", "IB_EE_2"}), "IB_EE_3")


if __name__ == "__main__":
    unittest.main()
