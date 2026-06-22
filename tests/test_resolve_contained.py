import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.papers import resolve_contained


class ResolveContainedTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.base = self.tmp / "papers"
        self.base.mkdir()
        (self.base / "ok.pdf").write_text("x")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_accepts_basename(self):
        p = resolve_contained(self.base, "ok.pdf")
        self.assertIsNotNone(p)
        self.assertTrue(p.is_relative_to(self.base.resolve()))

    def test_must_exist_true_accepts_present_file(self):
        self.assertIsNotNone(resolve_contained(self.base, "ok.pdf", must_exist=True))

    def test_must_exist_true_rejects_absent_file(self):
        self.assertIsNone(resolve_contained(self.base, "missing.pdf", must_exist=True))

    def test_must_exist_false_accepts_absent_file(self):
        # pre-write callers: a Path is returned even though the file is not there yet
        self.assertIsNotNone(resolve_contained(self.base, "new.pdf", must_exist=False))

    def test_rejects_parent_traversal(self):
        (self.tmp / "secret.pdf").write_text("s")
        self.assertIsNone(resolve_contained(self.base, "../secret.pdf"))

    def test_rejects_absolute_path(self):
        self.assertIsNone(resolve_contained(self.base, "/etc/passwd"))

    def test_rejects_symlink_escape(self):
        # safe_join would permit this (the name is just "link.pdf"); resolve() must not
        (self.tmp / "secret.pdf").write_text("s")
        os.symlink(self.tmp / "secret.pdf", self.base / "link.pdf")
        self.assertIsNone(resolve_contained(self.base, "link.pdf", must_exist=True))


if __name__ == "__main__":
    unittest.main()
