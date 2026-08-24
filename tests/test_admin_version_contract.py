import subprocess
import unittest
from unittest import mock

import services.version as version


class AdminVersionServiceContract(unittest.TestCase):
    def test_missing_git_degrades_without_crashing(self):
        with mock.patch.object(version.shutil, "which", return_value=None):
            result = version._git("rev-parse", "HEAD")
            snapshot = version.snapshot()
            ok, message = version.start_update()

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(snapshot["is_git"])
        self.assertIn("git is not installed", snapshot["check_error"])
        self.assertFalse(ok)
        self.assertIn("git is not installed", message)

    def test_snapshot_surfaces_the_real_git_error(self):
        stderr = (
            "fatal: detected dubious ownership in repository at '/Keydion'\n"
            "To add an exception, run: git config --global --add safe.directory /Keydion"
        )
        failed = subprocess.CompletedProcess(["git"], 128, "", stderr)
        with mock.patch.object(version.shutil, "which", return_value="/usr/bin/git"), \
             mock.patch.object(version.subprocess, "run", return_value=failed):
            snapshot = version.snapshot()

        self.assertIn("dubious ownership", snapshot["check_error"])
        self.assertIn("safe.directory", snapshot["check_error"])


if __name__ == "__main__":
    unittest.main()
