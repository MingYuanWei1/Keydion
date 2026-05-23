import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GuidesCssContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        css_path = ROOT / "static" / "css" / "guides.css"
        cls.css = css_path.read_text(encoding="utf-8") if css_path.exists() else ""

    def test_defines_design_tokens(self):
        # CSS custom properties under :root
        for var in [
            "--cream", "--cream-2", "--paper", "--border", "--border-soft",
            "--ink", "--ink-soft", "--muted", "--muted-2",
            "--accent", "--accent-hover", "--accent-tint", "--gold",
            "--serif", "--display", "--sans", "--mono",
        ]:
            self.assertIn(var, self.css, f"guides.css missing token {var}")


if __name__ == "__main__":
    unittest.main()
