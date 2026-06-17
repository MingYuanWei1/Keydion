import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import support  # noqa: E402


class UploadWizardGateContractTest(unittest.TestCase):
    """The abstract/IA auto-extract buttons are gated on EITHER model that can
    drive extraction being configured for a contributor; the EE extract button
    stays always-on. Both generators are vision-first (generate_abstract_keywords
    / generate_ia_scores return from the vision branch before touching the chat
    client), so a vision-only deployment can extract end-to-end — the gate must
    include vision_enabled(), per spec VD1/§7."""

    def test_extract_assist_gate_expression(self):
        src = support.source_of("_render_upload")
        self.assertIn("extract_assist_enabled", src)
        self.assertIn("(llm_client.vision_enabled() or llm_client.llm_enabled()) and _role >= 2", src)
        self.assertNotIn("\"llm_metadata_enabled\"", src)

    def test_wizard_js_consumes_renamed_flag(self):
        js = (ROOT / "static" / "js" / "upload-wizard.js").read_text()
        self.assertIn("BOOT.extract_assist_enabled", js)
        self.assertNotIn("BOOT.llm_metadata_enabled", js)


if __name__ == "__main__":
    unittest.main()
