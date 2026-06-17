import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import support  # noqa: E402


class UploadWizardGateContractTest(unittest.TestCase):
    """The abstract/IA auto-extract buttons are gated on vision OR chat LLM
    being configured for a contributor; the EE extract button stays always-on."""

    def test_extract_assist_gate_expression(self):
        src = support.source_of("_render_upload")
        self.assertIn("extract_assist_enabled", src)
        self.assertIn(
            "(llm_client.vision_enabled() or llm_client.llm_enabled())",
            src,
        )
        self.assertNotIn("\"llm_metadata_enabled\"", src)

    def test_wizard_js_consumes_renamed_flag(self):
        js = (ROOT / "static" / "js" / "upload-wizard.js").read_text()
        self.assertIn("BOOT.extract_assist_enabled", js)
        self.assertNotIn("BOOT.llm_metadata_enabled", js)


if __name__ == "__main__":
    unittest.main()
