import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class UploadTemplateDomContractTest(unittest.TestCase):
    def test_upload_javascript_references_existing_element_ids(self):
        upload_template = (ROOT / "templates" / "upload.html").read_text(encoding="utf-8")

        declared_ids = set(re.findall(r'\bid="([^"]+)"', upload_template))
        referenced_ids = set(
            re.findall(r'getElementById\(\s*["\']([^"\']+)["\']\s*\)', upload_template)
        )

        self.assertEqual([], sorted(referenced_ids - declared_ids))


if __name__ == "__main__":
    unittest.main()
