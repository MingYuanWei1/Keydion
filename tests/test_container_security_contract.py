import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ContainerSecurityContractTests(unittest.TestCase):
    def test_build_context_excludes_secrets_runtime_data_and_archives(self):
        rules = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        required = {
            ".env",
            ".env.*",
            "papers/",
            "data/",
            "resource_files/",
            "static/uploads/",
            "backups/",
            "*.key",
            "*.pem",
            "*.p12",
            "*.pfx",
            "*.sql",
            "*.dump",
            "*.tar",
            "*.tar.gz",
        }
        self.assertTrue(required.issubset(set(rules)))
        self.assertIn("!data/*.sample.json", rules)
        self.assertIn("!.env.example", rules)

    def test_dockerfile_uses_explicit_allowlist_and_non_root_runtime(self):
        source = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        copy_lines = [line.strip() for line in source.splitlines() if line.startswith("COPY ")]
        self.assertNotIn("COPY . /app", copy_lines)
        self.assertNotIn("COPY . .", copy_lines)
        self.assertIn("chmod -R a+rX /app", source)
        self.assertIn("USER keydion:keydion", source)
        self.assertIn("/var/run/keydion", source)
        self.assertIn("chown -R keydion:keydion", source)


if __name__ == "__main__":
    unittest.main()
