import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


class RateLimitContractTest(unittest.TestCase):
    def test_limiter_keys_on_remote_addr_not_spoofable_header(self):
        for fn in ("api_ai", "api_ai_attach"):
            src = support.source_of(fn)
            self.assertIn("request.remote_addr", src, f"{fn} must key on remote_addr")
            self.assertNotIn("X-Forwarded-For", src, f"{fn} must not parse raw XFF")
            self.assertNotIn('.split(",")[0]', src, f"{fn} must not take leftmost XFF")

    def test_proxyfix_hop_count_is_one(self):
        app_src = support.source_of("create_app")
        self.assertIn("ProxyFix", app_src)
        self.assertIn("x_for=1", app_src)
