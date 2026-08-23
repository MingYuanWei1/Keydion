"""Contract: provider probes connect only to vetted public addresses.

Security-review finding [11]: the probe resolved the submitted hostname,
checked that every answer was public, then DISCARDED the vetted addresses and
handed the hostname to the OpenAI SDK, which resolves again at connect time —
an attacker-controlled DNS name can answer public during validation and
private at connection (DNS rebinding).

Rules enforced here:
1. The probe never constructs an SDK/generic HTTP client from the base URL —
   every probe request flows through _pinned_probe_request.
2. _pinned_probe_request resolves once, refuses endpoints with no public
   answers, connects only to the vetted numeric addresses via the hardened
   pinned-connection helper, and preserves the original Host / TLS identity.
3. Oversized provider responses are refused.
"""
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import support

import services.llm_admin as la
import web_search


class ProbeTransportPinning(unittest.TestCase):
    def test_probe_functions_never_construct_sdk_clients(self):
        source = (
            support.source_of("_probe_models")
            + support.source_of("_probe_embedding")
        )
        self.assertNotIn("OpenAI(", source)
        self.assertIn("_pinned_probe_request(", source)

    def test_no_public_answers_are_refused_before_any_socket(self):
        with mock.patch.object(web_search, "_resolve_public_ips", return_value=()),              mock.patch.object(web_search, "_open_pinned_connection",
                               side_effect=AssertionError("must not connect")):
            with self.assertRaises(la.LLMAdminError):
                la._pinned_probe_request(
                    "https://provider.example.com", "sk-x", "GET", "models"
                )

    def test_request_connects_to_the_vetted_address_only(self):
        class FakeResponse:
            status = 200

            def read(self, _limit):
                return b'{"data": []}'

            def getheader(self, _name):
                return ""

        class FakeConnection:
            def __init__(self):
                self.sent = []

            def putrequest(self, method, target, **kwargs):
                self.sent.append(("method", method, target))

            def putheader(self, name, value):
                self.sent.append(("header", name, value))

            def endheaders(self, body=None):
                self.sent.append(("body", body))

            def getresponse(self):
                return FakeResponse()

            def close(self):
                pass

        opened = []

        def fake_open(parts, address, timeout):
            connection = FakeConnection()
            connection.address = address
            opened.append(connection)
            return connection

        with mock.patch.object(web_search, "_resolve_public_ips",
                               return_value=("203.0.113.10",)),              mock.patch.object(web_search, "_open_pinned_connection",
                               side_effect=fake_open):
            status, _payload = la._pinned_probe_request(
                "https://api.example.com/v1", "sk-x", "GET", "models"
            )

        self.assertEqual(status, 200)
        self.assertEqual(len(opened), 1)
        self.assertEqual(opened[0].address, "203.0.113.10")
        headers = {
            entry[1]: entry[2]
            for entry in opened[0].sent
            if entry[0] == "header"
        }
        self.assertEqual(headers.get("Host"), "api.example.com")
        self.assertEqual(headers.get("Authorization"), "Bearer sk-x")

    def test_oversized_response_is_refused(self):
        class FakeResponse:
            status = 200

            def read(self, _limit):
                return b"x" * (la._PROBE_MAX_RESPONSE_BYTES + 1)

            def getheader(self, _name):
                return ""

        class FakeConnection:
            def putrequest(self, *_args, **_kwargs):
                pass

            def putheader(self, *_args, **_kwargs):
                pass

            def endheaders(self, body=None):
                pass

            def getresponse(self):
                return FakeResponse()

            def close(self):
                pass

        with mock.patch.object(web_search, "_resolve_public_ips",
                               return_value=("203.0.113.10",)),              mock.patch.object(web_search, "_open_pinned_connection",
                               return_value=FakeConnection()):
            with self.assertRaises(la.LLMAdminError):
                la._pinned_probe_request(
                    "https://api.example.com", "sk-x", "GET", "models"
                )


if __name__ == "__main__":
    unittest.main()
