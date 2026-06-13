"""Contract: dashboard partial rendering is driven by the X-Partial-Content
header via the inject_partial_flag context processor.

Routes must NOT pass an explicit `partial=request.args.get("partial")` to
render_template: the JS partial loader sends a header (not a ?partial query
param), and an explicit render kwarg OVERRIDES the context processor (Flask
re-applies the passed context over processor values). The result is that a
partial fetch returns the full _dashboard_shell.html, which the loader injects
into #dashboardMain — nesting a second fixed-position shell + sidebar inside the
panel (the "sidebar jumps around" bug).
"""
import unittest

from tests.support import all_sources


class PartialFlagContractTest(unittest.TestCase):
    def test_no_route_reads_partial_from_query_args(self):
        src = all_sources()
        self.assertNotIn(
            'request.args.get("partial")', src,
            "Dashboard routes must rely on the inject_partial_flag context "
            "processor (header-based is_partial_request), not a ?partial query "
            "param. A query-param read is always None for the JS partial loader "
            "and overrides the context processor, returning the full shell.",
        )

    def test_partial_flag_comes_from_request_header(self):
        # The single source of truth: the context processor + is_partial_request.
        self.assertIn("is_partial_request()", all_sources())


if __name__ == "__main__":
    unittest.main()
