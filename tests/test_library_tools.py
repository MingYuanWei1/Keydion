"""Tests for library_tools — pure unit tests, no Flask/DB required."""

import sys
import os
import types
import unittest

# Ensure project root is on path when run directly.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import library_tools
from library_tools import (
    PAPER_TEXT_CHAR_CAP,
    TOOL_SCHEMAS,
    SourceRegistry,
    run_tool,
)


# ---------------------------------------------------------------------------
# Fake deps
# ---------------------------------------------------------------------------

def _make_deps(papers=None):
    """Return a simple fake deps object backed by a dict of paper dicts.

    Each paper dict may have: filename, title, authors, url, text, snippet.
    """
    papers = papers or {}

    def search(query):
        # Return all papers whose title or text contains the query (case-insensitive).
        q = query.lower()
        results = []
        for fn, p in papers.items():
            if q in (p.get("title", "") + p.get("text", "")).lower():
                results.append({
                    "filename": fn,
                    "title": p.get("title", ""),
                    "authors": p.get("authors", ""),
                    "url": p.get("url", ""),
                    "snippet": p.get("snippet", p.get("text", "")[:100]),
                })
        return results

    def full_text(filename):
        p = papers.get(filename)
        if p is None:
            return ""
        return p.get("text", "")

    def paper_meta(filename):
        p = papers.get(filename)
        if p is None:
            return {}
        return {"title": p.get("title", ""), "authors": p.get("authors", "")}

    def paper_url(filename):
        p = papers.get(filename)
        if p is None:
            return None
        return p.get("url")

    return types.SimpleNamespace(
        search=search,
        full_text=full_text,
        paper_meta=paper_meta,
        paper_url=paper_url,
    )


_SAMPLE_PAPERS = {
    "alpha.pdf": {
        "title": "Alpha Study",
        "authors": "Smith, J.",
        "url": "https://example.com/alpha",
        "text": "alpha content about alpha research",
        "snippet": "alpha snippet",
    },
    "beta.pdf": {
        "title": "Beta Research",
        "authors": "Doe, A.",
        "url": "https://example.com/beta",
        "text": "beta content about beta topic",
        "snippet": "beta snippet",
    },
}


# ---------------------------------------------------------------------------
# TOOL_SCHEMAS shape
# ---------------------------------------------------------------------------

class TestToolSchemas(unittest.TestCase):
    def test_exactly_two_tools(self):
        self.assertEqual(len(TOOL_SCHEMAS), 2)

    def test_names_are_search_library_and_read_paper(self):
        names = [t["function"]["name"] for t in TOOL_SCHEMAS]
        self.assertIn("search_library", names)
        self.assertIn("read_paper", names)

    def _schema_for(self, name):
        for t in TOOL_SCHEMAS:
            if t["function"]["name"] == name:
                return t
        self.fail(f"No tool named {name!r}")

    def test_search_library_required_contains_query(self):
        schema = self._schema_for("search_library")
        params = schema["function"]["parameters"]
        self.assertIn("query", params["required"])
        self.assertIn("query", params["properties"])
        self.assertEqual(params["properties"]["query"]["type"], "string")

    def test_read_paper_required_contains_filename(self):
        schema = self._schema_for("read_paper")
        params = schema["function"]["parameters"]
        self.assertIn("filename", params["required"])
        self.assertIn("filename", params["properties"])
        self.assertEqual(params["properties"]["filename"]["type"], "string")

    def test_each_schema_has_type_function(self):
        for t in TOOL_SCHEMAS:
            self.assertEqual(t["type"], "function")

    def test_each_function_has_description(self):
        for t in TOOL_SCHEMAS:
            desc = t["function"].get("description", "")
            self.assertTrue(desc.strip(), f"Empty description for {t['function']['name']}")


# ---------------------------------------------------------------------------
# SourceRegistry
# ---------------------------------------------------------------------------

class TestSourceRegistry(unittest.TestCase):
    def test_first_registration_returns_1(self):
        reg = SourceRegistry()
        n = reg.register("a.pdf", {"title": "A", "authors": "Author A", "url": ""})
        self.assertEqual(n, 1)

    def test_second_filename_returns_2(self):
        reg = SourceRegistry()
        reg.register("a.pdf", {"title": "A", "authors": "", "url": ""})
        n = reg.register("b.pdf", {"title": "B", "authors": "", "url": ""})
        self.assertEqual(n, 2)

    def test_idempotent_same_filename_same_n(self):
        reg = SourceRegistry()
        n1 = reg.register("a.pdf", {"title": "A", "authors": "", "url": ""})
        n2 = reg.register("a.pdf", {"title": "A", "authors": "", "url": ""})
        self.assertEqual(n1, n2)

    def test_repeat_call_does_not_increment_counter(self):
        reg = SourceRegistry()
        reg.register("a.pdf", {"title": "A", "authors": "", "url": ""})
        reg.register("a.pdf", {"title": "A", "authors": "", "url": ""})
        n = reg.register("b.pdf", {"title": "B", "authors": "", "url": ""})
        self.assertEqual(n, 2)

    def test_backfills_empty_fields_on_repeat_call(self):
        reg = SourceRegistry()
        reg.register("a.pdf", {"title": "", "authors": "", "url": ""})
        reg.register("a.pdf", {"title": "Alpha", "authors": "Smith", "url": "http://x"})
        citations = reg.as_citations()
        self.assertEqual(citations[0]["title"], "Alpha")
        self.assertEqual(citations[0]["authors"], "Smith")
        self.assertEqual(citations[0]["url"], "http://x")

    def test_backfill_does_not_overwrite_existing_data(self):
        reg = SourceRegistry()
        reg.register("a.pdf", {"title": "Original", "authors": "First", "url": ""})
        reg.register("a.pdf", {"title": "New Title", "authors": "Second", "url": "http://y"})
        citations = reg.as_citations()
        # Title and authors already set — must not be overwritten.
        self.assertEqual(citations[0]["title"], "Original")
        self.assertEqual(citations[0]["authors"], "First")
        # url was empty, so it should be backfilled.
        self.assertEqual(citations[0]["url"], "http://y")

    def test_empty_filename_returns_none(self):
        reg = SourceRegistry()
        result = reg.register("", {"title": "X", "authors": "", "url": ""})
        self.assertIsNone(result)

    def test_empty_filename_does_not_pollute_registry(self):
        reg = SourceRegistry()
        reg.register("", {"title": "X", "authors": "", "url": ""})
        self.assertEqual(reg.as_citations(), [])

    def test_as_citations_ascending_order(self):
        reg = SourceRegistry()
        reg.register("b.pdf", {"title": "B", "authors": "", "url": ""})
        reg.register("a.pdf", {"title": "A", "authors": "", "url": ""})
        reg.register("c.pdf", {"title": "C", "authors": "", "url": ""})
        ns = [c["n"] for c in reg.as_citations()]
        self.assertEqual(ns, [1, 2, 3])

    def test_as_citations_has_five_keys(self):
        reg = SourceRegistry()
        reg.register("a.pdf", {"title": "A", "authors": "X", "url": "http://u"})
        c = reg.as_citations()[0]
        for key in ("n", "filename", "title", "authors", "url"):
            self.assertIn(key, c)

    def test_title_falls_back_to_filename(self):
        reg = SourceRegistry()
        reg.register("a.pdf", {"title": "", "authors": "", "url": ""})
        c = reg.as_citations()[0]
        self.assertEqual(c["title"], "a.pdf")

    def test_empty_registry_returns_empty_list(self):
        reg = SourceRegistry()
        self.assertEqual(reg.as_citations(), [])


# ---------------------------------------------------------------------------
# run_tool — search_library
# ---------------------------------------------------------------------------

class TestRunToolSearchLibrary(unittest.TestCase):
    def setUp(self):
        self.deps = _make_deps(_SAMPLE_PAPERS)
        self.registry = SourceRegistry()

    def test_registers_candidates_with_sequential_numbers(self):
        # Search for "alpha" returns only alpha.pdf
        result = run_tool("search_library", '{"query": "alpha"}', self.registry, self.deps)
        self.assertIn("[1]", result)

    def test_result_contains_title(self):
        result = run_tool("search_library", '{"query": "alpha"}', self.registry, self.deps)
        self.assertIn("Alpha Study", result)

    def test_result_contains_authors(self):
        result = run_tool("search_library", '{"query": "alpha"}', self.registry, self.deps)
        self.assertIn("Smith, J.", result)

    def test_result_contains_filename(self):
        result = run_tool("search_library", '{"query": "alpha"}', self.registry, self.deps)
        self.assertIn("alpha.pdf", result)

    def test_result_contains_snippet(self):
        result = run_tool("search_library", '{"query": "alpha"}', self.registry, self.deps)
        self.assertIn("alpha snippet", result)

    def test_multiple_candidates_numbered_sequentially(self):
        # "content" appears in both papers' text — search all
        result = run_tool(
            "search_library", '{"query": "content"}', self.registry, self.deps
        )
        self.assertIn("[1]", result)
        self.assertIn("[2]", result)

    def test_no_results_returns_error_string(self):
        result = run_tool(
            "search_library", '{"query": "zzz_no_match_xyz"}', self.registry, self.deps
        )
        self.assertIsInstance(result, str)
        self.assertIn("zzz_no_match_xyz", result)
        # No citation registered.
        self.assertEqual(self.registry.as_citations(), [])

    def test_empty_query_returns_error_string(self):
        result = run_tool("search_library", '{"query": ""}', self.registry, self.deps)
        self.assertIsInstance(result, str)
        self.assertTrue(result.strip())
        self.assertIn("Error", result)

    def test_dict_arguments_accepted(self):
        result = run_tool("search_library", {"query": "alpha"}, self.registry, self.deps)
        self.assertIn("Alpha Study", result)

    def test_accepts_string_arguments(self):
        """Regression: arguments is a JSON string, not a dict."""
        result = run_tool("search_library", '{"query": "beta"}', self.registry, self.deps)
        self.assertIn("Beta Research", result)


# ---------------------------------------------------------------------------
# run_tool — read_paper
# ---------------------------------------------------------------------------

class TestRunToolReadPaper(unittest.TestCase):
    def setUp(self):
        self.deps = _make_deps(_SAMPLE_PAPERS)
        self.registry = SourceRegistry()

    def test_returns_source_prefix_with_n(self):
        result = run_tool(
            "read_paper", '{"filename": "alpha.pdf"}', self.registry, self.deps
        )
        self.assertIn("Source [1]:", result)

    def test_returns_title_in_header(self):
        result = run_tool(
            "read_paper", '{"filename": "alpha.pdf"}', self.registry, self.deps
        )
        self.assertIn("Alpha Study", result)

    def test_returns_paper_text(self):
        result = run_tool(
            "read_paper", '{"filename": "alpha.pdf"}', self.registry, self.deps
        )
        self.assertIn("alpha content", result)

    def test_registers_citation(self):
        run_tool("read_paper", '{"filename": "alpha.pdf"}', self.registry, self.deps)
        citations = self.registry.as_citations()
        self.assertEqual(len(citations), 1)
        self.assertEqual(citations[0]["filename"], "alpha.pdf")

    def test_unknown_filename_returns_error_string(self):
        result = run_tool(
            "read_paper", '{"filename": "ghost.pdf"}', self.registry, self.deps
        )
        self.assertIsInstance(result, str)
        self.assertIn("ghost.pdf", result)

    def test_unknown_filename_does_not_register_citation(self):
        run_tool("read_paper", '{"filename": "ghost.pdf"}', self.registry, self.deps)
        self.assertEqual(self.registry.as_citations(), [])

    def test_empty_filename_returns_error_string(self):
        result = run_tool(
            "read_paper", '{"filename": ""}', self.registry, self.deps
        )
        self.assertIsInstance(result, str)
        self.assertIn("Error", result)

    def test_empty_filename_does_not_register_citation(self):
        run_tool("read_paper", '{"filename": ""}', self.registry, self.deps)
        self.assertEqual(self.registry.as_citations(), [])

    def test_truncation_at_cap(self):
        long_text = "x" * (PAPER_TEXT_CHAR_CAP + 500)
        big_papers = {
            "big.pdf": {
                "title": "Big Paper",
                "authors": "Someone",
                "url": "",
                "text": long_text,
                "snippet": "snippet",
            }
        }
        deps = _make_deps(big_papers)
        result = run_tool(
            "read_paper", '{"filename": "big.pdf"}', SourceRegistry(), deps
        )
        # Result must end with [truncated] marker.
        self.assertTrue(result.endswith("[truncated]"))
        # The actual text in the result must not exceed the cap + header + marker.
        # Just verify the "x" section is capped.
        x_count = result.count("x")
        self.assertLessEqual(x_count, PAPER_TEXT_CHAR_CAP)

    def test_short_paper_not_truncated(self):
        result = run_tool(
            "read_paper", '{"filename": "alpha.pdf"}', self.registry, self.deps
        )
        self.assertNotIn("[truncated]", result)

    def test_paper_seen_in_search_keeps_original_n_when_read(self):
        """Stability: a paper first found via search keeps its [n] when later read."""
        # Search registers alpha.pdf as [1] and beta.pdf as [2].
        run_tool("search_library", '{"query": "content"}', self.registry, self.deps)
        citations_after_search = {c["filename"]: c["n"] for c in self.registry.as_citations()}

        # Now read alpha.pdf — it should still be [1].
        result = run_tool(
            "read_paper", '{"filename": "alpha.pdf"}', self.registry, self.deps
        )
        n = citations_after_search["alpha.pdf"]
        self.assertIn(f"Source [{n}]:", result)
        # Total citations must not grow.
        self.assertEqual(len(self.registry.as_citations()), 2)


# ---------------------------------------------------------------------------
# run_tool — malformed JSON and unknown tool
# ---------------------------------------------------------------------------

class TestRunToolEdgeCases(unittest.TestCase):
    def setUp(self):
        self.deps = _make_deps(_SAMPLE_PAPERS)
        self.registry = SourceRegistry()

    def test_malformed_json_returns_error_string(self):
        result = run_tool("search_library", "{not valid json", self.registry, self.deps)
        self.assertIsInstance(result, str)
        self.assertTrue(result.strip())
        # Should not crash; should mention error.
        self.assertIn("Error", result)

    def test_unknown_tool_name_returns_error_string(self):
        result = run_tool("banana_tool", '{"x": 1}', self.registry, self.deps)
        self.assertIsInstance(result, str)
        self.assertIn("banana_tool", result)

    def test_constant_paper_text_char_cap_value(self):
        self.assertEqual(PAPER_TEXT_CHAR_CAP, 200_000)


# ---------------------------------------------------------------------------
# run_tool — defensive / error-handling paths (new)
# ---------------------------------------------------------------------------

class TestRunToolDefensive(unittest.TestCase):
    """Tests for the 'never raises' guarantee and defensive normalisation."""

    def setUp(self):
        self.registry = SourceRegistry()

    # --- deps.search raises ---------------------------------------------------

    def test_search_dep_raises_returns_error_string(self):
        """If deps.search raises, run_tool must return an Error string, not propagate."""
        def bad_search(query):
            raise RuntimeError("DB connection pool timeout")

        deps = types.SimpleNamespace(
            search=bad_search,
            full_text=lambda fn: "",
            paper_meta=lambda fn: {},
            paper_url=lambda fn: None,
        )
        result = run_tool("search_library", '{"query": "climate"}', self.registry, deps)
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("Error"))
        self.assertNotEqual(result, "")

    # --- deps.full_text raises ------------------------------------------------

    def test_full_text_dep_raises_returns_error_string(self):
        """If deps.full_text raises, run_tool must return an Error string, not propagate."""
        def bad_full_text(filename):
            raise OSError("missing row in DB")

        deps = types.SimpleNamespace(
            search=lambda q: [{"filename": "x.pdf", "title": "X", "authors": "", "url": "", "snippet": ""}],
            full_text=bad_full_text,
            paper_meta=lambda fn: {},
            paper_url=lambda fn: None,
        )
        result = run_tool("read_paper", '{"filename": "x.pdf"}', self.registry, deps)
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("Error"))

    # --- non-dict JSON payload ------------------------------------------------

    def test_non_dict_json_scalar_returns_error_string(self):
        """A bare JSON scalar like '42' must yield an error string, not AttributeError."""
        deps = _make_deps(_SAMPLE_PAPERS)
        result = run_tool("search_library", "42", self.registry, deps)
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("Error"))

    def test_non_dict_json_array_returns_error_string(self):
        """A bare JSON array like '[1,2]' must yield an error string."""
        deps = _make_deps(_SAMPLE_PAPERS)
        result = run_tool("search_library", "[1, 2]", self.registry, deps)
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("Error"))

    def test_non_dict_passed_directly_returns_error_string(self):
        """A non-dict passed as arguments directly must yield an error string."""
        deps = _make_deps(_SAMPLE_PAPERS)
        result = run_tool("search_library", 42, self.registry, deps)
        self.assertIsInstance(result, str)
        self.assertTrue(result.startswith("Error"))

    # --- non-string arg values ------------------------------------------------

    def test_numeric_query_value_does_not_crash(self):
        """args={'query': 5} must not raise AttributeError on .strip()."""
        deps = _make_deps(_SAMPLE_PAPERS)
        result = run_tool("search_library", {"query": 5}, self.registry, deps)
        # Either finds results for "5" or returns a no-match / error string — must not crash.
        self.assertIsInstance(result, str)

    def test_numeric_filename_value_does_not_crash(self):
        """args={'filename': 123} must not raise AttributeError on .strip()."""
        deps = _make_deps(_SAMPLE_PAPERS)
        result = run_tool("read_paper", {"filename": 123}, self.registry, deps)
        self.assertIsInstance(result, str)

    # --- candidate missing filename / None fields ----------------------------

    def test_candidate_missing_filename_is_skipped(self):
        """A search candidate with no filename must not produce a [None] block."""
        def bad_search(query):
            return [
                {"filename": None, "title": "Ghost", "authors": "X", "url": "", "snippet": "s"},
                {"title": "Also missing", "authors": "Y", "url": "", "snippet": "t"},
            ]

        deps = types.SimpleNamespace(
            search=bad_search,
            full_text=lambda fn: "",
            paper_meta=lambda fn: {},
            paper_url=lambda fn: None,
        )
        result = run_tool("search_library", '{"query": "anything"}', self.registry, deps)
        self.assertNotIn("[None]", result)
        # No citations should be registered for filename-less candidates.
        self.assertEqual(self.registry.as_citations(), [])

    def test_candidate_with_none_snippet_does_not_render_none_text(self):
        """A candidate with snippet=None must not render the literal string 'None'."""
        def search_with_none_snippet(query):
            return [
                {"filename": "a.pdf", "title": "Alpha", "authors": "Smith", "url": "", "snippet": None},
            ]

        deps = types.SimpleNamespace(
            search=search_with_none_snippet,
            full_text=lambda fn: "",
            paper_meta=lambda fn: {},
            paper_url=lambda fn: None,
        )
        result = run_tool("search_library", '{"query": "alpha"}', self.registry, deps)
        # The block must be present (a.pdf was returned).
        self.assertIn("a.pdf", result)
        # The literal word "None" must not appear in the output.
        self.assertNotIn("None", result)

    def test_candidate_with_none_authors_does_not_render_none_text(self):
        """A candidate with authors=None must not render the literal string 'None'."""
        def search_with_none_authors(query):
            return [
                {"filename": "b.pdf", "title": "Beta", "authors": None, "url": "", "snippet": "some snippet"},
            ]

        deps = types.SimpleNamespace(
            search=search_with_none_authors,
            full_text=lambda fn: "",
            paper_meta=lambda fn: {},
            paper_url=lambda fn: None,
        )
        result = run_tool("search_library", '{"query": "beta"}', self.registry, deps)
        self.assertIn("b.pdf", result)
        self.assertNotIn("None", result)

    def test_mixed_candidates_valid_and_missing_filename(self):
        """Only candidates with a valid filename appear in output."""
        def mixed_search(query):
            return [
                {"filename": None, "title": "No name", "authors": "", "url": "", "snippet": "x"},
                {"filename": "real.pdf", "title": "Real", "authors": "A", "url": "", "snippet": "y"},
            ]

        deps = types.SimpleNamespace(
            search=mixed_search,
            full_text=lambda fn: "",
            paper_meta=lambda fn: {},
            paper_url=lambda fn: None,
        )
        result = run_tool("search_library", '{"query": "test"}', self.registry, deps)
        self.assertIn("real.pdf", result)
        self.assertNotIn("[None]", result)
        # Only one citation registered.
        self.assertEqual(len(self.registry.as_citations()), 1)


if __name__ == "__main__":
    unittest.main()
