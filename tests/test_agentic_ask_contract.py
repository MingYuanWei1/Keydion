# tests/test_agentic_ask_contract.py
"""Contract: /api/ai runs an agentic tool loop (search_library + read_paper)
over the library, with a legacy single-shot fallback when the provider lacks
tool support. Covers the source-level invariants, the agentic prompt builder,
and the tool-status text helper. A fake-client streaming test drives the real
loop through the test client when a database is available.
"""
import json
import os
import sys
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import app as app_module
import library_tools

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import support


class SourceContract(unittest.TestCase):
    def test_api_ask_wires_tool_loop(self):
        src = support.source_of("api_ai")
        self.assertIn("tools=", src)
        self.assertIn("run_tool", src)
        self.assertIn("build_tool_schemas", src)
        self.assertIn("MAX_TOOL_ROUNDS", src)

    def test_max_tool_rounds_is_five(self):
        self.assertEqual(app_module.MAX_TOOL_ROUNDS, 5)

    def test_legacy_fallback_and_round_cap_preserved(self):
        src = support.source_of("api_ai")
        # Fallback path reuses the original single-shot prompt builder.
        self.assertIn("_build_ask_prompt(", src)
        # Round-cap final call forces no further tool use.
        self.assertIn("tool_choice", src)


class AgenticPrompt(unittest.TestCase):
    def _candidates(self):
        return [
            {"n": 1, "title": "Photosynthesis in Algae", "authors": "Lee",
             "filename": "lee2020.pdf", "snippet": "Algae convert light...",
             "is_attachment": False},
            {"n": 2, "title": "Coral Reef Decline", "authors": "Ng",
             "filename": "ng2021.pdf", "snippet": "Reefs are bleaching...",
             "is_attachment": False},
        ]

    def test_lists_candidates_and_tools_english(self):
        p = app_module._build_agentic_ask_prompt("q", self._candidates(), [], "en")
        self.assertIn("[1]", p)
        self.assertIn("[2]", p)
        self.assertIn("Photosynthesis in Algae", p)
        self.assertIn("Coral Reef Decline", p)
        self.assertIn("read_paper", p)
        self.assertIn("search_library", p)
        self.assertIn("[n]", p)
        self.assertIn("English", p)
        self.assertNotIn("Answer in Chinese", p)

    def test_answers_in_chinese_for_zh(self):
        p = app_module._build_agentic_ask_prompt("q", self._candidates(), [], "zh")
        self.assertIn("Chinese", p)

    def test_empty_candidates_still_mentions_tools(self):
        p = app_module._build_agentic_ask_prompt("q", [], [], "en")
        self.assertTrue(p.strip())
        self.assertIn("read_paper", p)
        self.assertIn("search_library", p)

    def test_includes_web_sources(self):
        web = [{"n": 3, "title": "Live Result", "url": "http://x", "snippet": "snip"}]
        p = app_module._build_agentic_ask_prompt("q", self._candidates(), web, "en")
        self.assertIn("[3]", p)
        self.assertIn("Live Result", p)
        self.assertIn("(web)", p)


class ToolStatusText(unittest.TestCase):
    def test_search_status(self):
        reg = library_tools.SourceRegistry()
        out = app_module._tool_status_text("search_library", '{"query":"x"}', reg, None)
        self.assertIn("Searching", out)

    def test_read_status_uses_registered_title(self):
        reg = library_tools.SourceRegistry()
        reg.register("x.pdf", {"title": "A Fine Paper", "authors": "", "url": ""})
        out = app_module._tool_status_text(
            "read_paper", '{"filename":"x.pdf"}', reg, None)
        self.assertIn("A Fine Paper", out)

    def test_read_status_malformed_args_does_not_raise(self):
        reg = library_tools.SourceRegistry()
        out = app_module._tool_status_text("read_paper", "{not json", reg, None)
        self.assertTrue(isinstance(out, str) and out.strip())

    def test_unknown_tool_returns_nonempty(self):
        reg = library_tools.SourceRegistry()
        out = app_module._tool_status_text("mystery", "{}", reg, None)
        self.assertTrue(isinstance(out, str) and out.strip())


# ---------------------------------------------------------------------------
# Fake-client streaming test: drives the real generate() loop end-to-end via
# the test client. Round 1 emits a read_paper tool call; round 2 emits a final
# answer citing [1]. Asserts the SSE stream carries a status and a citations
# event. DB-dependent (the route runs retrieval), so it self-skips when MySQL
# is unreachable in the environment.
# ---------------------------------------------------------------------------

class _Fn:
    def __init__(self, name=None, arguments=None):
        self.name = name
        self.arguments = arguments


class _ToolCall:
    def __init__(self, index, id=None, name=None, arguments=None):
        self.index = index
        self.id = id
        self.function = _Fn(name, arguments)


class _Delta:
    def __init__(self, content=None, tool_calls=None):
        self.content = content
        self.tool_calls = tool_calls


class _Choice:
    def __init__(self, delta):
        self.delta = delta


class _Chunk:
    def __init__(self, delta):
        self.choices = [_Choice(delta)]


class _FakeCompletions:
    def __init__(self):
        self.calls = 0

    def create(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 1:
            # Round 1: a single read_paper tool call, arguments split in two.
            return iter([
                _Chunk(_Delta(tool_calls=[_ToolCall(0, id="call_1", name="read_paper")])),
                _Chunk(_Delta(tool_calls=[_ToolCall(0, arguments='{"filename":')])),
                _Chunk(_Delta(tool_calls=[_ToolCall(0, arguments='"lee2020.pdf"}')])),
            ])
        # Round 2: final answer that cites [1].
        return iter([
            _Chunk(_Delta(content="Algae photosynthesize [1].")),
        ])


class _FakeChat:
    def __init__(self):
        self.completions = _FakeCompletions()


class _FakeClient:
    def __init__(self):
        self.chat = _FakeChat()


def _make_client():
    try:
        app = app_module.create_app()
    except Exception as exc:  # pragma: no cover - environment dependent
        msg = str(exc).lower()
        if "connect" in msg or "refused" in msg or "mysql" in msg or "2003" in msg:
            raise unittest.SkipTest("database unavailable: %s" % exc)
        raise
    app.config["TESTING"] = True
    return app.test_client()


class AgenticLoopStreaming(unittest.TestCase):
    def setUp(self):
        self.client = _make_client()

    def test_loop_emits_status_and_citations(self):
        hit = {"filename": "lee2020.pdf", "title": "Photosynthesis in Algae",
               "content": "Algae convert light into energy.", "author_name": "Lee"}
        deps = mock.Mock()
        deps.full_text.return_value = "Full text of the algae paper."
        deps.paper_meta.return_value = {"title": "Photosynthesis in Algae", "authors": "Lee"}
        deps.paper_url.return_value = "/preview/lee2020.pdf"

        with mock.patch.dict(os.environ, {"LLM_API_KEY": "k"}, clear=False), \
             mock.patch.object(app_module.llm_client, "build_client",
                               return_value=_FakeClient()), \
             mock.patch.object(app_module.rag_index, "retrieve", return_value=[hit]), \
             mock.patch.object(app_module, "_attachment_grounding", return_value=[]), \
             mock.patch.object(app_module, "_build_library_deps", return_value=deps):
            resp = self.client.post("/api/ai",
                                    json={"question": "explain algae", "mode": "flash"})
            body = resp.get_data(as_text=True)

        self.assertIn('"type": "status"', body)
        self.assertIn('"type": "citations"', body)
        self.assertIn('"type": "done"', body)
        # The answer cited [1], so that source is shown.
        cite_events = [json.loads(line[len("data: "):])
                       for line in body.splitlines()
                       if line.startswith("data: ") and '"type": "citations"' in line]
        self.assertTrue(cite_events)
        items = cite_events[-1]["items"]
        self.assertEqual([it["n"] for it in items], [1])
        self.assertEqual(items[0]["filename"], "lee2020.pdf")


if __name__ == "__main__":
    unittest.main()
