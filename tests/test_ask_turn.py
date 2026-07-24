# tests/test_ask_turn.py
"""Behavioral contract for the Ask turn module.

Drives `services.ask_turn.run_ask_turn` directly with a fake OpenAI client,
fake library deps, a recording `persist_assistant`, and a stub logger — no
Flask app context, no MySQL. Asserts on the event-dict stream, not on SSE
strings or source shape. Replaces the loop-shape grep contracts that used
to live in test_agentic_ask_contract.py.
"""
import json
import os
import unittest
from unittest import mock

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import library_tools
from services import ask_turn
from services.ai import MAX_TOOL_ROUNDS, WEB_SEARCH_CALL_CAP

PAPER_A_ID = "00000000-0000-4000-8000-000000000921"
PAPER_B_ID = "00000000-0000-4000-8000-000000000922"


# ---------------------------------------------------------------------------
# Fake OpenAI streaming client. Mirrors the chunk shape the real client yields:
# chunk.choices[0].delta.{content, tool_calls[*].{index,id,function.{name,args}}}
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


class _FakeCreate:
    """Scripts `chat.completions.create` calls in order.

    Each entry is either an Exception instance to raise, or a list of _Chunks
    to return as the stream. Differentiated by call index.
    """
    def __init__(self, rounds):
        self.rounds = list(rounds)
        self.calls = []

    def create(self, *args, **kwargs):
        self.calls.append(kwargs)
        idx = len(self.calls) - 1
        if idx >= len(self.rounds):
            raise AssertionError("fake client create() called more than scripted")
        step = self.rounds[idx]
        if isinstance(step, Exception):
            raise step
        return iter(step)


class _FakeChat:
    def __init__(self, create):
        self.completions = create


class _FakeClient:
    def __init__(self, create):
        self.chat = _FakeChat(create)


def _hit(paper_id, title, n_content, rev=1, filename=None):
    return {
        "paper_id": paper_id,
        "revision_number": rev,
        "filename": filename or f"{paper_id[:8]}.pdf",
        "title": title,
        "author_name": "Author",
        "content": n_content,
        "url": f"/paper/{paper_id}",
    }


def _build_input(client, deps, persist, *, hits, forced=None, citations=None,
                 web_results=None, include_web=False, attachment_names=None,
                 question="explain", mode="flash", model="test-model"):
    return ask_turn.AskTurnInput(
        question=question,
        llm_messages=[{"role": "user", "content": question}],
        mode=mode,
        model=model,
        forced=forced or [],
        hits=hits,
        citations=citations if citations is not None else [],
        web_results=web_results or [],
        locale_code="en",
        include_web=include_web,
        attachment_names=attachment_names or [],
        client=client,
        deps=deps,
        persist_assistant=persist,
        logger=mock.Mock(),
    )


def _events(inp):
    return list(ask_turn.run_ask_turn(inp))


def _types(events):
    return [e["type"] for e in events]


def _deps_for(paper_id, title="A Fine Paper"):
    deps = mock.Mock()
    deps.full_text.return_value = "Full text of the paper."
    deps.paper_meta.return_value = {
        "paper_id": paper_id,
        "revision_number": 1,
        "filename": "x.pdf",
        "title": title,
        "authors": "Author",
    }
    deps.paper_url.return_value = f"/paper/{paper_id}"
    deps.web_search.return_value = []
    return deps


class HappyPathTest(unittest.TestCase):
    def test_round1_tool_call_then_round2_answer_citing_1(self):
        # Round 1: read_paper tool call, arguments split across chunks.
        # Round 2: final answer citing [1].
        create = _FakeCreate([
            [_Chunk(_Delta(tool_calls=[
                _ToolCall(0, id="call_1", name="read_paper"),
            ])),
             _Chunk(_Delta(tool_calls=[
                 _ToolCall(0, arguments='{"paper_id":'),
             ])),
             _Chunk(_Delta(tool_calls=[
                 _ToolCall(0, arguments=f'"{PAPER_A_ID}"}}'),
             ]))],
            [_Chunk(_Delta(content="Algae photosynthesize [1]."))],
        ])
        client = _FakeClient(create)
        persist = mock.Mock()
        deps = _deps_for(PAPER_A_ID, "Photosynthesis in Algae")
        inp = _build_input(client, deps, persist,
                           hits=[_hit(PAPER_A_ID, "Photosynthesis in Algae", "Algae convert light.")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID, "revision_number": 1,
                                       "filename": "lee2020.pdf", "title": "Photosynthesis in Algae",
                                       "authors": "Lee", "url": f"/paper/{PAPER_A_ID}"}])

        events = _events(inp)

        self.assertEqual(_types(events),
                         ["status", "token", "citations", "done"])
        self.assertEqual(events[1]["text"], "Algae photosynthesize [1].")
        cite = events[2]["items"]
        self.assertEqual([c["n"] for c in cite], [1])
        self.assertEqual(cite[0]["paper_id"], PAPER_A_ID)
        # persist called with the answer text + shown citations, before done.
        persist.assert_called_once()
        args, _ = persist.call_args
        self.assertIn("Algae photosynthesize [1].", args[0])
        self.assertEqual([c["n"] for c in args[1]], [1])


class LegacyFallbackTest(unittest.TestCase):
    def test_round0_create_failure_falls_back_to_single_shot(self):
        # Round 0 (with tools) raises -> legacy fallback (no tools) streams an
        # answer. Citations come from the input 6-hit list, NOT the registry.
        create = _FakeCreate([
            Exception("provider 400: tools not supported"),
            [_Chunk(_Delta(content="Single-shot answer [1]."))],
        ])
        client = _FakeClient(create)
        persist = mock.Mock()
        deps = _deps_for(PAPER_A_ID)
        inp = _build_input(client, deps, persist,
                           hits=[_hit(PAPER_A_ID, "Title", "content")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID,
                                       "revision_number": 1, "filename": "x.pdf",
                                       "title": "Title", "authors": "A",
                                       "url": f"/paper/{PAPER_A_ID}"}])

        events = _events(inp)

        # No status event (no tool dispatched). Token, citations, done.
        self.assertEqual(_types(events), ["token", "citations", "done"])
        self.assertEqual(events[1]["items"][0]["paper_id"], PAPER_A_ID)
        # calls[0] is the round-0 agentic call (carried tools=, raised);
        # calls[1] is the legacy fallback call (no tools=, no tool_choice).
        self.assertIn("tools", create.calls[0])
        self.assertNotIn("tools", create.calls[1])


class RoundCapTest(unittest.TestCase):
    def test_loop_exhaustion_forces_tool_choice_none_answer(self):
        # Every agentic round emits a tool call; after MAX_TOOL_ROUNDS the
        # module makes one final tool_choice="none" call for an answer.
        tool_round = [_Chunk(_Delta(tool_calls=[
            _ToolCall(0, id="c", name="read_paper", arguments=f'{{"paper_id":"{PAPER_A_ID}"}}'),
        ]))]
        create = _FakeCreate(
            [tool_round] * MAX_TOOL_ROUNDS +
            [[_Chunk(_Delta(content="Forced final answer [1]."))]]
        )
        client = _FakeClient(create)
        persist = mock.Mock()
        deps = _deps_for(PAPER_A_ID)
        inp = _build_input(client, deps, persist,
                           hits=[_hit(PAPER_A_ID, "Title", "content")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID,
                                       "revision_number": 1, "filename": "x.pdf",
                                       "title": "Title", "authors": "A",
                                       "url": f"/paper/{PAPER_A_ID}"}])

        events = _events(inp)

        # MAX_TOOL_ROUNDS status events (one per dispatched read_paper), then a
        # token from the forced answer, citations, done.
        self.assertEqual(_types(events).count("status"), MAX_TOOL_ROUNDS)
        self.assertEqual(_types(events)[-2:], ["citations", "done"])
        self.assertIn("Forced final answer", events[-3]["text"])
        # The last create call forced tool_choice="none".
        self.assertEqual(create.calls[-1].get("tool_choice"), "none")


class CitationFilteringTest(unittest.TestCase):
    def test_unreferenced_sources_dropped(self):
        # Answer cites [2] only; [1] must be dropped from citations.
        create = _FakeCreate([
            [_Chunk(_Delta(content="I only trust [2]."))],
        ])
        client = _FakeClient(create)
        persist = mock.Mock()
        deps = mock.Mock()
        inp = _build_input(client, deps, persist,
                           hits=[_hit(PAPER_A_ID, "Title A", "c", filename="a.pdf"),
                                 _hit(PAPER_B_ID, "Title B", "c", filename="b.pdf")],
                           citations=[
                               {"n": 1, "paper_id": PAPER_A_ID, "revision_number": 1,
                                "filename": "a.pdf", "title": "Title A",
                                "authors": "A", "url": f"/paper/{PAPER_A_ID}"},
                               {"n": 2, "paper_id": PAPER_B_ID, "revision_number": 1,
                                "filename": "b.pdf", "title": "Title B",
                                "authors": "B", "url": f"/paper/{PAPER_B_ID}"},
                           ])

        events = _events(inp)

        cite = events[1]["items"]  # token then citations
        self.assertEqual([c["n"] for c in cite], [2])
        self.assertEqual(cite[0]["paper_id"], PAPER_B_ID)


class ToolCallReassemblyTest(unittest.TestCase):
    def test_fragmented_arguments_are_joined_before_dispatch(self):
        # The model streams the tool call as id, then name, then two argument
        # fragments. run_tool must receive the joined JSON argument string.
        create = _FakeCreate([
            [_Chunk(_Delta(tool_calls=[
                _ToolCall(0, id="call_1", name="read_paper")])),
             _Chunk(_Delta(tool_calls=[
                _ToolCall(0, arguments='{"paper_')])),
             _Chunk(_Delta(tool_calls=[
                _ToolCall(0, arguments=f'id":"{PAPER_A_ID}"}}')])),
            ],
            [_Chunk(_Delta(content="Done [1]."))],
        ])
        client = _FakeClient(create)
        persist = mock.Mock()
        deps = _deps_for(PAPER_A_ID)
        inp = _build_input(client, deps, persist,
                           hits=[_hit(PAPER_A_ID, "Title", "content")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID,
                                       "revision_number": 1, "filename": "x.pdf",
                                       "title": "Title", "authors": "A",
                                       "url": f"/paper/{PAPER_A_ID}"}])

        with mock.patch("services.ask_turn.library_tools.run_tool",
                        wraps=library_tools.run_tool) as spy:
            events = _events(inp)

        # run_tool was called for the read_paper dispatch with the joined args.
        dispatched = [c for c in spy.call_args_list
                      if c.args and c.args[0] == "read_paper"]
        self.assertTrue(dispatched)
        joined = dispatched[0].args[1]
        self.assertEqual(json.loads(joined), {"paper_id": PAPER_A_ID})
        self.assertEqual(_types(events), ["status", "token", "citations", "done"])


class CapsTest(unittest.TestCase):
    def test_web_search_cap_stops_dispatch_after_limit(self):
        # Model calls web_search CAP+1 times across rounds. The CAP+1th call
        # is not dispatched (no status event for it) and does not crash.
        tool_round = [_Chunk(_Delta(tool_calls=[
            _ToolCall(0, id="c", name="web_search", arguments='{"query":"q"}'),
        ]))]
        create = _FakeCreate(
            [tool_round] * (WEB_SEARCH_CALL_CAP + 1) +
            [[_Chunk(_Delta(content="Answer [1]."))]]
        )
        client = _FakeClient(create)
        persist = mock.Mock()
        deps = mock.Mock()
        deps.web_search.return_value = [{"title": "t", "url": "http://x", "content": "c"}]
        inp = _build_input(client, deps, persist,
                           hits=[_hit(PAPER_A_ID, "Title", "content")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID,
                                       "revision_number": 1, "filename": "x.pdf",
                                       "title": "Title", "authors": "A",
                                       "url": f"/paper/{PAPER_A_ID}"}],
                           include_web=True)

        events = _events(inp)

        # Exactly CAP status events (one per dispatched web_search), not CAP+1.
        self.assertEqual(_types(events).count("status"), WEB_SEARCH_CALL_CAP)
        self.assertIn("done", _types(events))


class PersistFailureTest(unittest.TestCase):
    def test_done_still_emitted_when_persist_raises(self):
        create = _FakeCreate([
            [_Chunk(_Delta(content="Answer [1]."))],
        ])
        client = _FakeClient(create)
        persist = mock.Mock(side_effect=RuntimeError("db down"))
        deps = mock.Mock()
        inp = _build_input(client, deps, persist,
                           hits=[_hit(PAPER_A_ID, "Title", "content")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID,
                                       "revision_number": 1, "filename": "x.pdf",
                                       "title": "Title", "authors": "A",
                                       "url": f"/paper/{PAPER_A_ID}"}])

        events = _events(inp)

        # persist raised; logger.exception called; done still emitted last.
        persist.assert_called_once()
        inp.logger.exception.assert_called()
        self.assertEqual(events[-1]["type"], "done")


class ErrorPathTest(unittest.TestCase):
    def test_round_after_zero_failure_yields_bare_error_event(self):
        # Round 0 succeeds (tool call dispatched); round 1 create raises.
        # A round>0 failure is not the legacy-fallback path -> error event.
        create = _FakeCreate([
            [_Chunk(_Delta(tool_calls=[
                _ToolCall(0, id="c", name="read_paper",
                          arguments=f'{{"paper_id":"{PAPER_A_ID}"}}')])),
            ],
            Exception("stream broke"),
        ])
        client = _FakeClient(create)
        persist = mock.Mock()
        deps = _deps_for(PAPER_A_ID)
        inp = _build_input(client, deps, persist,
                           hits=[_hit(PAPER_A_ID, "Title", "content")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID,
                                       "revision_number": 1, "filename": "x.pdf",
                                       "title": "Title", "authors": "A",
                                       "url": f"/paper/{PAPER_A_ID}"}])

        events = _events(inp)

        self.assertEqual(events[-1]["type"], "error")
        # The module yields a bare error event (no message); the route fills it.
        self.assertNotIn("message", events[-1])
        persist.assert_not_called()
        inp.logger.exception.assert_called()


class DualPathDivergenceTest(unittest.TestCase):
    """Pins today's intentional divergence between the agentic and legacy
    citation sources. A paper surfaced only by read_paper mid-loop is cited
    in the agentic path but dropped in the legacy path (whose citation list
    is the pre-built 6-hit set). Fixing this is a separate, user-visible
    change — do not unify without a deliberate behavior decision.
    """
    def test_paper_only_from_read_paper_cited_agentic_dropped_legacy(self):
        # Agentic: round 1 calls read_paper on paper B (not in hits), answer
        # cites [2] (B's registry number). registry.as_citations() includes B.
        agentic_create = _FakeCreate([
            [_Chunk(_Delta(tool_calls=[
                _ToolCall(0, id="c", name="read_paper",
                          arguments=f'{{"paper_id":"{PAPER_B_ID}"}}')])),
            ],
            [_Chunk(_Delta(content="Cite [2]."))],
        ])
        deps = _deps_for(PAPER_B_ID, "Paper B")
        agentic_events = _events(_build_input(
            _FakeClient(agentic_create), deps, mock.Mock(),
            hits=[_hit(PAPER_A_ID, "Paper A", "content")],  # only A in hits
            citations=[{"n": 1, "paper_id": PAPER_A_ID, "revision_number": 1,
                        "filename": "a.pdf", "title": "Paper A", "authors": "A",
                        "url": f"/paper/{PAPER_A_ID}"}]))

        agentic_cited = {c["paper_id"] for c in agentic_events
                         if c["type"] == "citations" for c in c["items"]}
        self.assertIn(PAPER_B_ID, agentic_cited)  # surfaced via read_paper

        # Legacy: round 0 create raises -> fallback single-shot "Cite [2]."
        # The input citation list has only [1]=A, so [2] is dropped.
        legacy_create = _FakeCreate([
            Exception("no tools"),
            [_Chunk(_Delta(content="Cite [2]."))],
        ])
        deps2 = _deps_for(PAPER_B_ID, "Paper B")
        legacy_events = _events(_build_input(
            _FakeClient(legacy_create), deps2, mock.Mock(),
            hits=[_hit(PAPER_A_ID, "Paper A", "content")],
            citations=[{"n": 1, "paper_id": PAPER_A_ID, "revision_number": 1,
                        "filename": "a.pdf", "title": "Paper A", "authors": "A",
                        "url": f"/paper/{PAPER_A_ID}"}]))

        legacy_cited = {c["paper_id"] for c in legacy_events
                        if c["type"] == "citations" for c in c["items"]}
        self.assertNotIn(PAPER_B_ID, legacy_cited)  # dropped — divergence


class WebResultsSeedingTest(unittest.TestCase):
    def test_prefetched_web_results_seeded_as_is_web_and_cited(self):
        # Pre-fetched web results are seeded into the registry as is_web=True
        # (contiguous numbering) and survive the citation split when cited.
        create = _FakeCreate([
            [_Chunk(_Delta(content="See this [2]."))],
        ])
        client = _FakeClient(create)
        deps = mock.Mock()
        inp = _build_input(client, deps, mock.Mock(),
                           hits=[_hit(PAPER_A_ID, "Paper A", "content")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID,
                                       "revision_number": 1, "filename": "a.pdf",
                                       "title": "Paper A", "authors": "A",
                                       "url": f"/paper/{PAPER_A_ID}"}],
                           web_results=[{"title": "Live Result",
                                          "url": "https://example.com/x",
                                          "content": "web snippet"}],
                           include_web=True)

        events = _events(inp)

        # [1] is the library hit, [2] is the seeded web result.
        web_events = [e for e in events if e["type"] == "web"]
        self.assertTrue(web_events)
        self.assertEqual(web_events[0]["items"][0]["n"], 2)
        self.assertEqual(web_events[0]["items"][0]["url"], "https://example.com/x")


class AttachmentToolGateTest(unittest.TestCase):
    def test_attachment_names_non_empty_offers_read_attachment_tool(self):
        # When attachment_names is non-empty, build_tool_schemas is called with
        # include_attachment=True, so the tools offered include read_attachment.
        create = _FakeCreate([
            [_Chunk(_Delta(content="ok"))],
        ])
        client = _FakeClient(create)
        deps = mock.Mock()
        inp = _build_input(client, deps, mock.Mock(),
                           hits=[_hit(PAPER_A_ID, "Paper A", "content")],
                           citations=[{"n": 1, "paper_id": PAPER_A_ID,
                                       "revision_number": 1, "filename": "a.pdf",
                                       "title": "Paper A", "authors": "A",
                                       "url": f"/paper/{PAPER_A_ID}"}],
                           attachment_names=["notes.pdf"])

        _events(inp)

        tools = create.calls[0]["tools"]
        tool_names = [t["function"]["name"] for t in tools]
        self.assertIn("read_attachment", tool_names)


if __name__ == "__main__":
    unittest.main()
