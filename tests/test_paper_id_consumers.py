"""Cross-consumer contract for immutable Paper UUID identity.

Paper filenames remain display metadata.  New browser/API/tool writes identify
Papers by UUID, while filename lookup is confined to reading legacy stored
conversation records.  Attachments deliberately remain filename-keyed.
"""

from __future__ import annotations

import ast
import inspect
import json
import re
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from flask import Flask
import library_tools
from models import ChatMessageModel, ConversationModel
from routes.ai import register_routes as register_ai_routes
from services import ai as ai_service
from services.publishing_contracts import NotFound


ROOT = Path(__file__).resolve().parents[1]
PAPER_ID = "22222222-2222-4222-8222-222222222222"


def _record(**overrides):
    values = {
        "paper_id": PAPER_ID,
        "current_revision": 2,
        "filename": "paper.pdf",
        "title": "Paper",
        "author_name": "Author",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _Library:
    def __init__(self, *, aliases=None, records=None):
        self.aliases = aliases or {}
        self.records = records or {PAPER_ID: _record()}
        self.alias_calls = []

    def current_pdf(self, paper_id):
        record = self.records.get(paper_id)
        if record is None:
            raise NotFound()
        return SimpleNamespace(paper=record, revision=record.current_revision)

    def resolve_alias(self, filename):
        self.alias_calls.append(filename)
        record = self.aliases.get(filename)
        if record is None:
            raise NotFound()
        return record

    def list_visible(self):
        return tuple(self.records.values())


def _uses_filename_identity(node):
    for part in ast.walk(node):
        if isinstance(part, ast.Name) and part.id == "filename":
            return True
        if isinstance(part, ast.Attribute) and part.attr == "filename":
            return True
        if (
            isinstance(part, ast.Subscript)
            and isinstance(part.slice, ast.Constant)
            and part.slice.value == "filename"
        ):
            return True
        if (
            isinstance(part, ast.Call)
            and isinstance(part.func, ast.Attribute)
            and part.func.attr == "get"
            and part.args
            and isinstance(part.args[0], ast.Constant)
            and part.args[0].value == "filename"
        ):
            return True
    return False


class ToolIdentityContract(unittest.TestCase):
    def test_read_paper_schema_requires_uuid_not_filename(self):
        schema = next(
            item for item in library_tools.TOOL_SCHEMAS
            if item["function"]["name"] == "read_paper"
        )["function"]["parameters"]
        self.assertEqual(schema["required"], ["paper_id"])
        self.assertIn("paper_id", schema["properties"])
        self.assertNotIn("filename", schema["properties"])

    def test_registry_keeps_exact_paper_identity_and_display_projection(self):
        registry = library_tools.SourceRegistry()
        registry.register(PAPER_ID, {
            "paper_id": PAPER_ID,
            "revision_number": 2,
            "filename": "paper.pdf",
            "title": "Paper",
            "authors": "Author",
            "url": f"/paper/{PAPER_ID}",
        })

        citation = registry.as_citations()[0]
        self.assertEqual(citation.get("paper_id"), PAPER_ID)
        self.assertEqual(citation.get("revision_number"), 2)
        self.assertEqual(citation["filename"], "paper.pdf")
        self.assertEqual(citation["title"], "Paper")

    def test_registry_refreshes_current_projection_without_renumbering(self):
        registry = library_tools.SourceRegistry()
        first = registry.register(PAPER_ID, {
            "paper_id": PAPER_ID,
            "revision_number": 1,
            "filename": "old-name.pdf",
            "title": "Old title",
            "authors": "Old author",
            "url": f"/paper/{PAPER_ID}",
        })
        second = registry.register(PAPER_ID, {
            "paper_id": PAPER_ID,
            "revision_number": 2,
            "filename": "paper.pdf",
            "title": "Paper",
            "authors": "Author",
            "url": f"/paper/{PAPER_ID}",
        })

        self.assertEqual(first, second)
        citation = registry.as_citations()[0]
        self.assertEqual(citation["revision_number"], 2)
        self.assertEqual(citation["filename"], "paper.pdf")
        self.assertEqual(citation["title"], "Paper")
        self.assertEqual(citation["authors"], "Author")

    def test_read_paper_dispatches_uuid_to_every_dependency(self):
        calls = []

        def full_text(paper_id):
            calls.append(("text", paper_id))
            return "Full paper text"

        def paper_meta(paper_id):
            calls.append(("meta", paper_id))
            return {
                "paper_id": PAPER_ID,
                "revision_number": 2,
                "filename": "paper.pdf",
                "title": "Paper",
                "authors": "Author",
            }

        def paper_url(paper_id):
            calls.append(("url", paper_id))
            return f"/paper/{paper_id}"

        deps = SimpleNamespace(
            search=lambda _query: [],
            full_text=full_text,
            paper_meta=paper_meta,
            paper_url=paper_url,
        )
        registry = library_tools.SourceRegistry()

        result = library_tools.run_tool(
            "read_paper",
            json.dumps({"paper_id": PAPER_ID, "filename": "forged.pdf"}),
            registry,
            deps,
        )

        self.assertIn("Full paper text", result)
        self.assertEqual(calls, [
            ("meta", PAPER_ID),
            ("url", PAPER_ID),
            ("text", PAPER_ID),
            ("meta", PAPER_ID),
        ])
        self.assertEqual(registry.as_citations()[0]["filename"], "paper.pdf")

    def test_attachment_tool_remains_filename_keyed(self):
        schema = library_tools.READ_ATTACHMENT_SCHEMA["function"]["parameters"]
        self.assertEqual(schema["required"], ["filename"])
        self.assertIn("filename", schema["properties"])
        self.assertNotIn("paper_id", schema["properties"])


class ServerReferenceNormalizationContract(unittest.TestCase):
    def _normalizer(self):
        normalizer = getattr(ai_service, "_normalize_stored_paper_references", None)
        self.assertTrue(
            callable(normalizer),
            "services.ai must normalize Paper references through PaperLibrary",
        )
        return normalizer

    def test_new_reference_uses_uuid_and_ignores_posted_display_metadata(self):
        library = _Library()
        normalized = self._normalizer()(
            [{
                "paper_id": PAPER_ID,
                "revision_number": 99,
                "filename": "forged.pdf",
                "title": "Forged title",
            }],
            library,
            allow_legacy_aliases=False,
        )

        self.assertEqual(normalized, [{
            "paper_id": PAPER_ID,
            "revision_number": 2,
            "filename": "paper.pdf",
            "title": "Paper",
            "authors": "Author",
        }])
        self.assertEqual(library.alias_calls, [])

    def test_legacy_filename_is_resolved_only_during_stored_record_read(self):
        record = _record()
        library = _Library(aliases={"paper.pdf": record})
        normalizer = self._normalizer()

        self.assertEqual(
            normalizer(
                [{"filename": "paper.pdf", "title": "Old title"}],
                library,
                allow_legacy_aliases=True,
            ),
            [{
                "paper_id": PAPER_ID,
                "revision_number": 2,
                "filename": "paper.pdf",
                "title": "Paper",
                "authors": "Author",
            }],
        )
        self.assertEqual(library.alias_calls, ["paper.pdf"])

        self.assertEqual(
            normalizer(
                [{"filename": "paper.pdf", "title": "Old title"}],
                library,
                allow_legacy_aliases=False,
            ),
            [],
        )
        self.assertEqual(library.alias_calls, ["paper.pdf"])

    def test_missing_or_ambiguous_legacy_alias_invents_no_paper(self):
        library = _Library(aliases={})
        normalized = self._normalizer()(
            [
                {"filename": "missing.pdf", "title": "Missing"},
                {"filename": "ambiguous.pdf", "title": "Ambiguous"},
            ],
            library,
            allow_legacy_aliases=True,
        )
        self.assertEqual(normalized, [])

    def test_explicit_attachment_reference_stays_filename_keyed(self):
        library = _Library(aliases={"paper.pdf": _record()})
        normalizer = self._normalizer()
        self.assertIn("preserve_attachments", inspect.signature(normalizer).parameters)
        normalized = normalizer(
            [{
                "n": 3,
                "filename": "paper.pdf",
                "title": "Attached notes",
                "authors": "",
                "url": None,
                "is_attachment": True,
            }],
            library,
            allow_legacy_aliases=True,
            preserve_attachments=True,
        )

        self.assertEqual(normalized, [{
            "n": 3,
            "filename": "paper.pdf",
            "title": "Attached notes",
            "authors": "",
            "url": None,
            "is_attachment": True,
        }])
        self.assertEqual(library.alias_calls, [])

    def test_legacy_unflagged_attachment_with_falsy_url_stays_filename_keyed(self):
        normalizer = self._normalizer()
        for url_value in (None, ""):
            with self.subTest(url=url_value):
                library = _Library(aliases={"paper.pdf": _record()})
                legacy_attachment = [{
                    "n": 3,
                    "filename": "paper.pdf",
                    "title": "Attached notes",
                    "authors": "",
                    "url": url_value,
                }]

                self.assertEqual(
                    normalizer(
                        legacy_attachment,
                        library,
                        allow_legacy_aliases=True,
                        preserve_attachments=True,
                    ),
                    [{
                        "n": 3,
                        "filename": "paper.pdf",
                        "title": "Attached notes",
                        "authors": "",
                        "url": None,
                        "is_attachment": True,
                    }],
                )
                self.assertEqual(library.alias_calls, [])

                self.assertEqual(
                    normalizer(
                        legacy_attachment,
                        library,
                        allow_legacy_aliases=True,
                    ),
                    [],
                )
                self.assertEqual(library.alias_calls, [])

    def test_attachment_shape_is_rejected_from_paper_only_references(self):
        library = _Library(aliases={"paper.pdf": _record()})
        attachment = [{
            "filename": "paper.pdf",
            "title": "Attached notes",
            "is_attachment": True,
        }]

        self.assertEqual(
            self._normalizer()(
                attachment,
                library,
                allow_legacy_aliases=False,
            ),
            [],
        )
        self.assertEqual(
            self._normalizer()(
                attachment,
                library,
                allow_legacy_aliases=True,
            ),
            [],
        )
        self.assertEqual(library.alias_calls, [])


class BrowserAndApiIdentityContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.js = (ROOT / "static/js/ai.js").read_text(encoding="utf-8")
        cls.route_src = (ROOT / "routes/ai.py").read_text(encoding="utf-8")

    def test_browser_selection_is_keyed_by_paper_uuid(self):
        self.assertIn("selected[p.paper_id]", self.js)
        self.assertIn("paper_id: paperId", self.js)
        self.assertNotIn("selected[p.filename]", self.js)

    def test_paper_picker_api_projects_current_revision(self):
        tree = ast.parse(self.route_src)
        fn = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "api_ai_papers"
        )
        source = ast.get_source_segment(self.route_src, fn) or ""
        self.assertIn('"paper_id"', source)
        self.assertIn('"revision_number"', source)

    def test_new_message_writes_require_paper_id_and_do_not_trust_display_fields(self):
        tree = ast.parse(self.route_src)
        fn = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "api_ai"
        )
        source = ast.get_source_segment(self.route_src, fn) or ""
        self.assertIn("_normalize_stored_paper_references", source)
        self.assertIn("allow_legacy_aliases=False", source)
        self.assertNotIn('it.get("title")', source)
        self.assertNotIn('data.get("paper_filenames")', source)

    def test_agentic_citation_stream_keeps_explicit_attachments(self):
        self.assertNotIn(
            'and not c.get("is_attachment")',
            self.route_src,
        )


class StoredConversationReadContract(unittest.TestCase):
    class _Query:
        def __init__(self, *, first_value=None, all_value=None):
            self.first_value = first_value
            self.all_value = all_value or []

        def filter(self, *_args):
            return self

        def order_by(self, *_args):
            return self

        def distinct(self):
            return self

        def first(self):
            return self.first_value

        def all(self):
            return self.all_value

    class _Db:
        def __init__(self, conversation, messages):
            self.results = [
                StoredConversationReadContract._Query(first_value=conversation),
                StoredConversationReadContract._Query(all_value=messages),
                StoredConversationReadContract._Query(all_value=[]),
            ]

        def query(self, *_args):
            return self.results.pop(0)

    def test_get_reserializes_legacy_paper_and_preserves_unflagged_attachment(self):
        record = _record(filename="legacy.pdf", title="Current title")
        library = _Library(
            aliases={"legacy.pdf": record},
            records={PAPER_ID: record},
        )
        conversation = SimpleNamespace(id=7, title="Stored", serial="stored1")
        message = SimpleNamespace(
            role="assistant",
            content="Answer [1] and [2].",
            citations=json.dumps([
                {
                    "n": 1,
                    "filename": "legacy.pdf",
                    "title": "Old title",
                    "authors": "Old author",
                    "url": "/preview/legacy.pdf",
                },
                {
                    "n": 2,
                    "filename": "legacy.pdf",
                    "title": "Attached copy",
                    "authors": "",
                    "url": None,
                },
                {
                    "n": 3,
                    "filename": "missing.pdf",
                    "title": "Missing Paper",
                },
                {
                    "n": 4,
                    "filename": "flagged.txt",
                    "title": "Flagged attachment",
                    "authors": "",
                    "url": None,
                    "is_attachment": True,
                },
            ]),
            attachments="",
            cited_papers="",
        )
        fake_db = self._Db(conversation, [message])

        @contextmanager
        def fake_db_session():
            yield fake_db

        app = Flask(__name__)
        app.secret_key = "stored-conversation-read"

        @app.get("/paper/<uuid:paper_id>", endpoint="preview_paper")
        def preview_paper(paper_id):
            return str(paper_id)

        register_ai_routes(app)
        app.extensions["paper_library"] = library
        with mock.patch("routes.ai.OPEN_ACCESS", True), mock.patch(
            "routes.ai.db_session", fake_db_session
        ):
            response = app.test_client().get("/api/conversations/stored1")

        self.assertEqual(response.status_code, 200)
        citations = response.get_json()["messages"][0]["citations"]
        self.assertEqual(citations[0], {
            "n": 1,
            "paper_id": PAPER_ID,
            "revision_number": 2,
            "filename": "legacy.pdf",
            "title": "Current title",
            "authors": "Author",
            "url": f"/paper/{PAPER_ID}",
        })
        self.assertEqual(citations[1], {
            "n": 2,
            "filename": "legacy.pdf",
            "title": "Attached copy",
            "authors": "",
            "url": None,
            "is_attachment": True,
        })
        self.assertEqual(citations[2], {
            "n": 4,
            "filename": "flagged.txt",
            "title": "Flagged attachment",
            "authors": "",
            "url": None,
            "is_attachment": True,
        })
        self.assertEqual(library.alias_calls, ["legacy.pdf", "missing.pdf"])


class NewConversationWriteContract(unittest.TestCase):
    class _Query:
        def __init__(self, db, target):
            self.db = db
            self.target = target

        def filter(self, *_args):
            return self

        def order_by(self, *_args):
            return self

        def first(self):
            if self.target is ConversationModel:
                return self.db.conversation
            return None

        def all(self):
            if self.target is ChatMessageModel:
                return list(self.db.messages)
            return [
                (message.cited_papers,)
                for message in self.db.messages
                if message.role == "user"
            ]

    class _Db:
        def __init__(self, conversation):
            self.conversation = conversation
            self.messages = []

        def query(self, target):
            return NewConversationWriteContract._Query(self, target)

        def add(self, value):
            if isinstance(value, ChatMessageModel):
                self.messages.append(value)

        def flush(self):
            return None

    def test_post_uses_uuid_and_persists_only_server_metadata(self):
        record = _record(filename="current.pdf", title="Current title")
        library = _Library(
            aliases={"forged.pdf": record, "legacy-only.pdf": record},
            records={PAPER_ID: record},
        )
        conversation = SimpleNamespace(
            id=9,
            serial="write1",
            owner_key="owner",
            title="Existing conversation",
            updated_at="",
        )
        fake_db = self._Db(conversation)

        @contextmanager
        def fake_db_session():
            yield fake_db

        app = Flask(__name__)
        app.secret_key = "new-conversation-write"
        register_ai_routes(app)
        app.extensions["paper_library"] = library
        stream_client = SimpleNamespace(
            chat=SimpleNamespace(
                completions=SimpleNamespace(create=lambda **_kwargs: [])
            )
        )
        library_deps = SimpleNamespace(
            search=lambda _query: [],
            full_text=lambda _paper_id: "Current Paper text",
            paper_meta=lambda _paper_id: {
                "paper_id": PAPER_ID,
                "revision_number": 2,
                "filename": "current.pdf",
                "title": "Current title",
                "authors": "Author",
            },
            paper_url=lambda paper_id: f"/paper/{paper_id}",
        )

        with mock.patch("routes.ai.OPEN_ACCESS", True), mock.patch(
            "routes.ai._", side_effect=lambda message, **_kwargs: message
        ), mock.patch(
            "routes.ai.get_locale", return_value="en"
        ), mock.patch(
            "routes.ai.llm_client.llm_enabled", return_value=True
        ), mock.patch("routes.ai._ask_rate_ok", return_value=True), mock.patch(
            "routes.ai.db_session", fake_db_session
        ), mock.patch(
            "routes.ai._attachment_grounding", return_value=[]
        ), mock.patch(
            "routes.ai._forced_grounding", return_value=[]
        ) as forced_grounding, mock.patch(
            "routes.ai.llm_client.build_client", return_value=stream_client
        ), mock.patch(
            "routes.ai._build_library_deps", return_value=library_deps
        ), mock.patch(
            "routes.ai._attachment_filenames", return_value=[]
        ), mock.patch(
            "routes.ai.web_search.web_search_enabled", return_value=False
        ):
            response = app.test_client().post(
                "/api/ai",
                json={
                    "question": "Use the selected Paper",
                    "conversation_id": "write1",
                    "message_papers": [
                        {
                            "paper_id": PAPER_ID,
                            "revision_number": 999,
                            "filename": "forged.pdf",
                            "title": "Forged title",
                        },
                        {"filename": "legacy-only.pdf", "title": "Legacy write"},
                        {
                            "is_attachment": True,
                            "filename": "attachment-shaped.pdf",
                        },
                    ],
                    "paper_filenames": ["legacy-only.pdf"],
                },
                buffered=False,
            )

        self.assertEqual(response.status_code, 200)
        response.close()
        self.assertEqual(library.alias_calls, [])
        self.assertEqual(len(fake_db.messages), 1)
        stored = json.loads(fake_db.messages[0].cited_papers)
        self.assertEqual(stored, [{
            "paper_id": PAPER_ID,
            "revision_number": 2,
            "filename": "current.pdf",
            "title": "Current title",
            "authors": "Author",
        }])
        forced_grounding.assert_called_once_with(
            "Use the selected Paper", [PAPER_ID]
        )


class CanonicalConsumerInventory(unittest.TestCase):
    def test_templates_use_uuid_for_paper_forms_and_links(self):
        paths = (
            "templates/search.html",
            "templates/advanced_search.html",
            "templates/journal_detail.html",
            "templates/journal_edit.html",
        )
        joined = "\n".join((ROOT / path).read_text(encoding="utf-8") for path in paths)
        self.assertIn("paper.paper_id", joined)
        self.assertIn("p.paper_id", joined)
        canonical = (
            "preview_paper",
            "paper_file",
            "paper_info",
            "paper_modify",
            "paper_delete",
            "paper_revision_file",
            "paper_restore",
        )
        pattern = re.compile(
            r"url_for\(\s*['\"](?:"
            + "|".join(canonical)
            + r")[\'\"][^}]*\bfilename\s*=",
            re.DOTALL,
        )
        self.assertIsNone(pattern.search(joined))

    def test_canonical_endpoint_calls_never_pass_filename_keyword(self):
        offenders = []
        canonical = {
            "preview_paper",
            "paper_file",
            "paper_info",
            "paper_modify",
            "paper_delete",
            "paper_revision_file",
            "paper_restore",
        }
        for folder in ("routes", "services"):
            for path in (ROOT / folder).rglob("*.py"):
                source = path.read_text(encoding="utf-8")
                tree = ast.parse(source)
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call) or not node.args:
                        continue
                    if not (
                        isinstance(node.func, ast.Name)
                        and node.func.id == "url_for"
                        and isinstance(node.args[0], ast.Constant)
                        and node.args[0].value in canonical
                    ):
                        continue
                    if any(keyword.arg == "filename" for keyword in node.keywords):
                        offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
        self.assertEqual(offenders, [])

    def test_canonical_library_reads_never_receive_filename_identity(self):
        offenders = []
        read_names = {
            "current_pdf",
            "_lib_full_text",
            "_lib_paper_meta",
            "_lib_paper_url",
            "full_text",
            "paper_meta",
            "paper_url",
            "_rag_paper_text",
            "_live_paper_document",
        }
        paths = [ROOT / "library_tools.py"]
        paths.extend((ROOT / "routes").rglob("*.py"))
        paths.extend((ROOT / "services").rglob("*.py"))
        for path in paths:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                callee = (
                    node.func.id if isinstance(node.func, ast.Name)
                    else node.func.attr if isinstance(node.func, ast.Attribute)
                    else ""
                )
                if callee not in read_names:
                    continue
                supplied = list(node.args) + [kw.value for kw in node.keywords]
                uses_filename = any(
                    _uses_filename_identity(value) for value in supplied
                )
                has_filename_keyword = any(
                    keyword.arg == "filename" for keyword in node.keywords
                )
                if uses_filename or has_filename_keyword:
                    offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")

        self.assertEqual(offenders, [])

    def test_filename_identity_detector_catches_indirect_expressions(self):
        snippets = (
            'library.current_pdf(row["filename"])',
            'deps.full_text(item.get("filename"))',
            "library.current_pdf(record.filename)",
        )
        for snippet in snippets:
            with self.subTest(snippet=snippet):
                call = ast.parse(snippet).body[0].value
                self.assertTrue(
                    any(_uses_filename_identity(value) for value in call.args)
                )


if __name__ == "__main__":
    unittest.main()
