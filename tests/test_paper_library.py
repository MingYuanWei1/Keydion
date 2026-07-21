"""Read-side contracts for canonical Paper identity and visibility."""

from __future__ import annotations

import io
import unittest
from dataclasses import FrozenInstanceError
from unittest import mock

from sqlalchemy import text

from models import (
    PaperFilenameAliasModel,
    PaperMetadataModel,
    PaperRevisionModel,
)
from services.paper_identity import normalize_alias_key
from services.paper_library import PaperLibrary
from services.papers import gather_paper_records
from services.journals import get_journal_paper_counts, get_recent_journals
from services.publishing_contracts import Actor, Forbidden, NotFound, PdfUpload
from tests.publishing_support import PublishingLifecycleTestCase


class PaperLibraryVisibilityTest(PublishingLifecycleTestCase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.library = PaperLibrary(
            session_factory=self.session_factory,
            storage=self.storage,
        )

    def _seed_paper(
        self,
        *,
        paper_id: str,
        filename: str,
        lifecycle_state: str,
        current_revision: int | None,
        title: str = "Canonical Paper",
    ) -> None:
        with self.session_factory() as session:
            session.add(
                PaperMetadataModel(
                    id=paper_id,
                    filename=filename,
                    title=title,
                    journal="Journal",
                    category="science",
                    language="en",
                    keywords="canonical, identity",
                    abstract="A visible Paper record.",
                    author_name="Ada Author",
                    author_email="ada@example.test",
                    author_school="Example School",
                    published_at="2026-07-21",
                    ib_ee_data='{"is_ib_ee": true}',
                    is_ib_sample="",
                    cp_data="",
                    is_anonymous="",
                    ia_data="",
                    lifecycle_state=lifecycle_state,
                    current_revision=current_revision,
                    row_version=1 if current_revision is not None else 0,
                    index_status="pending",
                )
            )
            session.commit()

    def _store_revision(self, paper_id: str, revision: int):
        staged = self.storage.stage(
            PdfUpload(
                f"source-{revision}.pdf",
                io.BytesIO(self.valid_pdf_bytes(f"revision-{revision}")),
            ),
            f"seed-{paper_id}-{revision}",
        )
        return self.storage.promote(staged, paper_id, revision)

    def _seed_revision(
        self,
        paper_id: str,
        revision: int,
        *,
        sha256: str,
        size_bytes: int,
    ) -> None:
        with self.session_factory() as session:
            session.add(
                PaperRevisionModel(
                    paper_id=paper_id,
                    revision_number=revision,
                    sha256=sha256,
                    size_bytes=size_bytes,
                    created_at=self.now,
                    created_by="contributor",
                )
            )
            session.commit()

    def _seed_stored_revision(self, paper_id: str, revision: int):
        stored = self._store_revision(paper_id, revision)
        self._seed_revision(
            paper_id,
            revision,
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
        )
        return stored

    def _seed_alias(self, paper_id: str, filename: str) -> None:
        with self.session_factory() as session:
            session.add(
                PaperFilenameAliasModel(
                    lookup_key=normalize_alias_key(filename),
                    filename=filename,
                    paper_id=paper_id,
                    created_at=self.now,
                )
            )
            session.commit()

    def test_visible_by_id_normalizes_uuid_and_returns_a_detached_display_record(self):
        paper_id = "a0b1c2d3-e4f5-4678-9abc-def012345678"
        self._seed_paper(
            paper_id=paper_id,
            filename="canonical-paper.pdf",
            lifecycle_state="published",
            current_revision=1,
        )

        paper = self.library.visible_by_id("{A0B1C2D3-E4F5-4678-9ABC-DEF012345678}")

        self.assertEqual(paper.paper_id, paper_id)
        self.assertEqual(paper.current_revision, 1)
        self.assertEqual(paper.filename, "canonical-paper.pdf")
        self.assertEqual(paper.title, "Canonical Paper")
        self.assertEqual(paper.journal, "Journal")
        self.assertEqual(paper.author_name, "Ada Author")

    def test_visible_record_projects_display_metadata_and_uses_filename_stem_title(self):
        paper_id = "00000000-0000-4000-8000-000000000103"
        self._seed_paper(
            paper_id=paper_id,
            filename="fallback-title.pdf",
            lifecycle_state="published",
            current_revision=1,
            title="",
        )

        paper = self.library.visible_by_id(paper_id)

        self.assertEqual(paper.title, "fallback-title")
        self.assertEqual(paper.category, "science")
        self.assertEqual(paper.language, "en")
        self.assertEqual(paper.keywords, "canonical, identity")
        self.assertEqual(paper.abstract, "A visible Paper record.")
        self.assertEqual(paper.author_email, "ada@example.test")
        self.assertEqual(paper.author_school, "Example School")
        self.assertEqual(paper.published_at, "2026-07-21")
        self.assertEqual(paper.ib_ee_data, '{"is_ib_ee": true}')

    def test_visible_by_id_rejects_missing_malformed_and_hidden_papers(self):
        publishing_id = "00000000-0000-4000-8000-000000000101"
        deleting_id = "00000000-0000-4000-8000-000000000102"
        self._seed_paper(
            paper_id=publishing_id,
            filename="publishing.pdf",
            lifecycle_state="publishing",
            current_revision=None,
        )
        self._seed_paper(
            paper_id=deleting_id,
            filename="deleting.pdf",
            lifecycle_state="deleting",
            current_revision=1,
        )

        for paper_id in (
            "not-a-uuid",
            "00000000-0000-4000-8000-000000000999",
            publishing_id,
            deleting_id,
        ):
            with self.subTest(paper_id=paper_id), self.assertRaises(NotFound):
                self.library.visible_by_id(paper_id)

    def test_visible_by_id_rejects_invalid_published_current_revision_values(self):
        zero_id = "00000000-0000-4000-8000-000000000104"
        text_id = "00000000-0000-4000-8000-000000000105"
        self._seed_paper(
            paper_id=zero_id,
            filename="zero-revision.pdf",
            lifecycle_state="published",
            current_revision=0,
        )
        self._seed_paper(
            paper_id=text_id,
            filename="text-revision.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE papers_metadata "
                    "SET current_revision = 'not-an-integer' WHERE id = :paper_id"
                ),
                {"paper_id": text_id},
            )

        for paper_id in (zero_id, text_id):
            with self.subTest(paper_id=paper_id), self.assertRaises(NotFound):
                self.library.visible_by_id(paper_id)

    def test_current_pdf_requires_the_exact_current_revision_row(self):
        paper_id = "00000000-0000-4000-8000-000000000201"
        self._seed_paper(
            paper_id=paper_id,
            filename="missing-current-row.pdf",
            lifecycle_state="published",
            current_revision=2,
        )
        stored = self._store_revision(paper_id, 1)
        self._seed_revision(
            paper_id,
            1,
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
        )

        with self.assertRaises(NotFound):
            self.library.current_pdf(paper_id)

    def test_current_pdf_never_falls_back_when_the_current_file_is_missing(self):
        paper_id = "00000000-0000-4000-8000-000000000202"
        self._seed_paper(
            paper_id=paper_id,
            filename="missing-current-file.pdf",
            lifecycle_state="published",
            current_revision=2,
        )
        older = self._store_revision(paper_id, 1)
        self._seed_revision(
            paper_id,
            1,
            sha256=older.sha256,
            size_bytes=older.size_bytes,
        )
        self._seed_revision(
            paper_id,
            2,
            sha256="2" * 64,
            size_bytes=2,
        )

        with self.assertRaises(NotFound):
            self.library.current_pdf(paper_id)

    def test_current_pdf_rejects_checksum_or_size_mismatch(self):
        cases = (
            ("checksum", "0" * 64, None),
            ("size", None, 1),
        )
        for offset, (name, wrong_sha256, size_delta) in enumerate(cases, start=1):
            with self.subTest(name=name):
                paper_id = f"00000000-0000-4000-8000-{210 + offset:012d}"
                self._seed_paper(
                    paper_id=paper_id,
                    filename=f"{name}-mismatch.pdf",
                    lifecycle_state="published",
                    current_revision=1,
                )
                stored = self._store_revision(paper_id, 1)
                self._seed_revision(
                    paper_id,
                    1,
                    sha256=wrong_sha256 or stored.sha256,
                    size_bytes=stored.size_bytes + (size_delta or 0),
                )

                with self.assertRaises(NotFound):
                    self.library.current_pdf(paper_id)

    def test_current_pdf_returns_one_immutable_verified_snapshot(self):
        paper_id = "00000000-0000-4000-8000-000000000203"
        self._seed_paper(
            paper_id=paper_id,
            filename="current.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        stored = self._store_revision(paper_id, 1)
        self._seed_revision(
            paper_id,
            1,
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
        )

        document = self.library.current_pdf(paper_id)

        self.assertEqual(document.paper.paper_id, paper_id)
        self.assertEqual(document.paper.filename, "current.pdf")
        self.assertEqual(document.revision, 1)
        self.assertEqual(document.sha256, stored.sha256)
        self.assertEqual(document.size_bytes, stored.size_bytes)
        self.assertEqual(document.path, self.storage.revision_path(paper_id, 1))
        with self.assertRaises(FrozenInstanceError):
            document.revision = 2

    def test_current_pdf_fails_closed_if_current_revision_flips_during_verification(self):
        paper_id = "00000000-0000-4000-8000-000000000204"
        self._seed_paper(
            paper_id=paper_id,
            filename="flipping.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        stored = self._store_revision(paper_id, 1)
        self._seed_revision(
            paper_id,
            1,
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
        )

        test_case = self

        class FlippingStorage:
            def verify_revision(self, *args, **kwargs):
                verified = test_case.storage.verify_revision(*args, **kwargs)
                with test_case.session_factory() as session:
                    paper = session.get(PaperMetadataModel, paper_id)
                    paper.current_revision = 2
                    paper.row_version = 2
                    session.commit()
                return verified

        library = PaperLibrary(
            session_factory=self.session_factory,
            storage=FlippingStorage(),
        )

        with self.assertRaises(NotFound):
            library.current_pdf(paper_id)

    def test_resolve_alias_uses_the_canonical_unicode_key_for_a_live_paper(self):
        paper_id = "00000000-0000-4000-8000-000000000301"
        self._seed_paper(
            paper_id=paper_id,
            filename="live.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_alias(paper_id, "Ｆｏｏ.PDF")

        paper = self.library.resolve_alias("foo.pdf")

        self.assertEqual(paper.paper_id, paper_id)
        self.assertEqual(paper.filename, "live.pdf")

    def test_resolve_alias_rejects_missing_malformed_and_hidden_targets(self):
        publishing_id = "00000000-0000-4000-8000-000000000302"
        deleting_id = "00000000-0000-4000-8000-000000000303"
        self._seed_paper(
            paper_id=publishing_id,
            filename="hidden-publishing.pdf",
            lifecycle_state="publishing",
            current_revision=None,
        )
        self._seed_alias(publishing_id, "publishing-alias.pdf")
        self._seed_paper(
            paper_id=deleting_id,
            filename="hidden-deleting.pdf",
            lifecycle_state="deleting",
            current_revision=1,
        )
        self._seed_alias(deleting_id, "deleting-alias.pdf")

        for alias in (
            None,
            "missing.pdf",
            "PUBLISHING-ALIAS.PDF",
            "deleting-alias.pdf",
        ):
            with self.subTest(alias=alias), self.assertRaises(NotFound):
                self.library.resolve_alias(alias)

    def test_private_revision_authorizes_before_lookup_and_requires_exact_roles(self):
        paper_id = "00000000-0000-4000-8000-000000000401"
        self._seed_paper(
            paper_id=paper_id,
            filename="private.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_stored_revision(paper_id, 1)

        malformed_actors = (
            None,
            Actor("reader", 1),
            Actor("reader", True),
            Actor("reader", "2"),
            Actor("reader", 2.0),
            Actor("reader", 4),
            Actor("", 2),
            Actor(" padded ", 2),
            Actor("x" * 256, 2),
        )
        for actor in malformed_actors:
            with self.subTest(actor=actor), self.assertRaises(Forbidden):
                self.library.private_revision_pdf(
                    "00000000-0000-4000-8000-000000000999",
                    1,
                    actor=actor,
                )

    def test_private_revision_allows_contributor_and_curator_historical_access(self):
        paper_id = "00000000-0000-4000-8000-000000000402"
        self._seed_paper(
            paper_id=paper_id,
            filename="history.pdf",
            lifecycle_state="published",
            current_revision=2,
        )
        first = self._seed_stored_revision(paper_id, 1)
        self._seed_stored_revision(paper_id, 2)

        for role in (2, 3):
            with self.subTest(role=role):
                document = self.library.private_revision_pdf(
                    paper_id,
                    1,
                    actor=Actor("authorized", role),
                )
                self.assertEqual(document.paper.paper_id, paper_id)
                self.assertEqual(document.paper.current_revision, 2)
                self.assertEqual(document.revision, 1)
                self.assertEqual(document.sha256, first.sha256)
                self.assertEqual(
                    document.path,
                    self.storage.revision_path(paper_id, 1),
                )

    def test_private_revision_rejects_wrong_owner_hidden_paper_and_invalid_revision(self):
        owner_id = "00000000-0000-4000-8000-000000000403"
        other_id = "00000000-0000-4000-8000-000000000404"
        hidden_id = "00000000-0000-4000-8000-000000000405"
        self._seed_paper(
            paper_id=owner_id,
            filename="owner.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_stored_revision(owner_id, 1)
        self._seed_paper(
            paper_id=other_id,
            filename="other.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_paper(
            paper_id=hidden_id,
            filename="hidden-history.pdf",
            lifecycle_state="deleting",
            current_revision=1,
        )
        self._seed_stored_revision(hidden_id, 1)
        actor = Actor("contributor", 2)

        for target_id, revision in (
            (other_id, 1),
            (hidden_id, 1),
            (owner_id, 0),
            (owner_id, True),
            (owner_id, "1"),
        ):
            with (
                self.subTest(target_id=target_id, revision=revision),
                self.assertRaises(NotFound),
            ):
                self.library.private_revision_pdf(
                    target_id,
                    revision,
                    actor=actor,
                )

    def test_private_revision_rejects_missing_or_mismatched_immutable_file(self):
        actor = Actor("curator", 3)
        missing_id = "00000000-0000-4000-8000-000000000406"
        mismatch_id = "00000000-0000-4000-8000-000000000407"
        self._seed_paper(
            paper_id=missing_id,
            filename="missing-private.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_revision(
            missing_id,
            1,
            sha256="1" * 64,
            size_bytes=1,
        )
        self._seed_paper(
            paper_id=mismatch_id,
            filename="mismatch-private.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        stored = self._store_revision(mismatch_id, 1)
        self._seed_revision(
            mismatch_id,
            1,
            sha256="0" * 64,
            size_bytes=stored.size_bytes,
        )

        for paper_id in (missing_id, mismatch_id):
            with self.subTest(paper_id=paper_id), self.assertRaises(NotFound):
                self.library.private_revision_pdf(
                    paper_id,
                    1,
                    actor=actor,
                )

    def test_private_revision_fails_closed_if_verification_changes_db_snapshot(self):
        actor = Actor("curator", 3)
        cases = ("deleting", "revision-row")
        for offset, mutation in enumerate(cases, start=1):
            with self.subTest(mutation=mutation):
                paper_id = f"00000000-0000-4000-8000-{420 + offset:012d}"
                self._seed_paper(
                    paper_id=paper_id,
                    filename=f"{mutation}.pdf",
                    lifecycle_state="published",
                    current_revision=1,
                )
                self._seed_stored_revision(paper_id, 1)
                test_case = self

                class MutatingStorage:
                    def verify_revision(self, *args, **kwargs):
                        verified = test_case.storage.verify_revision(*args, **kwargs)
                        with test_case.session_factory() as session:
                            if mutation == "deleting":
                                paper = session.get(PaperMetadataModel, paper_id)
                                paper.lifecycle_state = "deleting"
                            else:
                                revision = session.get(
                                    PaperRevisionModel,
                                    (paper_id, 1),
                                )
                                revision.sha256 = "0" * 64
                            session.commit()
                        return verified

                library = PaperLibrary(
                    session_factory=self.session_factory,
                    storage=MutatingStorage(),
                )

                with self.assertRaises(NotFound):
                    library.private_revision_pdf(paper_id, 1, actor=actor)

    def test_library_keeps_its_caller_owned_storage_open(self):
        paper_id = "00000000-0000-4000-8000-000000000423"
        self._seed_paper(
            paper_id=paper_id,
            filename="caller-owned.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_stored_revision(paper_id, 1)

        self.library.current_pdf(paper_id)
        self.library.private_revision_pdf(
            paper_id,
            1,
            actor=Actor("contributor", 2),
        )

        self.assertEqual(
            self.storage.open_revision(paper_id, 1),
            self.storage.revision_path(paper_id, 1),
        )

    def test_list_visible_returns_only_verified_current_papers_in_display_order(self):
        older_id = "00000000-0000-4000-8000-000000000501"
        newer_id = "00000000-0000-4000-8000-000000000502"
        metadata_only_id = "00000000-0000-4000-8000-000000000503"
        missing_file_id = "00000000-0000-4000-8000-000000000504"
        hidden_id = "00000000-0000-4000-8000-000000000505"

        self._seed_paper(
            paper_id=older_id,
            filename="older.pdf",
            lifecycle_state="published",
            current_revision=1,
            title="Older",
        )
        self._seed_stored_revision(older_id, 1)
        self._seed_paper(
            paper_id=newer_id,
            filename="newer.pdf",
            lifecycle_state="published",
            current_revision=1,
            title="Newer",
        )
        self._seed_stored_revision(newer_id, 1)
        self._seed_paper(
            paper_id=metadata_only_id,
            filename="metadata-only.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_paper(
            paper_id=missing_file_id,
            filename="missing-file.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_revision(
            missing_file_id,
            1,
            sha256="5" * 64,
            size_bytes=5,
        )
        self._seed_paper(
            paper_id=hidden_id,
            filename="hidden.pdf",
            lifecycle_state="deleting",
            current_revision=1,
        )
        self._seed_stored_revision(hidden_id, 1)
        with self.session_factory() as session:
            older = session.get(PaperMetadataModel, older_id)
            newer = session.get(PaperMetadataModel, newer_id)
            older.published_at = "2026-07-20"
            newer.published_at = "2026-07-21"
            session.commit()

        records = self.library.list_visible()

        self.assertIsInstance(records, tuple)
        self.assertEqual(
            [record.paper_id for record in records],
            [newer_id, older_id],
        )

    def test_list_visible_never_treats_legacy_flat_pdfs_as_authoritative(self):
        stray = self.storage.papers_dir / "legacy-flat.pdf"
        stray.write_bytes(self.valid_pdf_bytes("legacy-flat"))
        stray.chmod(0o600)

        records = self.library.list_visible()

        self.assertEqual(records, ())

    def test_list_visible_opens_safely_without_hashing_the_corpus(self):
        paper_id = "00000000-0000-4000-8000-000000000506"
        self._seed_paper(
            paper_id=paper_id,
            filename="listed.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_stored_revision(paper_id, 1)
        test_case = self

        class ListingStorage:
            def open_revision(self, *args, **kwargs):
                return test_case.storage.open_revision(*args, **kwargs)

            def verify_revision(self, *args, **kwargs):
                raise AssertionError("listing must not hash Paper bytes")

        library = PaperLibrary(
            session_factory=self.session_factory,
            storage=ListingStorage(),
        )

        records = library.list_visible()

        self.assertEqual([record.paper_id for record in records], [paper_id])

    def test_list_visible_uses_stable_filename_and_id_tie_breakers(self):
        alpha_id = "00000000-0000-4000-8000-000000000601"
        zeta_id = "00000000-0000-4000-8000-000000000602"
        for paper_id, filename in (
            (alpha_id, "alpha.pdf"),
            (zeta_id, "zeta.pdf"),
        ):
            self._seed_paper(
                paper_id=paper_id,
                filename=filename,
                lifecycle_state="published",
                current_revision=1,
                title="Same Title",
            )
            self._seed_stored_revision(paper_id, 1)

        records = self.library.list_visible()

        self.assertEqual(
            [record.paper_id for record in records],
            [zeta_id, alpha_id],
        )

    def test_gather_records_projects_the_explicit_visible_library(self):
        paper_id = "00000000-0000-4000-8000-000000000603"
        self._seed_paper(
            paper_id=paper_id,
            filename="gathered.pdf",
            lifecycle_state="published",
            current_revision=1,
        )
        self._seed_stored_revision(paper_id, 1)

        records = gather_paper_records(self.library)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["paper_id"], paper_id)
        self.assertEqual(records[0]["current_revision"], 1)
        self.assertEqual(records[0]["filename"], "gathered.pdf")
        self.assertNotIn("row_version", records[0])

    def test_journal_counts_use_only_the_verified_visible_inventory(self):
        visible_id = "00000000-0000-4000-8000-000000000604"
        hidden_id = "00000000-0000-4000-8000-000000000605"
        unavailable_id = "00000000-0000-4000-8000-000000000606"
        for paper_id, state in (
            (visible_id, "published"),
            (hidden_id, "deleting"),
            (unavailable_id, "published"),
        ):
            self._seed_paper(
                paper_id=paper_id,
                filename=f"{paper_id[-3:]}.pdf",
                lifecycle_state=state,
                current_revision=1,
            )
        self._seed_stored_revision(visible_id, 1)
        self._seed_stored_revision(hidden_id, 1)

        counts = get_journal_paper_counts(self.library)

        self.assertEqual(counts, {"Journal": 1})

        with mock.patch(
            "services.journals.load_journals",
            return_value=[
                {
                    "id": "journal-1",
                    "name": "Journal",
                    "slug": "journal",
                    "created_at": "2026-07-21",
                }
            ],
        ):
            recent = get_recent_journals(4, library=self.library)

        self.assertEqual(recent[0]["paper_count"], 1)

    def test_list_visible_drops_candidates_changed_during_safe_open(self):
        mutations = (
            "lifecycle-state",
            "current-revision",
            "row-version",
            "revision-row",
        )
        for offset, mutation in enumerate(mutations, start=1):
            with self.subTest(mutation=mutation):
                paper_id = f"00000000-0000-4000-8000-{510 + offset:012d}"
                self._seed_paper(
                    paper_id=paper_id,
                    filename=f"{mutation}.pdf",
                    lifecycle_state="published",
                    current_revision=1,
                )
                self._seed_stored_revision(paper_id, 1)
                test_case = self

                class MutatingOpenStorage:
                    def open_revision(self, *args, **kwargs):
                        path = test_case.storage.open_revision(*args, **kwargs)
                        with test_case.session_factory() as session:
                            paper = session.get(PaperMetadataModel, paper_id)
                            if mutation == "lifecycle-state":
                                paper.lifecycle_state = "deleting"
                            elif mutation == "current-revision":
                                paper.current_revision = 2
                            elif mutation == "row-version":
                                paper.row_version += 1
                            else:
                                revision = session.get(
                                    PaperRevisionModel,
                                    (paper_id, 1),
                                )
                                revision.sha256 = "0" * 64
                            session.commit()
                        return path

                    def verify_revision(self, *args, **kwargs):
                        raise AssertionError("listing must not hash Paper bytes")

                library = PaperLibrary(
                    session_factory=self.session_factory,
                    storage=MutatingOpenStorage(),
                )

                self.assertNotIn(
                    paper_id,
                    {record.paper_id for record in library.list_visible()},
                )


if __name__ == "__main__":
    unittest.main()
