"""Framework-free reads for visible Papers and immutable revisions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from models import (
    PaperFilenameAliasModel,
    PaperMetadataModel,
    PaperRevisionModel,
)
from services.paper_identity import normalize_alias_key, validate_paper_id
from services.paper_storage import StorageError
from services.publishing_contracts import Actor, Forbidden, NotFound


@dataclass(frozen=True)
class PaperRecord:
    """Detached display identity for one currently visible Paper."""

    paper_id: str
    current_revision: int
    row_version: int
    filename: str
    title: str
    journal: str
    category: str
    language: str
    keywords: str
    abstract: str
    author_name: str
    author_email: str
    author_school: str
    published_at: str
    ib_ee_data: str
    is_ib_sample: str
    cp_data: str
    is_anonymous: str
    ia_data: str


@dataclass(frozen=True)
class PaperPdf:
    """One immutable metadata/revision/file snapshot."""

    paper: PaperRecord
    revision: int
    path: Path
    sha256: str
    size_bytes: int


class PaperLibrary:
    """Central read module for Paper visibility and immutable PDF access."""

    def __init__(self, *, session_factory, storage):
        """Use caller-owned dependencies; this module never closes ``storage``."""
        self._session_factory = session_factory
        self._storage = storage

    @staticmethod
    def _canonical_id(paper_id: str) -> str:
        try:
            return validate_paper_id(paper_id)
        except (AttributeError, TypeError, ValueError) as exc:
            raise NotFound() from exc

    @staticmethod
    def _record(paper: PaperMetadataModel) -> PaperRecord:
        filename = paper.filename
        return PaperRecord(
            paper_id=paper.id,
            current_revision=paper.current_revision,
            row_version=paper.row_version,
            filename=filename,
            title=paper.title or Path(filename).stem,
            journal=paper.journal or "",
            category=paper.category or "",
            language=paper.language or "",
            keywords=paper.keywords or "",
            abstract=paper.abstract or "",
            author_name=paper.author_name or "",
            author_email=paper.author_email or "",
            author_school=paper.author_school or "",
            published_at=paper.published_at or "",
            ib_ee_data=paper.ib_ee_data or "",
            is_ib_sample=paper.is_ib_sample or "",
            cp_data=paper.cp_data or "",
            is_anonymous=paper.is_anonymous or "",
            ia_data=paper.ia_data or "",
        )

    @staticmethod
    def _visible_row(session, canonical_id: str) -> PaperMetadataModel:
        paper = session.get(PaperMetadataModel, canonical_id)
        if (
            paper is None
            or paper.lifecycle_state != "published"
            or type(paper.current_revision) is not int
            or paper.current_revision < 1
        ):
            raise NotFound()
        return paper

    def visible_by_id(self, paper_id: str) -> PaperRecord:
        canonical_id = self._canonical_id(paper_id)
        with self._session_factory() as session:
            return self._record(self._visible_row(session, canonical_id))

    def resolve_alias(self, filename: str) -> PaperRecord:
        if type(filename) is not str:
            raise NotFound()
        try:
            lookup_key = normalize_alias_key(filename)
        except (TypeError, ValueError) as exc:
            raise NotFound() from exc
        with self._session_factory() as session:
            alias = session.get(PaperFilenameAliasModel, lookup_key)
            if alias is None:
                raise NotFound()
            return self._record(self._visible_row(session, alias.paper_id))

    def _current_snapshot(self, canonical_id: str):
        with self._session_factory() as session:
            paper = self._visible_row(session, canonical_id)
            revision_number = paper.current_revision
            if type(revision_number) is not int or revision_number < 1:
                raise NotFound()
            revision = session.get(
                PaperRevisionModel,
                (canonical_id, revision_number),
            )
            if revision is None:
                raise NotFound()
            return (
                self._record(paper),
                revision.sha256,
                revision.size_bytes,
            )

    def _revision_snapshot_still_matches(
        self,
        record: PaperRecord,
        *,
        revision_number: int,
        sha256: str,
        size_bytes: int,
    ) -> bool:
        with self._session_factory() as session:
            paper = session.get(PaperMetadataModel, record.paper_id)
            if (
                paper is None
                or paper.lifecycle_state != "published"
                or paper.current_revision != record.current_revision
                or paper.row_version != record.row_version
            ):
                return False
            revision = session.get(
                PaperRevisionModel,
                (record.paper_id, revision_number),
            )
            return (
                revision is not None
                and revision.sha256 == sha256
                and revision.size_bytes == size_bytes
            )

    def _verified_pdf(
        self,
        record: PaperRecord,
        *,
        revision_number: int,
        sha256: str,
        size_bytes: int,
    ) -> PaperPdf:
        try:
            stored = self._storage.verify_revision(
                record.paper_id,
                revision_number,
                sha256=sha256,
                size_bytes=size_bytes,
            )
        except StorageError as exc:
            raise NotFound() from exc
        if (
            stored.sha256 != sha256
            or stored.size_bytes != size_bytes
            or not self._revision_snapshot_still_matches(
                record,
                revision_number=revision_number,
                sha256=sha256,
                size_bytes=size_bytes,
            )
        ):
            raise NotFound()
        return PaperPdf(
            paper=record,
            revision=revision_number,
            path=stored.path,
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
        )

    def current_pdf(self, paper_id: str) -> PaperPdf:
        canonical_id = self._canonical_id(paper_id)
        record, sha256, size_bytes = self._current_snapshot(canonical_id)
        return self._verified_pdf(
            record,
            revision_number=record.current_revision,
            sha256=sha256,
            size_bytes=size_bytes,
        )

    def list_visible(self) -> tuple[PaperRecord, ...]:
        """Return safely openable current Papers without hashing PDF bodies."""
        with self._session_factory() as session:
            rows = (
                session.query(PaperMetadataModel, PaperRevisionModel)
                .join(
                    PaperRevisionModel,
                    (PaperRevisionModel.paper_id == PaperMetadataModel.id)
                    & (
                        PaperRevisionModel.revision_number
                        == PaperMetadataModel.current_revision
                    ),
                )
                .filter(PaperMetadataModel.lifecycle_state == "published")
                .all()
            )
            candidates = {
                paper.id: (
                    self._record(paper),
                    revision.sha256,
                    revision.size_bytes,
                )
                for paper, revision in rows
                if type(paper.current_revision) is int
                and paper.current_revision > 0
            }

        opened = {}
        for paper_id, snapshot in candidates.items():
            try:
                self._storage.open_revision(
                    paper_id,
                    snapshot[0].current_revision,
                )
            except StorageError:
                continue
            opened[paper_id] = snapshot

        if not opened:
            return ()

        with self._session_factory() as session:
            final_rows = (
                session.query(PaperMetadataModel, PaperRevisionModel)
                .join(
                    PaperRevisionModel,
                    (PaperRevisionModel.paper_id == PaperMetadataModel.id)
                    & (
                        PaperRevisionModel.revision_number
                        == PaperMetadataModel.current_revision
                    ),
                )
                .filter(PaperMetadataModel.lifecycle_state == "published")
                .all()
            )
            unchanged = {
                paper.id
                for paper, revision in final_rows
                if paper.id in opened
                and type(paper.current_revision) is int
                and paper.current_revision == opened[paper.id][0].current_revision
                and paper.row_version == opened[paper.id][0].row_version
                and revision.sha256 == opened[paper.id][1]
                and revision.size_bytes == opened[paper.id][2]
            }

        records = [opened[paper_id][0] for paper_id in unchanged]
        records.sort(
            key=lambda record: (
                record.published_at,
                record.title or record.filename,
                record.filename,
                record.paper_id,
            ),
            reverse=True,
        )
        return tuple(records)

    @staticmethod
    def _authorize_private_revision(actor: Actor) -> None:
        if (
            not isinstance(actor, Actor)
            or type(actor.role) is not int
            or actor.role not in {2, 3}
            or type(actor.user_id) is not str
            or not actor.user_id
            or actor.user_id != actor.user_id.strip()
            or len(actor.user_id) > 255
        ):
            raise Forbidden()

    def private_revision_pdf(
        self,
        paper_id: str,
        revision: int,
        *,
        actor: Actor,
    ) -> PaperPdf:
        self._authorize_private_revision(actor)
        if type(revision) is not int or revision < 1:
            raise NotFound()
        canonical_id = self._canonical_id(paper_id)
        with self._session_factory() as session:
            paper = self._visible_row(session, canonical_id)
            revision_row = session.get(
                PaperRevisionModel,
                (canonical_id, revision),
            )
            if revision_row is None:
                raise NotFound()
            record = self._record(paper)
            sha256 = revision_row.sha256
            size_bytes = revision_row.size_bytes
        return self._verified_pdf(
            record,
            revision_number=revision,
            sha256=sha256,
            size_bytes=size_bytes,
        )
