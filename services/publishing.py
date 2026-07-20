"""Framework-free Paper publication and revision-index lifecycle."""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Callable, Iterator, Protocol

from sqlalchemy.exc import IntegrityError

from models import (
    PaperChunkModel,
    PaperFilenameAliasModel,
    PaperMetadataModel,
    PaperRevisionModel,
    PublishingJobModel,
    RagIndexMetaModel,
)
from services.paper_identity import normalize_alias_key
from services.paper_storage import PaperStorage, StorageError
from services.publishing_contracts import (
    Actor,
    AliasConflict,
    DirectPublish,
    Forbidden,
    IdempotencyConflict,
    IndexDeadlineExceeded,
    IndexingOutcome,
    IndexingState,
    InvalidInput,
    JobLease,
    NormalizedPaperMetadata,
    PersistenceFailed,
    PreparedRevisionIndex,
    Published,
    StorageFailed,
)


_RESERVATION_TTL = timedelta(hours=1)
_REQUEST_LEASE_TTL = timedelta(seconds=1800)
_FIRST_RETRY_DELAY = timedelta(seconds=60)
_METADATA_STRING_LIMITS = {
    "filename": 255,
    "title": 255,
    "journal": 255,
    "category": 255,
    "language": 255,
    "author_name": 255,
    "author_email": 255,
    "author_school": 255,
    "published_at": 255,
    "is_ib_sample": 10,
    "is_anonymous": 10,
}


class RevisionIndexer(Protocol):
    def enabled(self) -> bool: ...

    def prepare(
        self,
        *,
        paper_id: str,
        revision_number: int,
        pdf_bytes: bytes,
        language: str,
        deadline: float,
    ) -> PreparedRevisionIndex: ...


class PublishingLifecycle:
    """Own direct publication across SQL, immutable storage, and derived RAG."""

    def __init__(
        self,
        *,
        session_factory,
        storage: PaperStorage,
        indexer: RevisionIndexer,
        clock: Callable,
        monotonic_clock: Callable[[], float],
        uuid_factory: Callable[[], str],
        jitter: Callable[[], float],
        inline_index_timeout_seconds: int = 45,
    ):
        self._session_factory = session_factory
        self._storage = storage
        self._indexer = indexer
        self._clock = clock
        self._monotonic_clock = monotonic_clock
        self._uuid_factory = uuid_factory
        self._jitter = jitter
        self._inline_index_timeout_seconds = inline_index_timeout_seconds

    @contextmanager
    def _session(self) -> Iterator:
        """Yield a caller-owned session; callers explicitly choose commit points."""
        session = self._session_factory()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _validate_intent(intent: DirectPublish) -> None:
        if not isinstance(intent, DirectPublish):
            raise InvalidInput({"intent": "must be a DirectPublish record"})
        if not isinstance(intent.actor, Actor) or (
            isinstance(intent.actor.role, bool)
            or not isinstance(intent.actor.role, int)
        ):
            raise InvalidInput({"actor": "is invalid"})
        if intent.actor.role < 2:
            raise Forbidden("direct publication requires Contributor access")

        errors: dict[str, str] = {}
        if (
            not isinstance(intent.actor.user_id, str)
            or not intent.actor.user_id
            or len(intent.actor.user_id) > 255
        ):
            errors["actor"] = "must identify a user"
        if (
            not isinstance(intent.idempotency_key, str)
            or not intent.idempotency_key
            or intent.idempotency_key != intent.idempotency_key.strip()
            or len(intent.idempotency_key) > 255
        ):
            errors["idempotency_key"] = "must be between 1 and 255 characters"

        metadata = intent.metadata
        if not isinstance(metadata, NormalizedPaperMetadata):
            raise InvalidInput({"metadata": "is invalid"})
        for name, value in asdict(metadata).items():
            if not isinstance(value, str):
                errors[name] = "must be a string"
                continue
            if value != value.strip():
                errors[name] = "must already be normalized"
            limit = _METADATA_STRING_LIMITS.get(name)
            if limit is not None and len(value) > limit:
                errors[name] = f"must be at most {limit} characters"

        filename = metadata.filename
        if (
            not filename
            or Path(filename).name != filename
            or "\\" in filename
            or filename.casefold().endswith(".pdf") is False
        ):
            errors["filename"] = "must be a normalized PDF filename"
        elif len(normalize_alias_key(filename)) > 255:
            errors["filename"] = "normalized filename is too long"
        if not metadata.title:
            errors["title"] = "is required"
        if not metadata.language:
            errors["language"] = "is required"
        if errors:
            raise InvalidInput(errors)

    @staticmethod
    def _payload_hash(intent: DirectPublish, source_sha256: str) -> str:
        payload = {
            "metadata": asdict(intent.metadata),
            "source_sha256": source_sha256,
        }
        canonical = json.dumps(
            payload,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def _new_reservation(self, intent: DirectPublish, payload_hash: str):
        now = self._clock()
        return PaperMetadataModel(
            id=self._uuid_factory(),
            **asdict(intent.metadata),
            lifecycle_state="publishing",
            current_revision=None,
            row_version=0,
            index_status="pending",
            indexed_revision=None,
            index_error=None,
            direct_idempotency_key=intent.idempotency_key,
            direct_payload_hash=payload_hash,
            reservation_expires_at=now + _RESERVATION_TTL,
        )

    def _reservation_conflict(self, intent: DirectPublish, payload_hash: str):
        """Read the row that won a unique-key race after the insert rolled back."""
        with self._session() as session:
            paper = (
                session.query(PaperMetadataModel)
                .filter(PaperMetadataModel.direct_idempotency_key == intent.idempotency_key)
                .with_for_update()
                .one_or_none()
            )
            if paper is None:
                alias = session.get(
                    PaperFilenameAliasModel,
                    normalize_alias_key(intent.metadata.filename),
                )
                filename_owner = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.filename == intent.metadata.filename)
                    .one_or_none()
                )
                if alias is not None or filename_owner is not None:
                    raise AliasConflict(intent.metadata.filename)
                raise PersistenceFailed("publication reservation conflicted")
            if paper.direct_payload_hash != payload_hash:
                raise IdempotencyConflict(intent.idempotency_key)
            if paper.lifecycle_state == "published":
                outcome = self._indexing_outcome(session, paper)
                return "published", paper.id, outcome
            if paper.lifecycle_state != "publishing" or paper.current_revision is not None:
                raise PersistenceFailed("publication reservation has an invalid state")
            if paper.reservation_expires_at is None or paper.reservation_expires_at <= self._clock():
                paper_id = paper.id
                session.delete(paper)
                session.commit()
                return "expired", paper_id, None
            return "reserved", paper.id, None

    def _reserve(self, intent: DirectPublish, payload_hash: str):
        """Reserve by insertion; unique constraints serialize key/filename races."""
        while True:
            reservation = self._new_reservation(intent, payload_hash)
            try:
                with self._session() as session:
                    session.add(reservation)
                    session.commit()
                return "reserved", reservation.id, None
            except IntegrityError:
                state, paper_id, outcome = self._reservation_conflict(intent, payload_hash)
                if state == "expired":
                    self._remove_unreferenced_paper_files(paper_id)
                    continue
                return state, paper_id, outcome
            except (AliasConflict, IdempotencyConflict, PersistenceFailed):
                raise
            except Exception as exc:
                raise PersistenceFailed("could not reserve publication") from exc

    def _remove_unreferenced_paper_files(self, paper_id: str) -> None:
        """Remove only a Paper ID proven absent or still hidden in SQL."""
        try:
            with self._session() as session:
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                if paper is not None and (
                    paper.lifecycle_state != "publishing" or paper.current_revision is not None
                ):
                    return
                referenced = (
                    session.query(PaperRevisionModel)
                    .filter(PaperRevisionModel.paper_id == paper_id)
                    .first()
                )
                if referenced is not None:
                    return
                self._storage.delete_paper(paper_id, ())
                session.commit()
        except StorageError as exc:
            raise StorageFailed("could not reconcile unpublished PDF") from exc
        except StorageFailed:
            raise
        except Exception as exc:
            raise PersistenceFailed("could not reconcile unpublished PDF") from exc

    def _abandon_hidden_reservation(self, paper_id: str) -> None:
        """Remove a terminally-conflicted reservation and its known final bytes."""
        try:
            with self._session() as session:
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                if paper is None:
                    self._storage.delete_paper(paper_id, ())
                    session.commit()
                    return
                if paper.lifecycle_state != "publishing" or paper.current_revision is not None:
                    return
                if (
                    session.query(PaperRevisionModel)
                    .filter(PaperRevisionModel.paper_id == paper_id)
                    .first()
                    is not None
                ):
                    return
                self._storage.delete_paper(paper_id, ())
                session.delete(paper)
                session.commit()
        except StorageError as exc:
            raise StorageFailed("could not remove conflicted publication") from exc
        except StorageFailed:
            raise
        except Exception as exc:
            raise PersistenceFailed("could not remove conflicted publication") from exc

    def _bump_rag_version(self, session) -> None:
        stamp = (
            session.query(RagIndexMetaModel)
            .filter(RagIndexMetaModel.name == "chunks_version")
            .with_for_update()
            .one_or_none()
        )
        if stamp is None:
            session.add(RagIndexMetaModel(name="chunks_version", value=1))
        else:
            stamp.value += 1

    def _enqueue_index_job(
        self,
        session,
        paper_id: str,
        revision: int,
        lease_token: str,
    ) -> JobLease:
        now = self._clock()
        lease_expires_at = now + _REQUEST_LEASE_TTL
        dedupe_key = f"index:{paper_id}:{revision}"
        job = (
            session.query(PublishingJobModel)
            .filter(PublishingJobModel.dedupe_key == dedupe_key)
            .with_for_update()
            .one_or_none()
        )
        if job is None:
            job = PublishingJobModel(
                id=self._uuid_factory(),
                kind="index_revision",
                paper_id=paper_id,
                revision_number=revision,
                dedupe_key=dedupe_key,
                state="running",
                attempts=1,
                available_at=now,
                lease_token=lease_token,
                lease_expires_at=lease_expires_at,
                last_error=None,
                created_at=now,
                updated_at=now,
            )
            session.add(job)
            previous_updated_at = now
        else:
            previous_updated_at = job.updated_at
            job.state = "running"
            job.attempts += 1
            job.lease_token = lease_token
            job.lease_expires_at = lease_expires_at
            job.updated_at = now
        return JobLease(
            job_id=job.id,
            paper_id=paper_id,
            revision=revision,
            kind="index_revision",
            attempts=job.attempts,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
            created_at=job.created_at,
            previous_updated_at=previous_updated_at,
        )

    def _make_visible(
        self,
        *,
        intent: DirectPublish,
        paper_id: str,
        payload_hash: str,
        stored,
        indexing_enabled: bool,
    ) -> JobLease | None:
        with self._session() as session:
            paper = (
                session.query(PaperMetadataModel)
                .filter(PaperMetadataModel.id == paper_id)
                .with_for_update()
                .one_or_none()
            )
            if paper is None:
                raise PersistenceFailed("publication reservation disappeared")
            if paper.direct_payload_hash != payload_hash:
                raise IdempotencyConflict(intent.idempotency_key)
            if paper.lifecycle_state == "published":
                return None
            if paper.lifecycle_state != "publishing" or paper.current_revision is not None:
                raise PersistenceFailed("publication reservation changed")

            lookup_key = normalize_alias_key(intent.metadata.filename)
            alias = (
                session.query(PaperFilenameAliasModel)
                .filter(PaperFilenameAliasModel.lookup_key == lookup_key)
                .with_for_update()
                .one_or_none()
            )
            if alias is not None and alias.paper_id != paper_id:
                raise AliasConflict(intent.metadata.filename)

            now = self._clock()
            session.add(
                PaperRevisionModel(
                    paper_id=paper_id,
                    revision_number=1,
                    sha256=stored.sha256,
                    size_bytes=stored.size_bytes,
                    created_at=now,
                    created_by=intent.actor.user_id,
                )
            )
            if alias is None:
                session.add(
                    PaperFilenameAliasModel(
                        lookup_key=lookup_key,
                        filename=intent.metadata.filename,
                        paper_id=paper_id,
                        created_at=now,
                    )
                )
            paper.lifecycle_state = "published"
            paper.current_revision = 1
            paper.row_version = 1
            paper.index_status = "pending"
            paper.indexed_revision = None
            paper.index_error = None
            paper.reservation_expires_at = None
            lease = None
            if indexing_enabled:
                lease = self._enqueue_index_job(
                    session,
                    paper_id,
                    1,
                    self._uuid_factory(),
                )
            self._bump_rag_version(session)
            session.commit()
            return lease

    def _published_row(self, paper_id: str):
        with self._session() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            if paper is None or paper.lifecycle_state != "published":
                return None
            return (
                paper.filename,
                paper.current_revision,
                paper.row_version,
                self._indexing_outcome(session, paper),
            )

    def _alias_owned_by_other(self, filename: str, paper_id: str) -> bool:
        with self._session() as session:
            alias = session.get(
                PaperFilenameAliasModel,
                normalize_alias_key(filename),
            )
            return alias is not None and alias.paper_id != paper_id

    @staticmethod
    def _lease_matches(job, paper, lease: JobLease) -> bool:
        return bool(
            job is not None
            and paper is not None
            and job.id == lease.job_id
            and job.kind == lease.kind == "index_revision"
            and job.paper_id == paper.id == lease.paper_id
            and job.revision_number == lease.revision
            and job.state == "running"
            and job.lease_token == lease.lease_token
            and job.lease_expires_at is not None
            and paper.lifecycle_state == "published"
            and paper.current_revision == lease.revision
        )

    def _complete_index(self, lease: JobLease, prepared: PreparedRevisionIndex) -> bool:
        if (
            prepared.paper_id != lease.paper_id
            or prepared.revision != lease.revision
        ):
            return False
        try:
            with self._session() as session:
                job = (
                    session.query(PublishingJobModel)
                    .filter(PublishingJobModel.id == lease.job_id)
                    .with_for_update()
                    .one_or_none()
                )
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == lease.paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                if (
                    not self._lease_matches(job, paper, lease)
                    or job.lease_expires_at <= self._clock()
                ):
                    return False
                session.query(PaperChunkModel).filter(
                    PaperChunkModel.paper_id == lease.paper_id
                ).delete(synchronize_session=False)
                for chunk in prepared.chunks:
                    session.add(
                        PaperChunkModel(
                            filename=paper.filename,
                            paper_id=lease.paper_id,
                            revision_number=lease.revision,
                            chunk_index=chunk.chunk_index,
                            content=chunk.content,
                            embedding_vec=json.dumps(
                                chunk.embedding,
                                separators=(",", ":"),
                            ),
                            lang=chunk.language,
                        )
                    )
                paper.index_status = "ready"
                paper.indexed_revision = lease.revision
                paper.index_error = None
                self._bump_rag_version(session)
                session.delete(job)
                session.commit()
                return True
        except Exception as exc:
            raise PersistenceFailed("could not persist prepared index") from exc

    @staticmethod
    def _redacted_index_error(error: Exception) -> str:
        if isinstance(error, IndexDeadlineExceeded):
            return "index deadline exceeded"
        return f"{type(error).__name__}: indexing failed"[:255]

    def _mark_index_failure(self, lease: JobLease, error: Exception) -> IndexingOutcome:
        retry_at = self._clock() + _FIRST_RETRY_DELAY + timedelta(
            seconds=max(0.0, min(1.0, float(self._jitter()))) * 30.0
        )
        redacted = self._redacted_index_error(error)
        try:
            with self._session() as session:
                job = (
                    session.query(PublishingJobModel)
                    .filter(PublishingJobModel.id == lease.job_id)
                    .with_for_update()
                    .one_or_none()
                )
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == lease.paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                if (
                    not self._lease_matches(job, paper, lease)
                    or job.lease_expires_at <= self._clock()
                ):
                    if paper is None:
                        return IndexingOutcome(IndexingState.PENDING)
                    return self._indexing_outcome(session, paper)
                job.state = "pending"
                job.available_at = retry_at
                job.lease_token = None
                job.lease_expires_at = None
                job.last_error = redacted
                job.updated_at = self._clock()
                paper.index_status = "failed"
                paper.indexed_revision = None
                paper.index_error = redacted
                session.commit()
                return IndexingOutcome(
                    IndexingState.FAILED,
                    job_id=job.id,
                    next_retry_at=retry_at,
                )
        except Exception:
            # The durable running lease remains recoverable if releasing it fails.
            return IndexingOutcome(IndexingState.PENDING, job_id=lease.job_id)

    @staticmethod
    def _indexing_outcome(session, paper) -> IndexingOutcome:
        if (
            paper.index_status == "ready"
            and paper.indexed_revision == paper.current_revision
        ):
            return IndexingOutcome(IndexingState.INDEXED)
        job = (
            session.query(PublishingJobModel)
            .filter(
                PublishingJobModel.paper_id == paper.id,
                PublishingJobModel.revision_number == paper.current_revision,
                PublishingJobModel.kind == "index_revision",
            )
            .one_or_none()
        )
        if job is None:
            return IndexingOutcome(IndexingState.NOT_REQUIRED)
        if job.state == "pending" and paper.index_status == "failed":
            return IndexingOutcome(
                IndexingState.FAILED,
                job_id=job.id,
                next_retry_at=job.available_at,
            )
        return IndexingOutcome(IndexingState.PENDING, job_id=job.id)

    def _run_inline_index(self, paper_id: str, lease: JobLease, deadline: float):
        try:
            if self._monotonic_clock() >= deadline:
                raise IndexDeadlineExceeded()
            pdf_path = self._storage.open_revision(paper_id, lease.revision)
            pdf_bytes = pdf_path.read_bytes()
            with self._session() as session:
                paper = session.get(PaperMetadataModel, paper_id)
                if paper is None:
                    return IndexingOutcome(IndexingState.PENDING, job_id=lease.job_id)
                language = paper.language or ""
            prepared = self._indexer.prepare(
                paper_id=paper_id,
                revision_number=lease.revision,
                pdf_bytes=pdf_bytes,
                language=language,
                deadline=deadline,
            )
            if (
                not isinstance(prepared, PreparedRevisionIndex)
                or prepared.paper_id != lease.paper_id
                or prepared.revision != lease.revision
            ):
                raise ValueError("indexer prepared the wrong Paper revision")
            if self._monotonic_clock() > deadline:
                raise IndexDeadlineExceeded()
            if self._complete_index(lease, prepared):
                return IndexingOutcome(IndexingState.INDEXED)
            with self._session() as session:
                paper = session.get(PaperMetadataModel, paper_id)
                if paper is None:
                    return IndexingOutcome(IndexingState.PENDING, job_id=lease.job_id)
                return self._indexing_outcome(session, paper)
        except Exception as exc:
            return self._mark_index_failure(lease, exc)

    def publish_direct(self, intent: DirectPublish) -> Published:
        """Publish a validated PDF; RAG failure never rolls back visibility."""
        self._validate_intent(intent)
        deadline = self._monotonic_clock() + self._inline_index_timeout_seconds
        operation_id = self._uuid_factory()
        try:
            staged = self._storage.stage(intent.pdf, operation_id)
        except StorageError as exc:
            raise StorageFailed("PDF could not be staged") from exc

        payload_hash = self._payload_hash(intent, staged.source_sha256)
        state, paper_id, replay_outcome = self._reserve(intent, payload_hash)
        if state == "published":
            with self._session() as session:
                paper = session.get(PaperMetadataModel, paper_id)
                return Published(
                    paper_id=paper.id,
                    filename=paper.filename,
                    revision=paper.current_revision,
                    row_version=paper.row_version,
                    replayed=True,
                    indexing=replay_outcome,
                )

        try:
            prepared_pdf = self._storage.apply_metadata(
                staged,
                title=intent.metadata.title,
                author=intent.metadata.author_name,
            )
            stored = self._storage.promote(prepared_pdf, paper_id, 1)
        except StorageError as exc:
            self._remove_unreferenced_paper_files(paper_id)
            raise StorageFailed("PDF could not be published") from exc

        enabled_error = None
        try:
            indexing_enabled = bool(self._indexer.enabled())
        except Exception as exc:
            # A broken capability probe is an indexing failure, not a reason to
            # hide an otherwise durable publication.  Reserve retry work.
            indexing_enabled = True
            enabled_error = exc
        try:
            lease = self._make_visible(
                intent=intent,
                paper_id=paper_id,
                payload_hash=payload_hash,
                stored=stored,
                indexing_enabled=indexing_enabled,
            )
        except AliasConflict:
            self._abandon_hidden_reservation(paper_id)
            raise
        except IdempotencyConflict:
            self._remove_unreferenced_paper_files(paper_id)
            raise
        except Exception as exc:
            if isinstance(exc, IntegrityError) and self._alias_owned_by_other(
                intent.metadata.filename,
                paper_id,
            ):
                self._abandon_hidden_reservation(paper_id)
                raise AliasConflict(intent.metadata.filename) from exc
            recovered = self._published_row(paper_id)
            if recovered is None:
                self._remove_unreferenced_paper_files(paper_id)
                if isinstance(exc, PersistenceFailed):
                    raise
                raise PersistenceFailed("could not make publication visible") from exc
            filename, revision, row_version, outcome = recovered
            return Published(
                paper_id=paper_id,
                filename=filename,
                revision=revision,
                row_version=row_version,
                replayed=False,
                indexing=outcome,
            )

        if not indexing_enabled:
            indexing = IndexingOutcome(IndexingState.NOT_REQUIRED)
        elif lease is None:
            recovered = self._published_row(paper_id)
            indexing = recovered[3]
        elif enabled_error is not None:
            indexing = self._mark_index_failure(lease, enabled_error)
        else:
            indexing = self._run_inline_index(paper_id, lease, deadline)
        return Published(
            paper_id=paper_id,
            filename=intent.metadata.filename,
            revision=1,
            row_version=1,
            replayed=False,
            indexing=indexing,
        )

    def direct_publish(self, intent: DirectPublish) -> Published:
        """Task 1 protocol-compatible spelling."""
        return self.publish_direct(intent)
