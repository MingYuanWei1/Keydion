"""Framework-free Paper publication and revision-index lifecycle."""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterator, Protocol

from sqlalchemy.exc import IntegrityError

from models import (
    PaperChunkModel,
    PaperFilenameAliasModel,
    PaperMetadataModel,
    PaperRevisionModel,
    PublishingJobModel,
    PublishingMigrationIssueModel,
    PublishingMigrationJournalModel,
    RagIndexMetaModel,
    SubmissionModel,
)
from services.paper_identity import normalize_alias_key, validate_paper_id
from services.paper_storage import (
    PaperStorage,
    PendingTrash,
    StorageError,
    SubmissionTrashRecord,
)
from services.submission_fence import lock_submission_creation_fence
from services.publishing_contracts import (
    Actor,
    AcceptSubmission,
    AliasConflict,
    BulkEditMetadata,
    BulkPapersChanged,
    CancelSubmission,
    DecisionConflict,
    DecisionRecorded,
    DeletePaper,
    DeletionProgress,
    DeletionState,
    DirectPublish,
    EditMetadata,
    Forbidden,
    IdempotencyConflict,
    IndexDeadlineExceeded,
    IndexingOutcome,
    IndexingState,
    InvalidInput,
    JobLease,
    JobProgress,
    JobState,
    MetadataPatch,
    NormalizedPaperMetadata,
    NotFound,
    PersistenceFailed,
    PaperChanged,
    PdfUpload,
    PreparedRevisionIndex,
    Published,
    RejectSubmission,
    RestoreRevision,
    RevisePdf,
    StaleVersion,
    StorageFailed,
    SubmissionCancelled,
    SubmissionNotPending,
)


_RESERVATION_TTL = timedelta(hours=1)
_REQUEST_LEASE_TTL = timedelta(seconds=1800)
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
_PAPER_METADATA_FIELDS = tuple(asdict(NormalizedPaperMetadata()).keys())


def _submission_trash_operation_id(submission_id: str) -> str:
    digest = hashlib.sha256(submission_id.encode("utf-8")).hexdigest()
    return f"submission-{digest}"


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


class _SubmissionDecision(DecisionRecorded):
    @property
    def decision(self) -> str:
        return "accepted" if self.accepted else "rejected"


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
            or intent.actor.role not in {1, 2, 3}
        ):
            raise InvalidInput({"actor": "is invalid"})
        if intent.actor.role < 2:
            raise Forbidden("direct publication requires Contributor access")

        errors: dict[str, str] = {}
        if (
            not isinstance(intent.actor.user_id, str)
            or not intent.actor.user_id
            or intent.actor.user_id != intent.actor.user_id.strip()
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

    @staticmethod
    def _validate_submission_actor(actor: Actor, *, role: int) -> None:
        if not isinstance(actor, Actor) or (
            isinstance(actor.role, bool)
            or not isinstance(actor.role, int)
            or actor.role not in {1, 2, 3}
        ):
            raise InvalidInput({"actor": "is invalid"})
        if actor.role != role:
            if role == 3:
                raise Forbidden("Submission review requires Curator access")
            raise Forbidden("only the submitting Reader may cancel")
        if (
            not isinstance(actor.user_id, str)
            or not actor.user_id
            or actor.user_id != actor.user_id.strip()
            or len(actor.user_id) > 255
        ):
            raise InvalidInput({"actor": "must identify a user"})

    @staticmethod
    def _validate_submission_id(submission_id: str) -> None:
        if (
            not isinstance(submission_id, str)
            or not submission_id
            or submission_id != submission_id.strip()
            or len(submission_id) > 255
        ):
            raise InvalidInput({"submission_id": "must identify a Submission"})

    @staticmethod
    def _submission_by_id(session, submission_id: str, *, locked: bool = False):
        query = session.query(SubmissionModel).filter(
            SubmissionModel.id == submission_id
        )
        if locked:
            query = query.with_for_update()
        submission = query.one_or_none()
        if submission is not None and submission.id != submission_id:
            return None
        return submission

    def _validate_acceptance(self, intent: AcceptSubmission) -> None:
        if not isinstance(intent, AcceptSubmission):
            raise InvalidInput({"intent": "must be a Submission review record"})
        self._validate_submission_actor(intent.actor, role=3)
        self._validate_submission_id(intent.submission_id)
        self._validate_intent(
            DirectPublish(
                actor=intent.actor,
                idempotency_key=intent.idempotency_key,
                metadata=intent.metadata,
                pdf=intent.pdf,
            )
        )

    def _validate_rejection(self, intent: RejectSubmission) -> None:
        if not isinstance(intent, RejectSubmission):
            raise InvalidInput({"intent": "must be a Submission review record"})
        self._validate_submission_actor(intent.actor, role=3)
        self._validate_submission_id(intent.submission_id)
        if not isinstance(intent.feedback, str):
            raise InvalidInput({"comment": "must be a string"})

    def _validate_cancellation(self, intent: CancelSubmission) -> None:
        if not isinstance(intent, CancelSubmission):
            raise InvalidInput({"intent": "must be a CancelSubmission record"})
        self._validate_submission_actor(intent.actor, role=1)
        self._validate_submission_id(intent.submission_id)

    @staticmethod
    def _decision_payload_hash(
        submission_id: str,
        decision: str,
        reviewer: str,
        comment: str,
    ) -> str:
        canonical = json.dumps(
            {
                "comment": comment,
                "decision": decision,
                "reviewer": reviewer,
                "submission_id": submission_id,
            },
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    @staticmethod
    def _rejection_key(submission_id: str, reviewer: str) -> str:
        canonical = f"{submission_id}\x00{reviewer}".encode("utf-8")
        return f"reject:{hashlib.sha256(canonical).hexdigest()}"

    @staticmethod
    def _metadata_matches(paper, metadata: NormalizedPaperMetadata) -> bool:
        return all(
            (getattr(paper, field) or "") == getattr(metadata, field)
            for field in _PAPER_METADATA_FIELDS
        )

    def _reconstruct_decision_locked(
        self,
        session,
        submission,
        *,
        decision: str,
        decision_key: str,
        payload_hash: str,
    ) -> _SubmissionDecision | None:
        if submission.status not in {"accepted", "rejected"}:
            return None
        if (
            submission.status != decision
            or submission.decision_idempotency_key != decision_key
            or submission.decision_payload_hash != payload_hash
        ):
            raise DecisionConflict("Submission already has a different decision")
        if decision == "rejected":
            if submission.paper_id is not None:
                raise PersistenceFailed("rejected Submission is linked to a Paper")
            return _SubmissionDecision(
                submission_id=submission.id,
                accepted=False,
                paper_id=None,
                replayed=True,
                indexing=None,
            )
        if submission.paper_id is None:
            # A decided acceptance is permanent even after its Paper is hidden
            # and deleted.  Exact decision key/hash validation above prevents a
            # migrated unresolved acceptance from being mistaken for this replay.
            return _SubmissionDecision(
                submission_id=submission.id,
                accepted=True,
                paper_id=None,
                replayed=True,
                indexing=None,
            )
        paper = session.get(PaperMetadataModel, submission.paper_id)
        if (
            paper is None
            or paper.lifecycle_state != "published"
            or paper.origin_submission_id != submission.id
        ):
            raise PersistenceFailed("accepted Submission has no visible Paper")
        return _SubmissionDecision(
            submission_id=submission.id,
            accepted=True,
            paper_id=paper.id,
            replayed=True,
            indexing=self._indexing_outcome(session, paper),
        )

    def _acceptance_preflight(
        self,
        intent: AcceptSubmission,
        decision_key: str,
        payload_hash: str,
    ) -> tuple[str, _SubmissionDecision | None]:
        try:
            with self._session() as session:
                lock_submission_creation_fence(session)
                submission = self._submission_by_id(
                    session, intent.submission_id, locked=True,
                )
                if submission is None:
                    raise NotFound("Submission not found")
                existing = self._reconstruct_decision_locked(
                    session,
                    submission,
                    decision="accepted",
                    decision_key=decision_key,
                    payload_hash=payload_hash,
                )
                if existing is not None:
                    return submission.pending_filename, existing
                if submission.status != "pending":
                    raise SubmissionNotPending("Submission is not pending")
                return submission.pending_filename, None
        except (NotFound, DecisionConflict, SubmissionNotPending, PersistenceFailed):
            raise
        except Exception as exc:
            raise PersistenceFailed("could not inspect Submission decision") from exc

    def _acceptance_reservation_conflict(
        self,
        intent: AcceptSubmission,
        pending_filename: str,
        decision_key: str,
        payload_hash: str,
    ):
        with self._session() as session:
            submission = self._submission_by_id(
                session, intent.submission_id, locked=True,
            )
            if submission is None:
                raise NotFound("Submission not found")
            existing_decision = self._reconstruct_decision_locked(
                session,
                submission,
                decision="accepted",
                decision_key=decision_key,
                payload_hash=payload_hash,
            )
            if existing_decision is not None:
                return "accepted", existing_decision.paper_id, existing_decision
            if submission.status != "pending":
                raise SubmissionNotPending("Submission is not pending")
            if submission.pending_filename != pending_filename:
                raise DecisionConflict("Submission source changed during review")
            paper = (
                session.query(PaperMetadataModel)
                .filter(PaperMetadataModel.origin_submission_id == intent.submission_id)
                .with_for_update()
                .one_or_none()
            )
            if paper is not None:
                if (
                    paper.lifecycle_state != "publishing"
                    or paper.current_revision is not None
                    or not self._metadata_matches(paper, intent.metadata)
                ):
                    raise DecisionConflict("Submission acceptance reservation conflicts")
                paper.reservation_expires_at = self._clock() + _RESERVATION_TTL
                session.commit()
                return "reserved", paper.id, None
            key_owner = (
                session.query(SubmissionModel)
                .filter(SubmissionModel.decision_idempotency_key == decision_key)
                .with_for_update()
                .one_or_none()
            )
            if key_owner is not None and key_owner.id != intent.submission_id:
                raise DecisionConflict("decision idempotency key belongs to another Submission")
            alias = session.get(
                PaperFilenameAliasModel,
                normalize_alias_key(intent.metadata.filename),
            )
            filename_owner = (
                session.query(PaperMetadataModel)
                .filter(PaperMetadataModel.filename == intent.metadata.filename)
                .with_for_update()
                .one_or_none()
            )
            if alias is not None or filename_owner is not None:
                raise AliasConflict(intent.metadata.filename)
            raise PersistenceFailed("acceptance reservation conflicted")

    def _reserve_acceptance(
        self,
        intent: AcceptSubmission,
        pending_filename: str,
        decision_key: str,
        payload_hash: str,
    ):
        reservation = PaperMetadataModel(
            id=self._uuid_factory(),
            **asdict(intent.metadata),
            lifecycle_state="publishing",
            current_revision=None,
            row_version=0,
            index_status="pending",
            indexed_revision=None,
            index_error=None,
            origin_submission_id=intent.submission_id,
            reservation_expires_at=self._clock() + _RESERVATION_TTL,
        )
        reservation_id = str(reservation.id)
        try:
            with self._session() as session:
                submission = self._submission_by_id(
                    session, intent.submission_id, locked=True,
                )
                if submission is None:
                    raise NotFound("Submission not found")
                existing_decision = self._reconstruct_decision_locked(
                    session,
                    submission,
                    decision="accepted",
                    decision_key=decision_key,
                    payload_hash=payload_hash,
                )
                if existing_decision is not None:
                    return "accepted", existing_decision.paper_id, existing_decision
                if submission.status != "pending":
                    raise SubmissionNotPending("Submission is not pending")
                if submission.pending_filename != pending_filename:
                    raise DecisionConflict("Submission source changed during review")
                existing_paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.origin_submission_id == intent.submission_id)
                    .with_for_update()
                    .one_or_none()
                )
                if existing_paper is not None:
                    if (
                        existing_paper.lifecycle_state != "publishing"
                        or existing_paper.current_revision is not None
                        or not self._metadata_matches(existing_paper, intent.metadata)
                    ):
                        raise DecisionConflict("Submission acceptance reservation conflicts")
                    existing_paper.reservation_expires_at = (
                        self._clock() + _RESERVATION_TTL
                    )
                    session.commit()
                    return "reserved", existing_paper.id, None
                key_owner = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.decision_idempotency_key == decision_key)
                    .with_for_update()
                    .one_or_none()
                )
                if key_owner is not None and key_owner.id != intent.submission_id:
                    raise DecisionConflict(
                        "decision idempotency key belongs to another Submission"
                    )
                alias = session.get(
                    PaperFilenameAliasModel,
                    normalize_alias_key(intent.metadata.filename),
                )
                filename_owner = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.filename == intent.metadata.filename)
                    .with_for_update()
                    .one_or_none()
                )
                if alias is not None or filename_owner is not None:
                    raise AliasConflict(intent.metadata.filename)
                session.add(reservation)
                session.commit()
                return "reserved", reservation_id, None
        except IntegrityError:
            return self._acceptance_reservation_conflict(
                intent,
                pending_filename,
                decision_key,
                payload_hash,
            )
        except (
            AliasConflict,
            DecisionConflict,
            NotFound,
            PersistenceFailed,
            SubmissionNotPending,
        ):
            raise
        except Exception as exc:
            raise PersistenceFailed("could not reserve Submission acceptance") from exc

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
                    .with_for_update()
                    .one_or_none()
                )
                filename_owner_id = (
                    filename_owner.id if filename_owner is not None else None
                )
                if filename_owner is not None and self._reconcile_locked_expired(
                    session,
                    filename_owner,
                ):
                    return "expired", filename_owner_id, None
                if alias is not None or filename_owner is not None:
                    raise AliasConflict(intent.metadata.filename)
                raise PersistenceFailed("publication reservation conflicted")
            paper_id = paper.id
            if self._reconcile_locked_expired(session, paper):
                return "expired", paper_id, None
            if paper.direct_payload_hash != payload_hash:
                raise IdempotencyConflict(intent.idempotency_key)
            if paper.lifecycle_state == "published":
                outcome = self._indexing_outcome(session, paper)
                return "published", paper.id, outcome
            if paper.lifecycle_state != "publishing" or paper.current_revision is not None:
                raise PersistenceFailed("publication reservation has an invalid state")
            return "reserved", paper.id, None

    def _reconcile_locked_expired(self, session, paper) -> bool:
        """Remove an expired hidden reservation only after storage is clean."""
        if not self._remove_expired_reservation_locked(session, paper):
            return False
        session.commit()
        return True

    def _remove_expired_reservation_locked(self, session, paper) -> bool:
        """Stage removal of one expired hidden reservation, storage first."""
        if (
            paper.lifecycle_state != "publishing"
            or paper.current_revision is not None
            or (
                paper.reservation_expires_at is not None
                and paper.reservation_expires_at > self._clock()
            )
        ):
            return False
        referenced = (
            session.query(PaperRevisionModel)
            .filter(PaperRevisionModel.paper_id == paper.id)
            .first()
        )
        if referenced is not None:
            raise PersistenceFailed("expired reservation owns a persisted revision")
        try:
            self._storage.delete_paper(paper.id, ())
        except StorageError as exc:
            raise StorageFailed("expired publication cleanup failed") from exc
        session.delete(paper)
        return True

    def _reserve(self, intent: DirectPublish, payload_hash: str):
        """Reserve by insertion; unique constraints serialize key/filename races."""
        while True:
            reservation = self._new_reservation(intent, payload_hash)
            reservation_id = str(reservation.id)
            try:
                with self._session() as session:
                    session.add(reservation)
                    session.commit()
                return "reserved", reservation_id, None
            except IntegrityError:
                state, paper_id, outcome = self._reservation_conflict(intent, payload_hash)
                if state == "expired":
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
        intent: DirectPublish | AcceptSubmission,
        paper_id: str,
        payload_hash: str,
        stored,
        indexing_enabled: bool,
        submission_decision: tuple[str, str, str] | None = None,
    ) -> JobLease | _SubmissionDecision | None:
        with self._session() as session:
            submission = None
            if submission_decision is not None:
                decision_key, decision_hash, comment = submission_decision
                submission = self._submission_by_id(
                    session, intent.submission_id, locked=True,
                )
                if submission is None:
                    raise NotFound("Submission not found")
                existing = self._reconstruct_decision_locked(
                    session,
                    submission,
                    decision="accepted",
                    decision_key=decision_key,
                    payload_hash=decision_hash,
                )
                if existing is not None:
                    return existing
                if submission.status != "pending":
                    raise SubmissionNotPending("Submission is not pending")
            paper = (
                session.query(PaperMetadataModel)
                .filter(PaperMetadataModel.id == paper_id)
                .with_for_update()
                .one_or_none()
            )
            if paper is None:
                raise PersistenceFailed("publication reservation disappeared")
            if submission is None:
                if paper.direct_payload_hash != payload_hash:
                    raise IdempotencyConflict(intent.idempotency_key)
            elif (
                paper.origin_submission_id != submission.id
                or not self._metadata_matches(paper, intent.metadata)
            ):
                raise DecisionConflict("Submission acceptance reservation changed")
            if paper.lifecycle_state == "published":
                return None
            if paper.lifecycle_state != "publishing" or paper.current_revision is not None:
                raise PersistenceFailed("publication reservation changed")
            if self._reconcile_locked_expired(session, paper):
                raise PersistenceFailed("publication reservation expired")

            lookup_key = normalize_alias_key(intent.metadata.filename)
            alias = (
                session.query(PaperFilenameAliasModel)
                .filter(PaperFilenameAliasModel.lookup_key == lookup_key)
                .with_for_update()
                .one_or_none()
            )
            if alias is not None and alias.paper_id != paper_id:
                raise AliasConflict(intent.metadata.filename)

            self._storage.verify_revision(
                paper_id,
                1,
                sha256=stored.sha256,
                size_bytes=stored.size_bytes,
            )

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
            if submission is not None:
                submission.status = "accepted"
                submission.paper_id = paper_id
                submission.reviewer = intent.actor.user_id
                submission.reviewed_at = now
                submission.comment = comment
                submission.decision_idempotency_key = decision_key
                submission.decision_payload_hash = decision_hash
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

    def _accepted_row(
        self,
        submission_id: str,
        decision_key: str,
        payload_hash: str,
    ) -> _SubmissionDecision | None:
        with self._session() as session:
            submission = self._submission_by_id(session, submission_id)
            if submission is None or submission.status != "accepted":
                return None
            return self._reconstruct_decision_locked(
                session,
                submission,
                decision="accepted",
                decision_key=decision_key,
                payload_hash=payload_hash,
            )

    def _rejected_row(
        self,
        submission_id: str,
        decision_key: str,
        payload_hash: str,
    ) -> _SubmissionDecision | None:
        with self._session() as session:
            submission = self._submission_by_id(session, submission_id)
            if submission is None or submission.status != "rejected":
                return None
            return self._reconstruct_decision_locked(
                session,
                submission,
                decision="rejected",
                decision_key=decision_key,
                payload_hash=payload_hash,
            )

    def _decision_key_owned_by_other(
        self,
        decision_key: str,
        submission_id: str,
    ) -> bool:
        with self._session() as session:
            owner = (
                session.query(SubmissionModel)
                .filter(SubmissionModel.decision_idempotency_key == decision_key)
                .one_or_none()
            )
            return owner is not None and owner.id != submission_id

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
            isinstance(lease, JobLease)
            and job is not None
            and paper is not None
            and job.id == lease.job_id
            and job.kind == lease.kind == "index_revision"
            and job.paper_id == paper.id == lease.paper_id
            and job.revision_number == lease.revision
            and job.state == "running"
            and job.attempts == lease.attempts
            and job.lease_token == lease.lease_token
            and job.lease_expires_at is not None
            and paper.lifecycle_state == "published"
            and paper.current_revision == lease.revision
        )

    def _complete_index(
        self,
        lease: JobLease,
        prepared: PreparedRevisionIndex,
        *,
        expected_sha256: str | None = None,
        expected_size_bytes: int | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> bool:
        if (
            prepared.paper_id != lease.paper_id
            or prepared.revision != lease.revision
        ):
            return False
        try:
            with self._session() as session:
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == lease.paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                job = (
                    session.query(PublishingJobModel)
                    .filter(PublishingJobModel.id == lease.job_id)
                    .with_for_update()
                    .one_or_none()
                )
                revision_row = (
                    session.query(PaperRevisionModel)
                    .filter(
                        PaperRevisionModel.paper_id == lease.paper_id,
                        PaperRevisionModel.revision_number == lease.revision,
                    )
                    .with_for_update()
                    .one_or_none()
                )
                exact_job = bool(
                    isinstance(lease, JobLease)
                    and job is not None
                    and job.id == lease.job_id
                    and job.kind == lease.kind == "index_revision"
                    and job.paper_id == lease.paper_id
                    and job.revision_number == lease.revision
                    and job.state == "running"
                    and job.attempts == lease.attempts
                    and job.lease_token == lease.lease_token
                    and job.lease_expires_at is not None
                )
                current_time = (clock or self._clock)()
                if not exact_job or job.lease_expires_at <= current_time:
                    return False
                if (
                    paper is None
                    or paper.id != lease.paper_id
                    or paper.lifecycle_state != "published"
                    or paper.current_revision != lease.revision
                    or revision_row is None
                    or (
                        expected_sha256 is not None
                        and revision_row.sha256 != expected_sha256
                    )
                    or (
                        expected_size_bytes is not None
                        and revision_row.size_bytes != expected_size_bytes
                    )
                ):
                    session.delete(job)
                    session.commit()
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
        from services.publishing_jobs import redact_job_error

        return redact_job_error(error)

    def _mark_index_failure(
        self,
        lease: JobLease,
        error: Exception,
        *,
        clock: Callable[[], datetime] | None = None,
        jitter: Callable[[], float] | None = None,
    ) -> IndexingOutcome:
        from services.publishing_jobs import release_failed_job

        try:
            progress = release_failed_job(
                self._session_factory,
                lease,
                error,
                (clock or self._clock)(),
                jitter=float((jitter or self._jitter)()),
            )
            if progress is not None and progress.state == JobState.PENDING:
                return IndexingOutcome(
                    IndexingState.FAILED,
                    job_id=progress.job_id,
                    next_retry_at=progress.next_retry_at,
                )
            with self._session() as session:
                paper = session.get(PaperMetadataModel, lease.paper_id)
                if paper is None:
                    return IndexingOutcome(IndexingState.PENDING)
                return self._indexing_outcome(session, paper)
        except Exception:
            # The durable running lease remains recoverable if releasing it fails.
            return IndexingOutcome(IndexingState.PENDING, job_id=lease.job_id)

    @staticmethod
    def _lease_progress(lease: JobLease) -> JobProgress:
        return JobProgress(
            job_id=lease.job_id,
            paper_id=lease.paper_id,
            revision=lease.revision,
            state=JobState.RUNNING,
            attempts=lease.attempts,
        )

    def _current_job_progress(self, lease: JobLease) -> JobProgress:
        with self._session() as session:
            job = session.get(PublishingJobModel, lease.job_id)
            if job is None:
                return self._lease_progress(lease)
            return JobProgress(
                job_id=job.id,
                paper_id=job.paper_id,
                revision=job.revision_number,
                state=JobState(job.state),
                attempts=job.attempts,
                next_retry_at=(job.available_at if job.state == "pending" else None),
            )

    def _recover_claimed(
        self,
        lease: JobLease,
        monotonic_deadline: float,
        *,
        clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        jitter: Callable[[], float] | None = None,
    ) -> JobProgress:
        """Dispatch one committed lease through the same request/worker seam."""
        clock = clock or self._clock
        monotonic_clock = monotonic_clock or self._monotonic_clock
        jitter = jitter or self._jitter
        if lease.kind == "delete_paper":
            self._run_delete_job(lease, clock=clock, jitter=jitter)
            return self._current_job_progress(lease)
        if lease.kind != "index_revision":
            from services.publishing_jobs import release_failed_job

            try:
                progress = release_failed_job(
                    self._session_factory,
                    lease,
                    ValueError(f"unknown publishing job kind: {lease.kind}"),
                    clock(),
                    jitter=float(jitter()),
                )
                return progress or self._current_job_progress(lease)
            except Exception:
                return self._current_job_progress(lease)

        try:
            if monotonic_clock() >= monotonic_deadline:
                raise IndexDeadlineExceeded()
            with self._session() as session:
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == lease.paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                job = (
                    session.query(PublishingJobModel)
                    .filter(PublishingJobModel.id == lease.job_id)
                    .with_for_update()
                    .one_or_none()
                )
                revision_row = (
                    session.query(PaperRevisionModel)
                    .filter(
                        PaperRevisionModel.paper_id == lease.paper_id,
                        PaperRevisionModel.revision_number == lease.revision,
                    )
                    .with_for_update()
                    .one_or_none()
                )
                exact_job = bool(
                    job is not None
                    and job.id == lease.job_id
                    and job.kind == lease.kind == "index_revision"
                    and job.paper_id == lease.paper_id
                    and job.revision_number == lease.revision
                    and job.state == "running"
                    and job.attempts == lease.attempts
                    and job.lease_token == lease.lease_token
                    and job.lease_expires_at is not None
                )
                if not exact_job or job.lease_expires_at <= clock():
                    if job is None:
                        return self._lease_progress(lease)
                    return JobProgress(
                        job_id=job.id,
                        paper_id=job.paper_id,
                        revision=job.revision_number,
                        state=JobState(job.state),
                        attempts=job.attempts,
                        next_retry_at=(
                            job.available_at if job.state == "pending" else None
                        ),
                    )
                if (
                    paper is None
                    or paper.lifecycle_state != "published"
                    or paper.current_revision != lease.revision
                    or revision_row is None
                ):
                    session.delete(job)
                    session.commit()
                    return self._lease_progress(lease)
                language = paper.language or ""
                revision_sha256 = revision_row.sha256
                revision_size_bytes = revision_row.size_bytes

            verified = self._storage.verify_revision(
                lease.paper_id,
                lease.revision,
                sha256=revision_sha256,
                size_bytes=revision_size_bytes,
            )
            pdf_bytes = verified.path.read_bytes()
            prepared = self._indexer.prepare(
                paper_id=lease.paper_id,
                revision_number=lease.revision,
                pdf_bytes=pdf_bytes,
                language=language,
                deadline=monotonic_deadline,
            )
            if (
                not isinstance(prepared, PreparedRevisionIndex)
                or prepared.paper_id != lease.paper_id
                or prepared.revision != lease.revision
            ):
                raise ValueError("indexer prepared the wrong Paper revision")
            if monotonic_clock() >= monotonic_deadline:
                raise IndexDeadlineExceeded()
            self._complete_index(
                lease,
                prepared,
                expected_sha256=revision_sha256,
                expected_size_bytes=revision_size_bytes,
                clock=clock,
            )
            return self._current_job_progress(lease)
        except Exception as exc:
            self._mark_index_failure(
                lease,
                exc,
                clock=clock,
                jitter=jitter,
            )
            return self._current_job_progress(lease)

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
        self._recover_claimed(lease, deadline)
        with self._session() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            if paper is None:
                return IndexingOutcome(IndexingState.PENDING, job_id=lease.job_id)
            return self._indexing_outcome(session, paper)

    def publish_direct(self, intent: DirectPublish) -> Published:
        """Publish a validated PDF; RAG failure never rolls back visibility."""
        self._validate_intent(intent)
        deadline = self._monotonic_clock() + self._inline_index_timeout_seconds
        operation_id = self._uuid_factory()
        try:
            staged = self._storage.stage(intent.pdf, operation_id)
        except StorageError as exc:
            raise StorageFailed("PDF could not be staged") from exc
        active_stage = [staged]
        try:
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
                active_stage[0] = prepared_pdf
                stored = self._storage.promote(prepared_pdf, paper_id, 1)
                self._storage.discard_stage(prepared_pdf)
                active_stage[0] = None
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
                    if isinstance(exc, StorageError):
                        raise StorageFailed("published PDF verification failed") from exc
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
        finally:
            if active_stage[0] is not None:
                try:
                    self._storage.discard_stage(active_stage[0])
                except StorageError as exc:
                    raise StorageFailed("staged PDF cleanup failed") from exc

    def direct_publish(self, intent: DirectPublish) -> Published:
        """Task 1 protocol-compatible spelling."""
        return self.publish_direct(intent)

    def _cleanup_accepted_pending(
        self,
        submission_id: str,
        pending_filename: str,
        *,
        raise_on_error: bool = False,
    ) -> bool:
        """Clean accepted bytes only while the accepted row remains locked."""
        try:
            with self._session() as session:
                lock_submission_creation_fence(session)
                submission = self._submission_by_id(
                    session, submission_id, locked=True,
                )
                if (
                    submission is None
                    or submission.id != submission_id
                    or submission.status != "accepted"
                    or submission.pending_filename != pending_filename
                ):
                    return False
                current = self._storage.submission_trash_record(submission_id)
                had_cleanup_work = (
                    current is not None
                    or self._storage.legacy_submission_trash_entry_present(
                        submission_id
                    )
                )
                if not had_cleanup_work:
                    had_cleanup_work = self._storage.pending_exists(
                        pending_filename
                    )
                token = self._prepare_submission_trash_for_discard(
                    submission_id,
                    pending_filename,
                )
                if token is not None:
                    self._storage.discard_pending_trash(token)
                session.commit()
            return had_cleanup_work
        except Exception:
            if raise_on_error:
                raise
            return False

    def _submission_trash_authority(
        self,
        submission_id: str,
        pending_filename: str,
    ) -> tuple[SubmissionTrashRecord | None, PendingTrash | None]:
        current = self._storage.submission_trash_record(submission_id)
        if current is not None:
            if self._storage.legacy_submission_trash_exists(
                pending_filename,
                submission_id,
            ):
                raise StorageError(
                    "Submission trash has dual current and legacy authority"
                )
            if current.original_name != pending_filename:
                raise StorageError("Submission trash conflicts with SQL provenance")
            token = self._storage.rehydrate_submission_trash(
                submission_id,
                pending_filename,
            )
            return current, token
        legacy = self._storage.resolve_legacy_submission_trash(
            pending_filename,
            submission_id,
        )
        return None, legacy

    def _prepare_submission_trash_for_discard(
        self,
        submission_id: str,
        pending_filename: str,
    ) -> PendingTrash | None:
        current, token = self._submission_trash_authority(
            submission_id,
            pending_filename,
        )
        if token is not None:
            return self._storage.commit_pending_trash(token)
        if current is not None:
            self._storage.discard_empty_submission_trash(current)
        if self._storage.pending_exists(pending_filename):
            return self._storage.trash_submission_pending(
                pending_filename,
                submission_id,
            )
        return None

    def _review_acceptance(self, intent: AcceptSubmission) -> _SubmissionDecision:
        self._validate_acceptance(intent)
        comment = ""
        payload_hash = self._decision_payload_hash(
            intent.submission_id,
            "accepted",
            intent.actor.user_id,
            comment,
        )
        pending_filename, existing = self._acceptance_preflight(
            intent,
            intent.idempotency_key,
            payload_hash,
        )
        if existing is not None:
            self._cleanup_accepted_pending(intent.submission_id, pending_filename)
            return existing

        deadline = self._monotonic_clock() + self._inline_index_timeout_seconds
        operation_id = self._uuid_factory()
        try:
            staged = self._storage.stage_pending(pending_filename, operation_id)
        except StorageError as exc:
            recovered = self._accepted_row(
                intent.submission_id,
                intent.idempotency_key,
                payload_hash,
            )
            if recovered is not None:
                self._cleanup_accepted_pending(
                    intent.submission_id,
                    pending_filename,
                )
                return recovered
            raise StorageFailed("pending Submission PDF could not be staged") from exc
        active_stage = [staged]
        try:
            state, paper_id, accepted = self._reserve_acceptance(
                intent,
                pending_filename,
                intent.idempotency_key,
                payload_hash,
            )
            if state == "accepted":
                self._cleanup_accepted_pending(intent.submission_id, pending_filename)
                return accepted

            try:
                prepared_pdf = self._storage.apply_metadata(
                    staged,
                    title=intent.metadata.title,
                    author=intent.metadata.author_name,
                )
                active_stage[0] = prepared_pdf
                stored = self._storage.promote(prepared_pdf, paper_id, 1)
                self._storage.discard_stage(prepared_pdf)
                active_stage[0] = None
            except StorageError as exc:
                self._remove_unreferenced_paper_files(paper_id)
                raise StorageFailed("pending Submission PDF could not be published") from exc

            enabled_error = None
            try:
                indexing_enabled = bool(self._indexer.enabled())
            except Exception as exc:
                indexing_enabled = True
                enabled_error = exc
            try:
                lease = self._make_visible(
                    intent=intent,
                    paper_id=paper_id,
                    payload_hash=payload_hash,
                    stored=stored,
                    indexing_enabled=indexing_enabled,
                    submission_decision=(
                        intent.idempotency_key,
                        payload_hash,
                        comment,
                    ),
                )
            except AliasConflict:
                self._abandon_hidden_reservation(paper_id)
                raise
            except (DecisionConflict, SubmissionNotPending, NotFound):
                self._remove_unreferenced_paper_files(paper_id)
                raise
            except Exception as exc:
                if isinstance(exc, IntegrityError):
                    if self._decision_key_owned_by_other(
                        intent.idempotency_key,
                        intent.submission_id,
                    ):
                        self._abandon_hidden_reservation(paper_id)
                        raise DecisionConflict(
                            "decision idempotency key belongs to another Submission"
                        ) from exc
                    if self._alias_owned_by_other(intent.metadata.filename, paper_id):
                        self._abandon_hidden_reservation(paper_id)
                        raise AliasConflict(intent.metadata.filename) from exc
                recovered = self._accepted_row(
                    intent.submission_id,
                    intent.idempotency_key,
                    payload_hash,
                )
                if recovered is None:
                    self._remove_unreferenced_paper_files(paper_id)
                    if isinstance(exc, PersistenceFailed):
                        raise
                    if isinstance(exc, StorageError):
                        raise StorageFailed("published PDF verification failed") from exc
                    raise PersistenceFailed(
                        "could not make Submission acceptance visible"
                    ) from exc
                self._cleanup_accepted_pending(intent.submission_id, pending_filename)
                return recovered

            if isinstance(lease, _SubmissionDecision):
                self._cleanup_accepted_pending(
                    intent.submission_id,
                    pending_filename,
                )
                return lease
            self._cleanup_accepted_pending(intent.submission_id, pending_filename)
            if not indexing_enabled:
                indexing = IndexingOutcome(IndexingState.NOT_REQUIRED)
            elif lease is None:
                recovered = self._accepted_row(
                    intent.submission_id,
                    intent.idempotency_key,
                    payload_hash,
                )
                indexing = recovered.indexing
            elif enabled_error is not None:
                indexing = self._mark_index_failure(lease, enabled_error)
            else:
                indexing = self._run_inline_index(paper_id, lease, deadline)
            return _SubmissionDecision(
                submission_id=intent.submission_id,
                accepted=True,
                paper_id=paper_id,
                replayed=False,
                indexing=indexing,
            )
        finally:
            if active_stage[0] is not None:
                try:
                    self._storage.discard_stage(active_stage[0])
                except StorageError as exc:
                    raise StorageFailed("staged PDF cleanup failed") from exc

    def _review_rejection(self, intent: RejectSubmission) -> _SubmissionDecision:
        self._validate_rejection(intent)
        comment = intent.feedback
        decision_key = self._rejection_key(intent.submission_id, intent.actor.user_id)
        payload_hash = self._decision_payload_hash(
            intent.submission_id,
            "rejected",
            intent.actor.user_id,
            comment,
        )
        try:
            with self._session() as session:
                submission = self._submission_by_id(
                    session, intent.submission_id, locked=True,
                )
                if submission is None:
                    raise NotFound("Submission not found")
                existing = self._reconstruct_decision_locked(
                    session,
                    submission,
                    decision="rejected",
                    decision_key=decision_key,
                    payload_hash=payload_hash,
                )
                if existing is not None:
                    return existing
                if submission.status != "pending":
                    raise SubmissionNotPending("Submission is not pending")
                reservation = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.origin_submission_id == submission.id)
                    .with_for_update()
                    .one_or_none()
                )
                if reservation is not None:
                    if not self._remove_expired_reservation_locked(
                        session,
                        reservation,
                    ):
                        raise DecisionConflict(
                            "Submission acceptance is already reserved"
                        )
                key_owner = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.decision_idempotency_key == decision_key)
                    .with_for_update()
                    .one_or_none()
                )
                if key_owner is not None and key_owner.id != submission.id:
                    raise DecisionConflict(
                        "decision idempotency key belongs to another Submission"
                    )
                submission.status = "rejected"
                submission.paper_id = None
                submission.reviewer = intent.actor.user_id
                submission.reviewed_at = self._clock()
                submission.comment = comment
                submission.decision_idempotency_key = decision_key
                submission.decision_payload_hash = payload_hash
                session.commit()
                return _SubmissionDecision(
                    submission_id=submission.id,
                    accepted=False,
                    paper_id=None,
                    replayed=False,
                    indexing=None,
                )
        except (
            DecisionConflict,
            NotFound,
            PersistenceFailed,
            SubmissionNotPending,
        ):
            raise
        except IntegrityError as exc:
            raise DecisionConflict("decision idempotency key conflicts") from exc
        except Exception as exc:
            recovered = self._rejected_row(
                intent.submission_id,
                decision_key,
                payload_hash,
            )
            if recovered is not None:
                return _SubmissionDecision(
                    submission_id=recovered.submission_id,
                    accepted=False,
                    paper_id=None,
                    replayed=False,
                    indexing=None,
                )
            raise PersistenceFailed("could not reject Submission") from exc

    def review_submission(
        self,
        intent: AcceptSubmission | RejectSubmission,
    ) -> _SubmissionDecision:
        if isinstance(intent, AcceptSubmission):
            return self._review_acceptance(intent)
        if isinstance(intent, RejectSubmission):
            return self._review_rejection(intent)
        raise InvalidInput({"intent": "must be an acceptance or rejection record"})

    def accept_submission(self, intent: AcceptSubmission) -> _SubmissionDecision:
        return self.review_submission(intent)

    def reject_submission(self, intent: RejectSubmission) -> _SubmissionDecision:
        return self.review_submission(intent)

    def _finish_cancellation(
        self,
        submission_id: str,
        *,
        expected_owner: str | None,
    ) -> SubmissionCancelled:
        token = None
        pending_filename = ""
        try:
            with self._session() as session:
                lock_submission_creation_fence(session)
                submission = self._submission_by_id(
                    session, submission_id, locked=True,
                )
                if submission is None:
                    return SubmissionCancelled(submission_id=submission_id)
                if expected_owner is not None and submission.submitted_by != expected_owner:
                    raise Forbidden("only the submitting Reader may cancel")
                if submission.status != "cancelling":
                    raise SubmissionNotPending("Submission is not cancelling")
                pending_filename = submission.pending_filename
                token = self._prepare_submission_trash_for_discard(
                    submission_id,
                    pending_filename,
                )
                reservation = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.origin_submission_id == submission_id)
                    .with_for_update()
                    .one_or_none()
                )
                if reservation is not None:
                    raise SubmissionNotPending(
                        "Submission acceptance reservation prevents cancellation"
                    )
                session.delete(submission)
                session.commit()
                committed = True
        except StorageError as exc:
            self._restore_cancellation_after_failure(submission_id, expected_owner)
            raise StorageFailed("pending Submission PDF could not be trashed") from exc
        except (Forbidden, SubmissionNotPending):
            self._restore_cancellation_after_failure(submission_id, expected_owner)
            raise
        except Exception as exc:
            with self._session() as session:
                lock_submission_creation_fence(session)
                surviving = self._submission_by_id(
                    session, submission_id, locked=True,
                )
                if surviving is None:
                    committed = True
                else:
                    try:
                        self._restore_submission_bytes_locked(surviving)
                    except StorageError as restore_exc:
                        raise StorageFailed(
                            "cancellation transaction failed and PDF restore failed"
                        ) from restore_exc
                    session.commit()
                    raise PersistenceFailed(
                        "could not delete cancelling Submission"
                    ) from exc

        if committed and token is not None:
            try:
                with self._session() as session:
                    lock_submission_creation_fence(session)
                    surviving = self._submission_by_id(
                        session, submission_id, locked=True,
                    )
                    if surviving is not None:
                        raise StorageError(
                            "Submission row reappeared before trash discard"
                        )
                    current = self._storage.submission_trash_record(submission_id)
                    legacy = None
                    if current is not None:
                        if self._storage.legacy_submission_trash_exists(
                            pending_filename,
                            submission_id,
                        ):
                            raise StorageError(
                                "Submission trash has dual current and legacy authority"
                            )
                    else:
                        legacy = self._storage.resolve_legacy_submission_trash(
                            pending_filename,
                            submission_id,
                        )
                    if token.namespace == "submission-v2":
                        if current is None or legacy is not None:
                            raise StorageError(
                                "current Submission trash authority changed"
                            )
                    elif (
                        token.namespace != "legacy"
                        or current is not None
                        or legacy is None
                        or (legacy.device, legacy.inode)
                        != (token.device, token.inode)
                    ):
                        raise StorageError(
                            "legacy Submission trash authority changed"
                        )
                    self._storage.discard_pending_trash(token)
                    session.commit()
            except Exception:
                pass
        return SubmissionCancelled(submission_id=submission_id)

    def _restore_submission_bytes_locked(self, submission) -> bool:
        """Restore only after the caller has transactionally locked the row."""
        current, token = self._submission_trash_authority(
            submission.id,
            submission.pending_filename,
        )
        if token is not None:
            self._storage.restore_pending(token)
            restored = True
        else:
            if current is not None:
                self._storage.discard_empty_submission_trash(current)
            restored = self._storage.pending_exists(submission.pending_filename)
        if restored and submission.status == "cancelling":
            submission.status = "pending"
            submission.reviewed_at = None
        return restored

    def _restore_cancellation_after_failure(
        self,
        submission_id: str,
        expected_owner: str | None,
    ) -> None:
        try:
            with self._session() as session:
                lock_submission_creation_fence(session)
                submission = self._submission_by_id(
                    session, submission_id, locked=True,
                )
                if submission is None or (
                    expected_owner is not None
                    and submission.submitted_by != expected_owner
                ):
                    return
                if self._restore_submission_bytes_locked(submission):
                    session.commit()
        except Exception:
            pass

    def cancel_submission(self, intent: CancelSubmission) -> SubmissionCancelled:
        self._validate_cancellation(intent)
        try:
            with self._session() as session:
                lock_submission_creation_fence(session)
                submission = self._submission_by_id(
                    session, intent.submission_id, locked=True,
                )
                if submission is None:
                    raise NotFound("Submission not found")
                if submission.submitted_by != intent.actor.user_id:
                    raise Forbidden("only the submitting Reader may cancel")
                if submission.status not in {"pending", "cancelling"}:
                    raise SubmissionNotPending("Submission is not pending")
                if submission.status == "pending":
                    try:
                        self._storage.pending_exists(submission.pending_filename)
                    except StorageError as exc:
                        raise StorageFailed(
                            "pending Submission PDF could not be audited"
                        ) from exc
                    reservation = (
                        session.query(PaperMetadataModel)
                        .filter(
                            PaperMetadataModel.origin_submission_id
                            == intent.submission_id
                        )
                        .with_for_update()
                        .one_or_none()
                    )
                    if reservation is not None:
                        if not self._remove_expired_reservation_locked(
                            session,
                            reservation,
                        ):
                            raise SubmissionNotPending(
                                "Submission acceptance reservation prevents cancellation"
                            )
                    submission.status = "cancelling"
                    submission.reviewed_at = self._clock()
                    session.commit()
        except (
            Forbidden,
            NotFound,
            PersistenceFailed,
            StorageFailed,
            SubmissionNotPending,
        ):
            raise
        except Exception as exc:
            raise PersistenceFailed("could not begin Submission cancellation") from exc
        return self._finish_cancellation(
            intent.submission_id,
            expected_owner=intent.actor.user_id,
        )

    def reconcile_submissions(
        self,
        *,
        on_error: Callable[[str, BaseException], None] | None = None,
    ) -> int:
        """Finish stale cancellation and deterministic pending-trash recovery."""
        cutoff = self._clock() - _RESERVATION_TTL
        try:
            with self._session() as session:
                cancelling_ids = [
                    row.id
                    for row in session.query(SubmissionModel)
                    .filter(
                        SubmissionModel.status == "cancelling",
                        SubmissionModel.reviewed_at <= cutoff,
                    )
                    .order_by(SubmissionModel.id)
                    .all()
                ]
                accepted = (
                    session.query(
                        SubmissionModel.id,
                        SubmissionModel.pending_filename,
                        SubmissionModel.paper_id,
                        SubmissionModel.decision_idempotency_key,
                        SubmissionModel.decision_payload_hash,
                        PaperMetadataModel.lifecycle_state,
                        PaperMetadataModel.current_revision,
                        PaperRevisionModel.sha256,
                        PaperRevisionModel.size_bytes,
                    )
                    .outerjoin(
                        PaperMetadataModel,
                        PaperMetadataModel.id == SubmissionModel.paper_id,
                    )
                    .outerjoin(
                        PaperRevisionModel,
                        (
                            PaperRevisionModel.paper_id == PaperMetadataModel.id
                        )
                        & (
                            PaperRevisionModel.revision_number
                            == PaperMetadataModel.current_revision
                        ),
                    )
                    .filter(
                        SubmissionModel.status == "accepted",
                        SubmissionModel.reviewed_at <= cutoff,
                    )
                    .order_by(SubmissionModel.id)
                    .all()
                )
                pending_origin_ids = [
                    row.id
                    for row in session.query(SubmissionModel)
                    .join(
                        PaperMetadataModel,
                        PaperMetadataModel.origin_submission_id
                        == SubmissionModel.id,
                    )
                    .filter(
                        SubmissionModel.status == "pending",
                        PaperMetadataModel.lifecycle_state == "publishing",
                        PaperMetadataModel.current_revision.is_(None),
                    )
                    .order_by(SubmissionModel.id)
                    .all()
                ]
            reconciled = 0
            for submission_id in pending_origin_ids:
                try:
                    with self._session() as session:
                        submission = self._submission_by_id(
                            session, submission_id, locked=True,
                        )
                        if submission is None or submission.status != "pending":
                            continue
                        reservation = (
                            session.query(PaperMetadataModel)
                            .filter(
                                PaperMetadataModel.origin_submission_id
                                == submission_id
                            )
                            .with_for_update()
                            .one_or_none()
                        )
                        if (
                            reservation is None
                            or not self._remove_expired_reservation_locked(
                                session,
                                reservation,
                            )
                        ):
                            continue
                        submission.paper_id = None
                        submission.reviewed_at = None
                        submission.reviewer = None
                        submission.comment = None
                        submission.decision_idempotency_key = None
                        submission.decision_payload_hash = None
                        session.commit()
                        reconciled += 1
                except Exception as exc:
                    if on_error is None:
                        raise
                    on_error(f"submission:{submission_id}", exc)
            for submission_id in cancelling_ids:
                try:
                    self._finish_cancellation(submission_id, expected_owner=None)
                    reconciled += 1
                except Exception as exc:
                    if on_error is None:
                        raise
                    on_error(f"submission:{submission_id}", exc)
            for (
                submission_id,
                pending_filename,
                paper_id,
                decision_key,
                decision_hash,
                paper_state,
                current_revision,
                revision_sha256,
                revision_size_bytes,
            ) in accepted:
                try:
                    if paper_id is None:
                        if decision_key is None or decision_hash is None:
                            # Unresolved migrated acceptances have no lifecycle
                            # proof that a public durable copy ever existed.
                            continue
                    elif (
                        paper_state != "published"
                        or current_revision is None
                        or revision_sha256 is None
                        or revision_size_bytes is None
                    ):
                        continue
                    else:
                        self._storage.verify_revision(
                            paper_id,
                            current_revision,
                            sha256=revision_sha256,
                            size_bytes=revision_size_bytes,
                        )
                    if self._cleanup_accepted_pending(
                        submission_id,
                        pending_filename,
                        raise_on_error=on_error is not None,
                    ):
                        reconciled += 1
                except Exception as exc:
                    if on_error is None:
                        raise
                    on_error(f"submission:{submission_id}", exc)

            for record in self._storage.stale_submission_trash(cutoff):
                try:
                    with self._session() as session:
                        lock_submission_creation_fence(session)
                        submission = self._submission_by_id(
                            session, record.submission_id, locked=True,
                        )
                        fresh = self._storage.submission_trash_record(
                            record.submission_id
                        )
                        if fresh != record:
                            raise StorageError(
                                "Submission trash changed after inventory audit"
                            )
                        legacy_exists = (
                            self._storage.legacy_submission_trash_exists(
                                submission.pending_filename
                                if submission is not None
                                else None,
                                record.submission_id,
                            )
                        )
                        if legacy_exists:
                            raise StorageError(
                                "Submission trash has dual current and legacy authority"
                            )
                        if submission is None:
                            token = self._storage.rehydrate_submission_trash(
                                record.submission_id
                            )
                            if token is not None:
                                token = self._storage.commit_pending_trash(token)
                                self._storage.discard_pending_trash(token)
                            else:
                                self._storage.discard_empty_submission_trash(record)
                            session.commit()
                            reconciled += 1
                            continue
                        if record.original_name != submission.pending_filename:
                            raise StorageError(
                                "Submission trash conflicts with SQL provenance"
                            )
                        if submission.status not in {
                            "pending",
                            "rejected",
                            "accepted",
                        }:
                            continue
                        token = self._storage.rehydrate_submission_trash(
                            submission.id,
                            submission.pending_filename,
                        )
                        if token is None:
                            self._storage.discard_empty_submission_trash(record)
                            session.commit()
                            reconciled += 1
                        elif submission.status in {"pending", "rejected"}:
                            self._storage.restore_pending(token)
                            session.commit()
                            reconciled += 1
                        elif submission.status == "accepted":
                            token = self._storage.commit_pending_trash(token)
                            self._storage.discard_pending_trash(token)
                            session.commit()
                            reconciled += 1
                        # Cancelling rows are completed through _finish_cancellation.
                except Exception as exc:
                    if on_error is None:
                        raise
                    on_error(f"submission-trash:{record.submission_id}", exc)

            for operation_id in self._storage.stale_pending_trash(cutoff):
                try:
                    with self._session() as session:
                        lock_submission_creation_fence(session)
                        submission = self._submission_by_id(
                            session, operation_id, locked=True,
                        )
                        if (
                            submission is not None
                            and submission.status
                            not in {"pending", "rejected", "accepted"}
                        ):
                            continue
                        current = self._storage.submission_trash_record(operation_id)
                        token = self._storage.resolve_legacy_submission_trash(
                            submission.pending_filename
                            if submission is not None
                            else None,
                            operation_id,
                        )
                        if current is not None and token is not None:
                            raise StorageError(
                                "Submission trash has dual current and legacy authority"
                            )
                        ambiguous = (
                            self._storage.is_ambiguous_legacy_submission_operation(
                                operation_id
                            )
                        )
                        if current is not None:
                            if ambiguous:
                                session.commit()
                                reconciled += 1
                            continue
                        if submission is None:
                            if token is not None:
                                self._storage.discard_pending_trash(token)
                                session.commit()
                                reconciled += 1
                            elif ambiguous:
                                session.commit()
                                reconciled += 1
                            continue
                        if token is None:
                            if ambiguous:
                                session.commit()
                                reconciled += 1
                            continue
                        if submission.status in {"pending", "rejected"}:
                            self._storage.restore_pending(token)
                            session.commit()
                            reconciled += 1
                        elif submission.status == "accepted":
                            token = self._storage.commit_pending_trash(token)
                            self._storage.discard_pending_trash(token)
                            session.commit()
                            reconciled += 1
                        # A still-cancelling row is retained for the next attempt;
                        # its deterministic trash is never generically removed.
                except Exception as exc:
                    if on_error is None:
                        raise
                    on_error(f"legacy-submission-trash:{operation_id}", exc)
            return reconciled
        except (StorageFailed, PersistenceFailed):
            raise
        except StorageError as exc:
            raise StorageFailed("Submission storage reconciliation failed") from exc
        except Exception as exc:
            raise PersistenceFailed("Submission reconciliation failed") from exc

    def reconcile_pending_cancellations(self) -> int:
        return self.reconcile_submissions()

    # Paper-change commands intentionally share one dispatch boundary.  The
    # thin HTTP adapters added later therefore cannot select a private storage
    # or transaction path based on which form field happened to be submitted.
    @staticmethod
    def _validate_change_actor(actor: Actor) -> None:
        if not isinstance(actor, Actor) or (
            isinstance(actor.role, bool)
            or not isinstance(actor.role, int)
            or actor.role not in {1, 2, 3}
        ):
            raise InvalidInput({"actor": "is invalid"})
        if (
            not isinstance(actor.user_id, str)
            or not actor.user_id
            or actor.user_id != actor.user_id.strip()
            or len(actor.user_id) > 255
        ):
            raise InvalidInput({"actor": "must identify a user"})
        if actor.role < 2:
            raise Forbidden("Paper changes require Contributor access")

    @staticmethod
    def _change_target(paper_id: str, expected_row_version: int) -> str:
        try:
            canonical = validate_paper_id(paper_id)
        except (AttributeError, TypeError, ValueError) as exc:
            raise InvalidInput({"paper_id": "must be a canonical UUID"}) from exc
        if (
            isinstance(expected_row_version, bool)
            or not isinstance(expected_row_version, int)
            or expected_row_version < 1
        ):
            raise InvalidInput({"expected_row_version": "must be a positive integer"})
        return canonical

    @staticmethod
    def _validate_metadata_patch(patch: MetadataPatch) -> str:
        if not isinstance(patch, MetadataPatch):
            raise InvalidInput({"patch": "must be a MetadataPatch"})
        paper_id = PublishingLifecycle._change_target(
            patch.paper_id,
            patch.expected_row_version,
        )
        if not patch.changes:
            raise InvalidInput({"changes": "must not be empty"})
        errors: dict[str, str] = {}
        for key, value in patch.changes:
            if value != value.strip():
                errors[key] = "must already be normalized"
                continue
            limit = _METADATA_STRING_LIMITS.get(key)
            if limit is not None and len(value) > limit:
                errors[key] = f"must be at most {limit} characters"
        fields = dict(patch.changes)
        if "filename" in fields:
            filename = fields["filename"]
            if (
                not filename
                or Path(filename).name != filename
                or "\\" in filename
                or not filename.casefold().endswith(".pdf")
            ):
                errors["filename"] = "must be a normalized PDF filename"
            elif len(normalize_alias_key(filename)) > 255:
                errors["filename"] = "normalized filename is too long"
        if "title" in fields and not fields["title"]:
            errors["title"] = "is required"
        if "language" in fields and not fields["language"]:
            errors["language"] = "is required"
        if errors:
            raise InvalidInput(errors)
        return paper_id

    @staticmethod
    def _locked_published_paper(session, paper_id: str, expected_row_version: int):
        paper = (
            session.query(PaperMetadataModel)
            .filter(PaperMetadataModel.id == paper_id)
            .with_for_update()
            .one_or_none()
        )
        if paper is None or paper.lifecycle_state != "published":
            raise NotFound("Paper not found")
        if paper.row_version != expected_row_version:
            raise StaleVersion(paper.row_version)
        return paper

    def _metadata_change(self, intent: EditMetadata) -> PaperChanged:
        if not isinstance(intent, EditMetadata):
            raise InvalidInput({"intent": "must be an EditMetadata record"})
        self._validate_change_actor(intent.actor)
        paper_id = self._validate_metadata_patch(intent.patch)
        filename = dict(intent.patch.changes).get("filename")
        try:
            with self._session() as session:
                paper = self._locked_published_paper(
                    session,
                    paper_id,
                    intent.patch.expected_row_version,
                )
                if filename is not None and filename != paper.filename:
                    lookup_key = normalize_alias_key(filename)
                    alias = (
                        session.query(PaperFilenameAliasModel)
                        .filter(PaperFilenameAliasModel.lookup_key == lookup_key)
                        .with_for_update()
                        .one_or_none()
                    )
                    if alias is not None and alias.paper_id != paper.id:
                        raise AliasConflict(filename)
                    filename_owner = (
                        session.query(PaperMetadataModel)
                        .filter(PaperMetadataModel.filename == filename)
                        .with_for_update()
                        .one_or_none()
                    )
                    if filename_owner is not None and filename_owner.id != paper.id:
                        raise AliasConflict(filename)
                    if alias is None:
                        session.add(
                            PaperFilenameAliasModel(
                                lookup_key=lookup_key,
                                filename=filename,
                                paper_id=paper.id,
                                created_at=self._clock(),
                            )
                        )
                for key, value in intent.patch.changes:
                    setattr(paper, key, value)
                paper.row_version += 1
                result = PaperChanged(
                    paper_id=paper.id,
                    filename=paper.filename,
                    revision=paper.current_revision,
                    row_version=paper.row_version,
                    indexing=IndexingOutcome(IndexingState.NOT_REQUIRED),
                )
                session.commit()
                return result
        except (AliasConflict, Forbidden, InvalidInput, NotFound, StaleVersion):
            raise
        except IntegrityError as exc:
            if filename is not None and self._alias_owned_by_other(filename, paper_id):
                raise AliasConflict(filename) from exc
            raise PersistenceFailed("could not update Paper metadata") from exc
        except Exception as exc:
            raise PersistenceFailed("could not update Paper metadata") from exc

    def _revision_preflight(
        self,
        paper_id: str,
        expected_row_version: int,
        *,
        source_revision: int | None = None,
    ) -> tuple[str, str, str, str | None, int | None]:
        with self._session() as session:
            paper = self._locked_published_paper(
                session,
                paper_id,
                expected_row_version,
            )
            if source_revision is not None:
                if (
                    isinstance(source_revision, bool)
                    or not isinstance(source_revision, int)
                    or source_revision < 1
                ):
                    raise InvalidInput({"revision": "must be a positive integer"})
                source = (
                    session.query(PaperRevisionModel)
                    .filter(
                        PaperRevisionModel.paper_id == paper.id,
                        PaperRevisionModel.revision_number == source_revision,
                    )
                    .one_or_none()
                )
                if source is None:
                    raise NotFound("Paper revision not found")
                source_sha256 = source.sha256
                source_size_bytes = source.size_bytes
            else:
                source_sha256 = None
                source_size_bytes = None
            return (
                paper.id,
                paper.title or "",
                paper.author_name or "",
                source_sha256,
                source_size_bytes,
            )

    def _reconcile_unowned_next_locked(
        self,
        session,
        paper,
        revision: int,
    ) -> None:
        """Remove crash residue while the Paper row remains authoritative.

        Every lifecycle writer takes the same Paper lock before choosing its
        next revision, so the no-row check and descriptor-safe removal form one
        serialized decision across app processes.
        """
        owner = (
            session.query(PaperRevisionModel)
            .filter(
                PaperRevisionModel.paper_id == paper.id,
                PaperRevisionModel.revision_number == revision,
            )
            .with_for_update()
            .one_or_none()
        )
        if owner is not None:
            raise PersistenceFailed("next Paper revision is already registered")
        self._storage.discard_unowned_revision(paper.id, revision)

    def _discard_unreferenced_append(
        self,
        paper_id: str,
        revision: int,
        *,
        sha256: str,
        size_bytes: int,
    ) -> bool:
        """Delete a promoted revision only after SQL proves it has no owner."""
        with self._session() as session:
            paper = (
                session.query(PaperMetadataModel)
                .filter(PaperMetadataModel.id == paper_id)
                .with_for_update()
                .one_or_none()
            )
            if paper is None:
                return False
            owned = (
                session.query(PaperRevisionModel)
                .filter(
                    PaperRevisionModel.paper_id == paper_id,
                    PaperRevisionModel.revision_number == revision,
                )
                .with_for_update()
                .one_or_none()
            )
            if owned is not None:
                return False
            self._storage.discard_unreferenced_revision(
                paper_id,
                revision,
                sha256=sha256,
                size_bytes=size_bytes,
            )
            session.commit()
            return True

    def _recover_appended_revision(
        self,
        paper_id: str,
        revision: int,
    ) -> PaperChanged | None:
        with self._session() as session:
            paper = session.get(PaperMetadataModel, paper_id)
            revision_row = session.get(PaperRevisionModel, (paper_id, revision))
            if (
                paper is None
                or revision_row is None
                or paper.lifecycle_state != "published"
                or paper.current_revision != revision
            ):
                return None
            return PaperChanged(
                paper_id=paper.id,
                filename=paper.filename,
                revision=revision,
                row_version=paper.row_version,
                indexing=self._indexing_outcome(session, paper),
            )

    def _append_staged_revision(
        self,
        *,
        paper_id: str,
        expected_row_version: int,
        actor: Actor,
        staged,
        restored_from_revision: int | None,
        indexing_enabled: bool,
    ) -> tuple[PaperChanged, JobLease | None, bool]:
        """Make one staged immutable PDF visible, then optionally index it.

        The PDF is promoted while the Paper row is locked and before the SQL
        visibility commit.  If that commit is known not to have persisted, the
        exact hash-bound storage primitive removes only this new revision.
        """
        attempted_revision: int | None = None
        stored = None
        try:
            with self._session() as session:
                paper = self._locked_published_paper(
                    session,
                    paper_id,
                    expected_row_version,
                )
                attempted_revision = paper.current_revision + 1
                self._reconcile_unowned_next_locked(
                    session,
                    paper,
                    attempted_revision,
                )
                stored = self._storage.promote(staged, paper.id, attempted_revision)
                self._storage.verify_revision(
                    paper.id,
                    attempted_revision,
                    sha256=stored.sha256,
                    size_bytes=stored.size_bytes,
                )
                now = self._clock()
                session.add(
                    PaperRevisionModel(
                        paper_id=paper.id,
                        revision_number=attempted_revision,
                        sha256=stored.sha256,
                        size_bytes=stored.size_bytes,
                        created_at=now,
                        created_by=actor.user_id,
                        restored_from_revision=restored_from_revision,
                    )
                )
                paper.current_revision = attempted_revision
                paper.row_version += 1
                paper.index_status = "pending"
                paper.indexed_revision = None
                paper.index_error = None
                lease = None
                if indexing_enabled:
                    lease = self._enqueue_index_job(
                        session,
                        paper.id,
                        attempted_revision,
                        self._uuid_factory(),
                    )
                self._bump_rag_version(session)
                result = PaperChanged(
                    paper_id=paper.id,
                    filename=paper.filename,
                    revision=attempted_revision,
                    row_version=paper.row_version,
                )
                session.commit()
                return result, lease, True
        except (Forbidden, InvalidInput, NotFound, PersistenceFailed, StaleVersion):
            if stored is not None and attempted_revision is not None:
                self._discard_unreferenced_append(
                    paper_id,
                    attempted_revision,
                    sha256=stored.sha256,
                    size_bytes=stored.size_bytes,
                )
            raise
        except StorageError as exc:
            if attempted_revision is not None:
                try:
                    self._discard_unreferenced_append(
                        paper_id,
                        attempted_revision,
                        sha256=(stored.sha256 if stored is not None else staged.sha256),
                        size_bytes=(stored.size_bytes if stored is not None else staged.size_bytes),
                    )
                except Exception as cleanup_exc:
                    raise StorageFailed("could not clean failed Paper revision") from cleanup_exc
            raise StorageFailed("PDF revision could not be published") from exc
        except Exception as exc:
            if attempted_revision is not None:
                recovered = self._recover_appended_revision(paper_id, attempted_revision)
                if recovered is not None:
                    return recovered, None, False
                if stored is not None:
                    try:
                        self._discard_unreferenced_append(
                            paper_id,
                            attempted_revision,
                            sha256=stored.sha256,
                            size_bytes=stored.size_bytes,
                        )
                    except Exception as cleanup_exc:
                        raise PersistenceFailed(
                            "could not clean uncommitted Paper revision"
                        ) from cleanup_exc
            raise PersistenceFailed("could not make Paper revision visible") from exc

    def _change_revision(
        self,
        intent: RevisePdf | RestoreRevision,
    ) -> PaperChanged:
        if not isinstance(intent, (RevisePdf, RestoreRevision)):
            raise InvalidInput({"intent": "must be a PDF revision record"})
        self._validate_change_actor(intent.actor)
        paper_id = self._change_target(intent.paper_id, intent.expected_row_version)
        source_revision = intent.revision if isinstance(intent, RestoreRevision) else None
        paper_id, title, author, source_sha256, source_size_bytes = self._revision_preflight(
            paper_id,
            intent.expected_row_version,
            source_revision=source_revision,
        )
        operation_id = self._uuid_factory()
        staged = None
        try:
            if isinstance(intent, RevisePdf):
                if not isinstance(intent.pdf, PdfUpload):
                    raise InvalidInput({"pdf": "must be a PdfUpload"})
                staged = self._storage.stage(intent.pdf, operation_id)
            else:
                staged = self._storage.stage_revision(
                    paper_id,
                    intent.revision,
                    operation_id,
                    sha256=source_sha256,
                    size_bytes=source_size_bytes,
                )
            staged = self._storage.apply_metadata(staged, title=title, author=author)
            probe_error = None
            try:
                indexing_enabled = bool(self._indexer.enabled())
            except Exception as exc:
                indexing_enabled = True
                probe_error = exc
            changed, lease, committed = self._append_staged_revision(
                paper_id=paper_id,
                expected_row_version=intent.expected_row_version,
                actor=intent.actor,
                staged=staged,
                restored_from_revision=source_revision,
                indexing_enabled=indexing_enabled,
            )
            if not committed:
                return changed
            if not indexing_enabled:
                indexing = IndexingOutcome(IndexingState.NOT_REQUIRED)
            elif probe_error is not None:
                indexing = self._mark_index_failure(lease, probe_error)
            else:
                indexing = self._run_inline_index(
                    paper_id,
                    lease,
                    self._monotonic_clock() + self._inline_index_timeout_seconds,
                )
            return PaperChanged(
                paper_id=changed.paper_id,
                filename=changed.filename,
                revision=changed.revision,
                row_version=changed.row_version,
                indexing=indexing,
            )
        except (Forbidden, InvalidInput, NotFound, PersistenceFailed, StaleVersion, StorageFailed):
            raise
        except StorageError as exc:
            raise StorageFailed("PDF revision could not be staged") from exc
        finally:
            if staged is not None:
                try:
                    self._storage.discard_stage(staged)
                except StorageError:
                    # The stage contains no public data and storage reconciliation
                    # owns a later durable retry.  Do not turn a committed Paper
                    # revision into an apparent failure merely because its already
                    # unlinked stage cannot be observed again.
                    pass

    def change_paper(self, intent: EditMetadata | RevisePdf | RestoreRevision) -> PaperChanged:
        if isinstance(intent, EditMetadata):
            return self._metadata_change(intent)
        if isinstance(intent, (RevisePdf, RestoreRevision)):
            return self._change_revision(intent)
        raise InvalidInput({"intent": "must be a Paper change record"})

    # Compatibility spellings remain shallow delegates while HTTP migration is
    # still pending; all behavior stays behind the unified dispatch above.
    def change_metadata(self, intent: EditMetadata) -> PaperChanged:
        return self.change_paper(intent)

    def revise_pdf(self, intent: RevisePdf) -> PaperChanged:
        return self.change_paper(intent)

    def restore_revision(self, intent: RestoreRevision) -> PaperChanged:
        return self.change_paper(intent)

    def change_many_metadata(self, intent: BulkEditMetadata) -> BulkPapersChanged:
        if not isinstance(intent, BulkEditMetadata):
            raise InvalidInput({"intent": "must be a BulkEditMetadata record"})
        self._validate_change_actor(intent.actor)
        if intent.actor.role != 3:
            raise Forbidden("bulk Paper changes require Curator access")
        patches: dict[str, MetadataPatch] = {}
        for patch in intent.patches:
            paper_id = self._validate_metadata_patch(patch)
            existing = patches.get(paper_id)
            if existing is not None and existing != patch:
                raise InvalidInput({"patches": "duplicate Paper IDs conflict"})
            patches[paper_id] = patch
        paper_ids = tuple(sorted(patches))
        try:
            with self._session() as session:
                by_id = {}
                for paper_id in paper_ids:
                    paper = (
                        session.query(PaperMetadataModel)
                        .filter(PaperMetadataModel.id == paper_id)
                        .with_for_update()
                        .one_or_none()
                    )
                    if paper is not None:
                        by_id[paper_id] = paper
                for paper_id in paper_ids:
                    paper = by_id.get(paper_id)
                    if paper is None or paper.lifecycle_state != "published":
                        raise NotFound("Paper not found")
                    patch = patches[paper_id]
                    if paper.row_version != patch.expected_row_version:
                        raise StaleVersion(paper.row_version)
                results = []
                for paper_id in paper_ids:
                    paper = by_id[paper_id]
                    for key, value in patches[paper_id].changes:
                        setattr(paper, key, value)
                    paper.row_version += 1
                    results.append(
                        PaperChanged(
                            paper_id=paper.id,
                            filename=paper.filename,
                            revision=paper.current_revision,
                            row_version=paper.row_version,
                            indexing=IndexingOutcome(IndexingState.NOT_REQUIRED),
                        )
                    )
                session.commit()
                return BulkPapersChanged(tuple(results))
        except (Forbidden, InvalidInput, NotFound, StaleVersion):
            raise
        except Exception as exc:
            raise PersistenceFailed("could not update Papers in bulk") from exc

    def ensure_index_job(
        self,
        paper_id: str,
        revision: int | None = None,
    ) -> IndexingOutcome:
        """Ensure exactly one durable rebuild job for the visible current revision."""
        try:
            canonical = validate_paper_id(paper_id)
        except (AttributeError, TypeError, ValueError) as exc:
            raise InvalidInput({"paper_id": "must be a canonical Paper UUID"}) from exc
        forced = revision is not None
        if revision is not None and (
            isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision <= 0
        ):
            raise InvalidInput({"revision": "must be a positive integer"})

        try:
            with self._session() as session:
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == canonical)
                    .with_for_update()
                    .one_or_none()
                )
                if paper is None or paper.lifecycle_state != "published":
                    raise NotFound("Paper not found")
                target = paper.current_revision if revision is None else revision
                if target != paper.current_revision:
                    raise NotFound("Paper revision is not current")
                revision_row = session.get(PaperRevisionModel, (paper.id, target))
                if revision_row is None:
                    raise NotFound("Paper revision not found")
                if not bool(self._indexer.enabled()):
                    return IndexingOutcome(IndexingState.NOT_REQUIRED)

                dedupe_key = f"index:{paper.id}:{target}"
                existing = (
                    session.query(PublishingJobModel)
                    .filter(PublishingJobModel.dedupe_key == dedupe_key)
                    .with_for_update()
                    .one_or_none()
                )
                if existing is not None:
                    return self._indexing_outcome(session, paper)
                if (
                    not forced
                    and paper.index_status == "ready"
                    and paper.indexed_revision == target
                ):
                    return IndexingOutcome(IndexingState.INDEXED)

                now = self._clock()
                job = PublishingJobModel(
                    id=self._uuid_factory(),
                    kind="index_revision",
                    paper_id=paper.id,
                    revision_number=target,
                    dedupe_key=dedupe_key,
                    state="pending",
                    attempts=0,
                    available_at=now,
                    lease_token=None,
                    lease_expires_at=None,
                    last_error=paper.index_error,
                    created_at=now,
                    updated_at=now,
                )
                session.add(job)
                if paper.index_status == "ready":
                    paper.index_status = "pending"
                    self._bump_rag_version(session)
                result_state = (
                    IndexingState.FAILED
                    if paper.index_status == "failed"
                    else IndexingState.PENDING
                )
                session.commit()
                return IndexingOutcome(
                    result_state,
                    job_id=job.id,
                    next_retry_at=(
                        now if result_state == IndexingState.FAILED else None
                    ),
                )
        except (InvalidInput, NotFound):
            raise
        except IntegrityError:
            with self._session() as session:
                paper = session.get(PaperMetadataModel, canonical)
                if paper is None or paper.lifecycle_state != "published":
                    raise NotFound("Paper not found")
                return self._indexing_outcome(session, paper)
        except Exception as exc:
            raise PersistenceFailed("could not ensure Paper index job") from exc

    def recover_job(self, job_id: str) -> JobProgress:
        """Claim one requested due job and recover it through the shared seam."""
        if (
            not isinstance(job_id, str)
            or not job_id
            or job_id != job_id.strip()
        ):
            raise InvalidInput({"job_id": "must identify a publishing job"})
        from services.publishing_jobs import claim_job_id

        try:
            lease = claim_job_id(
                self._session_factory,
                job_id,
                self._clock(),
                int(_REQUEST_LEASE_TTL.total_seconds()),
                lease_token_factory=self._uuid_factory,
            )
            if lease is not None:
                return self._recover_claimed(
                    lease,
                    self._monotonic_clock() + self._inline_index_timeout_seconds,
                )
            with self._session() as session:
                job = session.get(PublishingJobModel, job_id)
                if job is None:
                    raise NotFound("publishing job not found")
                return JobProgress(
                    job_id=job.id,
                    paper_id=job.paper_id,
                    revision=job.revision_number,
                    state=JobState(job.state),
                    attempts=job.attempts,
                    next_retry_at=(
                        job.available_at if job.state == "pending" else None
                    ),
                )
        except (InvalidInput, NotFound):
            raise
        except Exception as exc:
            raise PersistenceFailed("could not recover publishing job") from exc

    def _enqueue_delete_job(self, session, paper_id: str) -> JobLease:
        now = self._clock()
        lease_token = self._uuid_factory()
        lease_expires_at = now + _REQUEST_LEASE_TTL
        job = PublishingJobModel(
            id=self._uuid_factory(),
            kind="delete_paper",
            paper_id=paper_id,
            revision_number=0,
            dedupe_key=f"delete:{paper_id}",
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
        return JobLease(
            job_id=job.id,
            paper_id=paper_id,
            revision=0,
            kind="delete_paper",
            attempts=1,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
            created_at=now,
            previous_updated_at=now,
        )

    @staticmethod
    def _delete_lease_matches(job, paper, lease: JobLease) -> bool:
        return bool(
            isinstance(lease, JobLease)
            and job is not None
            and paper is not None
            and job.id == lease.job_id
            and job.kind == lease.kind == "delete_paper"
            and job.paper_id == paper.id == lease.paper_id
            and job.revision_number == lease.revision == 0
            and job.state == "running"
            and job.attempts == lease.attempts
            and job.lease_token == lease.lease_token
            and job.lease_expires_at is not None
            and paper.lifecycle_state == "deleting"
        )

    @staticmethod
    def _redacted_delete_error(error: Exception) -> str:
        from services.publishing_jobs import redact_job_error

        return redact_job_error(error)

    def _release_delete_failure(
        self,
        lease: JobLease,
        error: Exception,
        *,
        clock: Callable[[], datetime] | None = None,
        jitter: Callable[[], float] | None = None,
    ) -> DeletionProgress:
        from services.publishing_jobs import release_failed_job

        try:
            release_failed_job(
                self._session_factory,
                lease,
                error,
                (clock or self._clock)(),
                jitter=float((jitter or self._jitter)()),
            )
        except Exception:
            # A still-running durable lease becomes retryable when it expires.
            pass
        with self._session() as session:
            paper = session.get(PaperMetadataModel, lease.paper_id)
            if paper is None:
                return DeletionProgress(lease.paper_id, DeletionState.DELETED)
        return DeletionProgress(lease.paper_id, DeletionState.DELETING)

    def _run_delete_job(
        self,
        lease: JobLease,
        *,
        clock: Callable[[], datetime] | None = None,
        jitter: Callable[[], float] | None = None,
    ) -> DeletionProgress:
        """Run one exact deletion lease; stale leases never touch storage."""
        clock = clock or self._clock
        jitter = jitter or self._jitter
        try:
            with self._session() as session:
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == lease.paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                if paper is None:
                    return DeletionProgress(lease.paper_id, DeletionState.DELETED)
                job = (
                    session.query(PublishingJobModel)
                    .filter(PublishingJobModel.id == lease.job_id)
                    .with_for_update()
                    .one_or_none()
                )
                if (
                    not self._delete_lease_matches(job, paper, lease)
                    or job.lease_expires_at <= clock()
                ):
                    return DeletionProgress(lease.paper_id, DeletionState.DELETING)

                aliases = (
                    session.query(PaperFilenameAliasModel)
                    .filter(PaperFilenameAliasModel.paper_id == paper.id)
                    .order_by(PaperFilenameAliasModel.lookup_key)
                    .with_for_update()
                    .all()
                )
                retained_filenames = tuple(
                    sorted({paper.filename, *(alias.filename for alias in aliases)})
                )
                self._storage.delete_paper(paper.id, retained_filenames)

                session.query(PaperChunkModel).filter(
                    PaperChunkModel.paper_id == paper.id
                ).delete(synchronize_session=False)
                session.query(PaperRevisionModel).filter(
                    PaperRevisionModel.paper_id == paper.id
                ).delete(synchronize_session=False)
                session.query(PaperFilenameAliasModel).filter(
                    PaperFilenameAliasModel.paper_id == paper.id
                ).delete(synchronize_session=False)
                session.query(PublishingMigrationJournalModel).filter(
                    PublishingMigrationJournalModel.paper_id == paper.id
                ).delete(synchronize_session=False)
                session.query(PublishingMigrationIssueModel).filter(
                    PublishingMigrationIssueModel.paper_id == paper.id
                ).delete(synchronize_session=False)
                session.query(PublishingJobModel).filter(
                    PublishingJobModel.paper_id == paper.id
                ).delete(synchronize_session=False)
                self._bump_rag_version(session)
                session.delete(paper)
                session.commit()
                return DeletionProgress(lease.paper_id, DeletionState.DELETED)
        except Exception as exc:
            return self._release_delete_failure(
                lease,
                exc,
                clock=clock,
                jitter=jitter,
            )

    def delete_paper(self, intent: DeletePaper) -> DeletionProgress:
        """Hide a Paper transactionally, then attempt exact cleanup once."""
        if not isinstance(intent, DeletePaper):
            raise InvalidInput({"intent": "must be a DeletePaper record"})
        self._validate_change_actor(intent.actor)
        paper_id = self._change_target(
            intent.paper_id,
            intent.expected_row_version,
        )
        try:
            with self._session() as session:
                paper = (
                    session.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.id == paper_id)
                    .with_for_update()
                    .one_or_none()
                )
                if paper is None or paper.lifecycle_state == "publishing":
                    raise NotFound("Paper not found")
                locked_jobs = (
                    session.query(PublishingJobModel)
                    .filter(PublishingJobModel.paper_id == paper_id)
                    .order_by(PublishingJobModel.id)
                    .with_for_update()
                    .all()
                )
                if paper.lifecycle_state == "deleting":
                    matching_jobs = [
                        job
                        for job in locked_jobs
                        if job.kind == "delete_paper"
                        and job.dedupe_key == f"delete:{paper.id}"
                    ]
                    job = matching_jobs[0] if len(matching_jobs) == 1 else None
                    if job is None:
                        raise PersistenceFailed("deleting Paper has no cleanup job")
                    return DeletionProgress(paper.id, DeletionState.DELETING)
                if paper.lifecycle_state != "published":
                    raise NotFound("Paper not found")
                if paper.row_version != intent.expected_row_version:
                    raise StaleVersion(paper.row_version)

                paper.lifecycle_state = "deleting"
                paper.row_version += 1
                session.query(SubmissionModel).filter(
                    SubmissionModel.paper_id == paper.id
                ).update({SubmissionModel.paper_id: None}, synchronize_session=False)
                session.query(PublishingJobModel).filter(
                    PublishingJobModel.paper_id == paper.id
                ).delete(synchronize_session=False)
                lease = self._enqueue_delete_job(session, paper.id)
                self._bump_rag_version(session)
                session.commit()
        except (Forbidden, InvalidInput, NotFound, PersistenceFailed, StaleVersion):
            raise
        except Exception as exc:
            raise PersistenceFailed("could not begin Paper deletion") from exc
        return self._run_delete_job(lease)
