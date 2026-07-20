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
    SubmissionModel,
)
from services.paper_identity import normalize_alias_key
from services.paper_storage import PaperStorage, StorageError
from services.publishing_contracts import (
    Actor,
    AcceptSubmission,
    AliasConflict,
    CancelSubmission,
    DecisionConflict,
    DecisionRecorded,
    DirectPublish,
    Forbidden,
    IdempotencyConflict,
    IndexDeadlineExceeded,
    IndexingOutcome,
    IndexingState,
    InvalidInput,
    JobLease,
    NormalizedPaperMetadata,
    NotFound,
    PersistenceFailed,
    PreparedRevisionIndex,
    Published,
    RejectSubmission,
    StorageFailed,
    SubmissionCancelled,
    SubmissionNotPending,
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
_PAPER_METADATA_FIELDS = tuple(asdict(NormalizedPaperMetadata()).keys())


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
                submission = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.id == intent.submission_id)
                    .with_for_update()
                    .one_or_none()
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
            submission = (
                session.query(SubmissionModel)
                .filter(SubmissionModel.id == intent.submission_id)
                .with_for_update()
                .one_or_none()
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
                submission = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.id == intent.submission_id)
                    .with_for_update()
                    .one_or_none()
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
        session.commit()
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
    ) -> JobLease | None:
        with self._session() as session:
            submission = None
            if submission_decision is not None:
                decision_key, decision_hash, comment = submission_decision
                submission = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.id == intent.submission_id)
                    .with_for_update()
                    .one_or_none()
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
                    return None
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
            submission = session.get(SubmissionModel, submission_id)
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
            submission = session.get(SubmissionModel, submission_id)
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
            if self._monotonic_clock() >= deadline:
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
    ) -> bool:
        """Best-effort cleanup after the acceptance transaction is permanent."""
        try:
            token = self._storage.rehydrate_pending_trash(
                pending_filename,
                submission_id,
            )
            if token is not None:
                self._storage.discard_pending_trash(token)
            if self._storage.pending_exists(pending_filename):
                token = self._storage.trash_pending(pending_filename, submission_id)
                self._storage.discard_pending_trash(token)
            return True
        except StorageError:
            return False

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
                return _SubmissionDecision(
                    submission_id=recovered.submission_id,
                    accepted=True,
                    paper_id=recovered.paper_id,
                    replayed=False,
                    indexing=recovered.indexing,
                )

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
                submission = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.id == intent.submission_id)
                    .with_for_update()
                    .one_or_none()
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
                    raise DecisionConflict("Submission acceptance is already reserved")
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

    def _restore_cancellation_status(self, submission_id: str) -> None:
        try:
            with self._session() as session:
                submission = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.id == submission_id)
                    .with_for_update()
                    .one_or_none()
                )
                if submission is not None and submission.status == "cancelling":
                    submission.status = "pending"
                    submission.reviewed_at = None
                    session.commit()
        except Exception as exc:
            raise PersistenceFailed("could not restore pending Submission state") from exc

    def _finish_cancellation(
        self,
        submission_id: str,
        *,
        expected_owner: str | None,
    ) -> SubmissionCancelled:
        with self._session() as session:
            submission = (
                session.query(SubmissionModel)
                .filter(SubmissionModel.id == submission_id)
                .with_for_update()
                .one_or_none()
            )
            if submission is None:
                return SubmissionCancelled(submission_id=submission_id)
            if expected_owner is not None and submission.submitted_by != expected_owner:
                raise Forbidden("only the submitting Reader may cancel")
            if submission.status != "cancelling":
                raise SubmissionNotPending("Submission is not cancelling")
            pending_filename = submission.pending_filename

        token = None
        try:
            token = self._storage.rehydrate_pending_trash(
                pending_filename,
                submission_id,
            )
            if token is None and self._storage.pending_exists(pending_filename):
                token = self._storage.trash_pending(pending_filename, submission_id)
        except StorageError as exc:
            audit = None
            try:
                audit = self._storage.rehydrate_pending_trash(
                    None,
                    submission_id,
                )
                if audit is not None:
                    recoverable = self._storage.rehydrate_pending_trash(
                        pending_filename,
                        submission_id,
                    )
                    self._storage.restore_pending(recoverable)
            except StorageError:
                pass
            else:
                try:
                    self._restore_cancellation_status(submission_id)
                except PersistenceFailed:
                    pass
            raise StorageFailed("pending Submission PDF could not be trashed") from exc

        try:
            with self._session() as session:
                submission = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.id == submission_id)
                    .with_for_update()
                    .one_or_none()
                )
                if submission is None:
                    committed = True
                else:
                    committed = False
                    if expected_owner is not None and submission.submitted_by != expected_owner:
                        raise Forbidden("only the submitting Reader may cancel")
                    if submission.status != "cancelling":
                        raise SubmissionNotPending("Submission is not cancelling")
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
        except (Forbidden, SubmissionNotPending):
            if token is not None:
                self._storage.restore_pending(token)
            self._restore_cancellation_status(submission_id)
            raise
        except Exception as exc:
            with self._session() as session:
                row_survives = session.get(SubmissionModel, submission_id) is not None
            if not row_survives:
                committed = True
            else:
                if token is not None:
                    try:
                        self._storage.restore_pending(token)
                    except StorageError as restore_exc:
                        raise StorageFailed(
                            "cancellation transaction failed and PDF restore failed"
                        ) from restore_exc
                self._restore_cancellation_status(submission_id)
                raise PersistenceFailed("could not delete cancelling Submission") from exc

        if committed and token is not None:
            try:
                self._storage.discard_pending_trash(token)
            except StorageError:
                pass
        return SubmissionCancelled(submission_id=submission_id)

    def cancel_submission(self, intent: CancelSubmission) -> SubmissionCancelled:
        self._validate_cancellation(intent)
        try:
            with self._session() as session:
                submission = (
                    session.query(SubmissionModel)
                    .filter(SubmissionModel.id == intent.submission_id)
                    .with_for_update()
                    .one_or_none()
                )
                if submission is None:
                    raise NotFound("Submission not found")
                if submission.submitted_by != intent.actor.user_id:
                    raise Forbidden("only the submitting Reader may cancel")
                if submission.status not in {"pending", "cancelling"}:
                    raise SubmissionNotPending("Submission is not pending")
                if submission.status == "pending":
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
                        raise SubmissionNotPending(
                            "Submission acceptance reservation prevents cancellation"
                        )
                    submission.status = "cancelling"
                    submission.reviewed_at = self._clock()
                    session.commit()
        except (Forbidden, NotFound, SubmissionNotPending):
            raise
        except Exception as exc:
            raise PersistenceFailed("could not begin Submission cancellation") from exc
        return self._finish_cancellation(
            intent.submission_id,
            expected_owner=intent.actor.user_id,
        )

    def reconcile_submissions(self) -> int:
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
                accepted = [
                    (row.id, row.pending_filename)
                    for row in session.query(SubmissionModel)
                    .filter(
                        SubmissionModel.status == "accepted",
                        SubmissionModel.reviewed_at <= cutoff,
                    )
                    .order_by(SubmissionModel.id)
                    .all()
                ]
            reconciled = 0
            for submission_id in cancelling_ids:
                self._finish_cancellation(submission_id, expected_owner=None)
                reconciled += 1
            for submission_id, pending_filename in accepted:
                if self._cleanup_accepted_pending(submission_id, pending_filename):
                    reconciled += 1

            for operation_id in self._storage.stale_pending_trash(cutoff):
                with self._session() as session:
                    submission = (
                        session.query(SubmissionModel)
                        .filter(SubmissionModel.id == operation_id)
                        .with_for_update()
                        .one_or_none()
                    )
                    if submission is None:
                        token = self._storage.rehydrate_pending_trash(None, operation_id)
                        if token is not None:
                            self._storage.discard_pending_trash(token)
                            reconciled += 1
                        continue
                    token = self._storage.rehydrate_pending_trash(
                        submission.pending_filename,
                        operation_id,
                    )
                    if token is None:
                        continue
                    if submission.status in {"pending", "rejected"}:
                        self._storage.restore_pending(token)
                        reconciled += 1
                    elif submission.status == "accepted":
                        self._storage.discard_pending_trash(token)
                        reconciled += 1
                    # A still-cancelling row is retained for the next attempt;
                    # its deterministic trash is never generically removed.
            return reconciled
        except (StorageFailed, PersistenceFailed):
            raise
        except StorageError as exc:
            raise StorageFailed("Submission storage reconciliation failed") from exc
        except Exception as exc:
            raise PersistenceFailed("Submission reconciliation failed") from exc

    def reconcile_pending_cancellations(self) -> int:
        return self.reconcile_submissions()
