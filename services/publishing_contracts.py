"""Immutable, framework-free contracts for the Paper publishing lifecycle."""
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import BinaryIO, Protocol

from services.publishing_time import require_db_utc


class LifecycleError(Exception):
    pass


class InvalidInput(LifecycleError):
    def __init__(self, field_errors: dict[str, str]):
        super().__init__("invalid lifecycle input")
        self.field_errors = dict(field_errors)


class Forbidden(LifecycleError):
    pass


class NotFound(LifecycleError):
    pass


class StaleVersion(LifecycleError):
    def __init__(self, current_version: int):
        super().__init__(f"stale Paper version; current={current_version}")
        self.current_version = current_version


class SubmissionNotPending(LifecycleError):
    pass


class DecisionConflict(LifecycleError):
    pass


class IdempotencyConflict(LifecycleError):
    pass


class AliasConflict(LifecycleError):
    pass


class StorageFailed(LifecycleError):
    pass


class PersistenceFailed(LifecycleError):
    pass


class IndexDeadlineExceeded(Exception):
    pass


class IndexingState(StrEnum):
    NOT_REQUIRED = "not_required"
    INDEXED = "indexed"
    PENDING = "indexing_pending"
    FAILED = "indexing_failed"


class DeletionState(StrEnum):
    DELETING = "deleting"
    DELETED = "deleted"


class JobState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"


@dataclass(frozen=True)
class Actor:
    user_id: str
    role: int


@dataclass(frozen=True)
class NormalizedPaperMetadata:
    filename: str = ""
    title: str = ""
    journal: str = ""
    category: str = ""
    language: str = ""
    keywords: str = ""
    abstract: str = ""
    author_name: str = ""
    author_email: str = ""
    author_school: str = ""
    published_at: str = ""
    ib_ee_data: str = ""
    is_ib_sample: str = ""
    cp_data: str = ""
    is_anonymous: str = ""
    ia_data: str = ""


@dataclass(frozen=True)
class PdfUpload:
    filename: str
    stream: BinaryIO


@dataclass(frozen=True)
class DirectPublish:
    actor: Actor
    idempotency_key: str
    metadata: NormalizedPaperMetadata
    pdf: PdfUpload


@dataclass(frozen=True)
class AcceptSubmission:
    actor: Actor
    submission_id: str
    idempotency_key: str
    metadata: NormalizedPaperMetadata
    pdf: PdfUpload


@dataclass(frozen=True)
class RejectSubmission:
    actor: Actor
    submission_id: str
    idempotency_key: str
    feedback: str = ""


@dataclass(frozen=True)
class CancelSubmission:
    actor: Actor
    submission_id: str


MetadataValue = tuple[str, str]
MetadataChanges = tuple[MetadataValue, ...]
# Individual Paper edits may update any persisted display metadata.  Lifecycle,
# identity, revision, indexing, and storage state are deliberately absent.
EDITABLE_METADATA_FIELDS = frozenset(NormalizedPaperMetadata.__dataclass_fields__)
BULK_EDITABLE_METADATA_FIELDS = frozenset(
    {"journal", "category", "ib_ee_data", "cp_data", "ia_data"}
)


@dataclass(frozen=True)
class MetadataPatch:
    paper_id: str
    expected_row_version: int
    changes: MetadataChanges

    def __post_init__(self) -> None:
        if not isinstance(self.changes, tuple):
            raise InvalidInput({"changes": "must be an immutable tuple"})
        if any(
            not isinstance(change, tuple)
            or len(change) != 2
            or not all(isinstance(value, str) for value in change)
            for change in self.changes
        ):
            raise InvalidInput({"changes": "must contain key/value string tuples"})
        invalid = [key for key, _value in self.changes if key not in EDITABLE_METADATA_FIELDS]
        if invalid:
            raise InvalidInput({key: "is not editable" for key in invalid})
        duplicate_keys = {
            key for key, _value in self.changes
            if sum(1 for candidate, _value in self.changes if candidate == key) > 1
        }
        if duplicate_keys:
            raise InvalidInput({key: "may appear only once" for key in duplicate_keys})


@dataclass(frozen=True)
class EditMetadata:
    actor: Actor
    patch: MetadataPatch


@dataclass(frozen=True)
class BulkEditMetadata:
    actor: Actor
    patches: tuple[MetadataPatch, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.patches, tuple) or not self.patches or not all(
            isinstance(patch, MetadataPatch) for patch in self.patches
        ):
            raise InvalidInput({"patches": "must be a nonempty immutable tuple of MetadataPatch records"})
        invalid = {
            key
            for patch in self.patches
            for key, _value in patch.changes
            if key not in BULK_EDITABLE_METADATA_FIELDS
        }
        if invalid:
            raise InvalidInput({key: "is not bulk editable" for key in invalid})


@dataclass(frozen=True)
class RevisePdf:
    actor: Actor
    paper_id: str
    expected_row_version: int
    pdf: PdfUpload


@dataclass(frozen=True)
class RestoreRevision:
    actor: Actor
    paper_id: str
    expected_row_version: int
    revision: int


@dataclass(frozen=True)
class DeletePaper:
    actor: Actor
    paper_id: str
    expected_row_version: int


@dataclass(frozen=True)
class PreparedChunk:
    chunk_index: int
    content: str
    embedding: tuple[float, ...]
    language: str

    def __post_init__(self) -> None:
        if not isinstance(self.embedding, tuple) or not all(
            isinstance(value, float) for value in self.embedding
        ):
            raise InvalidInput({"embedding": "must be an immutable tuple of floats"})


@dataclass(frozen=True)
class PreparedRevisionIndex:
    paper_id: str
    revision: int
    chunks: tuple[PreparedChunk, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.chunks, tuple) or not all(
            isinstance(chunk, PreparedChunk) for chunk in self.chunks
        ):
            raise InvalidInput({"chunks": "must be an immutable tuple of PreparedChunk records"})


@dataclass(frozen=True)
class JobLease:
    job_id: str
    paper_id: str
    revision: int
    kind: str
    attempts: int
    lease_token: str
    lease_expires_at: datetime
    created_at: datetime
    previous_updated_at: datetime

    def __post_init__(self) -> None:
        require_db_utc(self.lease_expires_at)
        require_db_utc(self.created_at)
        require_db_utc(self.previous_updated_at)


@dataclass(frozen=True)
class IndexingOutcome:
    state: IndexingState
    job_id: str | None = None
    next_retry_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.next_retry_at is not None:
            require_db_utc(self.next_retry_at)


@dataclass(frozen=True)
class Published:
    paper_id: str
    filename: str
    revision: int
    row_version: int
    replayed: bool
    indexing: IndexingOutcome


@dataclass(frozen=True)
class DecisionRecorded:
    submission_id: str
    accepted: bool
    paper_id: str | None
    replayed: bool
    indexing: IndexingOutcome | None = None


@dataclass(frozen=True)
class SubmissionCancelled:
    submission_id: str


@dataclass(frozen=True)
class PaperChanged:
    paper_id: str
    filename: str
    revision: int
    row_version: int
    indexing: IndexingOutcome | None = None


@dataclass(frozen=True)
class BulkPapersChanged:
    papers: tuple[PaperChanged, ...]


@dataclass(frozen=True)
class DeletionProgress:
    paper_id: str
    state: DeletionState


@dataclass(frozen=True)
class JobProgress:
    job_id: str
    paper_id: str
    revision: int
    state: JobState
    attempts: int
    next_retry_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.next_retry_at is not None:
            require_db_utc(self.next_retry_at)


class PublishingLifecyclePort(Protocol):
    def direct_publish(self, intent: DirectPublish) -> Published: ...

    def accept_submission(self, intent: AcceptSubmission) -> DecisionRecorded: ...

    def reject_submission(self, intent: RejectSubmission) -> DecisionRecorded: ...

    def cancel_submission(self, intent: CancelSubmission) -> SubmissionCancelled: ...

    def change_paper(
        self,
        intent: EditMetadata | RevisePdf | RestoreRevision,
    ) -> PaperChanged: ...

    def change_metadata(self, intent: EditMetadata) -> PaperChanged: ...

    def change_many_metadata(self, intent: BulkEditMetadata) -> BulkPapersChanged: ...

    def revise_pdf(self, intent: RevisePdf) -> PaperChanged: ...

    def restore_revision(self, intent: RestoreRevision) -> PaperChanged: ...

    def delete_paper(self, intent: DeletePaper) -> DeletionProgress: ...

    def ensure_index_job(
        self,
        paper_id: str,
        revision: int | None = None,
    ) -> IndexingOutcome: ...

    def recover_job(self, job_id: str) -> JobProgress: ...
