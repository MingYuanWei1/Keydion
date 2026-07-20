"""Immutable, framework-free contracts for the Paper publishing lifecycle."""
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import BinaryIO, Protocol


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
    feedback: str = ""


@dataclass(frozen=True)
class CancelSubmission:
    actor: Actor
    submission_id: str


MetadataValue = tuple[str, str]
MetadataChanges = tuple[MetadataValue, ...]
EDITABLE_METADATA_FIELDS = frozenset({"journal", "category", "ib_ee_data", "cp_data", "ia_data"})


@dataclass(frozen=True)
class MetadataPatch:
    paper_id: str
    expected_row_version: int
    changes: MetadataChanges

    def __post_init__(self) -> None:
        if not isinstance(self.changes, tuple):
            raise InvalidInput({"changes": "must be an immutable tuple"})
        invalid = [key for key, _value in self.changes if key not in EDITABLE_METADATA_FIELDS]
        if invalid:
            raise InvalidInput({key: "is not editable" for key in invalid})


@dataclass(frozen=True)
class EditMetadata:
    actor: Actor
    patch: MetadataPatch


@dataclass(frozen=True)
class BulkEditMetadata:
    actor: Actor
    patches: tuple[MetadataPatch, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.patches, tuple) or not self.patches:
            raise InvalidInput({"patches": "must be a nonempty immutable tuple"})


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


@dataclass(frozen=True)
class PreparedRevisionIndex:
    paper_id: str
    revision: int
    chunks: tuple[PreparedChunk, ...]


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


@dataclass(frozen=True)
class IndexingOutcome:
    state: IndexingState
    job_id: str | None = None
    next_retry_at: datetime | None = None


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


class PublishingLifecyclePort(Protocol):
    def direct_publish(self, intent: DirectPublish) -> Published: ...

    def accept_submission(self, intent: AcceptSubmission) -> DecisionRecorded: ...

    def reject_submission(self, intent: RejectSubmission) -> DecisionRecorded: ...

    def cancel_submission(self, intent: CancelSubmission) -> SubmissionCancelled: ...

    def change_metadata(self, intent: EditMetadata) -> PaperChanged: ...

    def change_many_metadata(self, intent: BulkEditMetadata) -> BulkPapersChanged: ...

    def revise_pdf(self, intent: RevisePdf) -> PaperChanged: ...

    def restore_revision(self, intent: RestoreRevision) -> PaperChanged: ...

    def delete_paper(self, intent: DeletePaper) -> DeletionProgress: ...

    def ensure_index_job(self, paper_id: str, revision: int | None = None) -> JobProgress: ...

    def recover_job(self, job_id: str) -> JobProgress: ...
