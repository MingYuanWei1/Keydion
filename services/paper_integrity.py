"""Full-hash scanner for cached current-Paper integrity state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from models import PaperMetadataModel, PaperRevisionModel
from services.paper_identity import validate_paper_id
from services.paper_storage import StorageError


@dataclass(frozen=True)
class IntegrityScanResult:
    checked: int
    verified: int
    corrupt: int
    stale: int


def scan_current_revisions(
    *,
    session_factory,
    storage,
    paper_id: str | None = None,
) -> IntegrityScanResult:
    canonical = validate_paper_id(paper_id) if paper_id is not None else None
    with session_factory() as session:
        query = (
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
        )
        if canonical is not None:
            query = query.filter(PaperMetadataModel.id == canonical)
        snapshots = [
            (
                paper.id,
                paper.current_revision,
                paper.row_version,
                revision.sha256,
                revision.size_bytes,
            )
            for paper, revision in query.all()
        ]

    verified = corrupt = stale = 0
    for current_id, revision_number, row_version, sha256, size_bytes in snapshots:
        try:
            storage.verify_revision(
                current_id,
                revision_number,
                sha256=sha256,
                size_bytes=size_bytes,
            )
        except StorageError:
            status = "corrupt"
        else:
            status = "verified"

        with session_factory() as session:
            paper = (
                session.query(PaperMetadataModel)
                .filter(PaperMetadataModel.id == current_id)
                .with_for_update()
                .one_or_none()
            )
            current_revision = (
                session.get(PaperRevisionModel, (current_id, revision_number))
                if paper is not None
                else None
            )
            if (
                paper is None
                or paper.lifecycle_state != "published"
                or paper.current_revision != revision_number
                or paper.row_version != row_version
                or current_revision is None
                or current_revision.sha256 != sha256
                or current_revision.size_bytes != size_bytes
            ):
                stale += 1
                session.rollback()
                continue
            paper.integrity_status = status
            paper.integrity_checked_at = datetime.utcnow()
            paper.integrity_checked_revision = revision_number
            session.commit()
        if status == "verified":
            verified += 1
        else:
            corrupt += 1

    return IntegrityScanResult(
        checked=len(snapshots),
        verified=verified,
        corrupt=corrupt,
        stale=stale,
    )
