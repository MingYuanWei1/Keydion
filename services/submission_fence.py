"""Portable database serialization for Submission creation and orphan cleanup."""

from __future__ import annotations

from sqlalchemy import update

from models import SubmissionIdentityFenceModel


_FENCE_NAME = "global"


def lock_submission_creation_fence(session) -> None:
    """Acquire the durable singleton write fence for this SQL transaction.

    An actual UPDATE is deliberate: SQLite acquires its database write lock,
    while MySQL acquires the existing singleton row lock. Submission writers
    and no-row cleanup use this same operation before checking or changing
    Submission identity authority.
    """
    result = session.execute(
        update(SubmissionIdentityFenceModel)
        .where(SubmissionIdentityFenceModel.name == _FENCE_NAME)
        .values(
            generation=SubmissionIdentityFenceModel.generation + 1
        )
    )
    if result.rowcount != 1:
        raise RuntimeError("Submission creation fence is unavailable")
