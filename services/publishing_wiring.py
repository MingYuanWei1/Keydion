"""Production construction for the framework-free publishing lifecycle."""

from __future__ import annotations

import random
import time
import uuid

from services.paper_storage import PaperStorage
from services.publishing import PublishingLifecycle
from services.publishing_jobs import PublishingWorker
from services.publishing_rag import StrictRagAdapter


def _build_lifecycle(session_factory):
    from config import (
        PAPERS_DIR,
        PENDING_PAPERS_DIR,
        PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS,
    )
    from services.publishing_time import utc_now_db

    return PublishingLifecycle(
        session_factory=session_factory,
        storage=PaperStorage(PAPERS_DIR, PENDING_PAPERS_DIR),
        indexer=StrictRagAdapter(),
        clock=utc_now_db,
        monotonic_clock=time.monotonic,
        uuid_factory=lambda: str(uuid.uuid4()),
        jitter=random.random,
        inline_index_timeout_seconds=PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS,
    )


def build_publishing_lifecycle():
    from db import get_session_factory

    return _build_lifecycle(get_session_factory())


def build_publishing_worker():
    from config import (
        PUBLISHING_JOB_LEASE_SECONDS,
        PUBLISHING_RESERVATION_GRACE_SECONDS,
        PUBLISHING_WORKER_POLL_SECONDS,
    )
    from db import get_session_factory
    from services.publishing_time import utc_now_db

    session_factory = get_session_factory()
    lifecycle = _build_lifecycle(session_factory)
    return PublishingWorker(
        lifecycle=lifecycle,
        session_factory=session_factory,
        clock=utc_now_db,
        monotonic_clock=time.monotonic,
        lease_token_factory=lambda: str(uuid.uuid4()),
        jitter=random.random,
        lease_seconds=PUBLISHING_JOB_LEASE_SECONDS,
        reservation_grace_seconds=PUBLISHING_RESERVATION_GRACE_SECONDS,
        poll_seconds=PUBLISHING_WORKER_POLL_SECONDS,
    )
