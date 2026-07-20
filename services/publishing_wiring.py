"""Production construction for the framework-free publishing lifecycle."""

from __future__ import annotations

import random
import time
import uuid

from services.paper_storage import PaperStorage
from services.publishing import PublishingLifecycle


class StrictRagAdapter:
    """Fail loudly if production indexing is used before Task 11 installs it."""

    def enabled(self) -> bool:
        return True

    def prepare(
        self,
        *,
        paper_id,
        revision_number,
        pdf_bytes,
        language,
        deadline,
    ):
        raise NotImplementedError("UUID RAG adapter is installed in Task 11")


def build_publishing_lifecycle():
    from config import (
        PAPERS_DIR,
        PENDING_PAPERS_DIR,
        PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS,
    )
    from db import get_session_factory
    from services.publishing_time import utc_now_db

    return PublishingLifecycle(
        session_factory=get_session_factory(),
        storage=PaperStorage(PAPERS_DIR, PENDING_PAPERS_DIR),
        indexer=StrictRagAdapter(),
        clock=utc_now_db,
        monotonic_clock=time.monotonic,
        uuid_factory=lambda: str(uuid.uuid4()),
        jitter=random.random,
        inline_index_timeout_seconds=PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS,
    )
