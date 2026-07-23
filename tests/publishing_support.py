"""Reusable SQLite and filesystem adapters for publishing lifecycle tests."""

from __future__ import annotations

import io
import itertools
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from pypdf import PdfWriter
from sqlalchemy import create_engine, event
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from db import BASE
from models import (
    PaperFilenameAliasModel,
    PaperMetadataModel,
    PublishingJobModel,
    SubmissionIdentityFenceModel,
    VectorType,
)
from services.paper_storage import PaperStorage
from services.publishing import PublishingLifecycle
from services.publishing_contracts import PreparedRevisionIndex


@compiles(VectorType, "sqlite")
def _compile_vector_for_sqlite(_type, _compiler, **_kwargs):
    return "TEXT"


class FakeRevisionIndexer:
    def __init__(self, error=None, enabled=True):
        self.error = error
        self._enabled = enabled
        self.calls = []

    def enabled(self):
        return self._enabled

    def prepare(self, *, paper_id, revision_number, pdf_bytes, language, deadline):
        self.calls.append((paper_id, revision_number, language, deadline))
        if self.error is not None:
            raise self.error
        return PreparedRevisionIndex(
            paper_id=paper_id,
            revision=revision_number,
            chunks=(),
        )


class PublishingLifecycleTestCase:
    """Mixin exposing deterministic, framework-free lifecycle fixtures."""

    def setUp(self):
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.engine = create_engine(f"sqlite:///{root / 'publishing.sqlite'}")

        @event.listens_for(self.engine, "connect")
        def _enable_foreign_keys(connection, _record):
            connection.create_function("STRING_TO_VECTOR", 1, lambda value: value)
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

        BASE.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        with self.session_factory() as session:
            session.add(SubmissionIdentityFenceModel(name="global", generation=0))
            session.commit()
        self.storage = PaperStorage(root / "papers", root / "pending")
        self.now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.monotonic_now = 1000.0
        self._uuid_numbers = itertools.count(1)
        self.indexer = FakeRevisionIndexer()
        self.lifecycle = self.new_lifecycle(self.indexer)

    def tearDown(self):
        self.storage.close()
        self.engine.dispose()
        self.tmp.cleanup()
        super().tearDown()

    def uuid_factory(self):
        return f"00000000-0000-4000-8000-{next(self._uuid_numbers):012d}"

    def new_lifecycle(self, indexer=None, *, session_factory=None, storage=None):
        return PublishingLifecycle(
            session_factory=session_factory or self.session_factory,
            storage=storage or self.storage,
            indexer=indexer or FakeRevisionIndexer(),
            clock=lambda: self.now,
            monotonic_clock=lambda: self.monotonic_now,
            uuid_factory=self.uuid_factory,
            jitter=lambda: 0.0,
        )

    @staticmethod
    def valid_pdf_bytes(label="paper"):
        stream = io.BytesIO()
        writer = PdfWriter()
        writer.add_blank_page(width=72, height=72)
        writer.add_metadata({"/Subject": label})
        writer.write(stream)
        return stream.getvalue()

    def paper(self, paper_id):
        with self.session_factory() as session:
            return session.get(PaperMetadataModel, paper_id)

    def papers(self):
        with self.session_factory() as session:
            return session.query(PaperMetadataModel).order_by(PaperMetadataModel.id).all()

    def alias(self, filename):
        from services.paper_identity import normalize_alias_key

        with self.session_factory() as session:
            return session.get(PaperFilenameAliasModel, normalize_alias_key(filename))

    def jobs(self, paper_id):
        with self.session_factory() as session:
            return (
                session.query(PublishingJobModel)
                .filter(PublishingJobModel.paper_id == paper_id)
                .order_by(PublishingJobModel.created_at, PublishingJobModel.id)
                .all()
            )

    def staged_entries(self):
        return sorted(self.storage.staging_dir.iterdir())
