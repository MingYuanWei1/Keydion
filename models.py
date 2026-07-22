"""ORM models and startup schema-version verification."""
import uuid
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, Column, Date, DateTime, ForeignKey,
    ForeignKeyConstraint, Index, Integer, LargeBinary,
    String, Unicode, UnicodeText, UniqueConstraint, create_engine, func, inspect,
    select, text,
)
from sqlalchemy.dialects.mysql import MEDIUMBLOB, MEDIUMTEXT, base as mysql_base
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.types import UserDefinedType

import db
from db import BASE
from config import RAG_EMBED_DIM

class LocalUser(BASE):
    __tablename__ = "local_users"
    username = Column(Unicode(255), primary_key=True)
    password = Column(Unicode(255), nullable=False)
    registration_date = Column(Date)
    expiry_date = Column(Date)
    role = Column(Unicode(10), nullable=False)
    email = Column(Unicode(255))
    first_name = Column(Unicode(255))
    last_name = Column(Unicode(255))
    school = Column(Unicode(255))


class MsUser(BASE):
    __tablename__ = "ms_users"
    ms_id = Column(Unicode(255), primary_key=True)
    tenant_id = Column(Unicode(255))
    email = Column(Unicode(255))
    display_name = Column(Unicode(255))
    first_name = Column(Unicode(255))
    last_name = Column(Unicode(255))
    school = Column(Unicode(255))
    grade = Column(Unicode(255))
    role = Column(Unicode(10))
    password = Column(Unicode(255))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class JournalModel(BASE):
    __tablename__ = "journals"
    id = Column(Unicode(255), primary_key=True)
    name = Column(Unicode(255))
    slug = Column(Unicode(255))
    cover_image = Column(Unicode(255))
    introduction = Column(UnicodeText)
    created_at = Column(Unicode(255))

class PaperMetadataModel(BASE):
    __tablename__ = "papers_metadata"
    __table_args__ = (
        CheckConstraint(
            "(lifecycle_state = 'publishing' AND current_revision IS NULL) OR "
            "(lifecycle_state IN ('published', 'deleting') AND current_revision IS NOT NULL)",
            name="ck_papers_metadata_lifecycle_revision",
        ),
    )
    id = Column(Unicode(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    filename = Column(
        Unicode(255).with_variant(
            String(255, collation="utf8mb4_bin"), "mysql",
        ),
        nullable=False,
        unique=True,
    )
    title = Column(Unicode(255))
    journal = Column(Unicode(255))
    category = Column(Unicode(255))
    language = Column(Unicode(255))
    keywords = Column(UnicodeText)
    abstract = Column(UnicodeText)
    author_name = Column(Unicode(255))
    author_email = Column(Unicode(255))
    author_school = Column(Unicode(255))
    published_at = Column(Unicode(255))
    ib_ee_data = Column(UnicodeText)
    is_ib_sample = Column(Unicode(10))
    is_anonymous = Column(Unicode(10))
    cp_data = Column(UnicodeText)
    ia_data = Column(UnicodeText)
    lifecycle_state = Column(Unicode(16), nullable=False, default="publishing")
    current_revision = Column(Integer)
    row_version = Column(Integer, nullable=False, default=0)
    index_status = Column(Unicode(16), nullable=False, default="pending")
    indexed_revision = Column(Integer)
    index_error = Column(UnicodeText)
    direct_idempotency_key = Column(
        Unicode(255).with_variant(
            String(255, collation="utf8mb4_bin"), "mysql",
        ),
        unique=True,
    )
    direct_payload_hash = Column(Unicode(64))
    origin_submission_id = Column(
        Unicode(255).with_variant(
            String(255, collation="utf8mb4_bin"), "mysql",
        ),
        unique=True,
    )
    reservation_expires_at = Column(DateTime(timezone=False))
    integrity_status = Column(
        Unicode(16), nullable=False, default="unknown", server_default="unknown"
    )
    integrity_checked_at = Column(DateTime(timezone=False))
    integrity_checked_revision = Column(Integer)


class PaperRevisionModel(BASE):
    __tablename__ = "paper_revisions"
    paper_id = Column(
        Unicode(36), ForeignKey("papers_metadata.id", ondelete="CASCADE"),
        primary_key=True,
    )
    revision_number = Column(Integer, primary_key=True)
    sha256 = Column(Unicode(64), nullable=False)
    size_bytes = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=False), nullable=False)
    created_by = Column(Unicode(255), nullable=False)
    restored_from_revision = Column(Integer)


class PaperFilenameAliasModel(BASE):
    __tablename__ = "paper_filename_aliases"
    lookup_key = Column(
        Unicode(255).with_variant(
            String(255, collation="utf8mb4_bin"), "mysql",
        ),
        primary_key=True,
    )
    filename = Column(Unicode(255), nullable=False)
    paper_id = Column(
        Unicode(36), ForeignKey("papers_metadata.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    created_at = Column(DateTime(timezone=False), nullable=False)


class PublishingJobModel(BASE):
    __tablename__ = "publishing_jobs"
    __table_args__ = (
        Index(
            "ix_publishing_jobs_due_order",
            "available_at",
            "created_at",
            "id",
        ),
    )
    id = Column(Unicode(36), primary_key=True)
    kind = Column(Unicode(32), nullable=False)
    paper_id = Column(
        Unicode(36), ForeignKey("papers_metadata.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    revision_number = Column(Integer, nullable=False, default=0)
    dedupe_key = Column(Unicode(255), nullable=False, unique=True)
    state = Column(Unicode(16), nullable=False)
    attempts = Column(Integer, nullable=False, default=0)
    available_at = Column(DateTime(timezone=False), nullable=False)
    lease_token = Column(Unicode(36))
    lease_expires_at = Column(DateTime(timezone=False))
    last_error = Column(UnicodeText)
    created_at = Column(DateTime(timezone=False), nullable=False)
    updated_at = Column(DateTime(timezone=False), nullable=False)


class PublishingMigrationJournalModel(BASE):
    __tablename__ = "publishing_migration_journal"
    legacy_key = Column(Unicode(255), primary_key=True)
    paper_id = Column(
        Unicode(36), ForeignKey("papers_metadata.id", ondelete="CASCADE"),
        nullable=False, unique=True,
    )
    revision_number = Column(Integer, nullable=False, default=1)
    source_sha256 = Column(Unicode(64))
    source_size_bytes = Column(Integer)
    legacy_chunk_count = Column(Integer, nullable=False, default=0)
    legacy_chunk_fingerprint = Column(Unicode(64))
    checkpoint = Column(Unicode(32), nullable=False)
    created_at = Column(DateTime(timezone=False), nullable=False)
    updated_at = Column(DateTime(timezone=False), nullable=False)


class PublishingMigrationStateModel(BASE):
    __tablename__ = "publishing_migration_state"
    name = Column(Unicode(32), primary_key=True)
    paper_count = Column(Integer, nullable=False)
    submission_count = Column(Integer, nullable=False)
    chunk_count = Column(Integer, nullable=False)
    vector_count = Column(Integer, nullable=False)
    ddl_phase = Column(Unicode(32), nullable=False)
    captured_at = Column(DateTime(timezone=False), nullable=False)


class SubmissionIdentityFenceModel(BASE):
    """Singleton row serializing Submission identity creation and cleanup."""

    __tablename__ = "submission_identity_fence"
    name = Column(Unicode(32), primary_key=True)
    generation = Column(BigInteger, nullable=False, default=0)


class PublishingMigrationIssueModel(BASE):
    __tablename__ = "publishing_migration_issues"
    id = Column(Unicode(36), primary_key=True)
    kind = Column(Unicode(32), nullable=False)
    legacy_key = Column(Unicode(255))
    paper_id = Column(
        Unicode(36), ForeignKey("papers_metadata.id", ondelete="CASCADE"),
        index=True,
    )
    details = Column(UnicodeText, nullable=False)
    blocking = Column(Boolean, nullable=False, default=False)
    resolved_at = Column(DateTime(timezone=False))
    created_at = Column(DateTime(timezone=False), nullable=False)
    updated_at = Column(DateTime(timezone=False), nullable=False)


class _VectorReadExpression(FunctionElement):
    """Render a stable binary VECTOR result only for MySQL drivers."""

    inherit_cache = True
    name = "vector_read"
    type = LargeBinary()


@compiles(_VectorReadExpression)
def _compile_vector_read(expression, compiler, **kwargs):
    return compiler.process(next(iter(expression.clauses)), **kwargs)


@compiles(_VectorReadExpression, "mysql")
def _compile_mysql_vector_read(expression, compiler, **kwargs):
    column = compiler.process(next(iter(expression.clauses)), **kwargs)
    return f"CAST({column} AS BINARY)"


class VectorType(UserDefinedType):
    """MySQL 9 VECTOR(n) column. Python-side values are JSON-text vectors
    ("[0.1, 0.2, ...]") bound through STRING_TO_VECTOR(); reads come back as
    little-endian IEEE-754 float32 bytes (decode with numpy.frombuffer)."""
    cache_ok = True

    def __init__(self, dim):
        self.dim = dim

    def get_col_spec(self, **kw):
        return f"VECTOR({self.dim})"

    def bind_expression(self, bindvalue):
        return func.STRING_TO_VECTOR(bindvalue)

    def column_expression(self, column):
        return _VectorReadExpression(column)


# SQLAlchemy does not yet know MySQL 9's VECTOR type. Register it wherever the
# application model is imported, not only when Alembic happens to load first.
mysql_base.ischema_names["vector"] = VectorType


class PaperChunkModel(BASE):
    __tablename__ = "papers_chunks"
    __table_args__ = (
        ForeignKeyConstraint(
            ["paper_id", "revision_number"],
            ["paper_revisions.paper_id", "paper_revisions.revision_number"],
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "paper_id", "revision_number", "chunk_index",
            name="uq_papers_chunks_paper_revision_chunk",
        ),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(Unicode(255), index=True)
    paper_id = Column(Unicode(36), nullable=False, index=True)
    revision_number = Column(Integer, nullable=False)
    chunk_index = Column(Integer, nullable=False)
    content = Column(UnicodeText)
    # Binary chunk vector (MySQL 9 VECTOR). The legacy JSON `embedding` column
    # is intentionally unmapped; the approved Alembic migration owns its
    # backfill into this column and the eventual legacy-column drop.
    embedding_vec = Column(VectorType(RAG_EMBED_DIM))
    lang = Column(Unicode(10))


class RagIndexMetaModel(BASE):
    __tablename__ = "rag_index_meta"
    name = Column(Unicode(32), primary_key=True)
    value = Column(Integer, nullable=False, default=0)


class ConversationModel(BASE):
    __tablename__ = "conversations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    serial = Column(Unicode(6), unique=True, index=True)
    owner_key = Column(Unicode(64), index=True)
    title = Column(Unicode(255))
    created_at = Column(Unicode(40))
    updated_at = Column(Unicode(40))


class ChatMessageModel(BASE):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(Integer, index=True)
    role = Column(Unicode(16))          # "user" | "assistant"
    content = Column(UnicodeText)
    citations = Column(UnicodeText)     # JSON-encoded list
    attachments = Column(UnicodeText)   # JSON-encoded list of filenames (display-only)
    cited_papers = Column(UnicodeText)  # JSON list of Paper UUID/revision/display records
    created_at = Column(Unicode(40))


class AttachmentChunkModel(BASE):
    __tablename__ = "attachment_chunks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(Integer, index=True)
    filename = Column(Unicode(255))
    chunk_index = Column(Integer)
    content = Column(UnicodeText)
    embedding = Column(UnicodeText().with_variant(MEDIUMTEXT(), "mysql"))   # JSON list[float]; MEDIUMTEXT: Gemini vectors exceed TEXT's 64KB
    created_at = Column(Unicode(40))


class AttachmentJobModel(BASE):
    __tablename__ = "attachment_jobs"
    __table_args__ = (
        Index("ix_attachment_jobs_due", "state", "available_at", "created_at"),
        Index("ix_attachment_jobs_conversation", "conversation_id"),
    )

    id = Column(Unicode(36), primary_key=True)
    conversation_id = Column(Integer, nullable=False)
    filename = Column(Unicode(255), nullable=False)
    payload = Column(LargeBinary().with_variant(MEDIUMBLOB(), "mysql"))
    state = Column(Unicode(16), nullable=False)
    attempts = Column(Integer, nullable=False, default=0)
    available_at = Column(DateTime, nullable=False)
    lease_token = Column(Unicode(36))
    lease_expires_at = Column(DateTime)
    last_error = Column(UnicodeText)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class NewsArticleModel(BASE):
    __tablename__ = "news_articles"
    id = Column(Unicode(255), primary_key=True)
    title = Column(Unicode(255))
    category = Column(Unicode(255))
    abstract = Column(UnicodeText)
    body = Column(UnicodeText)
    author = Column(Unicode(255))
    image_url = Column(Unicode(255))
    published_at = Column(Unicode(255))
    status = Column(Unicode(20), default="published")


class GuideModel(BASE):
    __tablename__ = "guides"
    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(Unicode(120), unique=True, index=True, nullable=False)
    category = Column(Unicode(80), default="")
    sort_order = Column(Integer, default=100)
    published = Column(Boolean, default=False)
    title_en = Column(Unicode(200), default="")
    title_zh = Column(Unicode(200), default="")
    summary_en = Column(Unicode(300), default="")
    summary_zh = Column(Unicode(300), default="")
    body_en = Column(UnicodeText, default="")
    body_zh = Column(UnicodeText, default="")
    created_at = Column(Unicode(40), default="")
    updated_at = Column(Unicode(40), default="")


class ResourceNode(BASE):
    __tablename__ = "resource_nodes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, index=True, nullable=True)   # null = top level; no FK (app-managed)
    node_type = Column(Unicode(10))                          # "folder" | "file"
    name = Column(Unicode(255))
    stored_filename = Column(Unicode(255))                   # uuid name on disk (files only)
    original_filename = Column(Unicode(255))                 # original upload name -> download name
    mime_type = Column(Unicode(120))
    size_bytes = Column(Integer)
    description = Column(UnicodeText)
    min_role = Column(Integer, default=1)                    # min role to view THIS node
    created_at = Column(Unicode(40), default="")


class SubmissionModel(BASE):
    __tablename__ = "submissions"
    id = Column(
        Unicode(255).with_variant(
            String(255, collation="utf8mb4_bin"), "mysql",
        ),
        primary_key=True,
    )
    pdf_filename = Column(Unicode(255))
    pending_filename = Column(Unicode(255))
    title = Column(Unicode(255))
    author_name = Column(Unicode(255))
    author_email = Column(Unicode(255))
    author_school = Column(Unicode(255))
    status = Column(Unicode(50))
    submitted_at = Column(Unicode(255))
    feedback = Column(UnicodeText)
    abstract = Column(UnicodeText)
    keywords = Column(UnicodeText)
    journal = Column(Unicode(255))
    category = Column(Unicode(255))
    language = Column(Unicode(255))
    submitted_by = Column(Unicode(255))
    original_filename = Column(Unicode(255))
    ib_ee_data = Column(UnicodeText)
    is_ib_sample = Column(Unicode(10))
    is_anonymous = Column(Unicode(10))
    cp_data = Column(UnicodeText)
    ia_data = Column(UnicodeText)
    paper_id = Column(
        Unicode(36), ForeignKey("papers_metadata.id", ondelete="SET NULL"),
        unique=True,
    )
    submitter_name = Column(Unicode(255))
    reviewed_at = Column(DateTime(timezone=False))
    reviewer = Column(Unicode(255))
    comment = Column(UnicodeText)
    decision_idempotency_key = Column(
        Unicode(255).with_variant(
            String(255, collation="utf8mb4_bin"), "mysql",
        ),
        unique=True,
    )
    decision_payload_hash = Column(Unicode(64))


class SessionModel(BASE):
    __tablename__ = "sessions"
    __table_args__ = (
        CheckConstraint(
            "account_type IN ('local', 'microsoft')",
            name="ck_sessions_account_type",
        ),
        Index("ix_sessions_account", "account_type", "account_id"),
        Index("ix_sessions_last_seen", "last_seen"),
        Index("ix_sessions_expires_at", "expires_at"),
    )

    token = Column(Unicode(64), primary_key=True)
    account_type = Column(Unicode(16), nullable=False)
    account_id = Column(Unicode(255), nullable=False)
    last_seen = Column(DateTime, nullable=False)
    expires_at = Column(DateTime)


class OAuthLoginAttemptModel(BASE):
    __tablename__ = "oauth_login_attempts"
    __table_args__ = (
        Index("ix_oauth_login_attempts_expires_at", "expires_at"),
    )

    state_hash = Column(Unicode(64), primary_key=True)
    browser_hash = Column(Unicode(64), nullable=False)
    next_url = Column(UnicodeText, nullable=False, default="")
    remember = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False)
    expires_at = Column(DateTime, nullable=False)


class RateLimitBucketModel(BASE):
    __tablename__ = "rate_limit_buckets"
    __table_args__ = (
        Index("ix_rate_limit_buckets_expires_at", "expires_at"),
    )

    scope = Column(Unicode(64), primary_key=True)
    key_hash = Column(Unicode(64), primary_key=True)
    window_started_at = Column(DateTime, nullable=False)
    count = Column(Integer, nullable=False, default=0)
    blocked_until = Column(DateTime)
    expires_at = Column(DateTime, nullable=False)


def _alembic_config() -> Config:
    return Config(str(Path(__file__).resolve().parent / "alembic.ini"))


def _alembic_head() -> str:
    head = ScriptDirectory.from_config(_alembic_config()).get_current_head()
    if head is None:
        raise RuntimeError("Alembic has no configured head revision")
    return head


def _ensure_rag_version_row(connection, initial_value: int) -> None:
    table = RagIndexMetaModel.__table__
    current = connection.execute(
        select(table.c.value).where(table.c.name == "chunks_version")
    ).scalar_one_or_none()
    if current is None:
        connection.execute(
            table.insert().values(
                name="chunks_version",
                value=initial_value,
            )
        )


def _ensure_submission_identity_fence_row(connection) -> None:
    table = SubmissionIdentityFenceModel.__table__
    current = connection.execute(
        select(table.c.generation).where(table.c.name == "global")
    ).scalar_one_or_none()
    if current is None:
        connection.execute(table.insert().values(name="global", generation=0))


def bootstrap_empty_database(engine, *, lock_timeout: int = 30) -> str:
    """Create and stamp a genuinely empty database under an operator lock."""
    head = _alembic_head()
    with engine.connect() as connection:
        dialect = connection.dialect.name
        lock_acquired = False
        if dialect == "mysql":
            acquired = connection.execute(
                text("SELECT GET_LOCK('keydion_empty_bootstrap', :timeout)"),
                {"timeout": lock_timeout},
            ).scalar_one()
            connection.commit()
            if acquired != 1:
                raise RuntimeError("could not acquire the database bootstrap lock")
            lock_acquired = True
        elif dialect == "postgresql":
            connection.execute(text("SELECT pg_advisory_lock(1262836047)"))
            connection.commit()
            lock_acquired = True

        try:
            existing_tables = set(inspect(connection).get_table_names())
            if existing_tables:
                raise RuntimeError(
                    "database bootstrap requires a completely empty database; "
                    f"found tables: {', '.join(sorted(existing_tables))}"
                )
            # SQLAlchemy inspection may autobegin even though it only reads.
            connection.commit()

            with connection.begin():
                BASE.metadata.create_all(connection)
                _ensure_rag_version_row(connection, initial_value=0)
                _ensure_submission_identity_fence_row(connection)

            alembic_config = _alembic_config()
            alembic_config.attributes["connection"] = connection
            try:
                command.stamp(alembic_config, head)
            finally:
                alembic_config.attributes.pop("connection", None)

            current = tuple(
                MigrationContext.configure(connection).get_current_heads()
            )
            if current != (head,):
                raise RuntimeError(
                    "database bootstrap did not produce the expected Alembic head"
                )
            return head
        finally:
            if lock_acquired and dialect == "mysql":
                connection.execute(
                    text("SELECT RELEASE_LOCK('keydion_empty_bootstrap')")
                )
                connection.commit()
            elif lock_acquired and dialect == "postgresql":
                connection.execute(text("SELECT pg_advisory_unlock(1262836047)"))
                connection.commit()


def ensure_schema_current(engine) -> None:
    """Verify a current schema; web and worker startup never mutate it."""
    user_tables = set(inspect(engine).get_table_names()) - {"alembic_version"}
    head = _alembic_head()
    if not user_tables:
        raise RuntimeError(
            "database is empty; run `python -m tools.bootstrap_database "
            "--confirm-empty-bootstrap` before starting web or worker processes"
        )

    with engine.connect() as conn:
        current = tuple(MigrationContext.configure(conn).get_current_heads())
    if not current:
        raise RuntimeError(
            "database schema is unversioned; run the documented Alembic "
            "adoption procedure"
        )
    if current != (head,):
        raise RuntimeError(
            f"database schema state {current!r} does not match code head "
            f"{head!r}"
        )


def init_db() -> None:
    if db._ENGINE is None:
        if not db.DB_URL:
            raise RuntimeError(
                "PAPERQUERY_DATABASE_URL is not set; set it to a SQLAlchemy "
                "database URL (see .env.example)."
            )
        engine = create_engine(db.DB_URL, pool_pre_ping=True, pool_recycle=3600)
        try:
            ensure_schema_current(engine)
        except Exception:
            engine.dispose()
            raise

        session_factory = sessionmaker(bind=engine)
        db._ENGINE = engine
        db._SESSION_LOCAL = session_factory
