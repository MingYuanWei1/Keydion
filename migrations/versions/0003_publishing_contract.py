"""Contract the backfilled schema around stable Paper identity."""
from pathlib import Path

from alembic import op
import sqlalchemy as sa
from sqlalchemy import create_engine, pool

from config import PAPERS_DIR
from services.publishing_migration import validate_contract_ready


revision = "0003_publishing_contract"
down_revision = "0002_publishing_backfill"
branch_labels = None
depends_on = None


_LIFECYCLE_CHECK = (
    "(lifecycle_state = 'publishing' AND current_revision IS NULL) OR "
    "(lifecycle_state IN ('published', 'deleting') AND current_revision IS NOT NULL)"
)


def _validate_before_ddl():
    bind = op.get_bind()
    configured = op.get_context().config.attributes.get("papers_dir")
    papers_dir = Path(configured) if configured is not None else PAPERS_DIR
    validation_engine = create_engine(bind.engine.url, poolclass=pool.NullPool)
    try:
        validate_contract_ready(validation_engine, papers_dir)
    finally:
        validation_engine.dispose()


def _sqlite_contract():
    # SQLite cannot alter a primary key or add foreign keys.  Rebuild only the
    # three legacy tables; the flat filename and VECTOR payload columns remain.
    op.execute("""
        CREATE TABLE papers_metadata_contract (
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            filename VARCHAR(255) NOT NULL UNIQUE,
            title VARCHAR(255), journal VARCHAR(255), category VARCHAR(255),
            language VARCHAR(255), keywords TEXT, abstract TEXT,
            author_name VARCHAR(255), author_email VARCHAR(255),
            author_school VARCHAR(255), published_at VARCHAR(255),
            ib_ee_data TEXT, is_ib_sample VARCHAR(10), is_anonymous VARCHAR(10),
            cp_data TEXT, ia_data TEXT,
            lifecycle_state VARCHAR(16) NOT NULL,
            current_revision INTEGER,
            row_version INTEGER NOT NULL,
            index_status VARCHAR(16) NOT NULL,
            indexed_revision INTEGER,
            index_error TEXT,
            direct_idempotency_key VARCHAR(255) UNIQUE,
            direct_payload_hash VARCHAR(64),
            origin_submission_id VARCHAR(255) UNIQUE,
            reservation_expires_at DATETIME,
            CONSTRAINT ck_papers_metadata_lifecycle_revision CHECK (
                (lifecycle_state = 'publishing' AND current_revision IS NULL) OR
                (lifecycle_state IN ('published', 'deleting') AND current_revision IS NOT NULL)
            )
        )
    """)
    op.execute("""
        INSERT INTO papers_metadata_contract (
            id, filename, title, journal, category, language, keywords, abstract,
            author_name, author_email, author_school, published_at, ib_ee_data,
            is_ib_sample, is_anonymous, cp_data, ia_data, lifecycle_state,
            current_revision, row_version, index_status, indexed_revision,
            index_error, direct_idempotency_key, direct_payload_hash,
            origin_submission_id, reservation_expires_at
        )
        SELECT
            id, filename, title, journal, category, language, keywords, abstract,
            author_name, author_email, author_school, published_at, ib_ee_data,
            is_ib_sample, is_anonymous, cp_data, ia_data, lifecycle_state,
            current_revision, row_version, index_status, indexed_revision,
            index_error, direct_idempotency_key, direct_payload_hash,
            origin_submission_id, reservation_expires_at
        FROM papers_metadata
    """)
    op.drop_table("papers_metadata")
    op.rename_table("papers_metadata_contract", "papers_metadata")

    op.execute("""
        CREATE TABLE papers_chunks_contract (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            filename VARCHAR(255),
            paper_id VARCHAR(36) NOT NULL,
            revision_number INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            content TEXT,
            embedding_vec BLOB,
            lang VARCHAR(10),
            CONSTRAINT fk_papers_chunks_revision
                FOREIGN KEY (paper_id, revision_number)
                REFERENCES paper_revisions (paper_id, revision_number)
                ON DELETE CASCADE,
            CONSTRAINT uq_papers_chunks_paper_revision_chunk
                UNIQUE (paper_id, revision_number, chunk_index)
        )
    """)
    op.execute("""
        INSERT INTO papers_chunks_contract (
            id, filename, paper_id, revision_number, chunk_index,
            content, embedding_vec, lang
        )
        SELECT id, filename, paper_id, revision_number, chunk_index,
               content, embedding_vec, lang
        FROM papers_chunks
    """)
    op.drop_table("papers_chunks")
    op.rename_table("papers_chunks_contract", "papers_chunks")
    op.create_index("ix_papers_chunks_filename", "papers_chunks", ["filename"])
    op.create_index("ix_papers_chunks_paper_id", "papers_chunks", ["paper_id"])

    op.execute("""
        CREATE TABLE submissions_contract (
            id VARCHAR(255) NOT NULL PRIMARY KEY,
            pdf_filename VARCHAR(255), pending_filename VARCHAR(255),
            title VARCHAR(255), author_name VARCHAR(255), author_email VARCHAR(255),
            author_school VARCHAR(255), status VARCHAR(50), submitted_at VARCHAR(255),
            feedback TEXT, abstract TEXT, keywords TEXT, journal VARCHAR(255),
            category VARCHAR(255), language VARCHAR(255), submitted_by VARCHAR(255),
            original_filename VARCHAR(255), ib_ee_data TEXT, is_ib_sample VARCHAR(10),
            is_anonymous VARCHAR(10), cp_data TEXT, ia_data TEXT,
            paper_id VARCHAR(36), submitter_name VARCHAR(255), reviewed_at DATETIME,
            reviewer VARCHAR(255), comment TEXT,
            decision_idempotency_key VARCHAR(255) UNIQUE,
            decision_payload_hash VARCHAR(64),
            CONSTRAINT fk_submissions_paper
                FOREIGN KEY (paper_id) REFERENCES papers_metadata (id)
                ON DELETE SET NULL
        )
    """)
    op.execute("""
        INSERT INTO submissions_contract (
            id, pdf_filename, pending_filename, title, author_name, author_email,
            author_school, status, submitted_at, feedback, abstract, keywords,
            journal, category, language, submitted_by, original_filename,
            ib_ee_data, is_ib_sample, is_anonymous, cp_data, ia_data, paper_id,
            submitter_name, reviewed_at, reviewer, comment,
            decision_idempotency_key, decision_payload_hash
        )
        SELECT
            id, pdf_filename, pending_filename, title, author_name, author_email,
            author_school, status, submitted_at, feedback, abstract, keywords,
            journal, category, language, submitted_by, original_filename,
            ib_ee_data, is_ib_sample, is_anonymous, cp_data, ia_data, paper_id,
            submitter_name, reviewed_at, reviewer, comment,
            decision_idempotency_key, decision_payload_hash
        FROM submissions
    """)
    op.drop_table("submissions")
    op.rename_table("submissions_contract", "submissions")
    op.create_index("ix_submissions_paper_id", "submissions", ["paper_id"])

    _sqlite_paper_foreign_keys()


def _sqlite_paper_foreign_keys():
    relationships = (
        ("paper_revisions", "fk_paper_revisions_paper", "CASCADE"),
        ("paper_filename_aliases", "fk_paper_filename_aliases_paper", "CASCADE"),
        ("publishing_jobs", "fk_publishing_jobs_paper", "CASCADE"),
        ("publishing_migration_journal", "fk_publishing_migration_journal_paper", "CASCADE"),
        ("publishing_migration_issues", "fk_publishing_migration_issues_paper", "CASCADE"),
    )
    for table_name, constraint_name, ondelete in relationships:
        with op.batch_alter_table(table_name, recreate="always") as batch:
            batch.create_foreign_key(
                constraint_name,
                "papers_metadata",
                ["paper_id"],
                ["id"],
                ondelete=ondelete,
            )


def _mysql_contract():
    op.alter_column("papers_metadata", "id", existing_type=sa.String(36), nullable=False)
    op.alter_column(
        "papers_metadata", "lifecycle_state", existing_type=sa.String(16), nullable=False,
    )
    op.alter_column("papers_metadata", "row_version", existing_type=sa.Integer(), nullable=False)
    op.alter_column(
        "papers_metadata", "index_status", existing_type=sa.String(16), nullable=False,
    )
    op.drop_constraint("PRIMARY", "papers_metadata", type_="primary")
    op.create_primary_key("pk_papers_metadata", "papers_metadata", ["id"])
    op.create_unique_constraint(
        "uq_papers_metadata_filename", "papers_metadata", ["filename"],
    )
    op.drop_index("ux_papers_metadata_migration_id", table_name="papers_metadata")
    op.create_check_constraint(
        "ck_papers_metadata_lifecycle_revision", "papers_metadata", _LIFECYCLE_CHECK,
    )

    op.create_foreign_key(
        "fk_paper_revisions_paper", "paper_revisions", "papers_metadata",
        ["paper_id"], ["id"], ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_paper_filename_aliases_paper", "paper_filename_aliases", "papers_metadata",
        ["paper_id"], ["id"], ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_publishing_jobs_paper", "publishing_jobs", "papers_metadata",
        ["paper_id"], ["id"], ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_publishing_migration_journal_paper", "publishing_migration_journal",
        "papers_metadata", ["paper_id"], ["id"], ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_publishing_migration_issues_paper", "publishing_migration_issues",
        "papers_metadata", ["paper_id"], ["id"], ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_submissions_paper", "submissions", "papers_metadata",
        ["paper_id"], ["id"], ondelete="SET NULL",
    )

    op.alter_column("papers_chunks", "paper_id", existing_type=sa.String(36), nullable=False)
    op.alter_column("papers_chunks", "revision_number", existing_type=sa.Integer(), nullable=False)
    op.alter_column("papers_chunks", "chunk_index", existing_type=sa.Integer(), nullable=False)
    op.create_unique_constraint(
        "uq_papers_chunks_paper_revision_chunk",
        "papers_chunks",
        ["paper_id", "revision_number", "chunk_index"],
    )
    op.create_foreign_key(
        "fk_papers_chunks_revision", "papers_chunks", "paper_revisions",
        ["paper_id", "revision_number"], ["paper_id", "revision_number"],
        ondelete="CASCADE",
    )


def upgrade():
    # MySQL DDL implicitly commits, so every content/count/hash invariant is
    # validated on a read-only separate connection before the first DDL call.
    _validate_before_ddl()
    if op.get_bind().dialect.name == "sqlite":
        _sqlite_contract()
    else:
        _mysql_contract()


def downgrade():
    raise RuntimeError(
        "Publishing identity migrations require coordinated database and file snapshots; "
        "restore those snapshots instead of attempting a database-only downgrade."
    )
