"""Expand the legacy filename schema for resumable Paper migration."""
from pathlib import Path

from alembic import op
import sqlalchemy as sa
from sqlalchemy import create_engine, pool

from config import PAPERS_DIR
from services.publishing_migration import MigrationBlocked, migration_fence, run_preflight


revision = "0001_publishing_expand"
down_revision = "0000_legacy_baseline"
branch_labels = None
depends_on = None


def _validate_legacy_before_expand():
    bind = op.get_bind()
    configured = op.get_context().config.attributes.get("papers_dir")
    papers_dir = Path(configured) if configured is not None else PAPERS_DIR
    validation_engine = create_engine(bind.engine.url, poolclass=pool.NullPool)
    try:
        report = run_preflight(validation_engine, papers_dir)
    finally:
        validation_engine.dispose()
    if report.blockers:
        details = ", ".join(
            f"{issue.code}:{issue.legacy_key}" for issue in report.blockers
        )
        raise MigrationBlocked(f"publishing expand preflight blocked: {details}")


def _upgrade_unfenced():
    _validate_legacy_before_expand()
    # Keep filename as the legacy primary key throughout expand/backfill.  Every
    # added column is nullable until the contract validator proves it populated.
    for column in (
        sa.Column("id", sa.String(36), nullable=True),
        sa.Column("lifecycle_state", sa.String(16), nullable=True),
        sa.Column("current_revision", sa.Integer(), nullable=True),
        sa.Column("row_version", sa.Integer(), nullable=True),
        sa.Column("index_status", sa.String(16), nullable=True),
        sa.Column("indexed_revision", sa.Integer(), nullable=True),
        sa.Column("index_error", sa.Text(), nullable=True),
        sa.Column("direct_idempotency_key", sa.String(255), nullable=True),
        sa.Column("direct_payload_hash", sa.String(64), nullable=True),
        sa.Column("origin_submission_id", sa.String(255), nullable=True),
        sa.Column("reservation_expires_at", sa.DateTime(), nullable=True),
    ):
        op.add_column("papers_metadata", column)
    op.create_index(
        "ux_papers_metadata_migration_id",
        "papers_metadata",
        ["id"],
        unique=True,
    )
    op.create_index(
        "uq_papers_metadata_direct_idempotency_key",
        "papers_metadata",
        ["direct_idempotency_key"],
        unique=True,
    )
    op.create_index(
        "uq_papers_metadata_origin_submission_id",
        "papers_metadata",
        ["origin_submission_id"],
        unique=True,
    )

    op.add_column("papers_chunks", sa.Column("paper_id", sa.String(36), nullable=True))
    op.add_column("papers_chunks", sa.Column("revision_number", sa.Integer(), nullable=True))
    op.create_index("ix_papers_chunks_paper_id", "papers_chunks", ["paper_id"])

    for column in (
        sa.Column("paper_id", sa.String(36), nullable=True),
        sa.Column("submitter_name", sa.String(255), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(), nullable=True),
        sa.Column("reviewer", sa.String(255), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("decision_idempotency_key", sa.String(255), nullable=True),
        sa.Column("decision_payload_hash", sa.String(64), nullable=True),
    ):
        op.add_column("submissions", column)
    op.create_index("ix_submissions_paper_id", "submissions", ["paper_id"])
    op.create_index(
        "uq_submissions_decision_idempotency_key",
        "submissions",
        ["decision_idempotency_key"],
        unique=True,
    )

    op.create_table(
        "paper_revisions",
        sa.Column("paper_id", sa.String(36), nullable=False),
        sa.Column("revision_number", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column("restored_from_revision", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("paper_id", "revision_number"),
    )
    op.create_table(
        "paper_filename_aliases",
        sa.Column(
            "lookup_key",
            sa.String(255).with_variant(
                sa.String(255, collation="utf8mb4_bin"), "mysql",
            ),
            nullable=False,
        ),
        sa.Column("filename", sa.String(255), nullable=False),
        sa.Column("paper_id", sa.String(36), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("lookup_key"),
    )
    op.create_index(
        "ix_paper_filename_aliases_paper_id",
        "paper_filename_aliases",
        ["paper_id"],
    )
    op.create_table(
        "publishing_jobs",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("paper_id", sa.String(36), nullable=False),
        sa.Column("revision_number", sa.Integer(), nullable=False),
        sa.Column("dedupe_key", sa.String(255), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(), nullable=False),
        sa.Column("lease_token", sa.String(36), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedupe_key", name="uq_publishing_jobs_dedupe_key"),
    )
    op.create_index("ix_publishing_jobs_paper_id", "publishing_jobs", ["paper_id"])
    op.create_table(
        "publishing_migration_journal",
        sa.Column("legacy_key", sa.String(255), nullable=False),
        sa.Column("paper_id", sa.String(36), nullable=False),
        sa.Column("revision_number", sa.Integer(), nullable=False),
        sa.Column("source_sha256", sa.String(64), nullable=True),
        sa.Column("source_size_bytes", sa.Integer(), nullable=True),
        sa.Column("legacy_chunk_count", sa.Integer(), nullable=False),
        sa.Column("legacy_chunk_fingerprint", sa.String(64), nullable=True),
        sa.Column("checkpoint", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("legacy_key"),
        sa.UniqueConstraint("paper_id", name="uq_publishing_migration_journal_paper_id"),
    )
    op.create_table(
        "publishing_migration_state",
        sa.Column("name", sa.String(32), nullable=False),
        sa.Column("paper_count", sa.Integer(), nullable=False),
        sa.Column("submission_count", sa.Integer(), nullable=False),
        sa.Column("chunk_count", sa.Integer(), nullable=False),
        sa.Column("vector_count", sa.Integer(), nullable=False),
        sa.Column("captured_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("name"),
    )
    op.create_table(
        "publishing_migration_issues",
        sa.Column("id", sa.String(36), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("legacy_key", sa.String(255), nullable=True),
        sa.Column("paper_id", sa.String(36), nullable=True),
        sa.Column("details", sa.Text(), nullable=False),
        sa.Column("blocking", sa.Boolean(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_publishing_migration_issues_paper_id",
        "publishing_migration_issues",
        ["paper_id"],
    )


def upgrade():
    bind = op.get_bind()
    configured = op.get_context().config.attributes.get("papers_dir")
    papers_dir = Path(configured) if configured is not None else PAPERS_DIR
    migration_engine = create_engine(bind.engine.url, poolclass=pool.NullPool)
    try:
        with migration_fence(migration_engine, papers_dir):
            _upgrade_unfenced()
    finally:
        migration_engine.dispose()


def downgrade():
    raise RuntimeError(
        "Publishing identity migrations require coordinated database and file snapshots; "
        "restore those snapshots instead of attempting a database-only downgrade."
    )
