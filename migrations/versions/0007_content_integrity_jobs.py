"""Add durable attachment jobs and cached Paper integrity state."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import MEDIUMBLOB


revision = "0007_content_integrity_jobs"
down_revision = "0006_security_state"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "attachment_jobs",
        sa.Column("id", sa.Unicode(36), primary_key=True),
        sa.Column("conversation_id", sa.Integer(), nullable=False),
        sa.Column("filename", sa.Unicode(255), nullable=False),
        sa.Column(
            "payload",
            sa.LargeBinary().with_variant(MEDIUMBLOB(), "mysql"),
            nullable=True,
        ),
        sa.Column("state", sa.Unicode(16), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(), nullable=False),
        sa.Column("lease_token", sa.Unicode(36), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("last_error", sa.UnicodeText(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_attachment_jobs_due",
        "attachment_jobs",
        ["state", "available_at", "created_at"],
    )
    op.create_index(
        "ix_attachment_jobs_conversation",
        "attachment_jobs",
        ["conversation_id"],
    )
    op.add_column(
        "papers_metadata",
        sa.Column(
            "integrity_status",
            sa.Unicode(16),
            nullable=False,
            server_default="unknown",
        ),
    )
    op.add_column(
        "papers_metadata",
        sa.Column("integrity_checked_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "papers_metadata",
        sa.Column("integrity_checked_revision", sa.Integer(), nullable=True),
    )


def downgrade():
    op.drop_column("papers_metadata", "integrity_checked_revision")
    op.drop_column("papers_metadata", "integrity_checked_at")
    op.drop_column("papers_metadata", "integrity_status")
    op.drop_table("attachment_jobs")
