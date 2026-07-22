"""Replace the single-account session registry with per-device tokens."""

from alembic import op
import sqlalchemy as sa


revision = "0005_concurrent_login_sessions"
down_revision = "0004_submission_paper_uniqueness"
branch_labels = None
depends_on = None


def _create_concurrent_sessions_table():
    op.create_table(
        "sessions",
        sa.Column("token", sa.Unicode(64), primary_key=True),
        sa.Column("account_type", sa.Unicode(16), nullable=False),
        sa.Column("account_id", sa.Unicode(255), nullable=False),
        sa.Column("last_seen", sa.DateTime(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "account_type IN ('local', 'microsoft')",
            name="ck_sessions_account_type",
        ),
    )
    op.create_index(
        "ix_sessions_account",
        "sessions",
        ["account_type", "account_id"],
    )
    op.create_index("ix_sessions_last_seen", "sessions", ["last_seen"])
    op.create_index("ix_sessions_expires_at", "sessions", ["expires_at"])


def upgrade():
    op.drop_table("sessions")
    _create_concurrent_sessions_table()


def downgrade():
    op.drop_table("sessions")
    op.create_table(
        "sessions",
        sa.Column("username", sa.Unicode(255), primary_key=True),
        sa.Column("token", sa.Unicode(255)),
        sa.Column("last_seen", sa.Unicode(255)),
    )
