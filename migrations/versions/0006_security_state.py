"""Add server-side OAuth attempts and shared abuse-limit buckets."""

from alembic import op
import sqlalchemy as sa


revision = "0006_security_state"
down_revision = "0005_concurrent_login_sessions"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "oauth_login_attempts",
        sa.Column("state_hash", sa.Unicode(64), primary_key=True),
        sa.Column("browser_hash", sa.Unicode(64), nullable=False),
        sa.Column("next_url", sa.UnicodeText(), nullable=False),
        sa.Column("remember", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_oauth_login_attempts_expires_at",
        "oauth_login_attempts",
        ["expires_at"],
    )
    op.create_table(
        "rate_limit_buckets",
        sa.Column("scope", sa.Unicode(64), primary_key=True),
        sa.Column("key_hash", sa.Unicode(64), primary_key=True),
        sa.Column("window_started_at", sa.DateTime(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("blocked_until", sa.DateTime(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_rate_limit_buckets_expires_at",
        "rate_limit_buckets",
        ["expires_at"],
    )


def downgrade():
    op.drop_table("rate_limit_buckets")
    op.drop_table("oauth_login_attempts")
