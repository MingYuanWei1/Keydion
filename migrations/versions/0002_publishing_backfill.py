"""Backfill stable Paper identities, revision files, aliases, and links."""
from pathlib import Path

from alembic import op
from sqlalchemy import create_engine, pool

from config import PAPERS_DIR
from services.publishing_migration import backfill_all


revision = "0002_publishing_backfill"
down_revision = "0001_publishing_expand"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    configured = op.get_context().config.attributes.get("papers_dir")
    papers_dir = Path(configured) if configured is not None else PAPERS_DIR

    # Never make journal durability depend on Alembic's version-marker
    # transaction.  Each helper checkpoint commits on this separate engine.
    migration_engine = create_engine(
        bind.engine.url,
        poolclass=pool.NullPool,
    )
    try:
        backfill_all(migration_engine, papers_dir)
    finally:
        migration_engine.dispose()


def downgrade():
    raise RuntimeError(
        "Publishing identity migrations require coordinated database and file snapshots; "
        "restore those snapshots instead of attempting a database-only downgrade."
    )
