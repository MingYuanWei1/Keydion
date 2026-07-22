"""Align submission Paper uniqueness with ORM metadata."""

from alembic import op
import sqlalchemy as sa


revision = "0004_submission_paper_uniqueness"
down_revision = "0003_publishing_contract"
branch_labels = None
depends_on = None


_INDEX_NAME = "ix_submissions_paper_id"
_TABLE_NAME = "submissions"
_PAPER_ID_SIGNATURE = ("paper_id",)


def _has_unique_paper_id(inspector):
    index_signatures = {
        (tuple(index.get("column_names") or ()), bool(index.get("unique")))
        for index in inspector.get_indexes(_TABLE_NAME)
    }
    unique_signatures = {
        tuple(constraint.get("column_names") or ())
        for constraint in inspector.get_unique_constraints(_TABLE_NAME)
    }
    return (
        (_PAPER_ID_SIGNATURE, True) in index_signatures
        or _PAPER_ID_SIGNATURE in unique_signatures
    )


def _index_names(inspector):
    return {
        index.get("name")
        for index in inspector.get_indexes(_TABLE_NAME)
        if index.get("name")
    }


def upgrade():
    inspector = sa.inspect(op.get_bind())
    if not _has_unique_paper_id(inspector):
        raise RuntimeError(
            "cannot remove ix_submissions_paper_id without a unique "
            "submissions.paper_id contract"
        )
    if _INDEX_NAME in _index_names(inspector):
        op.drop_index(_INDEX_NAME, table_name=_TABLE_NAME)


def downgrade():
    inspector = sa.inspect(op.get_bind())
    if _INDEX_NAME not in _index_names(inspector):
        op.create_index(
            _INDEX_NAME,
            _TABLE_NAME,
            ["paper_id"],
            unique=False,
        )
