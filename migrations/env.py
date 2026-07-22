import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool
from sqlalchemy.dialects.mysql import base as mysql_base

import config as app_config  # noqa: F401  -- load configured environment first
from config import RAG_EMBED_DIM
from models import BASE, VectorType


alembic_config = context.config
if (
    alembic_config.config_file_name is not None
    and alembic_config.file_config.has_section("loggers")
):
    fileConfig(
        alembic_config.config_file_name,
        disable_existing_loggers=False,
    )

target_metadata = BASE.metadata

# SQLAlchemy does not yet reflect MySQL 9 VECTOR columns. Registering the
# application type preserves the reflected dimension for drift comparison.
mysql_base.ischema_names["vector"] = VectorType


def _compare_type(
    context,
    inspected_column,
    metadata_column,
    inspected_type,
    metadata_type,
):
    if context.dialect.name != "mysql" and isinstance(metadata_type, VectorType):
        # SQLite preserves the declaration but reflects VECTOR(n) using numeric
        # affinity. MySQL reflection is registered above and remains strict.
        return False
    if isinstance(inspected_type, VectorType) or isinstance(
        metadata_type, VectorType
    ):
        return (
            not isinstance(inspected_type, VectorType)
            or not isinstance(metadata_type, VectorType)
            or inspected_type.dim != metadata_type.dim
            or metadata_type.dim != RAG_EMBED_DIM
        )
    return None


def _render_item(type_, item, autogen_context):
    if type_ == "type" and isinstance(item, VectorType):
        autogen_context.imports.add("from models import VectorType")
        return f"VectorType({item.dim})"
    return False


def _configure(connection=None, url=None):
    context.configure(
        connection=connection,
        url=url,
        target_metadata=target_metadata,
        compare_type=_compare_type,
        render_item=_render_item,
        literal_binds=connection is None,
        dialect_opts={"paramstyle": "named"} if connection is None else None,
    )


def run_migrations_offline():
    database_url = os.environ.get("PAPERQUERY_DATABASE_URL")
    if not database_url:
        raise RuntimeError("PAPERQUERY_DATABASE_URL is not set")
    _configure(url=database_url)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    supplied_connection = alembic_config.attributes.get("connection")
    if supplied_connection is not None:
        _configure(connection=supplied_connection)
        with context.begin_transaction():
            context.run_migrations()
        return

    database_url = os.environ.get("PAPERQUERY_DATABASE_URL")
    if not database_url:
        raise RuntimeError("PAPERQUERY_DATABASE_URL is not set")
    connectable = create_engine(database_url, poolclass=pool.NullPool)
    try:
        with connectable.connect() as connection:
            _configure(connection=connection)
            with context.begin_transaction():
                context.run_migrations()
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
