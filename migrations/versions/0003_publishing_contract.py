"""Contract the backfilled schema around stable Paper identity."""
import re
from pathlib import Path

from alembic import op
import sqlalchemy as sa
from sqlalchemy import create_engine, pool

from config import PAPERS_DIR
from services.publishing_migration import migration_fence, validate_contract_ready


revision = "0003_publishing_contract"
down_revision = "0002_publishing_backfill"
branch_labels = None
depends_on = None


_LIFECYCLE_CHECK = (
    "(lifecycle_state = 'publishing' AND current_revision IS NULL) OR "
    "(lifecycle_state IN ('published', 'deleting') AND current_revision IS NOT NULL)"
)


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


_DDL_PHASES = (
    "expanded", "paper_contract", "relationships_contract", "complete",
)
_SIMPLE_FOREIGN_KEYS = (
    ("fk_paper_revisions_paper", "paper_revisions", "papers_metadata", ["paper_id"], ["id"], "CASCADE"),
    ("fk_paper_filename_aliases_paper", "paper_filename_aliases", "papers_metadata", ["paper_id"], ["id"], "CASCADE"),
    ("fk_publishing_jobs_paper", "publishing_jobs", "papers_metadata", ["paper_id"], ["id"], "CASCADE"),
    ("fk_publishing_migration_journal_paper", "publishing_migration_journal", "papers_metadata", ["paper_id"], ["id"], "CASCADE"),
    ("fk_publishing_migration_issues_paper", "publishing_migration_issues", "papers_metadata", ["paper_id"], ["id"], "CASCADE"),
    ("fk_submissions_paper", "submissions", "papers_metadata", ["paper_id"], ["id"], "SET NULL"),
)


def _snapshot_restore_required(details):
    raise RuntimeError(
        "unsafe partial publishing contract shape; restore coordinated database "
        f"and file snapshots before retrying ({details})"
    )


def _index_signatures(inspector, table_name):
    signatures = {
        (tuple(index.get("column_names") or ()), bool(index.get("unique")))
        for index in inspector.get_indexes(table_name)
    }
    signatures.update({
        (tuple(unique.get("column_names") or ()), True)
        for unique in inspector.get_unique_constraints(table_name)
    })
    return signatures


def _foreign_keys_by_name(inspector, table_name):
    return {
        foreign_key.get("name"): foreign_key
        for foreign_key in inspector.get_foreign_keys(table_name)
    }


def _foreign_key_matches(foreign_key, referred_table, local, remote, ondelete):
    return (
        foreign_key is not None
        and foreign_key.get("referred_table") == referred_table
        and foreign_key.get("constrained_columns") == local
        and foreign_key.get("referred_columns") == remote
        and str((foreign_key.get("options") or {}).get("ondelete", "")).upper() == ondelete
    )


_CHECK_TOKEN = re.compile(
    r"'(?:''|[^'])*'|<>|!=|<=|>=|=|\(|\)|,|[a-z_][a-z0-9_]*",
    re.IGNORECASE,
)


def _normalized_check_expression(expression):
    source = re.sub(
        r"_[a-z0-9]+(?=')", "", str(expression).replace("`", "").casefold(),
    )
    tokens = _CHECK_TOKEN.findall(source)
    position = 0

    def consume(expected=None):
        nonlocal position
        if position >= len(tokens):
            raise ValueError("unexpected end of lifecycle check")
        token = tokens[position]
        if expected is not None and token != expected:
            raise ValueError(f"expected {expected!r}, found {token!r}")
        position += 1
        return token

    def predicate():
        identifier = consume()
        operator = consume()
        if operator == "=":
            return ("eq", identifier, consume())
        if operator == "is":
            negate = position < len(tokens) and tokens[position] == "not"
            if negate:
                consume("not")
            consume("null")
            return ("is_null", identifier, negate)
        if operator == "in":
            consume("(")
            values = []
            while True:
                values.append(consume())
                if tokens[position] == ")":
                    consume(")")
                    break
                consume(",")
            return ("in", identifier, tuple(values))
        raise ValueError(f"unsupported lifecycle check operator {operator!r}")

    def factor():
        if tokens[position] == "(":
            consume("(")
            result = or_expression()
            consume(")")
            return result
        return predicate()

    def and_expression():
        parts = [factor()]
        while position < len(tokens) and tokens[position] == "and":
            consume("and")
            parts.append(factor())
        return parts[0] if len(parts) == 1 else ("and", tuple(parts))

    def or_expression():
        parts = [and_expression()]
        while position < len(tokens) and tokens[position] == "or":
            consume("or")
            parts.append(and_expression())
        return parts[0] if len(parts) == 1 else ("or", tuple(parts))

    try:
        normalized = or_expression()
        if position != len(tokens):
            raise ValueError("trailing lifecycle check tokens")
        return normalized
    except (IndexError, ValueError):
        return None


def _mysql_schema_phase(engine):
    inspector = sa.inspect(engine)
    paper_pk = inspector.get_pk_constraint("papers_metadata").get("constrained_columns")
    paper_columns = {
        column["name"]: column for column in inspector.get_columns("papers_metadata")
    }
    paper_required = ("id", "lifecycle_state", "row_version", "index_status")
    paper_indexes = _index_signatures(inspector, "papers_metadata")
    checks_by_name = {
        constraint.get("name"): constraint.get("sqltext")
        for constraint in inspector.get_check_constraints("papers_metadata")
    }
    if paper_pk == ["filename"]:
        if (
            any(not paper_columns[name]["nullable"] for name in paper_required)
            or (("id",), True) not in paper_indexes
        ):
            _snapshot_restore_required("malformed expanded Paper shape")
        return "expanded"
    if paper_pk != ["id"]:
        _snapshot_restore_required(f"unexpected papers_metadata primary key {paper_pk!r}")
    if (
        any(paper_columns[name]["nullable"] for name in paper_required)
        or (("filename",), True) not in paper_indexes
        or (("id",), True) in paper_indexes
        or _normalized_check_expression(checks_by_name.get(
            "ck_papers_metadata_lifecycle_revision", "",
        )) != _normalized_check_expression(_LIFECYCLE_CHECK)
    ):
        _snapshot_restore_required("incomplete atomic Paper contract group")

    all_simple_present = True
    for name, table_name, referred_table, local, remote, ondelete in _SIMPLE_FOREIGN_KEYS:
        foreign_key = _foreign_keys_by_name(inspector, table_name).get(name)
        if foreign_key is None:
            all_simple_present = False
        elif not _foreign_key_matches(
            foreign_key, referred_table, local, remote, ondelete,
        ):
            _snapshot_restore_required(f"foreign key {name} has an unexpected definition")

    chunk_columns = {
        column["name"]: column for column in inspector.get_columns("papers_chunks")
    }
    chunk_nullable = tuple(
        chunk_columns[name]["nullable"]
        for name in ("paper_id", "revision_number", "chunk_index")
    )
    chunk_unique = (
        (("paper_id", "revision_number", "chunk_index"), True)
        in _index_signatures(inspector, "papers_chunks")
    )
    chunk_fk = _foreign_keys_by_name(inspector, "papers_chunks").get(
        "fk_papers_chunks_revision"
    )
    chunk_fk_complete = _foreign_key_matches(
        chunk_fk,
        "paper_revisions",
        ["paper_id", "revision_number"],
        ["paper_id", "revision_number"],
        "CASCADE",
    )
    if chunk_nullable == (True, True, True) and not chunk_unique and chunk_fk is None:
        chunk_complete = False
    elif chunk_nullable == (False, False, False) and chunk_unique and chunk_fk_complete:
        chunk_complete = True
    else:
        _snapshot_restore_required("incomplete atomic chunk contract group")
    return "relationships_contract" if all_simple_present and chunk_complete else "paper_contract"


def _persisted_ddl_phase(engine):
    with engine.connect() as connection:
        phase = connection.execute(sa.text("""
            SELECT ddl_phase FROM publishing_migration_state
            WHERE name = 'pre_backfill'
        """)).scalar()
    if phase not in _DDL_PHASES:
        _snapshot_restore_required(f"unknown persisted DDL phase {phase!r}")
    return phase


def _set_ddl_phase(engine, phase):
    if phase not in _DDL_PHASES:
        raise ValueError(f"unknown publishing DDL phase: {phase}")
    with engine.begin() as connection:
        result = connection.execute(sa.text("""
            UPDATE publishing_migration_state
            SET ddl_phase = :ddl_phase
            WHERE name = 'pre_backfill'
        """), {"ddl_phase": phase})
        if result.rowcount != 1:
            _snapshot_restore_required("pre_backfill DDL phase row is missing")


def _reconcile_ddl_phase(engine):
    actual = _mysql_schema_phase(engine)
    persisted = _persisted_ddl_phase(engine)
    actual_rank = _DDL_PHASES.index(actual)
    persisted_rank = _DDL_PHASES.index(persisted)
    if persisted == "complete" and actual == "relationships_contract":
        return "complete"
    if persisted_rank > actual_rank:
        _snapshot_restore_required(
            f"persisted phase {persisted!r} is ahead of actual shape {actual!r}"
        )
    if actual_rank > persisted_rank:
        _set_ddl_phase(engine, actual)
    return actual


def _mysql_paper_contract_group():
    op.execute(sa.text(f"""
        ALTER TABLE papers_metadata
            MODIFY COLUMN id VARCHAR(36) NOT NULL,
            MODIFY COLUMN lifecycle_state VARCHAR(16) NOT NULL,
            MODIFY COLUMN row_version INTEGER NOT NULL,
            MODIFY COLUMN index_status VARCHAR(16) NOT NULL,
            DROP PRIMARY KEY,
            ADD CONSTRAINT pk_papers_metadata PRIMARY KEY (id),
            ADD CONSTRAINT uq_papers_metadata_filename UNIQUE (filename),
            DROP INDEX ux_papers_metadata_migration_id,
            ADD CONSTRAINT ck_papers_metadata_lifecycle_revision CHECK ({_LIFECYCLE_CHECK})
    """))


def _mysql_relationship_contract_group(engine):
    for name, table_name, referred_table, local, remote, ondelete in _SIMPLE_FOREIGN_KEYS:
        inspector = sa.inspect(engine)
        existing = _foreign_keys_by_name(inspector, table_name).get(name)
        if existing is not None:
            if not _foreign_key_matches(existing, referred_table, local, remote, ondelete):
                _snapshot_restore_required(f"foreign key {name} has an unexpected definition")
            continue
        op.create_foreign_key(
            name, table_name, referred_table, local, remote, ondelete=ondelete,
        )

    inspector = sa.inspect(engine)
    chunk_columns = {
        column["name"]: column for column in inspector.get_columns("papers_chunks")
    }
    chunk_notnull = all(
        not chunk_columns[name]["nullable"]
        for name in ("paper_id", "revision_number", "chunk_index")
    )
    chunk_unique = (
        (("paper_id", "revision_number", "chunk_index"), True)
        in _index_signatures(inspector, "papers_chunks")
    )
    chunk_fk = _foreign_keys_by_name(inspector, "papers_chunks").get(
        "fk_papers_chunks_revision"
    )
    if not chunk_notnull and not chunk_unique and chunk_fk is None:
        op.execute(sa.text("""
            ALTER TABLE papers_chunks
                MODIFY COLUMN paper_id VARCHAR(36) NOT NULL,
                MODIFY COLUMN revision_number INTEGER NOT NULL,
                MODIFY COLUMN chunk_index INTEGER NOT NULL,
                ADD CONSTRAINT uq_papers_chunks_paper_revision_chunk
                    UNIQUE (paper_id, revision_number, chunk_index),
                ADD CONSTRAINT fk_papers_chunks_revision
                    FOREIGN KEY (paper_id, revision_number)
                    REFERENCES paper_revisions (paper_id, revision_number)
                    ON DELETE CASCADE
        """))
    elif not (
        chunk_notnull
        and chunk_unique
        and _foreign_key_matches(
            chunk_fk,
            "paper_revisions",
            ["paper_id", "revision_number"],
            ["paper_id", "revision_number"],
            "CASCADE",
        )
    ):
        _snapshot_restore_required("incomplete atomic chunk contract group")


def _mysql_contract(engine, papers_dir):
    phase = _reconcile_ddl_phase(engine)
    if phase == "expanded":
        validate_contract_ready(engine, papers_dir, _fenced=True)
        _mysql_paper_contract_group()
        phase = _mysql_schema_phase(engine)
        if phase != "paper_contract":
            _snapshot_restore_required("Paper contract group did not reach its exact shape")
        _set_ddl_phase(engine, phase)
    if phase == "paper_contract":
        validate_contract_ready(engine, papers_dir, _fenced=True)
        _mysql_relationship_contract_group(engine)
        phase = _mysql_schema_phase(engine)
        if phase != "relationships_contract":
            _snapshot_restore_required("relationship contract group did not reach its exact shape")
        _set_ddl_phase(engine, phase)
    if phase == "relationships_contract":
        _set_ddl_phase(engine, "complete")


def upgrade():
    bind = op.get_bind()
    configured = op.get_context().config.attributes.get("papers_dir")
    papers_dir = Path(configured) if configured is not None else PAPERS_DIR
    validation_engine = create_engine(bind.engine.url, poolclass=pool.NullPool)
    try:
        # The explicit maintenance guard is an operator assertion that every
        # app/worker writer is stopped. The durable lock serializes migration
        # invocations and remains held through validation and irreversible DDL.
        with migration_fence(validation_engine, papers_dir):
            validate_contract_ready(
                validation_engine, papers_dir, _fenced=True,
            )
            if bind.dialect.name == "sqlite":
                _sqlite_contract()
                op.execute("""
                    UPDATE publishing_migration_state
                    SET ddl_phase = 'complete'
                    WHERE name = 'pre_backfill'
                """)
            else:
                _mysql_contract(validation_engine, papers_dir)
    finally:
        validation_engine.dispose()


def downgrade():
    raise RuntimeError(
        "Publishing identity migrations require coordinated database and file snapshots; "
        "restore those snapshots instead of attempting a database-only downgrade."
    )
