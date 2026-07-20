"""Offline, resumable migration from filename identity to Paper UUID identity.

The preflight in this module is deliberately read-only.  The backfill helpers
are deliberately not part of application startup: they are invoked only by the
ordered Alembic data migration after an operator has reviewed preflight output.
"""
from __future__ import annotations

import hashlib
import fcntl
import os
import shutil
import stat
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine

from config import RAG_EMBED_DIM
from services.paper_identity import normalize_alias_key
from services.papers import resolve_contained


ISSUE_CODES = frozenset({
    "wrong_mysql_version",
    "unexpected_legacy_schema",
    "non_innodb_table",
    "non_utf8mb4_column",
    "insufficient_disk",
    "cross_device_staging",
    "missing_pdf",
    "alias_collision",
    "unresolved_filename",
    "submission_unmatched",
    "submission_ambiguous",
    "duplicate_chunk",
})
NONBLOCKING_ISSUE_CODES = frozenset({
    "submission_unmatched",
    "submission_ambiguous",
})
_LEGACY_PAPER_COLUMNS = frozenset({
    "filename", "title", "journal", "category", "language", "keywords",
    "abstract", "author_name", "author_email", "author_school", "published_at",
    "ib_ee_data", "is_ib_sample", "is_anonymous", "cp_data", "ia_data",
})
_LEGACY_CHUNK_COLUMNS = frozenset({
    "id", "filename", "chunk_index", "content", "embedding_vec", "lang",
})
_LEGACY_SUBMISSION_COLUMNS = frozenset({
    "id", "pdf_filename", "pending_filename", "title", "author_name",
    "author_email", "author_school", "status", "submitted_at", "feedback",
    "abstract", "keywords", "journal", "category", "language", "submitted_by",
    "original_filename", "ib_ee_data", "is_ib_sample", "is_anonymous", "cp_data",
    "ia_data",
})
_EXPANDED_PAPER_COLUMNS = _LEGACY_PAPER_COLUMNS | frozenset({
    "id", "lifecycle_state", "current_revision", "row_version", "index_status",
    "indexed_revision", "index_error", "direct_idempotency_key",
    "direct_payload_hash", "origin_submission_id", "reservation_expires_at",
})
_EXPANDED_CHUNK_COLUMNS = _LEGACY_CHUNK_COLUMNS | frozenset({
    "paper_id", "revision_number",
})
_EXPANDED_SUBMISSION_COLUMNS = _LEGACY_SUBMISSION_COLUMNS | frozenset({
    "paper_id", "submitter_name", "reviewed_at", "reviewer", "comment",
    "decision_idempotency_key", "decision_payload_hash",
})
_EXPANDED_TABLES = frozenset({
    "paper_revisions", "paper_filename_aliases", "publishing_jobs",
    "publishing_migration_journal", "publishing_migration_state",
    "publishing_migration_issues",
})
_EXPANDED_TABLE_COLUMNS = {
    "paper_revisions": frozenset({
        "paper_id", "revision_number", "sha256", "size_bytes", "created_at",
        "created_by", "restored_from_revision",
    }),
    "paper_filename_aliases": frozenset({
        "lookup_key", "filename", "paper_id", "created_at",
    }),
    "publishing_jobs": frozenset({
        "id", "kind", "paper_id", "revision_number", "dedupe_key", "state",
        "attempts", "available_at", "lease_token", "lease_expires_at",
        "last_error", "created_at", "updated_at",
    }),
    "publishing_migration_journal": frozenset({
        "legacy_key", "paper_id", "revision_number", "source_sha256", "source_size_bytes",
        "legacy_chunk_count", "legacy_chunk_fingerprint", "checkpoint",
        "created_at", "updated_at",
    }),
    "publishing_migration_state": frozenset({
        "name", "paper_count", "submission_count", "chunk_count", "vector_count",
        "ddl_phase", "captured_at",
    }),
    "publishing_migration_issues": frozenset({
        "id", "kind", "legacy_key", "paper_id", "details", "blocking",
        "resolved_at", "created_at", "updated_at",
    }),
}
_MIGRATION_ACTOR = "publishing-migration"
_MAINTENANCE_ENV = "PAPERQUERY_PUBLISHING_MAINTENANCE"
_CHECKPOINT_ORDER = (
    "source_verified", "copy_verified", "destination_verified", "db_complete",
)
_PROCESS_LOCKS: dict[str, threading.RLock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()
_HELD_LOCKS = threading.local()


class MigrationBlocked(RuntimeError):
    """Raised before a migration would violate a cutover invariant."""


@dataclass(frozen=True)
class MigrationIssue:
    code: str
    legacy_key: str | None
    details: str
    blocking: bool
    paper_id: str | None = None

    def __post_init__(self) -> None:
        if self.code not in ISSUE_CODES:
            raise ValueError(f"unknown publishing migration issue code: {self.code}")
        if self.blocking == (self.code in NONBLOCKING_ISSUE_CODES):
            raise ValueError(f"invalid blocking policy for issue code: {self.code}")


@dataclass(frozen=True)
class PreflightReport:
    metadata_count: int
    flat_pdf_count: int
    total_pdf_bytes: int
    submission_count: int
    accepted_submission_count: int
    pending_submission_count: int
    rejected_submission_count: int
    chunk_count: int
    vector_count: int
    importable_file_only: tuple[str, ...]
    missing_pdfs: tuple[str, ...]
    alias_collisions: tuple[tuple[str, ...], ...]
    unavailable_rejected_pdfs: tuple[str, ...]
    issues: tuple[MigrationIssue, ...]

    @property
    def blockers(self) -> tuple[MigrationIssue, ...]:
        return tuple(issue for issue in self.issues if issue.blocking)


@dataclass(frozen=True)
class BackfilledPaper:
    paper_id: str
    legacy_filename: str
    revision_number: int
    sha256: str
    size_bytes: int
    destination: str
    resumed: bool


def _now() -> str:
    # The application contract stores naive UTC timestamps in MySQL DATETIME.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _issue(code: str, legacy_key: str | None, details: str,
           paper_id: str | None = None) -> MigrationIssue:
    return MigrationIssue(
        code=code,
        legacy_key=legacy_key,
        details=details,
        blocking=code not in NONBLOCKING_ISSUE_CODES,
        paper_id=paper_id,
    )


def _table_names(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names())


def _columns(engine: Engine, table_name: str) -> set[str]:
    if table_name not in _table_names(engine):
        return set()
    return {column["name"] for column in inspect(engine).get_columns(table_name)}


def _rows(engine: Engine, statement: str, parameters: dict | None = None):
    with engine.connect() as connection:
        return connection.execute(text(statement), parameters or {}).mappings().all()


def _scalar(engine: Engine, statement: str, parameters: dict | None = None,
            default=0):
    with engine.connect() as connection:
        value = connection.execute(text(statement), parameters or {}).scalar()
    return default if value is None else value


def _hash_file(path: Path) -> tuple[str, int]:
    """Compatibility helper for non-migration callers; migration I/O uses FDs."""
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)


def _policy_legacy_name(papers_dir: Path, legacy_filename: str) -> bool:
    """Apply the shared containment policy before descriptor-level validation."""
    if (
        not isinstance(legacy_filename, str)
        or not legacy_filename
        or "\\" in legacy_filename
        or any(ord(character) < 32 for character in legacy_filename)
        or Path(legacy_filename).name != legacy_filename
        or Path(legacy_filename).is_absolute()
        or Path(legacy_filename).suffix.casefold() != ".pdf"
    ):
        return False
    try:
        return resolve_contained(papers_dir, legacy_filename, must_exist=False) is not None
    except (OSError, RuntimeError, ValueError):
        return False


@contextmanager
def _trusted_root(papers_dir: Path):
    try:
        root_fd = os.open(os.fspath(papers_dir), _DIRECTORY_FLAGS)
    except OSError as exc:
        raise MigrationBlocked(f"Paper storage root is unsafe: {papers_dir}") from exc
    try:
        yield root_fd
    finally:
        os.close(root_fd)


def _after_source_lstat(_legacy_filename: str) -> None:
    """Test seam immediately before the no-follow source open."""


def _after_target_directory_opened(_paper_id: str) -> None:
    """Test seam after target directory FD acquisition."""


def _hash_fd(file_fd: int) -> tuple[str, int]:
    os.lseek(file_fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = os.read(file_fd, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _open_regular_at(
    directory_fd: int,
    name: str,
    *,
    source_hook: bool = False,
) -> tuple[int | None, os.stat_result | None, str]:
    """Open one direct regular entry without following or trusting pathnames."""
    try:
        before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None, None, "missing"
    except OSError:
        return None, None, "unresolved"
    if not stat.S_ISREG(before.st_mode):
        return None, None, "unresolved"
    if source_hook:
        _after_source_lstat(name)
    try:
        file_fd = os.open(name, _READ_FLAGS, dir_fd=directory_fd)
    except OSError:
        return None, None, "unresolved"
    after = os.fstat(file_fd)
    if (
        not stat.S_ISREG(after.st_mode)
        or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
    ):
        os.close(file_fd)
        return None, None, "unresolved"
    return file_fd, after, "ok"


def _source_details_at(
    papers_dir: Path,
    root_fd: int,
    legacy_filename: str,
    *,
    source_hook: bool = False,
) -> tuple[str, int, tuple[int, int]] | None:
    if not _policy_legacy_name(papers_dir, legacy_filename):
        return None
    file_fd, opened, classification = _open_regular_at(
        root_fd, legacy_filename, source_hook=source_hook,
    )
    if file_fd is None or opened is None or classification != "ok":
        return None
    try:
        source_hash, source_size = _hash_fd(file_fd)
        final = os.fstat(file_fd)
    finally:
        os.close(file_fd)
    if (
        (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        != (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns)
        or final.st_size != source_size
    ):
        return None
    return source_hash, source_size, (final.st_dev, final.st_ino)


def _resolved_regular_pdf(papers_dir: Path, legacy_filename: str) -> tuple[Path | None, str]:
    """Classify a direct source through a trusted root FD."""
    if not _policy_legacy_name(papers_dir, legacy_filename):
        return None, "unresolved"
    with _trusted_root(papers_dir) as root_fd:
        file_fd, _opened, classification = _open_regular_at(root_fd, legacy_filename)
        if file_fd is not None:
            os.close(file_fd)
    if classification != "ok":
        return None, classification
    return papers_dir / legacy_filename, "ok"


def _safe_source_details(papers_dir: Path, legacy_filename: str) -> tuple[Path, str, int]:
    with _trusted_root(papers_dir) as root_fd:
        details = _source_details_at(
            papers_dir, root_fd, legacy_filename, source_hook=True,
        )
    if details is None:
        raise MigrationBlocked(
            f"legacy PDF {legacy_filename!r} is missing, unsafe, changed, or not a regular PDF"
        )
    source_hash, source_size, _identity = details
    return papers_dir / legacy_filename, source_hash, source_size


def _flat_pdf_inventory(
    papers_dir: Path,
) -> tuple[dict[str, tuple[str, int, tuple[int, int]]], tuple[str, ...]]:
    safe: dict[str, tuple[str, int, tuple[int, int]]] = {}
    unsafe: list[str] = []
    with _trusted_root(papers_dir) as root_fd:
        names = sorted(entry.name for entry in os.scandir(root_fd))
        for name in names:
            if name in {
                ".publishing-migration-stage", ".publishing-migration-locks",
            } or Path(name).suffix.casefold() != ".pdf":
                continue
            details = _source_details_at(papers_dir, root_fd, name)
            if details is None:
                unsafe.append(name)
            else:
                safe[name] = details
    return safe, tuple(unsafe)


def _legacy_metadata_keys(engine: Engine) -> tuple[str, ...]:
    if "filename" not in _columns(engine, "papers_metadata"):
        return ()
    return tuple(
        row["filename"]
        for row in _rows(
            engine,
            "SELECT filename FROM papers_metadata WHERE filename IS NOT NULL ORDER BY filename",
        )
    )


def _duplicate_chunk_issues(
    engine: Engine, *, final_key: bool = False,
) -> tuple[MigrationIssue, ...]:
    columns = _columns(engine, "papers_chunks")
    if not {"filename", "chunk_index"}.issubset(columns):
        return ()
    if final_key:
        if not {"paper_id", "revision_number"}.issubset(columns):
            return ()
        paper_expression = "paper_id"
        revision_expression = "revision_number"
        legacy_expression = "MIN(filename)"
        group_expression = "paper_id, revision_number, chunk_index"
    else:
        paper_expression = "filename"
        revision_expression = "1"
        legacy_expression = "filename"
        group_expression = "filename, chunk_index"
    duplicates = _rows(engine, f"""
        SELECT {paper_expression} AS owner_key,
               {legacy_expression} AS legacy_key,
               {revision_expression} AS revision_number,
               chunk_index,
               COUNT(*) AS duplicate_count
        FROM papers_chunks
        GROUP BY {group_expression}
        HAVING COUNT(*) > 1
        ORDER BY legacy_key, revision_number, chunk_index
    """)
    return tuple(
        _issue(
            "duplicate_chunk",
            row["legacy_key"],
            f"duplicate chunk key owner={row['owner_key']!r}, revision={row['revision_number']}, index={row['chunk_index']}",
        )
        for row in duplicates
    )


def _submission_summary(engine: Engine, known_filenames: set[str]):
    columns = _columns(engine, "submissions")
    if not {"id", "status"}.issubset(columns):
        return 0, 0, 0, 0, (), (), ()
    candidate_columns = [
        name for name in ("pdf_filename", "pending_filename", "original_filename")
        if name in columns
    ]
    selected = ["id", "status"] + candidate_columns
    if "paper_id" in columns:
        selected.append("paper_id")
    rows = _rows(engine, f"SELECT {', '.join(selected)} FROM submissions ORDER BY id")
    accepted = pending = rejected = 0
    issues: list[MigrationIssue] = []
    unavailable: list[str] = []
    for row in rows:
        status = (row["status"] or "").casefold()
        if status == "accepted":
            accepted += 1
            candidates = {
                row[column]
                for column in candidate_columns
                if row[column] and row[column] in known_filenames
            }
            if not candidates:
                issues.append(_issue(
                    "submission_unmatched", row["id"],
                    "accepted Submission has no exact nonempty legacy filename match",
                ))
            elif len(candidates) > 1:
                issues.append(_issue(
                    "submission_ambiguous", row["id"],
                    "accepted Submission has multiple exact legacy filename matches",
                ))
        elif status in {"pending", "draft"}:
            pending += 1
        elif status == "rejected":
            rejected += 1
            # Legacy rejection deleted the private PDF.  Filename equality
            # with an unrelated published Paper cannot make that historic
            # private artifact available again.
            unavailable.append(row["id"])
    return (
        len(rows), accepted, pending, rejected, tuple(issues),
        tuple(sorted(unavailable)), tuple(rows),
    )


def _mysql_preflight_issues(
    engine: Engine, *, allow_contract_recovery: bool = False,
) -> tuple[MigrationIssue, ...]:
    if engine.dialect.name != "mysql":
        return ()
    issues: list[MigrationIssue] = []
    version = str(_scalar(engine, "SELECT VERSION()", default=""))
    try:
        major = int(version.split(".", 1)[0])
    except ValueError:
        major = -1
    if major != 9:
        issues.append(_issue(
            "wrong_mysql_version", None,
            f"MySQL 9 is required; server reports {version!r}",
        ))

    requirements = {
        "papers_metadata": _LEGACY_PAPER_COLUMNS,
        "papers_chunks": _LEGACY_CHUNK_COLUMNS,
        "submissions": _LEGACY_SUBMISSION_COLUMNS,
    }
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    actual_core_columns: dict[str, set[str]] = {}
    for table_name, expected_columns in requirements.items():
        actual = (
            {column["name"] for column in inspector.get_columns(table_name)}
            if table_name in table_names else set()
        )
        actual_core_columns[table_name] = actual
        missing = sorted(expected_columns - actual)
        if missing:
            issues.append(_issue(
                "unexpected_legacy_schema", table_name,
                f"missing required legacy columns: {', '.join(missing)}",
            ))
    baseline_shapes = {
        "papers_metadata": set(_LEGACY_PAPER_COLUMNS),
        "papers_chunks": set(_LEGACY_CHUNK_COLUMNS),
        "submissions": set(_LEGACY_SUBMISSION_COLUMNS),
    }
    expanded_shapes = {
        "papers_metadata": set(_EXPANDED_PAPER_COLUMNS),
        "papers_chunks": set(_EXPANDED_CHUNK_COLUMNS),
        "submissions": set(_EXPANDED_SUBMISSION_COLUMNS),
    }
    is_baseline = actual_core_columns == baseline_shapes
    is_expanded = actual_core_columns == expanded_shapes
    migration_tables = table_names & set(_EXPANDED_TABLES)
    paper_primary_key = (
        inspector.get_pk_constraint("papers_metadata").get("constrained_columns")
        if "papers_metadata" in table_names else []
    )
    recoverable_contract_shape = (
        allow_contract_recovery
        and is_expanded
        and paper_primary_key in (["filename"], ["id"])
    )
    if (
        not (is_baseline or is_expanded)
        or (is_baseline and migration_tables)
        or (is_expanded and migration_tables != set(_EXPANDED_TABLES))
        or (allow_contract_recovery and is_expanded and not recoverable_contract_shape)
    ):
        issues.append(_issue(
            "unexpected_legacy_schema", "publishing_schema_phase",
            "publishing tables are neither the exact legacy baseline nor the exact expanded shape",
        ))
    if is_expanded:
        for table_name, expected_columns in _EXPANDED_TABLE_COLUMNS.items():
            actual_columns = (
                {column["name"] for column in inspector.get_columns(table_name)}
                if table_name in table_names else set()
            )
            if actual_columns != set(expected_columns):
                issues.append(_issue(
                    "unexpected_legacy_schema", table_name,
                    "expanded table columns do not match the migration contract",
                ))

    rag_columns = (
        {column["name"]: column for column in inspector.get_columns("rag_index_meta")}
        if "rag_index_meta" in table_names else {}
    )
    if set(rag_columns) != {"name", "value"}:
        issues.append(_issue(
            "unexpected_legacy_schema", "rag_index_meta",
            "rag_index_meta must have exactly the singleton name/value shape",
        ))
    else:
        rag_name_type = str(rag_columns["name"]["type"]).casefold().replace(" ", "")
        rag_value_type = str(rag_columns["value"]["type"]).casefold().replace(" ", "")
        if (
            inspector.get_pk_constraint("rag_index_meta").get("constrained_columns") != ["name"]
            or rag_columns["name"]["nullable"]
            or rag_columns["value"]["nullable"]
            or rag_name_type != "varchar(32)"
            or not rag_value_type.startswith(("int", "integer"))
        ):
            issues.append(_issue(
                "unexpected_legacy_schema", "rag_index_meta",
                "rag_index_meta requires VARCHAR(32) primary key name and non-null INTEGER value",
            ))
        unexpected_rag_rows = int(_scalar(engine, """
            SELECT COUNT(*) FROM rag_index_meta WHERE name <> 'chunks_version'
        """))
        if unexpected_rag_rows:
            issues.append(_issue(
                "unexpected_legacy_schema", "rag_index_meta",
                "rag_index_meta contains keys other than the chunks_version singleton",
            ))

    if is_expanded:
        infrastructure_primary_keys = {
            "paper_revisions": ["paper_id", "revision_number"],
            "paper_filename_aliases": ["lookup_key"],
            "publishing_jobs": ["id"],
            "publishing_migration_journal": ["legacy_key"],
            "publishing_migration_state": ["name"],
            "publishing_migration_issues": ["id"],
        }
        for table_name, expected_key in infrastructure_primary_keys.items():
            actual_key = inspector.get_pk_constraint(table_name).get("constrained_columns")
            if actual_key != expected_key:
                issues.append(_issue(
                    "unexpected_legacy_schema", table_name,
                    f"expanded primary key must be {expected_key!r}, found {actual_key!r}",
                ))

        required_indexes = {
            "papers_metadata": {
                (("direct_idempotency_key",), True),
                (("origin_submission_id",), True),
            },
            "papers_chunks": {(('paper_id',), False)},
            "submissions": {
                (("paper_id",), False),
                (("decision_idempotency_key",), True),
            },
            "paper_filename_aliases": {(('paper_id',), False)},
            "publishing_jobs": {
                (("paper_id",), False),
                (("dedupe_key",), True),
            },
            "publishing_migration_journal": {(('paper_id',), True)},
            "publishing_migration_issues": {(('paper_id',), False)},
        }
        if paper_primary_key == ["filename"]:
            required_indexes["papers_metadata"].add((("id",), True))
        else:
            required_indexes["papers_metadata"].add((("filename",), True))
        for table_name, expected_indexes in required_indexes.items():
            actual_indexes = {
                (tuple(index.get("column_names") or ()), bool(index.get("unique")))
                for index in inspector.get_indexes(table_name)
            }
            actual_indexes.update({
                (tuple(unique.get("column_names") or ()), True)
                for unique in inspector.get_unique_constraints(table_name)
            })
            missing_indexes = expected_indexes - actual_indexes
            if missing_indexes:
                issues.append(_issue(
                    "unexpected_legacy_schema", table_name,
                    "expanded indexes/uniques are missing: " + repr(sorted(missing_indexes)),
                ))

        required_named_indexes = {
            "papers_metadata": {
                "uq_papers_metadata_direct_idempotency_key": (
                    ("direct_idempotency_key",), True,
                ),
                "uq_papers_metadata_origin_submission_id": (
                    ("origin_submission_id",), True,
                ),
            },
            "papers_chunks": {
                "ix_papers_chunks_paper_id": (("paper_id",), False),
            },
            "submissions": {
                "ix_submissions_paper_id": (("paper_id",), False),
                "uq_submissions_decision_idempotency_key": (
                    ("decision_idempotency_key",), True,
                ),
            },
            "paper_filename_aliases": {
                "ix_paper_filename_aliases_paper_id": (("paper_id",), False),
            },
            "publishing_jobs": {
                "ix_publishing_jobs_paper_id": (("paper_id",), False),
                "uq_publishing_jobs_dedupe_key": (("dedupe_key",), True),
            },
            "publishing_migration_journal": {
                "uq_publishing_migration_journal_paper_id": (("paper_id",), True),
            },
            "publishing_migration_issues": {
                "ix_publishing_migration_issues_paper_id": (("paper_id",), False),
            },
        }
        if paper_primary_key == ["filename"]:
            required_named_indexes["papers_metadata"][
                "ux_papers_metadata_migration_id"
            ] = (("id",), True)
        else:
            required_named_indexes["papers_metadata"][
                "uq_papers_metadata_filename"
            ] = (("filename",), True)
        for table_name, expected_by_name in required_named_indexes.items():
            actual_by_name = {
                index.get("name"): (
                    tuple(index.get("column_names") or ()),
                    bool(index.get("unique")),
                )
                for index in inspector.get_indexes(table_name)
            }
            actual_by_name.update({
                unique.get("name"): (
                    tuple(unique.get("column_names") or ()), True,
                )
                for unique in inspector.get_unique_constraints(table_name)
            })
            named_drift = {
                name: (expected, actual_by_name.get(name))
                for name, expected in expected_by_name.items()
                if actual_by_name.get(name) != expected
            }
            if named_drift:
                issues.append(_issue(
                    "unexpected_legacy_schema", table_name,
                    "expanded named index contract drift: " + repr(named_drift),
                ))

        required_nonnull = {
            "paper_revisions": {
                "paper_id", "revision_number", "sha256", "size_bytes",
                "created_at", "created_by",
            },
            "paper_filename_aliases": {"lookup_key", "filename", "paper_id", "created_at"},
            "publishing_jobs": {
                "id", "kind", "paper_id", "revision_number", "dedupe_key",
                "state", "attempts", "available_at", "created_at", "updated_at",
            },
            "publishing_migration_journal": {
                "legacy_key", "paper_id", "revision_number", "legacy_chunk_count",
                "checkpoint", "created_at", "updated_at",
            },
            "publishing_migration_state": {
                "name", "paper_count", "submission_count", "chunk_count",
                "vector_count", "ddl_phase", "captured_at",
            },
            "publishing_migration_issues": {
                "id", "kind", "details", "blocking", "created_at", "updated_at",
            },
        }
        for table_name, nonnull_columns in required_nonnull.items():
            definitions_by_name = {
                column["name"]: column for column in inspector.get_columns(table_name)
            }
            nullable_drift = sorted(
                name for name in nonnull_columns
                if definitions_by_name.get(name, {}).get("nullable", True)
            )
            if nullable_drift:
                issues.append(_issue(
                    "unexpected_legacy_schema", table_name,
                    "expanded non-null contract drift: " + ", ".join(nullable_drift),
                ))

        expanded_type_contracts = {
            "paper_revisions": {
                "paper_id": ("string", 36), "revision_number": ("integer", None),
                "sha256": ("string", 64), "size_bytes": ("integer", None),
                "created_at": ("datetime", None), "created_by": ("string", 255),
                "restored_from_revision": ("integer", None),
            },
            "paper_filename_aliases": {
                "lookup_key": ("string", 255), "filename": ("string", 255),
                "paper_id": ("string", 36), "created_at": ("datetime", None),
            },
            "publishing_jobs": {
                "id": ("string", 36), "kind": ("string", 32),
                "paper_id": ("string", 36), "revision_number": ("integer", None),
                "dedupe_key": ("string", 255), "state": ("string", 16),
                "attempts": ("integer", None), "available_at": ("datetime", None),
                "lease_token": ("string", 36), "lease_expires_at": ("datetime", None),
                "last_error": ("text", None), "created_at": ("datetime", None),
                "updated_at": ("datetime", None),
            },
            "publishing_migration_journal": {
                "legacy_key": ("string", 255), "paper_id": ("string", 36),
                "revision_number": ("integer", None), "source_sha256": ("string", 64),
                "source_size_bytes": ("integer", None),
                "legacy_chunk_count": ("integer", None),
                "legacy_chunk_fingerprint": ("string", 64),
                "checkpoint": ("string", 32), "created_at": ("datetime", None),
                "updated_at": ("datetime", None),
            },
            "publishing_migration_state": {
                "name": ("string", 32), "paper_count": ("integer", None),
                "submission_count": ("integer", None), "chunk_count": ("integer", None),
                "vector_count": ("integer", None), "ddl_phase": ("string", 32),
                "captured_at": ("datetime", None),
            },
            "publishing_migration_issues": {
                "id": ("string", 36), "kind": ("string", 32),
                "legacy_key": ("string", 255), "paper_id": ("string", 36),
                "details": ("text", None), "blocking": ("boolean", None),
                "resolved_at": ("datetime", None), "created_at": ("datetime", None),
                "updated_at": ("datetime", None),
            },
        }
        expanded_nullable = {
            "paper_revisions": {"restored_from_revision"},
            "paper_filename_aliases": set(),
            "publishing_jobs": {"lease_token", "lease_expires_at", "last_error"},
            "publishing_migration_journal": {
                "source_sha256", "source_size_bytes", "legacy_chunk_fingerprint",
            },
            "publishing_migration_state": set(),
            "publishing_migration_issues": {"legacy_key", "paper_id", "resolved_at"},
        }

        def reflected_kind(column):
            rendered = str(column["type"]).casefold().replace(" ", "")
            if rendered.startswith("varchar("):
                return "string", getattr(column["type"], "length", None)
            if rendered in {"text", "mediumtext", "longtext"}:
                return "text", None
            if rendered.startswith(("int", "integer", "bigint")):
                return "integer", None
            if rendered.startswith(("datetime", "timestamp")):
                return "datetime", None
            if (
                rendered in {"bool", "boolean", "tinyint(1)"}
                or (
                    rendered.startswith("tinyint")
                    and getattr(column["type"], "display_width", None) == 1
                )
            ):
                return "boolean", None
            return rendered, getattr(column["type"], "length", None)

        for table_name, expected_types in expanded_type_contracts.items():
            definitions_by_name = {
                column["name"]: column for column in inspector.get_columns(table_name)
            }
            drift: list[str] = []
            for column_name, expected_type in expected_types.items():
                actual_column = definitions_by_name[column_name]
                if reflected_kind(actual_column) != expected_type:
                    drift.append(
                        f"{column_name} type {reflected_kind(actual_column)!r} != {expected_type!r}"
                    )
                expected_is_nullable = column_name in expanded_nullable[table_name]
                if bool(actual_column["nullable"]) != expected_is_nullable:
                    drift.append(
                        f"{column_name} nullable={actual_column['nullable']!r}"
                    )
            if drift:
                issues.append(_issue(
                    "unexpected_legacy_schema", table_name,
                    "expanded type/nullability contract drift: " + "; ".join(drift),
                ))
        alias_lookup_collation = str(_scalar(engine, """
            SELECT COLLATION_NAME FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'paper_filename_aliases'
              AND COLUMN_NAME = 'lookup_key'
        """, default="")).casefold()
        if alias_lookup_collation != "utf8mb4_bin":
            issues.append(_issue(
                "unexpected_legacy_schema", "paper_filename_aliases.lookup_key",
                f"expanded alias lookup collation must be utf8mb4_bin, found {alias_lookup_collation!r}",
            ))

    expected_primary_keys = {
        "papers_metadata": ["filename"],
        "papers_chunks": ["id"],
        "submissions": ["id"],
    }
    for table_name, expected_key in expected_primary_keys.items():
        if table_name in table_names:
            primary_key = inspector.get_pk_constraint(table_name)
            actual_key = primary_key.get("constrained_columns")
            valid_keys = (
                (["filename"], ["id"])
                if allow_contract_recovery and table_name == "papers_metadata" and is_expanded
                else (expected_key,)
            )
            if actual_key not in valid_keys:
                issues.append(_issue(
                    "unexpected_legacy_schema", table_name,
                    f"expected primary key {expected_key!r} before contraction",
                ))

    column_contracts = {
        ("papers_metadata", "filename"): ("varchar(255)", "NO"),
        **{
            ("papers_metadata", name): ("varchar(255)", "YES")
            for name in (
                "title", "journal", "category", "language", "author_name",
                "author_email", "author_school", "published_at",
            )
        },
        **{
            ("papers_metadata", name): ("varchar(10)", "YES")
            for name in ("is_ib_sample", "is_anonymous")
        },
        **{
            ("papers_metadata", name): ("text", "YES")
            for name in ("keywords", "abstract", "ib_ee_data", "cp_data", "ia_data")
        },
        ("papers_chunks", "id"): ("int", "NO"),
        ("papers_chunks", "filename"): ("varchar(255)", "YES"),
        ("papers_chunks", "chunk_index"): ("int", "YES"),
        ("papers_chunks", "content"): ("text", "YES"),
        ("papers_chunks", "lang"): ("varchar(10)", "YES"),
        ("submissions", "id"): ("varchar(255)", "NO"),
        **{
            ("submissions", name): ("varchar(255)", "YES")
            for name in (
                "pdf_filename", "pending_filename", "title", "author_name",
                "author_email", "author_school", "submitted_at", "journal",
                "category", "language", "submitted_by", "original_filename",
            )
        },
        ("submissions", "status"): ("varchar(50)", "YES"),
        **{
            ("submissions", name): ("varchar(10)", "YES")
            for name in ("is_ib_sample", "is_anonymous")
        },
        **{
            ("submissions", name): ("text", "YES")
            for name in (
                "feedback", "abstract", "keywords", "ib_ee_data", "cp_data", "ia_data",
            )
        },
    }
    if is_expanded:
        paper_contract = paper_primary_key == ["id"]
        column_contracts.update({
            ("papers_metadata", "id"): ("varchar(36)", "NO" if paper_contract else "YES"),
            ("papers_metadata", "lifecycle_state"): ("varchar(16)", "NO" if paper_contract else "YES"),
            ("papers_metadata", "current_revision"): ("int", "YES"),
            ("papers_metadata", "row_version"): ("int", "NO" if paper_contract else "YES"),
            ("papers_metadata", "index_status"): ("varchar(16)", "NO" if paper_contract else "YES"),
            ("papers_metadata", "indexed_revision"): ("int", "YES"),
            ("papers_metadata", "index_error"): ("text", "YES"),
            ("papers_metadata", "direct_idempotency_key"): ("varchar(255)", "YES"),
            ("papers_metadata", "direct_payload_hash"): ("varchar(64)", "YES"),
            ("papers_metadata", "origin_submission_id"): ("varchar(255)", "YES"),
            ("papers_metadata", "reservation_expires_at"): ("datetime", "YES"),
            ("submissions", "paper_id"): ("varchar(36)", "YES"),
            ("submissions", "submitter_name"): ("varchar(255)", "YES"),
            ("submissions", "reviewed_at"): ("datetime", "YES"),
            ("submissions", "reviewer"): ("varchar(255)", "YES"),
            ("submissions", "comment"): ("text", "YES"),
            ("submissions", "decision_idempotency_key"): ("varchar(255)", "YES"),
            ("submissions", "decision_payload_hash"): ("varchar(64)", "YES"),
        })
        if not allow_contract_recovery:
            column_contracts.update({
                ("papers_chunks", "paper_id"): ("varchar(36)", "YES"),
                ("papers_chunks", "revision_number"): ("int", "YES"),
            })
        else:
            column_contracts.pop(("papers_chunks", "chunk_index"), None)
    definition_rows = _rows(engine, """
        SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name,
               COLUMN_TYPE AS column_type, IS_NULLABLE AS is_nullable
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME IN ('papers_metadata', 'papers_chunks', 'submissions')
    """)
    definitions = {
        (row["table_name"], row["column_name"]): (
            str(row["column_type"]).casefold(), str(row["is_nullable"]).upper(),
        )
        for row in definition_rows
    }
    for key, expected_definition in column_contracts.items():
        actual_definition = definitions.get(key)
        if actual_definition != expected_definition:
            issues.append(_issue(
                "unexpected_legacy_schema", f"{key[0]}.{key[1]}",
                f"expected definition {expected_definition!r}, found {actual_definition!r}",
            ))

    vector_type = _scalar(engine, """
        SELECT COLUMN_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'papers_chunks'
          AND COLUMN_NAME = 'embedding_vec'
    """, default="")
    if str(vector_type).casefold().replace(" ", "") != f"vector({RAG_EMBED_DIM})":
        issues.append(_issue(
            "unexpected_legacy_schema", "papers_chunks.embedding_vec",
            f"expected VECTOR({RAG_EMBED_DIM}), found {vector_type!r}",
        ))

    table_rows = _rows(engine, """
        SELECT TABLE_NAME AS table_name, ENGINE AS engine_name
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """)
    for row in table_rows:
        if (row["engine_name"] or "").casefold() != "innodb":
            issues.append(_issue(
                "non_innodb_table", row["table_name"],
                f"table engine is {row['engine_name']!r}, not InnoDB",
            ))

    column_rows = _rows(engine, """
        SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name,
               COLLATION_NAME AS collation_name
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND COLLATION_NAME IS NOT NULL
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """)
    for row in column_rows:
        if not (row["collation_name"] or "").casefold().startswith("utf8mb4_"):
            key = f"{row['table_name']}.{row['column_name']}"
            issues.append(_issue(
                "non_utf8mb4_column", key,
                f"column collation is {row['collation_name']!r}, not utf8mb4",
            ))

    identity_rows = _rows(engine, """
        SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name,
               COLUMN_TYPE AS column_type,
               CHARACTER_SET_NAME AS character_set_name,
               COLLATION_NAME AS collation_name
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND (
              (TABLE_NAME = 'papers_metadata' AND COLUMN_NAME = 'id') OR
              (TABLE_NAME IN (
                  'paper_revisions', 'paper_filename_aliases', 'publishing_jobs',
                  'publishing_migration_journal', 'publishing_migration_issues',
                  'submissions', 'papers_chunks'
              ) AND COLUMN_NAME = 'paper_id')
          )
        ORDER BY TABLE_NAME, COLUMN_NAME
    """)
    identity_by_key = {
        (row["table_name"], row["column_name"]): row for row in identity_rows
    }
    parent_identity = identity_by_key.get(("papers_metadata", "id"))
    if parent_identity is not None:
        expected_identity_keys = {
            ("papers_metadata", "id"),
            ("paper_revisions", "paper_id"),
            ("paper_filename_aliases", "paper_id"),
            ("publishing_jobs", "paper_id"),
            ("publishing_migration_journal", "paper_id"),
            ("publishing_migration_issues", "paper_id"),
            ("submissions", "paper_id"),
            ("papers_chunks", "paper_id"),
        }
        missing_identity_keys = expected_identity_keys - set(identity_by_key)
        if missing_identity_keys:
            issues.append(_issue(
                "unexpected_legacy_schema", "paper_identity_columns",
                "expanded Paper identity columns are missing: "
                + ", ".join(f"{table}.{column}" for table, column in sorted(missing_identity_keys)),
            ))
        expected_identity = (
            str(parent_identity["column_type"]).casefold(),
            str(parent_identity["character_set_name"]).casefold(),
            str(parent_identity["collation_name"]).casefold(),
        )
        for key, row in identity_by_key.items():
            actual_identity = (
                str(row["column_type"]).casefold(),
                str(row["character_set_name"]).casefold(),
                str(row["collation_name"]).casefold(),
            )
            if actual_identity != expected_identity:
                issues.append(_issue(
                    "unexpected_legacy_schema", f"{key[0]}.{key[1]}",
                    "Paper identity type/charset/collation does not match papers_metadata.id",
                ))
    else:
        # Before 0001, predict the inherited collations of its identity
        # columns: legacy ALTERs inherit their table default, while new child
        # tables inherit the database default.
        collation_rows = _rows(engine, """
            SELECT TABLE_NAME AS table_name, TABLE_COLLATION AS table_collation
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME IN ('papers_metadata', 'papers_chunks', 'submissions')
            UNION ALL
            SELECT '<database>' AS table_name,
                   DEFAULT_COLLATION_NAME AS table_collation
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME = DATABASE()
        """)
        inherited_collations = {
            str(row["table_collation"]).casefold() for row in collation_rows
        }
        if len(inherited_collations) > 1:
            issues.append(_issue(
                "unexpected_legacy_schema", "paper_identity_collation",
                "legacy/database collations would create incompatible Paper identity foreign keys",
            ))
    return tuple(issues)


def run_preflight(
    engine: Engine,
    papers_dir: Path,
    *,
    capacity_phase: str = "initial",
    allow_contract_recovery: bool = False,
) -> PreflightReport:
    """Inventory legacy SQL/files without modifying either storage system."""
    papers_dir = Path(papers_dir)
    if capacity_phase not in {"initial", "contract"}:
        raise ValueError(f"unknown publishing migration capacity phase: {capacity_phase}")
    try:
        with _trusted_root(papers_dir):
            pass
    except MigrationBlocked as exc:
        raise ValueError(f"papers directory does not exist or is unsafe: {papers_dir}") from exc

    metadata_keys = _legacy_metadata_keys(engine)
    flat_pdfs, unsafe_flat_pdfs = _flat_pdf_inventory(papers_dir)
    issues: list[MigrationIssue] = list(_mysql_preflight_issues(
        engine, allow_contract_recovery=allow_contract_recovery,
    ))
    issue_keys: set[tuple[str, str | None]] = {
        (issue.code, issue.legacy_key) for issue in issues
    }
    missing: list[str] = []
    safe_metadata: set[str] = set()
    safe_details = dict(flat_pdfs)

    for legacy_key in metadata_keys:
        if legacy_key in flat_pdfs:
            safe_metadata.add(legacy_key)
            continue
        _resolved, classification = _resolved_regular_pdf(papers_dir, legacy_key)
        if classification != "ok":
            if classification == "missing":
                missing.append(legacy_key)
                if ("missing_pdf", legacy_key) not in issue_keys:
                    issues.append(_issue(
                        "missing_pdf", legacy_key,
                        "metadata row has no contained regular legacy PDF",
                    ))
                    issue_keys.add(("missing_pdf", legacy_key))
            else:
                if ("unresolved_filename", legacy_key) not in issue_keys:
                    issues.append(_issue(
                        "unresolved_filename", legacy_key,
                        "legacy filename is nested, escaping, symlinked, changed, or non-regular",
                    ))
                    issue_keys.add(("unresolved_filename", legacy_key))
            continue

    for filename in unsafe_flat_pdfs:
        if ("unresolved_filename", filename) in issue_keys:
            continue
        issues.append(_issue(
            "unresolved_filename", filename,
            "direct flat PDF entry is symlinked, missing, changed, or non-regular",
        ))
        issue_keys.add(("unresolved_filename", filename))

    alias_groups: dict[str, list[str]] = {}
    for filename in sorted(set(safe_details)):
        lookup_key = normalize_alias_key(filename)
        alias_groups.setdefault(lookup_key, []).append(filename)
        if len(lookup_key) > 255:
            issues.append(_issue(
                "alias_collision", filename,
                "normalized alias exceeds the 255-character storage limit",
            ))
    collisions = tuple(
        tuple(sorted(filenames))
        for _lookup, filenames in sorted(alias_groups.items())
        if len(filenames) > 1
    )
    for filenames in collisions:
        issues.append(_issue(
            "alias_collision", filenames[0],
            "filenames normalize to the same alias: " + ", ".join(repr(name) for name in filenames),
        ))

    issues.extend(_duplicate_chunk_issues(engine))
    known_filenames = set(safe_metadata) | set(flat_pdfs)
    (
        submission_count,
        accepted_submission_count,
        pending_submission_count,
        rejected_submission_count,
        submission_issues,
        unavailable_rejected_pdfs,
        _submission_rows,
    ) = _submission_summary(engine, known_filenames)
    issues.extend(submission_issues)

    total_pdf_bytes = sum(size for _digest, size, _identity in safe_details.values())

    with _trusted_root(papers_dir) as root_fd:
        root_stat = os.fstat(root_fd)
        try:
            stage_lstat = os.stat(
                ".publishing-migration-stage",
                dir_fd=root_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            stage_lstat = None
        except OSError:
            stage_lstat = False
        if stage_lstat is False or (
            stage_lstat is not None and not stat.S_ISDIR(stage_lstat.st_mode)
        ):
            issues.append(_issue(
                "cross_device_staging", ".publishing-migration-stage",
                "staging path must be a no-follow directory",
            ))
        elif stage_lstat is not None:
            try:
                stage_fd = os.open(
                    ".publishing-migration-stage", _DIRECTORY_FLAGS, dir_fd=root_fd,
                )
            except OSError:
                issues.append(_issue(
                    "cross_device_staging", ".publishing-migration-stage",
                    "staging path changed or could not be opened without following links",
                ))
            else:
                try:
                    opened_stage = os.fstat(stage_fd)
                    if (
                        (stage_lstat.st_dev, stage_lstat.st_ino)
                        != (opened_stage.st_dev, opened_stage.st_ino)
                        or root_stat.st_dev != opened_stage.st_dev
                    ):
                        issues.append(_issue(
                            "cross_device_staging", ".publishing-migration-stage",
                            "staging directory changed or is on a different device",
                        ))
                finally:
                    os.close(stage_fd)

    free_bytes = shutil.disk_usage(papers_dir).free
    required_bytes = total_pdf_bytes * 2 if capacity_phase == "initial" else 0
    if free_bytes < required_bytes:
        issues.append(_issue(
            "insufficient_disk", None,
            f"need at least {required_bytes} free bytes, found {free_bytes}",
        ))

    chunk_count = 0
    vector_count = 0
    chunk_columns = _columns(engine, "papers_chunks")
    if chunk_columns:
        chunk_count = int(_scalar(engine, "SELECT COUNT(*) FROM papers_chunks"))
        if "embedding_vec" in chunk_columns:
            vector_count = int(_scalar(
                engine,
                "SELECT COUNT(*) FROM papers_chunks WHERE embedding_vec IS NOT NULL",
            ))

    return PreflightReport(
        metadata_count=len(metadata_keys),
        flat_pdf_count=len(flat_pdfs),
        total_pdf_bytes=total_pdf_bytes,
        submission_count=submission_count,
        accepted_submission_count=accepted_submission_count,
        pending_submission_count=pending_submission_count,
        rejected_submission_count=rejected_submission_count,
        chunk_count=chunk_count,
        vector_count=vector_count,
        importable_file_only=tuple(sorted(set(flat_pdfs) - set(metadata_keys))),
        missing_pdfs=tuple(sorted(missing)),
        alias_collisions=collisions,
        unavailable_rejected_pdfs=unavailable_rejected_pdfs,
        issues=tuple(issues),
    )


def _fingerprint_value(digest, value: bytes | str | int | None) -> None:
    if value is None:
        raw = b""
        marker = b"N"
    elif isinstance(value, bytes):
        raw = value
        marker = b"B"
    else:
        raw = str(value).encode("utf-8")
        marker = b"T"
    digest.update(marker)
    digest.update(len(raw).to_bytes(8, "big"))
    digest.update(raw)


def legacy_chunk_fingerprint(engine: Engine, legacy_filename: str) -> tuple[int, str]:
    columns = _columns(engine, "papers_chunks")
    if not _LEGACY_CHUNK_COLUMNS.issubset(columns):
        return 0, hashlib.sha256().hexdigest()
    rows = _rows(engine, """
        SELECT id, chunk_index, content, lang, embedding_vec
        FROM papers_chunks
        WHERE filename = :filename
        ORDER BY id
    """, {"filename": legacy_filename})
    digest = hashlib.sha256()
    for row in rows:
        vector = row["embedding_vec"]
        if isinstance(vector, memoryview):
            vector = vector.tobytes()
        if vector is not None and not isinstance(vector, bytes):
            raise MigrationBlocked(
                f"legacy vector for {legacy_filename!r} was not returned as raw bytes"
            )
        for value in (
            row["id"], row["chunk_index"], row["content"], row["lang"], vector,
        ):
            _fingerprint_value(digest, value)
    return len(rows), digest.hexdigest()


def _journal(engine: Engine, legacy_filename: str):
    rows = _rows(engine, """
        SELECT legacy_key, paper_id, revision_number, source_sha256, source_size_bytes,
               legacy_chunk_count, legacy_chunk_fingerprint, checkpoint
        FROM publishing_migration_journal
        WHERE legacy_key = :legacy_key
    """, {"legacy_key": legacy_filename})
    return rows[0] if rows else None


def _set_checkpoint(engine: Engine, legacy_filename: str, checkpoint: str) -> None:
    if checkpoint not in _CHECKPOINT_ORDER:
        raise ValueError(f"unknown migration checkpoint: {checkpoint}")
    allowed_predecessors = _CHECKPOINT_ORDER[:_CHECKPOINT_ORDER.index(checkpoint) + 1]
    placeholders = ", ".join(f":checkpoint_{index}" for index, _ in enumerate(allowed_predecessors))
    parameters = {
        f"checkpoint_{index}": value
        for index, value in enumerate(allowed_predecessors)
    }
    parameters.update({
        "checkpoint": checkpoint,
        "updated_at": _now(),
        "legacy_key": legacy_filename,
    })
    with engine.begin() as connection:
        result = connection.execute(text(f"""
            UPDATE publishing_migration_journal
            SET checkpoint = :checkpoint, updated_at = :updated_at
            WHERE legacy_key = :legacy_key
              AND checkpoint IN ({placeholders})
        """), parameters)
        if result.rowcount == 0:
            current = connection.execute(text("""
                SELECT checkpoint FROM publishing_migration_journal
                WHERE legacy_key = :legacy_key
            """), {"legacy_key": legacy_filename}).scalar()
            if current not in _CHECKPOINT_ORDER:
                raise MigrationBlocked(
                    f"journal has unknown checkpoint {current!r} for {legacy_filename!r}"
                )


def _insert_or_verify_journal(
    engine: Engine,
    legacy_filename: str,
    source_hash: str,
    source_size: int,
    chunk_count: int,
    chunk_fingerprint: str,
):
    existing = _journal(engine, legacy_filename)
    if existing is not None:
        try:
            uuid.UUID(existing["paper_id"])
        except (ValueError, TypeError) as exc:
            raise MigrationBlocked("migration journal contains an invalid Paper UUID") from exc
        expected = (
            1, source_hash, source_size, chunk_count, chunk_fingerprint,
        )
        actual = (
            existing["revision_number"], existing["source_sha256"], existing["source_size_bytes"],
            existing["legacy_chunk_count"], existing["legacy_chunk_fingerprint"],
        )
        if actual != expected:
            raise MigrationBlocked(
                f"legacy source or chunks changed after journaling {legacy_filename!r}"
            )
        return existing, True

    paper_id = str(uuid.uuid4())
    now = _now()
    try:
        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO publishing_migration_journal (
                    legacy_key, paper_id, revision_number,
                    source_sha256, source_size_bytes,
                    legacy_chunk_count, legacy_chunk_fingerprint, checkpoint,
                    created_at, updated_at
                ) VALUES (
                    :legacy_key, :paper_id, 1, :source_sha256, :source_size_bytes,
                    :legacy_chunk_count, :legacy_chunk_fingerprint, 'source_verified',
                    :created_at, :updated_at
                )
            """), {
                "legacy_key": legacy_filename,
                "paper_id": paper_id,
                "source_sha256": source_hash,
                "source_size_bytes": source_size,
                "legacy_chunk_count": chunk_count,
                "legacy_chunk_fingerprint": chunk_fingerprint,
                "created_at": now,
                "updated_at": now,
            })
    except IntegrityError:
        winner = _journal(engine, legacy_filename)
        if winner is None:
            raise
        verified, _resumed = _insert_or_verify_journal(
            engine, legacy_filename, source_hash, source_size,
            chunk_count, chunk_fingerprint,
        )
        return verified, True
    return _journal(engine, legacy_filename), False


def _policy_relative(papers_dir: Path, relative: str) -> bool:
    if (
        not relative
        or "\\" in relative
        or any(ord(character) < 32 for character in relative)
        or Path(relative).is_absolute()
        or any(component in {"", ".", ".."} for component in relative.split("/"))
    ):
        return False
    try:
        return resolve_contained(papers_dir, relative, must_exist=False) is not None
    except (OSError, RuntimeError, ValueError):
        return False


def _open_or_create_directory_at(parent_fd: int, name: str) -> int:
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        try:
            os.mkdir(name, mode=0o750, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError:
            pass
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise MigrationBlocked(f"migration directory {name!r} is unsafe") from exc
    if not stat.S_ISDIR(before.st_mode):
        raise MigrationBlocked(f"migration directory {name!r} is not a directory")
    try:
        child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise MigrationBlocked(f"migration directory {name!r} changed") from exc
    after = os.fstat(child_fd)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        os.close(child_fd)
        raise MigrationBlocked(f"migration directory {name!r} changed")
    return child_fd


def _open_existing_directory_at(parent_fd: int, name: str) -> int | None:
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise MigrationBlocked(f"migration directory {name!r} is unsafe") from exc
    if not stat.S_ISDIR(before.st_mode):
        raise MigrationBlocked(f"migration directory {name!r} is unsafe")
    try:
        child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise MigrationBlocked(f"migration directory {name!r} changed") from exc
    after = os.fstat(child_fd)
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        os.close(child_fd)
        raise MigrationBlocked(f"migration directory {name!r} changed")
    return child_fd


def require_publishing_maintenance() -> None:
    """Fail closed unless the operator has stopped every app/worker writer."""
    if os.environ.get(_MAINTENANCE_ENV) != "1":
        raise MigrationBlocked(
            f"publishing migration maintenance fence is closed; set {_MAINTENANCE_ENV}=1 "
            "only after all application and worker processes have been stopped"
        )


def _after_migration_lock_acquired(_paper_key: str | None) -> None:
    """Test seam reached only while the durable migration lock is held."""


def _process_lock(key: str) -> threading.RLock:
    with _PROCESS_LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(key, threading.RLock())


@contextmanager
def _single_migration_lock(
    engine: Engine,
    papers_dir: Path,
    scope: str,
):
    canonical = os.path.abspath(os.fspath(papers_dir))
    lock_key = f"{engine.url.render_as_string(hide_password=True)}|{canonical}|{scope}"
    held = getattr(_HELD_LOCKS, "keys", set())
    if lock_key in held:
        yield
        return

    process_lock = _process_lock(lock_key)
    with process_lock:
        connection = None
        lock_fd = None
        lock_directory_fd = None
        try:
            if engine.dialect.name == "mysql":
                connection = engine.connect()
                advisory_name = "keydion-pub-" + hashlib.sha256(
                    f"{canonical}|{scope}".encode("utf-8")
                ).hexdigest()[:48]
                acquired = connection.execute(
                    text("SELECT GET_LOCK(:lock_name, 60)"),
                    {"lock_name": advisory_name},
                ).scalar()
                if acquired != 1:
                    raise MigrationBlocked(
                        f"could not acquire MySQL publishing migration lock for {scope}"
                    )
            elif engine.dialect.name == "sqlite" and engine.url.database not in (None, ":memory:"):
                database_path = os.path.abspath(os.fspath(engine.url.database))
                lock_path = (
                    database_path + ".publishing-migration-"
                    + hashlib.sha256(scope.encode("utf-8")).hexdigest() + ".lock"
                )
                lock_fd = os.open(
                    lock_path,
                    os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
                    raise MigrationBlocked("publishing migration lock entry is unsafe")
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
            else:
                with _trusted_root(papers_dir) as root_fd:
                    lock_directory_fd = _open_or_create_directory_at(
                        root_fd, ".publishing-migration-locks",
                    )
                lock_name = hashlib.sha256(scope.encode("utf-8")).hexdigest() + ".lock"
                lock_fd = os.open(
                    lock_name,
                    os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=lock_directory_fd,
                )
                if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
                    raise MigrationBlocked("publishing migration lock entry is unsafe")
                fcntl.flock(lock_fd, fcntl.LOCK_EX)

            held = set(getattr(_HELD_LOCKS, "keys", set()))
            held.add(lock_key)
            _HELD_LOCKS.keys = held
            yield
        finally:
            held = set(getattr(_HELD_LOCKS, "keys", set()))
            held.discard(lock_key)
            _HELD_LOCKS.keys = held
            if lock_fd is not None:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            if lock_directory_fd is not None:
                os.close(lock_directory_fd)
            if connection is not None:
                try:
                    connection.execute(
                        text("SELECT RELEASE_LOCK(:lock_name)"),
                        {"lock_name": advisory_name},
                    )
                finally:
                    connection.close()


@contextmanager
def migration_fence(
    engine: Engine,
    papers_dir: Path,
    *,
    paper_key: str | None = None,
):
    """Require maintenance mode and serialize global and per-Paper work."""
    require_publishing_maintenance()
    with _single_migration_lock(engine, papers_dir, "global"):
        if paper_key is None:
            _after_migration_lock_acquired(None)
            yield
        else:
            paper_scope = "paper:" + hashlib.sha256(
                paper_key.encode("utf-8")
            ).hexdigest()
            with _single_migration_lock(engine, papers_dir, paper_scope):
                _after_migration_lock_acquired(paper_key)
                yield


def _verified_file_at(
    directory_fd: int,
    name: str,
    expected_hash: str,
    expected_size: int,
) -> os.stat_result | None:
    file_fd, opened, classification = _open_regular_at(directory_fd, name)
    if classification == "missing":
        return None
    if file_fd is None or opened is None:
        raise MigrationBlocked(f"migration file {name!r} is unsafe")
    try:
        actual_hash, actual_size = _hash_fd(file_fd)
        final = os.fstat(file_fd)
    finally:
        os.close(file_fd)
    if (
        (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        != (final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns)
    ):
        raise MigrationBlocked(f"migration file {name!r} changed while hashing")
    if (actual_hash, actual_size) != (expected_hash, expected_size):
        raise MigrationBlocked(f"migration file {name!r} exists with different bytes")
    return final


def _verified_existing_file(
    papers_dir: Path,
    relative: str,
    expected_hash: str,
    expected_size: int,
) -> Path | None:
    if not _policy_relative(papers_dir, relative):
        raise MigrationBlocked(f"unsafe migration destination {relative!r}")
    components = relative.split("/")
    with _trusted_root(papers_dir) as root_fd:
        parent_fd = os.dup(root_fd)
        try:
            for component in components[:-1]:
                next_fd = _open_existing_directory_at(parent_fd, component)
                if next_fd is None:
                    return None
                os.close(parent_fd)
                parent_fd = next_fd
            verified = _verified_file_at(
                parent_fd,
                components[-1],
                expected_hash,
                expected_size,
            )
            return papers_dir / relative if verified is not None else None
        finally:
            os.close(parent_fd)


def _after_copy_verified() -> None:
    """Test seam for simulating interruption after a durable verified copy."""


def _before_atomic_publication(_stage_name: str) -> None:
    """Test seam immediately before the no-replace hard-link publication."""


def _after_atomic_publication(_stage_name: str) -> None:
    """Test seam after the durable destination link exists."""


def _after_stage_candidate_hashed(_stage_name: str, _matched: bool) -> None:
    """Test seam proving a hashed stage pathname is never later unlinked."""


def _matching_stage_candidate(
    stage_fd: int,
    expected_hash: str,
    expected_size: int,
) -> str | None:
    names = sorted(entry.name for entry in os.scandir(stage_fd))
    for name in names:
        if not (
            name == "1.pdf.part"
            or (name.startswith("1.") and name.endswith(".pdf.part"))
        ):
            continue
        file_fd, opened, classification = _open_regular_at(stage_fd, name)
        if classification == "missing":
            continue
        if file_fd is None or opened is None:
            raise MigrationBlocked(f"migration stage candidate {name!r} is unsafe")
        try:
            actual_hash, actual_size = _hash_fd(file_fd)
            final = os.fstat(file_fd)
        finally:
            os.close(file_fd)
        if (
            opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns,
        ) != (
            final.st_dev, final.st_ino, final.st_size, final.st_mtime_ns,
        ):
            raise MigrationBlocked(f"migration stage candidate {name!r} changed")
        matched = (actual_hash, actual_size) == (expected_hash, expected_size)
        _after_stage_candidate_hashed(name, matched)
        if not matched:
            # Never unlink a pathname based on a descriptor hashed earlier.
            # A later attempt uses a new unpredictable stage name.
            continue
        try:
            current = os.stat(name, dir_fd=stage_fd, follow_symlinks=False)
        except OSError as exc:
            raise MigrationBlocked(f"migration stage candidate {name!r} changed") from exc
        if (current.st_dev, current.st_ino) != (final.st_dev, final.st_ino):
            raise MigrationBlocked(f"migration stage candidate {name!r} changed")
        return name
    return None


def _create_stage_file(stage_fd: int) -> tuple[str, int]:
    flags = (
        os.O_WRONLY | os.O_CREAT | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    candidates = ["1.pdf.part"]
    candidates.extend(f"1.{uuid.uuid4().hex}.pdf.part" for _ in range(4))
    for name in candidates:
        try:
            return name, os.open(name, flags, 0o640, dir_fd=stage_fd)
        except FileExistsError:
            continue
    raise MigrationBlocked("could not reserve a unique migration stage file")


def _copy_and_publish_revision(
    papers_dir: Path,
    legacy_filename: str,
    paper_id: str,
    expected_hash: str,
    expected_size: int,
    engine: Engine,
) -> Path:
    stage_relative = f".publishing-migration-stage/{paper_id}"
    target_relative = paper_id
    destination_relative = f"{target_relative}/1.pdf"
    if (
        not _policy_legacy_name(papers_dir, legacy_filename)
        or not _policy_relative(papers_dir, stage_relative)
        or not _policy_relative(papers_dir, destination_relative)
    ):
        raise MigrationBlocked("migration source or destination violates containment policy")

    with _trusted_root(papers_dir) as root_fd:
        source_fd, _source_stat, source_classification = _open_regular_at(
            root_fd, legacy_filename, source_hook=True,
        )
        if source_fd is None or source_classification != "ok":
            raise MigrationBlocked(f"legacy PDF {legacy_filename!r} changed after journaling")
        stage_root_fd = stage_fd = target_fd = None
        try:
            source_hash, source_size = _hash_fd(source_fd)
            if (source_hash, source_size) != (expected_hash, expected_size):
                raise MigrationBlocked(f"legacy PDF {legacy_filename!r} changed after journaling")

            stage_root_fd = _open_or_create_directory_at(
                root_fd, ".publishing-migration-stage",
            )
            if os.fstat(stage_root_fd).st_dev != os.fstat(root_fd).st_dev:
                raise MigrationBlocked("staging and Paper storage are on different devices")
            stage_fd = _open_or_create_directory_at(stage_root_fd, paper_id)
            target_fd = _open_or_create_directory_at(root_fd, paper_id)
            target_identity = os.fstat(target_fd)
            _after_target_directory_opened(paper_id)

            destination = _verified_file_at(
                target_fd, "1.pdf", expected_hash, expected_size,
            )
            if destination is not None:
                reopened = _open_existing_directory_at(root_fd, paper_id)
                if reopened is None:
                    raise MigrationBlocked("published Paper directory disappeared")
                try:
                    current = os.fstat(reopened)
                    if (current.st_dev, current.st_ino) != (
                        target_identity.st_dev, target_identity.st_ino,
                    ):
                        raise MigrationBlocked("published Paper directory changed")
                finally:
                    os.close(reopened)
                os.fsync(target_fd)
                _set_checkpoint(engine, legacy_filename, "destination_verified")
                return papers_dir / destination_relative

            stage_name = _matching_stage_candidate(
                stage_fd, expected_hash, expected_size,
            )
            copied = False
            if stage_name is None:
                os.lseek(source_fd, 0, os.SEEK_SET)
                stage_name, part_fd = _create_stage_file(stage_fd)
                try:
                    with os.fdopen(os.dup(source_fd), "rb") as source_stream, os.fdopen(
                        os.dup(part_fd), "wb"
                    ) as target_stream:
                        shutil.copyfileobj(source_stream, target_stream, length=1024 * 1024)
                        target_stream.flush()
                    os.fsync(part_fd)
                finally:
                    os.close(part_fd)
                os.fsync(stage_fd)
                part = _verified_file_at(
                    stage_fd, stage_name, expected_hash, expected_size,
                )
                if part is None:
                    raise MigrationBlocked("verified staging copy disappeared")
                copied = True

            _set_checkpoint(engine, legacy_filename, "copy_verified")
            if copied:
                _after_copy_verified()

            try:
                stage_before_publication = os.stat(
                    stage_name, dir_fd=stage_fd, follow_symlinks=False,
                )
            except OSError as exc:
                raise MigrationBlocked("verified migration stage file disappeared") from exc
            _before_atomic_publication(stage_name)
            try:
                stage_at_publication = os.stat(
                    stage_name, dir_fd=stage_fd, follow_symlinks=False,
                )
            except OSError as exc:
                raise MigrationBlocked("verified migration stage file changed") from exc
            if (
                stage_before_publication.st_dev,
                stage_before_publication.st_ino,
            ) != (stage_at_publication.st_dev, stage_at_publication.st_ino):
                raise MigrationBlocked("verified migration stage file changed")
            try:
                os.link(
                    stage_name,
                    "1.pdf",
                    src_dir_fd=stage_fd,
                    dst_dir_fd=target_fd,
                    follow_symlinks=False,
                )
            except FileExistsError:
                raced = _verified_file_at(
                    target_fd, "1.pdf", expected_hash, expected_size,
                )
                if raced is None:
                    raise MigrationBlocked("racing migration destination disappeared")
            except OSError as exc:
                raise MigrationBlocked("could not atomically reserve migration destination") from exc
            else:
                _after_atomic_publication(stage_name)
                linked_destination = _verified_file_at(
                    target_fd, "1.pdf", expected_hash, expected_size,
                )
                if linked_destination is None:
                    raise MigrationBlocked("migration destination reservation changed")

            os.fsync(target_fd)
            reopened = _open_existing_directory_at(root_fd, paper_id)
            if reopened is None:
                raise MigrationBlocked("published Paper directory disappeared")
            try:
                current = os.fstat(reopened)
                if (current.st_dev, current.st_ino) != (
                    target_identity.st_dev, target_identity.st_ino,
                ):
                    raise MigrationBlocked("published Paper directory changed")
                if _verified_file_at(
                    reopened, "1.pdf", expected_hash, expected_size,
                ) is None:
                    raise MigrationBlocked("published revision disappeared")
            finally:
                os.close(reopened)
            _set_checkpoint(engine, legacy_filename, "destination_verified")
            return papers_dir / destination_relative
        finally:
            os.close(source_fd)
            for directory_fd in (stage_fd, stage_root_fd, target_fd):
                if directory_fd is not None:
                    os.close(directory_fd)


def _capture_pre_backfill_state(engine: Engine, expected_paper_count: int) -> None:
    table_names = _table_names(engine)
    submission_count = (
        int(_scalar(engine, "SELECT COUNT(*) FROM submissions"))
        if "submissions" in table_names else 0
    )
    chunk_count = (
        int(_scalar(engine, "SELECT COUNT(*) FROM papers_chunks"))
        if "papers_chunks" in table_names else 0
    )
    vector_count = (
        int(_scalar(engine, "SELECT COUNT(*) FROM papers_chunks WHERE embedding_vec IS NOT NULL"))
        if "embedding_vec" in _columns(engine, "papers_chunks") else 0
    )
    with engine.begin() as connection:
        exists = connection.execute(text("""
            SELECT name FROM publishing_migration_state
            WHERE name = 'pre_backfill'
        """)).scalar()
        if exists is None:
            connection.execute(text("""
                INSERT INTO publishing_migration_state (
                    name, paper_count, submission_count, chunk_count,
                    vector_count, ddl_phase, captured_at
                ) VALUES (
                    'pre_backfill', :paper_count, :submission_count,
                    :chunk_count, :vector_count, 'expanded', :captured_at
                )
            """), {
                "paper_count": expected_paper_count,
                "submission_count": submission_count,
                "chunk_count": chunk_count,
                "vector_count": vector_count,
                "captured_at": _now(),
            })


def _persist_paper_rows(
    engine: Engine,
    legacy_filename: str,
    paper_id: str,
    source_hash: str,
    source_size: int,
    chunk_count: int,
) -> None:
    lookup_key = normalize_alias_key(legacy_filename)
    if len(lookup_key) > 255:
        raise MigrationBlocked(f"normalized alias is too long for {legacy_filename!r}")
    now = _now()
    with engine.begin() as connection:
        duplicates = connection.execute(text("""
            SELECT chunk_index, COUNT(*) AS duplicate_count
            FROM papers_chunks
            WHERE filename = :filename
            GROUP BY chunk_index
            HAVING COUNT(*) > 1
        """), {"filename": legacy_filename}).mappings().all()
        if duplicates:
            raise MigrationBlocked(f"duplicate legacy chunks for {legacy_filename!r}")

        paper = connection.execute(text("""
            SELECT filename, id FROM papers_metadata WHERE filename = :filename
        """), {"filename": legacy_filename}).mappings().first()
        index_status = "ready" if chunk_count else "pending"
        indexed_revision = 1 if chunk_count else None
        if paper is None:
            connection.execute(text("""
                INSERT INTO papers_metadata (
                    filename, id, lifecycle_state, current_revision, row_version,
                    index_status, indexed_revision
                ) VALUES (
                    :filename, :paper_id, 'published', 1, 0,
                    :index_status, :indexed_revision
                )
            """), {
                "filename": legacy_filename,
                "paper_id": paper_id,
                "index_status": index_status,
                "indexed_revision": indexed_revision,
            })
        elif paper["id"] not in (None, paper_id):
            raise MigrationBlocked(f"legacy metadata {legacy_filename!r} has a different Paper ID")
        else:
            connection.execute(text("""
                UPDATE papers_metadata
                SET id = :paper_id, lifecycle_state = 'published',
                    current_revision = 1, row_version = COALESCE(row_version, 0),
                    index_status = :index_status,
                    indexed_revision = :indexed_revision, index_error = NULL
                WHERE filename = :filename
            """), {
                "paper_id": paper_id,
                "filename": legacy_filename,
                "index_status": index_status,
                "indexed_revision": indexed_revision,
            })

        existing_revision = connection.execute(text("""
            SELECT sha256, size_bytes FROM paper_revisions
            WHERE paper_id = :paper_id AND revision_number = 1
        """), {"paper_id": paper_id}).mappings().first()
        if existing_revision is None:
            connection.execute(text("""
                INSERT INTO paper_revisions (
                    paper_id, revision_number, sha256, size_bytes,
                    created_at, created_by, restored_from_revision
                ) VALUES (
                    :paper_id, 1, :sha256, :size_bytes,
                    :created_at, :created_by, NULL
                )
            """), {
                "paper_id": paper_id,
                "sha256": source_hash,
                "size_bytes": source_size,
                "created_at": now,
                "created_by": _MIGRATION_ACTOR,
            })
        elif (
            existing_revision["sha256"], existing_revision["size_bytes"]
        ) != (source_hash, source_size):
            raise MigrationBlocked(f"revision 1 metadata differs for {legacy_filename!r}")

        alias = connection.execute(text("""
            SELECT filename, paper_id FROM paper_filename_aliases
            WHERE lookup_key = :lookup_key
        """), {"lookup_key": lookup_key}).mappings().first()
        if alias is None:
            connection.execute(text("""
                INSERT INTO paper_filename_aliases (
                    lookup_key, filename, paper_id, created_at
                ) VALUES (
                    :lookup_key, :filename, :paper_id, :created_at
                )
            """), {
                "lookup_key": lookup_key,
                "filename": legacy_filename,
                "paper_id": paper_id,
                "created_at": now,
            })
        elif (alias["filename"], alias["paper_id"]) != (legacy_filename, paper_id):
            raise MigrationBlocked(f"normalized alias collision for {legacy_filename!r}")

        already_mapped_chunks = connection.execute(text("""
            SELECT COUNT(*) FROM papers_chunks
            WHERE filename = :filename AND paper_id = :paper_id
              AND revision_number = 1
        """), {"filename": legacy_filename, "paper_id": paper_id}).scalar()

        # The vector column is intentionally absent from this UPDATE.  MySQL
        # therefore preserves the exact raw VECTOR bytes.
        connection.execute(text("""
            UPDATE papers_chunks
            SET paper_id = :paper_id, revision_number = 1
            WHERE filename = :filename
              AND (paper_id IS NULL OR paper_id = :paper_id)
              AND (revision_number IS NULL OR revision_number = 1)
        """), {"paper_id": paper_id, "filename": legacy_filename})
        mapped_chunks = connection.execute(text("""
            SELECT COUNT(*) FROM papers_chunks
            WHERE filename = :filename AND paper_id = :paper_id
              AND revision_number = 1
        """), {"filename": legacy_filename, "paper_id": paper_id}).scalar()
        if mapped_chunks != chunk_count:
            raise MigrationBlocked(f"not every legacy chunk mapped for {legacy_filename!r}")

        version = connection.execute(text("""
            SELECT value FROM rag_index_meta WHERE name = 'chunks_version'
        """)).scalar()
        if version is None:
            connection.execute(text("""
                INSERT INTO rag_index_meta (name, value) VALUES ('chunks_version', 0)
            """))
        if chunk_count:
            if already_mapped_chunks != chunk_count:
                connection.execute(text("""
                    UPDATE rag_index_meta SET value = value + 1
                    WHERE name = 'chunks_version'
                """))
        else:
            dedupe_key = f"index:{paper_id}:1"
            existing_job = connection.execute(text("""
                SELECT id FROM publishing_jobs WHERE dedupe_key = :dedupe_key
            """), {"dedupe_key": dedupe_key}).scalar()
            if existing_job is None:
                connection.execute(text("""
                    INSERT INTO publishing_jobs (
                        id, kind, paper_id, revision_number, dedupe_key, state,
                        attempts, available_at, lease_token, lease_expires_at,
                        last_error, created_at, updated_at
                    ) VALUES (
                        :id, 'index', :paper_id, 1, :dedupe_key, 'pending',
                        0, :available_at, NULL, NULL, NULL, :created_at, :updated_at
                    )
                """), {
                    "id": str(uuid.uuid4()),
                    "paper_id": paper_id,
                    "dedupe_key": dedupe_key,
                    "available_at": now,
                    "created_at": now,
                    "updated_at": now,
                })


def _backfill_one_paper_unfenced(
    engine: Engine, papers_dir: Path, legacy_filename: str,
) -> BackfilledPaper:
    """Backfill one legacy Paper durably and idempotently."""
    papers_dir = Path(papers_dir)
    source, source_hash, source_size = _safe_source_details(
        papers_dir, legacy_filename,
    )
    del source  # Every later file operation re-resolves the DB-supplied key.
    chunk_count, chunk_fingerprint = legacy_chunk_fingerprint(
        engine, legacy_filename,
    )
    journal, resumed = _insert_or_verify_journal(
        engine,
        legacy_filename,
        source_hash,
        source_size,
        chunk_count,
        chunk_fingerprint,
    )
    paper_id = journal["paper_id"]
    if journal["checkpoint"] == "db_complete":
        destination_relative = f"{paper_id}/1.pdf"
        destination = _verified_existing_file(
            papers_dir,
            destination_relative,
            source_hash,
            source_size,
        )
        if destination is None:
            raise MigrationBlocked(
                f"completed migration destination is missing for {legacy_filename!r}"
            )
        return BackfilledPaper(
            paper_id=paper_id,
            legacy_filename=legacy_filename,
            revision_number=1,
            sha256=source_hash,
            size_bytes=source_size,
            destination=str(destination),
            resumed=True,
        )
    destination = _copy_and_publish_revision(
        papers_dir,
        legacy_filename,
        paper_id,
        source_hash,
        source_size,
        engine,
    )
    if journal["checkpoint"] != "db_complete":
        _persist_paper_rows(
            engine,
            legacy_filename,
            paper_id,
            source_hash,
            source_size,
            chunk_count,
        )
        _set_checkpoint(engine, legacy_filename, "db_complete")
    return BackfilledPaper(
        paper_id=paper_id,
        legacy_filename=legacy_filename,
        revision_number=1,
        sha256=source_hash,
        size_bytes=source_size,
        destination=str(destination),
        resumed=resumed,
    )


def backfill_one_paper(
    engine: Engine, papers_dir: Path, legacy_filename: str,
) -> BackfilledPaper:
    papers_dir = Path(papers_dir)
    with migration_fence(engine, papers_dir, paper_key=legacy_filename):
        return _backfill_one_paper_unfenced(engine, papers_dir, legacy_filename)


def _persist_issue(
    engine: Engine, code: str, legacy_key: str, details: str,
) -> None:
    issue_id = str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"keydion:publishing-migration:{code}:{legacy_key}",
    ))
    now = _now()
    with engine.begin() as connection:
        existing = connection.execute(text("""
            SELECT id FROM publishing_migration_issues WHERE id = :id
        """), {"id": issue_id}).scalar()
        if existing is None:
            connection.execute(text("""
                INSERT INTO publishing_migration_issues (
                    id, kind, legacy_key, paper_id, details, blocking,
                    resolved_at, created_at, updated_at
                ) VALUES (
                    :id, :kind, :legacy_key, NULL, :details, 0,
                    NULL, :created_at, :updated_at
                )
            """), {
                "id": issue_id,
                "kind": code,
                "legacy_key": legacy_key,
                "details": details,
                "created_at": now,
                "updated_at": now,
            })
        else:
            connection.execute(text("""
                UPDATE publishing_migration_issues
                SET details = :details, blocking = 0, resolved_at = NULL,
                    updated_at = :updated_at, paper_id = NULL
                WHERE id = :id
            """), {
                "id": issue_id,
                "details": details,
                "updated_at": now,
            })


def _resolve_submission_issues(engine: Engine, submission_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE publishing_migration_issues
            SET resolved_at = :resolved_at, updated_at = :updated_at
            WHERE legacy_key = :legacy_key
              AND kind IN ('submission_unmatched', 'submission_ambiguous')
              AND resolved_at IS NULL
        """), {
            "legacy_key": submission_id,
            "resolved_at": _now(),
            "updated_at": _now(),
        })


def _link_submissions(engine: Engine) -> None:
    columns = _columns(engine, "submissions")
    if not {"id", "status", "paper_id"}.issubset(columns):
        return
    candidate_columns = [
        name for name in ("pdf_filename", "pending_filename", "original_filename")
        if name in columns
    ]
    selected = ", ".join(["id", "status", "paper_id"] + candidate_columns)
    submissions = _rows(engine, f"SELECT {selected} FROM submissions ORDER BY id")
    paper_rows = _rows(engine, """
        SELECT filename, id FROM papers_metadata WHERE id IS NOT NULL
    """)
    paper_by_filename = {row["filename"]: row["id"] for row in paper_rows}

    with engine.begin() as connection:
        if {"feedback", "comment"}.issubset(columns):
            connection.execute(text("""
                UPDATE submissions
                SET comment = feedback
                WHERE (comment IS NULL OR comment = '')
                  AND feedback IS NOT NULL AND feedback <> ''
            """))
        connection.execute(text("""
            UPDATE submissions SET paper_id = NULL
            WHERE LOWER(COALESCE(status, '')) IN ('pending', 'draft', 'rejected')
        """))

    for submission in submissions:
        if (submission["status"] or "").casefold() != "accepted":
            continue
        candidates = {
            paper_by_filename[submission[column]]
            for column in candidate_columns
            if submission[column] and submission[column] in paper_by_filename
        }
        if len(candidates) == 1:
            paper_id = next(iter(candidates))
            with engine.begin() as connection:
                connection.execute(text("""
                    UPDATE submissions SET paper_id = :paper_id WHERE id = :id
                """), {"paper_id": paper_id, "id": submission["id"]})
            _resolve_submission_issues(engine, submission["id"])
        else:
            code = "submission_unmatched" if not candidates else "submission_ambiguous"
            details = (
                "accepted Submission has no exact nonempty legacy filename match"
                if not candidates
                else "accepted Submission has multiple exact legacy filename matches"
            )
            with engine.begin() as connection:
                connection.execute(text("""
                    UPDATE submissions SET paper_id = NULL WHERE id = :id
                """), {"id": submission["id"]})
            _resolve_submission_issues(engine, submission["id"])
            _persist_issue(engine, code, submission["id"], details)


def _validate_submission_links(engine: Engine) -> None:
    columns = _columns(engine, "submissions")
    if not {"id", "status", "paper_id"}.issubset(columns):
        return
    candidate_columns = [
        name for name in ("pdf_filename", "pending_filename", "original_filename")
        if name in columns
    ]
    selected = ", ".join(["id", "status", "paper_id"] + candidate_columns)
    submissions = _rows(engine, f"SELECT {selected} FROM submissions ORDER BY id")
    paper_by_filename = {
        row["filename"]: row["id"]
        for row in _rows(engine, "SELECT filename, id FROM papers_metadata")
        if row["id"] is not None
    }
    for submission in submissions:
        status = (submission["status"] or "").casefold()
        candidates = {
            paper_by_filename[submission[column]]
            for column in candidate_columns
            if submission[column] and submission[column] in paper_by_filename
        }
        expected = next(iter(candidates)) if status == "accepted" and len(candidates) == 1 else None
        if submission["paper_id"] != expected:
            raise MigrationBlocked(
                f"accepted Submission link is not exact for {submission['id']!r}"
                if status == "accepted"
                else f"non-accepted Submission remains linked: {submission['id']!r}"
            )
        if status == "accepted" and len(candidates) != 1:
            expected_kind = "submission_unmatched" if not candidates else "submission_ambiguous"
            count = int(_scalar(engine, """
                SELECT COUNT(*) FROM publishing_migration_issues
                WHERE legacy_key = :legacy_key AND kind = :kind
                  AND blocking = 0 AND resolved_at IS NULL
            """, {"legacy_key": submission["id"], "kind": expected_kind}))
            if count != 1:
                raise MigrationBlocked(
                    f"accepted Submission issue is missing for {submission['id']!r}"
                )
            opposite_kind = (
                "submission_ambiguous"
                if expected_kind == "submission_unmatched"
                else "submission_unmatched"
            )
            stale_opposite = int(_scalar(engine, """
                SELECT COUNT(*) FROM publishing_migration_issues
                WHERE legacy_key = :legacy_key AND kind = :kind
                  AND resolved_at IS NULL
            """, {
                "legacy_key": submission["id"],
                "kind": opposite_kind,
            }))
            if stale_opposite:
                raise MigrationBlocked(
                    f"accepted Submission has a stale opposite diagnostic: {submission['id']!r}"
                )


def backfill_all(engine: Engine, papers_dir: Path) -> tuple[BackfilledPaper, ...]:
    """Backfill every safe metadata/file Paper, then link legacy Submissions."""
    papers_dir = Path(papers_dir)
    with migration_fence(engine, papers_dir):
        report = run_preflight(engine, papers_dir)
        if report.blockers:
            codes = ", ".join(issue.code for issue in report.blockers)
            raise MigrationBlocked(f"publishing migration preflight blockers: {codes}")
        filenames = sorted(set(_legacy_metadata_keys(engine)) | set(report.importable_file_only))
        _capture_pre_backfill_state(engine, len(filenames))
        results = tuple(
            backfill_one_paper(engine, papers_dir, filename)
            for filename in filenames
        )
        _link_submissions(engine)
        return results


def _assert_count(engine: Engine, statement: str, expected: int, label: str) -> None:
    actual = int(_scalar(engine, statement))
    if actual != expected:
        raise MigrationBlocked(f"{label} count changed: expected {expected}, found {actual}")


def validate_contract_ready(
    engine: Engine,
    papers_dir: Path,
    *,
    _fenced: bool = False,
) -> PreflightReport:
    """Refuse contraction unless every SQL/file identity invariant is proven."""
    if not _fenced:
        with migration_fence(engine, Path(papers_dir)):
            return validate_contract_ready(
                engine, papers_dir, _fenced=True,
            )
    report = run_preflight(
        engine,
        papers_dir,
        capacity_phase="contract",
        allow_contract_recovery=True,
    )
    if report.blockers:
        details = ", ".join(
            f"{issue.code}:{issue.legacy_key}" for issue in report.blockers
        )
        raise MigrationBlocked(f"publishing migration contract blocked: {details}")

    required_tables = {
        "paper_revisions", "paper_filename_aliases", "publishing_jobs",
        "publishing_migration_journal", "publishing_migration_state",
        "publishing_migration_issues",
    }
    missing_tables = required_tables - _table_names(engine)
    if missing_tables:
        raise MigrationBlocked(
            "publishing migration tables missing: " + ", ".join(sorted(missing_tables))
        )
    persisted_blockers = _rows(engine, """
        SELECT kind, legacy_key, blocking
        FROM publishing_migration_issues
        WHERE resolved_at IS NULL
          AND (blocking <> 0 OR kind NOT IN (
              'submission_unmatched', 'submission_ambiguous'
          ))
        ORDER BY kind, legacy_key
    """)
    if persisted_blockers:
        issue = persisted_blockers[0]
        raise MigrationBlocked(
            "persisted migration issue blocks contraction: "
            f"{issue['kind']}:{issue['legacy_key']}"
        )
    _validate_submission_links(engine)

    paper_orphan_checks = (
        ("paper_revisions", "paper_id", False),
        ("paper_filename_aliases", "paper_id", False),
        ("publishing_jobs", "paper_id", False),
        ("publishing_migration_journal", "paper_id", False),
        ("publishing_migration_issues", "paper_id", True),
        ("submissions", "paper_id", True),
    )
    for table_name, column_name, nullable in paper_orphan_checks:
        nullable_filter = f"child.{column_name} IS NOT NULL AND " if nullable else ""
        orphan_count = int(_scalar(engine, f"""
            SELECT COUNT(*)
            FROM {table_name} AS child
            LEFT JOIN papers_metadata AS paper
              ON paper.id = child.{column_name}
            WHERE {nullable_filter}paper.id IS NULL
        """))
        if orphan_count:
            raise MigrationBlocked(
                f"{table_name} has {orphan_count} orphan Paper relationship(s)"
            )
    orphan_chunks = int(_scalar(engine, """
        SELECT COUNT(*)
        FROM papers_chunks AS chunk
        LEFT JOIN paper_revisions AS revision
          ON revision.paper_id = chunk.paper_id
         AND revision.revision_number = chunk.revision_number
        WHERE chunk.paper_id IS NOT NULL
          AND chunk.revision_number IS NOT NULL
          AND revision.paper_id IS NULL
    """))
    if orphan_chunks:
        raise MigrationBlocked(
            f"papers_chunks has {orphan_chunks} orphan revision relationship(s)"
        )

    paper_rows = _rows(engine, """
        SELECT filename, id, lifecycle_state, current_revision,
               row_version, index_status, indexed_revision
        FROM papers_metadata ORDER BY filename
    """)
    for paper in paper_rows:
        if (
            not paper["id"]
            or paper["lifecycle_state"] != "published"
            or paper["current_revision"] != 1
            or paper["row_version"] is None
            or paper["index_status"] not in {"pending", "ready", "failed"}
        ):
            raise MigrationBlocked(f"Paper row is not contract-ready: {paper['filename']!r}")
        journal_rows = _rows(engine, """
            SELECT paper_id, revision_number, source_sha256, source_size_bytes,
                   legacy_chunk_count, legacy_chunk_fingerprint, checkpoint
            FROM publishing_migration_journal
            WHERE legacy_key = :legacy_key
        """, {"legacy_key": paper["filename"]})
        if (
            len(journal_rows) != 1
            or journal_rows[0]["paper_id"] != paper["id"]
            or journal_rows[0]["revision_number"] != 1
            or journal_rows[0]["checkpoint"] != "db_complete"
        ):
            raise MigrationBlocked(
                f"Paper journal is incomplete: {paper['filename']!r}"
            )
        journal = journal_rows[0]
        _source, source_hash, source_size = _safe_source_details(
            Path(papers_dir), paper["filename"],
        )
        if (
            source_hash != journal["source_sha256"]
            or source_size != journal["source_size_bytes"]
        ):
            raise MigrationBlocked(
                f"journaled source hash/size changed: {paper['filename']!r}"
            )
        chunk_count, chunk_fingerprint = legacy_chunk_fingerprint(
            engine, paper["filename"],
        )
        if (
            chunk_count != journal["legacy_chunk_count"]
            or chunk_fingerprint != journal["legacy_chunk_fingerprint"]
        ):
            raise MigrationBlocked(
                f"journaled chunk fingerprint changed: {paper['filename']!r}"
            )
        if chunk_count:
            if paper["index_status"] != "ready" or paper["indexed_revision"] != 1:
                raise MigrationBlocked(
                    f"Paper indexed state does not match preserved chunks: {paper['filename']!r}"
                )
        else:
            if paper["index_status"] != "pending" or paper["indexed_revision"] is not None:
                raise MigrationBlocked(
                    f"Paper pending index state is invalid: {paper['filename']!r}"
                )
            job_count = int(_scalar(engine, """
                SELECT COUNT(*) FROM publishing_jobs
                WHERE paper_id = :paper_id AND revision_number = 1
                  AND dedupe_key = :dedupe_key AND kind = 'index'
            """, {
                "paper_id": paper["id"],
                "dedupe_key": f"index:{paper['id']}:1",
            }))
            if job_count != 1:
                raise MigrationBlocked(
                    f"Paper index job is missing or duplicated: {paper['filename']!r}"
                )
        revision = _rows(engine, """
            SELECT sha256, size_bytes FROM paper_revisions
            WHERE paper_id = :paper_id AND revision_number = 1
        """, {"paper_id": paper["id"]})
        if len(revision) != 1:
            raise MigrationBlocked(f"Paper revision is missing: {paper['filename']!r}")
        if (
            revision[0]["sha256"] != journal["source_sha256"]
            or revision[0]["size_bytes"] != journal["source_size_bytes"]
        ):
            raise MigrationBlocked(
                f"Paper revision hash differs from journal: {paper['filename']!r}"
            )
        destination_relative = f"{paper['id']}/1.pdf"
        if _verified_existing_file(
            Path(papers_dir), destination_relative,
            revision[0]["sha256"], revision[0]["size_bytes"],
        ) is None:
            raise MigrationBlocked(f"Paper revision file is missing: {paper['filename']!r}")
        alias_count = int(_scalar(engine, """
            SELECT COUNT(*) FROM paper_filename_aliases
            WHERE lookup_key = :lookup_key AND filename = :filename
              AND paper_id = :paper_id
        """, {
            "lookup_key": normalize_alias_key(paper["filename"]),
            "filename": paper["filename"],
            "paper_id": paper["id"],
        }))
        if alias_count != 1:
            raise MigrationBlocked(f"Paper alias is missing: {paper['filename']!r}")

    journal_count = int(_scalar(
        engine, "SELECT COUNT(*) FROM publishing_migration_journal",
    ))
    if journal_count != len(paper_rows):
        raise MigrationBlocked(
            f"journal/Paper count differs: {journal_count} != {len(paper_rows)}"
        )

    unmapped_chunks = int(_scalar(engine, """
        SELECT COUNT(*) FROM papers_chunks
        WHERE paper_id IS NULL OR revision_number IS NULL OR chunk_index IS NULL
    """))
    if unmapped_chunks:
        raise MigrationBlocked(
            f"{unmapped_chunks} prospective chunk key(s) contain NULL"
        )
    if _duplicate_chunk_issues(engine, final_key=True):
        raise MigrationBlocked("duplicate chunk keys remain in the exact prospective key")

    state = _rows(engine, """
        SELECT paper_count, submission_count, chunk_count, vector_count, ddl_phase
        FROM publishing_migration_state WHERE name = 'pre_backfill'
    """)
    if len(state) != 1:
        raise MigrationBlocked("pre-backfill count snapshot is missing")
    expected = state[0]
    if expected["ddl_phase"] not in {
        "expanded", "paper_contract", "relationships_contract", "complete",
    }:
        raise MigrationBlocked(
            f"unknown persisted DDL phase: {expected['ddl_phase']!r}"
        )
    _assert_count(engine, "SELECT COUNT(*) FROM papers_metadata",
                  expected["paper_count"], "Paper")
    _assert_count(engine, "SELECT COUNT(*) FROM submissions",
                  expected["submission_count"], "Submission")
    _assert_count(engine, "SELECT COUNT(*) FROM papers_chunks",
                  expected["chunk_count"], "chunk")
    _assert_count(engine, "SELECT COUNT(*) FROM papers_chunks WHERE embedding_vec IS NOT NULL",
                  expected["vector_count"], "vector")
    return report
