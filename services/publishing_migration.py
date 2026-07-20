"""Offline, resumable migration from filename identity to Paper UUID identity.

The preflight in this module is deliberately read-only.  The backfill helpers
are deliberately not part of application startup: they are invoked only by the
ordered Alembic data migration after an operator has reviewed preflight output.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import inspect, text
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
        "legacy_key", "paper_id", "source_sha256", "source_size_bytes",
        "legacy_chunk_count", "legacy_chunk_fingerprint", "checkpoint",
        "created_at", "updated_at",
    }),
    "publishing_migration_state": frozenset({
        "name", "paper_count", "submission_count", "chunk_count", "vector_count",
        "captured_at",
    }),
    "publishing_migration_issues": frozenset({
        "id", "kind", "legacy_key", "paper_id", "details", "blocking",
        "resolved_at", "created_at", "updated_at",
    }),
}
_MIGRATION_ACTOR = "publishing-migration"


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


def _resolved_regular_pdf(papers_dir: Path, legacy_filename: str) -> tuple[Path | None, str]:
    """Return a safe regular PDF and classification, without unsafe dereference."""
    if (
        not isinstance(legacy_filename, str)
        or not legacy_filename
        or "\\" in legacy_filename
        or any(ord(character) < 32 for character in legacy_filename)
        or Path(legacy_filename).name != legacy_filename
        or Path(legacy_filename).is_absolute()
        or Path(legacy_filename).suffix.casefold() != ".pdf"
    ):
        return None, "unresolved"

    try:
        resolved = resolve_contained(papers_dir, legacy_filename, must_exist=True)
    except (OSError, RuntimeError, ValueError):
        return None, "unresolved"
    if resolved is None:
        # The non-existing resolution performs no read/stat.  It distinguishes a
        # simple missing filename from a symlink/traversal escape.
        try:
            unresolved_candidate = resolve_contained(
                papers_dir, legacy_filename, must_exist=False,
            )
        except (OSError, RuntimeError, ValueError):
            return None, "unresolved"
        return (None, "missing") if unresolved_candidate is not None else (None, "unresolved")
    try:
        regular = resolved.is_file()
    except OSError:
        return None, "unresolved"
    if not regular:
        return None, "unresolved"
    return resolved, "ok"


def _safe_source_details(papers_dir: Path, legacy_filename: str) -> tuple[Path, str, int]:
    source, classification = _resolved_regular_pdf(papers_dir, legacy_filename)
    if source is None:
        reason = "does not exist" if classification == "missing" else "is unsafe or not a regular PDF"
        raise MigrationBlocked(f"legacy PDF {legacy_filename!r} {reason}")

    # Re-resolve immediately before hashing and verify that the source remains a
    # contained regular file.  A later size check detects replacement mid-read.
    source, classification = _resolved_regular_pdf(papers_dir, legacy_filename)
    if source is None or classification != "ok":
        raise MigrationBlocked(f"legacy PDF {legacy_filename!r} changed during verification")
    source_hash, source_size = _hash_file(source)
    source_after, classification = _resolved_regular_pdf(papers_dir, legacy_filename)
    if source_after != source or classification != "ok" or source_after.stat().st_size != source_size:
        raise MigrationBlocked(f"legacy PDF {legacy_filename!r} changed during verification")
    return source, source_hash, source_size


def _flat_pdfs(papers_dir: Path) -> dict[str, Path]:
    entries: dict[str, Path] = {}
    for entry in sorted(papers_dir.iterdir(), key=lambda item: item.name):
        if entry.name == ".publishing-migration-stage" or entry.name.startswith("."):
            continue
        if entry.suffix.casefold() != ".pdf" or entry.name != Path(entry.name).name:
            continue
        resolved = resolve_contained(papers_dir, entry.name, must_exist=True)
        if resolved is None or entry.is_symlink() or not resolved.is_file():
            continue
        entries[entry.name] = resolved
    return entries


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


def _duplicate_chunk_issues(engine: Engine) -> tuple[MigrationIssue, ...]:
    columns = _columns(engine, "papers_chunks")
    if not {"filename", "chunk_index"}.issubset(columns):
        return ()
    revision_expression = "COALESCE(revision_number, 1)" if "revision_number" in columns else "1"
    paper_expression = "COALESCE(paper_id, filename)" if "paper_id" in columns else "filename"
    duplicates = _rows(engine, f"""
        SELECT {paper_expression} AS owner_key,
               filename AS legacy_key,
               {revision_expression} AS revision_number,
               chunk_index,
               COUNT(*) AS duplicate_count
        FROM papers_chunks
        GROUP BY {paper_expression}, filename, {revision_expression}, chunk_index
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
            if "paper_id" in row and row["paper_id"]:
                continue
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


def _mysql_preflight_issues(engine: Engine) -> tuple[MigrationIssue, ...]:
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
    if (
        not (is_baseline or is_expanded)
        or (is_baseline and migration_tables)
        or (is_expanded and migration_tables != set(_EXPANDED_TABLES))
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

    expected_primary_keys = {
        "papers_metadata": ["filename"],
        "papers_chunks": ["id"],
        "submissions": ["id"],
    }
    for table_name, expected_key in expected_primary_keys.items():
        if table_name in table_names:
            primary_key = inspector.get_pk_constraint(table_name)
            if primary_key.get("constrained_columns") != expected_key:
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
        column_contracts.update({
            ("papers_metadata", "id"): ("varchar(36)", "YES"),
            ("papers_metadata", "lifecycle_state"): ("varchar(16)", "YES"),
            ("papers_metadata", "current_revision"): ("int", "YES"),
            ("papers_metadata", "row_version"): ("int", "YES"),
            ("papers_metadata", "index_status"): ("varchar(16)", "YES"),
            ("papers_chunks", "paper_id"): ("varchar(36)", "YES"),
            ("papers_chunks", "revision_number"): ("int", "YES"),
            ("submissions", "paper_id"): ("varchar(36)", "YES"),
        })
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


def run_preflight(engine: Engine, papers_dir: Path) -> PreflightReport:
    """Inventory legacy SQL/files without modifying either storage system."""
    papers_dir = Path(papers_dir)
    if not papers_dir.is_dir():
        raise ValueError(f"papers directory does not exist: {papers_dir}")

    metadata_keys = _legacy_metadata_keys(engine)
    flat_pdfs = _flat_pdfs(papers_dir)
    issues: list[MigrationIssue] = list(_mysql_preflight_issues(engine))
    missing: list[str] = []
    safe_metadata: set[str] = set()
    safe_paths: dict[str, Path] = dict(flat_pdfs)

    for legacy_key in metadata_keys:
        resolved, classification = _resolved_regular_pdf(papers_dir, legacy_key)
        if resolved is None:
            if classification == "missing":
                missing.append(legacy_key)
                issues.append(_issue(
                    "missing_pdf", legacy_key,
                    "metadata row has no contained regular legacy PDF",
                ))
            else:
                issues.append(_issue(
                    "unresolved_filename", legacy_key,
                    "legacy filename is absolute, nested, escaping, symlinked outside, or non-regular",
                ))
            continue
        # Hash only after a second must-exist containment resolution.
        resolved, classification = _resolved_regular_pdf(papers_dir, legacy_key)
        if resolved is None or classification != "ok":
            issues.append(_issue(
                "unresolved_filename", legacy_key,
                "legacy PDF changed while preflight was resolving it",
            ))
            continue
        _hash_file(resolved)
        safe_metadata.add(legacy_key)
        safe_paths[legacy_key] = resolved

    for filename in sorted(set(flat_pdfs) - safe_metadata):
        resolved, classification = _resolved_regular_pdf(papers_dir, filename)
        if resolved is None or classification != "ok":
            # A direct file that changes during inventory is excluded rather
            # than dereferenced through its stale directory entry.
            safe_paths.pop(filename, None)
            flat_pdfs.pop(filename, None)
            issues.append(_issue(
                "unresolved_filename", filename,
                "flat PDF changed while preflight was resolving it",
            ))
            continue
        _hash_file(resolved)

    alias_groups: dict[str, list[str]] = {}
    for filename in sorted(set(safe_paths)):
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

    unique_sizes: dict[Path, int] = {}
    for filename, resolved in safe_paths.items():
        checked = resolve_contained(papers_dir, filename, must_exist=True)
        if checked == resolved and checked.is_file():
            unique_sizes[checked] = checked.stat().st_size
    total_pdf_bytes = sum(unique_sizes.values())

    staging_dir = papers_dir / ".publishing-migration-stage"
    if staging_dir.is_symlink():
        issues.append(_issue(
            "cross_device_staging", ".publishing-migration-stage",
            "staging path must not be a symlink",
        ))
    elif staging_dir.exists():
        resolved_stage = resolve_contained(
            papers_dir, ".publishing-migration-stage", must_exist=True,
        )
        if resolved_stage is None or not resolved_stage.is_dir():
            issues.append(_issue(
                "cross_device_staging", ".publishing-migration-stage",
                "staging path is not a contained directory",
            ))
        elif os.stat(papers_dir).st_dev != os.stat(resolved_stage).st_dev:
            issues.append(_issue(
                "cross_device_staging", ".publishing-migration-stage",
                "staging and Paper storage are on different devices",
            ))

    free_bytes = shutil.disk_usage(papers_dir).free
    required_bytes = total_pdf_bytes * 2
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
        SELECT legacy_key, paper_id, source_sha256, source_size_bytes,
               legacy_chunk_count, legacy_chunk_fingerprint, checkpoint
        FROM publishing_migration_journal
        WHERE legacy_key = :legacy_key
    """, {"legacy_key": legacy_filename})
    return rows[0] if rows else None


def _set_checkpoint(engine: Engine, legacy_filename: str, checkpoint: str) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE publishing_migration_journal
            SET checkpoint = :checkpoint, updated_at = :updated_at
            WHERE legacy_key = :legacy_key
        """), {
            "checkpoint": checkpoint,
            "updated_at": _now(),
            "legacy_key": legacy_filename,
        })


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
            source_hash, source_size, chunk_count, chunk_fingerprint,
        )
        actual = (
            existing["source_sha256"], existing["source_size_bytes"],
            existing["legacy_chunk_count"], existing["legacy_chunk_fingerprint"],
        )
        if actual != expected:
            raise MigrationBlocked(
                f"legacy source or chunks changed after journaling {legacy_filename!r}"
            )
        return existing, True

    paper_id = str(uuid.uuid4())
    now = _now()
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO publishing_migration_journal (
                legacy_key, paper_id, source_sha256, source_size_bytes,
                legacy_chunk_count, legacy_chunk_fingerprint, checkpoint,
                created_at, updated_at
            ) VALUES (
                :legacy_key, :paper_id, :source_sha256, :source_size_bytes,
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
    return _journal(engine, legacy_filename), False


def _contained_directory(papers_dir: Path, relative: str) -> Path:
    candidate = resolve_contained(papers_dir, relative, must_exist=False)
    if candidate is None:
        raise MigrationBlocked(f"unsafe migration directory {relative!r}")
    raw_candidate = papers_dir / relative
    if raw_candidate.exists():
        existing = resolve_contained(papers_dir, relative, must_exist=True)
        if existing is None or raw_candidate.is_symlink() or not existing.is_dir():
            raise MigrationBlocked(f"migration directory {relative!r} is unsafe")
        return existing
    raw_candidate.mkdir(parents=True, exist_ok=False)
    created = resolve_contained(papers_dir, relative, must_exist=True)
    if created is None or raw_candidate.is_symlink() or not created.is_dir():
        raise MigrationBlocked(f"could not create safe migration directory {relative!r}")
    return created


def _verified_existing_file(
    papers_dir: Path,
    relative: str,
    expected_hash: str,
    expected_size: int,
    *,
    discard_mismatch: bool = False,
) -> Path | None:
    candidate = papers_dir / relative
    if candidate.is_symlink():
        raise MigrationBlocked(f"migration destination {relative!r} is a symlink")
    if not candidate.exists():
        return None
    resolved = resolve_contained(papers_dir, relative, must_exist=True)
    if resolved is None or not resolved.is_file():
        raise MigrationBlocked(f"migration destination {relative!r} is unsafe")
    actual_hash, actual_size = _hash_file(resolved)
    if (actual_hash, actual_size) != (expected_hash, expected_size):
        if discard_mismatch:
            resolved.unlink()
            parent_fd = os.open(resolved.parent, os.O_RDONLY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
            return None
        raise MigrationBlocked(
            f"migration destination {relative!r} exists with different bytes"
        )
    return resolved


def _after_copy_verified() -> None:
    """Test seam for simulating interruption after a durable verified copy."""


def _fsync_directory(directory: Path) -> None:
    directory_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _discard_matching_stage_part(
    papers_dir: Path,
    part_relative: str,
    expected_hash: str,
    expected_size: int,
) -> None:
    raw_part = papers_dir / part_relative
    if not raw_part.exists() and not raw_part.is_symlink():
        return
    part = _verified_existing_file(
        papers_dir, part_relative, expected_hash, expected_size,
    )
    if part is None:
        return
    part.unlink()
    _fsync_directory(part.parent)


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
    _contained_directory(papers_dir, ".publishing-migration-stage")
    stage_dir = _contained_directory(papers_dir, stage_relative)
    target_dir = _contained_directory(papers_dir, target_relative)
    part_relative = f"{stage_relative}/1.pdf.part"
    destination_relative = f"{target_relative}/1.pdf"

    destination = _verified_existing_file(
        papers_dir, destination_relative, expected_hash, expected_size,
    )
    if destination is not None:
        _discard_matching_stage_part(
            papers_dir, part_relative, expected_hash, expected_size,
        )
        _fsync_directory(target_dir)
        _set_checkpoint(engine, legacy_filename, "destination_verified")
        return destination

    part = _verified_existing_file(
        papers_dir,
        part_relative,
        expected_hash,
        expected_size,
        discard_mismatch=True,
    )
    copied = False
    if part is None:
        source, source_hash, source_size = _safe_source_details(
            papers_dir, legacy_filename,
        )
        if (source_hash, source_size) != (expected_hash, expected_size):
            raise MigrationBlocked(f"legacy PDF {legacy_filename!r} changed after journaling")
        raw_part = papers_dir / part_relative
        if raw_part.is_symlink():
            raise MigrationBlocked(f"migration stage file {part_relative!r} is a symlink")
        with source.open("rb") as source_stream, raw_part.open("xb") as target_stream:
            shutil.copyfileobj(source_stream, target_stream, length=1024 * 1024)
            target_stream.flush()
            os.fsync(target_stream.fileno())
        part = _verified_existing_file(
            papers_dir, part_relative, expected_hash, expected_size,
        )
        if part is None:
            raise MigrationBlocked("verified staging copy disappeared")
        copied = True

    _set_checkpoint(engine, legacy_filename, "copy_verified")
    if copied:
        _after_copy_verified()

    # Re-resolve both parents and the part immediately before atomic rename.
    safe_stage = resolve_contained(papers_dir, stage_relative, must_exist=True)
    safe_target = resolve_contained(papers_dir, target_relative, must_exist=True)
    safe_part = resolve_contained(papers_dir, part_relative, must_exist=True)
    if (
        safe_stage != stage_dir
        or safe_target != target_dir
        or safe_part != part
        or (papers_dir / part_relative).is_symlink()
    ):
        raise MigrationBlocked("migration staging paths changed before atomic rename")
    raw_destination = papers_dir / destination_relative
    if raw_destination.exists() or raw_destination.is_symlink():
        existing_destination = _verified_existing_file(
            papers_dir, destination_relative, expected_hash, expected_size,
        )
        if existing_destination is None:
            raise MigrationBlocked("existing migration destination disappeared")
        return existing_destination

    # Reserve the absent destination atomically without clobbering a file that
    # appears after the preceding check.  The hard link and part are the same
    # inode; os.replace therefore performs the required atomic rename step
    # without ever replacing different destination bytes.
    try:
        os.link(part, raw_destination)
    except FileExistsError:
        raced_destination = _verified_existing_file(
            papers_dir, destination_relative, expected_hash, expected_size,
        )
        if raced_destination is None:
            raise MigrationBlocked("racing migration destination disappeared")
        _discard_matching_stage_part(
            papers_dir, part_relative, expected_hash, expected_size,
        )
        _fsync_directory(target_dir)
        _set_checkpoint(engine, legacy_filename, "destination_verified")
        return raced_destination
    except OSError as exc:
        raise MigrationBlocked("could not atomically reserve migration destination") from exc

    linked_destination = _verified_existing_file(
        papers_dir, destination_relative, expected_hash, expected_size,
    )
    linked_part = _verified_existing_file(
        papers_dir, part_relative, expected_hash, expected_size,
    )
    if linked_destination is None or linked_part is None:
        raise MigrationBlocked("atomic migration destination reservation changed")
    linked_stat = linked_destination.stat()
    part_stat = linked_part.stat()
    if (linked_stat.st_dev, linked_stat.st_ino) != (part_stat.st_dev, part_stat.st_ino):
        raise MigrationBlocked("migration destination reservation is not the staged inode")
    os.replace(part, raw_destination)
    if (papers_dir / part_relative).exists():
        (papers_dir / part_relative).unlink()
    _fsync_directory(target_dir)
    destination = _verified_existing_file(
        papers_dir, destination_relative, expected_hash, expected_size,
    )
    if destination is None:
        raise MigrationBlocked("published revision disappeared after atomic rename")
    _set_checkpoint(engine, legacy_filename, "destination_verified")
    return destination


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
                    vector_count, captured_at
                ) VALUES (
                    'pre_backfill', :paper_count, :submission_count,
                    :chunk_count, :vector_count, :captured_at
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


def backfill_one_paper(
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
        else:
            code = "submission_unmatched" if not candidates else "submission_ambiguous"
            details = (
                "accepted Submission has no exact nonempty legacy filename match"
                if not candidates
                else "accepted Submission has multiple exact legacy filename matches"
            )
            _persist_issue(engine, code, submission["id"], details)


def backfill_all(engine: Engine, papers_dir: Path) -> tuple[BackfilledPaper, ...]:
    """Backfill every safe metadata/file Paper, then link legacy Submissions."""
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


def validate_contract_ready(engine: Engine, papers_dir: Path) -> PreflightReport:
    """Refuse contraction unless every SQL/file identity invariant is proven."""
    report = run_preflight(engine, papers_dir)
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
            SELECT paper_id, source_sha256, source_size_bytes,
                   legacy_chunk_count, legacy_chunk_fingerprint, checkpoint
            FROM publishing_migration_journal
            WHERE legacy_key = :legacy_key
        """, {"legacy_key": paper["filename"]})
        if (
            len(journal_rows) != 1
            or journal_rows[0]["paper_id"] != paper["id"]
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
        WHERE paper_id IS NULL OR revision_number IS NULL
    """))
    if unmapped_chunks:
        raise MigrationBlocked(f"{unmapped_chunks} legacy chunks remain unmapped")
    if _duplicate_chunk_issues(engine):
        raise MigrationBlocked("duplicate chunk keys remain")

    state = _rows(engine, """
        SELECT paper_count, submission_count, chunk_count, vector_count
        FROM publishing_migration_state WHERE name = 'pre_backfill'
    """)
    if len(state) != 1:
        raise MigrationBlocked("pre-backfill count snapshot is missing")
    expected = state[0]
    _assert_count(engine, "SELECT COUNT(*) FROM papers_metadata",
                  expected["paper_count"], "Paper")
    _assert_count(engine, "SELECT COUNT(*) FROM submissions",
                  expected["submission_count"], "Submission")
    _assert_count(engine, "SELECT COUNT(*) FROM papers_chunks",
                  expected["chunk_count"], "chunk")
    _assert_count(engine, "SELECT COUNT(*) FROM papers_chunks WHERE embedding_vec IS NOT NULL",
                  expected["vector_count"], "vector")
    return report
