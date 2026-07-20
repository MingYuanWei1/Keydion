"""Durable, deterministic PDF storage for immutable Paper revisions."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator

from PyPDF2 import PdfReader, PdfWriter

from services.paper_identity import validate_paper_id
from services.papers import resolve_contained
from services.publishing_contracts import PdfUpload


_BLOCK_SIZE = 1024 * 1024
_OPERATION_ID = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,127})\Z")
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
_CREATE_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)


class StorageError(Exception):
    """A PDF storage invariant could not be satisfied."""


@dataclass(frozen=True)
class StagedPdf:
    path: Path
    source_sha256: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class StoredPdf:
    path: Path
    sha256: str
    size_bytes: int


def _fsync_directory_fd(directory_fd: int) -> None:
    os.fsync(directory_fd)


def _hash_reader(stream: BinaryIO) -> tuple[str, int]:
    stream.seek(0)
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = stream.read(_BLOCK_SIZE)
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            raise StorageError("PDF streams must yield bytes")
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _strict_pdf(stream: BinaryIO) -> int:
    stream.seek(0)
    if stream.read(5) != b"%PDF-":
        raise StorageError("upload is not a PDF")
    stream.seek(0)
    try:
        reader = PdfReader(stream, strict=True)
        page_count = len(reader.pages)
        for page in reader.pages:
            page.get_object()
    except Exception as exc:
        raise StorageError("PDF cannot be parsed strictly") from exc
    if page_count < 1:
        raise StorageError("PDF must contain at least one page")
    return page_count


def _canonical_paper_id(value: str) -> str:
    if not isinstance(value, str):
        raise StorageError("Paper ID must be a UUID string")
    try:
        return validate_paper_id(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise StorageError("invalid Paper ID") from exc


def _valid_revision(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise StorageError("revision must be a positive integer")
    return value


def _valid_operation_id(value: str) -> str:
    if not isinstance(value, str) or _OPERATION_ID.fullmatch(value) is None:
        raise StorageError("invalid storage operation ID")
    return value


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


class PaperStorage:
    """Store staged uploads and immutable revisions behind one filesystem seam."""

    def __init__(self, papers_dir: Path, pending_dir: Path):
        raw_papers = Path(papers_dir)
        raw_pending = Path(pending_dir)
        raw_papers.mkdir(parents=True, exist_ok=True)
        raw_pending.mkdir(parents=True, exist_ok=True)
        self.papers_dir = raw_papers.resolve()
        self.pending_dir = raw_pending.resolve()
        self.staging_dir = self.papers_dir / ".staging"
        self.trash_dir = self.pending_dir / ".trash"
        self.staging_dir.mkdir(exist_ok=True)
        self.trash_dir.mkdir(exist_ok=True)

    def _stage_path(self, operation_id: str) -> Path:
        return self.staging_dir / f"{_valid_operation_id(operation_id)}.pdf"

    def _relative_policy(
        self,
        root: Path,
        value: str,
        *,
        must_exist: bool,
    ) -> tuple[Path, tuple[str, ...]]:
        if (
            not isinstance(value, str)
            or not value
            or "\\" in value
            or "\x00" in value
            or Path(value).is_absolute()
        ):
            raise StorageError("unsafe stored filename")
        parts = Path(value).parts
        if not parts or any(part in ("", ".", "..") for part in parts):
            raise StorageError("unsafe stored filename")
        try:
            resolved = resolve_contained(root, value, must_exist=must_exist)
        except (OSError, RuntimeError, ValueError) as exc:
            raise StorageError("unsafe stored filename") from exc
        if resolved is None:
            raise StorageError("unsafe stored filename")
        return resolved, tuple(parts)

    @contextmanager
    def _opened_regular(
        self,
        root: Path,
        value: str,
    ) -> Iterator[tuple[int, int, str, Path, os.stat_result]]:
        resolved, parts = self._relative_policy(root, value, must_exist=True)
        directory_fds: list[int] = []
        file_fd: int | None = None
        try:
            directory_fds.append(os.open(os.fspath(root), _DIRECTORY_FLAGS))
            for component in parts[:-1]:
                directory_fds.append(
                    os.open(component, _DIRECTORY_FLAGS, dir_fd=directory_fds[-1])
                )
            parent_fd = directory_fds[-1]
            leaf = parts[-1]
            before = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISREG(before.st_mode):
                raise StorageError("stored PDF is not a regular file")
            file_fd = os.open(leaf, _READ_FLAGS, dir_fd=parent_fd)
            opened = os.fstat(file_fd)
            if not stat.S_ISREG(opened.st_mode) or not _same_inode(before, opened):
                raise StorageError("stored PDF changed while opening")
            yield file_fd, parent_fd, leaf, resolved, opened
        except StorageError:
            raise
        except (OSError, RuntimeError, ValueError) as exc:
            raise StorageError("stored PDF could not be opened safely") from exc
        finally:
            if file_fd is not None:
                os.close(file_fd)
            for directory_fd in reversed(directory_fds):
                os.close(directory_fd)

    def _stage_stream(self, stream: BinaryIO, operation_id: str) -> StagedPdf:
        path = self._stage_path(operation_id)
        created = False
        try:
            with path.open("x+b") as target:
                created = True
                stream.seek(0)
                digest = hashlib.sha256()
                size = 0
                while True:
                    block = stream.read(_BLOCK_SIZE)
                    if not block:
                        break
                    if not isinstance(block, bytes):
                        raise StorageError("PDF streams must yield bytes")
                    target.write(block)
                    digest.update(block)
                    size += len(block)
                target.flush()
                os.fsync(target.fileno())
                _strict_pdf(target)
            source_hash = digest.hexdigest()
            return StagedPdf(path, source_hash, source_hash, size)
        except StorageError:
            if created:
                path.unlink(missing_ok=True)
            raise
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            if created:
                path.unlink(missing_ok=True)
            raise StorageError("PDF could not be staged") from exc

    def stage(self, upload: PdfUpload, operation_id: str) -> StagedPdf:
        if not isinstance(upload, PdfUpload):
            raise StorageError("upload must be a PdfUpload")
        return self._stage_stream(upload.stream, operation_id)

    def stage_pending(self, filename: str, operation_id: str) -> StagedPdf:
        with self._opened_regular(self.pending_dir, filename) as (source_fd, _, _, _, _):
            with os.fdopen(os.dup(source_fd), "rb") as source:
                return self._stage_stream(source, operation_id)

    def _validated_stage(self, staged: StagedPdf) -> tuple[Path, str, int]:
        if not isinstance(staged, StagedPdf):
            raise StorageError("expected a staged PDF")
        path = Path(staged.path)
        try:
            relative = path.relative_to(self.staging_dir)
        except ValueError as exc:
            raise StorageError("stage is outside storage") from exc
        if relative.parent != Path(".") or path.suffix != ".pdf":
            raise StorageError("invalid staged PDF path")
        _valid_operation_id(path.stem)
        try:
            with self._opened_regular(self.staging_dir, path.name) as (file_fd, _, _, resolved, _):
                with os.fdopen(os.dup(file_fd), "rb") as source:
                    current_hash, current_size = _hash_reader(source)
        except StorageError:
            raise
        if current_hash != staged.sha256 or current_size != staged.size_bytes:
            raise StorageError("staged PDF bytes changed")
        return resolved, current_hash, current_size

    def apply_metadata(self, staged: StagedPdf, *, title: str, author: str) -> StagedPdf:
        source_path, _source_hash, _source_size = self._validated_stage(staged)
        temporary: Path | None = None
        temporary_fd: int | None = None
        try:
            temporary_fd, temporary_name = tempfile.mkstemp(
                prefix=f"{source_path.stem}.metadata-",
                suffix=".tmp",
                dir=self.staging_dir,
            )
            temporary = Path(temporary_name)
            with self._opened_regular(self.staging_dir, source_path.name) as (source_fd, _, _, _, _):
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    reader = PdfReader(source, strict=True)
                    source_page_count = len(reader.pages)
                    writer = PdfWriter()
                    for page in reader.pages:
                        writer.add_page(page)
                    writer.add_metadata({"/Title": str(title), "/Author": str(author)})
                    with os.fdopen(temporary_fd, "w+b") as output:
                        temporary_fd = None
                        writer.write(output)
                        output.flush()
                        os.fsync(output.fileno())
                        output.seek(0)
                        verified = PdfReader(output, strict=True)
                        if len(verified.pages) != source_page_count:
                            raise StorageError("metadata rewrite changed page count")
                        metadata = verified.metadata
                        if metadata.title != str(title) or metadata.author != str(author):
                            raise StorageError("metadata rewrite verification failed")
            self._validated_stage(staged)
            os.replace(temporary, source_path)
            temporary = None
            with os.fdopen(os.open(source_path, _READ_FLAGS), "rb") as prepared:
                _strict_pdf(prepared)
                stored_hash, stored_size = _hash_reader(prepared)
            directory_fd = os.open(os.fspath(self.staging_dir), _DIRECTORY_FLAGS)
            try:
                _fsync_directory_fd(directory_fd)
            finally:
                os.close(directory_fd)
            return StagedPdf(source_path, staged.source_sha256, stored_hash, stored_size)
        except StorageError:
            raise
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            raise StorageError("PDF metadata could not be written") from exc
        finally:
            if temporary_fd is not None:
                os.close(temporary_fd)
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def revision_path(self, paper_id: str, revision: int) -> Path:
        canonical = _canonical_paper_id(paper_id)
        number = _valid_revision(revision)
        return self.papers_dir / canonical / f"{number}.pdf"

    def _open_paper_directory(self, paper_id: str, *, create: bool) -> tuple[int, int, str]:
        canonical = _canonical_paper_id(paper_id)
        root_fd: int | None = None
        paper_fd: int | None = None
        try:
            root_fd = os.open(os.fspath(self.papers_dir), _DIRECTORY_FLAGS)
            if create:
                try:
                    os.mkdir(canonical, 0o700, dir_fd=root_fd)
                except FileExistsError:
                    pass
            paper_fd = os.open(canonical, _DIRECTORY_FLAGS, dir_fd=root_fd)
            return root_fd, paper_fd, canonical
        except (OSError, RuntimeError, ValueError) as exc:
            if paper_fd is not None:
                os.close(paper_fd)
            if root_fd is not None:
                os.close(root_fd)
            raise StorageError("Paper revision directory is unsafe") from exc

    def promote(self, staged: StagedPdf, paper_id: str, revision: int) -> StoredPdf:
        source_path, source_hash, source_size = self._validated_stage(staged)
        number = _valid_revision(revision)
        root_fd, paper_fd, canonical = self._open_paper_directory(paper_id, create=True)
        destination_name = f"{number}.pdf"
        destination = self.papers_dir / canonical / destination_name
        staging_fd: int | None = None
        temporary_name = f".promote-{source_path.stem}-{canonical}-{number}.pdf"
        moved_to_temporary = False
        reservation_stat: os.stat_result | None = None
        try:
            try:
                existing_fd = os.open(destination_name, _READ_FLAGS, dir_fd=paper_fd)
            except FileNotFoundError:
                existing_fd = None
            except OSError as exc:
                raise StorageError("existing revision is unsafe") from exc
            if existing_fd is not None:
                try:
                    opened = os.fstat(existing_fd)
                    if not stat.S_ISREG(opened.st_mode):
                        raise StorageError("existing revision is not a regular file")
                    with os.fdopen(os.dup(existing_fd), "rb") as existing:
                        existing_hash, existing_size = _hash_reader(existing)
                finally:
                    os.close(existing_fd)
                if (existing_hash, existing_size) != (source_hash, source_size):
                    raise StorageError("immutable revision already contains different bytes")
                source_path.unlink(missing_ok=True)
                staging_fd = os.open(os.fspath(self.staging_dir), _DIRECTORY_FLAGS)
                try:
                    _fsync_directory_fd(staging_fd)
                finally:
                    os.close(staging_fd)
                    staging_fd = None
                return StoredPdf(destination, existing_hash, existing_size)

            try:
                staging_fd = os.open(os.fspath(self.staging_dir), _DIRECTORY_FLAGS)
                reservation_fd = os.open(
                    temporary_name,
                    _CREATE_FLAGS,
                    0o600,
                    dir_fd=staging_fd,
                )
                reservation_stat = os.fstat(reservation_fd)
            except FileExistsError as exc:
                raise StorageError("stale promotion stage already exists") from exc
            try:
                os.close(reservation_fd)
                os.replace(
                    source_path.name,
                    temporary_name,
                    src_dir_fd=staging_fd,
                    dst_dir_fd=staging_fd,
                )
                moved_to_temporary = True
                temporary_fd = os.open(temporary_name, _READ_FLAGS, dir_fd=staging_fd)
                try:
                    with os.fdopen(os.dup(temporary_fd), "rb") as temporary:
                        moved_hash, moved_size = _hash_reader(temporary)
                finally:
                    os.close(temporary_fd)
                if (moved_hash, moved_size) != (source_hash, source_size):
                    raise StorageError("staged PDF changed during promotion")

                try:
                    os.link(
                        temporary_name,
                        destination_name,
                        src_dir_fd=staging_fd,
                        dst_dir_fd=paper_fd,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    existing_fd = os.open(destination_name, _READ_FLAGS, dir_fd=paper_fd)
                    try:
                        with os.fdopen(os.dup(existing_fd), "rb") as existing:
                            existing_hash, existing_size = _hash_reader(existing)
                    finally:
                        os.close(existing_fd)
                    if (existing_hash, existing_size) != (source_hash, source_size):
                        raise StorageError(
                            "immutable revision appeared with different bytes"
                        )
                _fsync_directory_fd(paper_fd)
                os.unlink(temporary_name, dir_fd=staging_fd)
                moved_to_temporary = False
                reservation_stat = None
                _fsync_directory_fd(staging_fd)
            finally:
                if moved_to_temporary:
                    try:
                        os.link(
                            temporary_name,
                            source_path.name,
                            src_dir_fd=staging_fd,
                            dst_dir_fd=staging_fd,
                            follow_symlinks=False,
                        )
                    except FileExistsError:
                        pass
                    else:
                        os.unlink(temporary_name, dir_fd=staging_fd)
                        moved_to_temporary = False
                        reservation_stat = None
                elif reservation_stat is not None:
                    try:
                        current = os.stat(
                            temporary_name,
                            dir_fd=staging_fd,
                            follow_symlinks=False,
                        )
                        if _same_inode(current, reservation_stat):
                            os.unlink(temporary_name, dir_fd=staging_fd)
                    except FileNotFoundError:
                        pass
            return StoredPdf(destination, source_hash, source_size)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("PDF revision could not be promoted") from exc
        finally:
            if staging_fd is not None:
                os.close(staging_fd)
            os.close(paper_fd)
            os.close(root_fd)

    def open_revision(self, paper_id: str, revision: int) -> Path:
        path = self.revision_path(paper_id, revision)
        relative = f"{path.parent.name}/{path.name}"
        try:
            with self._opened_regular(self.papers_dir, relative):
                return path
        except StorageError as exc:
            raise StorageError("Paper revision does not exist") from exc

    def copy_revision(
        self,
        paper_id: str,
        *,
        source_revision: int,
        target_revision: int,
        operation_id: str,
        title: str,
        author: str,
    ) -> StoredPdf:
        source = self.open_revision(paper_id, source_revision)
        relative = f"{source.parent.name}/{source.name}"
        with self._opened_regular(self.papers_dir, relative) as (source_fd, _, _, _, _):
            with os.fdopen(os.dup(source_fd), "rb") as stream:
                staged = self._stage_stream(stream, operation_id)
        prepared = self.apply_metadata(staged, title=title, author=author)
        return self.promote(prepared, paper_id, target_revision)

    def _legacy_names(self, filenames: Iterable[str]) -> tuple[str, ...]:
        validated: list[str] = []
        for filename in filenames:
            if (
                not isinstance(filename, str)
                or not filename
                or Path(filename).name != filename
                or Path(filename).suffix.casefold() != ".pdf"
            ):
                raise StorageError("unsafe retained legacy filename")
            try:
                resolved = resolve_contained(self.papers_dir, filename, must_exist=False)
            except (OSError, RuntimeError, ValueError) as exc:
                raise StorageError("unsafe retained legacy filename") from exc
            if resolved is None:
                raise StorageError("unsafe retained legacy filename")
            try:
                info = resolved.lstat()
            except FileNotFoundError:
                pass
            else:
                if not stat.S_ISREG(info.st_mode):
                    raise StorageError("legacy PDF is not a regular file")
            validated.append(filename)
        return tuple(validated)

    def _clear_directory(self, directory_fd: int) -> None:
        for name in os.listdir(directory_fd):
            try:
                before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISDIR(before.st_mode):
                child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_fd)
                try:
                    opened = os.fstat(child_fd)
                    if not _same_inode(before, opened):
                        raise StorageError("Paper directory changed during deletion")
                    self._clear_directory(child_fd)
                finally:
                    os.close(child_fd)
                try:
                    os.rmdir(name, dir_fd=directory_fd)
                except FileNotFoundError:
                    pass
            else:
                try:
                    os.unlink(name, dir_fd=directory_fd)
                except FileNotFoundError:
                    pass

    def delete_paper(self, paper_id: str, retained_legacy_filenames: Iterable[str]) -> None:
        canonical = _canonical_paper_id(paper_id)
        legacy_names = self._legacy_names(retained_legacy_filenames)
        root_fd = os.open(os.fspath(self.papers_dir), _DIRECTORY_FLAGS)
        try:
            try:
                paper_fd = os.open(canonical, _DIRECTORY_FLAGS, dir_fd=root_fd)
            except FileNotFoundError:
                paper_fd = None
            except OSError as exc:
                raise StorageError("Paper revision directory is unsafe") from exc
            if paper_fd is not None:
                try:
                    self._clear_directory(paper_fd)
                finally:
                    os.close(paper_fd)
                try:
                    os.rmdir(canonical, dir_fd=root_fd)
                except FileNotFoundError:
                    pass
            for filename in legacy_names:
                try:
                    os.unlink(filename, dir_fd=root_fd)
                except FileNotFoundError:
                    pass
                except IsADirectoryError as exc:
                    raise StorageError("legacy PDF is not a regular file") from exc
            _fsync_directory_fd(root_fd)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("Paper files could not be deleted") from exc
        finally:
            os.close(root_fd)

    def _reserve_and_replace(
        self,
        source_parent_fd: int,
        source_name: str,
        source_stat: os.stat_result,
        destination_parent_fd: int,
        destination_name: str,
    ) -> None:
        reservation_fd: int | None = None
        reservation_stat: os.stat_result | None = None
        try:
            reservation_fd = os.open(
                destination_name,
                _CREATE_FLAGS,
                0o600,
                dir_fd=destination_parent_fd,
            )
            reservation_stat = os.fstat(reservation_fd)
            os.close(reservation_fd)
            reservation_fd = None
            current = os.stat(source_name, dir_fd=source_parent_fd, follow_symlinks=False)
            if not stat.S_ISREG(current.st_mode) or not _same_inode(current, source_stat):
                raise StorageError("pending PDF changed before move")
            os.replace(
                source_name,
                destination_name,
                src_dir_fd=source_parent_fd,
                dst_dir_fd=destination_parent_fd,
            )
            moved = os.stat(destination_name, dir_fd=destination_parent_fd, follow_symlinks=False)
            if not _same_inode(moved, source_stat):
                raise StorageError("pending PDF changed during move")
            _fsync_directory_fd(source_parent_fd)
            if destination_parent_fd != source_parent_fd:
                _fsync_directory_fd(destination_parent_fd)
        except Exception:
            if reservation_fd is not None:
                os.close(reservation_fd)
            if reservation_stat is not None:
                try:
                    current = os.stat(
                        destination_name,
                        dir_fd=destination_parent_fd,
                        follow_symlinks=False,
                    )
                    if _same_inode(current, reservation_stat):
                        os.unlink(destination_name, dir_fd=destination_parent_fd)
                except FileNotFoundError:
                    pass
            raise

    def trash_pending(self, filename: str, operation_id: str) -> tuple[Path, Path]:
        trash_name = f"{_valid_operation_id(operation_id)}.pdf"
        trash_fd = os.open(os.fspath(self.trash_dir), _DIRECTORY_FLAGS)
        try:
            with self._opened_regular(self.pending_dir, filename) as (
                _source_fd,
                source_parent_fd,
                source_name,
                original,
                source_stat,
            ):
                self._reserve_and_replace(
                    source_parent_fd,
                    source_name,
                    source_stat,
                    trash_fd,
                    trash_name,
                )
            return original, self.trash_dir / trash_name
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending PDF could not be trashed") from exc
        finally:
            os.close(trash_fd)

    def _pending_relative_path(self, value: Path, *, must_exist: bool) -> tuple[Path, str]:
        path = Path(value)
        try:
            relative = path.relative_to(self.pending_dir)
        except ValueError as exc:
            raise StorageError("pending path is outside storage") from exc
        relative_name = relative.as_posix()
        resolved, _parts = self._relative_policy(
            self.pending_dir,
            relative_name,
            must_exist=must_exist,
        )
        return resolved, relative_name

    def _open_destination_parent(self, relative_name: str) -> tuple[list[int], int, str]:
        parts = Path(relative_name).parts
        fds = [os.open(os.fspath(self.pending_dir), _DIRECTORY_FLAGS)]
        try:
            for component in parts[:-1]:
                fds.append(os.open(component, _DIRECTORY_FLAGS, dir_fd=fds[-1]))
            return fds, fds[-1], parts[-1]
        except Exception:
            for directory_fd in reversed(fds):
                os.close(directory_fd)
            raise

    def restore_pending(self, original: Path, trashed: Path) -> None:
        _original_path, original_relative = self._pending_relative_path(
            original,
            must_exist=False,
        )
        trashed_path, trashed_relative = self._pending_relative_path(
            trashed,
            must_exist=True,
        )
        if Path(original_relative).parts[0] == ".trash":
            raise StorageError("original pending path cannot be trash")
        if Path(trashed_relative).parent != Path(".trash"):
            raise StorageError("trashed pending path is invalid")
        destination_fds: list[int] = []
        try:
            destination_fds, destination_parent_fd, destination_name = (
                self._open_destination_parent(original_relative)
            )
            with self._opened_regular(self.pending_dir, trashed_relative) as (
                _source_fd,
                source_parent_fd,
                source_name,
                _resolved,
                source_stat,
            ):
                self._reserve_and_replace(
                    source_parent_fd,
                    source_name,
                    source_stat,
                    destination_parent_fd,
                    destination_name,
                )
        except FileExistsError as exc:
            raise StorageError("original pending path is occupied") from exc
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending PDF could not be restored") from exc
        finally:
            for directory_fd in reversed(destination_fds):
                os.close(directory_fd)

    @staticmethod
    def _cutoff_timestamp(cutoff: datetime | float | int) -> float:
        if isinstance(cutoff, datetime):
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=timezone.utc)
            return cutoff.timestamp()
        if isinstance(cutoff, bool) or not isinstance(cutoff, (int, float)):
            raise StorageError("invalid reconciliation cutoff")
        return float(cutoff)

    def _remove_stale_entries(self, directory: Path, cutoff: float) -> int:
        removed = 0
        directory_fd = os.open(os.fspath(directory), _DIRECTORY_FLAGS)
        try:
            for name in os.listdir(directory_fd):
                try:
                    entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if entry.st_mtime >= cutoff or stat.S_ISDIR(entry.st_mode):
                    continue
                try:
                    resolved = resolve_contained(directory, name, must_exist=True)
                except (OSError, RuntimeError, ValueError):
                    resolved = None
                if resolved is None:
                    continue
                current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if not _same_inode(entry, current):
                    continue
                os.unlink(name, dir_fd=directory_fd)
                removed += 1
            if removed:
                _fsync_directory_fd(directory_fd)
            return removed
        finally:
            os.close(directory_fd)

    def reconcile_expired(
        self,
        cutoff: datetime | float | int,
        referenced_revisions: Iterable[tuple[str, int]],
    ) -> int:
        cutoff_timestamp = self._cutoff_timestamp(cutoff)
        referenced: set[tuple[str, int]] = set()
        for paper_id, revision in referenced_revisions:
            referenced.add((_canonical_paper_id(paper_id), _valid_revision(revision)))

        removed = self._remove_stale_entries(self.staging_dir, cutoff_timestamp)
        removed += self._remove_stale_entries(self.trash_dir, cutoff_timestamp)
        root_fd = os.open(os.fspath(self.papers_dir), _DIRECTORY_FLAGS)
        try:
            for paper_id in os.listdir(root_fd):
                try:
                    if _canonical_paper_id(paper_id) != paper_id:
                        continue
                except StorageError:
                    continue
                try:
                    paper_fd = os.open(paper_id, _DIRECTORY_FLAGS, dir_fd=root_fd)
                except (FileNotFoundError, NotADirectoryError, OSError):
                    continue
                paper_removed = 0
                try:
                    for filename in os.listdir(paper_fd):
                        if not filename.endswith(".pdf"):
                            continue
                        stem = filename[:-4]
                        if not stem.isdigit() or stem.startswith("0"):
                            continue
                        revision = int(stem)
                        if (paper_id, revision) in referenced:
                            continue
                        relative = f"{paper_id}/{filename}"
                        try:
                            resolved = resolve_contained(
                                self.papers_dir,
                                relative,
                                must_exist=True,
                            )
                        except (OSError, RuntimeError, ValueError):
                            resolved = None
                        if resolved is None:
                            continue
                        before = os.stat(filename, dir_fd=paper_fd, follow_symlinks=False)
                        if not stat.S_ISREG(before.st_mode):
                            continue
                        current = os.stat(filename, dir_fd=paper_fd, follow_symlinks=False)
                        if not _same_inode(before, current):
                            continue
                        os.unlink(filename, dir_fd=paper_fd)
                        removed += 1
                        paper_removed += 1
                    if paper_removed:
                        _fsync_directory_fd(paper_fd)
                finally:
                    os.close(paper_fd)
                try:
                    os.rmdir(paper_id, dir_fd=root_fd)
                except OSError:
                    pass
            return removed
        except OSError as exc:
            raise StorageError("storage reconciliation failed") from exc
        finally:
            os.close(root_fd)
