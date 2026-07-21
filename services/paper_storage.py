"""Durable, deterministic PDF storage for immutable Paper revisions."""

from __future__ import annotations

import hashlib
import ctypes
import errno
import fcntl
import functools
import json
import math
import os
import re
import secrets
import stat
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Callable, Iterable, Iterator

from PyPDF2 import PdfReader, PdfWriter

from services.paper_identity import validate_paper_id
from services.papers import resolve_contained
from services.publishing_contracts import PdfUpload


_BLOCK_SIZE = 1024 * 1024
_OPERATION_ID = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,127})\Z")
_SUBMISSION_TRASH_ENTRY = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_FORMER_SUBMISSION_TRASH_OPERATION = re.compile(
    r"submission-[0-9a-f]{64}\Z",
    re.ASCII,
)
_SUBMISSION_TRASH_DIRECTORY = "submissions-v2"
_SUBMISSION_TRASH_QUARANTINE_DIRECTORY = "submissions-v1-quarantine"
_SUBMISSION_TRASH_OWNER = "owner.json"
_SUBMISSION_TRASH_PAYLOAD = "payload.pdf"
_REVISION_FILENAME = re.compile(r"[1-9][0-9]*\.pdf\Z", re.ASCII)
_METADATA_BACKUP = re.compile(
    r"(?P<operation>[A-Za-z0-9](?:[A-Za-z0-9._-]{0,127}))"
    r"\.metadata-backup-[0-9a-f]{32}\.tmp\Z",
    re.ASCII,
)
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
_CREATE_RW_FLAGS = (_CREATE_FLAGS & ~os.O_WRONLY) | os.O_RDWR
_PROCESS_LOCKS: dict[tuple[int, int], threading.RLock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()


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


@dataclass(frozen=True)
class PendingTrash:
    """Process-local one-use authority for one audited pending-file trash.

    Tokens are intentionally not restart-persistent. Recovery reopens and
    verifies the deterministic private trash entry before issuing a new token;
    database text is never treated as filesystem authority.
    """

    original_name: str
    operation_id: str
    source_sha256: str
    size_bytes: int
    device: int
    inode: int
    namespace: str
    entry_name: str
    _capability: str = field(repr=False)


@dataclass(frozen=True)
class SubmissionTrashRecord:
    """Durable exact owner provenance for one versioned Submission trash entry."""

    submission_id: str
    original_name: str
    entry_name: str
    has_payload: bool
    modified_at: float


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


def _private_mode(info: os.stat_result, expected: int) -> bool:
    return stat.S_IMODE(info.st_mode) == expected and info.st_uid == os.getuid()


def _root_process_lock(info: os.stat_result) -> threading.RLock:
    key = (info.st_dev, info.st_ino)
    with _PROCESS_LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(key, threading.RLock())


def _publish_verified_fd_no_replace(
    source_fd: int,
    target_fd: int,
    destination_name: str,
) -> None:
    """Publish the exact open inode without replacing an existing destination."""
    libc = ctypes.CDLL(None, use_errno=True)
    encoded_destination = os.fsencode(destination_name)

    def failed(operation: str) -> None:
        error_number = ctypes.get_errno()
        if error_number == errno.EEXIST:
            raise FileExistsError(error_number, os.strerror(error_number), destination_name)
        raise OSError(error_number, f"{operation}: {os.strerror(error_number)}")

    if sys.platform.startswith("linux"):
        linkat = getattr(libc, "linkat", None)
        if linkat is None:
            raise StorageError("platform has no exact-FD publication primitive")
        linkat.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
        )
        linkat.restype = ctypes.c_int
        ctypes.set_errno(0)
        if linkat(source_fd, b"", target_fd, encoded_destination, 0x1000) == 0:
            return
        first_error = ctypes.get_errno()
        if first_error == errno.EEXIST:
            failed("linkat(AT_EMPTY_PATH)")
        descriptor_path = f"/proc/self/fd/{source_fd}"
        if not os.path.isdir("/proc/self/fd"):
            ctypes.set_errno(first_error)
            failed("linkat(AT_EMPTY_PATH)")
        ctypes.set_errno(0)
        if linkat(
            -100,
            os.fsencode(descriptor_path),
            target_fd,
            encoded_destination,
            0x400,
        ) == 0:
            return
        failed("linkat(descriptor)")

    if sys.platform == "darwin":
        fclonefileat = getattr(libc, "fclonefileat", None)
        if fclonefileat is None:
            raise StorageError("platform has no exact-FD publication primitive")
        fclonefileat.argtypes = (
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint32,
        )
        fclonefileat.restype = ctypes.c_int
        ctypes.set_errno(0)
        if fclonefileat(source_fd, target_fd, encoded_destination, 0) == 0:
            return
        failed("fclonefileat")

    raise StorageError("platform has no exact-FD publication primitive")


def _serialized(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        with self._storage_lock():
            return method(self, *args, **kwargs)

    return wrapped


class PaperStorage:
    """Store staged uploads and immutable revisions behind one filesystem seam.

    Roots are private app-owned namespaces. The root-scoped file/process lock
    serializes cooperating app instances; it is not a security boundary against
    arbitrary code already running as the app owner. Database-supplied names
    still receive descriptor-relative no-follow checks at the actual operation.
    """

    def __init__(self, papers_dir: Path, pending_dir: Path):
        self._closed = False
        self._lock_state = threading.local()
        self._trash_tokens: dict[str, PendingTrash] = {}
        self.papers_dir, self._papers_fd, self._papers_stat = self._open_private_root(
            Path(papers_dir), "Paper"
        )
        try:
            self.pending_dir, self._pending_fd, self._pending_stat = self._open_private_root(
                Path(pending_dir), "pending"
            )
        except Exception:
            os.close(self._papers_fd)
            self._papers_fd = None
            self._closed = True
            raise
        if (
            _same_inode(self._papers_stat, self._pending_stat)
            or self.papers_dir == self.pending_dir
            or self.papers_dir in self.pending_dir.parents
            or self.pending_dir in self.papers_dir.parents
        ):
            os.close(self._pending_fd)
            os.close(self._papers_fd)
            self._pending_fd = None
            self._papers_fd = None
            self._closed = True
            raise StorageError("Paper and pending storage roots must be disjoint")
        self._process_lock = _root_process_lock(self._papers_stat)
        self.staging_dir = self.papers_dir / ".staging"
        self.trash_dir = self.pending_dir / ".trash"
        self.submission_trash_dir = self.trash_dir / _SUBMISSION_TRASH_DIRECTORY
        self.submission_trash_quarantine_dir = (
            self.trash_dir / _SUBMISSION_TRASH_QUARANTINE_DIRECTORY
        )
        self._lock_fd = self._open_lock_file()
        try:
            with self._process_lock:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
                try:
                    self._staging_fd, self._staging_stat = self._open_private_directory(
                        self._papers_fd, ".staging"
                    )
                    try:
                        self._trash_fd, self._trash_stat = self._open_private_directory(
                            self._pending_fd, ".trash"
                        )
                        try:
                            (
                                self._submission_trash_fd,
                                self._submission_trash_stat,
                            ) = self._open_private_directory(
                                self._trash_fd,
                                _SUBMISSION_TRASH_DIRECTORY,
                            )
                            try:
                                (
                                    self._submission_trash_quarantine_fd,
                                    self._submission_trash_quarantine_stat,
                                ) = self._open_private_directory(
                                    self._trash_fd,
                                    _SUBMISSION_TRASH_QUARANTINE_DIRECTORY,
                                )
                            except Exception:
                                os.close(self._submission_trash_fd)
                                raise
                        except Exception:
                            os.close(self._trash_fd)
                            raise
                    except Exception:
                        os.close(self._staging_fd)
                        raise
                finally:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        except Exception:
            os.close(self._lock_fd)
            os.close(self._pending_fd)
            os.close(self._papers_fd)
            self._lock_fd = None
            self._pending_fd = None
            self._papers_fd = None
            self._closed = True
            raise

    @staticmethod
    def _open_private_root(path: Path, label: str) -> tuple[Path, int, os.stat_result]:
        absolute = Path(os.path.abspath(os.fspath(path)))
        try:
            absolute.mkdir(parents=True, mode=0o700)
        except FileExistsError:
            pass
        try:
            before = absolute.lstat()
            if not stat.S_ISDIR(before.st_mode) or not _private_mode(before, 0o700):
                raise StorageError(f"{label} storage root is not a private owned directory")
            root_fd = os.open(os.fspath(absolute), _DIRECTORY_FLAGS)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError(f"{label} storage root is unsafe") from exc
        try:
            opened = os.fstat(root_fd)
            if not _same_inode(before, opened) or opened.st_dev != before.st_dev:
                raise StorageError(f"{label} storage root changed while opening")
            return absolute.resolve(), root_fd, opened
        except Exception:
            os.close(root_fd)
            raise

    @staticmethod
    def _open_private_directory(
        parent_fd: int,
        name: str,
    ) -> tuple[int, os.stat_result]:
        created = False
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISDIR(before.st_mode) or not _private_mode(before, 0o700):
                raise StorageError(f"reserved directory {name!r} is unsafe")
            directory_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError(f"reserved directory {name!r} is unsafe") from exc
        try:
            opened = os.fstat(directory_fd)
            if not _same_inode(before, opened):
                raise StorageError(f"reserved directory {name!r} changed while opening")
            if created:
                os.fsync(parent_fd)
            return directory_fd, opened
        except Exception:
            os.close(directory_fd)
            raise

    def _open_lock_file(self) -> int:
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(
            os, "O_NOFOLLOW", 0
        )
        try:
            lock_fd = os.open(".paper-storage.lock", flags, 0o600, dir_fd=self._papers_fd)
        except OSError as exc:
            raise StorageError("storage lock file is unsafe") from exc
        try:
            info = os.fstat(lock_fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or not _private_mode(info, 0o600)
                or info.st_nlink != 1
                or info.st_dev != self._papers_stat.st_dev
            ):
                raise StorageError("storage lock file is unsafe")
            os.fsync(self._papers_fd)
            return lock_fd
        except Exception:
            os.close(lock_fd)
            raise

    def _verify_root_path(self, path: Path, expected: os.stat_result, label: str) -> None:
        try:
            current = path.lstat()
        except OSError as exc:
            raise StorageError(f"{label} storage root disappeared") from exc
        if not stat.S_ISDIR(current.st_mode) or not _same_inode(current, expected):
            raise StorageError(f"{label} storage root changed")

    def _verify_reserved_directory(
        self,
        parent_fd: int,
        name: str,
        expected: os.stat_result,
    ) -> None:
        try:
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise StorageError(f"reserved directory {name!r} disappeared") from exc
        if (
            not stat.S_ISDIR(current.st_mode)
            or not _private_mode(current, 0o700)
            or not _same_inode(current, expected)
        ):
            raise StorageError(f"reserved directory {name!r} changed")

    @contextmanager
    def _storage_lock(self):
        with self._process_lock:
            depth = getattr(self._lock_state, "depth", 0)
            if depth == 0:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
            self._lock_state.depth = depth + 1
            try:
                self._verify_root_path(self.papers_dir, self._papers_stat, "Paper")
                self._verify_root_path(self.pending_dir, self._pending_stat, "pending")
                self._verify_reserved_directory(
                    self._papers_fd, ".staging", self._staging_stat
                )
                self._verify_reserved_directory(self._pending_fd, ".trash", self._trash_stat)
                self._verify_reserved_directory(
                    self._trash_fd,
                    _SUBMISSION_TRASH_DIRECTORY,
                    self._submission_trash_stat,
                )
                self._verify_reserved_directory(
                    self._trash_fd,
                    _SUBMISSION_TRASH_QUARANTINE_DIRECTORY,
                    self._submission_trash_quarantine_stat,
                )
                yield
            finally:
                self._lock_state.depth -= 1
                if depth == 0:
                    fcntl.flock(self._lock_fd, fcntl.LOCK_UN)

    def close(self) -> None:
        if getattr(self, "_closed", True):
            return
        self._closed = True
        for name in (
            "_submission_trash_quarantine_fd",
            "_submission_trash_fd",
            "_trash_fd",
            "_staging_fd",
            "_lock_fd",
            "_pending_fd",
            "_papers_fd",
        ):
            descriptor = getattr(self, name, None)
            if descriptor is not None:
                os.close(descriptor)

    def __del__(self):
        if hasattr(self, "_closed"):
            try:
                self.close()
            except OSError:
                pass

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

    def _validate_pending_ingress(self, filename: str) -> None:
        _resolved, parts = self._relative_policy(
            self.pending_dir,
            filename,
            must_exist=True,
        )
        if parts[0] == ".trash":
            raise StorageError("pending ingress cannot address reserved trash storage")
        first_fd: int | None = None
        try:
            before = os.stat(
                parts[0],
                dir_fd=self._pending_fd,
                follow_symlinks=False,
            )
            if not stat.S_ISDIR(before.st_mode):
                return
            first_fd = os.open(parts[0], _DIRECTORY_FLAGS, dir_fd=self._pending_fd)
            opened = os.fstat(first_fd)
            if not _same_inode(before, opened):
                raise StorageError("pending ingress first component changed")
            if _same_inode(opened, self._trash_stat):
                raise StorageError("pending ingress cannot address reserved trash storage")
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending ingress first component is unsafe") from exc
        finally:
            if first_fd is not None:
                os.close(first_fd)

    def _validate_pending_recovery_name(self, filename: str) -> None:
        """Validate a persisted original name even when its leaf is absent."""
        _resolved, parts = self._relative_policy(
            self.pending_dir,
            filename,
            must_exist=False,
        )
        if parts[0] == ".trash":
            raise StorageError("pending ingress cannot address reserved trash storage")
        if len(parts) == 1:
            # Containment/symlink resolution above is sufficient for a leaf.
            # The descriptor-relative open/move remains the authority check.
            return
        first_fd: int | None = None
        try:
            before = os.stat(
                parts[0],
                dir_fd=self._pending_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            if len(parts) != 1:
                raise StorageError("pending destination parent is missing")
            return
        except OSError as exc:
            raise StorageError("pending ingress first component is unsafe") from exc
        try:
            if not stat.S_ISDIR(before.st_mode):
                if len(parts) != 1:
                    raise StorageError("pending destination parent is unsafe")
                return
            first_fd = os.open(parts[0], _DIRECTORY_FLAGS, dir_fd=self._pending_fd)
            opened = os.fstat(first_fd)
            if not _same_inode(before, opened):
                raise StorageError("pending ingress first component changed")
            if _same_inode(opened, self._trash_stat):
                raise StorageError("pending ingress cannot address reserved trash storage")
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending ingress first component is unsafe") from exc
        finally:
            if first_fd is not None:
                os.close(first_fd)

    @contextmanager
    def _opened_regular(
        self,
        root: Path,
        value: str,
        *,
        require_single_link: bool = False,
        require_private: bool = False,
    ) -> Iterator[tuple[int, int, str, Path, os.stat_result]]:
        resolved, parts = self._relative_policy(root, value, must_exist=True)
        directory_fds: list[int] = []
        file_fd: int | None = None
        try:
            if root == self.papers_dir:
                base_fd = self._papers_fd
            elif root == self.pending_dir:
                base_fd = self._pending_fd
            elif root == self.staging_dir:
                base_fd = self._staging_fd
            elif root == self.trash_dir:
                base_fd = self._trash_fd
            elif root == self.submission_trash_dir:
                base_fd = self._submission_trash_fd
            elif root == self.submission_trash_quarantine_dir:
                base_fd = self._submission_trash_quarantine_fd
            else:
                raise StorageError("unknown storage root")
            directory_fds.append(os.dup(base_fd))
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
            if (
                not stat.S_ISREG(opened.st_mode)
                or not _same_inode(before, opened)
                or (require_single_link and opened.st_nlink != 1)
                or (require_private and not _private_mode(opened, 0o600))
            ):
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
        name = path.name
        created: os.stat_result | None = None
        created_entry = False
        stage_fd: int | None = None
        try:
            stage_fd = os.open(name, _CREATE_RW_FLAGS, 0o600, dir_fd=self._staging_fd)
            created_entry = True
            created = os.fstat(stage_fd)
            if not stat.S_ISREG(created.st_mode) or not _private_mode(created, 0o600):
                raise StorageError("new stage is not a private regular file")
            with os.fdopen(stage_fd, "w+b") as target:
                stage_fd = None
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
            os.fsync(self._staging_fd)
            source_hash = digest.hexdigest()
            return StagedPdf(path, source_hash, source_hash, size)
        except StorageError:
            if created is not None:
                self._unlink_if_matching(self._staging_fd, name, created)
            elif created_entry:
                try:
                    os.unlink(name, dir_fd=self._staging_fd)
                except FileNotFoundError:
                    pass
            raise
        except Exception as exc:
            if created is not None:
                self._unlink_if_matching(self._staging_fd, name, created)
            elif created_entry:
                try:
                    os.unlink(name, dir_fd=self._staging_fd)
                except FileNotFoundError:
                    pass
            raise StorageError("PDF could not be staged") from exc
        finally:
            if stage_fd is not None:
                os.close(stage_fd)

    @staticmethod
    def _unlink_if_matching(
        directory_fd: int,
        name: str,
        expected: os.stat_result,
    ) -> bool:
        try:
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if not _same_inode(current, expected):
            return False
        os.unlink(name, dir_fd=directory_fd)
        return True

    @_serialized
    def stage(self, upload: PdfUpload, operation_id: str) -> StagedPdf:
        if not isinstance(upload, PdfUpload):
            raise StorageError("upload must be a PdfUpload")
        return self._stage_stream(upload.stream, operation_id)

    @_serialized
    def discard_stage(self, staged: StagedPdf) -> None:
        """Discard only the exact verified operation stage, if it still exists."""
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
            expected = os.stat(
                path.name,
                dir_fd=self._staging_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return
        except OSError as exc:
            raise StorageError("staged PDF could not be inspected") from exc
        try:
            with self._opened_regular(
                self.staging_dir,
                path.name,
                require_private=True,
            ) as (stage_fd, _, _, _, opened):
                if not _same_inode(expected, opened):
                    raise StorageError("staged PDF changed before discard")
                if opened.st_nlink == 2:
                    publication_links = self._publication_link_identities()
                    if (opened.st_dev, opened.st_ino) not in publication_links:
                        raise StorageError("staged PDF has an unknown hard link")
                elif opened.st_nlink != 1:
                    raise StorageError("staged PDF has an unknown hard link")
                with os.fdopen(os.dup(stage_fd), "rb") as source:
                    digest, size = _hash_reader(source)
                if (digest, size) != (staged.sha256, staged.size_bytes):
                    raise StorageError("staged PDF bytes changed")
            if not self._unlink_if_matching(self._staging_fd, path.name, expected):
                raise StorageError("staged PDF changed before discard")
            _fsync_directory_fd(self._staging_fd)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("staged PDF could not be discarded") from exc

    @_serialized
    def stage_pending(self, filename: str, operation_id: str) -> StagedPdf:
        self._validate_pending_ingress(filename)
        with self._opened_regular(
            self.pending_dir,
            filename,
            require_single_link=True,
        ) as (source_fd, _, _, _, opened):
            with os.fdopen(os.dup(source_fd), "rb") as source:
                staged = self._stage_stream(source, operation_id)
            final = os.fstat(source_fd)
            if final.st_nlink != 1 or not _same_inode(opened, final):
                self._unlink_if_matching(
                    self._staging_fd,
                    staged.path.name,
                    os.stat(staged.path.name, dir_fd=self._staging_fd, follow_symlinks=False),
                )
                raise StorageError("pending PDF changed while copying")
            return staged

    @_serialized
    def stage_revision(
        self,
        paper_id: str,
        revision: int,
        operation_id: str,
        *,
        sha256: str,
        size_bytes: int,
    ) -> StagedPdf:
        """Copy one immutable revision into a new durable staging entry.

        The source is re-opened descriptor-relatively while the storage lock is
        held, so lifecycle restoration never turns a returned filesystem path
        back into authority for a later name-based read.
        """
        if not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
            raise StorageError("invalid expected revision hash")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 1:
            raise StorageError("invalid expected revision size")
        path = self.open_revision(paper_id, revision)
        relative = f"{path.parent.name}/{path.name}"
        with self._opened_regular(
            self.papers_dir,
            relative,
            require_single_link=True,
            require_private=True,
        ) as (source_fd, _, _, _, source_stat):
            with os.fdopen(os.dup(source_fd), "rb") as source:
                staged = self._stage_stream(source, operation_id)
            current = os.fstat(source_fd)
            if (
                current.st_nlink != 1
                or not _same_inode(source_stat, current)
                or staged.source_sha256 != sha256
                or staged.size_bytes != size_bytes
            ):
                try:
                    expected = os.stat(
                        staged.path.name,
                        dir_fd=self._staging_fd,
                        follow_symlinks=False,
                    )
                    self._unlink_if_matching(self._staging_fd, staged.path.name, expected)
                    _fsync_directory_fd(self._staging_fd)
                except OSError:
                    pass
                raise StorageError("Paper revision does not match persistence")
            return staged

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
            with self._opened_regular(
                self.staging_dir,
                path.name,
                require_single_link=True,
                require_private=True,
            ) as (file_fd, _, _, resolved, _):
                with os.fdopen(os.dup(file_fd), "rb") as source:
                    current_hash, current_size = _hash_reader(source)
        except StorageError:
            raise
        if current_hash != staged.sha256 or current_size != staged.size_bytes:
            raise StorageError("staged PDF bytes changed")
        return resolved, current_hash, current_size

    def _create_staging_temporary(
        self,
        prefix: str,
        suffix: str,
    ) -> tuple[int, str, os.stat_result]:
        for _attempt in range(16):
            name = f"{prefix}{secrets.token_hex(16)}{suffix}"
            try:
                file_fd = os.open(name, _CREATE_RW_FLAGS, 0o600, dir_fd=self._staging_fd)
            except FileExistsError:
                continue
            try:
                opened = os.fstat(file_fd)
                if not stat.S_ISREG(opened.st_mode) or not _private_mode(opened, 0o600):
                    raise StorageError("temporary stage is not a private regular file")
                return file_fd, name, opened
            except Exception:
                os.close(file_fd)
                try:
                    os.unlink(name, dir_fd=self._staging_fd)
                except FileNotFoundError:
                    pass
                raise
        raise StorageError("could not allocate a unique staging temporary")

    @_serialized
    def apply_metadata(self, staged: StagedPdf, *, title: str, author: str) -> StagedPdf:
        source_path, _source_hash, _source_size = self._validated_stage(staged)
        temporary_name: str | None = None
        temporary_stat: os.stat_result | None = None
        temporary_fd: int | None = None
        backup_name: str | None = None
        backup_stat: os.stat_result | None = None
        backup_fd: int | None = None
        installed = False
        try:
            temporary_fd, temporary_name, temporary_stat = self._create_staging_temporary(
                f"{source_path.stem}.metadata-",
                ".tmp",
            )
            with self._opened_regular(
                self.staging_dir,
                source_path.name,
                require_single_link=True,
                require_private=True,
            ) as (source_fd, _, _, _, source_stat):
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
                        stored_hash, stored_size = _hash_reader(output)
                current_source = os.fstat(source_fd)
                if not _same_inode(current_source, source_stat):
                    raise StorageError("source stage changed during metadata rewrite")
                backup_name = (
                    f"{source_path.stem}.metadata-backup-{secrets.token_hex(16)}.tmp"
                )
                os.link(
                    source_path.name,
                    backup_name,
                    src_dir_fd=self._staging_fd,
                    dst_dir_fd=self._staging_fd,
                    follow_symlinks=False,
                )
                backup_stat = os.stat(
                    backup_name,
                    dir_fd=self._staging_fd,
                    follow_symlinks=False,
                )
                if not _same_inode(backup_stat, source_stat):
                    raise StorageError("metadata backup does not match source stage")
                backup_fd = os.open(backup_name, _READ_FLAGS, dir_fd=self._staging_fd)
                if not _same_inode(os.fstat(backup_fd), source_stat):
                    raise StorageError("metadata backup descriptor changed")

            with self._opened_regular(
                self.staging_dir,
                temporary_name,
                require_single_link=True,
                require_private=True,
            ) as (verified_fd, _, _, _, verified_stat):
                with os.fdopen(os.dup(verified_fd), "rb") as verified_file:
                    verified_hash, verified_size = _hash_reader(verified_file)
                if (
                    (verified_hash, verified_size) != (stored_hash, stored_size)
                    or not _same_inode(verified_stat, temporary_stat)
                ):
                    raise StorageError("metadata sibling changed before installation")

            os.replace(
                temporary_name,
                source_path.name,
                src_dir_fd=self._staging_fd,
                dst_dir_fd=self._staging_fd,
            )
            temporary_name = None
            temporary_stat = None
            installed = True
            _fsync_directory_fd(self._staging_fd)
            with self._opened_regular(
                self.staging_dir,
                source_path.name,
                require_single_link=True,
                require_private=True,
            ) as (prepared_fd, _, _, _, _):
                with os.fdopen(os.dup(prepared_fd), "rb") as prepared:
                    _strict_pdf(prepared)
                    final_hash, final_size = _hash_reader(prepared)
                    prepared.seek(0)
                    final_reader = PdfReader(prepared, strict=True)
                    if len(final_reader.pages) != source_page_count:
                        raise StorageError("installed metadata changed page count")
                    final_metadata = final_reader.metadata
                    if (
                        final_metadata.title != str(title)
                        or final_metadata.author != str(author)
                        or (final_hash, final_size) != (stored_hash, stored_size)
                    ):
                        raise StorageError("installed metadata verification failed")
            if backup_name is None or backup_stat is None:
                raise StorageError("metadata backup was not retained")
            if not self._unlink_if_matching(self._staging_fd, backup_name, backup_stat):
                raise StorageError("metadata backup changed before removal")
            backup_name = None
            backup_stat = None
            try:
                _fsync_directory_fd(self._staging_fd)
            except OSError:
                # The verified metadata rewrite is already committed and the
                # backup name is gone.  Do not report a retryable rewrite
                # failure for cleanup durability that reconciliation can audit.
                pass
            return StagedPdf(
                source_path,
                staged.source_sha256,
                stored_hash,
                stored_size,
            )
        except Exception as exc:
            if installed and backup_name is not None:
                try:
                    current_backup = os.stat(
                        backup_name,
                        dir_fd=self._staging_fd,
                        follow_symlinks=False,
                    )
                    if backup_fd is None or not _same_inode(
                        current_backup,
                        os.fstat(backup_fd),
                    ):
                        raise StorageError("metadata backup changed before rollback")
                    os.replace(
                        backup_name,
                        source_path.name,
                        src_dir_fd=self._staging_fd,
                        dst_dir_fd=self._staging_fd,
                    )
                    backup_name = None
                    backup_stat = None
                    _fsync_directory_fd(self._staging_fd)
                except Exception as rollback_exc:
                    raise StorageError("metadata rollback failed") from rollback_exc
            if isinstance(exc, StorageError):
                raise
            raise StorageError("PDF metadata could not be written") from exc
        finally:
            if backup_fd is not None:
                os.close(backup_fd)
            if temporary_fd is not None:
                os.close(temporary_fd)
            if temporary_name is not None and temporary_stat is not None:
                self._unlink_if_matching(
                    self._staging_fd,
                    temporary_name,
                    temporary_stat,
                )
            if backup_name is not None and backup_stat is not None:
                self._unlink_if_matching(
                    self._staging_fd,
                    backup_name,
                    backup_stat,
                )

    def revision_path(self, paper_id: str, revision: int) -> Path:
        canonical = _canonical_paper_id(paper_id)
        number = _valid_revision(revision)
        return self.papers_dir / canonical / f"{number}.pdf"

    def _open_paper_directory(
        self,
        paper_id: str,
        *,
        create: bool,
    ) -> tuple[int, int, str]:
        canonical = _canonical_paper_id(paper_id)
        root_fd: int | None = os.dup(self._papers_fd)
        paper_fd: int | None = None
        try:
            if create:
                try:
                    os.mkdir(canonical, 0o700, dir_fd=root_fd)
                    _fsync_directory_fd(root_fd)
                except FileExistsError:
                    pass
            before = os.stat(canonical, dir_fd=root_fd, follow_symlinks=False)
            if (
                not stat.S_ISDIR(before.st_mode)
                or not _private_mode(before, 0o700)
                or before.st_dev != self._papers_stat.st_dev
            ):
                raise StorageError("Paper revision directory is unsafe")
            paper_fd = os.open(canonical, _DIRECTORY_FLAGS, dir_fd=root_fd)
            opened = os.fstat(paper_fd)
            if not _same_inode(before, opened):
                raise StorageError("Paper revision directory changed while opening")
            return root_fd, paper_fd, canonical
        except StorageError:
            if paper_fd is not None:
                os.close(paper_fd)
            if root_fd is not None:
                os.close(root_fd)
            raise
        except (OSError, RuntimeError, ValueError) as exc:
            if paper_fd is not None:
                os.close(paper_fd)
            if root_fd is not None:
                os.close(root_fd)
            raise StorageError("Paper revision directory is unsafe") from exc

    @_serialized
    def promote(self, staged: StagedPdf, paper_id: str, revision: int) -> StoredPdf:
        source_path, source_hash, source_size = self._validated_stage(staged)
        number = _valid_revision(revision)
        root_fd, paper_fd, canonical = self._open_paper_directory(
            paper_id,
            create=True,
        )
        destination_name = f"{number}.pdf"
        destination = self.papers_dir / canonical / destination_name
        try:
            with self._opened_regular(
                self.staging_dir,
                source_path.name,
                require_single_link=True,
                require_private=True,
            ) as (source_fd, _, _, _, source_stat):
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    verified_hash, verified_size = _hash_reader(source)
                if (verified_hash, verified_size) != (source_hash, source_size):
                    raise StorageError("staged PDF changed before promotion")
                published = False
                try:
                    _publish_verified_fd_no_replace(
                        source_fd,
                        paper_fd,
                        destination_name,
                    )
                except FileExistsError:
                    pass
                else:
                    published = True

                try:
                    final_fd = os.open(destination_name, _READ_FLAGS, dir_fd=paper_fd)
                except OSError as exc:
                    raise StorageError("published revision could not be reopened") from exc
                try:
                    final_stat = os.fstat(final_fd)
                    if (
                        not stat.S_ISREG(final_stat.st_mode)
                        or not _private_mode(final_stat, 0o600)
                        or (
                            final_stat.st_nlink != 1
                            and not (
                                published
                                and final_stat.st_nlink == 2
                                and _same_inode(final_stat, source_stat)
                            )
                        )
                    ):
                        raise StorageError("published revision is not a private file")
                    with os.fdopen(os.dup(final_fd), "rb") as final_file:
                        final_hash, final_size = _hash_reader(final_file)
                    if (final_hash, final_size) != (source_hash, source_size):
                        raise StorageError("published revision bytes do not match the stage")
                    os.fsync(final_fd)
                finally:
                    os.close(final_fd)
                _fsync_directory_fd(paper_fd)
                try:
                    if self._unlink_if_matching(
                        self._staging_fd,
                        source_path.name,
                        source_stat,
                    ):
                        _fsync_directory_fd(self._staging_fd)
                except (OSError, StorageError):
                    # The final file and its parent directory are durable.
                    # Stage cleanup is reconcilable and cannot invalidate the
                    # already committed immutable revision.
                    pass
            return StoredPdf(destination, source_hash, source_size)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("PDF revision could not be promoted") from exc
        finally:
            os.close(paper_fd)
            os.close(root_fd)

    @_serialized
    def open_revision(self, paper_id: str, revision: int) -> Path:
        path = self.revision_path(paper_id, revision)
        relative = f"{path.parent.name}/{path.name}"
        try:
            with self._opened_regular(
                self.papers_dir,
                relative,
                require_single_link=True,
                require_private=True,
            ):
                return path
        except StorageError as exc:
            raise StorageError("Paper revision does not exist") from exc

    @_serialized
    def verify_revision(
        self,
        paper_id: str,
        revision: int,
        *,
        sha256: str,
        size_bytes: int,
    ) -> StoredPdf:
        """Reopen and hash the exact immutable revision under the storage lock."""
        path = self.revision_path(paper_id, revision)
        if not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
            raise StorageError("invalid expected revision hash")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 1:
            raise StorageError("invalid expected revision size")
        relative = f"{path.parent.name}/{path.name}"
        try:
            with self._opened_regular(
                self.papers_dir,
                relative,
                require_single_link=True,
                require_private=True,
            ) as (revision_fd, _, _, resolved, _):
                with os.fdopen(os.dup(revision_fd), "rb") as source:
                    digest, size = _hash_reader(source)
                if (digest, size) != (sha256, size_bytes):
                    raise StorageError("Paper revision bytes do not match persistence")
                return StoredPdf(resolved, digest, size)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("Paper revision could not be verified") from exc

    @_serialized
    def discard_unreferenced_revision(
        self,
        paper_id: str,
        revision: int,
        *,
        sha256: str,
        size_bytes: int,
    ) -> None:
        """Remove only an exact new immutable file after SQL visibility failed.

        The caller must first prove under its Paper row lock that no revision
        record owns this `(paper_id, revision)`.  Hash-and-size matching keeps
        this recovery primitive incapable of deleting a competing revision.
        """
        canonical = _canonical_paper_id(paper_id)
        number = _valid_revision(revision)
        if not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
            raise StorageError("invalid expected revision hash")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 1:
            raise StorageError("invalid expected revision size")
        root_fd: int | None = None
        paper_fd: int | None = None
        try:
            try:
                root_fd, paper_fd, _ = self._open_paper_directory(canonical, create=False)
            except StorageError as exc:
                # A missing Paper directory means no residual file remains.
                try:
                    os.stat(canonical, dir_fd=self._papers_fd, follow_symlinks=False)
                except FileNotFoundError:
                    return
                raise exc
            name = f"{number}.pdf"
            try:
                expected = os.stat(name, dir_fd=paper_fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if (
                not stat.S_ISREG(expected.st_mode)
                or not _private_mode(expected, 0o600)
                or expected.st_nlink != 1
            ):
                raise StorageError("unreferenced revision is unsafe")
            with self._opened_regular(
                self.papers_dir,
                f"{canonical}/{name}",
                require_single_link=True,
                require_private=True,
            ) as (file_fd, _, _, _, opened):
                if not _same_inode(expected, opened):
                    raise StorageError("unreferenced revision changed before discard")
                with os.fdopen(os.dup(file_fd), "rb") as source:
                    actual_hash, actual_size = _hash_reader(source)
            if (actual_hash, actual_size) != (sha256, size_bytes):
                raise StorageError("unreferenced revision bytes changed")
            if not self._unlink_if_matching(paper_fd, name, expected):
                raise StorageError("unreferenced revision changed during discard")
            _fsync_directory_fd(paper_fd)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("unreferenced revision could not be discarded") from exc
        finally:
            if paper_fd is not None:
                os.close(paper_fd)
            if root_fd is not None:
                os.close(root_fd)

    def _mid_promotion_stage_twin(
        self,
        final: os.stat_result,
    ) -> tuple[str, os.stat_result]:
        """Find the sole canonical staging name for a two-link final inode."""
        staging_directory = os.fstat(self._staging_fd)
        if (
            not stat.S_ISDIR(staging_directory.st_mode)
            or not _private_mode(staging_directory, 0o700)
            or not _same_inode(staging_directory, self._staging_stat)
        ):
            raise StorageError("staging namespace is unsafe")

        twins: list[tuple[str, os.stat_result]] = []
        for entry_name in os.listdir(self._staging_fd):
            entry = os.stat(
                entry_name,
                dir_fd=self._staging_fd,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(entry.st_mode)
                or not _private_mode(entry, 0o600)
                or entry.st_dev != staging_directory.st_dev
            ):
                raise StorageError("staging namespace contains an unsafe entry")
            if _same_inode(entry, final):
                twins.append((entry_name, entry))

        if len(twins) != 1:
            raise StorageError("unowned revision has no exact staging twin")
        twin_name, twin = twins[0]
        if (
            not twin_name.endswith(".pdf")
            or _OPERATION_ID.fullmatch(twin_name[:-4]) is None
            or final.st_nlink != 2
            or twin.st_nlink != 2
        ):
            raise StorageError("unowned revision staging twin is unsafe")
        return twin_name, twin

    @_serialized
    def discard_unowned_revision(
        self,
        paper_id: str,
        revision: int,
    ) -> StoredPdf | None:
        """Discard one audited revision after the lifecycle proves no SQL owner.

        Unlike ``discard_unreferenced_revision``, this crash-recovery primitive
        intentionally accepts unknown bytes.  Its caller must hold the Paper
        row lock and prove the exact next revision has no ``paper_revisions``
        row.  Descriptor-relative inode checks prevent a path swap during the
        audit/unlink operation.
        """
        canonical = _canonical_paper_id(paper_id)
        number = _valid_revision(revision)
        root_fd: int | None = None
        paper_fd: int | None = None
        try:
            try:
                root_fd, paper_fd, _ = self._open_paper_directory(
                    canonical,
                    create=False,
                )
            except StorageError as exc:
                try:
                    os.stat(canonical, dir_fd=self._papers_fd, follow_symlinks=False)
                except FileNotFoundError:
                    return None
                raise exc
            name = f"{number}.pdf"
            try:
                expected = os.stat(name, dir_fd=paper_fd, follow_symlinks=False)
            except FileNotFoundError:
                return None
            if (
                not stat.S_ISREG(expected.st_mode)
                or not _private_mode(expected, 0o600)
                or expected.st_nlink not in (1, 2)
            ):
                raise StorageError("unowned revision is unsafe")
            with self._opened_regular(
                self.papers_dir,
                f"{canonical}/{name}",
                require_private=True,
            ) as (file_fd, _, _, resolved, opened):
                if (
                    not _same_inode(expected, opened)
                    or opened.st_nlink != expected.st_nlink
                ):
                    raise StorageError("unowned revision changed before discard")
                with os.fdopen(os.dup(file_fd), "rb") as source:
                    actual_hash, actual_size = _hash_reader(source)

                if opened.st_nlink == 2:
                    twin_name, twin = self._mid_promotion_stage_twin(opened)
                    with self._opened_regular(
                        self.staging_dir,
                        twin_name,
                        require_private=True,
                    ) as (twin_fd, _, _, _, twin_opened):
                        final_before = os.stat(
                            name,
                            dir_fd=paper_fd,
                            follow_symlinks=False,
                        )
                        twin_before = os.stat(
                            twin_name,
                            dir_fd=self._staging_fd,
                            follow_symlinks=False,
                        )
                        final_descriptor = os.fstat(file_fd)
                        twin_descriptor = os.fstat(twin_fd)
                        if (
                            not stat.S_ISREG(final_before.st_mode)
                            or not stat.S_ISREG(twin_before.st_mode)
                            or not _private_mode(final_before, 0o600)
                            or not _private_mode(twin_before, 0o600)
                            or final_before.st_nlink != 2
                            or twin_before.st_nlink != 2
                            or final_descriptor.st_nlink != 2
                            or twin_descriptor.st_nlink != 2
                            or not _same_inode(opened, final_before)
                            or not _same_inode(twin, twin_before)
                            or not _same_inode(final_before, twin_before)
                            or not _same_inode(final_descriptor, twin_descriptor)
                        ):
                            raise StorageError(
                                "unowned revision promotion pair changed before discard"
                            )
                        if not self._unlink_if_matching(
                            self._staging_fd,
                            twin_name,
                            twin_before,
                        ):
                            raise StorageError(
                                "unowned revision staging twin changed during discard"
                            )
                        _fsync_directory_fd(self._staging_fd)

                        final_after = os.stat(
                            name,
                            dir_fd=paper_fd,
                            follow_symlinks=False,
                        )
                        final_descriptor = os.fstat(file_fd)
                        twin_descriptor = os.fstat(twin_fd)
                        if (
                            not stat.S_ISREG(final_after.st_mode)
                            or not _private_mode(final_after, 0o600)
                            or final_after.st_nlink != 1
                            or final_descriptor.st_nlink != 1
                            or twin_descriptor.st_nlink != 1
                            or not _same_inode(opened, final_after)
                            or not _same_inode(final_after, final_descriptor)
                            or not _same_inode(final_descriptor, twin_descriptor)
                        ):
                            raise StorageError(
                                "unowned revision promotion pair changed during discard"
                            )
                        expected = final_after

                if not self._unlink_if_matching(paper_fd, name, expected):
                    raise StorageError("unowned revision changed during discard")
                _fsync_directory_fd(paper_fd)
            return StoredPdf(resolved, actual_hash, actual_size)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("unowned revision could not be discarded") from exc
        finally:
            if paper_fd is not None:
                os.close(paper_fd)
            if root_fd is not None:
                os.close(root_fd)

    @_serialized
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
        with self._opened_regular(
            self.papers_dir,
            relative,
            require_single_link=True,
            require_private=True,
        ) as (source_fd, _, _, _, _):
            with os.fdopen(os.dup(source_fd), "rb") as stream:
                staged = self._stage_stream(stream, operation_id)
        prepared = self.apply_metadata(staged, title=title, author=author)
        return self.promote(prepared, paper_id, target_revision)

    def _legacy_entries(
        self,
        filenames: Iterable[str],
    ) -> dict[str, os.stat_result | None]:
        validated: dict[str, os.stat_result | None] = {}
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
                info = os.stat(
                    filename,
                    dir_fd=self._papers_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                info = None
            if info is not None and not stat.S_ISREG(info.st_mode):
                raise StorageError("legacy PDF is not a regular file")
            validated[filename] = info
        return validated

    @_serialized
    def delete_paper(self, paper_id: str, retained_legacy_filenames: Iterable[str]) -> None:
        canonical = _canonical_paper_id(paper_id)
        legacy_entries = self._legacy_entries(retained_legacy_filenames)
        root_fd = os.dup(self._papers_fd)
        paper_fd: int | None = None
        paper_stat: os.stat_result | None = None
        revision_entries: dict[str, os.stat_result] = {}
        try:
            try:
                paper_stat = os.stat(canonical, dir_fd=root_fd, follow_symlinks=False)
                paper_fd = os.open(canonical, _DIRECTORY_FLAGS, dir_fd=root_fd)
            except FileNotFoundError:
                paper_fd = None
            except OSError as exc:
                raise StorageError("Paper revision directory is unsafe") from exc
            if paper_fd is not None:
                opened = os.fstat(paper_fd)
                if (
                    paper_stat is None
                    or not _same_inode(paper_stat, opened)
                    or not _private_mode(opened, 0o700)
                    or opened.st_dev != self._papers_stat.st_dev
                ):
                    raise StorageError("Paper revision directory changed")
                for name in os.listdir(paper_fd):
                    entry = os.stat(name, dir_fd=paper_fd, follow_symlinks=False)
                    if (
                        _REVISION_FILENAME.fullmatch(name) is None
                        or not stat.S_ISREG(entry.st_mode)
                        or not _private_mode(entry, 0o600)
                        or entry.st_dev != opened.st_dev
                        or entry.st_nlink != 1
                    ):
                        raise StorageError("Paper revision layout is not flat and canonical")
                    revision_entries[name] = entry

            for filename, expected in legacy_entries.items():
                try:
                    current = os.stat(filename, dir_fd=root_fd, follow_symlinks=False)
                except FileNotFoundError:
                    current = None
                if (expected is None) != (current is None) or (
                    expected is not None
                    and current is not None
                    and not _same_inode(expected, current)
                ):
                    raise StorageError("legacy PDF changed before deletion")

            if paper_fd is not None:
                for name, expected in revision_entries.items():
                    current = os.stat(name, dir_fd=paper_fd, follow_symlinks=False)
                    if (
                        not _same_inode(expected, current)
                        or current.st_dev != expected.st_dev
                        or current.st_nlink != 1
                    ):
                        raise StorageError("Paper revision changed before deletion")
                for name, expected in revision_entries.items():
                    current = os.stat(name, dir_fd=paper_fd, follow_symlinks=False)
                    if (
                        not _same_inode(expected, current)
                        or current.st_dev != expected.st_dev
                        or current.st_nlink != 1
                    ):
                        raise StorageError("Paper revision changed before unlink")
                    if not self._unlink_if_matching(paper_fd, name, expected):
                        raise StorageError("Paper revision changed during deletion")
                _fsync_directory_fd(paper_fd)
                current_directory = os.stat(
                    canonical,
                    dir_fd=root_fd,
                    follow_symlinks=False,
                )
                if paper_stat is None or not _same_inode(paper_stat, current_directory):
                    raise StorageError("Paper directory changed before removal")
                os.rmdir(canonical, dir_fd=root_fd)
                _fsync_directory_fd(root_fd)
            for filename, expected in legacy_entries.items():
                try:
                    current = os.stat(
                        filename,
                        dir_fd=root_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    current = None
                if expected is None:
                    if current is not None:
                        raise StorageError("legacy PDF appeared during deletion")
                    continue
                if current is None or not _same_inode(expected, current):
                    raise StorageError("legacy PDF changed during deletion")
                if not self._unlink_if_matching(root_fd, filename, expected):
                    raise StorageError("legacy PDF changed during deletion")
            _fsync_directory_fd(root_fd)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("Paper files could not be deleted") from exc
        finally:
            if paper_fd is not None:
                os.close(paper_fd)
            os.close(root_fd)

    def _link_then_unlink(
        self,
        source_parent_fd: int,
        source_name: str,
        source_stat: os.stat_result,
        destination_parent_fd: int,
        destination_name: str,
    ) -> os.stat_result:
        linked = False
        committed = False
        try:
            current = os.stat(source_name, dir_fd=source_parent_fd, follow_symlinks=False)
            if (
                not stat.S_ISREG(current.st_mode)
                or not _same_inode(current, source_stat)
                or current.st_nlink != 1
            ):
                raise StorageError("pending PDF changed before move")
            os.link(
                source_name,
                destination_name,
                src_dir_fd=source_parent_fd,
                dst_dir_fd=destination_parent_fd,
                follow_symlinks=False,
            )
            linked = True
            destination = os.stat(
                destination_name,
                dir_fd=destination_parent_fd,
                follow_symlinks=False,
            )
            source_now = os.stat(
                source_name,
                dir_fd=source_parent_fd,
                follow_symlinks=False,
            )
            if (
                not _same_inode(destination, source_stat)
                or not _same_inode(source_now, source_stat)
            ):
                raise StorageError("pending PDF changed during no-clobber move")
            _fsync_directory_fd(destination_parent_fd)
            if not self._unlink_if_matching(source_parent_fd, source_name, source_stat):
                raise StorageError("pending PDF changed before source removal")
            committed = True
            try:
                _fsync_directory_fd(source_parent_fd)
                if destination_parent_fd != source_parent_fd:
                    _fsync_directory_fd(destination_parent_fd)
                final = os.stat(
                    destination_name,
                    dir_fd=destination_parent_fd,
                    follow_symlinks=False,
                )
            except OSError:
                # The exact source name is already gone.  Returning authority
                # for the committed destination is safer than reporting a
                # retryable failure that could orphan or reverse the move.
                return source_stat
            if not _same_inode(final, source_stat) or final.st_nlink != 1:
                raise StorageError("pending PDF move did not produce one private name")
            return final
        except FileExistsError as exc:
            raise StorageError("pending destination is occupied") from exc
        except (OSError, StorageError) as exc:
            if committed:
                if isinstance(exc, StorageError):
                    raise
                return source_stat
            if linked:
                try:
                    removed = self._unlink_if_matching(
                        destination_parent_fd,
                        destination_name,
                        source_stat,
                    )
                    if not removed:
                        raise StorageError(
                            "pending destination changed before move rollback"
                        )
                    _fsync_directory_fd(destination_parent_fd)
                except (OSError, StorageError) as rollback_exc:
                    raise StorageError("pending move rollback failed") from rollback_exc
            if isinstance(exc, StorageError):
                raise
            raise StorageError("pending PDF move failed before commit") from exc

    @staticmethod
    def _submission_trash_entry_name(submission_id: str) -> str:
        if (
            not isinstance(submission_id, str)
            or not submission_id
            or len(submission_id) > 255
            or "\x00" in submission_id
        ):
            raise StorageError("invalid Submission trash owner")
        return hashlib.sha256(submission_id.encode("utf-8")).hexdigest()

    @staticmethod
    def _submission_owner_bytes(submission_id: str, original_name: str) -> bytes:
        return (
            json.dumps(
                {
                    "original_name": original_name,
                    "submission_id": submission_id,
                    "version": 2,
                },
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")

    def _open_submission_trash_entry(
        self,
        entry_name: str,
    ) -> tuple[int, os.stat_result]:
        if _SUBMISSION_TRASH_ENTRY.fullmatch(entry_name) is None:
            raise StorageError("invalid Submission trash entry")
        try:
            before = os.stat(
                entry_name,
                dir_fd=self._submission_trash_fd,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISDIR(before.st_mode)
                or not _private_mode(before, 0o700)
                or before.st_dev != self._submission_trash_stat.st_dev
            ):
                raise StorageError("Submission trash entry is unsafe")
            entry_fd = os.open(
                entry_name,
                _DIRECTORY_FLAGS,
                dir_fd=self._submission_trash_fd,
            )
            opened = os.fstat(entry_fd)
            if not _same_inode(before, opened):
                raise StorageError("Submission trash entry changed while opening")
            return entry_fd, opened
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("Submission trash entry could not be opened") from exc

    def _submission_trash_record_unlocked(
        self,
        submission_id: str,
        *,
        missing_ok: bool = True,
    ) -> SubmissionTrashRecord | None:
        entry_name = self._submission_trash_entry_name(submission_id)
        try:
            try:
                entry_fd, entry_stat = self._open_submission_trash_entry(entry_name)
            except StorageError as exc:
                try:
                    os.stat(
                        entry_name,
                        dir_fd=self._submission_trash_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    if missing_ok:
                        return None
                raise exc
            try:
                names = set(os.listdir(entry_fd))
                if not names.issubset({_SUBMISSION_TRASH_OWNER, _SUBMISSION_TRASH_PAYLOAD}):
                    raise StorageError("Submission trash entry contains unknown data")
                if not names:
                    os.close(entry_fd)
                    entry_fd = -1
                    self._remove_payload_free_submission_trash_residue(
                        entry_name,
                        entry_stat,
                    )
                    return None
                if _SUBMISSION_TRASH_OWNER not in names:
                    raise StorageError("Submission trash payload has no owner provenance")
                owner_fd = os.open(_SUBMISSION_TRASH_OWNER, _READ_FLAGS, dir_fd=entry_fd)
                try:
                    owner_stat = os.fstat(owner_fd)
                    if (
                        not stat.S_ISREG(owner_stat.st_mode)
                        or not _private_mode(owner_stat, 0o600)
                        or owner_stat.st_nlink != 1
                        or owner_stat.st_dev != entry_stat.st_dev
                    ):
                        raise StorageError("Submission trash owner descriptor is unsafe")
                    with os.fdopen(os.dup(owner_fd), "rb") as owner_file:
                        owner_bytes = owner_file.read(4097)
                finally:
                    os.close(owner_fd)
                if len(owner_bytes) > 4096:
                    raise StorageError("Submission trash owner descriptor is too large")
                try:
                    owner = json.loads(owner_bytes.decode("utf-8"))
                    owner_valid = (
                        isinstance(owner, dict)
                        and set(owner) == {"version", "submission_id", "original_name"}
                        and owner.get("version") == 2
                        and owner.get("submission_id") == submission_id
                        and isinstance(owner.get("original_name"), str)
                        and owner_bytes
                        == self._submission_owner_bytes(
                            submission_id,
                            owner["original_name"],
                        )
                    )
                except (UnicodeDecodeError, json.JSONDecodeError):
                    owner = None
                    owner_valid = False
                if owner_valid:
                    try:
                        self._validate_pending_recovery_name(owner["original_name"])
                    except StorageError:
                        owner_valid = False
                if not owner_valid:
                    if _SUBMISSION_TRASH_PAYLOAD in names:
                        raise StorageError(
                            "Submission trash payload has invalid owner provenance"
                        )
                    os.close(entry_fd)
                    entry_fd = -1
                    self._remove_payload_free_submission_trash_residue(
                        entry_name,
                        entry_stat,
                    )
                    return None
                payload_stat = None
                if _SUBMISSION_TRASH_PAYLOAD in names:
                    payload_stat = os.stat(
                        _SUBMISSION_TRASH_PAYLOAD,
                        dir_fd=entry_fd,
                        follow_symlinks=False,
                    )
                    if (
                        not stat.S_ISREG(payload_stat.st_mode)
                        or not _private_mode(payload_stat, 0o600)
                        or payload_stat.st_nlink not in {1, 2}
                        or payload_stat.st_dev != entry_stat.st_dev
                    ):
                        raise StorageError("Submission trash payload is unsafe")
                modified_at = max(
                    entry_stat.st_mtime,
                    owner_stat.st_mtime,
                    payload_stat.st_mtime if payload_stat is not None else 0,
                )
                return SubmissionTrashRecord(
                    submission_id=submission_id,
                    original_name=owner["original_name"],
                    entry_name=entry_name,
                    has_payload=payload_stat is not None,
                    modified_at=modified_at,
                )
            finally:
                if entry_fd >= 0:
                    os.close(entry_fd)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("Submission trash provenance could not be audited") from exc

    def _remove_submission_trash_metadata(
        self,
        record: SubmissionTrashRecord,
    ) -> None:
        current = self._submission_trash_record_unlocked(
            record.submission_id,
            missing_ok=False,
        )
        if current != record or current.has_payload:
            raise StorageError("Submission trash provenance changed before cleanup")
        entry_fd, entry_stat = self._open_submission_trash_entry(record.entry_name)
        try:
            owner_stat = os.stat(
                _SUBMISSION_TRASH_OWNER,
                dir_fd=entry_fd,
                follow_symlinks=False,
            )
            if not self._unlink_if_matching(
                entry_fd,
                _SUBMISSION_TRASH_OWNER,
                owner_stat,
            ):
                raise StorageError("Submission trash owner changed before cleanup")
            _fsync_directory_fd(entry_fd)
        finally:
            os.close(entry_fd)
        current_dir = os.stat(
            record.entry_name,
            dir_fd=self._submission_trash_fd,
            follow_symlinks=False,
        )
        if not _same_inode(entry_stat, current_dir):
            raise StorageError("Submission trash entry changed before cleanup")
        os.rmdir(record.entry_name, dir_fd=self._submission_trash_fd)
        _fsync_directory_fd(self._submission_trash_fd)

    def _remove_payload_free_submission_trash_residue(
        self,
        entry_name: str,
        expected: os.stat_result,
    ) -> None:
        """Remove an exact construction/cleanup residue that has no payload."""
        entry_fd, opened = self._open_submission_trash_entry(entry_name)
        try:
            names = set(os.listdir(entry_fd))
            if (
                not _same_inode(expected, opened)
                or not names.issubset({_SUBMISSION_TRASH_OWNER})
            ):
                raise StorageError("payload-free Submission trash residue changed")
            if _SUBMISSION_TRASH_OWNER in names:
                owner = os.stat(
                    _SUBMISSION_TRASH_OWNER,
                    dir_fd=entry_fd,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(owner.st_mode)
                    or not _private_mode(owner, 0o600)
                    or owner.st_nlink != 1
                    or owner.st_dev != opened.st_dev
                ):
                    raise StorageError(
                        "payload-free Submission trash owner is unsafe"
                    )
                if not self._unlink_if_matching(
                    entry_fd,
                    _SUBMISSION_TRASH_OWNER,
                    owner,
                ):
                    raise StorageError(
                        "payload-free Submission trash owner changed"
                    )
                _fsync_directory_fd(entry_fd)
        finally:
            os.close(entry_fd)
        current = os.stat(
            entry_name,
            dir_fd=self._submission_trash_fd,
            follow_symlinks=False,
        )
        if not _same_inode(expected, current):
            raise StorageError(
                "payload-free Submission trash residue changed before removal"
            )
        os.rmdir(entry_name, dir_fd=self._submission_trash_fd)
        _fsync_directory_fd(self._submission_trash_fd)

    @_serialized
    def submission_trash_record(
        self,
        submission_id: str,
    ) -> SubmissionTrashRecord | None:
        return self._submission_trash_record_unlocked(submission_id)

    @_serialized
    def trash_submission_pending(
        self,
        filename: str,
        submission_id: str,
    ) -> PendingTrash:
        """Move pending bytes into the descriptor-owned Submission V2 namespace."""
        entry_name = self._submission_trash_entry_name(submission_id)
        self._validate_pending_ingress(filename)
        created_entry = False
        entry_fd: int | None = None
        try:
            os.mkdir(entry_name, mode=0o700, dir_fd=self._submission_trash_fd)
            created_entry = True
            _fsync_directory_fd(self._submission_trash_fd)
            entry_fd, _entry_stat = self._open_submission_trash_entry(entry_name)
            owner_bytes = self._submission_owner_bytes(submission_id, filename)
            owner_fd = os.open(
                _SUBMISSION_TRASH_OWNER,
                _CREATE_FLAGS,
                0o600,
                dir_fd=entry_fd,
            )
            try:
                with os.fdopen(owner_fd, "wb") as owner_file:
                    owner_fd = -1
                    owner_file.write(owner_bytes)
                    owner_file.flush()
                    os.fsync(owner_file.fileno())
            finally:
                if owner_fd >= 0:
                    os.close(owner_fd)
            _fsync_directory_fd(entry_fd)
            with self._opened_regular(
                self.pending_dir,
                filename,
                require_single_link=True,
            ) as (
                source_fd,
                source_parent_fd,
                source_name,
                _original,
                source_stat,
            ):
                os.fchmod(source_fd, 0o600)
                source_stat = os.fstat(source_fd)
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    source_hash, source_size = _hash_reader(source)
                final = self._link_then_unlink(
                    source_parent_fd,
                    source_name,
                    source_stat,
                    entry_fd,
                    _SUBMISSION_TRASH_PAYLOAD,
                )
            return self._issue_pending_trash_token(
                original_name=filename,
                operation_id=submission_id,
                source_sha256=source_hash,
                size_bytes=source_size,
                device=final.st_dev,
                inode=final.st_ino,
                namespace="submission-v2",
                entry_name=entry_name,
            )
        except FileExistsError as exc:
            raise StorageError("Submission trash destination is occupied") from exc
        except StorageError:
            if created_entry:
                try:
                    record = self._submission_trash_record_unlocked(submission_id)
                    if record is not None and not record.has_payload:
                        self._remove_submission_trash_metadata(record)
                except StorageError:
                    pass
            raise
        except OSError as exc:
            raise StorageError("pending Submission PDF could not be trashed") from exc
        finally:
            if entry_fd is not None:
                os.close(entry_fd)

    @_serialized
    def rehydrate_submission_trash(
        self,
        submission_id: str,
        expected_original_name: str | None = None,
    ) -> PendingTrash | None:
        """Re-audit exact V2 owner provenance and issue fresh local authority."""
        record = self._submission_trash_record_unlocked(submission_id)
        if record is None:
            return None
        if (
            expected_original_name is not None
            and record.original_name != expected_original_name
        ):
            raise StorageError("Submission trash owner conflicts with SQL provenance")
        if not record.has_payload:
            return None
        try:
            with self._opened_regular(
                self.submission_trash_dir,
                f"{record.entry_name}/{_SUBMISSION_TRASH_PAYLOAD}",
                require_private=True,
            ) as (source_fd, _, _, _, source_stat):
                if source_stat.st_nlink == 2:
                    with self._opened_regular(
                        self.pending_dir,
                        record.original_name,
                        require_private=True,
                    ) as (_, _, _, _, original_stat):
                        if (
                            original_stat.st_nlink != 2
                            or not _same_inode(source_stat, original_stat)
                        ):
                            raise StorageError(
                                "Submission trash is not its exact interrupted move pair"
                            )
                elif source_stat.st_nlink == 1:
                    if self.pending_exists(record.original_name):
                        raise StorageError(
                            "Submission trash conflicts with an occupied original name"
                        )
                else:
                    raise StorageError("Submission trash has an unsafe link count")
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    source_hash, source_size = _hash_reader(source)
                return self._issue_pending_trash_token(
                    original_name=record.original_name,
                    operation_id=submission_id,
                    source_sha256=source_hash,
                    size_bytes=source_size,
                    device=source_stat.st_dev,
                    inode=source_stat.st_ino,
                    namespace="submission-v2",
                    entry_name=record.entry_name,
                )
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("Submission trash could not be rehydrated") from exc

    def _stale_submission_trash_candidate(
        self,
        entry_name: str,
        cutoff_timestamp: float,
    ) -> SubmissionTrashRecord | None:
        if _SUBMISSION_TRASH_ENTRY.fullmatch(entry_name) is None:
            raise StorageError("Submission trash contains an unknown entry")
        entry_fd, entry_stat = self._open_submission_trash_entry(entry_name)
        try:
            names = set(os.listdir(entry_fd))
            if not names.issubset(
                {_SUBMISSION_TRASH_OWNER, _SUBMISSION_TRASH_PAYLOAD}
            ):
                raise StorageError("Submission trash entry contains unknown data")
            if not names:
                if entry_stat.st_mtime < cutoff_timestamp:
                    os.close(entry_fd)
                    entry_fd = -1
                    self._remove_payload_free_submission_trash_residue(
                        entry_name,
                        entry_stat,
                    )
                return None
            if _SUBMISSION_TRASH_OWNER not in names:
                raise StorageError("Submission trash payload has no owner provenance")
            owner_fd = os.open(
                _SUBMISSION_TRASH_OWNER,
                _READ_FLAGS,
                dir_fd=entry_fd,
            )
            try:
                owner_bytes = os.read(owner_fd, 4097)
            finally:
                os.close(owner_fd)
            try:
                owner = json.loads(owner_bytes.decode("utf-8"))
                submission_id = owner["submission_id"]
            except (
                KeyError,
                TypeError,
                UnicodeDecodeError,
                json.JSONDecodeError,
            ) as exc:
                if (
                    _SUBMISSION_TRASH_PAYLOAD not in names
                    and entry_stat.st_mtime < cutoff_timestamp
                ):
                    os.close(entry_fd)
                    entry_fd = -1
                    self._remove_payload_free_submission_trash_residue(
                        entry_name,
                        entry_stat,
                    )
                    return None
                if _SUBMISSION_TRASH_PAYLOAD not in names:
                    return None
                raise StorageError(
                    "Submission trash owner descriptor is invalid"
                ) from exc
        finally:
            if entry_fd >= 0:
                os.close(entry_fd)

        try:
            owner_entry_name = self._submission_trash_entry_name(submission_id)
        except StorageError:
            owner_entry_name = None
        if owner_entry_name != entry_name:
            if (
                _SUBMISSION_TRASH_PAYLOAD not in names
                and entry_stat.st_mtime < cutoff_timestamp
            ):
                self._remove_payload_free_submission_trash_residue(
                    entry_name,
                    entry_stat,
                )
                return None
            if _SUBMISSION_TRASH_PAYLOAD not in names:
                return None
            raise StorageError("Submission trash owner hashes to another entry")
        record = self._submission_trash_record_unlocked(
            submission_id,
            missing_ok=False,
        )
        if record is not None and record.modified_at < cutoff_timestamp:
            return record
        return None

    @_serialized
    def stale_submission_trash(
        self,
        cutoff: datetime | float | int,
        *,
        on_error: Callable[[str, BaseException], None] | None = None,
    ) -> tuple[SubmissionTrashRecord, ...]:
        cutoff_timestamp = self._cutoff_timestamp(cutoff)
        try:
            entry_names = tuple(sorted(os.listdir(self._submission_trash_fd)))
        except OSError as exc:
            raise StorageError("Submission trash could not be audited") from exc

        records: list[SubmissionTrashRecord] = []
        for entry_name in entry_names:
            candidate = f"submission-trash:{entry_name}"
            try:
                record = self._stale_submission_trash_candidate(
                    entry_name,
                    cutoff_timestamp,
                )
            except StorageError as exc:
                if on_error is None:
                    raise
                on_error(candidate, exc)
                continue
            except OSError as exc:
                error = StorageError("Submission trash could not be audited")
                if on_error is None:
                    raise error from exc
                on_error(candidate, error)
                continue
            if record is not None:
                records.append(record)
        return tuple(sorted(records, key=lambda record: record.submission_id))

    @_serialized
    def discard_empty_submission_trash(
        self,
        record: SubmissionTrashRecord,
    ) -> None:
        """Remove an exact audited provenance-only crash residue."""
        if not isinstance(record, SubmissionTrashRecord) or record.has_payload:
            raise StorageError("empty Submission trash cleanup requires provenance")
        self._remove_submission_trash_metadata(record)

    @_serialized
    def trash_pending(self, filename: str, operation_id: str) -> PendingTrash:
        trash_name = f"{_valid_operation_id(operation_id)}.pdf"
        self._validate_pending_ingress(filename)
        try:
            with self._opened_regular(
                self.pending_dir,
                filename,
                require_single_link=True,
            ) as (
                source_fd,
                source_parent_fd,
                source_name,
                _original,
                source_stat,
            ):
                os.fchmod(source_fd, 0o600)
                source_stat = os.fstat(source_fd)
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    source_hash, source_size = _hash_reader(source)
                final = self._link_then_unlink(
                    source_parent_fd,
                    source_name,
                    source_stat,
                    self._trash_fd,
                    trash_name,
                )
            return self._issue_pending_trash_token(
                original_name=filename,
                operation_id=operation_id,
                source_sha256=source_hash,
                size_bytes=source_size,
                device=final.st_dev,
                inode=final.st_ino,
            )
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending PDF could not be trashed") from exc

    def _issue_pending_trash_token(
        self,
        *,
        original_name: str,
        operation_id: str,
        source_sha256: str,
        size_bytes: int,
        device: int,
        inode: int,
        namespace: str = "legacy",
        entry_name: str | None = None,
    ) -> PendingTrash:
        capability = secrets.token_urlsafe(32)
        token = PendingTrash(
            original_name=original_name,
            operation_id=operation_id,
            source_sha256=source_sha256,
            size_bytes=size_bytes,
            device=device,
            inode=inode,
            namespace=namespace,
            entry_name=entry_name or f"{operation_id}.pdf",
            _capability=capability,
        )
        self._trash_tokens[capability] = token
        return token

    def _consume_pending_trash_tokens(self, token: PendingTrash) -> None:
        for capability, issued in tuple(self._trash_tokens.items()):
            if (
                issued.operation_id == token.operation_id
                and issued.namespace == token.namespace
                and issued.entry_name == token.entry_name
                and issued.device == token.device
                and issued.inode == token.inode
            ):
                del self._trash_tokens[capability]

    def _pending_trash_location(self, token: PendingTrash) -> tuple[Path, str]:
        if token.namespace == "legacy":
            return self.trash_dir, f"{_valid_operation_id(token.operation_id)}.pdf"
        if token.namespace == "submission-v2":
            expected = self._submission_trash_entry_name(token.operation_id)
            if token.entry_name != expected:
                raise StorageError("Submission trash token has invalid provenance")
            return (
                self.submission_trash_dir,
                f"{expected}/{_SUBMISSION_TRASH_PAYLOAD}",
            )
        raise StorageError("pending trash token has an unknown namespace")

    def _cleanup_consumed_submission_trash(self, token: PendingTrash) -> None:
        if token.namespace != "submission-v2":
            return
        record = self._submission_trash_record_unlocked(
            token.operation_id,
            missing_ok=False,
        )
        if (
            record.original_name != token.original_name
            or record.entry_name != token.entry_name
            or record.has_payload
        ):
            raise StorageError("Submission trash provenance changed before cleanup")
        self._remove_submission_trash_metadata(record)

    @_serialized
    def pending_exists(self, filename: str) -> bool:
        """Audit whether a contained, single-link pending PDF exists."""
        self._validate_pending_recovery_name(filename)
        destination_fds: list[int] = []
        try:
            destination_fds, parent_fd, name = self._open_destination_parent(filename)
            try:
                os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return None
            with self._opened_regular(
                self.pending_dir,
                filename,
                require_single_link=True,
            ):
                return True
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending PDF presence could not be audited") from exc
        finally:
            for directory_fd in reversed(destination_fds):
                os.close(directory_fd)

    @_serialized
    def rehydrate_pending_trash(
        self,
        original_name: str | None,
        operation_id: str,
    ) -> PendingTrash | None:
        """Re-audit deterministic trash and issue fresh process-local authority."""
        if original_name is not None:
            self._validate_pending_recovery_name(original_name)
        trash_name = f"{_valid_operation_id(operation_id)}.pdf"
        try:
            try:
                os.stat(trash_name, dir_fd=self._trash_fd, follow_symlinks=False)
            except FileNotFoundError:
                return None
            with self._opened_regular(
                self.trash_dir,
                trash_name,
                require_private=True,
            ) as (source_fd, _, _, _, source_stat):
                if source_stat.st_nlink not in {1, 2}:
                    raise StorageError("pending trash has an unsafe link count")
                if source_stat.st_nlink == 2:
                    if original_name is None:
                        raise StorageError(
                            "two-link pending trash requires its persisted original name"
                        )
                    with self._opened_regular(
                        self.pending_dir,
                        original_name,
                        require_private=True,
                    ) as (_, _, _, _, original_stat):
                        if (
                            original_stat.st_nlink != 2
                            or not _same_inode(source_stat, original_stat)
                        ):
                            raise StorageError(
                                "pending trash is not the exact interrupted move pair"
                            )
                elif original_name is not None and self.pending_exists(original_name):
                    raise StorageError(
                        "pending trash conflicts with an occupied original name"
                    )
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    source_hash, source_size = _hash_reader(source)
                return self._issue_pending_trash_token(
                    original_name=original_name or "",
                    operation_id=operation_id,
                    source_sha256=source_hash,
                    size_bytes=source_size,
                    device=source_stat.st_dev,
                    inode=source_stat.st_ino,
                )
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending trash could not be rehydrated") from exc

    @_serialized
    def rehydrate_legacy_submission_trash(
        self,
        original_name: str | None,
        submission_id: str,
    ) -> PendingTrash | None:
        """Audit only the historical flat entry named by the exact Submission ID."""
        if not isinstance(submission_id, str) or _OPERATION_ID.fullmatch(submission_id) is None:
            return None
        return self.rehydrate_pending_trash(original_name, submission_id)

    @_serialized
    def resolve_legacy_submission_trash(
        self,
        original_name: str | None,
        submission_id: str,
    ) -> PendingTrash | None:
        """Resolve legacy authority without guessing an ambiguous V1 hash key.

        A former-current-shaped flat name can belong either to the raw
        Submission ID or to the hash of a different ID.  Only an exact
        two-link interrupted move to the locked row's persisted original name
        proves the former.  All other such entries are quarantined.
        """
        if not self.is_ambiguous_legacy_submission_operation(submission_id):
            return self.rehydrate_legacy_submission_trash(
                original_name,
                submission_id,
            )
        if original_name is not None:
            self._validate_pending_recovery_name(original_name)
        trash_name = f"{submission_id}.pdf"
        try:
            try:
                os.stat(
                    trash_name,
                    dir_fd=self._trash_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return None
            try:
                with self._opened_regular(
                    self.trash_dir,
                    trash_name,
                    require_private=True,
                ) as (_, _, _, _, source_stat):
                    exact_pair = False
                    if source_stat.st_nlink == 2 and original_name is not None:
                        try:
                            with self._opened_regular(
                                self.pending_dir,
                                original_name,
                                require_private=True,
                            ) as (_, _, _, _, original_stat):
                                exact_pair = (
                                    original_stat.st_nlink == 2
                                    and _same_inode(source_stat, original_stat)
                                )
                        except FileNotFoundError:
                            exact_pair = False
                    elif source_stat.st_nlink not in {1, 2}:
                        raise StorageError(
                            "ambiguous Submission trash has an unsafe link count"
                        )
            except FileNotFoundError:
                return None
            if exact_pair:
                return self.rehydrate_pending_trash(original_name, submission_id)
            self.quarantine_ambiguous_legacy_submission_trash(submission_id)
            return None
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError(
                "ambiguous Submission trash could not be resolved"
            ) from exc

    @_serialized
    def legacy_submission_trash_exists(
        self,
        original_name: str | None,
        submission_id: str,
    ) -> bool:
        """Audit exact legacy authority without retaining a capability."""
        token = self.resolve_legacy_submission_trash(
            original_name,
            submission_id,
        )
        if token is None:
            return False
        self._trash_tokens.pop(token._capability, None)
        return True

    @_serialized
    def legacy_submission_trash_entry_present(self, submission_id: str) -> bool:
        """Audit whether an exact active flat entry exists without minting authority."""
        if not isinstance(submission_id, str) or _OPERATION_ID.fullmatch(submission_id) is None:
            return False
        trash_name = f"{submission_id}.pdf"
        try:
            try:
                os.stat(
                    trash_name,
                    dir_fd=self._trash_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return False
            try:
                with self._opened_regular(
                    self.trash_dir,
                    trash_name,
                    require_private=True,
                ) as (_, _, _, _, source_stat):
                    if source_stat.st_nlink not in {1, 2, 3}:
                        raise StorageError(
                            "legacy Submission trash has an unsafe link count"
                        )
                    return True
            except FileNotFoundError:
                return False
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError(
                "legacy Submission trash presence could not be audited"
            ) from exc

    @staticmethod
    def is_ambiguous_legacy_submission_operation(operation_id: str) -> bool:
        """Identify flat keys emitted by V1 and also valid as raw legacy IDs."""
        return (
            isinstance(operation_id, str)
            and _FORMER_SUBMISSION_TRASH_OPERATION.fullmatch(operation_id) is not None
        )

    @_serialized
    def quarantine_ambiguous_legacy_submission_trash(
        self,
        operation_id: str,
    ) -> bool:
        """Move an ownerless former-current flat entry out of active recovery."""
        if not self.is_ambiguous_legacy_submission_operation(operation_id):
            raise StorageError("legacy Submission trash key is not ambiguous")
        trash_name = f"{operation_id}.pdf"
        try:
            try:
                os.stat(trash_name, dir_fd=self._trash_fd, follow_symlinks=False)
            except FileNotFoundError:
                return False
            with self._opened_regular(
                self.trash_dir,
                trash_name,
                require_private=True,
            ) as (_, source_parent_fd, source_name, _, source_stat):
                try:
                    quarantined = os.stat(
                        trash_name,
                        follow_symlinks=False,
                        dir_fd=self._submission_trash_quarantine_fd,
                    )
                    if not _same_inode(quarantined, source_stat):
                        raise StorageError(
                            "ambiguous Submission quarantine is occupied"
                        )
                    if source_stat.st_nlink not in {2, 3}:
                        raise StorageError(
                            "ambiguous Submission trash has an unsafe link count"
                        )
                    linked = False
                except FileNotFoundError:
                    if source_stat.st_nlink not in {1, 2}:
                        raise StorageError(
                            "ambiguous Submission trash has an unsafe link count"
                        )
                    os.link(
                        source_name,
                        trash_name,
                        src_dir_fd=source_parent_fd,
                        dst_dir_fd=self._submission_trash_quarantine_fd,
                        follow_symlinks=False,
                    )
                    linked = True
                    _fsync_directory_fd(self._submission_trash_quarantine_fd)
                current = os.stat(
                    source_name,
                    dir_fd=source_parent_fd,
                    follow_symlinks=False,
                )
                quarantined = os.stat(
                    trash_name,
                    dir_fd=self._submission_trash_quarantine_fd,
                    follow_symlinks=False,
                )
                expected_links = source_stat.st_nlink + (1 if linked else 0)
                if (
                    not _same_inode(current, source_stat)
                    or not _same_inode(quarantined, source_stat)
                    or current.st_nlink != expected_links
                    or quarantined.st_nlink != expected_links
                ):
                    raise StorageError(
                        "ambiguous Submission trash changed during quarantine"
                    )
                if not self._unlink_if_matching(
                    source_parent_fd,
                    source_name,
                    current,
                ):
                    raise StorageError(
                        "ambiguous Submission trash changed before quarantine"
                    )
                _fsync_directory_fd(source_parent_fd)
                final = os.stat(
                    trash_name,
                    dir_fd=self._submission_trash_quarantine_fd,
                    follow_symlinks=False,
                )
                if (
                    not _same_inode(final, source_stat)
                    or final.st_nlink != expected_links - 1
                ):
                    raise StorageError(
                        "ambiguous Submission quarantine did not commit exactly"
                    )
            return True
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError(
                "ambiguous Submission trash could not be quarantined"
            ) from exc

    @_serialized
    def commit_pending_trash(self, token: PendingTrash) -> PendingTrash:
        """Finish an audited link-before-unlink crash in the trash direction."""
        if not isinstance(token, PendingTrash):
            raise StorageError("pending trash commit requires a trash token")
        stored = self._trash_tokens.get(token._capability)
        if stored != token:
            raise StorageError("pending trash token is invalid or already consumed")
        self._validate_pending_recovery_name(token.original_name)
        trash_root, trash_name = self._pending_trash_location(token)
        try:
            with self._opened_regular(
                trash_root,
                trash_name,
                require_private=True,
            ) as (source_fd, source_parent_fd, source_name, _, source_stat):
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    source_hash, source_size = _hash_reader(source)
                if (
                    (source_stat.st_dev, source_stat.st_ino)
                    != (token.device, token.inode)
                    or (source_hash, source_size)
                    != (token.source_sha256, token.size_bytes)
                ):
                    raise StorageError("trashed pending PDF does not match its token")
                if source_stat.st_nlink == 1:
                    return token
                if source_stat.st_nlink != 2:
                    raise StorageError("pending trash has an unsafe link count")
                with self._opened_regular(
                    self.pending_dir,
                    token.original_name,
                    require_private=True,
                ) as (_, original_parent_fd, original_name, _, original_stat):
                    if (
                        original_stat.st_nlink != 2
                        or not _same_inode(source_stat, original_stat)
                    ):
                        raise StorageError(
                            "pending trash is not the exact interrupted move pair"
                        )
                    if not self._unlink_if_matching(
                        original_parent_fd,
                        original_name,
                        original_stat,
                    ):
                        raise StorageError("pending original changed before trash commit")
                    _fsync_directory_fd(original_parent_fd)
                final = os.stat(
                    source_name,
                    dir_fd=source_parent_fd,
                    follow_symlinks=False,
                )
                if not _same_inode(final, source_stat) or final.st_nlink != 1:
                    raise StorageError("pending trash commit did not produce one name")
                return token
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending trash could not be committed") from exc

    def _open_destination_parent(self, relative_name: str) -> tuple[list[int], int, str]:
        parts = Path(relative_name).parts
        fds = [os.dup(self._pending_fd)]
        try:
            for component in parts[:-1]:
                fds.append(os.open(component, _DIRECTORY_FLAGS, dir_fd=fds[-1]))
            return fds, fds[-1], parts[-1]
        except Exception:
            for directory_fd in reversed(fds):
                os.close(directory_fd)
            raise

    @_serialized
    def restore_pending(self, token: PendingTrash) -> None:
        if not isinstance(token, PendingTrash):
            raise StorageError("pending restore requires a trash token")
        stored = self._trash_tokens.get(token._capability)
        if stored != token:
            raise StorageError("pending trash token is invalid or already consumed")
        self._validate_pending_recovery_name(token.original_name)
        original_relative = token.original_name
        trash_root, trash_name = self._pending_trash_location(token)
        destination_fds: list[int] = []
        payload_moved = False
        try:
            destination_fds, destination_parent_fd, destination_name = (
                self._open_destination_parent(original_relative)
            )
            with self._opened_regular(
                trash_root,
                trash_name,
                require_private=True,
            ) as (
                source_fd,
                source_parent_fd,
                source_name,
                _resolved,
                source_stat,
            ):
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    source_hash, source_size = _hash_reader(source)
                if (
                    (source_stat.st_dev, source_stat.st_ino)
                    != (token.device, token.inode)
                    or (source_hash, source_size)
                    != (token.source_sha256, token.size_bytes)
                ):
                    raise StorageError("trashed pending PDF does not match its token")
                if source_stat.st_nlink == 2:
                    with self._opened_regular(
                        self.pending_dir,
                        original_relative,
                        require_private=True,
                    ) as (_, _, _, _, original_stat):
                        if (
                            original_stat.st_nlink != 2
                            or not _same_inode(source_stat, original_stat)
                        ):
                            raise StorageError(
                                "pending trash is not the exact interrupted move pair"
                            )
                        if not self._unlink_if_matching(
                            source_parent_fd,
                            source_name,
                            source_stat,
                        ):
                            raise StorageError("pending trash changed before restore")
                        payload_moved = True
                        try:
                            _fsync_directory_fd(source_parent_fd)
                        except OSError:
                            pass
                elif source_stat.st_nlink == 1:
                    self._link_then_unlink(
                        source_parent_fd,
                        source_name,
                        source_stat,
                        destination_parent_fd,
                        destination_name,
                    )
                    payload_moved = True
                else:
                    raise StorageError("pending trash has an unsafe link count")
            self._cleanup_consumed_submission_trash(token)
            self._consume_pending_trash_tokens(token)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending PDF could not be restored") from exc
        finally:
            if payload_moved:
                self._consume_pending_trash_tokens(token)
            for directory_fd in reversed(destination_fds):
                os.close(directory_fd)

    @_serialized
    def discard_pending_trash(self, token: PendingTrash) -> None:
        """Consume one audited capability and durably remove its exact inode."""
        if not isinstance(token, PendingTrash):
            raise StorageError("pending discard requires a trash token")
        stored = self._trash_tokens.get(token._capability)
        if stored != token:
            raise StorageError("pending trash token is invalid or already consumed")
        trash_root, trash_name = self._pending_trash_location(token)
        payload_removed = False
        try:
            with self._opened_regular(
                trash_root,
                trash_name,
                require_single_link=True,
                require_private=True,
            ) as (source_fd, source_parent_fd, source_name, _, source_stat):
                with os.fdopen(os.dup(source_fd), "rb") as source:
                    source_hash, source_size = _hash_reader(source)
                if (
                    (source_stat.st_dev, source_stat.st_ino)
                    != (token.device, token.inode)
                    or (source_hash, source_size)
                    != (token.source_sha256, token.size_bytes)
                ):
                    raise StorageError("trashed pending PDF does not match its token")
                if not self._unlink_if_matching(
                    source_parent_fd,
                    source_name,
                    source_stat,
                ):
                    raise StorageError("pending trash changed before removal")
                payload_removed = True
                _fsync_directory_fd(source_parent_fd)
            self._cleanup_consumed_submission_trash(token)
            self._consume_pending_trash_tokens(token)
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending trash could not be discarded") from exc
        finally:
            if payload_removed:
                self._consume_pending_trash_tokens(token)

    def _stale_pending_trash_candidate(
        self,
        name: str,
        directory: os.stat_result,
        cutoff_timestamp: float,
    ) -> str | None:
        if name in {
            _SUBMISSION_TRASH_DIRECTORY,
            _SUBMISSION_TRASH_QUARANTINE_DIRECTORY,
        }:
            expected = (
                self._submission_trash_stat
                if name == _SUBMISSION_TRASH_DIRECTORY
                else self._submission_trash_quarantine_stat
            )
            self._verify_reserved_directory(self._trash_fd, name, expected)
            return None
        if not name.endswith(".pdf"):
            raise StorageError("reserved trash contains an unknown entry")
        operation_id = name[:-4]
        _valid_operation_id(operation_id)
        entry = os.stat(name, dir_fd=self._trash_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(entry.st_mode)
            or not _private_mode(entry, 0o600)
            or entry.st_nlink not in {1, 2}
            or entry.st_dev != directory.st_dev
        ):
            raise StorageError("reserved trash contains an unsafe entry")
        resolved = resolve_contained(self.trash_dir, name, must_exist=True)
        if resolved is None:
            raise StorageError("reserved trash entry is not contained")
        if entry.st_mtime < cutoff_timestamp:
            return operation_id
        return None

    @_serialized
    def stale_pending_trash(
        self,
        cutoff: datetime | float | int,
        *,
        on_error: Callable[[str, BaseException], None] | None = None,
    ) -> tuple[str, ...]:
        """List only descriptor-audited deterministic trash older than cutoff."""
        cutoff_timestamp = self._cutoff_timestamp(cutoff)
        try:
            directory = os.fstat(self._trash_fd)
            names = tuple(sorted(os.listdir(self._trash_fd)))
        except (OSError, RuntimeError, ValueError) as exc:
            raise StorageError("pending trash could not be audited") from exc

        operation_ids: list[str] = []
        for name in names:
            candidate = f"legacy-submission-trash:{name}"
            try:
                operation_id = self._stale_pending_trash_candidate(
                    name,
                    directory,
                    cutoff_timestamp,
                )
            except StorageError as exc:
                if on_error is None:
                    raise
                on_error(candidate, exc)
                continue
            except (OSError, RuntimeError, ValueError) as exc:
                error = StorageError("pending trash could not be audited")
                if on_error is None:
                    raise error from exc
                on_error(candidate, error)
                continue
            if operation_id is not None:
                operation_ids.append(operation_id)
        return tuple(sorted(operation_ids))

    @staticmethod
    def _cutoff_timestamp(cutoff: datetime | float | int) -> float:
        if isinstance(cutoff, datetime):
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=timezone.utc)
            timestamp = cutoff.timestamp()
            if not math.isfinite(timestamp):
                raise StorageError("invalid reconciliation cutoff")
            return timestamp
        if isinstance(cutoff, bool) or not isinstance(cutoff, (int, float)):
            raise StorageError("invalid reconciliation cutoff")
        timestamp = float(cutoff)
        if not math.isfinite(timestamp):
            raise StorageError("invalid reconciliation cutoff")
        return timestamp

    def _publication_link_identities(self) -> set[tuple[int, int]]:
        """Recognize only exact stage/final pairs left by committed promotion."""
        staged: dict[tuple[int, int], int] = {}
        finals: dict[tuple[int, int], int] = {}
        for name in os.listdir(self._staging_fd):
            if not name.endswith(".pdf") or _OPERATION_ID.fullmatch(name[:-4]) is None:
                continue
            entry = os.stat(name, dir_fd=self._staging_fd, follow_symlinks=False)
            if (
                stat.S_ISREG(entry.st_mode)
                and _private_mode(entry, 0o600)
                and entry.st_nlink == 2
            ):
                key = (entry.st_dev, entry.st_ino)
                staged[key] = staged.get(key, 0) + 1

        for paper_id in os.listdir(self._papers_fd):
            try:
                if _canonical_paper_id(paper_id) != paper_id:
                    continue
            except StorageError:
                continue
            try:
                paper_fd = os.open(paper_id, _DIRECTORY_FLAGS, dir_fd=self._papers_fd)
            except OSError:
                continue
            try:
                for filename in os.listdir(paper_fd):
                    if _REVISION_FILENAME.fullmatch(filename) is None:
                        continue
                    entry = os.stat(
                        filename,
                        dir_fd=paper_fd,
                        follow_symlinks=False,
                    )
                    if (
                        stat.S_ISREG(entry.st_mode)
                        and _private_mode(entry, 0o600)
                        and entry.st_nlink == 2
                    ):
                        key = (entry.st_dev, entry.st_ino)
                        finals[key] = finals.get(key, 0) + 1
            finally:
                os.close(paper_fd)
        return {
            key
            for key, count in staged.items()
            if count == 1 and finals.get(key) == 1
        }

    @staticmethod
    def _report_reconciliation_error(
        on_error: Callable[[str, BaseException], None] | None,
        candidate: str,
        error: BaseException,
    ) -> None:
        if on_error is None:
            raise error
        on_error(candidate, error)

    @_serialized
    def reconciliation_paper_ids(self) -> tuple[str, ...]:
        """Return canonical storage namespaces without auditing their contents."""
        try:
            paper_ids = []
            for name in os.listdir(self._papers_fd):
                try:
                    if _canonical_paper_id(name) == name:
                        paper_ids.append(name)
                except StorageError:
                    continue
            return tuple(sorted(paper_ids))
        except OSError as exc:
            raise StorageError("Paper revision namespaces could not be listed") from exc

    @_serialized
    def reconcile_staging_expired(
        self,
        cutoff: datetime | float | int,
        *,
        on_error: Callable[[str, BaseException], None] | None = None,
    ) -> int:
        """Remove old staging entries while isolating unsafe candidates."""
        cutoff_timestamp = self._cutoff_timestamp(cutoff)
        try:
            names = tuple(sorted(os.listdir(self._staging_fd)))
        except OSError as exc:
            raise StorageError("staging storage could not be listed") from exc

        try:
            publication_links = self._publication_link_identities()
        except Exception as exc:
            error = StorageError("publication links could not be audited")
            error.__cause__ = exc
            self._report_reconciliation_error(
                on_error,
                "staging:publication-links",
                error,
            )
            publication_links = set()

        directory_stat = os.fstat(self._staging_fd)
        entries: dict[str, os.stat_result] = {}
        errors: list[tuple[str, BaseException]] = []
        for name in names:
            try:
                entry = os.stat(
                    name,
                    dir_fd=self._staging_fd,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(entry.st_mode)
                    or not _private_mode(entry, 0o600)
                    or entry.st_dev != directory_stat.st_dev
                ):
                    raise StorageError("staging entry is unsafe")
                entries[name] = entry
            except Exception as exc:
                errors.append((f"staging:{name}", exc))

        eligible: dict[str, os.stat_result] = {}
        for name, entry in entries.items():
            try:
                if entry.st_nlink != 1:
                    allowed_internal_link = False
                    if entry.st_nlink == 2:
                        backup_match = _METADATA_BACKUP.fullmatch(name)
                        if backup_match is not None:
                            partner_name = f"{backup_match.group('operation')}.pdf"
                            partner = entries.get(partner_name)
                            allowed_internal_link = (
                                partner is not None
                                and partner.st_nlink == 2
                                and _same_inode(entry, partner)
                            )
                        elif (
                            name.endswith(".pdf")
                            and _OPERATION_ID.fullmatch(name[:-4]) is not None
                        ):
                            partners = [
                                other
                                for other_name, other in entries.items()
                                if (
                                    (match := _METADATA_BACKUP.fullmatch(other_name))
                                    is not None
                                    and match.group("operation") == name[:-4]
                                    and _same_inode(entry, other)
                                )
                            ]
                            allowed_internal_link = (
                                len(partners) == 1
                                or (entry.st_dev, entry.st_ino) in publication_links
                            )
                    if not allowed_internal_link:
                        raise StorageError("staging entry has an unknown hard link")
                try:
                    resolved = resolve_contained(
                        self.staging_dir,
                        name,
                        must_exist=True,
                    )
                except (OSError, RuntimeError, ValueError):
                    resolved = None
                if resolved is None:
                    raise StorageError("staging entry is not contained")
                if entry.st_mtime < cutoff_timestamp:
                    eligible[name] = entry
            except Exception as exc:
                errors.append((f"staging:{name}", exc))

        if on_error is None and errors:
            raise errors[0][1]
        for candidate, error in errors:
            self._report_reconciliation_error(on_error, candidate, error)

        removed = 0
        for name, expected in eligible.items():
            try:
                if not self._unlink_if_matching(self._staging_fd, name, expected):
                    raise StorageError("staging entry changed before removal")
                removed += 1
            except Exception as exc:
                self._report_reconciliation_error(
                    on_error,
                    f"staging:{name}",
                    exc,
                )
        if removed:
            _fsync_directory_fd(self._staging_fd)
        return removed

    def _paper_revision_namespace(
        self,
        paper_id: str,
        publication_links: set[tuple[int, int]],
    ) -> tuple[os.stat_result, dict[str, os.stat_result]] | None:
        try:
            directory = os.stat(
                paper_id,
                dir_fd=self._papers_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return None
        if (
            not stat.S_ISDIR(directory.st_mode)
            or not _private_mode(directory, 0o700)
            or directory.st_dev != self._papers_stat.st_dev
        ):
            raise StorageError("Paper revision namespace is unsafe")
        paper_fd = os.open(paper_id, _DIRECTORY_FLAGS, dir_fd=self._papers_fd)
        try:
            opened = os.fstat(paper_fd)
            if not _same_inode(directory, opened):
                raise StorageError("Paper revision directory changed")
            revisions: dict[str, os.stat_result] = {}
            for filename in os.listdir(paper_fd):
                entry = os.stat(
                    filename,
                    dir_fd=paper_fd,
                    follow_symlinks=False,
                )
                if stat.S_ISDIR(entry.st_mode):
                    raise StorageError("Paper revision layout contains a directory")
                if not stat.S_ISREG(entry.st_mode):
                    raise StorageError("Paper revision layout contains a special entry")
                if _REVISION_FILENAME.fullmatch(filename) is None:
                    continue
                if (
                    not _private_mode(entry, 0o600)
                    or entry.st_dev != opened.st_dev
                    or (
                        entry.st_nlink != 1
                        and (entry.st_dev, entry.st_ino) not in publication_links
                    )
                ):
                    raise StorageError("Paper revision file is unsafe")
                revisions[filename] = entry
            return directory, revisions
        finally:
            os.close(paper_fd)

    @_serialized
    def reconcile_paper_expired(
        self,
        paper_id: str,
        cutoff: datetime | float | int,
        referenced_revisions: Iterable[int],
    ) -> int:
        """Remove old unowned finals from one Paper namespace only."""
        canonical = _canonical_paper_id(paper_id)
        cutoff_timestamp = self._cutoff_timestamp(cutoff)
        referenced = {_valid_revision(value) for value in referenced_revisions}
        try:
            publication_links = self._publication_link_identities()
            namespace = self._paper_revision_namespace(canonical, publication_links)
            if namespace is None:
                return 0
            expected_directory, revisions = namespace
            paper_fd = os.open(canonical, _DIRECTORY_FLAGS, dir_fd=self._papers_fd)
            try:
                opened = os.fstat(paper_fd)
                if not _same_inode(expected_directory, opened):
                    raise StorageError("Paper directory changed before reconciliation")
                removed = 0
                for filename, expected in revisions.items():
                    revision = int(filename[:-4])
                    if revision in referenced or expected.st_mtime >= cutoff_timestamp:
                        continue
                    try:
                        resolved = resolve_contained(
                            self.papers_dir,
                            f"{canonical}/{filename}",
                            must_exist=True,
                        )
                    except (OSError, RuntimeError, ValueError):
                        resolved = None
                    if resolved is None:
                        raise StorageError("revision escaped containment before removal")
                    if not self._unlink_if_matching(paper_fd, filename, expected):
                        raise StorageError("revision changed before reconciliation")
                    removed += 1
                if removed:
                    _fsync_directory_fd(paper_fd)
            finally:
                os.close(paper_fd)

            try:
                check_fd = os.open(
                    canonical,
                    _DIRECTORY_FLAGS,
                    dir_fd=self._papers_fd,
                )
            except FileNotFoundError:
                remaining = None
            else:
                try:
                    remaining = os.listdir(check_fd)
                finally:
                    os.close(check_fd)
            if remaining == []:
                current_directory = os.stat(
                    canonical,
                    dir_fd=self._papers_fd,
                    follow_symlinks=False,
                )
                if not _same_inode(expected_directory, current_directory):
                    raise StorageError("Paper directory changed before reconciliation")
                os.rmdir(canonical, dir_fd=self._papers_fd)
                _fsync_directory_fd(self._papers_fd)
            return removed
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("Paper storage reconciliation failed") from exc

    @_serialized
    def reconcile_expired(
        self,
        cutoff: datetime | float | int,
        referenced_revisions: Iterable[tuple[str, int]],
    ) -> int:
        cutoff_timestamp = self._cutoff_timestamp(cutoff)
        referenced: set[tuple[str, int]] = set()
        for paper_id, revision in referenced_revisions:
            referenced.add((_canonical_paper_id(paper_id), _valid_revision(revision)))

        by_paper: dict[str, set[int]] = {}
        for paper_id, revision in referenced:
            by_paper.setdefault(paper_id, set()).add(revision)

        removed = self.reconcile_staging_expired(cutoff_timestamp)
        for paper_id in self.reconciliation_paper_ids():
            removed += self.reconcile_paper_expired(
                paper_id,
                cutoff_timestamp,
                by_paper.get(paper_id, set()),
            )
        return removed
