"""Durable, deterministic PDF storage for immutable Paper revisions."""

from __future__ import annotations

import hashlib
import ctypes
import errno
import fcntl
import functools
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
from typing import BinaryIO, Iterable, Iterator

from PyPDF2 import PdfReader, PdfWriter

from services.paper_identity import validate_paper_id
from services.papers import resolve_contained
from services.publishing_contracts import PdfUpload


_BLOCK_SIZE = 1024 * 1024
_OPERATION_ID = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,127})\Z")
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
    """Process-local one-use authority for one committed pending-file trash.

    Tokens are intentionally not restart-persistent. Failed or restarted
    operations leave only private managed entries for ``reconcile_expired``;
    automatic cross-process restore requires a later persisted-recovery
    contract.
    """

    original_name: str
    operation_id: str
    source_sha256: str
    size_bytes: int
    device: int
    inode: int
    _capability: str = field(repr=False)


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
        publication_links = self._publication_link_identities()
        try:
            with self._opened_regular(
                self.staging_dir,
                path.name,
                require_private=True,
            ) as (stage_fd, _, _, _, opened):
                if not _same_inode(expected, opened):
                    raise StorageError("staged PDF changed before discard")
                if opened.st_nlink != 1 and (
                    opened.st_nlink != 2
                    or (opened.st_dev, opened.st_ino) not in publication_links
                ):
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
                if expected is not None and not self._unlink_if_matching(
                    root_fd,
                    filename,
                    expected,
                ):
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
            capability = secrets.token_urlsafe(32)
            token = PendingTrash(
                original_name=filename,
                operation_id=operation_id,
                source_sha256=source_hash,
                size_bytes=source_size,
                device=final.st_dev,
                inode=final.st_ino,
                _capability=capability,
            )
            self._trash_tokens[capability] = token
            return token
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("pending PDF could not be trashed") from exc

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
        self._relative_policy(
            self.pending_dir,
            token.original_name,
            must_exist=False,
        )
        original_relative = token.original_name
        trash_name = f"{token.operation_id}.pdf"
        destination_fds: list[int] = []
        try:
            destination_fds, destination_parent_fd, destination_name = (
                self._open_destination_parent(original_relative)
            )
            with self._opened_regular(
                self.trash_dir,
                trash_name,
                require_single_link=True,
                require_private=True,
            ) as (
                source_fd,
                _source_parent_fd,
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
                self._link_then_unlink(
                    self._trash_fd,
                    source_name,
                    source_stat,
                    destination_parent_fd,
                    destination_name,
                )
            del self._trash_tokens[token._capability]
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

    def _stale_entries(
        self,
        directory: Path,
        directory_fd: int,
        cutoff: float,
        publication_links: set[tuple[int, int]],
    ) -> dict[str, os.stat_result]:
        stale: dict[str, os.stat_result] = {}
        entries: dict[str, os.stat_result] = {}
        directory_stat = os.fstat(directory_fd)
        for name in os.listdir(directory_fd):
            entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                not stat.S_ISREG(entry.st_mode)
                or not _private_mode(entry, 0o600)
                or entry.st_dev != directory_stat.st_dev
            ):
                raise StorageError("reserved storage contains an unsafe entry")
            entries[name] = entry

        for name, entry in entries.items():
            if entry.st_nlink != 1:
                allowed_internal_link = False
                if directory_fd == self._staging_fd and entry.st_nlink == 2:
                    backup_match = _METADATA_BACKUP.fullmatch(name)
                    if backup_match is not None:
                        partner_name = f"{backup_match.group('operation')}.pdf"
                        partner = entries.get(partner_name)
                        allowed_internal_link = (
                            partner is not None
                            and partner.st_nlink == 2
                            and _same_inode(entry, partner)
                        )
                    elif name.endswith(".pdf") and _OPERATION_ID.fullmatch(name[:-4]):
                        partners = [
                            other
                            for other_name, other in entries.items()
                            if _METADATA_BACKUP.fullmatch(other_name)
                            and _METADATA_BACKUP.fullmatch(other_name).group("operation")
                            == name[:-4]
                            and _same_inode(entry, other)
                        ]
                        allowed_internal_link = (
                            len(partners) == 1
                            or (entry.st_dev, entry.st_ino) in publication_links
                        )
                elif directory_fd == self._trash_fd and entry.st_nlink == 2:
                    allowed_internal_link = (
                        name.endswith(".pdf")
                        and _OPERATION_ID.fullmatch(name[:-4]) is not None
                    )
                if not allowed_internal_link:
                    raise StorageError("reserved storage contains an unknown hard link")
            try:
                resolved = resolve_contained(directory, name, must_exist=True)
            except (OSError, RuntimeError, ValueError):
                resolved = None
            if resolved is None:
                raise StorageError("reserved storage entry is not contained")
            if entry.st_mtime < cutoff:
                stale[name] = entry
        return stale

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

    def _revision_namespace(
        self,
        publication_links: set[tuple[int, int]],
    ) -> dict[str, tuple[os.stat_result, dict[str, os.stat_result]]]:
        namespace: dict[str, tuple[os.stat_result, dict[str, os.stat_result]]] = {}
        for paper_id in os.listdir(self._papers_fd):
            try:
                if _canonical_paper_id(paper_id) != paper_id:
                    continue
            except StorageError:
                continue
            directory = os.stat(
                paper_id,
                dir_fd=self._papers_fd,
                follow_symlinks=False,
            )
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
                namespace[paper_id] = (directory, revisions)
            finally:
                os.close(paper_fd)
        return namespace

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

        publication_links = self._publication_link_identities()
        staging_stale = self._stale_entries(
            self.staging_dir,
            self._staging_fd,
            cutoff_timestamp,
            publication_links,
        )
        trash_stale = self._stale_entries(
            self.trash_dir,
            self._trash_fd,
            cutoff_timestamp,
            publication_links,
        )
        namespace = self._revision_namespace(publication_links)
        removed = 0
        try:
            for directory_fd, entries in (
                (self._staging_fd, staging_stale),
                (self._trash_fd, trash_stale),
            ):
                directory_removed = 0
                for name, expected in entries.items():
                    if not self._unlink_if_matching(directory_fd, name, expected):
                        raise StorageError("stale storage entry changed before removal")
                    removed += 1
                    directory_removed += 1
                    if directory_fd == self._trash_fd:
                        operation_id = name[:-4] if name.endswith(".pdf") else None
                        for capability, token in tuple(self._trash_tokens.items()):
                            if token.operation_id == operation_id:
                                del self._trash_tokens[capability]
                if directory_removed:
                    _fsync_directory_fd(directory_fd)

            for paper_id, (expected_directory, revisions) in namespace.items():
                paper_fd = os.open(paper_id, _DIRECTORY_FLAGS, dir_fd=self._papers_fd)
                try:
                    opened = os.fstat(paper_fd)
                    if not _same_inode(expected_directory, opened):
                        raise StorageError("Paper directory changed before reconciliation")
                    paper_removed = 0
                    for filename, expected in revisions.items():
                        revision = int(filename[:-4])
                        if (paper_id, revision) in referenced:
                            continue
                        try:
                            resolved = resolve_contained(
                                self.papers_dir,
                                f"{paper_id}/{filename}",
                                must_exist=True,
                            )
                        except (OSError, RuntimeError, ValueError):
                            resolved = None
                        if resolved is None:
                            raise StorageError("revision escaped containment before removal")
                        if not self._unlink_if_matching(paper_fd, filename, expected):
                            raise StorageError("revision changed before reconciliation")
                        removed += 1
                        paper_removed += 1
                    if paper_removed:
                        _fsync_directory_fd(paper_fd)
                finally:
                    os.close(paper_fd)
                try:
                    check_fd = os.open(
                        paper_id,
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
                        paper_id,
                        dir_fd=self._papers_fd,
                        follow_symlinks=False,
                    )
                    if not _same_inode(expected_directory, current_directory):
                        raise StorageError("Paper directory changed before reconciliation")
                    os.rmdir(paper_id, dir_fd=self._papers_fd)
                    _fsync_directory_fd(self._papers_fd)
            return removed
        except StorageError:
            raise
        except OSError as exc:
            raise StorageError("storage reconciliation failed") from exc
