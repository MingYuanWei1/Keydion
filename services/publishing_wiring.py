"""Production construction for the framework-free publishing lifecycle."""

from __future__ import annotations

import os
import random
import stat
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from services.paper_library import PaperLibrary
from services.paper_storage import PaperStorage, StorageError
from services.publishing import PublishingLifecycle
from services.publishing_jobs import PublishingWorker
from services.publishing_rag import StrictRagAdapter


_PARENT_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
_DIRECTORY_FLAGS = _PARENT_DIRECTORY_FLAGS | getattr(os, "O_NOFOLLOW", 0)


@dataclass(frozen=True)
class WebPublishingServices:
    """App-owned services sharing one caller-owned PaperStorage instance."""

    storage: PaperStorage
    lifecycle: PublishingLifecycle
    library: PaperLibrary


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _after_private_storage_root_opened(_path: Path, _descriptor: int) -> None:
    """Test seam reached before an opened root is allowed to be changed."""


def _validate_storage_root_topology(papers_dir: Path, pending_dir: Path) -> None:
    """Reject same or nested roots before either path can be mutated."""
    papers = Path(os.path.abspath(os.fspath(papers_dir)))
    pending = Path(os.path.abspath(os.fspath(pending_dir)))
    try:
        physical_papers = papers.resolve(strict=False)
        physical_pending = pending.resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise StorageError("storage root relationship is unsafe") from exc

    def related(left: Path, right: Path) -> bool:
        return left == right or left in right.parents or right in left.parents

    if related(papers, pending) or related(physical_papers, physical_pending):
        raise StorageError("Paper and pending storage roots must be disjoint")

    try:
        papers_info = papers.stat()
    except FileNotFoundError:
        papers_info = None
    except OSError as exc:
        raise StorageError("Paper storage root is unsafe") from exc
    try:
        pending_info = pending.stat()
    except FileNotFoundError:
        pending_info = None
    except OSError as exc:
        raise StorageError("pending storage root is unsafe") from exc
    if (
        papers_info is not None
        and pending_info is not None
        and _same_inode(papers_info, pending_info)
    ):
        raise StorageError("Paper and pending storage roots must be disjoint")


def _prepare_private_storage_root(path: Path) -> Path:
    """Create or safely tighten one app-owned storage root to mode 0700."""
    absolute = Path(os.path.abspath(os.fspath(path)))
    name = absolute.name
    if not name:
        raise StorageError("storage root cannot be a filesystem root")
    try:
        absolute.parent.mkdir(parents=True, exist_ok=True)
        parent_descriptor = os.open(
            os.fspath(absolute.parent),
            _PARENT_DIRECTORY_FLAGS,
        )
    except OSError as exc:
        raise StorageError("storage root parent is unsafe") from exc
    descriptor = None
    try:
        parent_identity = os.fstat(parent_descriptor)
        needs_parent_fsync = False
        try:
            before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            needs_parent_fsync = True
            try:
                os.mkdir(name, mode=0o700, dir_fd=parent_descriptor)
            except FileExistsError:
                pass
            before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if not stat.S_ISDIR(before.st_mode) or before.st_uid != os.getuid():
            raise StorageError("storage root is not an app-owned real directory")

        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
        opened = os.fstat(descriptor)
        current_parent = absolute.parent.stat()
        current_entry = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        current_path = absolute.lstat()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_uid != os.getuid()
            or not _same_inode(before, opened)
            or not _same_inode(parent_identity, current_parent)
            or not _same_inode(opened, current_entry)
            or not _same_inode(opened, current_path)
        ):
            raise StorageError("storage root changed while opening")

        _after_private_storage_root_opened(absolute, descriptor)
        current_parent = absolute.parent.stat()
        current_entry = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        current_path = absolute.lstat()
        if (
            not _same_inode(parent_identity, current_parent)
            or not _same_inode(opened, current_entry)
            or not _same_inode(opened, current_path)
        ):
            raise StorageError("storage root changed while opening")

        os.fchmod(descriptor, 0o700)
        tightened = os.fstat(descriptor)
        final_parent = absolute.parent.stat()
        final_entry = os.stat(
            name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        final_path = absolute.lstat()
        if (
            not stat.S_ISDIR(tightened.st_mode)
            or tightened.st_uid != os.getuid()
            or stat.S_IMODE(tightened.st_mode) != 0o700
            or not _same_inode(opened, tightened)
            or not _same_inode(parent_identity, final_parent)
            or not _same_inode(tightened, final_entry)
            or not _same_inode(tightened, final_path)
        ):
            raise StorageError("storage root could not be made private")
        os.fsync(descriptor)
        if needs_parent_fsync:
            os.fsync(parent_descriptor)
    except StorageError:
        raise
    except OSError as exc:
        raise StorageError("storage root could not be prepared safely") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent_descriptor)
    return absolute


def _build_storage() -> PaperStorage:
    from config import (
        PAPERS_DIR,
        PENDING_PAPERS_DIR,
    )

    _validate_storage_root_topology(PAPERS_DIR, PENDING_PAPERS_DIR)
    papers_root = _prepare_private_storage_root(PAPERS_DIR)
    pending_root = _prepare_private_storage_root(PENDING_PAPERS_DIR)
    return PaperStorage(papers_root, pending_root)


def _build_lifecycle(session_factory, *, storage=None):
    from config import PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS
    from services.publishing_time import utc_now_db

    return PublishingLifecycle(
        session_factory=session_factory,
        storage=storage if storage is not None else _build_storage(),
        indexer=StrictRagAdapter(),
        clock=utc_now_db,
        monotonic_clock=time.monotonic,
        uuid_factory=lambda: str(uuid.uuid4()),
        jitter=random.random,
        inline_index_timeout_seconds=PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS,
    )


def _build_web_services(session_factory) -> WebPublishingServices:
    storage = _build_storage()
    try:
        lifecycle = _build_lifecycle(session_factory, storage=storage)
        library = PaperLibrary(
            session_factory=session_factory,
            storage=storage,
        )
        return WebPublishingServices(
            storage=storage,
            lifecycle=lifecycle,
            library=library,
        )
    except Exception:
        storage.close()
        raise


def build_publishing_services() -> WebPublishingServices:
    from db import get_session_factory

    return _build_web_services(get_session_factory())


def build_publishing_lifecycle():
    from db import get_session_factory

    return _build_lifecycle(get_session_factory())


def build_publishing_worker():
    from config import (
        PUBLISHING_JOB_LEASE_SECONDS,
        PUBLISHING_RESERVATION_GRACE_SECONDS,
        PUBLISHING_WORKER_POLL_SECONDS,
    )
    from db import get_session_factory
    from services.publishing_time import utc_now_db

    session_factory = get_session_factory()
    lifecycle = _build_lifecycle(session_factory)
    return PublishingWorker(
        lifecycle=lifecycle,
        session_factory=session_factory,
        clock=utc_now_db,
        monotonic_clock=time.monotonic,
        lease_token_factory=lambda: str(uuid.uuid4()),
        jitter=random.random,
        lease_seconds=PUBLISHING_JOB_LEASE_SECONDS,
        reservation_grace_seconds=PUBLISHING_RESERVATION_GRACE_SECONDS,
        poll_seconds=PUBLISHING_WORKER_POLL_SECONDS,
    )
