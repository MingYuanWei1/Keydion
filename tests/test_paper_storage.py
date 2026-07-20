import hashlib
import io
import math
import os
import stat
import tempfile
import threading
import time
import unittest
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from unittest import mock

from PyPDF2 import PdfReader, PdfWriter

import services.paper_storage as storage_module
from services.paper_storage import PaperStorage, StorageError
from services.publishing_contracts import PdfUpload


PAPER_ID = "11111111-1111-4111-8111-111111111111"
OTHER_PAPER_ID = "22222222-2222-4222-8222-222222222222"


class PaperStorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.storage = PaperStorage(root / "papers", root / "pending")

    def tearDown(self):
        self.tmp.cleanup()

    def valid_pdf_upload(self, filename, width=72):
        stream = io.BytesIO()
        writer = PdfWriter()
        writer.add_blank_page(width=width, height=72)
        writer.write(stream)
        stream.seek(0)
        return PdfUpload(filename, stream)

    def write_pending_pdf(self, filename="pending.pdf", width=72):
        path = self.storage.pending_dir / filename
        upload = self.valid_pdf_upload(filename, width=width)
        path.write_bytes(upload.stream.read())
        return path

    def test_constructor_rejects_symlinked_roots_and_reserved_directories(self):
        root = Path(self.tmp.name) / "unsafe-constructor"
        root.mkdir(mode=0o700)
        outside = root / "outside"
        outside.mkdir(mode=0o700)

        papers_link = root / "papers-link"
        papers_link.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(StorageError):
            PaperStorage(papers_link, root / "pending-a")

        papers = root / "papers"
        pending = root / "pending-b"
        papers.mkdir(mode=0o700)
        pending.mkdir(mode=0o700)
        (papers / ".staging").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(StorageError):
            PaperStorage(papers, pending)

        papers_c = root / "papers-c"
        pending_c = root / "pending-c"
        papers_c.mkdir(mode=0o700)
        pending_c.mkdir(mode=0o700)
        (pending_c / ".trash").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(StorageError):
            PaperStorage(papers_c, pending_c)

        papers_d = root / "papers-d"
        pending_d = root / "pending-d"
        papers_d.mkdir(mode=0o755)
        pending_d.mkdir(mode=0o700)
        with self.assertRaises(StorageError):
            PaperStorage(papers_d, pending_d)
        self.assertEqual(list(outside.iterdir()), [])

    def test_constructor_rejects_equal_roots_before_reserved_mutation(self):
        common = Path(self.tmp.name) / "equal-root"

        with self.assertRaises(StorageError):
            PaperStorage(common, common)

        self.assertTrue(common.is_dir())
        self.assertEqual(list(common.iterdir()), [])

    def test_constructor_rejects_nested_roots_before_reserved_mutation(self):
        cases = (
            ("papers-parent", Path("papers"), Path("papers/pending")),
            ("pending-parent", Path("pending/papers"), Path("pending")),
        )
        for label, papers_relative, pending_relative in cases:
            with self.subTest(label=label):
                root = Path(self.tmp.name) / label
                papers = root / papers_relative
                pending = root / pending_relative

                with self.assertRaises(StorageError):
                    PaperStorage(papers, pending)

                self.assertFalse((papers / ".staging").exists())
                self.assertFalse((pending / ".trash").exists())
                self.assertFalse((papers / ".paper-storage.lock").exists())

    def test_private_storage_modes_are_explicit(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-mode")
        stored = self.storage.promote(staged, PAPER_ID, 1)
        for directory in (
            self.storage.papers_dir,
            self.storage.pending_dir,
            self.storage.staging_dir,
            self.storage.trash_dir,
            stored.path.parent,
        ):
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        for private_file in (
            stored.path,
            self.storage.papers_dir / ".paper-storage.lock",
        ):
            self.assertEqual(stat.S_IMODE(private_file.stat().st_mode), 0o600)

    def test_stage_fails_closed_if_reserved_directory_is_replaced(self):
        original_staging = self.storage.staging_dir.with_name("detached-staging")
        self.storage.staging_dir.rename(original_staging)
        outside = Path(self.tmp.name) / "outside-stage"
        outside.mkdir(mode=0o700)
        self.storage.staging_dir.symlink_to(outside, target_is_directory=True)

        with self.assertRaises(StorageError):
            self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-escape")

        self.assertEqual(list(outside.iterdir()), [])

    def test_stage_pending_rejects_hard_link_before_copying_bytes(self):
        outside = Path(self.tmp.name) / "outside-hardlink.pdf"
        outside.write_bytes(self.valid_pdf_upload("outside.pdf").stream.read())
        ingress = self.storage.pending_dir / "hardlink.pdf"
        os.link(outside, ingress)

        with mock.patch("services.paper_storage.os.fdopen", wraps=os.fdopen) as fdopen:
            with self.assertRaises(StorageError):
                self.storage.stage_pending(ingress.name, "op-hardlink")
        fdopen.assert_not_called()
        self.assertFalse((self.storage.staging_dir / "op-hardlink.pdf").exists())
        self.assertTrue(outside.exists())

    def test_storage_mutations_are_serialized_across_instances(self):
        second = PaperStorage(self.storage.papers_dir, self.storage.pending_dir)
        pdf_bytes = self.valid_pdf_upload("blocking.pdf").stream.read()
        entered = threading.Event()
        release = threading.Event()
        reconciled = threading.Event()
        errors = []

        class BlockingStream(io.BytesIO):
            def read(self, size=-1):
                if not entered.is_set():
                    entered.set()
                    if not release.wait(2):
                        raise RuntimeError("test release timed out")
                return super().read(size)

        def stage_pdf():
            try:
                self.storage.stage(
                    PdfUpload("blocking.pdf", BlockingStream(pdf_bytes)),
                    "op-blocking",
                )
            except Exception as exc:
                errors.append(exc)

        def reconcile():
            try:
                second.reconcile_expired(time.time() + 60, set())
            except Exception as exc:
                errors.append(exc)
            finally:
                reconciled.set()

        stage_thread = threading.Thread(target=stage_pdf)
        reconcile_thread = threading.Thread(target=reconcile)
        stage_thread.start()
        self.assertTrue(entered.wait(1))
        reconcile_thread.start()
        self.assertFalse(reconciled.wait(0.1))
        release.set()
        stage_thread.join(2)
        reconcile_thread.join(2)
        self.assertFalse(stage_thread.is_alive())
        self.assertFalse(reconcile_thread.is_alive())
        self.assertEqual(errors, [])

    def test_promote_uses_only_paper_id_and_revision(self):
        staged = self.storage.stage(
            self.valid_pdf_upload("../../display.pdf"),
            operation_id="op-1",
        )
        stored = self.storage.promote(staged, PAPER_ID, 1)
        self.assertEqual(
            stored.path.relative_to(self.storage.papers_dir),
            Path(f"{PAPER_ID}/1.pdf"),
        )
        self.assertGreater(stored.size_bytes, 100)

    def test_existing_revision_bytes_are_immutable(self):
        first = self.storage.stage(self.valid_pdf_upload("a.pdf", width=72), "op-1")
        original = first.path.read_bytes()
        self.storage.promote(first, PAPER_ID, 1)
        second = self.storage.stage(self.valid_pdf_upload("b.pdf", width=73), "op-2")
        with self.assertRaises(StorageError):
            self.storage.promote(second, PAPER_ID, 1)
        self.assertEqual(self.storage.revision_path(PAPER_ID, 1).read_bytes(), original)
        self.assertTrue(second.path.exists())

    def test_same_revision_bytes_are_an_idempotent_success(self):
        first = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-1")
        stored = self.storage.promote(first, PAPER_ID, 1)
        second = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-2")
        repeated = self.storage.promote(second, PAPER_ID, 1)
        self.assertEqual(repeated, stored)
        self.assertFalse(second.path.exists())

    def test_idempotent_promotion_rejects_multiply_linked_final_revision(self):
        first = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-first-link")
        stored = self.storage.promote(first, PAPER_ID, 1)
        alias = self.storage.papers_dir / "unexpected-hardlink.pdf"
        os.link(stored.path, alias)
        second = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-second-link")

        with self.assertRaises(StorageError):
            self.storage.promote(second, PAPER_ID, 1)

        self.assertTrue(second.path.exists())
        self.assertEqual(stored.path.stat().st_nlink, 2)

    def test_magic_header_without_parseable_pdf_is_rejected(self):
        upload = PdfUpload("broken.pdf", io.BytesIO(b"%PDF-1.4\nnot-a-pdf"))
        with self.assertRaises(StorageError):
            self.storage.stage(upload, "op-broken")

    def test_invalid_input_removes_only_its_new_stage(self):
        retained = self.storage.stage(self.valid_pdf_upload("good.pdf"), "op-good")
        with self.assertRaises(StorageError):
            self.storage.stage(PdfUpload("text.pdf", io.BytesIO(b"not pdf")), "op-bad")
        self.assertTrue(retained.path.exists())
        self.assertFalse((self.storage.papers_dir / ".staging" / "op-bad.pdf").exists())

    def test_unexpected_upload_stream_exception_is_wrapped_and_cleans_stage(self):
        pdf_bytes = self.valid_pdf_upload("exploding.pdf").stream.read()

        class ExplodingStream(io.BytesIO):
            def __init__(self, contents):
                super().__init__(contents)
                self.read_count = 0

            def read(self, size=-1):
                self.read_count += 1
                if self.read_count == 2:
                    raise KeyError("injected upload stream failure")
                return super().read(size)

        upload = PdfUpload("exploding.pdf", ExplodingStream(pdf_bytes))

        with self.assertRaises(StorageError):
            self.storage.stage(upload, "op-exploding")

        self.assertFalse((self.storage.staging_dir / "op-exploding.pdf").exists())

    def test_delete_is_idempotent(self):
        self.storage.delete_paper(PAPER_ID, ["legacy.pdf"])
        self.storage.delete_paper(PAPER_ID, ["legacy.pdf"])

    def test_delete_removes_only_paper_directory_and_explicit_flat_legacy_files(self):
        revision = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-1")
        self.storage.promote(revision, PAPER_ID, 1)
        for name in ("legacy-a.pdf", "legacy-b.pdf", "keep.pdf"):
            (self.storage.papers_dir / name).write_bytes(b"legacy")
        other_dir = self.storage.papers_dir / OTHER_PAPER_ID
        other_dir.mkdir()
        (other_dir / "1.pdf").write_bytes(b"other")

        self.storage.delete_paper(PAPER_ID, ["legacy-a.pdf", "legacy-b.pdf"])

        self.assertFalse((self.storage.papers_dir / PAPER_ID).exists())
        self.assertFalse((self.storage.papers_dir / "legacy-a.pdf").exists())
        self.assertFalse((self.storage.papers_dir / "legacy-b.pdf").exists())
        self.assertTrue((self.storage.papers_dir / "keep.pdf").exists())
        self.assertTrue((other_dir / "1.pdf").exists())

    def test_delete_rejects_invalid_legacy_name_before_deleting_anything(self):
        revision = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-1")
        self.storage.promote(revision, PAPER_ID, 1)
        legacy = self.storage.papers_dir / "legacy.pdf"
        legacy.write_bytes(b"legacy")
        with self.assertRaises(StorageError):
            self.storage.delete_paper(PAPER_ID, ["legacy.pdf", "../outside.pdf"])
        self.assertTrue(self.storage.revision_path(PAPER_ID, 1).exists())
        self.assertTrue(legacy.exists())

    def test_delete_rejects_nested_paper_layout_before_any_removal(self):
        revision = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-nested-delete")
        stored = self.storage.promote(revision, PAPER_ID, 1)
        unexpected = stored.path.parent / "unexpected"
        unexpected.mkdir(mode=0o700)
        (unexpected / "payload").write_bytes(b"keep")

        with self.assertRaises(StorageError):
            self.storage.delete_paper(PAPER_ID, [])

        self.assertTrue(stored.path.exists())
        self.assertEqual((unexpected / "payload").read_bytes(), b"keep")

    def test_delete_rechecks_legacy_inode_before_unlink(self):
        legacy = self.storage.papers_dir / "legacy.pdf"
        legacy.write_bytes(b"original")
        real_stat = os.stat
        calls = 0

        def replace_before_recheck(path, *args, **kwargs):
            nonlocal calls
            if path == legacy.name and kwargs.get("dir_fd") is not None:
                calls += 1
                if calls == 2:
                    legacy.unlink()
                    legacy.write_bytes(b"replacement")
            return real_stat(path, *args, **kwargs)

        with mock.patch("services.paper_storage.os.stat", side_effect=replace_before_recheck):
            with self.assertRaises(StorageError):
                self.storage.delete_paper(PAPER_ID, [legacy.name])
        self.assertEqual(legacy.read_bytes(), b"replacement")

    def test_pdf_metadata_is_written_only_to_staged_bytes(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-1")
        source_hash = staged.source_sha256
        prepared = self.storage.apply_metadata(staged, title="Paper", author="Alice")
        self.assertEqual(prepared.source_sha256, source_hash)
        self.assertEqual(
            prepared.sha256,
            hashlib.sha256(prepared.path.read_bytes()).hexdigest(),
        )
        metadata = PdfReader(prepared.path).metadata
        self.assertEqual(metadata.title, "Paper")
        self.assertEqual(metadata.author, "Alice")

    def test_metadata_backup_removal_is_followed_by_staging_fsync(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-backup-fsync")
        staging_identity = (
            os.fstat(self.storage._staging_fd).st_dev,
            os.fstat(self.storage._staging_fd).st_ino,
        )
        backup_presence_at_fsync = []
        real_fsync = storage_module._fsync_directory_fd

        def record_backup_state(directory_fd):
            info = os.fstat(directory_fd)
            if (info.st_dev, info.st_ino) == staging_identity:
                backup_presence_at_fsync.append(
                    any(".metadata-backup-" in name for name in os.listdir(directory_fd))
                )
            return real_fsync(directory_fd)

        with mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=record_backup_state,
        ):
            self.storage.apply_metadata(staged, title="Paper", author="Alice")

        self.assertEqual(backup_presence_at_fsync, [True, False])

    def test_metadata_writer_failure_preserves_staged_source(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-1")
        before = staged.path.read_bytes()
        with mock.patch(
            "services.paper_storage.PdfWriter.write",
            side_effect=OSError("disk full"),
        ):
            with self.assertRaises(StorageError):
                self.storage.apply_metadata(staged, title="Paper", author="Alice")
        self.assertEqual(staged.path.read_bytes(), before)
        self.assertEqual(list(staged.path.parent.glob("*.metadata-*.tmp")), [])

    def test_metadata_post_replace_validation_failure_restores_source(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-validate")
        before = staged.path.read_bytes()
        with mock.patch(
            "services.paper_storage._strict_pdf",
            side_effect=StorageError("injected final validation failure"),
        ):
            with self.assertRaises(StorageError):
                self.storage.apply_metadata(staged, title="Paper", author="Alice")
        self.assertEqual(staged.path.read_bytes(), before)
        self.assertEqual(list(self.storage.staging_dir.glob("*.metadata-*")), [])

    def test_metadata_directory_fsync_failure_restores_source(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-fsync")
        before = staged.path.read_bytes()
        with mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=[OSError("injected fsync failure"), None],
        ):
            with self.assertRaises(StorageError):
                self.storage.apply_metadata(staged, title="Paper", author="Alice")
        self.assertEqual(staged.path.read_bytes(), before)
        self.assertEqual(list(self.storage.staging_dir.glob("*.metadata-*")), [])

    def test_unexpected_metadata_library_failure_is_wrapped_and_preserves_source(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-library")
        before = staged.path.read_bytes()
        with mock.patch(
            "services.paper_storage.PdfWriter.add_page",
            side_effect=KeyError("injected library failure"),
        ):
            with self.assertRaises(StorageError):
                self.storage.apply_metadata(staged, title="Paper", author="Alice")
        self.assertEqual(staged.path.read_bytes(), before)

    def test_final_path_byte_mismatch_is_rejected_without_clobber(self):
        destination = self.storage.revision_path(PAPER_ID, 1)
        destination.parent.mkdir()
        destination.write_bytes(b"existing bytes")
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-1")
        with self.assertRaises(StorageError):
            self.storage.promote(staged, PAPER_ID, 1)
        self.assertEqual(destination.read_bytes(), b"existing bytes")
        self.assertTrue(staged.path.exists())

    def test_destination_replaced_during_promotion_is_never_overwritten(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-race")
        destination = self.storage.revision_path(PAPER_ID, 1)
        real_publish = storage_module._publish_verified_fd_no_replace
        injected = False

        def publish_after_injection(source_fd, target_fd, destination_name):
            nonlocal injected
            injected = True
            attacker_fd = os.open(
                destination_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=target_fd,
            )
            try:
                os.write(attacker_fd, b"racing bytes")
            finally:
                os.close(attacker_fd)
            return real_publish(source_fd, target_fd, destination_name)

        with mock.patch(
            "services.paper_storage._publish_verified_fd_no_replace",
            side_effect=publish_after_injection,
        ):
            with self.assertRaises(StorageError):
                self.storage.promote(staged, PAPER_ID, 1)
        self.assertTrue(injected)
        self.assertEqual(destination.read_bytes(), b"racing bytes")
        self.assertTrue(staged.path.exists())

    def test_promotion_uses_verified_fd_and_never_unlinks_swapped_stage(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-stage-swap")
        original = staged.path.read_bytes()
        real_publish = getattr(storage_module, "_publish_verified_fd_no_replace", None)
        injected = False

        def publish_after_stage_swap(source_fd, target_fd, destination_name):
            nonlocal injected
            injected = True
            staged.path.unlink()
            staged.path.write_bytes(b"racing stage")
            staged.path.chmod(0o600)
            return real_publish(source_fd, target_fd, destination_name)

        with mock.patch(
            "services.paper_storage._publish_verified_fd_no_replace",
            create=True,
            side_effect=publish_after_stage_swap,
        ):
            stored = self.storage.promote(staged, PAPER_ID, 1)
        self.assertTrue(injected)
        self.assertEqual(stored.path.read_bytes(), original)
        self.assertEqual(staged.path.read_bytes(), b"racing stage")

    def test_linux_style_hardlink_publication_finishes_with_single_link(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-linux-link")

        def publish_as_hardlink(_source_fd, target_fd, destination_name):
            os.link(
                staged.path.name,
                destination_name,
                src_dir_fd=self.storage._staging_fd,
                dst_dir_fd=target_fd,
                follow_symlinks=False,
            )

        with mock.patch(
            "services.paper_storage._publish_verified_fd_no_replace",
            side_effect=publish_as_hardlink,
        ):
            stored = self.storage.promote(staged, PAPER_ID, 1)

        self.assertFalse(staged.path.exists())
        self.assertEqual(stored.path.stat().st_nlink, 1)

    def test_promotion_reopens_final_bytes_before_success(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-final-swap")
        real_publish = getattr(storage_module, "_publish_verified_fd_no_replace", None)
        injected = False

        def publish_then_replace(source_fd, target_fd, destination_name):
            nonlocal injected
            real_publish(source_fd, target_fd, destination_name)
            os.unlink(destination_name, dir_fd=target_fd)
            replacement = os.open(
                destination_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=target_fd,
            )
            try:
                os.write(replacement, b"racing final")
            finally:
                os.close(replacement)
            injected = True

        with mock.patch(
            "services.paper_storage._publish_verified_fd_no_replace",
            create=True,
            side_effect=publish_then_replace,
        ):
            with self.assertRaises(StorageError):
                self.storage.promote(staged, PAPER_ID, 1)
        self.assertTrue(injected)
        self.assertEqual(self.storage.revision_path(PAPER_ID, 1).read_bytes(), b"racing final")
        self.assertTrue(staged.path.exists())

    def test_promotion_fsyncs_new_uuid_parent_and_revision_directory(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-fsync-order")
        papers_identity = (
            os.fstat(self.storage._papers_fd).st_dev,
            os.fstat(self.storage._papers_fd).st_ino,
        )
        synced = []
        real_fsync = storage_module._fsync_directory_fd

        def record_fsync(directory_fd):
            info = os.fstat(directory_fd)
            synced.append((info.st_dev, info.st_ino))
            return real_fsync(directory_fd)

        with mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=record_fsync,
        ):
            stored = self.storage.promote(staged, PAPER_ID, 1)
        paper_info = stored.path.parent.stat()
        paper_identity = (paper_info.st_dev, paper_info.st_ino)
        self.assertIn(papers_identity, synced)
        self.assertIn(paper_identity, synced)
        self.assertLess(synced.index(papers_identity), synced.index(paper_identity))

    def test_promotion_fsyncs_verified_final_file_before_paper_directory(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-final-fsync")
        events = []
        real_os_fsync = os.fsync
        real_directory_fsync = storage_module._fsync_directory_fd

        def record_file_fsync(file_fd):
            info = os.fstat(file_fd)
            if stat.S_ISREG(info.st_mode):
                events.append(("file", info.st_dev, info.st_ino))
            return real_os_fsync(file_fd)

        def record_directory_fsync(directory_fd):
            info = os.fstat(directory_fd)
            events.append(("directory", info.st_dev, info.st_ino))
            return real_directory_fsync(directory_fd)

        with mock.patch("services.paper_storage.os.fsync", side_effect=record_file_fsync), mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=record_directory_fsync,
        ):
            stored = self.storage.promote(staged, PAPER_ID, 1)

        final_info = stored.path.stat()
        paper_info = stored.path.parent.stat()
        final_event = ("file", final_info.st_dev, final_info.st_ino)
        paper_event = ("directory", paper_info.st_dev, paper_info.st_ino)
        self.assertIn(final_event, events)
        self.assertIn(paper_event, events)
        self.assertLess(events.index(final_event), events.index(paper_event))

    def test_post_commit_staging_fsync_failure_does_not_fail_promotion(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-cleanup-fsync")
        expected = staged.path.read_bytes()
        staging_identity = (
            os.fstat(self.storage._staging_fd).st_dev,
            os.fstat(self.storage._staging_fd).st_ino,
        )
        real_fsync = storage_module._fsync_directory_fd
        injected = False

        def fail_staging_fsync(directory_fd):
            nonlocal injected
            info = os.fstat(directory_fd)
            if (info.st_dev, info.st_ino) == staging_identity and not injected:
                injected = True
                raise OSError("injected post-commit staging fsync failure")
            return real_fsync(directory_fd)

        with mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=fail_staging_fsync,
        ):
            stored = self.storage.promote(staged, PAPER_ID, 1)

        self.assertTrue(injected)
        self.assertEqual(stored.path.read_bytes(), expected)
        self.assertFalse(staged.path.exists())

    def test_post_commit_stage_unlink_failure_is_reconciled_without_losing_final(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-cleanup-unlink")
        real_unlink_matching = self.storage._unlink_if_matching

        def publish_as_hardlink(_source_fd, target_fd, destination_name):
            os.link(
                staged.path.name,
                destination_name,
                src_dir_fd=self.storage._staging_fd,
                dst_dir_fd=target_fd,
                follow_symlinks=False,
            )

        def fail_stage_cleanup(directory_fd, name, expected):
            if directory_fd == self.storage._staging_fd and name == staged.path.name:
                raise OSError("injected stage unlink failure")
            return real_unlink_matching(directory_fd, name, expected)

        with mock.patch(
            "services.paper_storage._publish_verified_fd_no_replace",
            side_effect=publish_as_hardlink,
        ), mock.patch.object(
            self.storage,
            "_unlink_if_matching",
            side_effect=fail_stage_cleanup,
        ):
            stored = self.storage.promote(staged, PAPER_ID, 1)

        self.assertTrue(staged.path.exists())
        self.assertEqual(stored.path.stat().st_nlink, 2)
        removed = self.storage.reconcile_expired(time.time() + 60, {(PAPER_ID, 1)})
        self.assertEqual(removed, 1)
        self.assertFalse(staged.path.exists())
        self.assertEqual(stored.path.stat().st_nlink, 1)

    def test_uuid_revision_and_operation_traversal_are_rejected(self):
        for paper_id in ("../escape", "/absolute", "not-a-uuid"):
            with self.subTest(paper_id=paper_id):
                with self.assertRaises(StorageError):
                    self.storage.revision_path(paper_id, 1)
        for revision in (0, -1, "../1", True):
            with self.subTest(revision=revision):
                with self.assertRaises(StorageError):
                    self.storage.revision_path(PAPER_ID, revision)
        with self.assertRaises(StorageError):
            self.storage.stage(self.valid_pdf_upload("a.pdf"), "../escape")

    def test_later_revision_never_changes_earlier_revision(self):
        first = self.storage.stage(self.valid_pdf_upload("a.pdf", width=72), "op-1")
        first_stored = self.storage.promote(first, PAPER_ID, 1)
        first_bytes = first_stored.path.read_bytes()
        second = self.storage.stage(self.valid_pdf_upload("b.pdf", width=73), "op-2")
        self.storage.promote(second, PAPER_ID, 2)
        self.assertEqual(first_stored.path.read_bytes(), first_bytes)

    def test_stage_pending_copies_valid_regular_file(self):
        source = self.write_pending_pdf()
        staged = self.storage.stage_pending(source.name, "op-pending")
        self.assertTrue(source.exists())
        self.assertEqual(staged.source_sha256, hashlib.sha256(source.read_bytes()).hexdigest())

    def test_stage_pending_rejects_unsafe_names_before_reading_outside(self):
        outside = Path(self.tmp.name) / "outside.pdf"
        outside.write_bytes(self.valid_pdf_upload("outside.pdf").stream.read())
        symlink = self.storage.pending_dir / "escape.pdf"
        symlink.symlink_to(outside)
        for filename in ("../outside.pdf", str(outside), symlink.name):
            with self.subTest(filename=filename):
                with mock.patch(
                    "services.paper_storage.os.open",
                    wraps=os.open,
                ) as descriptor_open:
                    with self.assertRaises(StorageError):
                        self.storage.stage_pending(filename, "op-pending")
                opened_names = [call.args[0] for call in descriptor_open.call_args_list]
                self.assertNotIn(filename, opened_names)
                self.assertNotIn(str(outside), opened_names)
        self.assertTrue(outside.exists())

    def test_stage_pending_does_not_follow_source_swapped_to_symlink(self):
        source = self.write_pending_pdf()
        outside = Path(self.tmp.name) / "outside.pdf"
        outside.write_bytes(self.valid_pdf_upload("outside.pdf", width=99).stream.read())
        real_open = os.open
        swapped = False

        def swap_then_open(path, flags, *args, **kwargs):
            nonlocal swapped
            if path == source.name and kwargs.get("dir_fd") is not None and not swapped:
                swapped = True
                source.unlink()
                source.symlink_to(outside)
            return real_open(path, flags, *args, **kwargs)

        with mock.patch("services.paper_storage.os.open", side_effect=swap_then_open):
            with self.assertRaises(StorageError):
                self.storage.stage_pending(source.name, "op-race")
        self.assertTrue(outside.exists())
        self.assertFalse((self.storage.papers_dir / ".staging" / "op-race.pdf").exists())

    def test_trash_pending_rejects_unsafe_names_before_moving_outside(self):
        outside = Path(self.tmp.name) / "outside.pdf"
        outside.write_bytes(self.valid_pdf_upload("outside.pdf").stream.read())
        symlink = self.storage.pending_dir / "escape.pdf"
        symlink.symlink_to(outside)
        for index, filename in enumerate(("../outside.pdf", str(outside), symlink.name)):
            with self.subTest(filename=filename):
                with self.assertRaises(StorageError):
                    self.storage.trash_pending(filename, f"op-trash-{index}")
        self.assertTrue(outside.exists())
        self.assertEqual(outside.read_bytes()[:5], b"%PDF-")

    def test_trash_token_is_bound_one_use_and_restores_exact_pending_file(self):
        source = self.write_pending_pdf()
        token = self.storage.trash_pending(source.name, "op-trash")
        trashed = self.storage.trash_dir / "op-trash.pdf"
        self.assertEqual(token.original_name, source.name)
        self.assertEqual(token.operation_id, "op-trash")
        with self.assertRaises(FrozenInstanceError):
            token.operation_id = "changed"
        forged = replace(token, operation_id="other")
        with self.assertRaises(StorageError):
            self.storage.restore_pending(forged)
        self.storage.restore_pending(token)
        self.assertTrue(source.exists())
        self.assertFalse(trashed.exists())
        with self.assertRaises(StorageError):
            self.storage.restore_pending(token)

    def test_trash_pending_race_preserves_incumbent_and_source_bytes(self):
        source = self.write_pending_pdf()
        source_bytes = source.read_bytes()
        destination = self.storage.trash_dir / "op-trash-race.pdf"
        real_link = os.link

        def inject_incumbent(source_name, destination_name, *args, **kwargs):
            if destination_name == destination.name:
                incumbent_fd = os.open(
                    destination_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=kwargs["dst_dir_fd"],
                )
                try:
                    os.write(incumbent_fd, b"incumbent trash")
                finally:
                    os.close(incumbent_fd)
            return real_link(source_name, destination_name, *args, **kwargs)

        with mock.patch("services.paper_storage.os.link", side_effect=inject_incumbent):
            with self.assertRaises(StorageError):
                self.storage.trash_pending(source.name, "op-trash-race")
        self.assertEqual(source.read_bytes(), source_bytes)
        self.assertEqual(destination.read_bytes(), b"incumbent trash")

    def test_trash_destination_stat_failure_before_unlink_rolls_back_link(self):
        source = self.write_pending_pdf("stat-before.pdf")
        destination = self.storage.trash_dir / "op-stat-before.pdf"
        real_stat = os.stat
        injected = False

        def fail_first_destination_stat(path, *args, **kwargs):
            nonlocal injected
            if path == destination.name and kwargs.get("dir_fd") == self.storage._trash_fd and not injected:
                injected = True
                raise OSError("injected destination stat failure")
            return real_stat(path, *args, **kwargs)

        with mock.patch("services.paper_storage.os.stat", side_effect=fail_first_destination_stat):
            with self.assertRaises(StorageError):
                self.storage.trash_pending(source.name, "op-stat-before")

        self.assertTrue(injected)
        self.assertTrue(source.exists())
        self.assertEqual(source.stat().st_nlink, 1)
        self.assertFalse(destination.exists())

    def test_trash_destination_fsync_failure_before_unlink_rolls_back_link(self):
        source = self.write_pending_pdf("fsync-before.pdf")
        destination = self.storage.trash_dir / "op-fsync-before.pdf"
        trash_identity = (
            os.fstat(self.storage._trash_fd).st_dev,
            os.fstat(self.storage._trash_fd).st_ino,
        )
        real_fsync = storage_module._fsync_directory_fd
        injected = False

        def fail_first_trash_fsync(directory_fd):
            nonlocal injected
            info = os.fstat(directory_fd)
            if (info.st_dev, info.st_ino) == trash_identity and not injected:
                injected = True
                raise OSError("injected destination fsync failure")
            return real_fsync(directory_fd)

        with mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=fail_first_trash_fsync,
        ):
            with self.assertRaises(StorageError):
                self.storage.trash_pending(source.name, "op-fsync-before")

        self.assertTrue(injected)
        self.assertTrue(source.exists())
        self.assertEqual(source.stat().st_nlink, 1)
        self.assertFalse(destination.exists())

    def test_trash_source_fsync_failure_after_unlink_returns_restorable_token(self):
        source = self.write_pending_pdf("fsync-after.pdf")
        destination = self.storage.trash_dir / "op-fsync-after.pdf"
        pending_identity = (
            os.fstat(self.storage._pending_fd).st_dev,
            os.fstat(self.storage._pending_fd).st_ino,
        )
        real_fsync = storage_module._fsync_directory_fd
        injected = False

        def fail_first_pending_fsync(directory_fd):
            nonlocal injected
            info = os.fstat(directory_fd)
            if (info.st_dev, info.st_ino) == pending_identity and not injected:
                injected = True
                raise OSError("injected source fsync failure")
            return real_fsync(directory_fd)

        with mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=fail_first_pending_fsync,
        ):
            token = self.storage.trash_pending(source.name, "op-fsync-after")

        self.assertTrue(injected)
        self.assertFalse(source.exists())
        self.assertEqual(destination.stat().st_nlink, 1)
        self.storage.restore_pending(token)
        self.assertTrue(source.exists())

    def test_trash_final_stat_failure_after_unlink_returns_restorable_token(self):
        source = self.write_pending_pdf("stat-after.pdf")
        destination = self.storage.trash_dir / "op-stat-after.pdf"
        real_stat = os.stat
        destination_stats = 0

        def fail_final_destination_stat(path, *args, **kwargs):
            nonlocal destination_stats
            if path == destination.name and kwargs.get("dir_fd") == self.storage._trash_fd:
                destination_stats += 1
                if destination_stats == 2:
                    raise OSError("injected final stat failure")
            return real_stat(path, *args, **kwargs)

        with mock.patch("services.paper_storage.os.stat", side_effect=fail_final_destination_stat):
            token = self.storage.trash_pending(source.name, "op-stat-after")

        self.assertEqual(destination_stats, 2)
        self.assertFalse(source.exists())
        self.assertEqual(destination.stat().st_nlink, 1)
        self.storage.restore_pending(token)
        self.assertTrue(source.exists())

    def test_restore_pending_race_preserves_incumbent_and_trash_bytes(self):
        source = self.write_pending_pdf()
        source_bytes = source.read_bytes()
        token = self.storage.trash_pending(source.name, "op-restore-race")
        trashed = self.storage.trash_dir / "op-restore-race.pdf"
        real_link = os.link

        def inject_incumbent(source_name, destination_name, *args, **kwargs):
            if destination_name == source.name:
                incumbent_fd = os.open(
                    destination_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=kwargs["dst_dir_fd"],
                )
                try:
                    os.write(incumbent_fd, b"incumbent pending")
                finally:
                    os.close(incumbent_fd)
            return real_link(source_name, destination_name, *args, **kwargs)

        with mock.patch("services.paper_storage.os.link", side_effect=inject_incumbent):
            with self.assertRaises(StorageError):
                self.storage.restore_pending(token)
        self.assertEqual(source.read_bytes(), b"incumbent pending")
        self.assertEqual(trashed.read_bytes(), source_bytes)
        source.unlink()
        self.storage.restore_pending(token)
        self.assertEqual(source.read_bytes(), source_bytes)

    def test_restore_destination_stat_failure_before_unlink_is_retryable(self):
        source = self.write_pending_pdf("restore-stat-before.pdf")
        token = self.storage.trash_pending(source.name, "op-restore-stat-before")
        trashed = self.storage.trash_dir / "op-restore-stat-before.pdf"
        pending_identity = (
            os.fstat(self.storage._pending_fd).st_dev,
            os.fstat(self.storage._pending_fd).st_ino,
        )
        real_stat = os.stat
        injected = False

        def fail_first_destination_stat(path, *args, **kwargs):
            nonlocal injected
            directory_fd = kwargs.get("dir_fd")
            if directory_fd is not None:
                info = os.fstat(directory_fd)
                if (
                    path == source.name
                    and (info.st_dev, info.st_ino) == pending_identity
                    and not injected
                ):
                    injected = True
                    raise OSError("injected restore destination stat failure")
            return real_stat(path, *args, **kwargs)

        with mock.patch("services.paper_storage.os.stat", side_effect=fail_first_destination_stat):
            with self.assertRaises(StorageError):
                self.storage.restore_pending(token)

        self.assertTrue(injected)
        self.assertFalse(source.exists())
        self.assertEqual(trashed.stat().st_nlink, 1)
        self.storage.restore_pending(token)
        self.assertTrue(source.exists())

    def test_restore_destination_fsync_failure_before_unlink_is_retryable(self):
        source = self.write_pending_pdf("restore-fsync-before.pdf")
        token = self.storage.trash_pending(source.name, "op-restore-fsync-before")
        trashed = self.storage.trash_dir / "op-restore-fsync-before.pdf"
        pending_identity = (
            os.fstat(self.storage._pending_fd).st_dev,
            os.fstat(self.storage._pending_fd).st_ino,
        )
        real_fsync = storage_module._fsync_directory_fd
        injected = False

        def fail_first_pending_fsync(directory_fd):
            nonlocal injected
            info = os.fstat(directory_fd)
            if (info.st_dev, info.st_ino) == pending_identity and not injected:
                injected = True
                raise OSError("injected restore destination fsync failure")
            return real_fsync(directory_fd)

        with mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=fail_first_pending_fsync,
        ):
            with self.assertRaises(StorageError):
                self.storage.restore_pending(token)

        self.assertTrue(injected)
        self.assertFalse(source.exists())
        self.assertEqual(trashed.stat().st_nlink, 1)
        self.storage.restore_pending(token)
        self.assertTrue(source.exists())

    def test_restore_source_fsync_failure_after_unlink_consumes_token(self):
        source = self.write_pending_pdf("restore-fsync-after.pdf")
        token = self.storage.trash_pending(source.name, "op-restore-fsync-after")
        trashed = self.storage.trash_dir / "op-restore-fsync-after.pdf"
        trash_identity = (
            os.fstat(self.storage._trash_fd).st_dev,
            os.fstat(self.storage._trash_fd).st_ino,
        )
        real_fsync = storage_module._fsync_directory_fd
        injected = False

        def fail_first_trash_fsync(directory_fd):
            nonlocal injected
            info = os.fstat(directory_fd)
            if (info.st_dev, info.st_ino) == trash_identity and not injected:
                injected = True
                raise OSError("injected restore source fsync failure")
            return real_fsync(directory_fd)

        with mock.patch(
            "services.paper_storage._fsync_directory_fd",
            side_effect=fail_first_trash_fsync,
        ):
            self.storage.restore_pending(token)

        self.assertTrue(injected)
        self.assertTrue(source.exists())
        self.assertFalse(trashed.exists())
        with self.assertRaises(StorageError):
            self.storage.restore_pending(token)

    def test_restore_final_stat_failure_after_unlink_consumes_token(self):
        source = self.write_pending_pdf("restore-stat-after.pdf")
        token = self.storage.trash_pending(source.name, "op-restore-stat-after")
        trashed = self.storage.trash_dir / "op-restore-stat-after.pdf"
        pending_identity = (
            os.fstat(self.storage._pending_fd).st_dev,
            os.fstat(self.storage._pending_fd).st_ino,
        )
        real_stat = os.stat
        destination_stats = 0

        def fail_final_destination_stat(path, *args, **kwargs):
            nonlocal destination_stats
            directory_fd = kwargs.get("dir_fd")
            if directory_fd is not None:
                info = os.fstat(directory_fd)
                if path == source.name and (info.st_dev, info.st_ino) == pending_identity:
                    destination_stats += 1
                    if destination_stats == 2:
                        raise OSError("injected restore final stat failure")
            return real_stat(path, *args, **kwargs)

        with mock.patch("services.paper_storage.os.stat", side_effect=fail_final_destination_stat):
            self.storage.restore_pending(token)

        self.assertEqual(destination_stats, 2)
        self.assertTrue(source.exists())
        self.assertFalse(trashed.exists())
        with self.assertRaises(StorageError):
            self.storage.restore_pending(token)

    def test_copy_revision_rewrites_metadata_into_new_immutable_revision(self):
        first = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-1")
        self.storage.promote(first, PAPER_ID, 1)
        copied = self.storage.copy_revision(
            PAPER_ID,
            source_revision=1,
            target_revision=2,
            operation_id="op-copy",
            title="Restored Paper",
            author="Alice",
        )
        self.assertEqual(copied.path, self.storage.open_revision(PAPER_ID, 2))
        metadata = PdfReader(copied.path).metadata
        self.assertEqual(metadata.title, "Restored Paper")
        self.assertEqual(metadata.author, "Alice")
        with self.assertRaises(StorageError):
            self.storage.open_revision(PAPER_ID, 3)

    def test_reconcile_expired_removes_only_stale_unreferenced_storage(self):
        referenced = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-ref")
        self.storage.promote(referenced, PAPER_ID, 1)
        unreferenced = self.storage.stage(self.valid_pdf_upload("b.pdf"), "op-unref")
        self.storage.promote(unreferenced, PAPER_ID, 2)
        old_stage = self.storage.stage(self.valid_pdf_upload("c.pdf"), "op-old")
        new_stage = self.storage.stage(self.valid_pdf_upload("d.pdf"), "op-new")
        pending = self.write_pending_pdf("trash.pdf")
        self.storage.trash_pending(pending.name, "op-trash")
        old_trash = self.storage.trash_dir / "op-trash.pdf"
        old = time.time() - 120
        os.utime(old_stage.path, (old, old))
        os.utime(old_trash, (old, old))
        cutoff = time.time() - 60

        removed = self.storage.reconcile_expired(cutoff, {(PAPER_ID, 1)})

        self.assertEqual(removed, 3)
        self.assertFalse(old_stage.path.exists())
        self.assertTrue(new_stage.path.exists())
        self.assertFalse(old_trash.exists())
        self.assertTrue(self.storage.revision_path(PAPER_ID, 1).exists())
        self.assertFalse(self.storage.revision_path(PAPER_ID, 2).exists())

    def test_reconcile_rejects_non_finite_cutoff_before_mutation(self):
        for index, cutoff in enumerate((math.nan, math.inf, -math.inf)):
            with self.subTest(cutoff=cutoff):
                staged = self.storage.stage(
                    self.valid_pdf_upload(f"finite-{index}.pdf"),
                    f"op-finite-{index}",
                )
                with self.assertRaises(StorageError):
                    self.storage.reconcile_expired(cutoff, set())
                self.assertTrue(staged.path.exists())

    def test_reconcile_recognizes_only_its_internal_metadata_backup_link(self):
        staged = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-backup")
        backup = self.storage.staging_dir / (
            "op-backup.metadata-backup-11111111111111111111111111111111.tmp"
        )
        os.link(staged.path, backup)
        old = time.time() - 120
        os.utime(staged.path, (old, old))

        removed = self.storage.reconcile_expired(time.time() - 60, set())

        self.assertEqual(removed, 2)
        self.assertFalse(staged.path.exists())
        self.assertFalse(backup.exists())

    def test_reconcile_rejects_nested_paper_layout_before_removing_revisions(self):
        revision = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-nested-reconcile")
        stored = self.storage.promote(revision, PAPER_ID, 1)
        unexpected = stored.path.parent / "unexpected"
        unexpected.mkdir(mode=0o700)

        with self.assertRaises(StorageError):
            self.storage.reconcile_expired(time.time() + 60, set())

        self.assertTrue(stored.path.exists())
        self.assertTrue(unexpected.exists())

    def test_reconcile_never_treats_unicode_digits_as_revision_names(self):
        revision = self.storage.stage(self.valid_pdf_upload("a.pdf"), "op-ascii")
        stored = self.storage.promote(revision, PAPER_ID, 1)
        unicode_revision = stored.path.parent / "١.pdf"
        unicode_revision.write_bytes(b"not a revision")
        unicode_revision.chmod(0o600)

        self.storage.reconcile_expired(time.time() + 60, set())

        self.assertFalse(stored.path.exists())
        self.assertTrue(unicode_revision.exists())


if __name__ == "__main__":
    unittest.main()
