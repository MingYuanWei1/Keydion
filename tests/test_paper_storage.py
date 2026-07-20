import hashlib
import io
import os
import stat
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from PyPDF2 import PdfReader, PdfWriter

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
        self.assertEqual(list(outside.iterdir()), [])

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
        real_link = os.link
        injected = False

        def link_after_injection(source, target, *args, **kwargs):
            nonlocal injected
            if target == "1.pdf" and kwargs.get("dst_dir_fd") is not None:
                injected = True
                try:
                    os.unlink(target, dir_fd=kwargs["dst_dir_fd"])
                except FileNotFoundError:
                    pass
                attacker_fd = os.open(
                    target,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=kwargs["dst_dir_fd"],
                )
                try:
                    os.write(attacker_fd, b"racing bytes")
                finally:
                    os.close(attacker_fd)
            return real_link(source, target, *args, **kwargs)

        with mock.patch("services.paper_storage.os.link", side_effect=link_after_injection):
            with self.assertRaises(StorageError):
                self.storage.promote(staged, PAPER_ID, 1)
        self.assertTrue(injected)
        self.assertEqual(destination.read_bytes(), b"racing bytes")
        self.assertTrue(staged.path.exists())

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
                with mock.patch("builtins.open", side_effect=AssertionError("outside read")):
                    with self.assertRaises(StorageError):
                        self.storage.stage_pending(filename, "op-pending")
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

    def test_trash_and_restore_pending_are_contained_and_no_clobber(self):
        source = self.write_pending_pdf()
        original, trashed = self.storage.trash_pending(source.name, "op-trash")
        self.assertEqual(original, source.resolve())
        self.assertFalse(original.exists())
        self.assertTrue(trashed.exists())
        self.storage.restore_pending(original, trashed)
        self.assertTrue(original.exists())
        self.assertFalse(trashed.exists())

        original.write_bytes(b"occupied")
        trashed.write_bytes(b"trashed")
        with self.assertRaises(StorageError):
            self.storage.restore_pending(original, trashed)
        self.assertEqual(original.read_bytes(), b"occupied")
        self.assertEqual(trashed.read_bytes(), b"trashed")

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
        _original, old_trash = self.storage.trash_pending(pending.name, "op-trash")
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


if __name__ == "__main__":
    unittest.main()
