"""Bounded validation and subprocess extraction for untrusted attachments."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import zipfile
from io import BytesIO
from pathlib import PurePosixPath


MAX_ATTACH_BYTES = 5 * 1024 * 1024
MAX_DOCX_MEMBERS = 256
MAX_DOCX_TOTAL_BYTES = 25 * 1024 * 1024
MAX_DOCX_PART_BYTES = 10 * 1024 * 1024
MAX_DOCX_COMPRESSION_RATIO = 100
MAX_EXTRACTED_TEXT_BYTES = 2 * 1024 * 1024
EXTRACTION_TIMEOUT_SECONDS = 60


class AttachmentProcessingError(ValueError):
    pass


def preflight_docx(raw: bytes) -> None:
    """Reject malformed, encrypted, traversal, XML-expanding, or bomb-like DOCX."""
    if len(raw) > MAX_ATTACH_BYTES:
        raise AttachmentProcessingError("attachment exceeds the upload limit")
    try:
        with zipfile.ZipFile(BytesIO(raw)) as archive:
            members = archive.infolist()
            if not members or len(members) > MAX_DOCX_MEMBERS:
                raise AttachmentProcessingError("invalid DOCX member count")
            names = {member.filename for member in members}
            if not {"[Content_Types].xml", "word/document.xml"}.issubset(names):
                raise AttachmentProcessingError("invalid DOCX structure")

            total = 0
            for member in members:
                name = member.filename
                path = PurePosixPath(name)
                if (
                    not name
                    or "\x00" in name
                    or "\\" in name
                    or name.startswith("/")
                    or ".." in path.parts
                ):
                    raise AttachmentProcessingError("unsafe DOCX member name")
                if member.flag_bits & 0x1:
                    raise AttachmentProcessingError("encrypted DOCX is not supported")
                if member.file_size > MAX_DOCX_PART_BYTES:
                    raise AttachmentProcessingError("DOCX part is too large")
                total += member.file_size
                if total > MAX_DOCX_TOTAL_BYTES:
                    raise AttachmentProcessingError("DOCX expands beyond the limit")
                if member.file_size:
                    if member.compress_size <= 0:
                        raise AttachmentProcessingError("invalid DOCX compression data")
                    if member.file_size / member.compress_size > MAX_DOCX_COMPRESSION_RATIO:
                        raise AttachmentProcessingError("DOCX compression ratio is unsafe")

            # OOXML should not contain DTD/entity declarations.  Reading XML
            # only after the central-directory caps bounds this validation.
            for member in members:
                if member.filename.lower().endswith(".xml") and member.file_size:
                    xml = archive.read(member)
                    upper = xml.upper()
                    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
                        raise AttachmentProcessingError("unsafe XML declaration in DOCX")
    except (zipfile.BadZipFile, OSError, RuntimeError) as exc:
        raise AttachmentProcessingError("malformed DOCX archive") from exc


def extract_in_subprocess(filename: str, raw: bytes) -> str:
    """Extract in a separate process with wall-clock and output-size bounds."""
    if len(raw) > MAX_ATTACH_BYTES:
        raise AttachmentProcessingError("attachment exceeds the upload limit")
    command = [sys.executable, "-m", "tools.extract_attachment", filename]
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        output, _error = process.communicate(
            input=raw,
            timeout=EXTRACTION_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:  # pragma: no cover - production is Linux
            process.kill()
        process.communicate()
        raise AttachmentProcessingError("attachment extraction timed out") from exc
    if process.returncode != 0:
        raise AttachmentProcessingError("attachment extraction failed")
    if len(output) > MAX_EXTRACTED_TEXT_BYTES:
        raise AttachmentProcessingError("extracted attachment text is too large")
    try:
        return output.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise AttachmentProcessingError("attachment extraction returned invalid text") from exc
