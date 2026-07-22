"""Resource-constrained child entry point for untrusted attachment parsing."""

from __future__ import annotations

import os
import sys


def _apply_resource_limits() -> None:
    if os.name != "posix":  # pragma: no cover - production is Linux
        return
    import resource

    limits = (
        (resource.RLIMIT_AS, 1024 * 1024 * 1024),
        (resource.RLIMIT_CPU, 30),
        (resource.RLIMIT_FSIZE, 4 * 1024 * 1024),
        (resource.RLIMIT_NOFILE, 64),
        (resource.RLIMIT_CORE, 0),
    )
    for kind, maximum in limits:
        try:
            resource.setrlimit(kind, (maximum, maximum))
        except (OSError, ValueError):
            pass


def main(argv: list[str] | None = None) -> int:
    from services.attachment_processing import (
        MAX_ATTACH_BYTES,
        MAX_EXTRACTED_TEXT_BYTES,
    )

    arguments = list(argv if argv is not None else sys.argv[1:])
    if len(arguments) != 1:
        return 2
    _apply_resource_limits()
    raw = sys.stdin.buffer.read(MAX_ATTACH_BYTES + 1)
    if len(raw) > MAX_ATTACH_BYTES:
        return 2

    from services.papers import extract_text_from_upload

    try:
        text = extract_text_from_upload(arguments[0], raw)
        encoded = text.encode("utf-8")
    except Exception:
        return 1
    if len(encoded) > MAX_EXTRACTED_TEXT_BYTES:
        return 1
    sys.stdout.buffer.write(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
