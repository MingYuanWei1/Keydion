#!/usr/bin/env python3
"""
Extract translatable strings into messages.pot.

Usage:
    python tools/extract_translations.py

This wraps Babel's ``pybabel extract`` with an explicit ``--ignore-dirs`` set.
Babel's *default* directory filter skips every directory whose name starts with
``.`` or ``_`` (see ``babel.messages.extract.default_directory_filter``). That
silently drops nested template packages such as ``templates/_dashboard/`` — the
strings render at runtime but never reach ``messages.pot``, so ``pybabel update``
would mark their translations obsolete and they'd be lost.

We therefore replace the default filter with one that skips the dot-dirs and the
virtualenv/build dirs by name, but leaves ``_``-prefixed source dirs alone. The
file -> extraction-method mapping itself lives in ``babel.cfg``.
"""

from __future__ import annotations

from pathlib import Path

try:
    from babel.messages.frontend import CommandLineInterface
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "Babel is required. Please run 'pip install -r requirements.txt' before extracting."
    ) from exc

ROOT = Path(__file__).resolve().parents[1]

# fnmatch patterns matched against each *directory basename* during the walk.
# Crucially this does NOT include "_*", so templates/_dashboard/ is kept.
IGNORE_DIRS = ".* venv venv_new env __pycache__ node_modules"


def main() -> None:
    argv = [
        "pybabel",
        "extract",
        "-F",
        str(ROOT / "babel.cfg"),
        "-k",
        "_",
        "-k",
        "_l",
        "--ignore-dirs",
        IGNORE_DIRS,
        "-o",
        str(ROOT / "messages.pot"),
        str(ROOT),
    ]
    CommandLineInterface().run(argv)


if __name__ == "__main__":
    main()
