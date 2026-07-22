#!/usr/bin/env python3
"""
Merge messages.pot into each locale's messages.po (Babel's ``pybabel update``).

Usage:
    python tools/update_translations.py   # after tools/extract_translations.py

This is the step that means you never re-link translations by hand. ``msgmerge``
keys on the source text (the ``msgid``), so:

  * unchanged strings keep their existing translation across any code move,
  * brand-new strings are added with an empty ``msgstr``,
  * strings whose source text changed are marked ``#, fuzzy`` with the old
    translation kept as a starting point (via fuzzy matching),
  * strings that vanished from the source become obsolete ``#~`` comments
    rather than being deleted (so a returning string recovers its translation).

After running this, only the new + fuzzy entries need human (or LLM) attention.
Recompile with ``tools/compile_translations.py`` and restart the dev server.
"""

from __future__ import annotations

from pathlib import Path

try:
    from babel.messages.frontend import CommandLineInterface
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "Babel is required. Please run 'pip install --require-hashes -r requirements.lock' before updating."
    ) from exc

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    pot_path = ROOT / "messages.pot"
    if not pot_path.exists():
        raise SystemExit(
            "messages.pot not found. Run 'python tools/extract_translations.py' first."
        )

    argv = [
        "pybabel",
        "update",
        "-i",
        str(pot_path),
        "-d",
        str(ROOT / "translations"),
        "-D",
        "messages",
        # Record the previous msgid for fuzzy entries so reviewers see what changed.
        "--previous",
    ]
    CommandLineInterface().run(argv)


if __name__ == "__main__":
    main()
