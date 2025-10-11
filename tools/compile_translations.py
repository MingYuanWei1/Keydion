#!/usr/bin/env python3
"""
Compile translation catalogs (*.po -> *.mo) using Babel utilities.

Usage:
    python tools/compile_translations.py
"""

from __future__ import annotations

from pathlib import Path

try:
    from babel.messages import mofile, pofile
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "Babel is required. Please run 'pip install -r requirements.txt' before compiling translations."
    ) from exc


def compile_catalog(po_path: Path) -> None:
    catalog = pofile.read_po(po_path.open("r", encoding="utf-8"))
    mo_path = po_path.with_suffix(".mo")
    with mo_path.open("wb") as mo_file:
        mofile.write_mo(mo_file, catalog)
    print(f"Compiled {po_path} -> {mo_path}")


def main() -> None:
    base_dir = Path(__file__).resolve().parents[1] / "translations"
    if not base_dir.exists():
        raise SystemExit("translations directory not found.")

    count = 0
    for locale_dir in base_dir.iterdir():
        po_path = locale_dir / "LC_MESSAGES" / "messages.po"
        if po_path.exists():
            compile_catalog(po_path)
            count += 1
    if count == 0:
        print("No translation catalogs found.")


if __name__ == "__main__":
    main()
