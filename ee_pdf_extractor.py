"""IB Extended Essay 'commentary for example essay' PDF parser.

Public surface:
    extract_ee_metadata(file_bytes) -> dict
    EePdfExtractionError
"""

from __future__ import annotations


class EePdfExtractionError(Exception):
    """Raised when the PDF cannot be processed at all (corrupt, encrypted, scanned)."""


def _empty_result() -> dict:
    return {
        "core_subject": "",
        "interdisciplinary_subject": "",
        "framework": "",
        "research_question": "",
        "criteria": {letter: {"score": None, "comment": ""} for letter in "ABCDE"},
        "holistic_comment": "",
        "warnings": [],
    }


def extract_ee_metadata(file_bytes: bytes) -> dict:
    """Parse an IB EE commentary PDF. See module docstring for contract."""
    if not file_bytes:
        raise EePdfExtractionError("Empty file")
    return _empty_result()
