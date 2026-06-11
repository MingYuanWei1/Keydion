"""Papers: metadata, records, EE/CP form data, PDF utilities, categories."""
import json
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

from werkzeug.utils import secure_filename

import pdf_text
from config import (
    ALLOWED_EXTENSIONS,
    CP_CRITERIA_DEFS,
    DATA_DIR,
    IB_EE_CRITERIA_DEFS,
    METADATA_FIELDS,
    PAPERS_DIR,
    _DEFAULT_PAPER_CATEGORIES,
    _EE_SUBJECTS_DEFAULT,
    _EE_SUBJECTS_PATH,
)
from db import db_session
from models import PaperMetadataModel


def load_paper_metadata() -> List[Dict[str, str]]:
    with db_session() as db:
        papers = db.query(PaperMetadataModel).all()
        return [{field: (getattr(p, field) or "") for field in METADATA_FIELDS} for p in papers]


def save_paper_metadata(rows: List[Dict[str, str]]) -> None:
    with db_session() as db:
        db.query(PaperMetadataModel).delete()
        for r in rows:
            db.add(PaperMetadataModel(**{field: r.get(field, "") for field in METADATA_FIELDS}))
        db.commit()


def build_paper_record(filename: str, metadata_index: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, str]:
    if metadata_index is None:
        metadata_index = {row["filename"]: row for row in load_paper_metadata()}
    record = {field: "" for field in METADATA_FIELDS}
    record["filename"] = filename
    data = metadata_index.get(filename)
    if data:
        for field in METADATA_FIELDS:
            if field in data and data[field] is not None:
                record[field] = data[field]
    if not record["title"]:
        record["title"] = Path(filename).stem
    return record


def gather_paper_records() -> List[Dict[str, str]]:
    metadata_rows = load_paper_metadata()
    metadata_index = {row["filename"]: row for row in metadata_rows}
    records: List[Dict[str, str]] = []
    for pdf_path in sorted(PAPERS_DIR.glob("*.pdf"), key=lambda item: item.name.lower()):
        records.append(build_paper_record(pdf_path.name, metadata_index))
    records.sort(key=lambda row: (row.get("published_at") or "", row.get("title") or row["filename"]), reverse=True)
    return records


def upsert_paper_metadata(filename: str, data: Dict[str, str]) -> None:
    rows = load_paper_metadata()
    updated = False
    for row in rows:
        if row.get("filename") == filename:
            for field in METADATA_FIELDS:
                if field == "filename":
                    continue
                row[field] = data.get(field, row.get(field, ""))
            updated = True
            break
    if not updated:
        new_row = {field: "" for field in METADATA_FIELDS}
        new_row["filename"] = filename
        for field in METADATA_FIELDS:
            if field != "filename":
                new_row[field] = data.get(field, "")
        rows.append(new_row)
    save_paper_metadata(rows)


def remove_paper_metadata(filename: str) -> None:
    rows = load_paper_metadata()
    filtered = [row for row in rows if row.get("filename") != filename]
    if len(filtered) != len(rows):
        save_paper_metadata(filtered)


def load_paper_categories() -> list:
    """Load paper subject categories from JSON."""
    path = DATA_DIR / "paper_categories.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    save_paper_categories(_DEFAULT_PAPER_CATEGORIES)
    return list(_DEFAULT_PAPER_CATEGORIES)


def save_paper_categories(cats: list) -> None:
    path = DATA_DIR / "paper_categories.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cats, ensure_ascii=False, indent=2), encoding="utf-8")


def load_ee_subjects() -> dict:
    """Load IB EE subject groups from JSON, seeding defaults if needed."""
    if _EE_SUBJECTS_PATH.exists():
        try:
            return json.loads(_EE_SUBJECTS_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    save_ee_subjects(_EE_SUBJECTS_DEFAULT)
    return dict(_EE_SUBJECTS_DEFAULT)


def save_ee_subjects(data: dict) -> None:
    """Save IB EE subject groups to JSON."""
    _EE_SUBJECTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _EE_SUBJECTS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _get_ee_subjects_list() -> list:
    """Return a flat sorted list of all EE subjects."""
    data = load_ee_subjects()
    subjects = set()
    for group in data.get("groups", []):
        for s in group.get("subjects", []):
            subjects.add(s.strip())
    return sorted(subjects)


def _form_int(form, name: str) -> int:
    raw = form.get(name, "").strip()
    return int(raw) if raw.isdigit() else 0


def _build_safe_paper_filename(title: str, author: str = "") -> str:
    """Return a safe PDF filename from a title (+ optional author).

    Caps title at 120 chars and author at 50 chars so the result stays well
    under the filesystem's NAME_MAX (255 bytes on ext4) even after a UUID
    prefix is later prepended for pending uploads. EE research questions
    routinely exceed 255 chars, which would otherwise trigger ENAMETOOLONG.
    """
    safe_title = secure_filename(title or "")[:120]
    safe_author = secure_filename(author or "")[:50]
    if safe_title and safe_author:
        return f"{safe_title}_{safe_author}.pdf"
    if safe_title:
        return f"{safe_title}.pdf"
    if safe_author:
        return f"{safe_author}.pdf"
    return f"{uuid4().hex[:12]}.pdf"


def build_ib_ee_data_from_form(form) -> str:
    criteria = {}
    for letter, label, max_mark in IB_EE_CRITERIA_DEFS:
        criteria[letter] = {
            "label": label,
            "max": max_mark,
            "score": _form_int(form, f"ib_crit_{letter}_score"),
            "comment": form.get(f"ib_crit_{letter}_comment", "").strip(),
        }
    total_score = sum(criterion["score"] for criterion in criteria.values())
    return json.dumps(
        {
            "is_ib_ee": True,
            "core_subject": form.get("ib_ee_core_subject", "").strip(),
            "interdisciplinary_subject": form.get("ib_ee_interdisciplinary_subject", "").strip(),
            "total_grade_letter": form.get("ib_total_grade_letter", "").strip(),
            "total_grade_number": str(total_score),
            "criteria": criteria,
            "holistic_comment": form.get("ib_holistic_comment", "").strip(),
        },
        ensure_ascii=False,
    )


def build_cp_data_from_form(form) -> str:
    criteria = {}
    for letter, label, max_mark in CP_CRITERIA_DEFS:
        criteria[letter] = {
            "label": label,
            "max": max_mark,
            "score": _form_int(form, f"cp_crit_{letter}_score"),
            "comment": form.get(f"cp_crit_{letter}_comment", "").strip(),
        }
    total_score = int(round(sum(criteria[c]["score"] for c in ["A", "B", "C", "D"]) / 4.0))
    return json.dumps(
        {
            "is_cp_paper": True,
            "global_context": form.get("cp_global_context", "").strip(),
            "action_types": form.getlist("cp_action_type"),
            "criteria": criteria,
            "total_score": total_score,
        },
        ensure_ascii=False,
    )


def parse_ib_ee_data_for_form(json_str) -> dict:
    """Flatten ib_ee_data JSON back into form-style keys for draft hydration.

    Returns {} for missing/invalid input so callers can safely .update() the result.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    out = {
        "is_ib_ee": "1",
        "ib_ee_core_subject": data.get("core_subject", ""),
        "ib_ee_interdisciplinary_subject": data.get("interdisciplinary_subject", ""),
        "ib_holistic_comment": data.get("holistic_comment", ""),
    }
    for letter, criterion in (data.get("criteria") or {}).items():
        out[f"ib_crit_{letter}_score"] = str(criterion.get("score", ""))
        out[f"ib_crit_{letter}_comment"] = criterion.get("comment", "")
    return out


def parse_cp_data_for_form(json_str) -> dict:
    """Flatten cp_data JSON back into form-style keys for draft hydration.

    Returns {} for missing/invalid input.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    out = {
        "is_cp_paper": "1",
        "cp_global_context": data.get("global_context", ""),
        "cp_action_types": data.get("action_types") or [],
    }
    for letter, criterion in (data.get("criteria") or {}).items():
        out[f"cp_crit_{letter}_score"] = str(criterion.get("score", ""))
    return out


def _is_ee_paper(record: dict) -> bool:
    raw = record.get("ib_ee_data", "")
    if not raw:
        return False
    try:
        return bool(json.loads(raw).get("is_ib_ee"))
    except (json.JSONDecodeError, TypeError):
        return False


def _is_cp_paper(record: dict) -> bool:
    raw = record.get("cp_data", "")
    if not raw:
        return False
    try:
        return bool(json.loads(raw).get("is_cp_paper"))
    except (json.JSONDecodeError, TypeError):
        return False


def _matches_ee_subject(record: dict, subject: str) -> bool:
    raw = record.get("ib_ee_data", "")
    if not raw:
        return False
    try:
        ib = json.loads(raw)
        s = subject.lower()
        return s in (ib.get("core_subject", "") + " " + ib.get("interdisciplinary_subject", "")).lower()
    except (json.JSONDecodeError, TypeError):
        return False


def _matches_cp_context(record: dict, context: str) -> bool:
    raw = record.get("cp_data", "")
    if not raw:
        return False
    try:
        cp = json.loads(raw)
        return context.lower() in (cp.get("global_context", "")).lower()
    except (json.JSONDecodeError, TypeError):
        return False


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        from PyPDF2 import PdfReader
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("PyPDF2 is required for PDF search.") from exc

    reader = PdfReader(str(pdf_path))
    text_parts: List[str] = []
    for page in reader.pages:
        text_parts.append(page.extract_text() or "")
    return "\n".join(text_parts)


def extract_text_from_upload(filename: str, raw: bytes) -> str:
    """Extract plain text from an uploaded attachment by extension.

    Supports PDF (PyPDF2), DOCX (python-docx), and TXT/MD (utf-8). Raises
    ValueError for anything else.
    """
    name = (filename or "").lower()
    if name.endswith(".pdf"):
        return pdf_text.extract_pdf_text(raw)
    if name.endswith(".docx"):
        from docx import Document
        doc = Document(BytesIO(raw))
        return "\n".join(p.text for p in doc.paragraphs)
    if name.endswith((".txt", ".md")):
        return raw.decode("utf-8", "ignore")
    raise ValueError("unsupported file type")


def set_pdf_metadata(pdf_path: Path, title: str, author: str) -> None:
    try:
        from PyPDF2 import PdfReader, PdfWriter
    except ImportError as exc:
        print(f"PyPDF2 not installed, unable to set metadata: {exc}")
        return

    try:
        reader = PdfReader(str(pdf_path))
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)

        metadata = reader.metadata or {}
        new_metadata = dict(metadata)
        if title:
            new_metadata["/Title"] = title
        if author:
            new_metadata["/Author"] = author

        writer.add_metadata(new_metadata)

        with open(pdf_path, "wb") as f:
            writer.write(f)
    except Exception as exc:
        print(f"Failed to update PDF metadata for {pdf_path}: {exc}")


def build_preview_pdf(pdf_path: Path, *, max_pages: int = 2) -> BytesIO:
    try:
        from PyPDF2 import PdfReader, PdfWriter
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("PyPDF2 is required for PDF previews.") from exc

    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()
    for page in reader.pages[:max_pages]:
        writer.add_page(page)
    buffer = BytesIO()
    writer.write(buffer)
    buffer.seek(0)
    return buffer
