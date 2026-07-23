"""Papers: metadata, records, EE/CP form data, PDF utilities, categories."""
import json
import os
from dataclasses import asdict
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
    _IA_SUBJECTS_DEFAULT,
    _IA_SUBJECTS_PATH,
)
from db import db_session
from models import PaperMetadataModel


def _atomic_json_write(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_paper_metadata() -> List[Dict[str, str]]:
    with db_session() as db:
        papers = db.query(PaperMetadataModel).all()
        return [{field: (getattr(p, field) or "") for field in METADATA_FIELDS} for p in papers]


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


def gather_paper_records(library) -> List[Dict[str, str]]:
    """Project an explicitly injected PaperLibrary's visible inventory."""
    records = []
    for paper in library.list_visible():
        record = asdict(paper)
        record.pop("row_version", None)
        record["revision_number"] = paper.current_revision
        records.append(record)
    return records


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
    _atomic_json_write(path, cats)


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
    _atomic_json_write(_EE_SUBJECTS_PATH, data)


def _get_ee_subjects_list() -> list:
    """Return a flat sorted list of all EE subjects."""
    data = load_ee_subjects()
    subjects = set()
    for group in data.get("groups", []):
        for s in group.get("subjects", []):
            subjects.add(s.strip())
    return sorted(subjects)


def reconcile_ee_subjects(old_tree: dict, payload: dict) -> dict:
    """Pure diff of a posted EE-subjects payload against the saved tree.

    Returns ``{"tree", "renames", "deletions", "errors"}`` and touches no
    DB or filesystem. ``tree`` is in the on-disk shape (groups carry plain
    name lists; a top-level ``interdisciplinary_subjects`` list is rebuilt
    from the per-subject flags).
    """
    errors: List[str] = []
    old_names = set()
    for g in (old_tree or {}).get("groups", []):
        for s in g.get("subjects", []):
            old_names.add(s)

    existing_ids = [g.get("id") for g in (old_tree or {}).get("groups", [])
                    if isinstance(g.get("id"), int)]
    next_id = (max(existing_ids) + 1) if existing_ids else 1

    groups_out = []
    renames = []
    seen_originals = set()
    interdisciplinary: List[str] = []

    for g in payload.get("groups", []):
        name = (g.get("name") or "").strip()
        if not name:
            errors.append("Group name cannot be empty.")
        gid = g.get("id")
        if not isinstance(gid, int):
            gid = next_id
            next_id += 1
        subj_names = []
        seen_in_group = set()
        for s in g.get("subjects", []):
            sname = (s.get("name") or "").strip()
            if not sname:
                errors.append("Subject name cannot be empty.")
                continue
            if sname.lower() in seen_in_group:
                errors.append("Duplicate subject '%s' in group '%s'." % (sname, name))
                continue
            seen_in_group.add(sname.lower())
            subj_names.append(sname)
            orig = s.get("original_name")
            if orig:
                seen_originals.add(orig)
                if orig != sname:
                    renames.append((orig, sname))
            if s.get("interdisciplinary"):
                interdisciplinary.append(sname)
        groups_out.append({"id": gid, "name": name, "subjects": subj_names})

    output_names = {s for g in groups_out for s in g["subjects"]}
    deletions = sorted(old_names - seen_originals - output_names)
    tree = {"groups": groups_out, "interdisciplinary_subjects": interdisciplinary}
    return {"tree": tree, "renames": renames, "deletions": deletions, "errors": errors}


def count_papers_using_ee_subject(name: str) -> int:
    """Number of papers whose ib_ee_data core/interdisciplinary subject equals name."""
    count = 0
    for row in load_paper_metadata():
        raw = row.get("ib_ee_data", "")
        if not raw:
            continue
        try:
            ib = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if ib.get("core_subject") == name or ib.get("interdisciplinary_subject") == name:
            count += 1
    return count


def load_ia_subjects() -> dict:
    """Load IB IA subject groups from JSON, seeding defaults if needed."""
    if _IA_SUBJECTS_PATH.exists():
        try:
            return json.loads(_IA_SUBJECTS_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    save_ia_subjects(_IA_SUBJECTS_DEFAULT)
    return dict(_IA_SUBJECTS_DEFAULT)


def save_ia_subjects(data: dict) -> None:
    """Save IB IA subject groups to JSON."""
    _atomic_json_write(_IA_SUBJECTS_PATH, data)


def _get_ia_subjects_list() -> list:
    """Return a flat sorted list of all IA subject names across groups."""
    data = load_ia_subjects()
    subjects = set()
    for group in data.get("groups", []):
        for s in group.get("subjects", []):
            name = (s.get("name") or "").strip()
            if name:
                subjects.add(name)
    return sorted(subjects)


def reconcile_ia_subjects(old_tree: dict, payload: dict) -> dict:
    """Pure diff of a posted IA-subjects payload against the saved tree.

    Returns ``{"tree", "renames", "deletions", "errors"}`` and touches no
    DB or filesystem. ``tree`` is in the on-disk shape: groups carry object
    subjects ``{"name", "criteria": [{"name", "max"}]}``. There is NO
    interdisciplinary handling for IA. New groups get the next int id.
    """
    errors: List[str] = []
    old_names = set()
    for g in (old_tree or {}).get("groups", []):
        for s in g.get("subjects", []):
            nm = (s.get("name") or "").strip() if isinstance(s, dict) else (s or "").strip()
            if nm:
                old_names.add(nm)

    existing_ids = [g.get("id") for g in (old_tree or {}).get("groups", [])
                    if isinstance(g.get("id"), int)]
    next_id = (max(existing_ids) + 1) if existing_ids else 1

    groups_out = []
    renames = []
    seen_originals = set()

    for g in payload.get("groups", []):
        name = (g.get("name") or "").strip()
        if not name:
            errors.append("Group name cannot be empty.")
        gid = g.get("id")
        if not isinstance(gid, int):
            gid = next_id
            next_id += 1
        subjects_out = []
        seen_in_group = set()
        for s in g.get("subjects", []):
            sname = (s.get("name") or "").strip()
            if not sname:
                errors.append("Subject name cannot be empty.")
                continue
            if sname.lower() in seen_in_group:
                errors.append("Duplicate subject '%s' in group '%s'." % (sname, name))
                continue
            seen_in_group.add(sname.lower())
            criteria_out = []
            for c in s.get("criteria", []):
                cname = (c.get("name") or "").strip()
                if not cname:
                    errors.append("Criterion name cannot be empty in subject '%s'." % sname)
                    continue
                cmax = c.get("max")
                if not isinstance(cmax, int) or isinstance(cmax, bool) or cmax < 1:
                    errors.append("Criterion '%s' max must be an integer >= 1." % cname)
                    continue
                criteria_out.append({"name": cname, "max": cmax})
            subjects_out.append({"name": sname, "criteria": criteria_out})
            orig = s.get("original_name")
            if orig:
                seen_originals.add(orig)
                if orig != sname:
                    renames.append((orig, sname))
        groups_out.append({"id": gid, "name": name, "subjects": subjects_out})

    output_names = {s["name"] for g in groups_out for s in g["subjects"]}
    deletions = sorted(old_names - seen_originals - output_names)
    tree = {"groups": groups_out}
    return {"tree": tree, "renames": renames, "deletions": deletions, "errors": errors}


def count_papers_using_ia_subject(name: str) -> int:
    """Number of papers whose ia_data subject equals name."""
    count = 0
    for row in load_paper_metadata():
        raw = row.get("ia_data", "")
        if not raw:
            continue
        try:
            ia = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        if ia.get("subject") == name:
            count += 1
    return count


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


def resolve_contained(base_dir: Path, filename: str, *, must_exist: bool = False) -> Optional[Path]:
    """Resolve a user-supplied filename against a store root, rejecting escapes.

    Returns the canonical absolute Path of ``base_dir / filename`` when it stays
    inside ``base_dir`` after symlink resolution, else ``None``. With
    ``must_exist=True`` a contained-but-absent path also returns ``None`` (read /
    serve / delete callers); pre-write callers leave it False. Never raises — each
    caller decides its own response to ``None`` (abort, flash, continue, or "").
    """
    root = base_dir.resolve()
    candidate = (base_dir / filename).resolve()
    if not candidate.is_relative_to(root):
        return None
    if must_exist and not candidate.exists():
        return None
    return candidate


def build_ib_ee_data_from_form(form) -> str:
    criteria = {}
    for letter, label, max_mark in IB_EE_CRITERIA_DEFS:
        criteria[letter] = {
            "label": label,
            "max": max_mark,
            "score": min(_form_int(form, f"ib_crit_{letter}_score"), max_mark),
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
            "score": min(_form_int(form, f"cp_crit_{letter}_score"), max_mark),
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


def _ia_criteria_for_subject(subject: str, data: dict = None) -> list:
    """Return the criteria list ([{name, max}, ...]) for an IA subject by name.

    Walks the on-disk taxonomy (groups -> subjects), loading it via
    ``load_ia_subjects()`` when ``data`` is not supplied. Returns [] when the
    subject is unknown, so a stray form value yields an empty (zero-total) blob.
    """
    target = (subject or "").strip().lower()
    if not target:
        return []
    if data is None:
        data = load_ia_subjects()
    for group in data.get("groups", []):
        for s in group.get("subjects", []):
            if (s.get("name") or "").strip().lower() == target:
                return s.get("criteria") or []
    return []


def build_ia_data_from_form(form) -> str:
    subject = form.get("ia_subject", "").strip()
    # Criterion list (and thus each max) comes from the on-disk taxonomy;
    # the form's numbers are never trusted.
    defs = _ia_criteria_for_subject(subject, load_ia_subjects())
    # Holistic-only mode: the user enters the overall mark directly and skips
    # per-criterion scoring, so per-criterion scores are left blank (None).
    holistic_only = form.get("ia_holistic_only") == "1"
    criteria = []
    for i, cdef in enumerate(defs):
        max_mark = int(cdef.get("max", 0))
        score = None if holistic_only else min(_form_int(form, f"ia_crit_{i}_score"), max_mark)
        criteria.append({
            "name": cdef.get("name", ""),
            "max": max_mark,
            "score": score,
            "comment": form.get(f"ia_crit_{i}_comment", "").strip(),
        })
    total_max = sum(c["max"] for c in criteria)
    if holistic_only:
        # Direct overall mark — clamped server-side to [0, total_max] (the only
        # value trusted from the form, and still bounded by the subject config).
        total_score = max(0, min(_form_int(form, "ia_total_score"), total_max))
    else:
        total_score = sum((c["score"] or 0) for c in criteria)
    return json.dumps(
        {
            "is_ia": True,
            "subject": subject,
            "holistic_only": holistic_only,
            "criteria": criteria,
            "total_score": total_score,
            "total_max": total_max,
            "holistic_comment": form.get("ia_holistic_comment", "").strip(),
        },
        ensure_ascii=False,
    )


def parse_ia_data_for_form(json_str) -> dict:
    """Flatten ia_data JSON back into form-style keys for draft hydration.

    Returns {} for missing/invalid input so callers can safely .update() it.
    """
    if not json_str:
        return {}
    try:
        data = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
    holistic_only = bool(data.get("holistic_only"))
    out = {
        "is_ia": "1",
        "ia_subject": data.get("subject", ""),
        "ia_holistic_comment": data.get("holistic_comment", ""),
        "ia_holistic_only": "1" if holistic_only else "",
        "ia_total_score": str(data.get("total_score", "")) if holistic_only else "",
    }
    for i, criterion in enumerate(data.get("criteria") or []):
        score = criterion.get("score")
        out[f"ia_crit_{i}_score"] = "" if score is None else str(score)
        out[f"ia_crit_{i}_comment"] = criterion.get("comment", "")
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


def _is_ia_paper(record: dict) -> bool:
    raw = record.get("ia_data", "")
    if not raw:
        return False
    try:
        return bool(json.loads(raw).get("is_ia"))
    except (json.JSONDecodeError, TypeError):
        return False


def _matches_ia_subject(record: dict, subject: str) -> bool:
    raw = record.get("ia_data", "")
    if not raw:
        return False
    try:
        ia = json.loads(raw)
        return subject.lower() in (ia.get("subject", "")).lower()
    except (json.JSONDecodeError, TypeError):
        return False


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("pypdf is required for PDF search.") from exc

    reader = PdfReader(str(pdf_path))
    text_parts: List[str] = []
    for page in reader.pages:
        text_parts.append(page.extract_text() or "")
    return "\n".join(text_parts)


def extract_text_from_upload(filename: str, raw: bytes) -> str:
    """Extract plain text from an uploaded attachment by extension.

    Supports PDF (pypdf), DOCX (python-docx), and TXT/MD (utf-8). Raises
    ValueError for anything else.
    """
    name = (filename or "").lower()
    if name.endswith(".pdf"):
        # pypdf + bounded local OCR only — deliberately no vision fallback:
        # attachments are untrusted user uploads, and opting them into paid
        # vision calls is a DoS/cost-abuse vector (reverts commit 6b34d80).
        return pdf_text.extract_pdf_text(raw)
    if name.endswith(".docx"):
        from services.attachment_processing import preflight_docx
        from docx import Document

        preflight_docx(raw)
        doc = Document(BytesIO(raw))
        return "\n".join(p.text for p in doc.paragraphs)
    if name.endswith((".txt", ".md")):
        return raw.decode("utf-8", "ignore")
    raise ValueError("unsupported file type")


def set_pdf_metadata(pdf_path: Path, title: str, author: str) -> None:
    try:
        from pypdf import PdfReader, PdfWriter
    except ImportError as exc:
        print(f"pypdf not installed, unable to set metadata: {exc}")
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
        from pypdf import PdfReader, PdfWriter
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("pypdf is required for PDF previews.") from exc

    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()
    for page in reader.pages[:max_pages]:
        writer.add_page(page)
    buffer = BytesIO()
    writer.write(buffer)
    buffer.seek(0)
    return buffer
