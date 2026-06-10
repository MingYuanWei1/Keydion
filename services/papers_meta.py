"""Paper metadata: DB-backed storage helpers shared across domains.

Interim module — will be folded into services/papers.py when the papers
domain is extracted. Do not add new helpers here.
"""
from pathlib import Path
from typing import Dict, List, Optional

from config import METADATA_FIELDS, PAPERS_DIR
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
