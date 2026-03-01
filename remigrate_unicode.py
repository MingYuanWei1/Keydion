import os
from dotenv import load_dotenv

load_dotenv()

from app import (
    BASE, _ENGINE, db_session, db_enabled, init_db,
    METADATA_CSV, METADATA_FIELDS, read_csv_rows, PaperMetadataModel,
    NEWS_CSV, NEWS_FIELDS, NewsArticleModel,
    SUBMISSIONS_JSON, SubmissionModel,
    JOURNALS_JSON, JournalModel,
    LocalUser, MsUser,
    DATA_DIR
)
import json
from datetime import datetime

USERS_CSV = DATA_DIR / "users.csv"
LOCAL_USER_FIELDS = ["username", "password", "registration_date", "expiry_date", "role", "email", "first_name", "last_name", "school"]

MS_USERS_CSV = DATA_DIR / "ms_users.csv"
MS_USER_FIELDS = ["ms_id", "tenant_id", "email", "display_name", "first_name", "last_name", "school", "grade", "role", "created_at", "updated_at"]

def parse_date(date_str: str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        return None

def main():
    print("Dropping and Re-creating Azure Tables as Unicode (NVARCHAR)...")
    init_db()
    
    # Drop all tables to completely destroy the bad VARCHAR schema
    from app import _ENGINE
    BASE.metadata.drop_all(_ENGINE)
    BASE.metadata.create_all(_ENGINE)
    
    # We briefly turn off db_enabled to easily use our read_csv_rows helpers locally
    # but still write explicitly to Azure DB session.

    print("Migrating Local Users...")
    if USERS_CSV.exists():
        with db_session() as db:
            for u in read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS):
                db.add(LocalUser(
                    username=u.get("username"), password=u.get("password"),
                    registration_date=parse_date(u.get("registration_date")),
                    expiry_date=parse_date(u.get("expiry_date")), role=u.get("role"),
                    email=u.get("email"), first_name=u.get("first_name"),
                    last_name=u.get("last_name"), school=u.get("school")
                ))
            db.commit()

    print("Migrating MS Users...")
    if MS_USERS_CSV.exists():
        with db_session() as db:
            for mu in read_csv_rows(MS_USERS_CSV, MS_USER_FIELDS):
                db.add(MsUser(
                    ms_id=mu.get("ms_id"), tenant_id=mu.get("tenant_id"),
                    email=mu.get("email"), display_name=mu.get("display_name"),
                    first_name=mu.get("first_name"), last_name=mu.get("last_name"),
                    school=mu.get("school"), grade=mu.get("grade"), role=mu.get("role"),
                    created_at=parse_date(mu.get("created_at")), updated_at=parse_date(mu.get("updated_at"))
                ))
            db.commit()

    print("Migrating Papers Metadata...")
    if METADATA_CSV.exists():
        with db_session() as db:
            for r in read_csv_rows(METADATA_CSV, METADATA_FIELDS):
                db.add(PaperMetadataModel(**{field: r.get(field, "") for field in METADATA_FIELDS}))
            db.commit()

    print("Migrating News Articles...")
    if NEWS_CSV.exists():
        with db_session() as db:
            for article in read_csv_rows(NEWS_CSV, NEWS_FIELDS):
                db.add(NewsArticleModel(**{field: article.get(field, "") for field in NEWS_FIELDS}))
            db.commit()

    print("Migrating Submissions...")
    if SUBMISSIONS_JSON.exists():
        subs = json.loads(SUBMISSIONS_JSON.read_text(encoding="utf-8"))
        with db_session() as db:
            for s in subs:
                db.add(SubmissionModel(
                    id=s.get("id"), pdf_filename=s.get("pdf_filename"),
                    pending_filename=s.get("pending_filename"), title=s.get("title"),
                    author_name=s.get("author_name"), author_email=s.get("author_email"),
                    author_school=s.get("author_school"), status=s.get("status"),
                    submitted_at=s.get("submitted_at"), feedback=s.get("feedback"),
                    abstract=s.get("abstract"), keywords=s.get("keywords"),
                    journal=s.get("journal"), category=s.get("category"),
                    language=s.get("language"), submitted_by=s.get("submitter"),
                    original_filename=s.get("original_filename")
                ))
            db.commit()

    print("Migrating Journals...")
    if JOURNALS_JSON.exists():
        journals = json.loads(JOURNALS_JSON.read_text(encoding="utf-8"))
        with db_session() as db:
            for j in journals:
                db.add(JournalModel(
                    id=j.get("id"), name=j.get("name"), cover_image=j.get("cover_image"),
                    introduction=j.get("introduction"), created_at=j.get("created_at")
                ))
            db.commit()

    print("Unicode Matrix Clean Data Re-Migration Complete!")

if __name__ == "__main__":
    main()
