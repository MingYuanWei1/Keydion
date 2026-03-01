import os
from dotenv import load_dotenv

# Set the flag to use CSV so we can read from it, but actually we need BOTH.
# So we will manually read CSVs and then write via SQLAlchemy.
# To do this safely, we will import the functions directly while disabling db_enabled temporarily,
# or just read the CSVs using the helpers.

load_dotenv()

# We temporarily force db_enabled() to return True by keeping PAPERQUERY_DATABASE_URL
# but we will manually read the CSVs.

from app import (
    init_db,
    db_session,
    METADATA_CSV, METADATA_FIELDS, read_csv_rows, ensure_metadata_file, PaperMetadataModel,
    NEWS_CSV, NEWS_FIELDS, NewsArticleModel,
    SUBMISSIONS_JSON, SubmissionModel,
    JOURNALS_JSON, JournalModel
)
import json
import csv

def main():
    print("Initializing Database connection...")
    init_db()

    print("Migrating Papers Metadata...")
    ensure_metadata_file()
    with open(METADATA_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        paper_rows = []
        for raw_row in reader:
            normalized = {field: (raw_row.get(field, "") or "").strip() for field in METADATA_FIELDS}
            paper_rows.append(normalized)
    
    with db_session() as db:
        # Avoid duplicate data
        db.query(PaperMetadataModel).delete()
        for r in paper_rows:
            db.add(PaperMetadataModel(**{field: r.get(field, "") for field in METADATA_FIELDS}))
        db.commit()
    print(f"Migrated {len(paper_rows)} papers.")

    print("Migrating News Articles...")
    try:
        if NEWS_CSV.exists():
            news_rows = read_csv_rows(NEWS_CSV, NEWS_FIELDS)
            with db_session() as db:
                db.query(NewsArticleModel).delete()
                for article in news_rows:
                    db.add(NewsArticleModel(**{field: article.get(field, "") for field in NEWS_FIELDS}))
                db.commit()
            print(f"Migrated {len(news_rows)} news articles.")
    except Exception as e:
        print(f"Error migrating news: {e}")

    print("Migrating Submissions...")
    try:
        if SUBMISSIONS_JSON.exists():
            subs = json.loads(SUBMISSIONS_JSON.read_text(encoding="utf-8"))
            with db_session() as db:
                db.query(SubmissionModel).delete()
                for s in subs:
                    db.add(SubmissionModel(
                        id=s.get("id"),
                        pdf_filename=s.get("pdf_filename"),
                        pending_filename=s.get("pending_filename"),
                        title=s.get("title"),
                        author_name=s.get("author_name"),
                        author_email=s.get("author_email"),
                        author_school=s.get("author_school"),
                        status=s.get("status"),
                        submitted_at=s.get("submitted_at"),
                        feedback=s.get("feedback"),
                        abstract=s.get("abstract"),
                        keywords=s.get("keywords"),
                        journal=s.get("journal"),
                        category=s.get("category"),
                        language=s.get("language"),
                        submitted_by=s.get("submitter"),
                        original_filename=s.get("original_filename")
                    ))
                db.commit()
            print(f"Migrated {len(subs)} submissions.")
    except Exception as e:
        print(f"Error migrating submissions: {e}")

    print("Migrating Journals...")
    try:
        if JOURNALS_JSON.exists():
            journals = json.loads(JOURNALS_JSON.read_text(encoding="utf-8"))
            with db_session() as db:
                db.query(JournalModel).delete()
                for j in journals:
                    db.add(JournalModel(
                        id=j.get("id"),
                        name=j.get("name"),
                        cover_image=j.get("cover_image"),
                        introduction=j.get("introduction"),
                        created_at=j.get("created_at")
                    ))
                db.commit()
            print(f"Migrated {len(journals)} journals.")
    except Exception as e:
        print(f"Error migrating journals: {e}")

    print("Migration Complete!")

if __name__ == "__main__":
    main()
