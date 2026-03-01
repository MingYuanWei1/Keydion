#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import Column, Date, DateTime, ForeignKey, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

DB_URL = os.environ.get("PAPERQUERY_DATABASE_URL")
BASE = declarative_base()


class LocalUser(BASE):
    __tablename__ = "local_users"
    username = Column(String(255), primary_key=True)
    password = Column(String(255), nullable=False)
    registration_date = Column(Date)
    expiry_date = Column(Date)
    role = Column(String(10), nullable=False)


class MsUser(BASE):
    __tablename__ = "ms_users"
    ms_id = Column(String(255), primary_key=True)
    tenant_id = Column(String(255))
    email = Column(String(255))
    display_name = Column(String(255))
    first_name = Column(String(255))
    last_name = Column(String(255))
    school = Column(String(255))
    grade = Column(String(255))
    role = Column(String(10))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class AccountLink(BASE):
    __tablename__ = "account_links"
    username = Column(String(255), ForeignKey("local_users.username", ondelete="CASCADE"), primary_key=True)
    ms_id = Column(String(255), ForeignKey("ms_users.ms_id", ondelete="CASCADE"), primary_key=True)
    linked_at = Column(DateTime)


def parse_date(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_datetime(value: str):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def load_csv(path: Path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    if not DB_URL:
        raise SystemExit("PAPERQUERY_DATABASE_URL is not set.")

    base_dir = Path(__file__).resolve().parents[1]
    data_dir = Path(os.environ.get("PAPERQUERY_DATA_DIR", base_dir / "data")).resolve()
    users_csv = Path(os.environ.get("PAPERQUERY_USERS_CSV", data_dir / "users.csv")).resolve()
    ms_users_csv = data_dir / "ms_users.csv"
    links_csv = data_dir / "account_links.csv"

    engine = create_engine(DB_URL, pool_pre_ping=True)
    BASE.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()

    try:
        for row in load_csv(users_csv):
            username = row.get("username", "").strip()
            if not username or session.get(LocalUser, username):
                continue
            session.add(
                LocalUser(
                    username=username,
                    password=row.get("password", ""),
                    registration_date=parse_date(row.get("registration_date", "")),
                    expiry_date=parse_date(row.get("expiry_date", "")),
                    role=row.get("role", "1") or "1",
                )
            )

        for row in load_csv(ms_users_csv):
            ms_id = row.get("ms_id", "").strip()
            if not ms_id or session.get(MsUser, ms_id):
                continue
            session.add(
                MsUser(
                    ms_id=ms_id,
                    tenant_id=row.get("tenant_id", ""),
                    email=row.get("email", ""),
                    display_name=row.get("display_name", ""),
                    first_name=row.get("first_name", ""),
                    last_name=row.get("last_name", ""),
                    school=row.get("school", ""),
                    grade=row.get("grade", ""),
                    role=row.get("role", "1") or "1",
                    created_at=parse_datetime(row.get("created_at", "")),
                    updated_at=parse_datetime(row.get("updated_at", "")),
                )
            )

        for row in load_csv(links_csv):
            username = row.get("username", "").strip()
            ms_id = row.get("ms_id", "").strip()
            if not username or not ms_id:
                continue
            exists = (
                session.query(AccountLink)
                .filter(AccountLink.username == username, AccountLink.ms_id == ms_id)
                .first()
            )
            if exists:
                continue
            session.add(
                AccountLink(
                    username=username,
                    ms_id=ms_id,
                    linked_at=parse_datetime(row.get("linked_at", "")),
                )
            )

        session.commit()
        print("Import completed.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
