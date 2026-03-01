import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from app import (
    init_db,
    db_session,
    USERS_CSV, LOCAL_USER_FIELDS, read_csv_rows, LocalUser,
    MS_USERS_CSV, MS_USER_FIELDS, MsUser
)

def parse_date(date_str: str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        return None

def main():
    print("Initializing Database connection for Users...")
    init_db()

    print("Migrating Local Users...")
    try:
        if USERS_CSV.exists():
            local_user_rows = read_csv_rows(USERS_CSV, LOCAL_USER_FIELDS)
            with db_session() as db:
                db.query(LocalUser).delete()
                for u in local_user_rows:
                    db.add(LocalUser(
                        username=u.get("username"),
                        password=u.get("password"),
                        registration_date=parse_date(u.get("registration_date")),
                        expiry_date=parse_date(u.get("expiry_date")),
                        role=u.get("role"),
                        email=u.get("email"),
                        first_name=u.get("first_name"),
                        last_name=u.get("last_name"),
                        school=u.get("school")
                    ))
                db.commit()
            print(f"Migrated {len(local_user_rows)} local users.")
    except Exception as e:
        print(f"Error migrating local users: {e}")

    print("Migrating MS Users...")
    try:
        if MS_USERS_CSV.exists():
            ms_user_rows = read_csv_rows(MS_USERS_CSV, MS_USER_FIELDS)
            with db_session() as db:
                db.query(MsUser).delete()
                for mu in ms_user_rows:
                    db.add(MsUser(
                        ms_id=mu.get("ms_id"),
                        tenant_id=mu.get("tenant_id"),
                        email=mu.get("email"),
                        display_name=mu.get("display_name"),
                        first_name=mu.get("first_name"),
                        last_name=mu.get("last_name"),
                        school=mu.get("school"),
                        grade=mu.get("grade"),
                        role=mu.get("role"),
                        created_at=parse_date(mu.get("created_at")),
                        updated_at=parse_date(mu.get("updated_at"))
                    ))
                db.commit()
            print(f"Migrated {len(ms_user_rows)} MS users.")
    except Exception as e:
        print(f"Error migrating MS users: {e}")

    print("User Migration Complete!")

if __name__ == "__main__":
    main()
