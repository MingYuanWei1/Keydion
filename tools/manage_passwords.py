#!/usr/bin/env python3
"""
PaperQuery password management helper.

Use this utility to:
  * generate PBKDF2 hashes compatible with the web app
  * create or update SQL user records
  * inspect existing users (without exposing hashes)

Examples:
  python tools/manage_passwords.py set --username alice --password secret --role 2
  python tools/manage_passwords.py list
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add the repository root so the command can be run as a script.
sys.path.append(str(Path(__file__).resolve().parents[1]))

from db import db_session
from models import LocalUser, init_db
from services.auth import hash_password, validate_password

def handle_set(args: argparse.Namespace) -> None:
    if not args.password:
        raise SystemExit("Error: --password is required.")
    try:
        validate_password(args.password)
    except ValueError as exc:
        raise SystemExit(f"Error: {exc}") from exc
    
    with db_session() as db:
        user = db.query(LocalUser).filter(LocalUser.username == args.username).first()
        hashed = hash_password(args.password)
        
        if user:
            user.password = hashed
            user.role = str(args.role)
            if args.registration_date:
                from datetime import datetime
                user.registration_date = datetime.strptime(args.registration_date, "%Y-%m-%d").date()
            if args.expiry_date:
                from datetime import datetime
                user.expiry_date = datetime.strptime(args.expiry_date, "%Y-%m-%d").date()
            print(f"Updated user {args.username}.")
        else:
            from datetime import datetime
            new_user = LocalUser(
                username=args.username,
                password=hashed,
                role=str(args.role),
                registration_date=datetime.strptime(args.registration_date, "%Y-%m-%d").date() if args.registration_date else datetime.utcnow().date(),
                expiry_date=datetime.strptime(args.expiry_date, "%Y-%m-%d").date() if args.expiry_date else None
            )
            db.add(new_user)
            print(f"Created user {args.username}.")

def handle_list() -> None:
    with db_session() as db:
        users = db.query(LocalUser).all()
        if not users:
            print("No users found.")
            return
        print(f"{len(users)} user(s) available in database:")
        for user in users:
            print(
                f"- {user.username} (role {user.role}, registered {user.registration_date or 'n/a'}, "
                f"expires {user.expiry_date or 'no limit'})"
            )

def main() -> None:
    # Startup is verification-only: operators must migrate/bootstrap first.
    init_db()

    parser = argparse.ArgumentParser(description="PaperQuery password and user management utility")
    subparsers = parser.add_subparsers(dest="command", required=True)

    set_parser = subparsers.add_parser("set", help="Create or update a user and hashed password")
    set_parser.add_argument("--username", required=True, help="Username")
    set_parser.add_argument("--password", required=True, help="Plaintext password")
    set_parser.add_argument(
        "--role",
        type=int,
        choices=[1, 2, 3],
        default=1,
        help="Role (1=Reader, 2=Contributor, 3=Curator)",
    )
    set_parser.add_argument("--registration-date", help="Registration date, YYYY-MM-DD")
    set_parser.add_argument("--expiry-date", help="Expiry date, YYYY-MM-DD")

    subparsers.add_parser("list", help="List current users")

    args = parser.parse_args()
    if args.command == "set":
        handle_set(args)
    elif args.command == "list":
        handle_list()

if __name__ == "__main__":
    main()
