#!/usr/bin/env python3
"""
PaperQuery password management helper.

Use this utility to:
  * generate PBKDF2 hashes compatible with the web app
  * create or update CSV user records
  * inspect existing users (without exposing hashes)

Examples:
  python tools/manage_passwords.py set --username alice --password secret --role 2
  python tools/manage_passwords.py list
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

PASSWORD_SCHEME = "pbkdf2_sha256"
DEFAULT_ITERATIONS = int(os.environ.get("PAPERQUERY_PBKDF_ITERATIONS", "260000"))
CSV_HEADERS = ["username", "password", "registration_date", "expiry_date", "role"]


@dataclass
class UserRecord:
    username: str
    password: str
    registration_date: str = ""
    expiry_date: str = ""
    role: str = "1"

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "UserRecord":
        return cls(
            username=data.get("username", "").strip(),
            password=data.get("password", "").strip(),
            registration_date=data.get("registration_date", "").strip(),
            expiry_date=data.get("expiry_date", "").strip(),
            role=data.get("role", "").strip() or "1",
        )

    def as_dict(self) -> Dict[str, str]:
        return {
            "username": self.username,
            "password": self.password,
            "registration_date": self.registration_date,
            "expiry_date": self.expiry_date,
            "role": self.role,
        }


def resolve_users_csv(base_dir: Path) -> Path:
    env_path = os.environ.get("PAPERQUERY_USERS_CSV")
    return Path(env_path).resolve() if env_path else (base_dir / "data" / "users.csv")


def hash_password(password: str, *, iterations: int = DEFAULT_ITERATIONS) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    salt_b64 = base64.b64encode(salt).decode("ascii")
    digest_b64 = base64.b64encode(digest).decode("ascii")
    return f"{PASSWORD_SCHEME}${iterations}${salt_b64}${digest_b64}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, iterations_raw, salt_b64, digest_b64 = encoded.split("$", 3)
    except ValueError:
        return False
    if scheme != PASSWORD_SCHEME:
        return False
    try:
        iterations = int(iterations_raw)
        salt = base64.b64decode(salt_b64)
        digest = base64.b64decode(digest_b64)
    except (ValueError, TypeError):
        return False
    check = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=len(digest))
    return hmac.compare_digest(check, digest)


def load_users(path: Path) -> List[UserRecord]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return [UserRecord.from_dict(row) for row in reader]


def save_users(path: Path, users: Iterable[UserRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for user in sorted(users, key=lambda u: u.username.lower()):
            writer.writerow(user.as_dict())


def handle_set(args: argparse.Namespace, csv_path: Path) -> None:
    if not args.password:
        raise SystemExit("Error: --password is required.")
    users = load_users(csv_path)
    hashed = hash_password(args.password)
    incoming = UserRecord(
        username=args.username,
        password=hashed,
        registration_date=args.registration_date or "",
        expiry_date=args.expiry_date or "",
        role=str(args.role),
    )

    updated = False
    for idx, user in enumerate(users):
        if user.username == incoming.username:
            users[idx] = incoming
            updated = True
            break
    if not updated:
        users.append(incoming)

    save_users(csv_path, users)
    action = "Updated" if updated else "Created"
    print(f"{action} user {incoming.username}.")


def handle_list(csv_path: Path) -> None:
    users = load_users(csv_path)
    if not users:
        print("No users found.")
        return
    print(f"{len(users)} user(s) available:")
    for user in users:
        print(
            f"- {user.username} (role {user.role}, registered {user.registration_date or 'n/a'}, "
            f"expires {user.expiry_date or 'no limit'})"
        )


def main() -> None:
    base_dir = Path(__file__).resolve().parents[1]
    csv_path = resolve_users_csv(base_dir)

    parser = argparse.ArgumentParser(description="PaperQuery password and user management utility")
    subparsers = parser.add_subparsers(dest="command", required=True)

    set_parser = subparsers.add_parser("set", help="Create or update a user and hashed password")
    set_parser.add_argument("--username", required=True, help="Username")
    set_parser.add_argument("--password", required=True, help="Plaintext password")
    set_parser.add_argument("--role", type=int, choices=[1, 2, 3], default=1, help="Role (1=read, 2=upload, 3=admin)")
    set_parser.add_argument("--registration-date", help="Registration date, YYYY-MM-DD")
    set_parser.add_argument("--expiry-date", help="Expiry date, YYYY-MM-DD")

    subparsers.add_parser("list", help="List current users")

    args = parser.parse_args()
    if args.command == "set":
        handle_set(args, csv_path)
    elif args.command == "list":
        handle_list(csv_path)


if __name__ == "__main__":
    main()
