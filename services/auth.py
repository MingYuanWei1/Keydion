"""Users, sessions, authentication, MS Graph OAuth helpers."""
import base64
import binascii
import hashlib
import hmac
import os
import secrets
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

import msal
import requests
from flask import flash, request, session
from flask_babel import gettext as _

from config import (
    MS_AUTHORITY, MS_CLIENT_ID, MS_CLIENT_SECRET, MS_GRAPH_ME_URL,
    MS_USER_FIELDS, PASSWORD_SCHEME, SESSION_TIMEOUT, SESSION_TIMEOUT_SECONDS,
)
from db import db_session
from models import LocalUser, MsUser, SessionModel


def load_users() -> List[Dict[str, str]]:
    with db_session() as db:
        users = db.query(LocalUser).order_by(LocalUser.username.asc()).all()
        return [
            {
                "username": user.username,
                "password": user.password,
                "registration_date": user.registration_date.isoformat() if user.registration_date else "",
                "expiry_date": user.expiry_date.isoformat() if user.expiry_date else "",
                "role": user.role,
                "email": user.email or "",
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "school": user.school or "",
            }
            for user in users
        ]


def get_local_user(username: str) -> Optional[Dict[str, str]]:
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return None
        return {
            "username": user.username,
            "password": user.password,
            "registration_date": user.registration_date.isoformat() if user.registration_date else "",
            "expiry_date": user.expiry_date.isoformat() if user.expiry_date else "",
            "role": user.role,
            "email": user.email or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
        }


def get_local_user_by_email(email: str) -> Optional[Dict[str, str]]:
    """Look up a local user by email address."""
    if not email:
        return None
    with db_session() as db:
        user = db.query(LocalUser).filter(LocalUser.email == email).first()
        if not user:
            return None
        return {
            "username": user.username,
            "password": user.password,
            "registration_date": user.registration_date.isoformat() if user.registration_date else "",
            "expiry_date": user.expiry_date.isoformat() if user.expiry_date else "",
            "role": user.role,
            "email": user.email or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
        }


def hash_password(password: str) -> str:
    iterations = int(os.environ.get("PAPERQUERY_PBKDF_ITERATIONS", "260000"))
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    salt_b64 = base64.b64encode(salt).decode("ascii")
    digest_b64 = base64.b64encode(digest).decode("ascii")
    return f"{PASSWORD_SCHEME}${iterations}${salt_b64}${digest_b64}"


def create_local_user(
    username: str,
    password: str,
    role: str = "1",
    email: str = "",
    first_name: str = "",
    last_name: str = "",
    school: str = "",
) -> Dict[str, str]:
    with db_session() as db:
        if db.get(LocalUser, username):
            raise ValueError("Username already exists.")
        record = LocalUser(
            username=username,
            password=hash_password(password),
            registration_date=datetime.utcnow().date(),
            expiry_date=None,
            role=role,
            email=email,
            first_name=first_name,
            last_name=last_name,
            school=school,
        )
        db.add(record)
        return {
            "username": record.username,
            "password": record.password,
            "registration_date": record.registration_date.isoformat() if record.registration_date else "",
            "expiry_date": "",
            "role": record.role,
            "email": record.email or "",
            "first_name": record.first_name or "",
            "last_name": record.last_name or "",
            "school": record.school or "",
        }


def update_local_user_role(username: str, role: str) -> bool:
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return False
        user.role = role
        return True


def update_local_user_password(username: str, password: str) -> bool:
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return False
        user.password = hash_password(password)
        return True


def delete_local_user(username: str) -> bool:
    with db_session() as db:
        user = db.get(LocalUser, username)
        if not user:
            return False
        db.delete(user)
        return True


def authenticate(username: str, password: str) -> Optional[Dict[str, str]]:
    today = datetime.utcnow().date()
    for user in load_users():
        if user.get("username") != username:
            continue
        encoded = user.get("password", "")
        if not encoded:
            continue
        if not verify_password(password, encoded):
            continue
        expiry_str = user.get("expiry_date")
        if expiry_str:
            try:
                expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
            except ValueError:
                expiry_date = None
            if expiry_date and expiry_date < today:
                return None
        return {
            "username": user.get("username", ""),
            "role": user.get("role", "1"),
            "registered_at": user.get("registration_date", ""),
            "expiry_date": expiry_str or "",
        }
    return None


def load_active_local_user(username: str) -> Optional[Dict[str, str]]:
    record = get_local_user(username)
    if not record:
        return None
    expiry_str = record.get("expiry_date", "")
    if expiry_str:
        try:
            expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        except ValueError:
            expiry_date = None
        if expiry_date and expiry_date < datetime.utcnow().date():
            return None
    return {
        "username": record.get("username", ""),
        "role": record.get("role", "1"),
        "registered_at": record.get("registration_date", ""),
        "expiry_date": expiry_str or "",
    }


def start_local_session(
    user: Dict[str, str],
    *,
    ms_id: str = "",
    display_name: str = "",
    email: str = "",
) -> None:
    preferred_lang = session.get("language")
    session.clear()
    if preferred_lang:
        session["language"] = preferred_lang
    token = register_active_session(user["username"])
    session["user"] = {
        "username": user.get("username", ""),
        "role": user.get("role", "1"),
        "registered_at": user.get("registered_at", ""),
        "expiry_date": user.get("expiry_date", ""),
        "ms_id": ms_id,
        "display_name": display_name,
        "email": email,
        "is_local": True,
    }
    session["session_token"] = token


def start_ms_session(ms_user: Dict[str, str], *, linked_username: str = "") -> None:
    preferred_lang = session.get("language")
    session.clear()
    if preferred_lang:
        session["language"] = preferred_lang
    token = register_active_session(ms_user.get("ms_id", ""))
    session_user = build_session_user(ms_user)
    session_user["is_local"] = False
    session_user["linked_username"] = linked_username
    session["user"] = session_user
    session["session_token"] = token


def is_ms_configured() -> bool:
    return bool(MS_CLIENT_ID and MS_CLIENT_SECRET)


def build_msal_app() -> msal.ConfidentialClientApplication:
    return msal.ConfidentialClientApplication(
        MS_CLIENT_ID,
        authority=MS_AUTHORITY,
        client_credential=MS_CLIENT_SECRET,
    )


def fetch_ms_profile(token_result: Dict[str, str]) -> Dict[str, str]:
    claims = token_result.get("id_token_claims") or {}
    profile: Dict[str, str] = {
        "ms_id": claims.get("oid") or claims.get("sub") or "",
        "tenant_id": claims.get("tid") or "",
        "email": claims.get("preferred_username") or claims.get("email") or "",
        "display_name": claims.get("name") or "",
        "role": "1",
    }

    access_token = token_result.get("access_token")
    if access_token:
        try:
            response = requests.get(
                MS_GRAPH_ME_URL,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10,
            )
            if response.ok:
                payload = response.json()
                profile["ms_id"] = profile["ms_id"] or payload.get("id", "")
                profile["display_name"] = payload.get("displayName") or profile["display_name"]
                profile["email"] = payload.get("mail") or payload.get("userPrincipalName") or profile["email"]
        except requests.RequestException:
            pass

    return profile


def build_session_user(record: Dict[str, str]) -> Dict[str, str]:
    email = record.get("email", "")
    username = record.get("ms_id", "")
    # Prefer user-entered first/last name over MS-provided display_name
    first = record.get("first_name", "").strip()
    last = record.get("last_name", "").strip()
    if first or last:
        display_name = f"{first} {last}".strip()
    else:
        display_name = (record.get("display_name", "") or "").strip()
    return {
        "username": username,
        "ms_id": record.get("ms_id", ""),
        "email": email,
        "display_name": display_name,
        "first_name": record.get("first_name", ""),
        "last_name": record.get("last_name", ""),
        "role": record.get("role", "1") or "1",
    }


def load_ms_users() -> List[Dict[str, str]]:
    with db_session() as db:
        users = db.query(MsUser).order_by(MsUser.ms_id.asc()).all()
        return [
            {
                "ms_id": user.ms_id,
                "tenant_id": user.tenant_id or "",
                "email": user.email or "",
                "display_name": user.display_name or "",
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "school": user.school or "",
                "grade": user.grade or "",
                "role": user.role or "1",
                "created_at": user.created_at.isoformat() if user.created_at else "",
                "updated_at": user.updated_at.isoformat() if user.updated_at else "",
            }
            for user in users
        ]



def get_ms_user(ms_id: str) -> Optional[Dict[str, str]]:
    if not ms_id:
        return None
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return None
        return {
            "ms_id": user.ms_id,
            "tenant_id": user.tenant_id or "",
            "email": user.email or "",
            "display_name": user.display_name or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
            "grade": user.grade or "",
            "role": user.role or "1",
            "password": user.password or "",
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def get_ms_user_by_email(email: str) -> Optional[Dict[str, str]]:
    """Look up a Microsoft user by email address."""
    if not email:
        return None
    with db_session() as db:
        user = db.query(MsUser).filter(MsUser.email == email).first()
        if not user:
            return None
        return {
            "ms_id": user.ms_id,
            "tenant_id": user.tenant_id or "",
            "email": user.email or "",
            "display_name": user.display_name or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
            "grade": user.grade or "",
            "role": user.role or "1",
            "password": user.password or "",
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def update_ms_user_password(ms_id: str, password: str) -> bool:
    """Set or update the password for an MS user."""
    hashed = hash_password(password)
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return False
        user.password = hashed
        user.updated_at = datetime.utcnow()
        return True


def upsert_ms_user(profile: Dict[str, str]) -> Dict[str, str]:
    ms_id = profile.get("ms_id", "")
    now = datetime.utcnow()
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            user = MsUser(ms_id=ms_id, created_at=now)
            db.add(user)
        user.tenant_id = profile.get("tenant_id", "") or user.tenant_id
        user.email = profile.get("email", "") or user.email
        user.display_name = profile.get("display_name", "") or user.display_name
        user.role = user.role or "1"
        user.updated_at = now
        return {
            "ms_id": user.ms_id,
            "tenant_id": user.tenant_id or "",
            "email": user.email or "",
            "display_name": user.display_name or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
            "grade": user.grade or "",
            "role": user.role or "1",
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def update_ms_user(ms_id: str, updates: Dict[str, str]) -> Optional[Dict[str, str]]:
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return None
        for key, value in updates.items():
            if key in MS_USER_FIELDS:
                setattr(user, key, value)
        if user.first_name or user.last_name:
            user.display_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
        user.updated_at = datetime.utcnow()
        return {
            "ms_id": user.ms_id,
            "tenant_id": user.tenant_id or "",
            "email": user.email or "",
            "display_name": user.display_name or "",
            "first_name": user.first_name or "",
            "last_name": user.last_name or "",
            "school": user.school or "",
            "grade": user.grade or "",
            "role": user.role or "1",
            "created_at": user.created_at.isoformat() if user.created_at else "",
            "updated_at": user.updated_at.isoformat() if user.updated_at else "",
        }


def update_ms_user_role(ms_id: str, role: str) -> bool:
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return False
        user.role = role
        user.updated_at = datetime.utcnow()
        return True


def delete_ms_user(ms_id: str) -> bool:
    with db_session() as db:
        user = db.get(MsUser, ms_id)
        if not user:
            return False
        db.delete(user)
        return True


def is_profile_complete(record: Dict[str, str]) -> bool:
    return bool(
        record.get("first_name")
        and record.get("last_name")
    )


def get_active_user() -> Optional[Dict[str, str]]:
    user = session.get("user")
    if not user:
        return None
    username = user.get("username", "")
    token = session.get("session_token")
    if not username or not token:
        session.clear()
        return None
    if not refresh_session(username, token):
        session.clear()
        return None
    return user


def require_login(level: int = 1) -> Optional[Dict[str, str]]:
    user = session.get("user")
    
    def _fail_login(msg):
        if request.method == "GET":
            session["next"] = request.url
        flash(msg, "warning")
        return None

    if not user:
        return _fail_login(_("Please sign in first."))
    username = user.get("username", "")
    token = session.get("session_token")
    if not username or not token:
        session.clear()
        return _fail_login(_("Session expired. Please sign in again."))
    if not refresh_session(username, token):
        session.clear()
        return _fail_login(_("Session timed out. Please sign in again."))
    try:
        role = int(user.get("role", "1"))
    except ValueError:
        role = 1
    if role < level:
        flash(_("You do not have access to that action."), "danger")
        return None
    return user


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, iterations_raw, salt_b64, hash_b64 = encoded.split("$", 3)
    except ValueError:
        return False
    if scheme != PASSWORD_SCHEME:
        return False
    try:
        iterations = int(iterations_raw)
    except ValueError:
        return False
    try:
        salt = base64.b64decode(salt_b64)
        stored_hash = base64.b64decode(hash_b64)
    except (ValueError, binascii.Error, TypeError):
        return False

    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
        dklen=len(stored_hash),
    )
    return hmac.compare_digest(dk, stored_hash)


def load_sessions() -> Dict[str, Dict[str, str]]:
    with db_session() as db:
        rows = db.query(SessionModel).all()
        return {r.username: {"token": r.token or "", "last_seen": r.last_seen or ""} for r in rows}


def is_session_expired(entry: Dict[str, str]) -> bool:
    last_seen = entry.get("last_seen")
    if not last_seen:
        return True
    try:
        timestamp = datetime.fromisoformat(last_seen)
    except ValueError:
        return True
    return datetime.utcnow() - timestamp > SESSION_TIMEOUT


def ensure_login_available(username: str) -> Tuple[bool, str]:
    sessions = load_sessions()
    entry = sessions.get(username)
    if not entry:
        return True, ""
    if is_session_expired(entry):
        with db_session() as db:
            db.query(SessionModel).filter(SessionModel.username == username).delete()
            db.commit()
        return True, ""
    minutes = max(1, SESSION_TIMEOUT_SECONDS // 60)
    return False, _(
        "This account is already signed in. Please sign out from the other session or wait %(minutes)d minutes.",
        minutes=minutes,
    )


def register_active_session(username: str) -> str:
    token = uuid4().hex
    now = datetime.utcnow().isoformat()
    with db_session() as db:
        existing = db.query(SessionModel).filter(SessionModel.username == username).first()
        if existing:
            existing.token = token
            existing.last_seen = now
        else:
            db.add(SessionModel(username=username, token=token, last_seen=now))
        db.commit()
    return token


def release_active_session(username: str, token: Optional[str]) -> None:
    if not username:
        return
    with db_session() as db:
        entry = db.query(SessionModel).filter(SessionModel.username == username).first()
        if entry and (token is None or entry.token == token):
            db.delete(entry)
            db.commit()


def force_release_session(username: str) -> None:
    if not username:
        return
    with db_session() as db:
        db.query(SessionModel).filter(SessionModel.username == username).delete()
        db.commit()


def refresh_session(username: str, token: str) -> bool:
    with db_session() as db:
        entry = db.query(SessionModel).filter(SessionModel.username == username).first()
        if not entry or entry.token != token or is_session_expired({
            "token": entry.token or "",
            "last_seen": entry.last_seen or "",
        }):
            if entry:
                db.delete(entry)
                db.commit()
            return False
        entry.last_seen = datetime.utcnow().isoformat()
        db.commit()
        return True
