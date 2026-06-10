"""Academic Resources: folder/file tree services."""
import re
from datetime import datetime
from uuid import uuid4

from flask_babel import gettext as _

from config import (
    OPEN_ACCESS,
    PREVIEWABLE_MIMES,
    RESOURCE_ALLOWED_EXTENSIONS,
    RESOURCE_MAX_BYTES,
    RESOURCES_DIR,
)
from db import db_session
from models import ResourceNode
from services.auth import get_active_user


def _resource_node_to_dict(r) -> dict:
    return {
        "id": r.id,
        "parent_id": r.parent_id,
        "node_type": r.node_type,
        "name": r.name,
        "stored_filename": r.stored_filename,
        "original_filename": r.original_filename,
        "mime_type": r.mime_type,
        "size_bytes": r.size_bytes,
        "description": r.description or "",
        "min_role": r.min_role or 1,
        "is_previewable": bool(r.mime_type and r.mime_type in PREVIEWABLE_MIMES),
        "slug": slugify_resource_name(r.name),
    }


def _can_view_node(eff_min_role: int, viewer_role) -> bool:
    """viewer_role: int role for a logged-in user, 0 for an OPEN_ACCESS guest,
    None for no access at all."""
    if viewer_role is None:
        return False
    if viewer_role == 0:               # OPEN_ACCESS guest
        return eff_min_role <= 1
    return viewer_role >= eff_min_role


def _resource_viewer_role():
    """Effective viewer role: user's int role, 0 for an OPEN_ACCESS guest,
    or None when access is denied entirely."""
    user = get_active_user()
    if user:
        try:
            return int(user.get("role", "1"))
        except (TypeError, ValueError):
            return 1
    if OPEN_ACCESS:
        return 0
    return None


def get_resource_node(node_id: int):
    with db_session() as db:
        n = db.query(ResourceNode).filter_by(id=node_id).first()
        return _resource_node_to_dict(n) if n else None


def effective_min_role(node_id: int) -> int:
    """Max min_role over a node and all its ancestors."""
    with db_session() as db:
        eff = 1
        seen = set()
        current = db.query(ResourceNode).filter_by(id=node_id).first()
        while current is not None and current.id not in seen:
            seen.add(current.id)
            eff = max(eff, current.min_role or 1)
            if current.parent_id is None:
                break
            current = db.query(ResourceNode).filter_by(id=current.parent_id).first()
        return eff


def resource_breadcrumbs(node_id: int) -> list:
    crumbs = []
    with db_session() as db:
        seen = set()
        current = db.query(ResourceNode).filter_by(id=node_id).first()
        while current is not None and current.id not in seen:
            seen.add(current.id)
            crumbs.append({"id": current.id, "name": current.name})
            if current.parent_id is None:
                break
            current = db.query(ResourceNode).filter_by(id=current.parent_id).first()
    crumbs.reverse()
    return crumbs


def load_resource_children(parent_id, viewer_role) -> list:
    """Visible children of parent_id (None = root), folders first then alpha."""
    parent_eff = effective_min_role(parent_id) if parent_id else 1
    with db_session() as db:
        rows = db.query(ResourceNode).filter_by(parent_id=parent_id).all()
        children = []
        for r in rows:
            eff = max(parent_eff, r.min_role or 1)
            if not _can_view_node(eff, viewer_role):
                continue
            children.append(_resource_node_to_dict(r))
    children.sort(key=lambda c: (0 if c["node_type"] == "folder" else 1, (c["name"] or "").lower()))
    return children


def load_resource_folder_tree() -> list:
    """Flat, depth-ordered list of all folders for the move/destination picker."""
    with db_session() as db:
        rows = db.query(ResourceNode).filter_by(node_type="folder").all()
        nodes = [_resource_node_to_dict(r) for r in rows]
    by_parent = {}
    for n in nodes:
        by_parent.setdefault(n["parent_id"], []).append(n)
    for lst in by_parent.values():
        lst.sort(key=lambda c: (c["name"] or "").lower())
    out = []

    def walk(parent_id, depth):
        for n in by_parent.get(parent_id, []):
            out.append({"id": n["id"], "name": n["name"], "depth": depth})
            walk(n["id"], depth + 1)

    walk(None, 0)
    return out


def _clamp_role(value, default=1) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        return default
    return v if v in (1, 2, 3) else default


def create_resource_folder(parent_id, name, min_role=1, description="") -> int:
    with db_session() as db:
        node = ResourceNode(
            parent_id=parent_id, node_type="folder", name=(name or "").strip(),
            min_role=_clamp_role(min_role), description=(description or "").strip(),
            created_at=datetime.utcnow().isoformat(),
        )
        db.add(node)
        db.commit()
        return node.id


def save_resource_file(parent_id, file_storage, name, description, min_role):
    """Returns (node_id, None) on success or (None, error_message)."""
    filename = file_storage.filename or ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in RESOURCE_ALLOWED_EXTENSIONS:
        return None, _("Unsupported file type.")
    file_storage.stream.seek(0, 2)
    size = file_storage.stream.tell()
    file_storage.stream.seek(0)
    if size > RESOURCE_MAX_BYTES:
        return None, _("File is too large.")
    RESOURCES_DIR.mkdir(parents=True, exist_ok=True)
    stored = f"{uuid4().hex[:12]}.{ext}"
    file_storage.save(RESOURCES_DIR / stored)
    display_name = (name or "").strip() or filename
    with db_session() as db:
        node = ResourceNode(
            parent_id=parent_id, node_type="file", name=display_name,
            stored_filename=stored, original_filename=filename,
            mime_type=RESOURCE_ALLOWED_EXTENSIONS[ext], size_bytes=size,
            description=(description or "").strip(), min_role=_clamp_role(min_role),
            created_at=datetime.utcnow().isoformat(),
        )
        db.add(node)
        db.commit()
        return node.id, None


def update_resource_node(node_id, name, description, min_role) -> bool:
    with db_session() as db:
        n = db.query(ResourceNode).filter_by(id=node_id).first()
        if not n:
            return False
        if (name or "").strip():
            n.name = name.strip()
        n.description = (description or "").strip()
        n.min_role = _clamp_role(min_role, default=n.min_role or 1)
        db.commit()
        return True


def move_resource_node(node_id, new_parent_id):
    """Returns (True, None) on success or (False, error_message)."""
    with db_session() as db:
        node = db.query(ResourceNode).filter_by(id=node_id).first()
        if not node:
            return False, _("Item not found.")
        if new_parent_id is not None:
            if new_parent_id == node_id:
                return False, _("Cannot move a folder into itself.")
            target = db.query(ResourceNode).filter_by(id=new_parent_id).first()
            if not target or target.node_type != "folder":
                return False, _("Invalid destination.")
            # Walk up from target; if we reach node_id, target is a descendant.
            cursor, seen = target, set()
            while cursor is not None and cursor.id not in seen:
                seen.add(cursor.id)
                if cursor.id == node_id:
                    return False, _("Cannot move a folder into its own subfolder.")
                if cursor.parent_id is None:
                    break
                cursor = db.query(ResourceNode).filter_by(id=cursor.parent_id).first()
        node.parent_id = new_parent_id
        db.commit()
        return True, None


def delete_resource_node(node_id) -> bool:
    """Recursively delete a node, its descendant rows, and their blobs."""
    with db_session() as db:
        root = db.query(ResourceNode).filter_by(id=node_id).first()
        if not root:
            return False
        to_delete, queue = [], [root]
        while queue:
            n = queue.pop()
            to_delete.append(n)
            queue.extend(db.query(ResourceNode).filter_by(parent_id=n.id).all())
        for n in to_delete:
            if n.node_type == "file" and n.stored_filename:
                try:
                    (RESOURCES_DIR / n.stored_filename).unlink()
                except OSError:
                    pass
            db.delete(n)
        db.commit()
        return True


def slugify_resource_name(name: str) -> str:
    """Display name -> URL slug: lowercase, whitespace runs collapsed to '_'."""
    return re.sub(r"\s+", "_", (name or "").strip().lower())


_RESOURCE_SLUG_RE = re.compile(r"[a-z0-9_.-]+")


def resource_name_is_valid(name: str) -> bool:
    """Acceptable iff the slug is non-empty, only [a-z0-9_.-], and not a path
    token. Rejects names that can't form a clean URL segment (Chinese, '/', '?'…)."""
    slug = slugify_resource_name(name)
    if slug in ("", ".", ".."):
        return False
    return bool(_RESOURCE_SLUG_RE.fullmatch(slug))


def resource_slug_conflict(parent_id, slug, exclude_id=None) -> bool:
    """True if a sibling under parent_id already resolves to the same slug."""
    with db_session() as db:
        rows = db.query(ResourceNode).filter_by(parent_id=parent_id).all()
        for r in rows:
            if exclude_id is not None and r.id == exclude_id:
                continue
            if slugify_resource_name(r.name) == slug:
                return True
    return False


def resolve_resource_path(slug_path):
    """Resolve a '/'-joined slug path to a node dict, or None. Walks level by
    level matching each lowercased segment against children's slugified names.
    A file may only be the final segment."""
    segments = [s.strip().lower() for s in (slug_path or "").split("/") if s.strip()]
    if not segments:
        return None
    parent_id, node = None, None
    for i, seg in enumerate(segments):
        with db_session() as db:
            rows = db.query(ResourceNode).filter_by(parent_id=parent_id).all()
            match = next((r for r in rows if slugify_resource_name(r.name) == seg), None)
            if match is None:
                return None
            node = _resource_node_to_dict(match)
            parent_id = match.id
        if node["node_type"] == "file" and i != len(segments) - 1:
            return None  # can't descend through a file
    return node


def resource_breadcrumb_paths(node_id):
    """[{name, path}] root -> node_id, each with its cumulative slug path."""
    out, acc = [], []
    for c in resource_breadcrumbs(node_id):
        acc.append(slugify_resource_name(c["name"]))
        out.append({"name": c["name"], "path": "/".join(acc)})
    return out
