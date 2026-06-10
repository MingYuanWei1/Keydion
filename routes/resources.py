"""Public + admin routes for Academic Resources."""
from flask import (
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from flask_babel import gettext as _

from config import RESOURCES_DIR
from services.auth import require_login
from services.resources import (
    _can_view_node,
    _resource_viewer_role,
    create_resource_folder,
    delete_resource_node,
    effective_min_role,
    get_resource_node,
    load_resource_children,
    load_resource_folder_tree,
    move_resource_node,
    resource_breadcrumb_paths,
    resource_breadcrumbs,
    resource_name_is_valid,
    resource_slug_conflict,
    resolve_resource_path,
    save_resource_file,
    slugify_resource_name,
    update_resource_node,
)


def register_routes(app):

    # ==================== ACADEMIC RESOURCES (public) ====================

    @app.route("/resources")
    @app.route("/resources/<path:slug_path>")
    def resources(slug_path=None):
        viewer_role = _resource_viewer_role()
        if viewer_role is None:
            require_login()
            return redirect(url_for("login"))
        node = None
        if slug_path is not None:
            node = resolve_resource_path(slug_path)
            if node is None:
                abort(404)
            if not _can_view_node(effective_min_role(node["id"]), viewer_role):
                abort(403)
            if node["node_type"] == "file":
                stored = node["stored_filename"]
                if not stored or not (RESOURCES_DIR / stored).exists():
                    abort(404)
                as_attachment = (request.args.get("download") or "").lower() in ("1", "true", "yes")
                return send_from_directory(
                    RESOURCES_DIR, stored,
                    as_attachment=as_attachment,
                    download_name=node["original_filename"] or stored,
                    mimetype=node["mime_type"] or None,
                )
        folder_id = node["id"] if node else None
        breadcrumbs = resource_breadcrumb_paths(folder_id) if folder_id else []
        base_path = breadcrumbs[-1]["path"] if breadcrumbs else ""
        children = load_resource_children(folder_id, viewer_role)
        for c in children:
            c["path"] = (base_path + "/" + c["slug"]).strip("/") if base_path else c["slug"]
        return render_template("resources.html", current=node,
                               children=children, breadcrumbs=breadcrumbs)

    # ==================== ACADEMIC RESOURCES (admin) ====================

    def _resources_redirect(folder_id):
        if folder_id:
            return redirect(url_for("admin_resources_manage", folder_id=folder_id))
        return redirect(url_for("admin_resources_manage"))

    @app.route("/dashboard/admin/resources")
    @app.route("/dashboard/admin/resources/<int:folder_id>")
    def admin_resources_manage(folder_id=None):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        current = get_resource_node(folder_id) if folder_id else None
        if folder_id and (not current or current["node_type"] != "folder"):
            flash(_("Folder not found."), "warning")
            return redirect(url_for("admin_resources_manage"))
        children = load_resource_children(folder_id, viewer_role=3)
        breadcrumbs = resource_breadcrumbs(folder_id) if folder_id else []
        return render_template("resource_manage.html", user=user, current=current,
                               children=children, breadcrumbs=breadcrumbs,
                               parent_id=folder_id, move_targets=load_resource_folder_tree())

    @app.route("/dashboard/admin/resources/folder", methods=["POST"], endpoint="admin_resources_folder_new")
    def admin_resources_folder_new():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        parent_id = request.form.get("parent_id", type=int)
        name = (request.form.get("name") or "").strip()
        if not name:
            flash(_("Folder name is required."), "warning")
        elif not resource_name_is_valid(name):
            flash(_("Names may only use English letters, numbers, spaces, and . _ -"), "warning")
        elif resource_slug_conflict(parent_id, slugify_resource_name(name)):
            flash(_("An item with that name already exists in this folder."), "warning")
        else:
            create_resource_folder(parent_id, name,
                                   request.form.get("min_role", type=int) or 1,
                                   request.form.get("description", ""))
            flash(_("Folder created."), "success")
        return _resources_redirect(parent_id)

    @app.route("/dashboard/admin/resources/upload", methods=["POST"], endpoint="admin_resources_upload")
    def admin_resources_upload():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        parent_id = request.form.get("parent_id", type=int)
        f = request.files.get("file")
        if not f or not f.filename:
            flash(_("Please choose a file."), "warning")
            return _resources_redirect(parent_id)
        name = request.form.get("name", "")
        effective = (name or "").strip() or (f.filename or "")
        if not resource_name_is_valid(effective):
            flash(_("Names may only use English letters, numbers, spaces, and . _ -"), "warning")
        elif resource_slug_conflict(parent_id, slugify_resource_name(effective)):
            flash(_("An item with that name already exists in this folder."), "warning")
        else:
            _id, err = save_resource_file(parent_id, f, name,
                                          request.form.get("description", ""),
                                          request.form.get("min_role", type=int) or 1)
            flash(err or _("File uploaded."), "warning" if err else "success")
        return _resources_redirect(parent_id)

    @app.route("/dashboard/admin/resources/<int:node_id>/edit", methods=["POST"], endpoint="admin_resources_edit")
    def admin_resources_edit(node_id):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        node = get_resource_node(node_id)
        if not node:
            flash(_("Item not found."), "warning")
            return redirect(url_for("admin_resources_manage"))
        name = (request.form.get("name") or "").strip()
        if name and not resource_name_is_valid(name):
            flash(_("Names may only use English letters, numbers, spaces, and . _ -"), "warning")
            return _resources_redirect(node["parent_id"])
        if name and resource_slug_conflict(node["parent_id"], slugify_resource_name(name), exclude_id=node_id):
            flash(_("An item with that name already exists in this folder."), "warning")
            return _resources_redirect(node["parent_id"])
        update_resource_node(node_id, request.form.get("name", ""),
                             request.form.get("description", ""),
                             request.form.get("min_role", type=int) or node["min_role"])
        flash(_("Saved."), "success")
        return _resources_redirect(node["parent_id"])

    @app.route("/dashboard/admin/resources/<int:node_id>/move", methods=["POST"], endpoint="admin_resources_move")
    def admin_resources_move(node_id):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        node = get_resource_node(node_id)
        if not node:
            flash(_("Item not found."), "warning")
            return redirect(url_for("admin_resources_manage"))
        dest = request.form.get("new_parent_id", type=int)
        new_parent_id = dest if dest else None
        if resource_slug_conflict(new_parent_id, node["slug"], exclude_id=node_id):
            flash(_("An item with that name already exists in the destination."), "warning")
            return _resources_redirect(node["parent_id"])
        ok, err = move_resource_node(node_id, new_parent_id)
        flash(err or _("Moved."), "warning" if err else "success")
        return _resources_redirect(new_parent_id)

    @app.route("/dashboard/admin/resources/<int:node_id>/delete", methods=["POST"], endpoint="admin_resources_delete")
    def admin_resources_delete(node_id):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        node = get_resource_node(node_id)
        parent_id = node["parent_id"] if node else None
        delete_resource_node(node_id)
        flash(_("Deleted."), "success")
        return _resources_redirect(parent_id)

    @app.route("/admin/resources", endpoint="admin_resources_manage_legacy")
    def admin_resources_manage_legacy():
        return redirect(url_for("admin_resources_manage"), code=301)
