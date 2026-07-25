"""Public + admin routes for Guides."""
from datetime import datetime
from uuid import uuid4

from flask import (
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_babel import gettext as _
from werkzeug.utils import secure_filename

from config import (
    ALLOWED_IMAGE_EXTENSIONS,
    GUIDE_IMAGE_MAX_BYTES,
    GUIDE_IMAGES_DIR,
)
from db import db_session
from models import GuideModel
from services.auth import require_login
from services.guides import (
    _group_guides_for_index,
    _load_guide_categories,
    _order_guides_for_index,
    _read_guide_form,
    _sanitize_guide_html,
    _slugify,
    delete_guide,
    get_guide,
    get_guide_by_slug,
    load_guides,
    save_guide,
    slug_exists,
    update_guide,
)


def register_routes(app):

    # ==================== GUIDE ROUTES ====================

    @app.route("/guides")
    def guides():
        all_guides = load_guides(published_only=True)
        grouped = _group_guides_for_index(all_guides, _load_guide_categories())
        return render_template("guides.html", grouped=grouped, total=len(all_guides))

    @app.route("/guides/<slug>")
    def guide_article(slug):
        guide = get_guide_by_slug(slug)
        if not guide or not guide.get("published"):
            abort(404)
        # Compute prev/next from the same ordered list the index uses.
        flat = _order_guides_for_index(
            load_guides(published_only=True),
            _load_guide_categories(),
        )
        idx = next((i for i, g in enumerate(flat) if g.get("slug") == slug), -1)
        prev_guide = flat[idx - 1] if idx > 0 else None
        next_guide = flat[idx + 1] if 0 <= idx < len(flat) - 1 else None
        return render_template(
            "guide_article.html",
            guide=guide,
            prev_guide=prev_guide,
            next_guide=next_guide,
            preview_mode=False,
        )

    @app.route("/dashboard/admin/guides/upload-image", methods=["POST"], endpoint="admin_guides_upload_image")
    def admin_guide_upload_image():
        user = require_login(level=3)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        img_file = request.files.get("file")
        if not img_file or not img_file.filename:
            return jsonify({"error": "No file provided"}), 400
        img_file.stream.seek(0, 2)  # seek to end to measure size
        size = img_file.stream.tell()
        img_file.stream.seek(0)
        if size > GUIDE_IMAGE_MAX_BYTES:
            return jsonify({"error": "File too large"}), 400
        ext = img_file.filename.rsplit(".", 1)[-1].lower() if "." in img_file.filename else ""
        if ext not in ALLOWED_IMAGE_EXTENSIONS:
            return jsonify({"error": "Invalid image format"}), 400
        GUIDE_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        unique_name = f"{uuid4().hex[:12]}_{secure_filename(img_file.filename)}"
        img_file.save(GUIDE_IMAGES_DIR / unique_name)
        img_url = url_for("static", filename=f"uploads/guides/{unique_name}")
        return jsonify({"url": img_url})

    @app.route("/dashboard/admin/guides/new", methods=["GET", "POST"], endpoint="admin_guide_new")
    @app.route("/dashboard/admin/guides/<int:guide_id>/edit", methods=["GET", "POST"], endpoint="admin_guide_edit")
    def admin_guide_publish(guide_id: int = None):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))

        editing = guide_id is not None
        guide = get_guide(guide_id) if editing else None
        if editing and not guide:
            flash(_("Guide not found."), "warning")
            return redirect(url_for("admin_guides_manage"))

        form_data = {
            "slug": (guide or {}).get("slug", ""),
            "category": (guide or {}).get("category", ""),
            "sort_order": (guide or {}).get("sort_order", 100),
            "published": bool((guide or {}).get("published", False)),
            "title_en": (guide or {}).get("title_en", ""),
            "title_zh": (guide or {}).get("title_zh", ""),
            "summary_en": (guide or {}).get("summary_en", ""),
            "summary_zh": (guide or {}).get("summary_zh", ""),
            "body_en": (guide or {}).get("body_en", ""),
            "body_zh": (guide or {}).get("body_zh", ""),
        }

        if request.method == "POST":
            form_data = _read_guide_form(request.form)
            # Auto-generate slug if blank
            if not form_data["slug"]:
                form_data["slug"] = _slugify(form_data["title_en"] or form_data["title_zh"])
            else:
                form_data["slug"] = _slugify(form_data["slug"])

            error = None
            if not form_data["title_en"] and not form_data["title_zh"]:
                error = _("Please enter a title in at least one language.")
            elif not form_data["slug"]:
                error = _("Please enter a slug.")
            elif slug_exists(form_data["slug"], exclude_id=guide_id or 0):
                error = _("That slug is already taken. Pick another.")

            if error:
                flash(error, "warning")
            else:
                if editing:
                    update_guide(guide_id, form_data)
                    flash(_("Guide updated."), "success")
                else:
                    save_guide(form_data)
                    flash(_("Guide published."), "success")
                return redirect(url_for("admin_guides_manage"))

        return render_template(
            "guide_publish.html",
            form_data=form_data,
            categories=_load_guide_categories(),
            editing=editing,
            guide_id=guide_id,
            user=user,
        )

    @app.route("/dashboard/admin/guides")
    def admin_guides_manage():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        guides = load_guides(published_only=False)
        return render_template("guide_manage.html", guides=guides, user=user)

    @app.route("/dashboard/admin/guides/<int:guide_id>/delete", methods=["POST"])
    def admin_guide_delete(guide_id: int):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        if delete_guide(guide_id):
            flash(_("Guide deleted."), "success")
        else:
            flash(_("Guide not found."), "warning")
        return redirect(url_for("admin_guides_manage"))

    @app.route("/dashboard/admin/guides/reorder", methods=["POST"], endpoint="admin_guides_reorder")
    def admin_guides_reorder():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.get_json(silent=True) or {}
        items = data.get("items") or []
        with db_session() as db:
            for it in items:
                try:
                    gid = int(it.get("id"))
                except (TypeError, ValueError):
                    continue
                g = db.query(GuideModel).filter_by(id=gid).first()
                if not g:
                    continue
                try:
                    g.sort_order = int(it.get("sort_order"))
                except (TypeError, ValueError):
                    pass
                if "category" in it:
                    g.category = (it.get("category") or "").strip()
                g.updated_at = datetime.utcnow().isoformat()
            db.commit()
        return jsonify(ok=True)

    @app.route("/dashboard/admin/guides/<int:guide_id>/toggle", methods=["POST"], endpoint="admin_guide_toggle_published")
    def admin_guide_toggle_published(guide_id: int):
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.get_json(silent=True) or {}
        with db_session() as db:
            g = db.query(GuideModel).filter_by(id=guide_id).first()
            if not g:
                return jsonify(error="not found"), 404
            if "published" in data:
                g.published = bool(data["published"])
            else:
                g.published = not bool(g.published)
            g.updated_at = datetime.utcnow().isoformat()
            new_state = bool(g.published)
            db.commit()
        return jsonify(ok=True, published=new_state)

    @app.route("/dashboard/admin/guides/preview", methods=["POST"], endpoint="admin_guide_preview")
    def admin_guide_preview():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        data = _read_guide_form(request.form)
        # Sanitize bodies the same way the persisted save path would, so the
        # preview reflects exactly what would end up in the DB.
        data["body_en"] = _sanitize_guide_html(data.get("body_en", ""))
        data["body_zh"] = _sanitize_guide_html(data.get("body_zh", ""))
        guide = {
            "slug": data["slug"] or "preview",
            "category": data["category"],
            "title_en": data["title_en"],
            "title_zh": data["title_zh"],
            "summary_en": data["summary_en"],
            "summary_zh": data["summary_zh"],
            "body_en": data["body_en"],
            "body_zh": data["body_zh"],
            "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
            "published": data["published"],
        }
        return render_template("guide_article.html",
            guide=guide,
            prev_guide=None,
            next_guide=None,
            preview_mode=True,
        )

    @app.route("/admin/guides", endpoint="admin_guides_manage_legacy")
    def admin_guides_manage_legacy():
        return redirect(url_for("admin_guides_manage"), code=301)

    @app.route("/admin/guides/new", endpoint="admin_guide_new_legacy")
    def admin_guide_new_legacy():
        return redirect(url_for("admin_guide_new"), code=301)

    @app.route("/admin/guides/<int:guide_id>/edit", endpoint="admin_guide_edit_legacy")
    def admin_guide_edit_legacy(guide_id):
        return redirect(url_for("admin_guide_edit", guide_id=guide_id), code=301)
