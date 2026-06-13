"""Public + admin routes for Journals."""
from datetime import datetime
from uuid import uuid4

from flask import (
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_babel import gettext as _

from config import (
    ALLOWED_IMAGE_EXTENSIONS,
    JOURNAL_COVERS_DIR,
)
from services.auth import get_active_user, require_login
from services.journals import (
    get_journal_by_id,
    get_journal_by_slug,
    get_journal_paper_counts,
    load_journals,
    save_journals,
    set_unique_slug,
)
from services.papers import (
    gather_paper_records,
    load_paper_metadata,
    save_paper_metadata,
)


def register_routes(app):

    # ==================== JOURNALS ROUTES ====================

    @app.route("/dashboard/admin/journals/add", methods=["POST"], endpoint="admin_journals_add")
    def journal_add():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Journal name is required."))), 400
        journals = load_journals()
        existing_names = [j["name"] for j in journals]
        if name in existing_names:
            return jsonify(error=str(_("Journal already exists."))), 409
        new_id = uuid4().hex[:12]
        new_journal = {
            "id": new_id,
            "name": name,
            "slug": set_unique_slug(name, fallback=new_id),
            "cover_image": "",
            "introduction": "",
            "created_at": datetime.utcnow().date().isoformat(),
        }
        journals.append(new_journal)
        save_journals(journals)
        return jsonify(items=journals)

    @app.route("/dashboard/admin/journals/delete", methods=["POST"], endpoint="admin_journals_delete")
    def journal_delete():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        journal_id = (request.json or {}).get("id", "").strip()
        if not journal_id:
            return jsonify(error=str(_("Journal ID is required."))), 400
        journals = load_journals()
        journal = next((j for j in journals if j["id"] == journal_id), None)
        if not journal:
            return jsonify(error=str(_("Journal not found."))), 404
        # Clear journal field from papers
        old_name = journal["name"]
        meta_rows = load_paper_metadata()
        changed = False
        for row in meta_rows:
            if row.get("journal") == old_name:
                row["journal"] = ""
                changed = True
        if changed:
            save_paper_metadata(meta_rows)
        journals = [j for j in journals if j["id"] != journal_id]
        save_journals(journals)
        return jsonify(items=journals)

    @app.route("/dashboard/admin/journals", endpoint="admin_journals_manage")
    def journals_manage():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        journals = load_journals()
        counts = get_journal_paper_counts()
        for j in journals:
            j["paper_count"] = counts.get(j["name"], 0)
        return render_template("journal_manage.html", user=user, journals=journals)

    @app.route("/dashboard/admin/journal/<journal_id>/edit", methods=["GET", "POST"], endpoint="admin_journal_edit")
    def journal_edit(journal_id):
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        journal = get_journal_by_id(journal_id)
        if not journal:
            flash(_("Journal not found."), "warning")
            return redirect(url_for("admin_journals_manage"))

        if request.method == "POST":
            old_name = journal["name"]
            new_name = request.form.get("name", "").strip()
            introduction = request.form.get("introduction", "").strip()

            if not new_name:
                flash(_("Journal name is required."), "danger")
                return redirect(url_for("admin_journal_edit", journal_id=journal_id))

            journals = load_journals()
            for j in journals:
                if j["id"] == journal_id:
                    j["name"] = new_name
                    j["introduction"] = introduction
                    j["slug"] = set_unique_slug(new_name, exclude_id=journal_id, fallback=journal_id)

                    # Handle cover image upload
                    cover_file = request.files.get("cover_image")
                    if cover_file and cover_file.filename:
                        ext = cover_file.filename.rsplit(".", 1)[-1].lower()
                        if ext in ALLOWED_IMAGE_EXTENSIONS:
                            JOURNAL_COVERS_DIR.mkdir(parents=True, exist_ok=True)
                            cover_filename = f"journal_{journal_id}.{ext}"
                            cover_file.save(JOURNAL_COVERS_DIR / cover_filename)
                            j["cover_image"] = cover_filename
                    break

            save_journals(journals)

            # Update paper metadata if name changed
            if old_name != new_name:
                meta_rows = load_paper_metadata()
                changed = False
                for row in meta_rows:
                    if row.get("journal") == old_name:
                        row["journal"] = new_name
                        changed = True
                if changed:
                    save_paper_metadata(meta_rows)

            flash(_("Journal updated."), "success")
            return redirect(url_for("admin_journal_edit", journal_id=journal_id))

        # GET: load papers belonging to this journal + the full pool for the picker
        all_papers = gather_paper_records()
        journal_papers = [p for p in all_papers if p.get("journal") == journal["name"]]
        journal_papers.sort(key=lambda r: r.get("published_at") or "", reverse=True)
        journal_paper_filenames = [p.get("filename") for p in journal_papers]

        return render_template("journal_edit.html", user=user, journal=journal,
                               all_papers=all_papers,
                               journal_paper_filenames=journal_paper_filenames)

    @app.route("/dashboard/admin/journal/<journal_id>/papers", methods=["POST"], endpoint="admin_journal_papers")
    def journal_papers(journal_id):
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        journal = get_journal_by_id(journal_id)
        if not journal:
            return jsonify(error=str(_("Journal not found."))), 404
        desired = set((request.json or {}).get("filenames", []))
        name = journal["name"]
        meta_rows = load_paper_metadata()
        changed = False
        for row in meta_rows:
            fn = row.get("filename")
            if fn in desired and row.get("journal") != name:
                row["journal"] = name
                changed = True
            elif fn not in desired and row.get("journal") == name:
                row["journal"] = ""
                changed = True
        if changed:
            save_paper_metadata(meta_rows)
        return jsonify(ok=True)

    @app.route("/admin/journal/<journal_id>/edit", endpoint="admin_journal_edit_legacy")
    def admin_journal_edit_legacy(journal_id):
        return redirect(url_for("admin_journal_edit", journal_id=journal_id), code=301)

    # ---------- Public journal pages ----------
    @app.route("/journals")
    def journal_list_page():
        journals = load_journals()
        counts = get_journal_paper_counts()
        for j in journals:
            j["paper_count"] = counts.get(j["name"], 0)
        return render_template("journal_list.html", journals=journals)

    @app.route("/journals/<slug>")
    def journal_detail(slug):
        journal = get_journal_by_slug(slug)
        if not journal:
            flash(_("Journal not found."), "warning")
            return redirect(url_for("journal_list_page"))
        # Get papers in this journal
        all_papers = gather_paper_records()
        journal_papers = [p for p in all_papers if p.get("journal") == journal["name"]]
        journal_papers.sort(key=lambda r: r.get("published_at") or "", reverse=True)

        user = get_active_user()
        is_guest = user is None
        return render_template("journal_detail.html", journal=journal, papers=journal_papers, user=user, is_guest=is_guest)

    @app.route("/journal/<journal_id>", endpoint="journal_detail_legacy")
    def journal_detail_legacy(journal_id):
        journal = get_journal_by_id(journal_id)
        if not journal or not journal.get("slug"):
            return redirect(url_for("journal_list_page"), code=301)
        return redirect(url_for("journal_detail", slug=journal["slug"]), code=301)
