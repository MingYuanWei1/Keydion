"""Paper search, preview, serving, modify/delete, and manage routes."""
import json
from dataclasses import asdict

from flask import (
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask_babel import gettext as _
from werkzeug.routing import PathConverter

import llm_client
import rag_index
from config import (
    CP_ACTION_TYPES,
    CP_CRITERIA_DEFS,
    CP_GLOBAL_CONTEXTS,
    IB_EE_CRITERIA_DEFS,
    OPEN_ACCESS,
    PAPERS_DIR,
)
from routes.shared import paginate_records
from services.auth import get_active_user, require_login
from services.journals import get_journal_id_map, get_journal_names, get_journal_slug_map, load_journals
from services.papers import (
    _build_safe_paper_filename,
    _get_ee_subjects_list,
    _get_ia_subjects_list,
    _is_cp_paper,
    _is_ee_paper,
    _is_ia_paper,
    _matches_cp_context,
    _matches_ee_subject,
    _matches_ia_subject,
    build_cp_data_from_form,
    build_ia_data_from_form,
    build_ib_ee_data_from_form,
    build_preview_pdf,
    count_papers_using_ee_subject,
    count_papers_using_ia_subject,
    gather_paper_records,
    load_ee_subjects,
    load_ia_subjects,
    load_paper_categories,
    load_paper_metadata,
    reconcile_ee_subjects,
    reconcile_ia_subjects,
    remove_paper_metadata,
    rename_ee_subject_in_papers,
    rename_ia_subject_in_papers,
    resolve_contained,
    save_ee_subjects,
    save_ia_subjects,
    set_pdf_metadata,
    upsert_paper_metadata,
)
from services.search import _hybrid_search_records
from services.publishing_contracts import NotFound


class _LegacyPaperPathConverter(PathConverter):
    """Keep UUID-shaped identities out of filename compatibility rules."""

    _uuid = (
        r"[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-"
        r"[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}"
    )
    regex = rf"(?!(?:{_uuid})(?=(?:/(?:info|modify|delete))?$)).+?"


def _paper_library():
    return current_app.extensions["paper_library"]


def _current_paper_pdf(paper_id):
    try:
        return _paper_library().current_pdf(str(paper_id))
    except NotFound:
        abort(404)


def _paper_record_payload(record):
    payload = asdict(record)
    payload.pop("row_version", None)
    return payload


def _legacy_paper_document(filename):
    try:
        record = _paper_library().resolve_alias(filename)
        return _paper_library().current_pdf(record.paper_id)
    except NotFound:
        abort(404)


def register_routes(app):
    def legacy_paper_path_route(rule, **options):
        def decorate(view):
            original_path_converter = app.url_map.converters["path"]
            app.url_map.converters["path"] = _LegacyPaperPathConverter
            try:
                return app.route(rule, **options)(view)
            finally:
                app.url_map.converters["path"] = original_path_converter

        return decorate

    # ==================== PAPERS / SEARCH ROUTES ====================

    @app.route("/advanced-search")
    def advanced_search():
        user = get_active_user()
        is_guest = user is None
        journals = load_journals()
        return render_template("advanced_search.html", user=user, journals=journals,
                               ee_subjects_list=_get_ee_subjects_list(),
                               ia_subjects_list=_get_ia_subjects_list(),
                               cp_contexts=CP_GLOBAL_CONTEXTS)

    @app.route("/search", methods=["GET", "POST"])
    def search():
        user = get_active_user()
        is_guest = user is None

        if request.method == "POST":
            query_value = request.form.get("query", "").strip()
            if not query_value:
                flash(_("Enter a keyword to search."), "warning")
                return redirect(url_for("search"))
            return redirect(url_for("search", q=query_value))

        query = request.args.get("q", "").strip()
        category_filter = request.args.get("category", "").strip()
        language_filter = request.args.get("language", "").strip()
        date_filter = request.args.get("date", "").strip()
        author_filter = request.args.get("author", "").strip().lower()
        title_filter = request.args.get("title", "").strip().lower()
        start_year = request.args.get("start_year", "").strip()
        end_year = request.args.get("end_year", "").strip()
        journal_filter = request.args.get("journal", "").strip()
        paper_type_filter = request.args.get("paper_type", "").strip()
        ee_subject_filter = request.args.get("ee_subject", "").strip()
        cp_context_filter = request.args.get("cp_context", "").strip()
        ia_subject_filter = request.args.get("ia_subject", "").strip()

        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1

        per_page = 20
        filtered = bool(query) or bool(category_filter) or bool(language_filter) or bool(date_filter) or bool(author_filter) or bool(title_filter) or bool(start_year) or bool(end_year) or bool(journal_filter) or bool(paper_type_filter) or bool(ee_subject_filter) or bool(cp_context_filter) or bool(ia_subject_filter)

        visible_records = gather_paper_records(_paper_library())
        if query:
            visible_by_id = {
                record["paper_id"]: record for record in visible_records
            }
            record_pool = [
                visible_by_id[record["paper_id"]]
                for record in _hybrid_search_records(query)
                if record.get("paper_id") in visible_by_id
            ]
        else:
            record_pool = visible_records

        # Apply additional filters
        if category_filter:
            record_pool = [r for r in record_pool if r.get("category") == category_filter]
        if language_filter:
            record_pool = [r for r in record_pool if r.get("language") == language_filter]
        if date_filter:
            # Simple substring match for date filter (e.g., '2023', '2023-10')
            record_pool = [r for r in record_pool if (r.get("published_at") or "").startswith(date_filter)]

        if author_filter:
            record_pool = [r for r in record_pool if author_filter in (r.get("author_name") or "").lower()]
        if title_filter:
            record_pool = [r for r in record_pool if title_filter in (r.get("title") or "").lower() or title_filter in r.get("filename", "").lower()]

        if start_year:
            record_pool = [r for r in record_pool if str(r.get("published_at") or "")[:4] >= start_year]
        if end_year:
            record_pool = [r for r in record_pool if str(r.get("published_at") or "")[:4] <= end_year]

        if journal_filter:
            record_pool = [r for r in record_pool if r.get("journal") == journal_filter]

        if paper_type_filter:
            if paper_type_filter == "ee":
                record_pool = [r for r in record_pool if _is_ee_paper(r)]
            elif paper_type_filter == "cp":
                record_pool = [r for r in record_pool if _is_cp_paper(r)]
            elif paper_type_filter == "ia":
                record_pool = [r for r in record_pool if _is_ia_paper(r)]
            elif paper_type_filter == "independent":
                record_pool = [r for r in record_pool
                               if not _is_ee_paper(r) and not _is_cp_paper(r) and not _is_ia_paper(r)]

        if ee_subject_filter:
            record_pool = [r for r in record_pool if _matches_ee_subject(r, ee_subject_filter)]

        if ia_subject_filter:
            record_pool = [r for r in record_pool if _matches_ia_subject(r, ia_subject_filter)]

        if cp_context_filter:
            record_pool = [r for r in record_pool if _matches_cp_context(r, cp_context_filter)]

        if filtered and not record_pool:
            flash(_("No matching papers found."), "info")

        pagination = paginate_records(record_pool, page, per_page)

        for p in pagination["items"]:
            if p.get("author_school"):
                unique_schools = []
                for s in p["author_school"].split(","):
                    s_clean = s.strip()
                    if s_clean and s_clean not in unique_schools:
                        unique_schools.append(s_clean)
                p["author_school_deduped"] = ", ".join(unique_schools) if unique_schools else p["author_school"]
            # Parse paper type for display (CP > EE > Independent Research)
            p["paper_type"] = "Independent Research"
            raw_cp = p.get("cp_data", "")
            if raw_cp:
                try:
                    cp_info = json.loads(raw_cp)
                    if cp_info.get("is_cp_paper"):
                        p["paper_type"] = "Community Project"
                        p["cp_global_context"] = cp_info.get("global_context", "")
                        p["cp_action_types"] = cp_info.get("action_types", [])
                        p["cp_total_score"] = cp_info.get("total_score", 0)
                except (json.JSONDecodeError, TypeError):
                    pass
            raw_ib = p.get("ib_ee_data", "")
            if raw_ib and p["paper_type"] == "Independent Research":
                try:
                    ib_info = json.loads(raw_ib)
                    if ib_info.get("is_ib_ee"):
                        p["paper_type"] = "Extended Essay"
                        p["ee_core_subject"] = ib_info.get("core_subject", "")
                        p["ee_interdisciplinary_subject"] = ib_info.get("interdisciplinary_subject", "")
                except (json.JSONDecodeError, TypeError):
                    pass
            raw_ia = p.get("ia_data", "")
            if raw_ia and p["paper_type"] == "Independent Research":
                try:
                    ia_info = json.loads(raw_ia)
                    if ia_info.get("is_ia"):
                        p["paper_type"] = "Internal Assessment"
                        p["ia_subject"] = ia_info.get("subject", "")
                except (json.JSONDecodeError, TypeError):
                    pass

        return render_template(
            "search.html",
            user=user,
            query=query,
            category_filter=category_filter,
            language_filter=language_filter,
            date_filter=date_filter,
            paper_type_filter=paper_type_filter,
            ee_subject_filter=ee_subject_filter,
            ee_subjects_list=_get_ee_subjects_list(),
            ia_subject_filter=ia_subject_filter,
            ia_subjects_list=_get_ia_subjects_list(),
            cp_context_filter=cp_context_filter,
            cp_contexts=CP_GLOBAL_CONTEXTS,
            filtered=filtered,
            records=pagination["items"],
            pagination=pagination,
            is_guest=is_guest,
            total_matches=len(record_pool),
            paper_categories=load_paper_categories(),
            journal_id_map=get_journal_id_map(),
            journals=get_journal_names(),
            journal_filter=journal_filter,
        )

    @app.route("/dashboard/admin/papers")
    def paper_manage():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        papers = gather_paper_records(_paper_library())
        for paper in papers:
            if _is_cp_paper(paper):
                paper_type = "Community Project"
            elif _is_ee_paper(paper):
                paper_type = "Extended Essay"
            elif _is_ia_paper(paper):
                paper_type = "Internal Assessment"
            else:
                paper_type = "Independent Research"
            paper["paper_type"] = paper_type

        return render_template("paper_manage.html", user=user,
                               papers=papers, journals=get_journal_names(),
                               paper_categories=load_paper_categories())

    @app.route("/admin/papers", endpoint="paper_manage_legacy")
    def paper_manage_legacy():
        return redirect(url_for("paper_manage"), code=301)

    @app.route("/dashboard/admin/papers/bulk", methods=["POST"])
    def papers_bulk_action():
        user = require_login(level=3)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        data = request.get_json(silent=True) or {}
        filenames = data.get("filenames", [])
        op = data.get("op", "")
        if op == "delete":
            deleted = []
            for fname in filenames:
                p = resolve_contained(PAPERS_DIR, fname, must_exist=True)
                if p is None:
                    continue
                remove_paper_metadata(fname)
                p.unlink(missing_ok=True)
                try:
                    rag_index.purge(fname)
                except Exception:
                    app.logger.exception("purge failed")
                deleted.append(fname)
            return jsonify({"deleted": deleted, "count": len(deleted)})
        if op == "set_journal":
            journal = (data.get("journal") or "").strip()
            updated = []
            for fname in filenames:
                if resolve_contained(PAPERS_DIR, fname, must_exist=True) is None:
                    continue
                upsert_paper_metadata(fname, {"journal": journal})
                updated.append(fname)
            return jsonify({"updated": updated, "count": len(updated)})
        return jsonify({"error": "Unsupported operation"}), 400

    @app.route("/paper/<uuid:paper_id>")
    def preview_paper(paper_id):
        document = _current_paper_pdf(paper_id)
        paper = _paper_record_payload(document.paper)
        user = get_active_user()
        is_guest = user is None
        source_query = request.args.get("q", "").strip()
        source_page = request.args.get("page", "").strip()

        related_pairs = []
        try:
            if llm_client.llm_enabled():
                related_pairs = rag_index.related_papers(
                    document.paper.paper_id,
                    k=5,
                )
        except Exception:  # never break the page on a ranking failure
            app.logger.warning("related-paper ranking failed")

        related_papers = []
        if related_pairs:
            for related_id, _score in related_pairs:
                try:
                    related_document = _paper_library().current_pdf(
                        str(related_id)
                    )
                except NotFound:
                    continue
                if related_document.paper.paper_id == document.paper.paper_id:
                    continue
                related_papers.append(
                    _paper_record_payload(related_document.paper)
                )
        elif document.paper.category:
            related_papers = [
                _paper_record_payload(record)
                for record in _paper_library().list_visible()
                if record.category == document.paper.category
                and record.paper_id != document.paper.paper_id
            ][:5]

        pdf_url = url_for(
            "paper_file"
            if (not is_guest or OPEN_ACCESS)
            else "paper_preview",
            paper_id=document.paper.paper_id,
        )

        names = document.paper.author_name.split(", ")
        emails = document.paper.author_email.split(", ")
        schools = document.paper.author_school.split(", ")
        parsed_authors = []
        for index, name in enumerate(names):
            if name.strip():
                parsed_authors.append(
                    {
                        "name": name.strip(),
                        "email": (
                            emails[index].strip()
                            if index < len(emails)
                            else ""
                        ),
                        "school": (
                            schools[index].strip()
                            if index < len(schools)
                            else ""
                        ),
                    }
                )

        unique_schools = []
        for school in schools:
            cleaned = school.strip()
            if cleaned and cleaned not in unique_schools:
                unique_schools.append(cleaned)

        def parsed_json(value):
            if not value:
                return None
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return None

        return render_template(
            "preview.html",
            user=user,
            paper=paper,
            parsed_authors=parsed_authors,
            unique_schools_str=", ".join(unique_schools),
            related_papers=related_papers,
            source_query=source_query,
            source_page=source_page,
            is_guest=is_guest,
            pdf_url=pdf_url,
            journal_id_map=get_journal_id_map(),
            journal_slug_map=get_journal_slug_map(),
            ib_ee_info=parsed_json(document.paper.ib_ee_data),
            cp_info=parsed_json(document.paper.cp_data),
            ia_info=parsed_json(document.paper.ia_data),
        )

    @app.route("/paper/<uuid:paper_id>/pdf")
    def paper_file(paper_id):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        document = _current_paper_pdf(paper_id)
        return send_file(
            document.path,
            mimetype="application/pdf",
            as_attachment=request.args.get("download") == "1",
            download_name=document.paper.filename,
        )

    @app.route("/paper/<uuid:paper_id>/preview.pdf")
    def paper_preview(paper_id):
        document = _current_paper_pdf(paper_id)
        if OPEN_ACCESS or get_active_user() is not None:
            return send_file(
                document.path,
                mimetype="application/pdf",
                as_attachment=False,
                download_name=document.paper.filename,
            )
        preview_stream = build_preview_pdf(document.path, max_pages=2)
        return send_file(
            preview_stream,
            mimetype="application/pdf",
            as_attachment=False,
            download_name=document.paper.filename,
        )

    @app.route("/paper/<uuid:paper_id>/info")
    def paper_info(paper_id):
        user = require_login(level=3)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        document = _current_paper_pdf(paper_id)
        payload = _paper_record_payload(document.paper)
        payload["pdf_url"] = url_for(
            "paper_file",
            paper_id=document.paper.paper_id,
        )
        return jsonify(payload)

    @legacy_paper_path_route("/paper/<path:filename>/info")
    def paper_info_legacy(filename):
        user = require_login(level=3)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        document = _legacy_paper_document(filename)
        return redirect(
            url_for("paper_info", paper_id=document.paper.paper_id),
            code=301,
        )

    @legacy_paper_path_route(
        "/dashboard/paper/<path:filename>/modify",
        methods=["GET", "POST"],
    )
    def paper_modify(filename):
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)

        document = _legacy_paper_document(filename)
        paper_id = document.paper.paper_id
        paper_path = resolve_contained(PAPERS_DIR, filename, must_exist=True)
        if paper_path is None:
            flash(_("Paper not found."), "warning")
            return redirect(url_for("paper_manage"))

        meta_rows = load_paper_metadata()
        meta = {}
        for r in meta_rows:
            if r.get("filename") == filename:
                meta = r
                break

        def parsed_authors_from_meta(meta_row):
            names = meta_row.get("author_name", "").split(", ")
            emails = meta_row.get("author_email", "").split(", ")
            schools = meta_row.get("author_school", "").split(", ")

            parsed = []
            for i, name in enumerate(names):
                if name.strip():
                    parsed.append({
                        "name": name.strip(),
                        "email": emails[i].strip() if i < len(emails) else "",
                        "school": schools[i].strip() if i < len(schools) else ""
                    })
            if not parsed:
                parsed = [{"name": "", "email": "", "school": ""}]
            return parsed

        def render_modify_form(meta_row):
            return render_template(
                "paper_modify.html",
                user=user,
                filename=filename,
                paper_id=paper_id,
                meta=meta_row,
                parsed_authors=parsed_authors_from_meta(meta_row),
                categories=load_paper_categories(),
                journals=get_journal_names(),
                ee_subjects=load_ee_subjects(),
                ia_subjects=load_ia_subjects(),
                cp_global_contexts=CP_GLOBAL_CONTEXTS,
                cp_action_types=CP_ACTION_TYPES,
                ib_criteria_defs=IB_EE_CRITERIA_DEFS,
                cp_criteria_defs=CP_CRITERIA_DEFS,
            )

        if request.method == "POST":
            title = request.form.get("title", "").strip()

            raw_names = request.form.getlist("author_name")
            raw_emails = request.form.getlist("author_email")
            raw_schools = request.form.getlist("author_school")

            is_ib_sample = request.form.get("is_ib_sample") == "1"
            is_anonymous = not is_ib_sample and request.form.get("is_anonymous") == "1"
            is_ib_ee = request.form.get("is_ib_ee") == "1"
            is_cp_paper = request.form.get("is_cp_paper") == "1"
            is_ia = request.form.get("is_ia") == "1"
            ib_ee_data = build_ib_ee_data_from_form(request.form) if is_ib_ee else ""
            cp_data = build_cp_data_from_form(request.form) if is_cp_paper else ""
            ia_data = build_ia_data_from_form(request.form) if is_ia else ""

            if is_ib_sample:
                author_names = ["IB SAMPLE"]
                author_emails = [""]
                author_schools = [""]
            elif is_anonymous:
                author_names = []
                author_emails = []
                author_schools = []
            else:
                author_names = []
                author_emails = []
                author_schools = []
                for i, name in enumerate(raw_names):
                    if name.strip():
                        author_names.append(name.strip())
                        author_emails.append(raw_emails[i].strip() if i < len(raw_emails) else "")
                        author_schools.append(raw_schools[i].strip() if i < len(raw_schools) else "")

            final_author_name = ", ".join(author_names)
            final_author_email = ", ".join(author_emails)
            final_author_school = ", ".join(author_schools)

            form_meta = {
                **meta,
                "title": title,
                "journal": request.form.get("journal", "").strip(),
                "category": request.form.get("category", "").strip(),
                "language": request.form.get("language", "").strip(),
                "keywords": request.form.get("keywords", "").strip(),
                "abstract": request.form.get("abstract", "").strip(),
                "author_name": final_author_name,
                "author_email": final_author_email,
                "author_school": final_author_school,
                "published_at": meta.get("published_at", ""),
                "is_ib_sample": "1" if is_ib_sample else "",
                "is_anonymous": "1" if is_anonymous else "",
                "ib_ee_data": ib_ee_data,
                "cp_data": cp_data,
                "ia_data": ia_data,
            }

            if sum([is_ib_ee, is_cp_paper, is_ia]) > 1:
                flash(_("A paper can only be one of: Extended Essay, Community Project, or Internal Assessment."), "danger")
                return render_modify_form(form_meta)
            if is_ib_ee and not request.form.get("ib_ee_core_subject", "").strip():
                flash(_("Please select an EE core subject."), "danger")
                return render_modify_form(form_meta)
            if is_cp_paper and not request.form.get("cp_global_context", "").strip():
                flash(_("Please select a Global Context."), "danger")
                return render_modify_form(form_meta)
            if is_cp_paper and not request.form.getlist("cp_action_type"):
                flash(_("Please select at least one Type of Action."), "danger")
                return render_modify_form(form_meta)
            if is_ia:
                ia_parsed = json.loads(ia_data)
                if not ia_parsed.get("subject"):
                    flash(_("Please select an IA subject."), "danger")
                    return render_modify_form(form_meta)
                if not ia_parsed.get("criteria"):
                    flash(_("The selected IA subject has no assessment criteria configured."), "danger")
                    return render_modify_form(form_meta)

            # We use the raw first author for the filename
            primary_author = author_names[0] if author_names else ""
            new_filename = _build_safe_paper_filename(title, primary_author)
            if new_filename != filename:
                new_paper_path = PAPERS_DIR / new_filename
                if new_paper_path.exists():
                    flash(_("A file with the new name already exists, unable to rename."), "warning")
                    return redirect(url_for("paper_modify", filename=filename))
                else:
                    paper_path.rename(new_paper_path)
                    remove_paper_metadata(filename)
                    filename = new_filename

            set_pdf_metadata(PAPERS_DIR / filename, title, final_author_name)

            upsert_paper_metadata(filename, {
                "title": title,
                "journal": request.form.get("journal", "").strip(),
                "category": request.form.get("category", "").strip(),
                "language": request.form.get("language", "").strip(),
                "keywords": request.form.get("keywords", "").strip(),
                "abstract": request.form.get("abstract", "").strip(),
                "author_name": final_author_name,
                "author_email": final_author_email,
                "author_school": final_author_school,
                "published_at": meta.get("published_at", ""),
                "is_ib_sample": "1" if is_ib_sample else "",
                "is_anonymous": "1" if is_anonymous else "",
                "ib_ee_data": ib_ee_data,
                "cp_data": cp_data,
                "ia_data": ia_data,
            })
            flash(_("Paper information updated."), "success")
            return redirect(url_for("paper_manage"))

        return render_modify_form(meta)

    @legacy_paper_path_route(
        "/paper/<path:filename>/modify",
        endpoint="paper_modify_legacy",
    )
    def paper_modify_legacy(filename):
        return redirect(url_for("paper_modify", filename=filename), code=301)

    @legacy_paper_path_route(
        "/dashboard/paper/<path:filename>/delete",
        methods=["POST"],
    )
    def paper_delete(filename):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))

        paper_path = resolve_contained(PAPERS_DIR, filename, must_exist=True)
        if paper_path is None:
            flash(_("Paper not found."), "warning")
            return redirect(url_for("paper_manage"))

        remove_paper_metadata(filename)
        paper_path.unlink(missing_ok=True)
        flash(_("Deleted %(filename)s.", filename=filename), "success")
        try:
            rag_index.purge(filename)
        except Exception:
            app.logger.exception("Failed to purge chunks for deleted paper")
        return redirect(url_for("paper_manage"))

    @app.route("/preview/<path:filename>")
    def preview_paper_legacy(filename: str):
        document = _legacy_paper_document(filename)
        values = {"paper_id": document.paper.paper_id}
        for parameter in ("q", "page"):
            value = request.args.get(parameter, "").strip()
            if value:
                values[parameter] = value
        return redirect(
            url_for("preview_paper", **values),
            code=301,
        )

    @app.route("/papers/preview/<path:filename>")
    def paper_preview_legacy(filename: str):
        document = _legacy_paper_document(filename)
        return redirect(
            url_for(
                "paper_preview",
                paper_id=document.paper.paper_id,
            ),
            code=301,
        )

    @app.route("/papers/raw/<path:filename>")
    def paper_file_legacy(filename: str):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        document = _legacy_paper_document(filename)
        return redirect(
            url_for("paper_file", paper_id=document.paper.paper_id),
            code=301,
        )

    @app.route("/papers/<path:filename>")
    def download_legacy(filename: str):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        document = _legacy_paper_document(filename)
        return redirect(
            url_for(
                "paper_file",
                paper_id=document.paper.paper_id,
                download=1,
            ),
            code=301,
        )

    # ---------- EE subjects management ----------
    @app.route("/dashboard/admin/ee-subjects", endpoint="ee_subjects_manage")
    def ee_subjects_manage():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        return render_template("ee_subjects_manage.html", user=user,
                               ee_subjects=load_ee_subjects())

    @app.route("/admin/categories", endpoint="ee_subjects_manage_legacy")
    def ee_subjects_manage_legacy():
        return redirect(url_for("ee_subjects_manage"), code=301)

    @app.route("/dashboard/admin/categories", endpoint="ee_subjects_manage_legacy_dash")
    def ee_subjects_manage_legacy_dash():
        return redirect(url_for("ee_subjects_manage"), code=301)

    @app.route("/dashboard/admin/ee-subjects/save", methods=["POST"], endpoint="admin_ee_subjects_save")
    def ee_subjects_save():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        payload = request.json or {}
        result = reconcile_ee_subjects(load_ee_subjects(), payload)
        if result["errors"]:
            return jsonify(error=str(_("Please fix the highlighted fields before saving.")),
                           details=result["errors"]), 400
        conflicts = []
        for name in result["deletions"]:
            n = count_papers_using_ee_subject(name)
            if n > 0:
                conflicts.append({"subject": name, "paper_count": n})
        if conflicts:
            return jsonify(error=str(_("Some subjects are still used by papers.")),
                           conflicts=conflicts), 409
        for old_name, new_name in result["renames"]:
            rename_ee_subject_in_papers(old_name, new_name)
        save_ee_subjects(result["tree"])
        return jsonify(ok=True, **result["tree"])

    # ---------- IA subjects management ----------
    @app.route("/dashboard/admin/ia-subjects", endpoint="ia_subjects_manage")
    def ia_subjects_manage():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if not session.get("user") else url_for("dashboard")
            return redirect(target)
        return render_template("ia_subjects_manage.html", user=user,
                               ia_subjects=load_ia_subjects())

    @app.route("/dashboard/admin/ia-subjects/save", methods=["POST"], endpoint="admin_ia_subjects_save")
    def admin_ia_subjects_save():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        payload = request.json or {}
        result = reconcile_ia_subjects(load_ia_subjects(), payload)
        if result["errors"]:
            return jsonify(error=str(_("Please fix the highlighted fields before saving.")),
                           details=result["errors"]), 400
        conflicts = []
        for name in result["deletions"]:
            n = count_papers_using_ia_subject(name)
            if n > 0:
                conflicts.append({"subject": name, "paper_count": n})
        if conflicts:
            return jsonify(error=str(_("Some subjects are still used by papers.")),
                           conflicts=conflicts), 409
        for old_name, new_name in result["renames"]:
            rename_ia_subject_in_papers(old_name, new_name)
        save_ia_subjects(result["tree"])
        return jsonify(ok=True, **result["tree"])
