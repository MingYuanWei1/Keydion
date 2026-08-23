"""Paper search, preview, serving, modify/delete, and manage routes."""
import json
from dataclasses import asdict

from flask import (
    abort,
    current_app,
    flash,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask_babel import gettext as _
from werkzeug.routing import PathConverter
from werkzeug.utils import secure_filename

import llm_client
import rag_index
from config import (
    CP_ACTION_TYPES,
    CP_CRITERIA_DEFS,
    CP_GLOBAL_CONTEXTS,
    IB_EE_CRITERIA_DEFS,
    MAX_SEARCH_QUERY_CHARS,
    OPEN_ACCESS,
    RESTORE_RATE_LIMIT,
    RESTORE_RATE_WINDOW,
    SEARCH_RATE_LIMIT,
    SEARCH_RATE_WINDOW,
)
from routes.shared import paginate_records
from services.auth import get_active_user, require_login
from services.rate_limit import consume as consume_rate_limit
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
    resolve_contained,
    save_ee_subjects,
    save_ia_subjects,
    save_paper_categories,
)
from services.search import _hybrid_search_records
from services.publishing_contracts import NotFound
from services.publishing_contracts import (
    DeletePaper,
    DeletionState,
    EditMetadata,
    BulkEditMetadata,
    IndexingState,
    InvalidInput,
    LifecycleError,
    MetadataPatch,
    PdfUpload,
    RestoreRevision,
    RevisePdf,
    StaleVersion,
)
from routes.publishing_http import (
    actor_from_session,
    lifecycle_error_response,
    lifecycle_from_app,
)


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
    def change_return_endpoint(user):
        return "paper_manage" if str(user.get("role", "1")) == "3" else "dashboard"

    def subject_rename_patches(field, renames):
        patches = []
        rename_map = dict(renames)
        for paper in _paper_library().list_visible():
            raw = getattr(paper, field)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
            touched = False
            if field == "ib_ee_data":
                for key in ("core_subject", "interdisciplinary_subject"):
                    if payload.get(key) in rename_map:
                        payload[key] = rename_map[payload[key]]
                        touched = True
            elif payload.get("subject") in rename_map:
                payload["subject"] = rename_map[payload["subject"]]
                touched = True
            if touched:
                patches.append(
                    MetadataPatch(
                        paper_id=paper.paper_id,
                        expected_row_version=paper.row_version,
                        changes=((field, json.dumps(payload, ensure_ascii=False)),),
                    )
                )
        return tuple(patches)

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

    @app.route("/dashboard/admin/paper-categories/add", methods=["POST"], endpoint="admin_paper_categories_add")
    def paper_category_add():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Category name is required."))), 400
        categories = load_paper_categories()
        if name in categories:
            return jsonify(error=str(_("Category already exists."))), 409
        categories.append(name)
        save_paper_categories(categories)
        return jsonify(items=categories)

    @app.route("/dashboard/admin/paper-categories/rename", methods=["POST"], endpoint="admin_paper_categories_rename")
    def paper_category_rename():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.json or {}
        old_name = data.get("old_name", "").strip()
        new_name = data.get("new_name", "").strip()
        if not old_name or not new_name:
            return jsonify(error=str(_("Both old and new names are required."))), 400
        categories = load_paper_categories()
        if old_name not in categories:
            return jsonify(error=str(_("Category not found."))), 404
        if new_name in categories:
            return jsonify(error=str(_("A category with that name already exists."))), 409
        categories[categories.index(old_name)] = new_name
        affected = [
            paper
            for paper in _paper_library().list_visible()
            if paper.category == old_name
        ]
        if affected:
            try:
                lifecycle_from_app().change_many_metadata(
                    BulkEditMetadata(
                        actor_from_session(),
                        tuple(
                            MetadataPatch(
                                paper.paper_id,
                                paper.row_version,
                                (("category", new_name),),
                            )
                            for paper in affected
                        ),
                    )
                )
            except LifecycleError as error:
                return lifecycle_error_response(error)
        save_paper_categories(categories)
        return jsonify(items=categories)

    @app.route("/dashboard/admin/paper-categories/delete", methods=["POST"], endpoint="admin_paper_categories_delete")
    def paper_category_delete():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Category name is required."))), 400
        categories = load_paper_categories()
        if name not in categories:
            return jsonify(error=str(_("Category not found."))), 404
        categories.remove(name)
        save_paper_categories(categories)
        return jsonify(items=categories)

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
        if query:
            # Admission control for public search (security finding: anonymous
            # queries drove corpus-wide work and paid embeddings without any
            # budget). Reject over-long queries and throttle query work BEFORE
            # touching the corpus, database, or embedding provider.
            if len(query) > MAX_SEARCH_QUERY_CHARS:
                flash(_("Search query is too long."), "warning")
                return redirect(url_for("search"))
            throttle_key = (user.get("username") if user else "") \
                or request.remote_addr or "anonymous"
            decision = consume_rate_limit(
                "search.query",
                throttle_key,
                limit=SEARCH_RATE_LIMIT,
                window_seconds=SEARCH_RATE_WINDOW,
                base_block_seconds=2,
                max_block_seconds=300,
            )
            if not decision.allowed:
                response = make_response(
                    str(_("Too many requests — please slow down.")), 429)
                response.headers["Retry-After"] = str(max(decision.retry_after, 1))
                return response
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
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
            return redirect(target)

        papers = []
        for summary in _paper_library().list_managed():
            paper = asdict(summary.paper)
            paper["lifecycle_state"] = summary.lifecycle_state
            paper["index_status"] = summary.index_status
            paper["index_error"] = summary.index_error
            papers.append(paper)
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
        paper_ids = data.get("paper_ids", [])
        row_versions = data.get("row_versions", {})
        op = data.get("op", "")
        if (
            not isinstance(paper_ids, list)
            or not paper_ids
            or not isinstance(row_versions, dict)
            or any(
                type(paper_id) is not str
                or type(row_versions.get(paper_id)) is not int
                for paper_id in paper_ids
            )
        ):
            return jsonify({"error": "Paper UUIDs and row versions are required"}), 422
        actor = actor_from_session()
        if op == "delete":
            deleted = []
            deleting = []
            stale = []
            not_found = []
            for paper_id in paper_ids:
                try:
                    outcome = lifecycle_from_app().delete_paper(
                        DeletePaper(actor, paper_id, row_versions[paper_id])
                    )
                except StaleVersion as error:
                    stale.append(
                        {"paper_id": paper_id, "current_version": error.current_version}
                    )
                except NotFound:
                    not_found.append(paper_id)
                except LifecycleError as error:
                    return lifecycle_error_response(error)
                else:
                    if outcome.state is DeletionState.DELETED:
                        deleted.append(paper_id)
                    else:
                        deleting.append(paper_id)
            return jsonify(
                deleted=deleted,
                deleting=deleting,
                stale=stale,
                not_found=not_found,
                count=len(deleted),
                deleting_count=len(deleting),
            )
        if op == "set_journal":
            journal = (data.get("journal") or "").strip()
            try:
                outcome = lifecycle_from_app().change_many_metadata(
                    BulkEditMetadata(
                        actor=actor,
                        patches=tuple(
                            MetadataPatch(
                                paper_id=paper_id,
                                expected_row_version=row_versions[paper_id],
                                changes=(("journal", journal),),
                            )
                            for paper_id in paper_ids
                        ),
                    )
                )
            except LifecycleError as error:
                return lifecycle_error_response(error)
            updated = [paper.paper_id for paper in outcome.papers]
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

    @app.route(
        "/dashboard/paper/<uuid:paper_id>/modify",
        methods=["GET", "POST"],
    )
    def paper_modify(paper_id):
        user = require_login(level=2)
        if not user:
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
            return redirect(target)

        document = _current_paper_pdf(paper_id)
        paper_id = document.paper.paper_id
        filename = document.paper.filename
        meta = asdict(document.paper)
        return_endpoint = change_return_endpoint(user)
        management = (
            _paper_library().management_record(paper_id)
            if request.method == "GET"
            else None
        )

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

        def render_modify_form(
            meta_row,
            publishing_error=None,
            row_version_value=None,
        ):
            return render_template(
                "paper_modify.html",
                user=user,
                filename=filename,
                paper_id=paper_id,
                row_version=(
                    document.paper.row_version
                    if row_version_value is None
                    else row_version_value
                ),
                management=management,
                return_endpoint=return_endpoint,
                meta=meta_row,
                publishing_error=publishing_error,
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

        def metadata_edits_requested(current, proposed):
            json_fields = {"ib_ee_data", "cp_data", "ia_data"}
            flag_fields = {"is_ib_sample", "is_anonymous"}

            def normalized(key, value):
                if key in flag_fields:
                    return str(value or "").strip().casefold() in {
                        "1", "true", "yes", "on"
                    }
                if key in json_fields:
                    if not value:
                        return None
                    try:
                        return json.loads(value)
                    except (TypeError, json.JSONDecodeError):
                        return value
                return str(value or "").strip()

            editable_fields = (
                "title",
                "journal",
                "category",
                "language",
                "keywords",
                "abstract",
                "author_name",
                "author_email",
                "author_school",
                "is_ib_sample",
                "is_anonymous",
                "ib_ee_data",
                "cp_data",
                "ia_data",
            )
            return any(
                normalized(key, current.get(key))
                != normalized(key, proposed.get(key))
                for key in editable_fields
            )

        if request.method == "POST":
            submitted_row_version = request.form.get("row_version", "")
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
                return render_modify_form(
                    form_meta, row_version_value=submitted_row_version
                )
            if is_ib_ee and not request.form.get("ib_ee_core_subject", "").strip():
                flash(_("Please select an EE core subject."), "danger")
                return render_modify_form(
                    form_meta, row_version_value=submitted_row_version
                )
            if is_cp_paper and not request.form.get("cp_global_context", "").strip():
                flash(_("Please select a Global Context."), "danger")
                return render_modify_form(
                    form_meta, row_version_value=submitted_row_version
                )
            if is_cp_paper and not request.form.getlist("cp_action_type"):
                flash(_("Please select at least one Type of Action."), "danger")
                return render_modify_form(
                    form_meta, row_version_value=submitted_row_version
                )
            if is_ia:
                ia_parsed = json.loads(ia_data)
                if not ia_parsed.get("subject"):
                    flash(_("Please select an IA subject."), "danger")
                    return render_modify_form(
                        form_meta, row_version_value=submitted_row_version
                    )
                if not ia_parsed.get("criteria"):
                    flash(_("The selected IA subject has no assessment criteria configured."), "danger")
                    return render_modify_form(
                        form_meta, row_version_value=submitted_row_version
                    )

            primary_author = author_names[0] if author_names else ""
            new_filename = _build_safe_paper_filename(title, primary_author)
            try:
                expected_version = int(request.form.get("row_version", ""))
            except (TypeError, ValueError):
                error = InvalidInput({"row_version": "must be an integer"})
                return lifecycle_error_response(
                    error,
                    html_renderer=lambda payload, status: (
                        render_modify_form(
                            form_meta,
                            publishing_error=payload,
                            row_version_value=submitted_row_version,
                        ),
                        status,
                    ),
                )

            replacement = request.files.get("replacement_pdf")
            if replacement and replacement.filename:
                if metadata_edits_requested(meta, form_meta):
                    flash(
                        _(
                            "Save metadata changes separately before uploading "
                            "a replacement PDF."
                        ),
                        "danger",
                    )
                    return render_modify_form(
                        form_meta, row_version_value=submitted_row_version
                    ), 422
                intent = RevisePdf(
                    actor=actor_from_session(),
                    paper_id=paper_id,
                    expected_row_version=expected_version,
                    pdf=PdfUpload(
                        filename=secure_filename(replacement.filename),
                        stream=replacement.stream,
                    ),
                )
            else:
                intent = EditMetadata(
                    actor=actor_from_session(),
                    patch=MetadataPatch(
                        paper_id=paper_id,
                        expected_row_version=expected_version,
                        changes=tuple(
                            (key, value)
                            for key, value in (
                                ("filename", new_filename),
                                ("title", title),
                                ("journal", form_meta["journal"]),
                                ("category", form_meta["category"]),
                                ("language", form_meta["language"]),
                                ("keywords", form_meta["keywords"]),
                                ("abstract", form_meta["abstract"]),
                                ("author_name", final_author_name),
                                ("author_email", final_author_email),
                                ("author_school", final_author_school),
                                ("is_ib_sample", form_meta["is_ib_sample"]),
                                ("is_anonymous", form_meta["is_anonymous"]),
                                ("ib_ee_data", ib_ee_data),
                                ("cp_data", cp_data),
                                ("ia_data", ia_data),
                            )
                        ),
                    ),
                )
            try:
                outcome = lifecycle_from_app().change_paper(intent)
            except LifecycleError as error:
                return lifecycle_error_response(
                    error,
                    html_renderer=lambda payload, status: (
                        render_modify_form(
                            form_meta,
                            publishing_error=payload,
                            row_version_value=submitted_row_version,
                        ),
                        status,
                    ),
                )
            if replacement and replacement.filename:
                if (
                    outcome.indexing
                    and outcome.indexing.state is IndexingState.FAILED
                ):
                    flash(
                        _("Paper PDF revised, but RAG indexing failed."),
                        "warning",
                    )
                else:
                    flash(_("Paper PDF revised."), "success")
            else:
                flash(_("Paper information updated."), "success")
            return redirect(url_for(return_endpoint))

        return render_modify_form(meta)

    @legacy_paper_path_route(
        "/dashboard/paper/<path:filename>/modify",
        methods=["GET", "POST"],
        endpoint="paper_modify_legacy_dashboard",
    )
    def paper_modify_legacy_dashboard(filename):
        user = require_login(level=2)
        if not user:
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
            return redirect(target)
        document = _legacy_paper_document(filename)
        return redirect(
            url_for("paper_modify", paper_id=document.paper.paper_id),
            code=301 if request.method == "GET" else 308,
        )

    @legacy_paper_path_route(
        "/paper/<path:filename>/modify",
        endpoint="paper_modify_legacy",
    )
    def paper_modify_legacy(filename):
        user = require_login(level=2)
        if not user:
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
            return redirect(target)
        document = _legacy_paper_document(filename)
        return redirect(
            url_for("paper_modify", paper_id=document.paper.paper_id),
            code=301,
        )

    @app.route(
        "/dashboard/paper/<uuid:paper_id>/delete",
        methods=["POST"],
    )
    def paper_delete(paper_id):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        return_endpoint = change_return_endpoint(user)
        try:
            expected_version = int(request.form.get("row_version", ""))
        except (TypeError, ValueError):
            return lifecycle_error_response(
                InvalidInput({"row_version": "must be an integer"}),
                redirect_endpoint=return_endpoint,
            )
        try:
            outcome = lifecycle_from_app().delete_paper(
                DeletePaper(
                    actor=actor_from_session(),
                    paper_id=str(paper_id),
                    expected_row_version=expected_version,
                )
            )
        except LifecycleError as error:
            return lifecycle_error_response(
                error,
                redirect_endpoint=return_endpoint,
            )
        if outcome.state is DeletionState.DELETED:
            flash(_("Paper deleted."), "success")
        else:
            flash(_("Paper deletion is in progress."), "warning")
        return redirect(url_for(return_endpoint))

    @legacy_paper_path_route(
        "/dashboard/paper/<path:filename>/delete",
        methods=["POST"],
        endpoint="paper_delete_legacy",
    )
    def paper_delete_legacy(filename):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        document = _legacy_paper_document(filename)
        return redirect(
            url_for("paper_delete", paper_id=document.paper.paper_id),
            code=308,
        )

    @app.route(
        "/dashboard/paper/<uuid:paper_id>/revisions/<int:revision>/pdf"
    )
    def paper_revision_file(paper_id, revision):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        try:
            document = _paper_library().private_revision_pdf(
                str(paper_id),
                revision,
                actor=actor_from_session(),
            )
        except NotFound:
            abort(404)
        except LifecycleError as error:
            return lifecycle_error_response(
                error,
                redirect_endpoint="paper_manage",
            )
        return send_file(
            document.path,
            mimetype="application/pdf",
            download_name=document.paper.filename,
        )

    @app.route(
        "/dashboard/paper/<uuid:paper_id>/restore/<int:revision>",
        methods=["POST"],
    )
    def paper_restore(paper_id, revision):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        return_endpoint = change_return_endpoint(user)
        # Restore duplicates an entire immutable PDF server-side per call; a
        # tiny repeated request must not be able to churn storage and indexing
        # without bound (security finding: uncontrolled resource consumption).
        decision = consume_rate_limit(
            "paper.restore",
            user.get("username") or request.remote_addr or "?",
            limit=RESTORE_RATE_LIMIT,
            window_seconds=RESTORE_RATE_WINDOW,
            base_block_seconds=10,
            max_block_seconds=3600,
        )
        if not decision.allowed:
            flash(_("Too many requests — please slow down."), "warning")
            return redirect(url_for(return_endpoint))
        try:
            expected_version = int(request.form.get("row_version", ""))
        except (TypeError, ValueError):
            return lifecycle_error_response(
                InvalidInput({"row_version": "must be an integer"}),
                redirect_endpoint=return_endpoint,
            )
        try:
            outcome = lifecycle_from_app().change_paper(
                RestoreRevision(
                    actor=actor_from_session(),
                    paper_id=str(paper_id),
                    expected_row_version=expected_version,
                    revision=revision,
                )
            )
        except LifecycleError as error:
            return lifecycle_error_response(
                error,
                redirect_endpoint=return_endpoint,
            )
        if outcome.indexing and outcome.indexing.state is IndexingState.FAILED:
            flash(
                _("Paper revision restored, but RAG indexing failed."),
                "warning",
            )
        else:
            flash(_("Paper revision restored."), "success")
        return redirect(url_for(return_endpoint))

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
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
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
        patches = subject_rename_patches("ib_ee_data", result["renames"])
        if patches:
            try:
                lifecycle_from_app().change_many_metadata(
                    BulkEditMetadata(actor_from_session(), patches)
                )
            except LifecycleError as error:
                return lifecycle_error_response(error)
        save_ee_subjects(result["tree"])
        return jsonify(ok=True, **result["tree"])

    # ---------- IA subjects management ----------
    @app.route("/dashboard/admin/ia-subjects", endpoint="ia_subjects_manage")
    def ia_subjects_manage():
        user = require_login(level=3)
        if not user:
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
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
        patches = subject_rename_patches("ia_data", result["renames"])
        if patches:
            try:
                lifecycle_from_app().change_many_metadata(
                    BulkEditMetadata(actor_from_session(), patches)
                )
            except LifecycleError as error:
                return lifecycle_error_response(error)
        save_ia_subjects(result["tree"])
        return jsonify(ok=True, **result["tree"])
