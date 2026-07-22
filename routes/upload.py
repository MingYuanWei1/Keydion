"""Upload wizard + upload-related API routes."""
import json
from uuid import uuid4

from flask import (
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_babel import gettext as _
from werkzeug.utils import secure_filename

import llm_client
from config import (
    CP_ACTION_TYPES,
    CP_GLOBAL_CONTEXTS,
    PENDING_PAPERS_DIR,
    _MISSING_FIELD_MESSAGES,
)
from ee_pdf_extractor import EePdfExtractionError, extract_ee_metadata
from ia_metadata import IAMetadataError, generate_ia_scores
from llm_metadata import LLMMetadataError, generate_abstract_keywords
from services.auth import get_active_user, require_login
from services.journals import get_journal_names
from services.publishing_contracts import (
    DirectPublish,
    IndexingState,
    LifecycleError,
    NormalizedPaperMetadata,
    PdfUpload,
)
from services.papers import (
    _build_safe_paper_filename,
    _ia_criteria_for_subject,
    allowed_file,
    build_cp_data_from_form,
    build_ia_data_from_form,
    build_ib_ee_data_from_form,
    load_ee_subjects,
    load_ia_subjects,
    load_paper_categories,
    parse_cp_data_for_form,
    parse_ia_data_for_form,
    parse_ib_ee_data_for_form,
    resolve_contained,
)
from services.submissions import (
    _get_submission,
    _save_submission,
    _store_pending_submission_pdf,
    _update_submission,
)
from services.publishing_time import utc_now_db
from services.rate_limit import consume as consume_rate_limit
from routes.publishing_http import (
    actor_from_session,
    lifecycle_error_response,
    lifecycle_from_app,
)


def register_routes(app):

    # ==================== UPLOAD ROUTES ====================

    def _expensive_operation_limited(user, scope):
        keys = (
            (f"upload.{scope}.ip", request.remote_addr or "unknown"),
            (f"upload.{scope}.account", user.get("username") or "unknown"),
        )
        for bucket_scope, key in keys:
            decision = consume_rate_limit(
                bucket_scope,
                key,
                limit=10,
                window_seconds=300,
                base_block_seconds=5,
                max_block_seconds=900,
            )
            if not decision.allowed:
                response = jsonify({"error": str(_("Too many requests — please slow down."))})
                response.status_code = 429
                response.headers["Retry-After"] = str(decision.retry_after)
                return response
        return None

    def _render_upload(user, form_data, draft_id, publishing_error=None):
        """Render upload.html with the wizard_boot context the JS needs."""
        form_data.setdefault("publishing_idempotency_key", str(uuid4()))
        try:
            _role = int(user.get("role", "1"))
        except (TypeError, ValueError):
            _role = 1
        wizard_boot = {
            "submit_url": url_for("upload"),
            "draft_id": draft_id or "",
            "form_data": form_data,
            "paper_categories": load_paper_categories(),
            "journals": get_journal_names(),
            "ee_subjects": load_ee_subjects(),
            "ia_subjects": load_ia_subjects(),
            "cp_global_contexts": CP_GLOBAL_CONTEXTS,
            "cp_action_types": CP_ACTION_TYPES,
            "user_key": user.get("username", ""),
            "extract_assist_enabled": (llm_client.vision_enabled() or llm_client.llm_enabled()) and _role >= 2,
            "i18n": {
                "step_name_type": _("Paper Type"),
                "step_name_metadata": _("Metadata"),
                "step_name_authors": _("Authors"),
                "step_name_file": _("File"),
                "step_name_review": _("Review"),
                "step_label": _("Step %(n)s"),
                "submit_paper": _("Submit Paper"),
                "continue": _("Continue →"),
                "back": _("← Back"),
                "save_draft": _("Save Draft"),
                "choose_paper_type": _("Choose paper type"),
                "what_kind": _("What kind of paper are you submitting?"),
                "what_kind_sub": _("The fields you'll be asked for next depend on this. You can come back and change it before submitting."),
                "type_tag_standard": _("Independent Research"),
                "type_title_standard": _("Standard Paper"),
                "type_body_standard": _("A self-directed research paper, conference paper, or article."),
                "type_meta_standard": _("Title · authors · abstract · subject"),
                "type_tag_ee": _("IB Diploma"),
                "type_title_ee": _("Extended Essay (EE)"),
                "type_body_ee": _("A 4,000-word IB Diploma research essay with structured criterion scores (A–E) and an EE subject from the six IB subject groups."),
                "type_meta_ee": _("Research Question · EE subject · criterion scores A–E"),
                "type_tag_cp": _("IB Diploma"),
                "type_title_cp": _("Community Project (CP)"),
                "type_body_cp": _("An IB MYP Community Project graded against Criteria A–D, with a Global Context and a chosen type of action."),
                "type_meta_cp": _("Title · Global Context · type of action · criteria A–D"),
                "type_tag_ia": _("IB Diploma"),
                "type_title_ia": _("Internal Assessment (IA)"),
                "type_body_ia": _("A subject-specific IB Internal Assessment graded against that subject's assessment criteria."),
                "type_meta_ia": _("Title · IA subject · per-criterion scores"),
                "type_ia": _("IB Internal Assessment"),
                "tell_us_ia": _("Tell us about your assessment"),
                "ia_subject": _("IA Subject"),
                "select_ia_subject": _("Select an IA subject…"),
                "overall_ia_sub": _("Calculated server-side from the criteria above"),
                "ia_details": _("IA Details"),
                "ia_score_x": _("IA criterion score %(k)s"),
                "ia_holistic_only": _("Enter the overall score directly"),
                "ia_holistic_only_hint": _("Skip per-criterion scoring and enter just the total mark for the assessment."),
                "overall_ia_direct_sub": _("Enter the overall mark for the assessment"),
                "overall_score": _("Overall Score"),
                "ia_overall_score": _("IA overall score"),
                "ia_no_criteria": _("This subject has no assessment criteria configured yet. Choose another subject or ask an administrator to add criteria."),
                "ia_no_criteria_short": _("IA subject has no criteria"),
                "ia_comment_ph": _("Commentary for this criterion…"),
                "ia_autofill_btn": _("Auto-fill scores from PDF"),
                "ia_autofill_extracting": _("Extracting…"),
                "ia_autofill_ok": _("Extracted scores."),
                "ia_autofill_error": _("Auto-fill failed — try again or fill manually."),
                "ia_autofill_no_subject": _("Choose an IA subject first."),
                "research_question": _("Research Question"),
                "paper_title": _("Paper Title"),
                "research_question_ph": _("e.g. To what extent did monetary policy contribute to the 2008 financial crisis?"),
                "paper_title_ph": _("Enter the complete paper title"),
                "tell_us_ee": _("Tell us about your essay"),
                "tell_us_cp": _("Tell us about your community project"),
                "tell_us_std": _("Tell us about your paper"),
                "metadata_sub_ib": _("IB grading information and bibliographic details for the submission."),
                "metadata_sub_std": _("Bibliographic information that will appear on the public paper page."),
                "paper_details": _("Paper details"),
                "bibliographic": _("Bibliographic"),
                "language": _("Language"),
                "english": _("English"),
                "chinese": _("Chinese"),
                "subject_category": _("Subject Category"),
                "choose_category": _("Choose a subject category…"),
                "keywords": _("Keywords"),
                "add_another": _("Add another…"),
                "keyword_ph": _("Type a keyword and press Enter"),
                "keyword_hint": _("Press Enter or comma to add. Aim for 3–6 keywords."),
                "added": _("added"),
                "abstract": _("Abstract"),
                "abstract_ph": _("Briefly describe your research background, methods, and conclusions…"),
                "abstract_hint": _("A short summary that appears in search results."),
                "is_ib_sample": _("This is an IB Sample Paper"),
                "is_ib_sample_hint": _("Sample papers are reference essays without an identified author."),
                "author_mode_named": _("Named authors"),
                "author_mode_named_hint": _("List the authors with their contact details."),
                "upload_anonymous": _("Upload as anonymous"),
                "upload_anonymous_hint": _("The paper is published without any author information."),
                "crit_ee_A": _("Framework for the essay"),
                "crit_ee_B": _("Knowledge and understanding"),
                "crit_ee_C": _("Analysis and line of argument"),
                "crit_ee_D": _("Discussion and evaluation"),
                "crit_ee_E": _("Reflection"),
                "ee_subject": _("EE Subject"),
                "core_subject": _("Core Subject"),
                "select_core": _("Select a core subject…"),
                "inter_subject": _("Interdisciplinary"),
                "optional": _("Optional"),
                "select_inter": _("Optional — select if applicable…"),
                "crit_scores": _("Criterion Scores"),
                "crit": _("Crit."),
                "criterion": _("Criterion"),
                "score": _("Score"),
                "overall_grade": _("Overall Grade"),
                "overall_ee_sub": _("Calculated server-side from the criteria above"),
                "crit_comments": _("Criterion Commentaries"),
                "include_comments": _("Include commentaries for all criteria"),
                "include_comments_hint": _("Provide short remarks on each criterion plus an optional overall holistic commentary."),
                "crit_comment_ph": _("Commentary for Criterion %(k)s…"),
                "holistic_comment": _("Holistic Commentary"),
                "holistic_ph": _("An overall holistic commentary for the essay…"),
                "crit_cp_A": _("Investigating"),
                "crit_cp_B": _("Planning"),
                "crit_cp_C": _("Taking Action"),
                "crit_cp_D": _("Reflecting"),
                "global_context": _("Global Context"),
                "select_global": _("Select a Global Context…"),
                "global_contexts": _("Global Contexts"),
                "type_of_action": _("Type of Action"),
                "overall_cp_sub": _("Mean of the four criterion scores, rounded"),
                "author_info": _("Author information"),
                "who_wrote": _("Who wrote this?"),
                "authors_sub": _("The first author's contact details are required. Add co-authors as needed."),
                "name": _("Name"),
                "email": _("Email"),
                "school": _("School / Institution"),
                "remove_author": _("Remove author"),
                "add_author": _("+ Add another author"),
                "file_upload": _("File upload"),
                "upload_pdf": _("Upload your PDF"),
                "upload_pdf_sub": _("Submit a single PDF, up to 50 MB. You can change this before publishing."),
                "no_file_chosen": _("No file chosen"),
                "pdf_only_single": _("PDF only · single file"),
                "replace_file": _("Replace"),
                "choose_file": _("Choose file"),
                "file_save_hint": _("If you'd like to come back to this later, click Save Draft below — your form will be restored next time you visit."),
                "paper_type": _("Paper Type"),
                "first_author": _("First author (name, email, school)"),
                "ee_core": _("EE core subject"),
                "ee_score_x": _("EE criterion score %(k)s"),
                "cp_global": _("Global context"),
                "cp_action_label": _("Type of action"),
                "cp_score_x": _("CP criterion score %(k)s"),
                "pdf_file": _("PDF file"),
                "type_standard": _("Independent Research Paper"),
                "type_ee": _("IB Extended Essay"),
                "type_cp": _("IB Community Project"),
                "review_submit": _("Review & submit"),
                "almost_there": _("Almost there — review your submission"),
                "review_sub": _("Make sure everything looks right. You can jump back to any section to make changes."),
                "missing_fields_one": _("1 field still needs attention"),
                "missing_fields_many": _("%(n)s fields still need attention"),
                "go_to": _("go to %(step)s"),
                "everything_filled": _("Everything required is filled in."),
                "submit_cta": _("Click Submit Paper below to send your submission for review."),
                "edit": _("Edit"),
                "type": _("Type"),
                "metadata_title": _("Metadata"),
                "research_q_short": _("Research Q."),
                "title_short": _("Title"),
                "not_provided": _("Not provided"),
                "not_chosen": _("Not chosen"),
                "subject": _("Subject"),
                "none": _("None"),
                "not_written": _("Not written"),
                "ib_sample": _("IB Sample"),
                "yes_skipped": _("Yes — author info skipped"),
                "anonymous_skipped": _("Anonymous — author info skipped"),
                "no": _("No"),
                "authors": _("Authors"),
                "author": _("Author"),
                "file": _("File"),
                "no_file_uploaded": _("No file uploaded"),
                "ee_details": _("EE Details"),
                "total": _("Total"),
                "cp_details": _("CP Details"),
                "none_selected": _("None selected"),
                "avg_grade": _("Avg. Grade"),
                "saving": _("Saving…"),
                "draft_saved_at": _("Draft saved · %(time)s"),
                "restore_banner_title": _("Unsaved changes from earlier"),
                "restore_banner_body": _("Your last session in this browser had changes you didn't save. Restore them?"),
                "discard_btn": _("Discard"),
                "restore_btn": _("Restore"),
                "search": _("Search…"),
                "no_matches": _("No matches"),
                "ee_autofill_btn": _("Auto-fill from commentary PDF"),
                "ee_autofill_extracting": _("Extracting…"),
                "ee_autofill_ok": _("Extracted all fields."),
                "ee_autofill_partial": _("Extracted %(filled)s of %(total)s fields."),
                "ee_autofill_error": _("Auto-fill failed — try again or fill manually."),
                "ee_autofill_overwrite": _("Replace your existing EE entries with values from the PDF?"),
                "meta_autofill_btn": _("Generate abstract & keywords from PDF"),
                "meta_autofill_extracting": _("Generating…"),
                "meta_autofill_ok": _("Generated abstract and keywords."),
                "meta_autofill_error": _("Generation failed — try again or fill manually."),
                "meta_autofill_no_file": _("Upload your PDF in the File step first."),
                "meta_autofill_overwrite": _("Replace your existing title, authors, abstract and keywords with AI-generated ones?"),
                "journal": _("Journal"),
                "journal_none": _("— None —"),
                "journal_hint": _("Optional — assign this paper to a journal."),
                "uploading": _("Uploading… %(pct)s%"),
                "upload_finishing": _("Finishing up…"),
                "upload_failed": _("Upload failed. Please check your connection and try again."),
                "upload_done": _("Upload successful!"),
                "try_again": _("Try again"),
            },
        }
        return render_template("upload.html",
            user=user,
            form_data=form_data,
            journals=get_journal_names(),
            paper_categories=load_paper_categories(),
            ee_subjects=load_ee_subjects(),
            ia_subjects=load_ia_subjects(),
            cp_global_contexts=CP_GLOBAL_CONTEXTS,
            cp_action_types=CP_ACTION_TYPES,
            draft_id=draft_id,
            wizard_boot=wizard_boot,
            publishing_error=publishing_error,
        )

    @app.route("/dashboard/upload", methods=["GET", "POST"])
    def upload():
        user = require_login(level=1)
        if not user:
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
            return redirect(target)

        today = utc_now_db().date().isoformat()
        draft_id = request.args.get("draft", "")
        is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

        # If editing an existing draft, pre-fill form data
        if request.method == "GET" and draft_id:
            draft = _get_submission(draft_id)
            if draft and draft.get("status") == "draft" and draft.get("submitter") == user.get("username", ""):
                form_data = {
                    "title": draft.get("title", ""),
                    "journal": draft.get("journal", ""),
                    "category": draft.get("category", ""),
                    "language": draft.get("language", ""),
                    "keywords": draft.get("keywords", ""),
                    "abstract": draft.get("abstract", ""),
                    "author_name": draft.get("author_name", ""),
                    "author_email": draft.get("author_email", ""),
                    "author_school": draft.get("author_school", ""),
                    "is_ib_sample": draft.get("is_ib_sample", ""),
                    "is_anonymous": draft.get("is_anonymous", ""),
                    "ib_ee_data": draft.get("ib_ee_data", ""),
                    "cp_data": draft.get("cp_data", ""),
                    "ia_data": draft.get("ia_data", ""),
                    "published_at": today,
                }
                # Hydrate EE/CP/IA fieldsets so the wizard can repopulate them.
                form_data.update(parse_ib_ee_data_for_form(draft.get("ib_ee_data", "")))
                form_data.update(parse_cp_data_for_form(draft.get("cp_data", "")))
                form_data.update(parse_ia_data_for_form(draft.get("ia_data", "")))
                return _render_upload(user, form_data, draft_id)

        raw_names = request.form.getlist("author_name")
        raw_emails = request.form.getlist("author_email")
        raw_schools = request.form.getlist("author_school")

        is_ib_sample = request.form.get("is_ib_sample") == "1"
        is_anonymous = not is_ib_sample and request.form.get("is_anonymous") == "1"

        if is_ib_sample:
            author_names = ["IB SAMPLE"]
            author_emails = [""]
            author_schools = [""]
        elif is_anonymous:
            # Anonymous upload: no author info is collected or stored.
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


        form_data = {
            "publishing_idempotency_key": request.form.get(
                "publishing_idempotency_key", ""
            ).strip()
            or str(uuid4()),
            "title": request.form.get("title", "").strip(),
            "journal": request.form.get("journal", "").strip(),
            "category": request.form.get("category", "").strip(),
            "language": request.form.get("language", "").strip(),
            "keywords": request.form.get("keywords", "").strip(),
            "abstract": request.form.get("abstract", "").strip(),
            "author_name": ", ".join(author_names),
            "author_email": ", ".join(author_emails),
            "author_school": ", ".join(author_schools),
            "published_at": today,
            "is_ib_sample": "1" if is_ib_sample else "",
            "is_anonymous": "1" if is_anonymous else "",
        }

        # ---- IB EE data processing ----
        is_ib_ee = request.form.get("is_ib_ee") == "1"
        if is_ib_ee:
            form_data["ib_ee_data"] = build_ib_ee_data_from_form(request.form)
            form_data["is_ib_ee"] = "1"
        else:
            form_data["ib_ee_data"] = ""

        # ---- CP Paper data processing ----
        is_cp_paper = request.form.get("is_cp_paper") == "1"
        if is_cp_paper:
            form_data["cp_data"] = build_cp_data_from_form(request.form)
            form_data["is_cp_paper"] = "1"
        else:
            form_data["cp_data"] = ""

        # ---- IA data processing ----
        is_ia = request.form.get("is_ia") == "1"
        if is_ia:
            form_data["ia_data"] = build_ia_data_from_form(request.form)
            form_data["is_ia"] = "1"
        else:
            form_data["ia_data"] = ""

        if request.method == "POST":
            # Snapshot pre-existing flashes so an AJAX error response reports only
            # this request's validation flashes, not unrelated stale session ones.
            flash_baseline = len(session.get("_flashes", []))
            # Handle "Save as Draft"
            draft_id = request.form.get("draft_id", "").strip()
            if "save_draft" in request.form:
                if not form_data["title"]:
                    flash(_("Please enter at least a paper title to save a draft."), "warning")
                    return _render_upload(user, form_data, draft_id)
                # Format keywords
                if form_data["keywords"]:
                    form_data["keywords"] = ", ".join(
                        [kw.strip() for kw in form_data["keywords"].split(",") if kw.strip()]
                    )
                now = utc_now_db().isoformat()
                if draft_id:
                    # Update existing draft
                    updated = _update_submission(draft_id, {
                        "title": form_data["title"],
                        "journal": form_data["journal"],
                        "category": form_data["category"],
                        "language": form_data["language"],
                        "keywords": form_data["keywords"],
                        "abstract": form_data["abstract"],
                        "author_name": form_data["author_name"],
                        "author_email": form_data["author_email"],
                        "author_school": form_data["author_school"],
                        "is_ib_sample": form_data.get("is_ib_sample", ""),
                        "is_anonymous": form_data.get("is_anonymous", ""),
                        "ib_ee_data": form_data.get("ib_ee_data", ""),
                        "cp_data": form_data.get("cp_data", ""),
                        "ia_data": form_data.get("ia_data", ""),
                        "submitted_at": now,
                    }, expected_submitter=user.get("username", ""), expected_status="draft")
                    if updated is None:
                        abort(404)
                else:
                    # Create new draft
                    sub_id = uuid4().hex[:12]
                    submission = {
                        "id": sub_id,
                        "pdf_filename": "",
                        "pending_filename": "",
                        "submitter": user.get("username", ""),
                        "submitter_name": user.get("display_name", "") or user.get("first_name", "") or user.get("username", ""),
                        "status": "draft",
                        "submitted_at": now,
                        "reviewed_at": None,
                        "reviewer": "",
                        "comment": "",
                        "title": form_data["title"],
                        "journal": form_data["journal"],
                        "category": form_data["category"],
                        "language": form_data["language"],
                        "keywords": form_data["keywords"],
                        "abstract": form_data["abstract"],
                        "author_name": form_data["author_name"],
                        "author_email": form_data["author_email"],
                        "author_school": form_data["author_school"],
                        "is_ib_sample": form_data.get("is_ib_sample", ""),
                        "is_anonymous": form_data.get("is_anonymous", ""),
                        "ib_ee_data": form_data.get("ib_ee_data", ""),
                        "cp_data": form_data.get("cp_data", ""),
                        "ia_data": form_data.get("ia_data", ""),
                    }
                    _save_submission(submission)
                flash(_("Draft saved successfully."), "success")
                return redirect(url_for("my_submissions"))

            # Per-type required-field cascade. Keywords/abstract apply to Standard
            # papers only; author fields are skipped for IB Sample and anonymous submissions.
            required = ["title", "language"] if is_cp_paper else ["title", "category", "language"]
            if not (is_ib_ee or is_cp_paper or is_ia):
                required += ["keywords", "abstract"]
            if not (is_ib_sample or is_anonymous):
                required += ["author_name", "author_email", "author_school"]

            for field in required:
                if not form_data.get(field):
                    flash(_MISSING_FIELD_MESSAGES[field], "danger")
                    return _render_upload(user, form_data, draft_id)

            if sum([is_ib_ee, is_cp_paper, is_ia]) > 1:
                flash(_("A paper can only be one of: Extended Essay, Community Project, or Internal Assessment."), "danger")
                return _render_upload(user, form_data, draft_id)
            if is_ib_ee:
                ib_data = json.loads(form_data["ib_ee_data"])
                if not ib_data.get("core_subject"):
                    flash(_("Please select an EE core subject."), "danger")
                    return _render_upload(user, form_data, draft_id)
            if is_cp_paper:
                cp_data = json.loads(form_data["cp_data"])
                if not cp_data.get("global_context"):
                    flash(_("Please select a Global Context."), "danger")
                    return _render_upload(user, form_data, draft_id)
                if not cp_data.get("action_types"):
                    flash(_("Please select at least one Type of Action."), "danger")
                    return _render_upload(user, form_data, draft_id)
            if is_ia:
                ia_data = json.loads(form_data["ia_data"])
                if not ia_data.get("subject"):
                    flash(_("Please select an IA subject."), "danger")
                    return _render_upload(user, form_data, draft_id)
                if not ia_data.get("criteria"):
                    flash(_("The selected IA subject has no assessment criteria configured."), "danger")
                    return _render_upload(user, form_data, draft_id)

            # 格式化关键词
            if form_data["keywords"]:
                form_data["keywords"] = ", ".join(
                    [kw.strip() for kw in form_data["keywords"].split(",") if kw.strip()]
                )

            file = request.files.get("paper")
            if not file or file.filename == "":
                flash(_("Please select a file to upload"), "warning")
            else:
                original_filename = secure_filename(file.filename)
                if not original_filename:
                    original_filename = f"{uuid4().hex[:8]}.pdf"
                magic = file.stream.read(5)
                file.stream.seek(0)
                if not allowed_file(original_filename):
                    flash(_("Only PDF files are supported"), "danger")
                elif not magic.startswith(b"%PDF-"):
                    flash(_("File is not a valid PDF"), "danger")
                else:
                    # Build a safe filename: try title+author first, fall back to UUID
                    filename = _build_safe_paper_filename(
                        form_data["title"], form_data["author_name"]
                    )
                    role = int(user.get("role", "1"))
                    if role >= 2:
                        # Contributor / Curator: publish through the sole lifecycle writer.
                        intent = DirectPublish(
                            actor=actor_from_session(),
                            idempotency_key=form_data["publishing_idempotency_key"],
                            metadata=NormalizedPaperMetadata(
                                filename=filename,
                                title=form_data["title"],
                                journal=form_data["journal"],
                                category=form_data["category"],
                                language=form_data["language"],
                                keywords=form_data["keywords"],
                                abstract=form_data["abstract"],
                                author_name=form_data["author_name"],
                                author_email=form_data["author_email"],
                                author_school=form_data["author_school"],
                                published_at=form_data["published_at"],
                                is_ib_sample=form_data.get("is_ib_sample", ""),
                                is_anonymous=form_data.get("is_anonymous", ""),
                                ib_ee_data=form_data.get("ib_ee_data", ""),
                                cp_data=form_data.get("cp_data", ""),
                                ia_data=form_data.get("ia_data", ""),
                            ),
                            pdf=PdfUpload(
                                filename=original_filename,
                                stream=file.stream,
                            ),
                        )
                        try:
                            outcome = lifecycle_from_app().publish_direct(intent)
                        except LifecycleError as error:
                            return lifecycle_error_response(
                                error,
                                html_renderer=lambda payload, status: (
                                    _render_upload(
                                        user,
                                        form_data,
                                        draft_id,
                                        publishing_error=payload,
                                    ),
                                    status,
                                ),
                            )
                        if outcome.indexing.state is IndexingState.FAILED:
                            msg = _(
                                "%(paper_name)s uploaded successfully, but RAG indexing failed.",
                                paper_name=outcome.filename,
                            )
                            category = "warning"
                        else:
                            msg = _(
                                "Paper %(filename)s uploaded successfully!",
                                filename=outcome.filename,
                            )
                            category = "success"
                        if is_ajax:
                            return jsonify(ok=True, redirect=url_for("upload"), message=msg)
                        flash(msg, category)
                        return redirect(url_for("upload"))
                    else:
                        # Reader: save to pending review queue
                        if draft_id:
                            existing = _get_submission(draft_id)
                            if not existing or existing.get("submitter") != user.get("username", ""):
                                abort(404)
                            sub_id = draft_id
                        else:
                            sub_id = uuid4().hex[:12]
                        pending_filename = f"{sub_id}_{filename}"
                        pending_path = resolve_contained(PENDING_PAPERS_DIR, pending_filename, must_exist=False)
                        if pending_path is None:
                            abort(400)  # unreachable for server-minted names; defense-in-depth
                        def write_pending_pdf():
                            _store_pending_submission_pdf(
                                file,
                                pending_path,
                                title=form_data["title"],
                                author=form_data["author_name"],
                            )

                        def discard_failed_pending_pdf():
                            pending_path.unlink(missing_ok=True)

                        if draft_id:
                            updated = _update_submission(draft_id, {
                                "pdf_filename": filename,
                                "pending_filename": pending_filename,
                                "status": "pending",
                                "submitted_at": utc_now_db().isoformat(),
                                "title": form_data["title"],
                                "journal": form_data["journal"],
                                "category": form_data["category"],
                                "language": form_data["language"],
                                "keywords": form_data["keywords"],
                                "abstract": form_data["abstract"],
                                "author_name": form_data["author_name"],
                                "author_email": form_data["author_email"],
                                "author_school": form_data["author_school"],
                                "is_ib_sample": form_data.get("is_ib_sample", ""),
                                "is_anonymous": form_data.get("is_anonymous", ""),
                                "ib_ee_data": form_data.get("ib_ee_data", ""),
                                "cp_data": form_data.get("cp_data", ""),
                                "ia_data": form_data.get("ia_data", ""),
                            }, expected_submitter=user.get("username", ""), expected_status="draft",
                                pending_write=write_pending_pdf,
                                pending_cleanup_on_failure=discard_failed_pending_pdf)
                            if updated is None:
                                abort(404)
                        else:
                            submission = {
                                "id": sub_id,
                                "pdf_filename": filename,
                                "pending_filename": pending_filename,
                                "submitter": user.get("username", ""),
                                "submitter_name": user.get("display_name", "") or user.get("first_name", "") or user.get("username", ""),
                                "status": "pending",
                                "submitted_at": utc_now_db().isoformat(),
                                "reviewed_at": None,
                                "reviewer": "",
                                "comment": "",
                                "title": form_data["title"],
                                "journal": form_data["journal"],
                                "category": form_data["category"],
                                "language": form_data["language"],
                                "keywords": form_data["keywords"],
                                "abstract": form_data["abstract"],
                                "author_name": form_data["author_name"],
                                "author_email": form_data["author_email"],
                                "author_school": form_data["author_school"],
                                "is_ib_sample": form_data.get("is_ib_sample", ""),
                                "is_anonymous": form_data.get("is_anonymous", ""),
                                "ib_ee_data": form_data.get("ib_ee_data", ""),
                                "cp_data": form_data.get("cp_data", ""),
                                "ia_data": form_data.get("ia_data", ""),
                            }
                            _save_submission(
                                submission,
                                pending_write=write_pending_pdf,
                                pending_cleanup_on_failure=discard_failed_pending_pdf,
                            )
                        msg = _("Your paper has been submitted and is now pending review.")
                        if is_ajax:
                            return jsonify(ok=True, redirect=url_for("upload"), message=msg)
                        flash(msg, "success")
                        return redirect(url_for("upload"))

        if is_ajax and request.method == "POST":
            errors = [m for _cat, m in session.get("_flashes", [])[flash_baseline:]]
            return jsonify(
                ok=False,
                error="；".join(errors) if errors else _("Upload failed. Please try again."),
            ), 400
        return _render_upload(user, form_data, request.args.get("draft", ""))

    @app.route("/upload", endpoint="upload_legacy")
    def upload_legacy():
        return redirect(url_for("upload"), code=301)

    @app.route("/api/upload/extract-ee-metadata", methods=["POST"])
    def api_extract_ee_metadata():
        user = require_login(level=2)
        if not user:
            return jsonify({"error": str(_("Unauthorized"))}), 401
        limited = _expensive_operation_limited(user, "ee")
        if limited is not None:
            return limited

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        if not upload.filename.lower().endswith(".pdf"):
            return jsonify({"error": str(_("File must be a PDF"))}), 400

        raw = upload.read()
        if not raw.startswith(b"%PDF-"):
            return jsonify({"error": str(_("File is not a valid PDF"))}), 400

        try:
            result = extract_ee_metadata(raw)
        except EePdfExtractionError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 200

    @app.route("/api/upload/generate-abstract-keywords", methods=["POST"])
    def api_generate_abstract_keywords():
        user = require_login(level=2)
        if not user:
            return jsonify({"error": str(_("Unauthorized"))}), 401
        limited = _expensive_operation_limited(user, "abstract")
        if limited is not None:
            return limited

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        if not upload.filename.lower().endswith(".pdf"):
            return jsonify({"error": str(_("File must be a PDF"))}), 400

        raw = upload.read()
        if not raw.startswith(b"%PDF-"):
            return jsonify({"error": str(_("File is not a valid PDF"))}), 400

        language = request.form.get("language", "en")
        try:
            result = generate_abstract_keywords(raw, language)
        except LLMMetadataError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 200

    @app.route("/api/upload/extract-ia-metadata", methods=["POST"])
    def api_extract_ia_metadata():
        user = require_login(level=2)
        if not user:
            return jsonify({"error": str(_("Unauthorized"))}), 401
        limited = _expensive_operation_limited(user, "ia")
        if limited is not None:
            return limited

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        if not upload.filename.lower().endswith(".pdf"):
            return jsonify({"error": str(_("File must be a PDF"))}), 400

        raw = upload.read()
        if not raw.startswith(b"%PDF-"):
            return jsonify({"error": str(_("File is not a valid PDF"))}), 400

        language = request.form.get("language", "en")
        subject = request.form.get("subject", "").strip()
        criteria = _ia_criteria_for_subject(subject)
        if not criteria:
            return jsonify({"error": str(_("Unknown or unconfigured IA subject"))}), 400

        try:
            result = generate_ia_scores(raw, subject, criteria, language)
        except IAMetadataError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 200
