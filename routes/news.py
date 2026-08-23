"""Public + admin routes for News."""
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
from werkzeug.utils import secure_filename

from config import (
    ALLOWED_IMAGE_EXTENSIONS,
    NEWS_IMAGES_DIR,
)
from db import db_session
from models import NewsArticleModel
from services.auth import get_active_user, require_login
from services.news import (
    category_name_validation_error,
    delete_news_article,
    get_news_article,
    load_categories,
    load_news_articles,
    sanitize_news_body,
    save_categories,
    save_news_article,
    update_news_article,
)
from routes.shared import paginate_records


def register_routes(app):

    # ==================== NEWS ROUTES ====================

    @app.route("/news")
    def news_list():
        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        per_page = 15
        all_articles = load_news_articles(status="published")
        pagination = paginate_records(all_articles, page, per_page)
        recent = all_articles[:6]
        return render_template(
            "news.html",
            articles=pagination["items"],
            pagination=pagination,
            recent=recent,
        )

    @app.route("/dashboard/news/upload-inline-image", methods=["POST"])
    def news_upload_inline_image():
        """AJAX endpoint: upload an image for the block editor and return its URL."""
        user = require_login(level=2)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        img_file = request.files.get("file")
        if not img_file or not img_file.filename:
            return jsonify({"error": "No file provided"}), 400
        img_ext = img_file.filename.rsplit(".", 1)[-1].lower() if "." in img_file.filename else ""
        if img_ext not in ALLOWED_IMAGE_EXTENSIONS:
            return jsonify({"error": "Invalid image format"}), 400
        NEWS_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        unique_name = f"{uuid4().hex[:12]}_{secure_filename(img_file.filename)}"
        img_file.save(NEWS_IMAGES_DIR / unique_name)
        img_url = url_for("static", filename=f"uploads/news/{unique_name}")
        return jsonify({"url": img_url})

    @app.route("/dashboard/news/publish", methods=["GET", "POST"])
    def news_publish():
        user = require_login(level=2)
        if not user:
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
            return redirect(target)

        display_name = user.get("display_name") or user.get("username", "")
        form_data = {
            "title": request.form.get("title", "").strip(),
            "category": request.form.get("category", "").strip(),
            "abstract": request.form.get("abstract", "").strip(),
            "body": sanitize_news_body(request.form.get("body", "").strip()),
            "author": request.form.get("author", "").strip() or display_name,
            "image_url": "",
            "status": "",
        }

        if request.method == "POST":
            action = request.form.get("action", "publish")
            is_draft = action == "draft"

            # Drafts require only a title; publish requires the full set.
            if not form_data["title"]:
                flash(_("Please enter a title."), "warning")
            elif not is_draft and not form_data["category"]:
                flash(_("Please select a category."), "warning")
            elif not is_draft and not form_data["abstract"]:
                flash(_("Please enter an abstract."), "warning")
            elif not is_draft and not form_data["body"]:
                flash(_("Please write the article body."), "warning")
            else:
                article_id = uuid4().hex[:12]
                image_url = ""
                cover_file = request.files.get("cover_image")
                if cover_file and cover_file.filename:
                    img_ext = cover_file.filename.rsplit(".", 1)[-1].lower() if "." in cover_file.filename else ""
                    if img_ext in ALLOWED_IMAGE_EXTENSIONS:
                        NEWS_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
                        safe_name = f"{article_id}_{secure_filename(cover_file.filename)}"
                        cover_file.save(NEWS_IMAGES_DIR / safe_name)
                        image_url = url_for("static", filename=f"uploads/news/{safe_name}")
                    else:
                        flash(_("Cover image must be PNG, JPG, GIF, or WebP."), "warning")
                        return render_template(
                            "news_publish.html",
                            form_data=form_data,
                            categories=load_categories(),
                            editing=False,
                            user=user,
                        )
                article = {
                    "id": article_id,
                    "title": form_data["title"],
                    "category": form_data["category"],
                    "abstract": form_data["abstract"],
                    "body": form_data["body"],
                    "author": form_data["author"],
                    "image_url": image_url,
                    "published_at": "" if is_draft else datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                    "status": "pending" if is_draft else "published",
                }
                save_news_article(article)
                if is_draft:
                    flash(_("Draft saved."), "success")
                else:
                    flash(_("Article published successfully."), "success")
                return redirect(url_for("news_manage"))

        return render_template(
            "news_publish.html",
            form_data=form_data,
            categories=load_categories(),
            editing=False,
            user=user,
        )

    @app.route("/dashboard/news/<news_id>/edit", methods=["GET", "POST"])
    def news_edit(news_id: str):
        user = require_login(level=2)
        if not user:
            target = url_for("login") if get_active_user() is None else url_for("dashboard")
            return redirect(target)

        article = get_news_article(news_id)
        if not article:
            flash(_("Article not found."), "warning")
            return redirect(url_for("news_list"))

        form_data = {
            "title": article["title"],
            "category": article["category"],
            "abstract": article["abstract"],
            "body": article["body"],
            "author": article["author"],
            "image_url": article["image_url"],
            "status": article.get("status", "published"),
        }

        if request.method == "POST":
            action = request.form.get("action", "publish")
            is_draft = action == "draft"
            form_data = {
                "title": request.form.get("title", "").strip(),
                "category": request.form.get("category", "").strip(),
                "abstract": request.form.get("abstract", "").strip(),
                "body": sanitize_news_body(request.form.get("body", "").strip()),
                "author": request.form.get("author", "").strip(),
                "image_url": article["image_url"],
                "status": article.get("status", "published"),
            }
            if not form_data["title"]:
                flash(_("Please enter a title."), "warning")
            elif not is_draft and not form_data["category"]:
                flash(_("Please select a category."), "warning")
            elif not is_draft and not form_data["abstract"]:
                flash(_("Please enter an abstract."), "warning")
            elif not is_draft and not form_data["body"]:
                flash(_("Please write the article body."), "warning")
            else:
                cover_file = request.files.get("cover_image")
                if cover_file and cover_file.filename:
                    img_ext = cover_file.filename.rsplit(".", 1)[-1].lower() if "." in cover_file.filename else ""
                    if img_ext in ALLOWED_IMAGE_EXTENSIONS:
                        NEWS_IMAGES_DIR.mkdir(parents=True, exist_ok=True)
                        safe_name = f"{news_id}_{secure_filename(cover_file.filename)}"
                        cover_file.save(NEWS_IMAGES_DIR / safe_name)
                        form_data["image_url"] = url_for("static", filename=f"uploads/news/{safe_name}")
                    else:
                        flash(_("Cover image must be PNG, JPG, GIF, or WebP."), "warning")
                        return render_template(
                            "news_publish.html",
                            form_data=form_data,
                            categories=load_categories(),
                            editing=True,
                            user=user,
                        )
                if request.form.get("remove_image") == "1":
                    form_data["image_url"] = ""
                form_data["status"] = "pending" if is_draft else "published"
                update_news_article(news_id, form_data)
                if is_draft:
                    flash(_("Draft saved."), "success")
                else:
                    flash(_("Article updated."), "success")
                return redirect(url_for("news_manage"))

        return render_template(
            "news_publish.html",
            form_data=form_data,
            categories=load_categories(),
            editing=True,
            user=user,
        )

    @app.route("/dashboard/news/<news_id>/delete", methods=["POST"])
    def news_delete(news_id: str):
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        if delete_news_article(news_id):
            flash(_("Article deleted."), "success")
        else:
            flash(_("Article not found."), "warning")
        return redirect(url_for("news_manage"))

    @app.route("/dashboard/news/manage")
    def news_manage():
        user = require_login(level=2)
        if not user:
            return redirect(url_for("login"))
        articles = load_news_articles()
        return render_template("news_manage.html", articles=articles, user=user, categories=load_categories())

    # ---------- Category management API ----------
    @app.route("/dashboard/news/categories/add", methods=["POST"], endpoint="news_categories_add")
    def news_category_add():
        user = require_login(level=2)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        error = category_name_validation_error(name)
        if error == "required":
            return jsonify(error=str(_("Category name is required."))), 400
        if error == "too_long":
            return jsonify(error=str(_("Category name must be 50 characters or fewer."))), 400
        if error:
            return jsonify(error=str(_("Category name contains unsupported characters."))), 400
        cats = load_categories()
        if name in cats:
            return jsonify(error=str(_("Category already exists."))), 409
        cats.append(name)
        save_categories(cats)
        return jsonify(categories=cats)

    @app.route("/dashboard/news/categories/rename", methods=["POST"], endpoint="news_categories_rename")
    def news_category_rename():
        user = require_login(level=2)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.json or {}
        old_name = data.get("old_name", "").strip()
        new_name = data.get("new_name", "").strip()
        if not old_name or not new_name:
            return jsonify(error=str(_("Both old and new names are required."))), 400
        error = category_name_validation_error(new_name)
        if error == "too_long":
            return jsonify(error=str(_("Category name must be 50 characters or fewer."))), 400
        if error:
            return jsonify(error=str(_("Category name contains unsupported characters."))), 400
        cats = load_categories()
        if old_name not in cats:
            return jsonify(error=str(_("Category not found."))), 404
        if new_name in cats:
            return jsonify(error=str(_("A category with that name already exists."))), 409
        cats[cats.index(old_name)] = new_name
        save_categories(cats)
        # Also update existing articles that use the old category name
        articles = load_news_articles()
        changed = False
        for art in articles:
            if art.get("category") == old_name:
                art["category"] = new_name
                changed = True
        if changed:
            with db_session() as db:
                for art in articles:
                    if art.get("category") == new_name:
                        db_art = db.query(NewsArticleModel).filter_by(id=art.get("id")).first()
                        if db_art:
                            db_art.category = new_name
                db.commit()
        return jsonify(categories=cats)

    @app.route("/dashboard/news/categories/delete", methods=["POST"], endpoint="news_categories_delete")
    def news_category_delete():
        user = require_login(level=2)
        if not user:
            return jsonify(error="Unauthorized"), 401
        name = (request.json or {}).get("name", "").strip()
        if not name:
            return jsonify(error=str(_("Category name is required."))), 400
        cats = load_categories()
        if name not in cats:
            return jsonify(error=str(_("Category not found."))), 404
        cats.remove(name)
        save_categories(cats)
        return jsonify(categories=cats)

    @app.route("/dashboard/news/bulk_action", methods=["POST"], endpoint="news_bulk_action")
    def news_bulk_action():
        user = require_login(level=2)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.get_json(silent=True) or {}
        ids = [str(x) for x in (data.get("ids") or [])]
        op = data.get("op")
        if op not in {"publish", "unpublish", "delete"}:
            return jsonify(error="bad op"), 400
        affected = 0
        with db_session() as db:
            rows = db.query(NewsArticleModel).filter(NewsArticleModel.id.in_(ids)).all()
            for r in rows:
                if op == "publish":
                    r.status = "published"
                    if not r.published_at:
                        r.published_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
                elif op == "unpublish":
                    r.status = "pending"
                elif op == "delete":
                    db.delete(r)
                affected += 1
            db.commit()
        return jsonify(ok=True, affected=affected)

    # ---------- Legacy redirects (curator news routes) ----------
    @app.route("/news/publish", endpoint="news_publish_legacy")
    def news_publish_legacy():
        return redirect(url_for("news_publish"), code=301)

    @app.route("/news/<news_id>/edit", endpoint="news_edit_legacy")
    def news_edit_legacy(news_id):
        return redirect(url_for("news_edit", news_id=news_id), code=301)

    @app.route("/news/manage", endpoint="news_manage_legacy")
    def news_manage_legacy():
        return redirect(url_for("news_manage"), code=301)

    @app.route("/news/<news_id>")
    def news_detail(news_id: str):
        article = get_news_article(news_id)
        if not article:
            flash(_("Article not found."), "warning")
            return redirect(url_for("news_list"))
        if article.get("status") == "pending":
            viewer = get_active_user()
            try:
                viewer_role = int(viewer.get("role", "1")) if viewer else 0
            except (TypeError, ValueError):
                viewer_role = 0
            if viewer_role < 2:
                flash(_("Article not found."), "warning")
                return redirect(url_for("news_list"))
        all_articles = load_news_articles(status="published")
        related = [a for a in all_articles if a.get("id") != news_id][:3]
        return render_template("news_article.html", article=article, related=related)
