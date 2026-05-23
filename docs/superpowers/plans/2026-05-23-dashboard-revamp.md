# Dashboard Revamp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the existing dashboard with a sidebar-driven SPA-style user hub that partial-loads sidebar destinations into the main panel, and add a news draft/publish workflow that the new dashboard stats depend on.

**Architecture:** A `_bare.html` partial layout is rendered when requests carry the `X-Partial-Content: 1` header; dashboard sidebar links fetch their targets with that header and JavaScript swaps the response into the main panel. The existing `dashboard()` route is rewritten to compute role-gated stats (papers, pending reviews, published news, pending-news drafts). News articles gain a `status` column (`'published'` / `'pending'`) so curators can save drafts via a new "Save as Draft" button next to the existing "Publish" button on `news_publish.html`; drafts appear in `news_manage.html` with a "Draft" badge and remain editable.

**Tech Stack:** Flask + SQLAlchemy + Jinja2 + Flask-Babel + vanilla JS (no build step) + Python `unittest` AST/template contract tests.

**Bundle source (read-only):** `/tmp/dashboard_revamp/` — already unzipped from `Dashboard_revamp.zip`. INTEGRATION.md inside it has the original integration guide; this plan is the adapted, codebase-specific version.

**Working tree note:** This plan touches `app.py`, ~14 templates, 2 new static files, and 2 translation catalogs. Frequent commits between tasks.

---

## File Structure

### New files
- `templates/_bare.html` — minimal layout for partial responses (no `<html>`, header, footer; just flashes + `{% block content %}`).
- `templates/_dashboard/overview.html` — welcome panel (hero, role-gated stats row, quick actions); rendered inline by `dashboard.html` and as a partial by the dashboard route.
- `static/css/dashboard.css` — sidebar + main panel styles (~17 KB, copied from bundle unchanged).
- `static/js/dashboard.js` — sidebar cycle + fetch-and-swap + form interception (~6 KB, copied from bundle unchanged).
- `tests/test_news_status_contract.py` — AST/template contract tests for the news draft workflow.
- `tests/test_dashboard_revamp_contract.py` — AST/template contract tests for the dashboard route, overview partial, and shell.
- `tests/test_partial_request_contract.py` — AST tests for `is_partial_request` helper and partial-aware template `extends` switches.

### Modified files
- `app.py` — schema (`NEWS_FIELDS` + `NewsArticleModel.status`), `init_db()` migration, `load_news_articles()` filter, `news_publish()` + `news_edit()` action branching, public news views filtered to `published`, `is_partial_request()` helper + `inject_partial_flag` context processor, `dashboard()` route rewrite.
- `templates/dashboard.html` — full replacement with bundle's sidebar shell.
- `templates/news_publish.html` — dual submit buttons (Save as Draft / Publish Article); JS submit-shortcut fix.
- `templates/news_manage.html` — Status column with "Draft" / "Published" pill.
- `templates/base.html` — remove the duplicated "Upload" top-nav link.
- Eleven sidebar-destination templates (one-line `extends` switch each):
  `upload.html`, `my_submissions.html`, `review_list.html`, `review_paper.html`, `delete.html`, `paper_manage.html`, `news_publish.html`, `news_manage.html`, `admin_users.html`, `guide_manage.html`, `change_password.html`.
- `translations/en/LC_MESSAGES/messages.po` and `translations/zh/LC_MESSAGES/messages.po` — new strings.

---

## Phase 0 — News draft / publish workflow

The dashboard's "Pending news" stat depends on `status` existing, so this phase ships first.

### Task 0.1: Add `status` to news schema

**Files:**
- Modify: `app.py:52-53` (NEWS_FIELDS constant)
- Modify: `app.py:483-492` (NewsArticleModel)
- Modify: `app.py` `init_db()` function (idempotent ALTER)
- Test: `tests/test_news_status_contract.py` (new file)

- [ ] **Step 1: Write the failing test**

Create `tests/test_news_status_contract.py`:

```python
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class NewsStatusSchemaContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_news_fields_includes_status(self):
        # NEWS_FIELDS is the canonical list used by load/save helpers.
        self.assertIn('"status"', self._find_assignment_source("NEWS_FIELDS"))

    def test_news_article_model_has_status_column(self):
        model_src = self._find_class_source("NewsArticleModel")
        self.assertIn("status = Column", model_src)
        # Default must be 'published' so existing rows remain visible.
        self.assertIn('default="published"', model_src)

    def test_init_db_runs_idempotent_status_migration(self):
        init_db = self._find_function_source("init_db")
        self.assertIn("ALTER TABLE news_articles ADD COLUMN status", init_db)

    def _find_assignment_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == name:
                        return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find assignment for {name}")

    def _find_class_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.ClassDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find class {name}")

    def _find_function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find function {name}")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_news_status_contract.py -v`
Expected: 3 failures — NEWS_FIELDS does not contain `"status"`, model has no `status` column, init_db has no ALTER for it.

- [ ] **Step 3: Add `status` to `NEWS_FIELDS`**

Edit `app.py:53`:

```python
NEWS_FIELDS = ["id", "title", "category", "abstract", "body", "author", "image_url", "published_at", "status"]
```

- [ ] **Step 4: Add `status` column to `NewsArticleModel`**

Edit `app.py:483-492`. Add the `status` column at the end of the class body:

```python
class NewsArticleModel(BASE):
    __tablename__ = "news_articles"
    id = Column(Unicode(255), primary_key=True)
    title = Column(Unicode(255))
    category = Column(Unicode(255))
    abstract = Column(UnicodeText)
    body = Column(UnicodeText)
    author = Column(Unicode(255))
    image_url = Column(Unicode(255))
    published_at = Column(Unicode(255))
    status = Column(Unicode(20), default="published")
```

- [ ] **Step 5: Add idempotent ALTER in `init_db()`**

Find `init_db()` in `app.py` (`grep -n "def init_db" app.py`). Inside the function, alongside the other `try/except` ALTER blocks, append:

```python
try:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE news_articles ADD COLUMN status VARCHAR(20) DEFAULT 'published'"))
        conn.execute(text("UPDATE news_articles SET status = 'published' WHERE status IS NULL OR status = ''"))
except Exception:
    pass
```

Use whatever connection / `text(...)` pattern the existing ALTER blocks in `init_db()` already use — copy that style verbatim. If the file imports `text` from sqlalchemy already, reuse it; otherwise add the import next to the other sqlalchemy imports.

- [ ] **Step 6: Run test to verify it passes**

Run: `python -m unittest tests/test_news_status_contract.py -v`
Expected: 3 tests pass.

- [ ] **Step 7: Run the full test suite for regressions**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add app.py tests/test_news_status_contract.py
git commit -m "feat(news): add status column to news articles for draft/publish workflow"
```

---

### Task 0.2: Filter `load_news_articles` by status

**Files:**
- Modify: `app.py:3788-3794` (`load_news_articles` function)
- Test: `tests/test_news_status_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append a new test class to `tests/test_news_status_contract.py`:

```python
class LoadNewsArticlesFilterContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_load_news_articles_accepts_status_filter(self):
        fn = None
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == "load_news_articles":
                fn = node
                break
        self.assertIsNotNone(fn, "load_news_articles not found")
        arg_names = [a.arg for a in fn.args.args]
        self.assertIn("status", arg_names)
        src = ast.get_source_segment(self.app_source, fn)
        self.assertIn("filter_by(status=", src)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_news_status_contract.py::LoadNewsArticlesFilterContractTest -v`
Expected: FAIL — function has no `status` parameter.

- [ ] **Step 3: Modify `load_news_articles`**

Edit `app.py:3788-3794`:

```python
def load_news_articles(status: Optional[str] = None) -> List[Dict[str, str]]:
    """Return news articles sorted by published_at descending.

    When status is provided, only articles with that status are returned.
    With status=None (default), all articles are returned regardless of status.
    """
    with db_session() as db:
        query = db.query(NewsArticleModel)
        if status:
            query = query.filter_by(status=status)
        articles = query.all()
        rows = [{field: (getattr(a, field) or "") for field in NEWS_FIELDS} for a in articles]
        rows.sort(key=lambda r: r.get("published_at", ""), reverse=True)
        return rows
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_news_status_contract.py -v`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_news_status_contract.py
git commit -m "feat(news): support filtering load_news_articles by status"
```

---

### Task 0.3: Filter public news views to `status='published'`

**Files:**
- Modify: `app.py:697` (landing latest news)
- Modify: `app.py:1869-1875` (news_list route)
- Modify: `app.py:2513-2520` (news_detail route — gate drafts)
- Test: `tests/test_news_status_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_news_status_contract.py`:

```python
class PublicNewsViewsFilterContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _find_function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find function {name}")

    def test_news_list_filters_to_published(self):
        src = self._find_function_source("news_list")
        self.assertIn('load_news_articles(status="published")', src.replace("'", '"'))

    def test_news_detail_blocks_drafts_from_non_editors(self):
        src = self._find_function_source("news_detail")
        # Must check status and require role >= 2 (editor/admin) to view drafts.
        self.assertIn('"status"', src.replace("'", '"'))
        self.assertIn("pending", src)

    def test_landing_index_filters_latest_news_to_published(self):
        # The landing route is index(); it pulls latest_news from load_news_articles.
        src = self._find_function_source("index")
        self.assertIn('load_news_articles(status="published")', src.replace("'", '"'))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_news_status_contract.py::PublicNewsViewsFilterContractTest -v`
Expected: 3 failures.

- [ ] **Step 3: Update landing route (`app.py:697`)**

Find the line `latest_news = load_news_articles()[:4]` and change to:

```python
latest_news = load_news_articles(status="published")[:4]
```

- [ ] **Step 4: Update `news_list` (`app.py:1875`)**

Find the line `all_articles = load_news_articles()` inside `news_list()` and change to:

```python
all_articles = load_news_articles(status="published")
```

- [ ] **Step 5: Gate drafts in `news_detail`**

Find `news_detail` (around `app.py:2513`). After the article is loaded (look for the line that finds the article by id; the existing code iterates `all_articles = load_news_articles()`), add a draft guard. The full route handler should:

1. Continue using `load_news_articles()` (no filter — we need to find by id regardless of status).
2. After locating the matching article dict, if `article.get("status") == "pending"`:
   - Check `get_active_user()` and read the role from the session user dict (`int(user.get("role", "1"))`).
   - If no user or role < 2, `flash(_("Article not found."), "warning")` and `return redirect(url_for("news_list"))`.

Concretely, after the existing article-lookup block, insert:

```python
if article and article.get("status") == "pending":
    viewer = get_active_user()
    viewer_role = int(viewer.get("role", "1")) if viewer else 0
    if viewer_role < 2:
        flash(_("Article not found."), "warning")
        return redirect(url_for("news_list"))
```

Place this BEFORE the rendering branch. The exact variable name (`article` vs whatever the code uses) — match what's in the current function body; read `app.py:2513-2570` to see.

- [ ] **Step 6: Run test to verify it passes**

Run: `python -m unittest tests/test_news_status_contract.py::PublicNewsViewsFilterContractTest -v`
Expected: 3 tests pass.

- [ ] **Step 7: Run full suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add app.py tests/test_news_status_contract.py
git commit -m "feat(news): hide drafts from public news views and gate detail access"
```

---

### Task 0.4: Branch `news_publish` and `news_edit` on submit action

**Files:**
- Modify: `app.py:1904-1968` (news_publish)
- Modify: `app.py:1970-2038` (news_edit)
- Modify: `app.py:3811-3822` (update_news_article — allow status updates)
- Test: `tests/test_news_status_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_news_status_contract.py`:

```python
class NewsPublishActionContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _find_function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        self.fail(f"Could not find function {name}")

    def test_news_publish_reads_action_and_sets_status(self):
        src = self._find_function_source("news_publish")
        self.assertIn('request.form.get("action"', src)
        self.assertIn('"pending"', src)
        self.assertIn('"published"', src)

    def test_news_publish_allows_draft_with_only_title(self):
        # Drafts skip the abstract/category/body validation.
        src = self._find_function_source("news_publish")
        self.assertIn("action == \"draft\"", src.replace("'", '"'))

    def test_news_edit_reads_action_and_sets_status(self):
        src = self._find_function_source("news_edit")
        self.assertIn('request.form.get("action"', src)
        self.assertIn('"pending"', src)
        self.assertIn('"published"', src)

    def test_update_news_article_can_update_status(self):
        src = self._find_function_source("update_news_article")
        # status must not be in the skip list.
        # The function currently skips id and published_at; ensure status is settable.
        # We do that by checking that 'status' isn't listed in the skip tuple.
        self.assertNotRegex(src, r"if field in \([^)]*['\"]status['\"]")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_news_status_contract.py::NewsPublishActionContractTest -v`
Expected: 3-4 failures.

- [ ] **Step 3: Rewrite `news_publish` body**

Edit `app.py:1904-1968`. Replace the entire function body with:

```python
def news_publish():
    user = require_login(level=2)
    if not user:
        target = url_for("login") if not session.get("user") else url_for("dashboard")
        return redirect(target)

    display_name = user.get("display_name") or user.get("username", "")
    form_data = {
        "title": request.form.get("title", "").strip(),
        "category": request.form.get("category", "").strip(),
        "abstract": request.form.get("abstract", "").strip(),
        "body": request.form.get("body", "").strip(),
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
                    )
            article = {
                "id": article_id,
                "title": form_data["title"],
                "category": form_data["category"],
                "abstract": form_data["abstract"],
                "body": form_data["body"],
                "author": form_data["author"],
                "image_url": image_url,
                "published_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                "status": "pending" if is_draft else "published",
            }
            save_news_article(article)
            if is_draft:
                flash(_("Draft saved."), "success")
                return redirect(url_for("news_manage"))
            flash(_("Article published successfully."), "success")
            return redirect(url_for("news_list"))

    return render_template(
        "news_publish.html",
        form_data=form_data,
        categories=load_categories(),
        editing=False,
    )
```

- [ ] **Step 4: Rewrite `news_edit` body**

Edit `app.py:1970-2038`. Replace the function body with:

```python
def news_edit(news_id: str):
    user = require_login(level=2)
    if not user:
        target = url_for("login") if not session.get("user") else url_for("dashboard")
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
            "body": request.form.get("body", "").strip(),
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
                    )
            if request.form.get("remove_image") == "1":
                form_data["image_url"] = ""
            form_data["status"] = "pending" if is_draft else "published"
            update_news_article(news_id, form_data)
            if is_draft:
                flash(_("Draft saved."), "success")
                return redirect(url_for("news_manage"))
            flash(_("Article updated."), "success")
            return redirect(url_for("news_list"))

    return render_template(
        "news_publish.html",
        form_data=form_data,
        categories=load_categories(),
        editing=True,
    )
```

- [ ] **Step 5: Allow `update_news_article` to update `status`**

Edit `app.py:3811-3822`. The existing function skips `id` and `published_at` when updating. `status` should be settable. Replace with:

```python
def update_news_article(article_id: str, data: Dict[str, str]) -> bool:
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            for field in NEWS_FIELDS:
                if field in ("id", "published_at"):
                    continue
                if field in data:
                    setattr(article, field, data[field])
            # When transitioning from draft to published, refresh published_at.
            if data.get("status") == "published" and (article.published_at is None or article.published_at == ""):
                article.published_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            db.commit()
            return True
        return False
```

(The existing function already loops over `NEWS_FIELDS`, which now includes `status` thanks to Task 0.1, so the column is picked up automatically.)

- [ ] **Step 6: Run test to verify it passes**

Run: `python -m unittest tests/test_news_status_contract.py::NewsPublishActionContractTest -v`
Expected: all tests pass.

- [ ] **Step 7: Run full suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add app.py tests/test_news_status_contract.py
git commit -m "feat(news): branch publish/edit on draft vs publish action"
```

---

### Task 0.5: Dual submit buttons in `news_publish.html`

**Files:**
- Modify: `templates/news_publish.html:968-973` (editor-actions buttons)
- Modify: `templates/news_publish.html:1543-1548` (Ctrl/⌘+Enter handler)
- Test: `tests/test_news_status_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_news_status_contract.py`:

```python
class NewsPublishTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = (ROOT / "templates" / "news_publish.html").read_text(encoding="utf-8")

    def test_template_has_save_as_draft_button(self):
        self.assertIn('name="action" value="draft"', self.template)

    def test_template_has_publish_action_button(self):
        self.assertIn('name="action" value="publish"', self.template)

    def test_ctrl_enter_triggers_publish_button(self):
        # The Ctrl/Cmd+Enter shortcut must click the publish button (not the draft button),
        # since requestSubmit() without a specific button omits the action field.
        self.assertIn("btnPublish", self.template)
        self.assertIn("getElementById('btnPublish').click()", self.template)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_news_status_contract.py::NewsPublishTemplateContractTest -v`
Expected: 3 failures.

- [ ] **Step 3: Replace the editor actions block (lines 968–973)**

In `templates/news_publish.html`, find the block:

```jinja
<div class="editor-actions">
    <a class="btn-ed" href="{{ url_for('news_list') }}">{{ _('Cancel') }}</a>
    <button type="submit" class="btn-ed primary" id="btnPublish">
        {% if editing %}{{ _('Save Changes') }}{% else %}{{ _('Publish Article') }}{% endif %}
    </button>
</div>
```

Replace with:

```jinja
<div class="editor-actions">
    <a class="btn-ed" href="{{ url_for('news_list') }}">{{ _('Cancel') }}</a>
    <button type="submit" name="action" value="draft" class="btn-ed" id="btnDraft">
        {% if editing and form_data.status == 'pending' %}{{ _('Save Draft') }}{% else %}{{ _('Save as Draft') }}{% endif %}
    </button>
    <button type="submit" name="action" value="publish" class="btn-ed primary" id="btnPublish">
        {% if editing %}{{ _('Publish Changes') }}{% else %}{{ _('Publish Article') }}{% endif %}
    </button>
</div>
```

- [ ] **Step 4: Fix the Ctrl/⌘+Enter handler (line 1543–1548)**

Find:

```javascript
document.addEventListener('keydown', function (e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        document.getElementById('publishForm').requestSubmit();
    }
});
```

Replace with:

```javascript
document.addEventListener('keydown', function (e) {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        document.getElementById('btnPublish').click();
    }
});
```

(Using `.click()` on the specific button ensures the form submission carries `action=publish`. `requestSubmit()` without a button argument omits it.)

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m unittest tests/test_news_status_contract.py::NewsPublishTemplateContractTest -v`
Expected: 3 tests pass.

- [ ] **Step 6: Commit**

```bash
git add templates/news_publish.html tests/test_news_status_contract.py
git commit -m "feat(news): add Save as Draft button alongside Publish"
```

---

### Task 0.6: Status column in `news_manage.html`

**Files:**
- Modify: `templates/news_manage.html:456-493` (table head + body)
- Test: `tests/test_news_status_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_news_status_contract.py`:

```python
class NewsManageTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = (ROOT / "templates" / "news_manage.html").read_text(encoding="utf-8")

    def test_table_has_status_header(self):
        # Must show a Status column header.
        self.assertRegex(self.template, r"<th[^>]*>\s*\{\{\s*_\(\s*'Status'\s*\)\s*\}\}\s*</th>")

    def test_table_renders_status_pill(self):
        # Body must reference item.status and render distinct pills for published vs pending.
        self.assertIn("item.status", self.template)
        self.assertIn("'pending'", self.template)
        self.assertIn("'published'", self.template)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_news_status_contract.py::NewsManageTemplateContractTest -v`
Expected: 3 failures.

- [ ] **Step 3: Add Status column to the table**

In `templates/news_manage.html`, locate the table head (around line 457-466):

```jinja
<thead class="table-light">
    <tr>
        <th style="min-width:300px">{{ _('Title') }}</th>
        <th>{{ _('Category') }}</th>
        <th>{{ _('Author') }}</th>
        <th>{{ _('Published') }}</th>
        <th class="text-end" style="min-width:150px">{{ _('Actions') }}</th>
    </tr>
</thead>
```

Replace with (insert Status between Category and Author):

```jinja
<thead class="table-light">
    <tr>
        <th style="min-width:300px">{{ _('Title') }}</th>
        <th>{{ _('Category') }}</th>
        <th>{{ _('Status') }}</th>
        <th>{{ _('Author') }}</th>
        <th>{{ _('Published') }}</th>
        <th class="text-end" style="min-width:150px">{{ _('Actions') }}</th>
    </tr>
</thead>
```

- [ ] **Step 4: Add status cell to each row**

Locate the row body (around lines 469-489):

```jinja
<tr>
    <td>
        <a href="{{ url_for('news_detail', news_id=item.id) }}" class="fw-semibold text-decoration-none">{{ item.title }}</a>
        ...
    </td>
    <td><span class="badge bg-secondary">{{ item.category }}</span></td>
    <td>{{ item.author }}</td>
    <td class="text-muted small">{{ item.published_at }}</td>
    <td class="text-end">
        ...
    </td>
</tr>
```

After the category `<td>` and before the author `<td>`, insert:

```jinja
<td>
    {% if item.status == 'pending' %}
    <span class="badge bg-warning text-dark">{{ _('Draft') }}</span>
    {% elif item.status == 'published' %}
    <span class="badge bg-success">{{ _('Published') }}</span>
    {% else %}
    <span class="badge bg-secondary">{{ item.status or _('Published') }}</span>
    {% endif %}
</td>
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m unittest tests/test_news_status_contract.py::NewsManageTemplateContractTest -v`
Expected: 3 tests pass.

- [ ] **Step 6: Run full suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add templates/news_manage.html tests/test_news_status_contract.py
git commit -m "feat(news): show draft status pill in news manage table"
```

---

### Task 0.7: Manual verification of the draft workflow

- [ ] **Step 1: Start the dev server**

Run: `./start_local.sh`

Expected: server boots without errors (the `init_db()` ALTER for `status` runs idempotently on first hit).

- [ ] **Step 2: Verify draft create + edit + publish flow**

As a curator (role ≥ 2):

1. Visit `/news/publish`. Fill in a title only. Click "Save as Draft". Expect redirect to `/news/manage`, flash "Draft saved.", new row shows the title with an amber "Draft" pill.
2. Click "Edit" on the draft. Fill in the rest of the fields. Click "Save as Draft" again. Expect another "Draft saved." flash; row still shows "Draft" pill.
3. Click "Edit" again. Click "Publish Changes". Expect "Article updated." flash and redirect to `/news`. The article appears in the public list. Back in `/news/manage` it now shows a green "Published" pill.

- [ ] **Step 3: Verify drafts hidden from public**

As a reader (role 1, or logged out):
1. Visit `/news` — no drafts listed.
2. Visit the draft's direct URL `/news/<id>` — flash "Article not found." and redirect to `/news`.

- [ ] **Step 4: Stop the dev server**

Ctrl-C the running server.

---

## Phase 1 — Partial-request plumbing

### Task 1.1: Add `is_partial_request` helper and context processor

**Files:**
- Modify: `app.py` (insert near the existing `inject_helpers` context processor at line 631)
- Test: `tests/test_partial_request_contract.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_partial_request_contract.py`:

```python
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PartialRequestContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def test_is_partial_request_helper_exists(self):
        found = any(
            isinstance(node, ast.FunctionDef) and node.name == "is_partial_request"
            for node in ast.walk(self.app_tree)
        )
        self.assertTrue(found, "is_partial_request helper not found")

    def test_helper_reads_x_partial_content_header(self):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == "is_partial_request":
                src = ast.get_source_segment(self.app_source, node)
                self.assertIn("X-Partial-Content", src)
                return
        self.fail("is_partial_request helper not found")

    def test_inject_partial_flag_context_processor_exists(self):
        found = any(
            isinstance(node, ast.FunctionDef) and node.name == "inject_partial_flag"
            for node in ast.walk(self.app_tree)
        )
        self.assertTrue(found, "inject_partial_flag context processor not found")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_partial_request_contract.py -v`
Expected: 3 failures.

- [ ] **Step 3: Add helper and context processor**

Find the `@app.context_processor` decorator for `inject_helpers` at `app.py:631`. Immediately AFTER that function (before the next `@app.context_processor` block at line 654), insert:

```python
    def is_partial_request():
        """True when the request carries X-Partial-Content: 1.

        Used by routes to render either the full base.html shell or just the
        inner content block via _bare.html, so the dashboard can fetch a route
        and swap its content into the main panel.
        """
        return request.headers.get("X-Partial-Content") == "1"

    @app.context_processor
    def inject_partial_flag():
        return {"partial": is_partial_request()}
```

Note the four-space indentation — it's inside `create_app()` like the other context processors.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_partial_request_contract.py -v`
Expected: 3 tests pass.

- [ ] **Step 5: Run full suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add app.py tests/test_partial_request_contract.py
git commit -m "feat(dashboard): add is_partial_request helper and partial template flag"
```

---

### Task 1.2: Add `_bare.html` partial layout

**Files:**
- Create: `templates/_bare.html`
- Test: `tests/test_partial_request_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_partial_request_contract.py`:

```python
class BareTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / "templates" / "_bare.html"

    def test_bare_template_exists(self):
        self.assertTrue(self.path.exists(), "_bare.html does not exist")

    def test_bare_template_has_content_block(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("{% block content %}{% endblock %}", src)

    def test_bare_template_renders_flash_messages(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("get_flashed_messages", src)

    def test_bare_template_has_no_html_tag(self):
        # The bare template must NOT render <html>, <head>, or <body> tags.
        src = self.path.read_text(encoding="utf-8")
        self.assertNotIn("<html", src.lower())
        self.assertNotIn("<head>", src.lower())
        self.assertNotIn("<body", src.lower())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_partial_request_contract.py::BareTemplateContractTest -v`
Expected: 4 failures.

- [ ] **Step 3: Copy `_bare.html` from the bundle**

Run: `cp /tmp/dashboard_revamp/templates/_bare.html /Users/mingyuanw/Desktop/Project/Keydion/templates/_bare.html`

The bundle file contents are:

```jinja
{# =============================================================
   templates/_bare.html — minimal layout used when a route is hit
   with the X-Partial-Content: 1 header. Emits the content block
   *only* — no <html>, no header, no footer.

   Pages that want to be partial-loadable into the dashboard shell
   should use:
       {% extends "_bare.html" if partial else "base.html" %}
   See INTEGRATION.md for the full pattern.
   ============================================================= #}
{# Render alert/flash messages inline so the user sees them in-panel. #}
{% with messages = get_flashed_messages(with_categories=True) %}
{% if messages %}
{% for category, message in messages %}
<div class="alert alert-{{ category }} alert-dismissible fade show" role="alert">
  {{ message }}
  <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>
{% endfor %}
{% endif %}
{% endwith %}

{% block content %}{% endblock %}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_partial_request_contract.py::BareTemplateContractTest -v`
Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add templates/_bare.html tests/test_partial_request_contract.py
git commit -m "feat(dashboard): add _bare.html layout for partial responses"
```

---

### Task 1.3: Make sidebar-destination templates partial-aware

**Files:**
- Modify (first line only) on each of:
  `templates/upload.html`, `templates/my_submissions.html`, `templates/review_list.html`, `templates/review_paper.html`, `templates/delete.html`, `templates/paper_manage.html`, `templates/news_publish.html`, `templates/news_manage.html`, `templates/admin_users.html`, `templates/guide_manage.html`, `templates/change_password.html`
- Test: `tests/test_partial_request_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_partial_request_contract.py`:

```python
class PartialAwareTemplatesContractTest(unittest.TestCase):
    TEMPLATES = [
        "upload.html",
        "my_submissions.html",
        "review_list.html",
        "review_paper.html",
        "delete.html",
        "paper_manage.html",
        "news_publish.html",
        "news_manage.html",
        "admin_users.html",
        "guide_manage.html",
        "change_password.html",
    ]

    def test_all_sidebar_destinations_extend_conditionally(self):
        expected = '{% extends "_bare.html" if partial else "base.html" %}'
        for name in self.TEMPLATES:
            path = ROOT / "templates" / name
            self.assertTrue(path.exists(), f"{name} missing")
            first_line = path.read_text(encoding="utf-8").splitlines()[0].strip()
            self.assertEqual(
                first_line,
                expected,
                f"{name} first line should be conditional extends, got: {first_line!r}",
            )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_partial_request_contract.py::PartialAwareTemplatesContractTest -v`
Expected: FAIL — none of the templates use the conditional extends yet.

- [ ] **Step 3: Update each template's first line**

For each of the 11 templates listed in the test, change the first line from:

```jinja
{% extends "base.html" %}
```

to:

```jinja
{% extends "_bare.html" if partial else "base.html" %}
```

Use the Edit tool with exact replacement on the first line. Do this 11 times — one per file.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_partial_request_contract.py::PartialAwareTemplatesContractTest -v`
Expected: PASS.

- [ ] **Step 5: Run full suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

- [ ] **Step 6: Manual smoke check (hard refresh still uses base.html)**

Run: `./start_local.sh`
- Visit `/upload` directly in the browser (no `X-Partial-Content` header). Expect the full layout with the global navbar and footer.
- Stop the server.

- [ ] **Step 7: Commit**

```bash
git add templates/upload.html templates/my_submissions.html templates/review_list.html \
        templates/review_paper.html templates/delete.html templates/paper_manage.html \
        templates/news_publish.html templates/news_manage.html templates/admin_users.html \
        templates/guide_manage.html templates/change_password.html \
        tests/test_partial_request_contract.py
git commit -m "feat(dashboard): make sidebar destinations partial-aware"
```

---

## Phase 2 — Dashboard core (route, partial, shell, assets)

### Task 2.1: Copy dashboard CSS and JS assets

**Files:**
- Create: `static/js/` directory (does not exist yet)
- Create: `static/css/dashboard.css`
- Create: `static/js/dashboard.js`
- Test: `tests/test_dashboard_revamp_contract.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/test_dashboard_revamp_contract.py`:

```python
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class DashboardAssetsContractTest(unittest.TestCase):
    def test_dashboard_css_exists(self):
        self.assertTrue((ROOT / "static" / "css" / "dashboard.css").exists())

    def test_dashboard_js_exists(self):
        self.assertTrue((ROOT / "static" / "js" / "dashboard.js").exists())

    def test_dashboard_js_intercepts_partial_links(self):
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertIn("X-Partial-Content", src)
        self.assertIn("data-partial-href", src)

    def test_dashboard_js_persists_sidebar_state(self):
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertIn("keydion.sidebar", src)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py -v`
Expected: 4 failures (paths don't exist).

- [ ] **Step 3: Create static/js directory and copy assets**

Run:

```bash
mkdir -p /Users/mingyuanw/Desktop/Project/Keydion/static/js
cp /tmp/dashboard_revamp/static/css/dashboard.css /Users/mingyuanw/Desktop/Project/Keydion/static/css/dashboard.css
cp /tmp/dashboard_revamp/static/js/dashboard.js /Users/mingyuanw/Desktop/Project/Keydion/static/js/dashboard.js
```

(Do NOT copy `keydion-base.css` or `dashboard-preview.js` — INTEGRATION.md flags those as preview-only.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py -v`
Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add static/css/dashboard.css static/js/dashboard.js tests/test_dashboard_revamp_contract.py
git commit -m "feat(dashboard): add sidebar styles and partial-load JS"
```

---

### Task 2.2: Add `_dashboard/overview.html` with role-1 modifications

**Files:**
- Create: `templates/_dashboard/overview.html`
- Test: `tests/test_dashboard_revamp_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_dashboard_revamp_contract.py`:

```python
class OverviewPartialContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = ROOT / "templates" / "_dashboard" / "overview.html"

    def test_overview_exists(self):
        self.assertTrue(self.path.exists())

    def test_overview_hides_stats_row_for_role_1(self):
        src = self.path.read_text(encoding="utf-8")
        # The stats-row block must be wrapped in {% if role > 1 %}.
        self.assertRegex(src, r"\{%\s*if\s+role\s*>\s*1\s*%\}\s*\n?\s*<section class=\"stats-row\"")

    def test_overview_role_1_quick_actions_are_upload_and_change_password_only(self):
        src = self.path.read_text(encoding="utf-8")
        # "My submissions" action card (the role==1 block) must be removed.
        self.assertNotIn("View submissions", src)
        # Upload research + Change password cards must remain.
        self.assertIn("Open uploader", src)
        self.assertIn("Update security", src)

    def test_overview_published_news_tile_uses_published_news_stat(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("Published news", src)
        self.assertIn("dashboard_stats.published_news", src)

    def test_overview_pending_news_tile_uses_pending_news_stat(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("Pending news", src)
        self.assertIn("dashboard_stats.pending_news", src)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py::OverviewPartialContractTest -v`
Expected: 5 failures.

- [ ] **Step 3: Create `templates/_dashboard/overview.html`**

Run: `mkdir -p /Users/mingyuanw/Desktop/Project/Keydion/templates/_dashboard`

Then create `templates/_dashboard/overview.html` with this content (the bundle template, modified to: hide stats for role 1, rename "Your uploads"→"Published news" / "News drafts"→"Pending news" with corresponding key changes, and remove the role-1 "My submissions" quick-action card):

```jinja
{# =============================================================
   templates/_dashboard/overview.html — default panel for the user hub.
   Rendered inline by dashboard.html on first paint, and re-rendered
   as a partial when the user clicks the "Overview" sidebar item.
   ============================================================= #}
{% set role = user.role|int %}
{% set display_name = user.display_name or (user.first_name ~ ' ' ~ user.last_name)|trim or user.email or user.username %}
{% set first_name = display_name.split(' ')[0] %}

<article class="panel panel--overview" data-screen-label="Overview">
  <header class="panel-hero">
    <div class="panel-hero__text">
      <h1 class="panel-hero__title">{{ _('Hello, %(name)s.', name=first_name) }}</h1>
      <p class="panel-hero__sub">{{ _('Manage your account access, uploads, and the Keydion library.') }}</p>
    </div>
    <div class="panel-hero__meta">
      <span class="role-pill">{{ role_label(role) }}</span>
      {% if user.expiry_date %}
      <span class="role-expiry">{{ _('Access valid through %(date)s', date=user.expiry_date) }}</span>
      {% endif %}
    </div>
  </header>

  {% if role > 1 and dashboard_stats %}
  <section class="stats-row" aria-label="{{ _('At a glance') }}">
    {% if role >= 3 and dashboard_stats.papers_in_library is defined %}
    <a class="stat-tile" href="{{ url_for('manage') }}" data-partial-href="{{ url_for('manage') }}">
      <div class="stat-tile__label">{{ _('Papers in library') }}</div>
      <div class="stat-tile__value">{{ '{:,}'.format(dashboard_stats.papers_in_library) }}</div>
      {% if dashboard_stats.papers_delta_label %}
      <div class="stat-tile__delta">{{ dashboard_stats.papers_delta_label }}</div>
      {% endif %}
    </a>
    {% endif %}

    {% if role >= 2 and dashboard_stats.pending_reviews is defined %}
    <a class="stat-tile {% if dashboard_stats.pending_reviews > 0 %}stat-tile--alert{% endif %}"
       href="{{ url_for('review_list') }}" data-partial-href="{{ url_for('review_list') }}">
      <div class="stat-tile__label">{{ _('Pending reviews') }}</div>
      <div class="stat-tile__value">{{ dashboard_stats.pending_reviews }}</div>
      {% if dashboard_stats.pending_oldest_label %}
      <div class="stat-tile__delta">{{ dashboard_stats.pending_oldest_label }}</div>
      {% endif %}
    </a>
    {% endif %}

    {% if role >= 2 and dashboard_stats.published_news is defined %}
    <a class="stat-tile" href="{{ url_for('news_list') }}" data-partial-href="{{ url_for('news_list') }}">
      <div class="stat-tile__label">{{ _('Published news') }}</div>
      <div class="stat-tile__value">{{ dashboard_stats.published_news }}</div>
    </a>
    {% endif %}

    {% if role >= 2 and dashboard_stats.pending_news is defined %}
    <a class="stat-tile" href="{{ url_for('news_manage') }}" data-partial-href="{{ url_for('news_manage') }}">
      <div class="stat-tile__label">{{ _('Pending news') }}</div>
      <div class="stat-tile__value">{{ dashboard_stats.pending_news }}</div>
    </a>
    {% endif %}
  </section>
  {% endif %}

  <section class="quick-actions" aria-label="{{ _('Quick actions') }}">
    <div class="section-head">
      <h2>{{ _('Quick actions') }}</h2>
    </div>
    <div class="quick-actions__grid">
      <a class="action-card" href="{{ url_for('upload') }}" data-partial-href="{{ url_for('upload') }}">
        <div class="action-card__title">{{ _('Upload research') }}</div>
        <p class="action-card__body">{{ _('Share a new paper for peers to explore. Supports PDF with metadata extraction.') }}</p>
        <span class="action-card__cta">{{ _('Open uploader') }} →</span>
      </a>

      {% if role >= 2 %}
      <a class="action-card" href="{{ url_for('review_list') }}" data-partial-href="{{ url_for('review_list') }}">
        <div class="action-card__title">{{ _('Review the queue') }}</div>
        <p class="action-card__body">
          {% if dashboard_stats and dashboard_stats.pending_reviews %}
          {{ _('%(n)d submissions are waiting for editorial decision.', n=dashboard_stats.pending_reviews) }}
          {% else %}
          {{ _('Accept or reject papers submitted by readers.') }}
          {% endif %}
        </p>
        <span class="action-card__cta">{{ _('Open queue') }} →</span>
      </a>
      {% endif %}

      {% if role >= 2 %}
      <a class="action-card" href="{{ url_for('news_publish') }}" data-partial-href="{{ url_for('news_publish') }}">
        <div class="action-card__title">{{ _('Write an article') }}</div>
        <p class="action-card__body">{{ _('Publish updates, announcements and stories for the Keydion community.') }}</p>
        <span class="action-card__cta">{{ _('Open editor') }} →</span>
      </a>
      {% endif %}

      {% if role >= 3 %}
      <a class="action-card" href="{{ url_for('admin_users') }}" data-partial-href="{{ url_for('admin_users') }}">
        <div class="action-card__title">{{ _('Manage users') }}</div>
        <p class="action-card__body">{{ _('Review local and Microsoft-linked accounts, adjust roles, set passwords.') }}</p>
        <span class="action-card__cta">{{ _('Open admin') }} →</span>
      </a>
      {% endif %}

      <a class="action-card" href="{{ url_for('change_password') }}" data-partial-href="{{ url_for('change_password') }}">
        <div class="action-card__title">{{ _('Change password') }}</div>
        <p class="action-card__body">{{ _('Set or change your account password.') }}</p>
        <span class="action-card__cta">{{ _('Update security') }} →</span>
      </a>
    </div>
  </section>
</article>
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py::OverviewPartialContractTest -v`
Expected: 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add templates/_dashboard/overview.html tests/test_dashboard_revamp_contract.py
git commit -m "feat(dashboard): add overview panel with role-gated stats"
```

---

### Task 2.3: Replace `templates/dashboard.html` with the sidebar shell

**Files:**
- Modify: `templates/dashboard.html` (full replacement)
- Test: `tests/test_dashboard_revamp_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_dashboard_revamp_contract.py`:

```python
class DashboardShellTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = (ROOT / "templates" / "dashboard.html").read_text(encoding="utf-8")

    def test_shell_extends_base(self):
        self.assertIn('{% extends "base.html" %}', self.src)

    def test_shell_loads_dashboard_assets(self):
        self.assertIn("static/css/dashboard.css", self.src.replace("'", "\"")) or self.assertIn(
            "filename='css/dashboard.css'", self.src
        )
        self.assertIn("dashboard.js", self.src)

    def test_shell_includes_overview_partial(self):
        self.assertIn('include "_dashboard/overview.html"', self.src)

    def test_shell_has_sidebar_groups(self):
        # Workspace + Account are always present; others gated by role in template.
        self.assertIn("'Workspace'", self.src)
        self.assertIn("'Account'", self.src)

    def test_shell_links_use_data_partial_href(self):
        # Sidebar nav items must opt into partial loading.
        self.assertIn("data-partial-href", self.src)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py::DashboardShellTemplateContractTest -v`
Expected: failures — current dashboard.html has none of this.

- [ ] **Step 3: Replace `templates/dashboard.html`**

Run: `cp /tmp/dashboard_revamp/templates/dashboard.html /Users/mingyuanw/Desktop/Project/Keydion/templates/dashboard.html`

The bundle file is ~10 KB and uses only `url_for` endpoints that already exist in this codebase (verified: `dashboard`, `upload`, `my_submissions`, `review_list`, `manage`, `paper_manage`, `news_publish`, `news_manage`, `admin_users`, `admin_guides_manage`, `change_password`, `logout`).

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py::DashboardShellTemplateContractTest -v`
Expected: 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add templates/dashboard.html tests/test_dashboard_revamp_contract.py
git commit -m "feat(dashboard): replace dashboard shell with sidebar-driven layout"
```

---

### Task 2.4: Rewrite the `dashboard()` route with stats

**Files:**
- Modify: `app.py:1055-1059`
- Test: `tests/test_dashboard_revamp_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_dashboard_revamp_contract.py`:

```python
class DashboardRouteContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _dashboard_source(self):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == "dashboard":
                return ast.get_source_segment(self.app_source, node)
        self.fail("dashboard route not found")

    def test_dashboard_branches_on_partial_request(self):
        src = self._dashboard_source()
        self.assertIn("is_partial_request()", src)
        self.assertIn("_dashboard/overview.html", src)

    def test_dashboard_computes_role_gated_stats(self):
        src = self._dashboard_source()
        # Role-2+ stats.
        self.assertIn("pending_reviews", src)
        self.assertIn("published_news", src)
        self.assertIn("pending_news", src)
        # Role-3+ stat.
        self.assertIn("papers_in_library", src)

    def test_dashboard_passes_stats_to_templates(self):
        src = self._dashboard_source()
        self.assertIn("dashboard_stats=", src)

    def test_dashboard_does_not_compute_stats_for_role_1(self):
        src = self._dashboard_source()
        # Stats should be gated so role 1 gets an empty dict.
        self.assertIn("role >= 2", src)
        self.assertIn("role >= 3", src)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py::DashboardRouteContractTest -v`
Expected: failures.

- [ ] **Step 3: Rewrite the `dashboard()` route**

Edit `app.py:1055-1059`. Replace:

```python
    def dashboard():
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        return render_template("dashboard.html", user=user)
```

with:

```python
    def dashboard():
        user = require_login()
        if not user:
            return redirect(url_for("login"))

        try:
            role = int(user.get("role", "1"))
        except (TypeError, ValueError):
            role = 1

        stats: Dict[str, object] = {}

        if role >= 2:
            with db_session() as db:
                pending_subs = db.query(SubmissionModel).filter_by(status="pending").all()
                stats["pending_reviews"] = len(pending_subs)
                # Oldest submission delta — submitted_at is a Unicode ISO string.
                oldest_days = None
                for sub in pending_subs:
                    ts = (sub.submitted_at or "").strip()
                    if not ts:
                        continue
                    try:
                        dt = datetime.fromisoformat(ts)
                    except ValueError:
                        continue
                    days = (datetime.utcnow() - dt).days
                    if oldest_days is None or days > oldest_days:
                        oldest_days = days
                if oldest_days is not None and oldest_days > 0:
                    stats["pending_oldest_label"] = _("oldest %(n)d days ago") % {"n": oldest_days}

                stats["published_news"] = db.query(NewsArticleModel).filter_by(status="published").count()
                stats["pending_news"] = db.query(NewsArticleModel).filter_by(status="pending").count()

        if role >= 3:
            with db_session() as db:
                stats["papers_in_library"] = db.query(PaperMetadataModel).count()
                # "+N this month" delta via string-prefix comparison on the YYYY-MM portion.
                current_prefix = datetime.utcnow().strftime("%Y-%m")
                new_this_month = (
                    db.query(PaperMetadataModel)
                    .filter(PaperMetadataModel.published_at.like(f"{current_prefix}%"))
                    .count()
                )
                if new_this_month:
                    stats["papers_delta_label"] = _("+%(n)d this month") % {"n": new_this_month}

        if is_partial_request():
            return render_template(
                "_dashboard/overview.html",
                user=user,
                dashboard_stats=stats,
            )

        return render_template(
            "dashboard.html",
            user=user,
            dashboard_stats=stats,
        )
```

If `Dict` is not yet imported at the top of `app.py`, the existing code uses `Dict[str, str]` elsewhere (see line 3234) so the import already exists — no change needed. If `db_session`, `SubmissionModel`, `NewsArticleModel`, `PaperMetadataModel`, and `datetime` aren't accessible from this scope, they already are: the existing `app.py` uses them in nearby routes.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py::DashboardRouteContractTest -v`
Expected: 4 tests pass.

- [ ] **Step 5: Run full suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add app.py tests/test_dashboard_revamp_contract.py
git commit -m "feat(dashboard): compute role-gated stats and serve overview partial"
```

---

### Task 2.5: End-to-end manual verification of the dashboard

- [ ] **Step 1: Start the dev server**

Run: `./start_local.sh`

- [ ] **Step 2: Verify admin (role 3) view**

Visit `/dashboard` as an admin:
- Sidebar shows: Overview, Workspace (Upload), Review (Review submissions), Collection (Manage papers, Categories & journals), News (Write an article, Manage news), Admin (Manage users, Manage guides), Account (Change password, Sign out).
- Stats row shows four tiles: Papers in library, Pending reviews, Published news, Pending news.
- Quick actions show 5 cards including admin/news ones.

- [ ] **Step 3: Verify partial navigation**

Click each sidebar item in turn. For each one:
- The main panel content swaps without a full page reload.
- The URL bar updates to the destination (e.g. `/upload`, `/news/manage`).
- The clicked sidebar item gets the `is-active` highlight.
- The global navbar and footer remain unchanged (proves the partial swap is working).

- [ ] **Step 4: Verify Overview round-trip**

Click "Overview" in the sidebar. The main panel returns to the welcome/stats/quick-actions view.

- [ ] **Step 5: Verify back/forward**

Click an item, then press the browser's Back button. The previous panel restores. Press Forward — the panel restores again.

- [ ] **Step 6: Verify sidebar state cycle**

Click the toggle in the sidebar head three times — sidebar should go full → icons → hidden → full. Reload the page; the state persists (via `localStorage['keydion.sidebar']`).

- [ ] **Step 7: Verify hard-refresh deep-link**

Hard-refresh `/upload`. The page renders standalone with the normal Keydion navbar and footer (proves `partial` is only `True` when the header is set on fetch).

- [ ] **Step 8: Verify in-panel form submission**

From the dashboard, click "Change password". Submit the form (with wrong current password to keep it safe). The form posts, the flash message appears IN the panel, and the layout stays in the dashboard shell.

- [ ] **Step 9: Verify reader (role 1) view**

Sign in as a reader (use `python tools/manage_passwords.py set --username readeruser --password test --role 1` if needed):
- Sidebar shows only: Overview, Workspace (Upload + My submissions), Account (Change password + Sign out).
- Stats row is **not shown** at all.
- Quick actions has exactly **two cards**: Upload research + Change password.
- Quick actions visually sits higher (no stats row above it).

- [ ] **Step 10: Stop the dev server**

Ctrl-C.

---

## Phase 3 — Cleanup and i18n

### Task 3.1: Remove duplicated Upload link from base.html top nav

**Files:**
- Modify: `templates/base.html:60-62`
- Test: `tests/test_dashboard_revamp_contract.py` (extend)

- [ ] **Step 1: Add failing test**

Append to `tests/test_dashboard_revamp_contract.py`:

```python
class BaseNavCleanupContractTest(unittest.TestCase):
    def test_base_html_no_longer_has_top_nav_upload_link(self):
        src = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
        # The Upload link inside the nav block is removed; the only remaining
        # references to 'upload' should be in user-menu / dashboard contexts, not nav-link.
        # Stronger check: no <a class="nav-link" ...url_for('upload')...> in the file.
        self.assertNotRegex(
            src,
            r"<a class=\"nav-link\"[^>]+url_for\('upload'\)",
            "base.html top-nav Upload link must be removed",
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py::BaseNavCleanupContractTest -v`
Expected: FAIL.

- [ ] **Step 3: Remove the Upload nav-link**

Edit `templates/base.html:60-62`. Remove these three lines:

```jinja
{% if session.get('user') %}
<a class="nav-link" href="{{ url_for('upload') }}">{{ _('Upload') }}</a>
{% endif %}
```

(The Upload action remains accessible via the dashboard sidebar.)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_dashboard_revamp_contract.py::BaseNavCleanupContractTest -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add templates/base.html tests/test_dashboard_revamp_contract.py
git commit -m "refactor(nav): remove top-nav Upload link now served by dashboard sidebar"
```

---

### Task 3.2: Add new translation strings

**Files:**
- Modify: `translations/en/LC_MESSAGES/messages.po`
- Modify: `translations/zh/LC_MESSAGES/messages.po`
- Compile: `python tools/compile_translations.py`

- [ ] **Step 1: List the new English strings**

The following strings are introduced by this plan and need entries in both `.po` catalogs:

```
"User Hub · Keydion"
"User hub navigation"
"Functions"
"Account"
"Cycle sidebar"
"Show sidebar"
"Overview"
"Workspace"
"Review"
"Collection"
"Admin"
"At a glance"
"Quick actions"
"Hello, %(name)s."
"Manage your account access, uploads, and the Keydion library."
"Papers in library"
"Pending reviews"
"Published news"
"Pending news"
"oldest %(n)d days ago"
"+%(n)d this month"
"Upload research"
"My submissions"
"Review submissions"
"Manage papers"
"Categories & journals"
"Write an article"
"Manage news"
"Manage users"
"Manage guides"
"Change password"
"Sign out"
"Review the queue"
"%(n)d submissions are waiting for editorial decision."
"Accept or reject papers submitted by readers."
"Share a new paper for peers to explore. Supports PDF with metadata extraction."
"Publish updates, announcements and stories for the Keydion community."
"Review local and Microsoft-linked accounts, adjust roles, set passwords."
"Set or change your account password."
"Track the review status of your submitted papers."
"Open uploader"
"Open queue"
"Open editor"
"Open admin"
"View submissions"
"Update security"
"Could not load this section."
"Save as Draft"
"Save Draft"
"Publish Changes"
"Draft saved."
"Draft"
"Status"
"Published"
```

- [ ] **Step 2: Append to `translations/en/LC_MESSAGES/messages.po`**

For each new string, append a `msgid` / `msgstr` pair where `msgstr` is the same English string. Example for the first few:

```
msgid "User Hub · Keydion"
msgstr "User Hub · Keydion"

msgid "Overview"
msgstr "Overview"

msgid "Workspace"
msgstr "Workspace"

# ... repeat for every string in the list above
```

Skip any string that already exists in the catalog (e.g. "Sign out" may already be there from the existing dashboard; grep first: `grep '^msgid "Sign out"' translations/en/LC_MESSAGES/messages.po`).

- [ ] **Step 3: Append to `translations/zh/LC_MESSAGES/messages.po`**

For each new string, append a `msgid` / `msgstr` pair with the Chinese translation. Suggested translations:

```
msgid "User Hub · Keydion"
msgstr "用户中心 · Keydion"

msgid "Overview"
msgstr "概览"

msgid "Workspace"
msgstr "工作区"

msgid "Review"
msgstr "审核"

msgid "Collection"
msgstr "收藏"

msgid "Admin"
msgstr "管理"

msgid "Account"
msgstr "账户"

msgid "Functions"
msgstr "功能"

msgid "User hub navigation"
msgstr "用户中心导航"

msgid "Cycle sidebar"
msgstr "切换侧边栏"

msgid "Show sidebar"
msgstr "显示侧边栏"

msgid "At a glance"
msgstr "概况"

msgid "Quick actions"
msgstr "快速操作"

msgid "Hello, %(name)s."
msgstr "你好,%(name)s。"

msgid "Manage your account access, uploads, and the Keydion library."
msgstr "管理你的账户、上传以及 Keydion 文献库。"

msgid "Papers in library"
msgstr "文献库总数"

msgid "Pending reviews"
msgstr "待审稿件"

msgid "Published news"
msgstr "已发布新闻"

msgid "Pending news"
msgstr "新闻草稿"

msgid "oldest %(n)d days ago"
msgstr "最早 %(n)d 天前"

msgid "+%(n)d this month"
msgstr "本月新增 %(n)d 篇"

msgid "Upload research"
msgstr "上传研究"

msgid "My submissions"
msgstr "我的投稿"

msgid "Review submissions"
msgstr "审核投稿"

msgid "Manage papers"
msgstr "管理文献"

msgid "Categories & journals"
msgstr "分类与期刊"

msgid "Write an article"
msgstr "撰写文章"

msgid "Manage news"
msgstr "管理新闻"

msgid "Manage users"
msgstr "管理用户"

msgid "Manage guides"
msgstr "管理指南"

msgid "Change password"
msgstr "修改密码"

msgid "Sign out"
msgstr "退出登录"

msgid "Review the queue"
msgstr "审核队列"

msgid "%(n)d submissions are waiting for editorial decision."
msgstr "有 %(n)d 篇投稿等待编辑处理。"

msgid "Accept or reject papers submitted by readers."
msgstr "接受或拒绝读者提交的文献。"

msgid "Share a new paper for peers to explore. Supports PDF with metadata extraction."
msgstr "分享新的研究文献,支持 PDF 元数据提取。"

msgid "Publish updates, announcements and stories for the Keydion community."
msgstr "为 Keydion 社区发布更新、公告和文章。"

msgid "Review local and Microsoft-linked accounts, adjust roles, set passwords."
msgstr "管理本地账户和 Microsoft 关联账户,调整角色与密码。"

msgid "Set or change your account password."
msgstr "设置或修改你的账户密码。"

msgid "Track the review status of your submitted papers."
msgstr "查看你提交文献的审核状态。"

msgid "Open uploader"
msgstr "打开上传"

msgid "Open queue"
msgstr "打开队列"

msgid "Open editor"
msgstr "打开编辑器"

msgid "Open admin"
msgstr "打开管理"

msgid "View submissions"
msgstr "查看投稿"

msgid "Update security"
msgstr "更新密码"

msgid "Could not load this section."
msgstr "无法加载该部分。"

msgid "Save as Draft"
msgstr "保存为草稿"

msgid "Save Draft"
msgstr "保存草稿"

msgid "Publish Changes"
msgstr "发布修改"

msgid "Draft saved."
msgstr "草稿已保存。"

msgid "Draft"
msgstr "草稿"

msgid "Status"
msgstr "状态"

msgid "Published"
msgstr "已发布"
```

Skip any string that already exists; grep first.

- [ ] **Step 4: Compile the catalogs**

Run: `python tools/compile_translations.py`
Expected: writes `messages.mo` for both locales without errors.

- [ ] **Step 5: Run full suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add translations/en/LC_MESSAGES/messages.po translations/en/LC_MESSAGES/messages.mo \
        translations/zh/LC_MESSAGES/messages.po translations/zh/LC_MESSAGES/messages.mo
git commit -m "i18n(dashboard): add EN and ZH translations for revamped dashboard"
```

---

### Task 3.3: Final cross-role end-to-end verification

- [ ] **Step 1: Start the dev server**

Run: `./start_local.sh`

- [ ] **Step 2: Walk the full INTEGRATION.md test checklist**

For each role (admin / curator / reader), confirm:
- Sidebar shows the correct groups (admin: all; curator: Workspace, Review, News, Account; reader: Workspace + Account only).
- Reader sees no stats row and only 2 quick-action cards.
- Curator sees 3 stat tiles (Pending reviews, Published news, Pending news) and curator-level quick actions.
- Admin sees all 4 stat tiles.
- Each sidebar item swaps the main panel without full reload.
- Browser back/forward restores panels.
- Sidebar state (full/icons/hidden) persists across reloads.
- Hard-refreshing a sidebar destination URL renders the full layout.
- Form submission inside the panel stays in the panel.
- A news article saved as draft does not appear on `/news` for any role; pending pill shows in `/news/manage`.
- Switching the locale to Chinese (via the existing language switcher) renders all new strings in Chinese.

- [ ] **Step 3: Switch to Chinese locale and recheck**

In the language switcher, select 中文. Repeat the admin walkthrough — all new dashboard strings should render in Chinese.

- [ ] **Step 4: Stop the dev server**

Ctrl-C.

- [ ] **Step 5: Final test sweep**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass.

---

## Self-Review Notes (author)

- **Spec coverage:** Every spec item maps to a task — news draft column → Task 0.1; status filter → 0.2; public hiding → 0.3; route branching → 0.4; UI dual button → 0.5; manage table status → 0.6; manual draft verification → 0.7; partial helper → 1.1; bare template → 1.2; partial-aware templates → 1.3; assets → 2.1; overview partial with role-1 modifications → 2.2; shell → 2.3; route stats → 2.4; manual dashboard verification → 2.5; nav cleanup → 3.1; i18n → 3.2; final verification → 3.3.
- **Placeholder scan:** All TDD steps include the actual test and implementation code. The Chinese translations are concrete sentences, not "TBD." The two ALTER patterns reference the existing codebase's idiom in `init_db()` rather than fabricating a new one — the engineer must read 2-3 lines of context to match style, which is intentional rather than a placeholder.
- **Type/name consistency:** stat keys `papers_in_library`, `pending_reviews`, `published_news`, `pending_news`, `pending_oldest_label`, `papers_delta_label` match between the route (Task 2.4), the overview partial (Task 2.2), and the test assertions. `status` values are exclusively `"pending"` / `"published"` everywhere. Submit-button `action` values are exclusively `"draft"` / `"publish"`.
