# User Guides Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a public, categorized, bilingual "Guides" subsystem to the Keydion Flask app, authored by role-3 admins through a Quill WYSIWYG editor, and wire the landing-page footer's "Submission Guide" link to its index.

**Architecture:** Single-table `GuideModel` in `app.py`, mirroring the existing news subsystem's routing/template patterns but using a Quill HTML body field (per-language EN/ZH) sanitized with `bleach`. Categories are stored as a JSON file under `data/`, identical to `news_categories.json`. Quill is vendored as static JS/CSS — no build step.

**Tech Stack:** Flask 3, SQLAlchemy 2 ORM, Flask-Babel, Jinja2, `bleach` (new pip dep), Quill 1.3.7 (vendored JS/CSS), Bootstrap 5 (already vendored), Python `unittest` + AST contract tests.

---

## Spec reference

Full design in `docs/superpowers/specs/2026-05-22-user-guides-design.md`. All decisions there are authoritative; this plan implements them with no scope expansion.

## File map (created or modified)

**Created:**
- `static/vendor/quill/quill.min.js`
- `static/vendor/quill/quill.snow.css`
- `data/guide_categories.json`
- `templates/guides.html`
- `templates/guide_article.html`
- `templates/guide_manage.html`
- `templates/guide_publish.html`
- `tests/test_guide_routes_contract.py`
- `tests/test_guide_template_contract.py`
- `tests/test_guide_sanitization_contract.py`

**Modified:**
- `requirements.txt` — add `bleach>=6.0`
- `.gitignore` — ignore `static/uploads/guides/`
- `app.py` — add model, helpers, 6 routes, register sanitizer
- `templates/landing.html:1006` — point footer link to `url_for('guides')`
- `translations/en/LC_MESSAGES/messages.po` and `translations/zh/LC_MESSAGES/messages.po` — new strings

`static/uploads/guides/` is created at runtime by the upload handler; not checked in.

---

## Task 0: Setup

**Files:**
- Create: `static/vendor/quill/quill.min.js`, `static/vendor/quill/quill.snow.css`
- Modify: `requirements.txt`, `.gitignore`

- [ ] **Step 1: Vendor Quill 1.3.7**

Run these from the repo root:

```bash
mkdir -p static/vendor/quill
curl -L -o static/vendor/quill/quill.min.js https://cdn.jsdelivr.net/npm/quill@1.3.7/dist/quill.min.js
curl -L -o static/vendor/quill/quill.snow.css https://cdn.jsdelivr.net/npm/quill@1.3.7/dist/quill.snow.css
```

Verify both files exist and are non-empty:

```bash
ls -l static/vendor/quill/quill.min.js static/vendor/quill/quill.snow.css
```

Expected: `quill.min.js` is ~200 KB and `quill.snow.css` is ~40 KB.

- [ ] **Step 2: Add `bleach` to requirements**

Open `requirements.txt` and append a new line at the end:

```
bleach>=6.0
```

Install it locally:

```bash
pip install -r requirements.txt
```

Verify:

```bash
python -c "import bleach; print(bleach.__version__)"
```

Expected: a version string like `6.x.x`.

- [ ] **Step 3: Ignore runtime upload directory**

Open `.gitignore` and add a new line at the end:

```
static/uploads/guides/
```

- [ ] **Step 4: Commit**

```bash
git add static/vendor/quill/ requirements.txt .gitignore
git commit -m "chore: vendor Quill 1.3.7 and add bleach for guides"
```

---

## Task 1: HTML sanitizer (TDD)

The sanitizer is a pure function — perfect for TDD. We build it first so all later routes can rely on it.

**Files:**
- Test: `tests/test_guide_sanitization_contract.py`
- Modify: `app.py` (add `_sanitize_guide_html` near the news helpers, around line 3406)

- [ ] **Step 1: Write the failing test**

Create `tests/test_guide_sanitization_contract.py`:

```python
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import _sanitize_guide_html  # noqa: E402


class GuideSanitizationContractTest(unittest.TestCase):
    def test_strips_script_tags(self):
        result = _sanitize_guide_html("<p>Hi</p><script>alert(1)</script>")
        self.assertIn("<p>Hi</p>", result)
        self.assertNotIn("<script", result)
        self.assertNotIn("alert(1)", result)

    def test_strips_javascript_href(self):
        result = _sanitize_guide_html('<a href="javascript:alert(1)">x</a>')
        self.assertNotIn("javascript:", result)

    def test_allows_safe_formatting(self):
        html = (
            "<h2>Login</h2><p><strong>Click</strong> the "
            '<a href="/login">login</a> button.</p>'
            '<ul><li>Step 1</li></ul>'
            '<img src="/static/uploads/guides/x.png" alt="screenshot">'
        )
        result = _sanitize_guide_html(html)
        self.assertIn("<h2>Login</h2>", result)
        self.assertIn("<strong>Click</strong>", result)
        self.assertIn('href="/login"', result)
        self.assertIn("<li>Step 1</li>", result)
        self.assertIn('src="/static/uploads/guides/x.png"', result)

    def test_strips_event_handlers(self):
        result = _sanitize_guide_html('<p onclick="evil()">hi</p>')
        self.assertNotIn("onclick", result)
        self.assertIn("hi", result)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test, verify it fails**

```bash
python -m unittest tests/test_guide_sanitization_contract.py -v
```

Expected: ImportError — `cannot import name '_sanitize_guide_html' from 'app'`.

- [ ] **Step 3: Implement the sanitizer**

Open `app.py`. Find the line `# ==================== NEWS HELPERS ====================` (around line 3406). Immediately **before** that line, insert:

```python
# ==================== GUIDE HELPERS ====================

import bleach

GUIDE_ALLOWED_TAGS = [
    "h1", "h2", "h3", "h4", "p", "strong", "em", "u", "s",
    "ul", "ol", "li", "a", "img", "blockquote", "code", "pre",
    "br", "hr", "span", "div",
]
GUIDE_ALLOWED_ATTRS = {
    "a": ["href", "title", "target", "rel"],
    "img": ["src", "alt", "width", "height"],
    "span": ["class"],
    "div": ["class"],
}
GUIDE_ALLOWED_PROTOCOLS = ["http", "https"]


def _sanitize_guide_html(html: str) -> str:
    """Strip dangerous HTML from a Quill body before storing it."""
    if not html:
        return ""
    return bleach.clean(
        html,
        tags=GUIDE_ALLOWED_TAGS,
        attributes=GUIDE_ALLOWED_ATTRS,
        protocols=GUIDE_ALLOWED_PROTOCOLS,
        strip=True,
    )
```

`bleach.clean` strips `javascript:` hrefs automatically because that protocol is not in `GUIDE_ALLOWED_PROTOCOLS`, and `strip=True` removes disallowed tags entirely (rather than escaping them).

- [ ] **Step 4: Run the test, verify it passes**

```bash
python -m unittest tests/test_guide_sanitization_contract.py -v
```

Expected: 4 tests, all pass.

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_guide_sanitization_contract.py
git commit -m "feat(guides): add bleach-based HTML sanitizer for guide bodies"
```

---

## Task 2: Data model + low-level CRUD helpers

**Files:**
- Modify: `app.py` (add `GuideModel`, `GUIDE_FIELDS`, CRUD helpers, init_db migration)

- [ ] **Step 1: Add `GUIDE_FIELDS` constant**

Open `app.py`. Find `NEWS_FIELDS = [...]` (line 49). Immediately after that line, add:

```python
GUIDE_FIELDS = [
    "id", "slug", "category", "sort_order", "published",
    "title_en", "title_zh", "summary_en", "summary_zh",
    "body_en", "body_zh", "created_at", "updated_at",
]
GUIDE_CATEGORIES_JSON = DATA_DIR / "guide_categories.json"
_DEFAULT_GUIDE_CATEGORIES = [
    "Getting Started", "Account", "Submissions", "News", "Other",
]
```

- [ ] **Step 2: Add the SQLAlchemy model**

Find the `class NewsArticleModel(BASE):` block (line 468). Immediately **after** the closing of that class (line 477, the `published_at` line), and **before** the blank line that precedes `class SubmissionModel`, insert:

```python

class GuideModel(BASE):
    __tablename__ = "guides"
    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(Unicode(120), unique=True, index=True, nullable=False)
    category = Column(Unicode(80), default="")
    sort_order = Column(Integer, default=100)
    published = Column(Boolean, default=False)
    title_en = Column(Unicode(200), default="")
    title_zh = Column(Unicode(200), default="")
    summary_en = Column(Unicode(300), default="")
    summary_zh = Column(Unicode(300), default="")
    body_en = Column(UnicodeText, default="")
    body_zh = Column(UnicodeText, default="")
    created_at = Column(Unicode(40), default="")
    updated_at = Column(Unicode(40), default="")
```

You also need `Integer` and `Boolean` in the imports. Check the existing `from sqlalchemy ...` import (around line 30) — if either is missing, add it. To check:

```bash
grep -n "^from sqlalchemy " app.py
```

If the import line is e.g. `from sqlalchemy import Column, Unicode, UnicodeText, create_engine`, change it to:

```python
from sqlalchemy import Boolean, Column, Integer, Unicode, UnicodeText, create_engine
```

(Keep any other names that were already imported; add `Boolean` and `Integer` alphabetically if not already present.)

- [ ] **Step 3: Add CRUD helpers**

Find the line you added in Task 1 step 3: `# ==================== GUIDE HELPERS ====================`. Below the `_sanitize_guide_html` function you added there, append the following helpers (still under the GUIDE HELPERS section):

```python
def _load_guide_categories() -> list:
    """Load guide categories from JSON file, seeding from defaults if needed."""
    if GUIDE_CATEGORIES_JSON.exists():
        try:
            return json.loads(GUIDE_CATEGORIES_JSON.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    _save_guide_categories(_DEFAULT_GUIDE_CATEGORIES)
    return list(_DEFAULT_GUIDE_CATEGORIES)


def _save_guide_categories(cats: list) -> None:
    GUIDE_CATEGORIES_JSON.parent.mkdir(parents=True, exist_ok=True)
    GUIDE_CATEGORIES_JSON.write_text(
        json.dumps(cats, ensure_ascii=False, indent=2), encoding="utf-8"
    )


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(text: str) -> str:
    """Lowercase ASCII slug, dash-separated, max 120 chars."""
    if not text:
        return ""
    slug = _SLUG_RE.sub("-", text.lower()).strip("-")
    return slug[:120] or "guide"


def _guide_to_dict(g: "GuideModel") -> dict:
    return {field: getattr(g, field) for field in GUIDE_FIELDS}


def load_guides(published_only: bool = True) -> list:
    """Return all guides, ordered by category then sort_order ascending."""
    with db_session() as db:
        q = db.query(GuideModel)
        if published_only:
            q = q.filter(GuideModel.published == True)  # noqa: E712
        guides = q.all()
        rows = [_guide_to_dict(g) for g in guides]
        rows.sort(key=lambda r: (r.get("category") or "", r.get("sort_order") or 0))
        return rows


def get_guide_by_slug(slug: str):
    with db_session() as db:
        g = db.query(GuideModel).filter_by(slug=slug).first()
        return _guide_to_dict(g) if g else None


def get_guide(guide_id: int):
    with db_session() as db:
        g = db.query(GuideModel).filter_by(id=guide_id).first()
        return _guide_to_dict(g) if g else None


def save_guide(data: dict) -> int:
    """Insert a new guide and return its id."""
    now = datetime.utcnow().isoformat()
    payload = {f: data.get(f, "") for f in GUIDE_FIELDS if f != "id"}
    payload["created_at"] = now
    payload["updated_at"] = now
    payload["body_en"] = _sanitize_guide_html(payload.get("body_en", ""))
    payload["body_zh"] = _sanitize_guide_html(payload.get("body_zh", ""))
    payload["published"] = bool(payload.get("published"))
    payload["sort_order"] = int(payload.get("sort_order") or 100)
    with db_session() as db:
        g = GuideModel(**payload)
        db.add(g)
        db.commit()
        return g.id


def update_guide(guide_id: int, data: dict) -> bool:
    with db_session() as db:
        g = db.query(GuideModel).filter_by(id=guide_id).first()
        if not g:
            return False
        for field in GUIDE_FIELDS:
            if field in ("id", "created_at"):
                continue
            if field not in data:
                continue
            value = data[field]
            if field in ("body_en", "body_zh"):
                value = _sanitize_guide_html(value or "")
            elif field == "published":
                value = bool(value)
            elif field == "sort_order":
                value = int(value or 100)
            setattr(g, field, value)
        g.updated_at = datetime.utcnow().isoformat()
        db.commit()
        return True


def delete_guide(guide_id: int) -> bool:
    with db_session() as db:
        g = db.query(GuideModel).filter_by(id=guide_id).first()
        if not g:
            return False
        db.delete(g)
        db.commit()
        return True


def slug_exists(slug: str, exclude_id: int = 0) -> bool:
    with db_session() as db:
        q = db.query(GuideModel).filter_by(slug=slug)
        if exclude_id:
            q = q.filter(GuideModel.id != exclude_id)
        return db.query(q.exists()).scalar()
```

Confirm `re` is already imported at the top of `app.py`:

```bash
grep -n "^import re" app.py
```

If not present, add `import re` to the imports block.

Also confirm `datetime` is imported (it is — used throughout). If you get a NameError later, add `from datetime import datetime` to the imports block.

- [ ] **Step 4: Confirm `init_db()` auto-creates the new table**

Look at `init_db()` around line 511 — it calls `BASE.metadata.create_all(_ENGINE)`, which picks up any new `BASE`-derived model automatically. No additional migration code is needed because this is a brand-new table.

To verify the table is created, start a Python REPL with the env vars set as in CLAUDE.md and run:

```bash
python -c "from app import init_db, db_session, GuideModel; init_db(); from sqlalchemy import inspect; from app import _ENGINE; print('guides' in inspect(_ENGINE).get_table_names())"
```

Expected: `True`.

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "feat(guides): add GuideModel, CRUD helpers, and slug/category utilities"
```

---

## Task 3: Categories JSON file

**Files:**
- Create: `data/guide_categories.json`

- [ ] **Step 1: Create the seed categories file**

Create `data/guide_categories.json` with the same shape as `data/news_categories.json`:

```json
[
  "Getting Started",
  "Account",
  "Submissions",
  "News",
  "Other"
]
```

(If the file already exists from a previous `_load_guide_categories()` invocation, leave it.)

- [ ] **Step 2: Commit**

```bash
git add data/guide_categories.json
git commit -m "feat(guides): seed default guide categories"
```

---

## Task 4: Public index route + template

**Files:**
- Modify: `app.py` (add `guides()` route inside `create_app()`)
- Create: `templates/guides.html`

- [ ] **Step 1: Add the route**

Open `app.py`. Find the `# ==================== NEWS ROUTES ====================` block (around line 1833). **After** the last news route (`news_category_delete`, around line 2090) but **before** the next major section, add a new section. To find a clean insertion point, search for the next `# ====` separator after the news routes:

```bash
grep -n "# ====" app.py | head -40
```

Insert this new block before the next major section divider that follows the news routes:

```python
    # ==================== GUIDE ROUTES ====================

    @app.route("/guides")
    def guides():
        all_guides = load_guides(published_only=True)
        # Group by category, preserving the order from guide_categories.json,
        # then any unknown categories at the end.
        categories_in_order = _load_guide_categories()
        seen = set()
        grouped = []
        for cat in categories_in_order:
            items = [g for g in all_guides if g.get("category") == cat]
            if items:
                grouped.append((cat, items))
                seen.add(cat)
        # Any leftover categories not in the JSON list
        extras = {}
        for g in all_guides:
            cat = g.get("category") or ""
            if cat and cat not in seen:
                extras.setdefault(cat, []).append(g)
        for cat in sorted(extras):
            grouped.append((cat, extras[cat]))
        return render_template("guides.html", grouped=grouped)
```

- [ ] **Step 2: Create the index template**

Create `templates/guides.html`:

```html
{% extends "base.html" %}
{% block title %}{{ _('Guides · Keydion') }}{% endblock %}
{% block content %}
<div class="container py-4" style="max-width: 820px;">
  <div class="mb-4">
    <h1 class="display-6 fw-semibold mb-2">{{ _('Guides') }}</h1>
    <p class="text-muted mb-0">{{ _('Step-by-step help for using Keydion.') }}</p>
  </div>

  {% if not grouped %}
    <div class="text-center py-5 text-muted">
      <p>{{ _('No guides published yet. Check back soon.') }}</p>
    </div>
  {% else %}
    {% for category, items in grouped %}
      <section class="mb-4">
        <h2 class="h5 fw-semibold text-secondary text-uppercase mb-3" style="letter-spacing:.05em;">
          {{ category }}
        </h2>
        <ul class="list-group list-group-flush border-top border-bottom">
          {% for g in items %}
            {% set title = g.title_zh if get_locale()|string == 'zh' and g.title_zh else g.title_en or g.title_zh %}
            {% set summary = g.summary_zh if get_locale()|string == 'zh' and g.summary_zh else g.summary_en or g.summary_zh %}
            <li class="list-group-item px-0 py-3">
              <a class="text-decoration-none d-block" href="{{ url_for('guide_article', slug=g.slug) }}">
                <div class="fw-semibold text-dark">{{ title }}</div>
                {% if summary %}
                  <div class="text-muted small mt-1">{{ summary }}</div>
                {% endif %}
              </a>
            </li>
          {% endfor %}
        </ul>
      </section>
    {% endfor %}
  {% endif %}
</div>
{% endblock %}
```

`get_locale` is the Flask-Babel function — confirm it's available in templates. Inspect existing templates with:

```bash
grep -rn "get_locale" templates/ | head -5
```

If `get_locale` is **not** exposed in templates, add this near the top of `create_app()` in `app.py`, just after `app.config.update(...)` and before any route definitions:

```bash
grep -n "context_processor\|jinja_env.globals" app.py | head
```

If you need to add it, find a spot just after `app = Flask(__name__)` block setup and add:

```python
    @app.context_processor
    def _inject_locale():
        return {"get_locale": get_locale}
```

- [ ] **Step 3: Smoke test in browser**

Start the dev server:

```bash
./start_local.sh
```

In a browser visit `http://localhost:5000/guides`. Expected: empty-state message "No guides published yet." renders without error.

- [ ] **Step 4: Commit**

```bash
git add app.py templates/guides.html
git commit -m "feat(guides): add public guide index page grouped by category"
```

---

## Task 5: Public article route + template

**Files:**
- Modify: `app.py` (add `guide_article(slug)` route)
- Create: `templates/guide_article.html`

- [ ] **Step 1: Add the route**

In `app.py`, immediately after the `guides()` function you just added, append inside the same `# ==================== GUIDE ROUTES ====================` block:

```python
    @app.route("/guides/<slug>")
    def guide_article(slug):
        guide = get_guide_by_slug(slug)
        if not guide or not guide.get("published"):
            abort(404)
        return render_template("guide_article.html", guide=guide)
```

- [ ] **Step 2: Create the article template**

Create `templates/guide_article.html`:

```html
{% extends "base.html" %}
{% set locale_str = get_locale()|string %}
{% set title = guide.title_zh if locale_str == 'zh' and guide.title_zh else (guide.title_en or guide.title_zh) %}
{% set body = guide.body_zh if locale_str == 'zh' and guide.body_zh else (guide.body_en or guide.body_zh) %}
{% block title %}{{ title }} · Keydion{% endblock %}
{% block content %}
<div class="container py-4" style="max-width: 820px;">
  <a class="text-decoration-none small text-muted" href="{{ url_for('guides') }}">
    &larr; {{ _('Back to guides') }}
  </a>
  <h1 class="display-6 fw-semibold mt-2 mb-2">{{ title }}</h1>
  <div class="mb-4 d-flex gap-2 align-items-center">
    {% if guide.category %}
      <span class="badge bg-secondary">{{ guide.category }}</span>
    {% endif %}
    {% if guide.updated_at %}
      <span class="text-muted small">{{ _('Updated') }} {{ guide.updated_at[:10] }}</span>
    {% endif %}
  </div>
  <article class="guide-body">
    {{ body | safe }}
  </article>
</div>

<style>
  .guide-body h1, .guide-body h2, .guide-body h3, .guide-body h4 { margin-top: 1.4em; margin-bottom: .5em; font-weight: 600; }
  .guide-body p { line-height: 1.7; margin-bottom: 1em; }
  .guide-body img { max-width: 100%; height: auto; border-radius: 6px; margin: 1em 0; }
  .guide-body ul, .guide-body ol { margin-bottom: 1em; padding-left: 1.5em; }
  .guide-body li { margin-bottom: .3em; }
  .guide-body blockquote { border-left: 3px solid #d1d5db; padding-left: 1em; color: #6b7280; margin: 1em 0; }
  .guide-body code { background: #f3f4f6; padding: 1px 5px; border-radius: 3px; font-size: .9em; }
  .guide-body pre { background: #f3f4f6; padding: 12px 14px; border-radius: 6px; overflow-x: auto; }
  .guide-body a { color: #2563eb; }
</style>
{% endblock %}
```

The `body | safe` is acceptable here because `_sanitize_guide_html` has already cleaned the stored HTML on save.

- [ ] **Step 3: Smoke test**

With the dev server running, visit `http://localhost:5000/guides/nonexistent`. Expected: 404 page. (We'll test the happy path after the publish form exists.)

- [ ] **Step 4: Commit**

```bash
git add app.py templates/guide_article.html
git commit -m "feat(guides): add public guide article page with locale-aware body"
```

---

## Task 6: Image upload route

The upload endpoint is needed for Quill, so we add it before the publish form.

**Files:**
- Modify: `app.py` (add `admin_guide_upload_image()` route)
- Modify: `app.py` (add `GUIDE_IMAGES_DIR` constant near the existing `NEWS_IMAGES_DIR`)

- [ ] **Step 1: Add the constant**

Find the line:

```python
NEWS_IMAGES_DIR = BASE_DIR / "static" / "uploads" / "news"
```

(Around line 239.) Immediately **after** it, add:

```python
GUIDE_IMAGES_DIR = BASE_DIR / "static" / "uploads" / "guides"
GUIDE_IMAGE_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
```

- [ ] **Step 2: Add the route**

In the `# ==================== GUIDE ROUTES ====================` block in `create_app()`, after `guide_article`, append:

```python
    @app.route("/admin/guides/upload-image", methods=["POST"])
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
```

`uuid4`, `secure_filename`, `jsonify`, `request`, `url_for`, and `ALLOWED_IMAGE_EXTENSIONS` are all already imported/defined — confirm with `grep -n "from uuid\|secure_filename\|ALLOWED_IMAGE_EXTENSIONS" app.py | head`.

- [ ] **Step 3: Commit**

```bash
git add app.py
git commit -m "feat(guides): add admin image upload endpoint for Quill"
```

---

## Task 7: Admin publish/edit route + Quill template

This is the biggest task — the WYSIWYG admin form.

**Files:**
- Modify: `app.py` (add `admin_guide_publish()` route, handles both new and edit)
- Create: `templates/guide_publish.html`

- [ ] **Step 1: Add the route**

In the same `# ==================== GUIDE ROUTES ====================` block, after `admin_guide_upload_image`, append:

```python
    @app.route("/admin/guides/new", methods=["GET", "POST"], endpoint="admin_guide_new")
    @app.route("/admin/guides/<int:guide_id>/edit", methods=["GET", "POST"], endpoint="admin_guide_edit")
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
            form_data = {
                "slug": request.form.get("slug", "").strip(),
                "category": request.form.get("category", "").strip(),
                "sort_order": request.form.get("sort_order", "100").strip() or "100",
                "published": request.form.get("published") == "1",
                "title_en": request.form.get("title_en", "").strip(),
                "title_zh": request.form.get("title_zh", "").strip(),
                "summary_en": request.form.get("summary_en", "").strip(),
                "summary_zh": request.form.get("summary_zh", "").strip(),
                "body_en": request.form.get("body_en", "").strip(),
                "body_zh": request.form.get("body_zh", "").strip(),
            }
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
        )
```

- [ ] **Step 2: Create the publish template**

Create `templates/guide_publish.html`:

```html
{% extends "base.html" %}
{% block title %}
  {% if editing %}{{ _('Edit Guide') }}{% else %}{{ _('New Guide') }}{% endif %} · Keydion
{% endblock %}
{% block content %}
<link rel="stylesheet" href="{{ url_for('static', filename='vendor/quill/quill.snow.css') }}">

<div class="container py-4" style="max-width: 980px;">
  <div class="d-flex justify-content-between align-items-center mb-4">
    <h1 class="h3 fw-semibold mb-0">
      {% if editing %}{{ _('Edit Guide') }}{% else %}{{ _('New Guide') }}{% endif %}
    </h1>
    <a class="btn btn-outline-secondary btn-sm" href="{{ url_for('admin_guides_manage') }}">
      {{ _('Back to manage') }}
    </a>
  </div>

  <form method="post" id="guideForm">
    <div class="row g-4 mb-4">
      <div class="col-md-6">
        <label class="form-label fw-semibold">{{ _('Category') }}</label>
        <select name="category" class="form-select">
          <option value="">{{ _('— Select —') }}</option>
          {% for cat in categories %}
            <option value="{{ cat }}" {% if form_data.category == cat %}selected{% endif %}>{{ cat }}</option>
          {% endfor %}
        </select>
      </div>
      <div class="col-md-3">
        <label class="form-label fw-semibold">{{ _('Sort order') }}</label>
        <input type="number" name="sort_order" class="form-control" value="{{ form_data.sort_order }}" min="0" max="9999">
      </div>
      <div class="col-md-3 d-flex align-items-end">
        <div class="form-check">
          <input type="checkbox" name="published" id="publishedCheck" value="1" class="form-check-input" {% if form_data.published %}checked{% endif %}>
          <label for="publishedCheck" class="form-check-label">{{ _('Published') }}</label>
        </div>
      </div>
      <div class="col-12">
        <label class="form-label fw-semibold">{{ _('Slug') }}</label>
        <input type="text" name="slug" id="slugInput" class="form-control" value="{{ form_data.slug }}" placeholder="auto-generated from title">
        <div class="form-text">{{ _('Used in the URL: /guides/<slug>. Lowercase letters, numbers, dashes only.') }}</div>
      </div>
    </div>

    <!-- English section -->
    <div class="card mb-4">
      <div class="card-header bg-light fw-semibold">English</div>
      <div class="card-body">
        <div class="mb-3">
          <label class="form-label">{{ _('Title (EN)') }}</label>
          <input type="text" name="title_en" class="form-control" value="{{ form_data.title_en }}">
        </div>
        <div class="mb-3">
          <label class="form-label">{{ _('Summary (EN)') }}</label>
          <input type="text" name="summary_en" class="form-control" value="{{ form_data.summary_en }}" placeholder="{{ _('Optional one-line description shown on the index') }}">
        </div>
        <div class="mb-2">
          <label class="form-label">{{ _('Body (EN)') }}</label>
          <div id="editorEn" style="min-height: 280px; background: #fff;"></div>
          <input type="hidden" name="body_en" id="bodyEnField" value="{{ form_data.body_en }}">
        </div>
      </div>
    </div>

    <!-- Chinese section -->
    <div class="card mb-4">
      <div class="card-header bg-light fw-semibold">中文</div>
      <div class="card-body">
        <div class="mb-3">
          <label class="form-label">{{ _('Title (ZH)') }}</label>
          <input type="text" name="title_zh" class="form-control" value="{{ form_data.title_zh }}">
        </div>
        <div class="mb-3">
          <label class="form-label">{{ _('Summary (ZH)') }}</label>
          <input type="text" name="summary_zh" class="form-control" value="{{ form_data.summary_zh }}">
        </div>
        <div class="mb-2">
          <label class="form-label">{{ _('Body (ZH)') }}</label>
          <div id="editorZh" style="min-height: 280px; background: #fff;"></div>
          <input type="hidden" name="body_zh" id="bodyZhField" value="{{ form_data.body_zh }}">
        </div>
      </div>
    </div>

    <div class="d-flex justify-content-end gap-2">
      <a href="{{ url_for('admin_guides_manage') }}" class="btn btn-outline-secondary">{{ _('Cancel') }}</a>
      <button type="submit" class="btn btn-primary">{{ _('Save') }}</button>
    </div>
  </form>

  {% if editing %}
    <!-- Delete must live outside the main form: nested <form> is invalid HTML. -->
    <form method="post" action="{{ url_for('admin_guide_delete', guide_id=guide_id) }}" class="mt-4"
          onsubmit="return confirm('{{ _('Delete this guide? This cannot be undone.') }}')">
      <button type="submit" class="btn btn-sm btn-outline-danger">{{ _('Delete guide') }}</button>
    </form>
  {% endif %}
</div>

<script src="{{ url_for('static', filename='vendor/quill/quill.min.js') }}"></script>
<script>
  (function () {
    var toolbar = [
      [{ header: [1, 2, 3, false] }],
      ['bold', 'italic', 'underline', 'strike'],
      [{ list: 'ordered' }, { list: 'bullet' }],
      ['blockquote', 'code-block'],
      ['link', 'image'],
      ['clean']
    ];

    function makeEditor(elId, hiddenId) {
      var hidden = document.getElementById(hiddenId);
      var editor = new Quill('#' + elId, {
        theme: 'snow',
        modules: { toolbar: toolbar }
      });
      // Load existing content
      if (hidden.value) {
        editor.clipboard.dangerouslyPasteHTML(0, hidden.value);
      }
      // Hook image button to our upload endpoint
      editor.getModule('toolbar').addHandler('image', function () {
        var input = document.createElement('input');
        input.type = 'file';
        input.accept = 'image/png,image/jpeg,image/gif,image/webp';
        input.onchange = function () {
          var file = input.files && input.files[0];
          if (!file) return;
          var fd = new FormData();
          fd.append('file', file);
          fetch("{{ url_for('admin_guide_upload_image') }}", {
            method: 'POST', body: fd
          })
            .then(function (r) { return r.json(); })
            .then(function (data) {
              if (data.url) {
                var range = editor.getSelection(true);
                editor.insertEmbed(range.index, 'image', data.url, 'user');
                editor.setSelection(range.index + 1);
              } else {
                alert(data.error || 'Upload failed');
              }
            })
            .catch(function () { alert("{{ _('Image upload failed.') }}"); });
        };
        input.click();
      });
      // Sync to hidden field on every change
      editor.on('text-change', function () {
        hidden.value = editor.root.innerHTML;
      });
      // Final sync on submit
      document.getElementById('guideForm').addEventListener('submit', function () {
        hidden.value = editor.root.innerHTML;
      });
    }

    makeEditor('editorEn', 'bodyEnField');
    makeEditor('editorZh', 'bodyZhField');

    // Auto-suggest slug from EN title
    var slugInput = document.getElementById('slugInput');
    var titleEn = document.querySelector('input[name="title_en"]');
    titleEn.addEventListener('blur', function () {
      if (!slugInput.value && titleEn.value) {
        slugInput.value = titleEn.value
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, '-')
          .replace(/^-+|-+$/g, '');
      }
    });
  })();
</script>
{% endblock %}
```

- [ ] **Step 3: Commit**

```bash
git add app.py templates/guide_publish.html
git commit -m "feat(guides): add admin publish/edit form with Quill WYSIWYG"
```

---

## Task 8: Admin manage page + delete route

**Files:**
- Modify: `app.py` (add `admin_guides_manage` and `admin_guide_delete` routes)
- Create: `templates/guide_manage.html`

- [ ] **Step 1: Add the manage and delete routes**

In the GUIDE ROUTES block, append:

```python
    @app.route("/admin/guides")
    def admin_guides_manage():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        guides = load_guides(published_only=False)
        return render_template("guide_manage.html", guides=guides, user=user)

    @app.route("/admin/guides/<int:guide_id>/delete", methods=["POST"])
    def admin_guide_delete(guide_id: int):
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        if delete_guide(guide_id):
            flash(_("Guide deleted."), "success")
        else:
            flash(_("Guide not found."), "warning")
        return redirect(url_for("admin_guides_manage"))
```

- [ ] **Step 2: Create the manage template**

Create `templates/guide_manage.html`:

```html
{% extends "base.html" %}
{% block title %}{{ _('Manage Guides · Keydion') }}{% endblock %}
{% block content %}
<div class="container py-4" style="max-width: 1080px;">
  <div class="d-flex justify-content-between align-items-center mb-4">
    <h1 class="h3 fw-semibold mb-0">{{ _('Manage Guides') }}</h1>
    <a class="btn btn-primary" href="{{ url_for('admin_guide_new') }}">
      + {{ _('New Guide') }}
    </a>
  </div>

  {% if not guides %}
    <div class="text-center py-5 text-muted">
      <p class="mb-3">{{ _('No guides yet.') }}</p>
      <a href="{{ url_for('admin_guide_new') }}" class="btn btn-outline-primary">{{ _('Create the first guide') }}</a>
    </div>
  {% else %}
    <div class="table-responsive">
      <table class="table table-hover align-middle">
        <thead class="table-light">
          <tr>
            <th>{{ _('Title (EN)') }}</th>
            <th>{{ _('Title (ZH)') }}</th>
            <th>{{ _('Category') }}</th>
            <th>{{ _('Slug') }}</th>
            <th>{{ _('Order') }}</th>
            <th>{{ _('Status') }}</th>
            <th class="text-end">{{ _('Actions') }}</th>
          </tr>
        </thead>
        <tbody>
          {% for g in guides %}
            <tr>
              <td>{{ g.title_en or '—' }}</td>
              <td>{{ g.title_zh or '—' }}</td>
              <td>{% if g.category %}<span class="badge bg-secondary">{{ g.category }}</span>{% endif %}</td>
              <td><code class="small">{{ g.slug }}</code></td>
              <td class="text-muted">{{ g.sort_order }}</td>
              <td>
                {% if g.published %}
                  <span class="badge bg-success">{{ _('Published') }}</span>
                {% else %}
                  <span class="badge bg-warning text-dark">{{ _('Draft') }}</span>
                {% endif %}
              </td>
              <td class="text-end">
                <a class="btn btn-sm btn-outline-primary" href="{{ url_for('admin_guide_edit', guide_id=g.id) }}">{{ _('Edit') }}</a>
                <form method="post" action="{{ url_for('admin_guide_delete', guide_id=g.id) }}" class="d-inline"
                      onsubmit="return confirm('{{ _('Delete this guide?') }}')">
                  <button type="submit" class="btn btn-sm btn-outline-danger">{{ _('Delete') }}</button>
                </form>
              </td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  {% endif %}
</div>
{% endblock %}
```

- [ ] **Step 3: Commit**

```bash
git add app.py templates/guide_manage.html
git commit -m "feat(guides): add admin manage page and delete route"
```

---

## Task 9: Wire footer link on landing page

**Files:**
- Modify: `templates/landing.html:1006`

- [ ] **Step 1: Update the footer link**

Find this exact line in `templates/landing.html`:

```html
            <li><a href="#">{{ _('Submission Guide') }}</a></li>
```

Replace with:

```html
            <li><a href="{{ url_for('guides') }}">{{ _('Submission Guide') }}</a></li>
```

- [ ] **Step 2: Commit**

```bash
git add templates/landing.html
git commit -m "feat(guides): point landing footer Submission Guide link to /guides"
```

---

## Task 10: Route contract test

**Files:**
- Create: `tests/test_guide_routes_contract.py`

- [ ] **Step 1: Write the test**

Create `tests/test_guide_routes_contract.py`:

```python
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GuideRoutesContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)
        cls.landing = (ROOT / "templates" / "landing.html").read_text(encoding="utf-8")

    def _route_decorators(self, func_name):
        """Return list of @app.route decorator argument dicts for a given function."""
        decorators = []
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                for dec in node.decorator_list:
                    if isinstance(dec, ast.Call) and getattr(dec.func, "attr", "") == "route":
                        path = dec.args[0].value if dec.args else None
                        methods = []
                        for kw in dec.keywords:
                            if kw.arg == "methods" and isinstance(kw.value, ast.List):
                                methods = [e.value for e in kw.value.elts]
                        decorators.append({"path": path, "methods": methods or ["GET"]})
        return decorators

    def _function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        return ""

    def test_public_routes_exist(self):
        self.assertEqual(self._route_decorators("guides"), [{"path": "/guides", "methods": ["GET"]}])
        self.assertEqual(
            self._route_decorators("guide_article"),
            [{"path": "/guides/<slug>", "methods": ["GET"]}],
        )

    def test_admin_manage_route(self):
        decs = self._route_decorators("admin_guides_manage")
        self.assertEqual(decs, [{"path": "/admin/guides", "methods": ["GET"]}])
        self.assertIn("require_login(level=3)", self._function_source("admin_guides_manage"))

    def test_admin_publish_route_handles_new_and_edit(self):
        decs = self._route_decorators("admin_guide_publish")
        paths = {(d["path"], tuple(sorted(d["methods"]))) for d in decs}
        self.assertIn(("/admin/guides/new", ("GET", "POST")), paths)
        self.assertIn(("/admin/guides/<int:guide_id>/edit", ("GET", "POST")), paths)
        self.assertIn("require_login(level=3)", self._function_source("admin_guide_publish"))

    def test_admin_delete_route(self):
        decs = self._route_decorators("admin_guide_delete")
        self.assertEqual(
            decs,
            [{"path": "/admin/guides/<int:guide_id>/delete", "methods": ["POST"]}],
        )
        self.assertIn("require_login(level=3)", self._function_source("admin_guide_delete"))

    def test_admin_upload_image_route(self):
        decs = self._route_decorators("admin_guide_upload_image")
        self.assertEqual(
            decs,
            [{"path": "/admin/guides/upload-image", "methods": ["POST"]}],
        )
        src = self._function_source("admin_guide_upload_image")
        self.assertIn("require_login(level=3)", src)
        self.assertIn("ALLOWED_IMAGE_EXTENSIONS", src)

    def test_landing_footer_links_to_guides(self):
        self.assertIn("url_for('guides')", self.landing)
        self.assertNotIn(
            '<li><a href="#">{{ _(\'Submission Guide\') }}</a></li>',
            self.landing,
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test, verify it passes**

```bash
python -m unittest tests/test_guide_routes_contract.py -v
```

Expected: all 6 tests pass. (If any fail, fix the corresponding route definition before continuing.)

- [ ] **Step 3: Commit**

```bash
git add tests/test_guide_routes_contract.py
git commit -m "test(guides): add AST contract test for guide routes"
```

---

## Task 11: Template contract test

**Files:**
- Create: `tests/test_guide_template_contract.py`

- [ ] **Step 1: Write the test**

Create `tests/test_guide_template_contract.py`:

```python
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class GuideTemplateContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.publish_tpl = (ROOT / "templates" / "guide_publish.html").read_text(encoding="utf-8")
        cls.manage_tpl = (ROOT / "templates" / "guide_manage.html").read_text(encoding="utf-8")
        cls.index_tpl = (ROOT / "templates" / "guides.html").read_text(encoding="utf-8")
        cls.article_tpl = (ROOT / "templates" / "guide_article.html").read_text(encoding="utf-8")

    def test_publish_form_has_all_body_fields(self):
        # Per-language title/summary inputs
        for name in ("title_en", "title_zh", "summary_en", "summary_zh"):
            self.assertIn(f'name="{name}"', self.publish_tpl,
                          f"publish template missing input name={name}")
        # Hidden body fields populated by Quill
        self.assertIn('name="body_en"', self.publish_tpl)
        self.assertIn('name="body_zh"', self.publish_tpl)
        self.assertIn('id="bodyEnField"', self.publish_tpl)
        self.assertIn('id="bodyZhField"', self.publish_tpl)
        # Metadata
        for name in ("slug", "category", "sort_order", "published"):
            self.assertIn(f'name="{name}"', self.publish_tpl)

    def test_publish_template_loads_quill(self):
        self.assertIn("vendor/quill/quill.snow.css", self.publish_tpl)
        self.assertIn("vendor/quill/quill.min.js", self.publish_tpl)
        self.assertIn("new Quill(", self.publish_tpl)

    def test_publish_template_wires_image_upload(self):
        self.assertIn("admin_guide_upload_image", self.publish_tpl)

    def test_manage_template_links_to_new_and_edit(self):
        self.assertIn("admin_guide_new", self.manage_tpl)
        self.assertIn("admin_guide_edit", self.manage_tpl)
        self.assertIn("admin_guide_delete", self.manage_tpl)

    def test_index_template_links_to_articles_by_slug(self):
        self.assertIn("guide_article", self.index_tpl)
        self.assertIn("slug=g.slug", self.index_tpl)

    def test_article_template_renders_body_safe(self):
        # body is sanitized server-side, so `| safe` is correct here
        self.assertIn("| safe", self.article_tpl)
        self.assertIn("guide.body_en", self.article_tpl)
        self.assertIn("guide.body_zh", self.article_tpl)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test, verify it passes**

```bash
python -m unittest tests/test_guide_template_contract.py -v
```

Expected: 6 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_guide_template_contract.py
git commit -m "test(guides): add template contract tests for guide pages"
```

---

## Task 12: i18n strings

**Files:**
- Modify: `translations/en/LC_MESSAGES/messages.po`, `translations/zh/LC_MESSAGES/messages.po`

- [ ] **Step 1: Extract translatable strings**

The project uses Flask-Babel. Check how `.po` files are typically updated:

```bash
ls translations/ && grep -l "Submission Guide" translations/*/LC_MESSAGES/messages.po
```

For each new `_(...)` string introduced in this feature, add a corresponding entry to **both** `translations/en/LC_MESSAGES/messages.po` and `translations/zh/LC_MESSAGES/messages.po`. The full list of new strings (copy-paste each as an `msgid`):

- `Guides`
- `Step-by-step help for using Keydion.`
- `No guides published yet. Check back soon.`
- `Back to guides`
- `Updated`
- `Guide not found.`
- `Please enter a title in at least one language.`
- `Please enter a slug.`
- `That slug is already taken. Pick another.`
- `Guide updated.`
- `Guide published.`
- `Guide deleted.`
- `Edit Guide`
- `New Guide`
- `Manage Guides`
- `Back to manage`
- `Category`
- `Sort order`
- `Published`
- `Slug`
- `Used in the URL: /guides/<slug>. Lowercase letters, numbers, dashes only.`
- `Title (EN)`
- `Title (ZH)`
- `Summary (EN)`
- `Summary (ZH)`
- `Optional one-line description shown on the index`
- `Body (EN)`
- `Body (ZH)`
- `— Select —`
- `Save`
- `Cancel`
- `Delete this guide? This cannot be undone.`
- `Delete guide`
- `Image upload failed.`
- `No guides yet.`
- `Create the first guide`
- `Order`
- `Status`
- `Actions`
- `Draft`
- `Edit`
- `Delete`
- `Delete this guide?`

Several of these (`Category`, `Save`, `Cancel`, `Edit`, `Delete`, `Published`, `Status`, `Actions`) likely already have translations from other features — `grep` to confirm before duplicating:

```bash
grep -n 'msgid "Category"' translations/en/LC_MESSAGES/messages.po
```

Only add entries for strings that aren't already present.

For the **English** `.po`, the `msgstr` is the same as the `msgid`. For the **Chinese** `.po`, provide Chinese translations; sample mapping (translate the rest using context):

| msgid | zh msgstr |
|---|---|
| `Guides` | `使用指南` |
| `Step-by-step help for using Keydion.` | `Keydion 使用步骤指引。` |
| `No guides published yet. Check back soon.` | `暂无指南，请稍后查看。` |
| `Back to guides` | `返回指南列表` |
| `Updated` | `更新于` |
| `Guide not found.` | `未找到该指南。` |
| `New Guide` | `新建指南` |
| `Edit Guide` | `编辑指南` |
| `Manage Guides` | `管理指南` |
| `Slug` | `链接标识` |
| `Sort order` | `排序顺序` |
| `Draft` | `草稿` |
| `Published` | `已发布` |

Use these patterns to fill in the rest with idiomatic Chinese.

- [ ] **Step 2: Compile translations**

```bash
python tools/compile_translations.py
```

Expected: command completes without errors, updating `.mo` files in both `translations/en/` and `translations/zh/`.

- [ ] **Step 3: Commit**

```bash
git add translations/
git commit -m "i18n(guides): add EN and ZH translations for guide UI strings"
```

---

## Task 13: End-to-end smoke test

This task is manual verification — no code change.

- [ ] **Step 1: Start the server**

```bash
./start_local.sh
```

- [ ] **Step 2: As role-3 admin, create a guide**

1. Visit `http://localhost:5000/admin/login`, log in as a role-3 user.
2. Visit `http://localhost:5000/admin/guides` — confirm the empty state renders.
3. Click "New Guide".
4. Fill the form:
   - Category: pick one
   - Title (EN): "How to Log In"
   - Body (EN): type some text, **bold** a phrase, insert a **link** (e.g. https://example.com), and **upload an image** through the Quill toolbar
   - Title (ZH): "如何登录"
   - Body (ZH): type some Chinese text
   - Tick "Published"
   - Save.
5. Confirm the manage page now shows the guide with "Published" badge.

- [ ] **Step 3: Verify public-facing pages**

1. Log out (or open an incognito window).
2. Visit `http://localhost:5000/guides` — confirm the guide appears under its category.
3. Click into it — confirm title, formatted body, uploaded image, and link all render. The image source should be under `/static/uploads/guides/`.
4. Switch the locale to ZH (use the language toggle if available, or set `?lang=zh`) and confirm the Chinese title and body render.
5. Visit the landing page — scroll to the footer and click "Submission Guide". Confirm it lands on `/guides`.

- [ ] **Step 4: Verify sanitization**

1. As admin, edit the guide.
2. Open browser devtools, find the hidden `<input id="bodyEnField">`, and manually set its value to: `<p>OK</p><script>alert(1)</script>`. (Or paste raw HTML containing a `<script>` tag via Quill's source view if available.)
3. Save the guide.
4. Visit it as a public user. Confirm `<script>` did not execute (no alert) and the saved HTML in DB does not contain `<script`.

You can verify the DB content directly:

```bash
python -c "from app import init_db, db_session, GuideModel; init_db()
from app import _SESSION_LOCAL
db = _SESSION_LOCAL()
for g in db.query(GuideModel).all():
    print(g.slug, '|', g.body_en[:200])
"
```

Expected: no `<script` substring in `body_en`.

- [ ] **Step 5: Run the full test suite**

```bash
python -m unittest discover -s tests -p "test_*.py" -v
```

Expected: all tests pass, including the 3 new guide test files (16 tests total added).

- [ ] **Step 6: Final commit (only if any cleanup needed)**

If any of the smoke tests above required code fixes, commit them now. Otherwise skip.

---

## Out of scope reminders (do not implement)

- Drag-to-reorder for guides
- Revision history beyond `published` flag
- Comments, ratings, view counters, search-within-guides
- Markdown import/export
- Category management UI for guides (admins edit `data/guide_categories.json` directly)
- "Save & Preview" button — single Save button only

These are deferred per the spec's §8.
