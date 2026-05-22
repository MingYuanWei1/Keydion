# User Guides — Design Spec

**Date:** 2026-05-22
**Status:** Approved (pending implementation plan)

## 1. Summary

A self-contained "Guides" subsystem for publishing categorized how-to articles
(Login, Upload, News, etc.). Public read access, role-3 admin authoring via a
Quill WYSIWYG editor, per-language EN/ZH content, and in-editor image uploads.

The footer "Submission Guide" link on the landing page is repurposed to point
at the new guide index.

## 2. User-facing pages

### `/guides` — Guide index

- Linked from `landing.html:1006` (currently `href="#"`).
- Page title and a 1-line intro.
- Guides rendered grouped by category, in admin-controlled order (ascending
  `sort_order`).
- Each row: title plus optional 1-line summary; clicking opens the article.
- Categories with zero published guides are hidden.
- Visual style: "precise and simple" — list layout, not cards.

### `/guides/<slug>` — Guide article

- Title, category badge, last-updated date, rendered body HTML.
- "← Back to guides" link.
- Locale-aware: shows `title_zh`/`body_zh` for `zh` users, `title_en`/`body_en`
  for `en` users. Fallback: if the locale-specific field is empty, render the
  other language's content rather than 404.

## 3. Admin pages (role ≥ 3)

### `/admin/guides` — Manage guides

Mirrors `news_manage.html` in layout.

- Table of all guides (published and unpublished): title, category, published
  Y/N, sort_order, actions (Edit / Toggle published / Delete).
- Buttons: "New guide", link to manage categories.

### `/admin/guides/new` and `/admin/guides/<id>/edit`

One template, `guide_publish.html`, modeled after `news_publish.html`.

Form fields:

- Category — dropdown sourced from `data/guide_categories.json`
- Slug — auto-generated from EN title, editable, validated unique
- Sort order — integer (default 100)
- Published — checkbox
- **EN section:** `title_en`, `summary_en` (optional), `body_en` (Quill editor)
- **ZH section:** `title_zh`, `summary_zh` (optional), `body_zh` (Quill editor)

Buttons: Save, Save & Preview, Delete (edit page only).

### `/admin/guides/upload-image` (POST)

- Role ≥ 3 required.
- Accepts a single image file (multipart form).
- Validates: extension in {png, jpg, jpeg, gif, webp}, max size 5 MB.
- Saves to `static/uploads/guides/<uuid>.<ext>`.
- Returns JSON `{"url": "/static/uploads/guides/<uuid>.<ext>"}` for Quill to
  embed in the body.

## 4. Data model

New `GuideModel` in `app.py`:

| Column | Type | Notes |
|---|---|---|
| `id` | int PK | |
| `slug` | str(120), unique, indexed | URL slug, e.g. `login` |
| `category` | str(80) | Must match an entry in `guide_categories.json` |
| `sort_order` | int, default 100 | Ascending; lower = higher in category |
| `published` | bool, default False | |
| `title_en` | str(200) | |
| `title_zh` | str(200) | |
| `summary_en` | str(300), nullable | Optional 1-line index description |
| `summary_zh` | str(300), nullable | |
| `body_en` | TEXT | Sanitized HTML from Quill |
| `body_zh` | TEXT | Sanitized HTML from Quill |
| `created_at` | datetime | Set on insert |
| `updated_at` | datetime | Updated on every save |

**Sanitization:** apply `bleach.clean()` with an allowlist on save (not on
render — stored HTML is already clean). Allowed tags: `h1`, `h2`, `h3`, `h4`,
`p`, `strong`, `em`, `u`, `s`, `ul`, `ol`, `li`, `a`, `img`, `blockquote`,
`code`, `pre`, `br`, `hr`, `span`, `div`. Allowed attributes: `a[href, title,
target]`, `img[src, alt, width, height]`, `span[class]`, `div[class]`. URL
schemes restricted to `http`, `https`, `/` (relative).

**DB migration:** add to `init_db()` as a `CREATE TABLE IF NOT EXISTS`
following the existing ad-hoc pattern. No try/except `ALTER` needed for a new
table.

## 5. File additions

```
templates/
  guides.html                  # public index
  guide_article.html           # public single guide
  guide_manage.html            # admin list
  guide_publish.html           # admin new/edit
data/
  guide_categories.json        # mirrors news_categories.json shape
static/
  vendor/quill/quill.min.js
  vendor/quill/quill.snow.css
  uploads/guides/              # gitignored; created at runtime
```

`app.py` additions:

- `GuideModel` class (after `NewsArticleModel`)
- `GUIDE_FIELDS` constant (list of column names)
- Helpers: `_load_guides(category=None, published_only=True)`,
  `_load_guide_categories()`, `_slugify(text)`,
  `_sanitize_guide_html(html)`
- Routes:
  - `guides()` → GET `/guides`
  - `guide_article(slug)` → GET `/guides/<slug>`
  - `admin_guides_manage()` → GET `/admin/guides`
  - `admin_guide_publish(guide_id=None)` → GET/POST `/admin/guides/new` and
    `/admin/guides/<id>/edit`
  - `admin_guide_delete(guide_id)` → POST `/admin/guides/<id>/delete`
  - `admin_guide_upload_image()` → POST `/admin/guides/upload-image`
- Update `landing.html:1006` footer link from `"#"` to `url_for('guides')`

`.gitignore`: add `static/uploads/guides/`.

## 6. i18n

- All static UI strings in the new templates wrapped in `_()` / `_l()`.
- New entries added to `translations/en/LC_MESSAGES/messages.po` and
  `translations/zh/LC_MESSAGES/messages.po`.
- Run `python tools/compile_translations.py` after editing `.po` files.
- Guide content itself is per-language fields on the model, not Babel-
  translated.

## 7. Testing

Following the project's contract-test pattern (AST-parse `app.py`, render
Jinja templates with mock data):

- `tests/test_guide_routes_contract.py` — verify the 6 new routes exist with
  correct HTTP methods and the expected `require_login` level on admin routes.
- `tests/test_guide_template_contract.py` — render `guide_publish.html` with
  mock data; assert each field in `GUIDE_FIELDS` (excluding `id`,
  `created_at`, `updated_at`) appears as a form input.
- `tests/test_guide_sanitization_contract.py` — call
  `_sanitize_guide_html()` with `<script>alert(1)</script>` and `<a
  href="javascript:...">` payloads; assert both are stripped.

## 8. Out of scope (deliberate YAGNI)

- Drag-to-reorder (numeric `sort_order` only)
- Revision history / drafts beyond the `published` flag
- Comments, ratings, view counts, search-within-guides
- Markdown import/export
- Bulk image upload / image library browser

## 9. New dependencies

- `bleach` — HTML sanitizer, pinned in `requirements.txt`.
- Quill 1.3.x — vendored JS/CSS in `static/vendor/quill/`, no npm/build step.
  Source: official CDN release tarball.

## 10. Risks and mitigations

- **XSS via WYSIWYG body** — mitigated by `bleach` sanitization on save with a
  strict allowlist. Tested in
  `tests/test_guide_sanitization_contract.py`.
- **Image upload abuse** — mitigated by role ≥ 3 requirement, extension
  allowlist, and 5 MB size cap.
- **Slug collisions** — slug column has a unique constraint; the publish
  handler returns a form error if the user-edited slug clashes.
- **Empty locale fallback** — guide article handler falls back to the other
  language rather than 404'ing on a half-translated guide.
