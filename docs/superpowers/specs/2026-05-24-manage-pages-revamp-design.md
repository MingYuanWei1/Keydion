# Manage News & Manage Guides revamp — design spec

**Date:** 2026-05-24
**Source:** `handoff/` package dropped into repo root (README + PORT_NOTES + new templates + `manage.css` + standalone preview HTML)
**Scope:** Drop in the two redesigned templates and the shared CSS, AND wire up all three optional new endpoints so the new UI is fully functional rather than degrading behind feature flags.

## Goal

Replace two dashboard pages — `templates/news_manage.html` and `templates/guide_manage.html` — with the redesigned versions from `handoff/`, restyled to the cream-paper / serif / crimson system already established by `dashboard.css`. The redesign adds filters, sort, bulk actions, drag-to-reorder, and inline publish toggles. Both pages must remain operable for editor (level 2) / curator (level 3) roles as today.

## Non-goals

- No changes to `NewsArticleModel`, `GuideModel`, or any other DB schema.
- No new `word_count` field on guides (template degrades gracefully when absent).
- No changes to the publish/edit pages (`news_publish.html`, `guide_publish.html`).
- No refactor of `app.py` beyond adding the three new endpoints.
- No new design tokens; reuse `--cream / --paper-warm / --border / --ink / --accent` from `static/css/styles.css`.

## File changes

| Path | Action | Source |
|------|--------|--------|
| `templates/news_manage.html` | replace | `handoff/templates/news_manage.html` |
| `templates/guide_manage.html` | replace | `handoff/templates/guide_manage.html` |
| `static/css/manage.css` | create | `handoff/static/css/manage.css` |
| `app.py` | edit (add ~30 lines) | new endpoints, see below |
| `translations/zh/LC_MESSAGES/messages.po` | edit | new gettext strings (compile after) |
| `tests/test_manage_pages_contract.py` | create | smoke render tests |

## New endpoints

All three are written in the project's idioms, NOT the handoff's sketch idioms:

- `require_login(level=N)` (NOT `@login_required`)
- `with db_session() as db:` context manager (NOT `db.session` directly)
- `_()` for any user-visible strings
- Routes mounted under `/dashboard/...` to match the existing manage pages

### 1. News bulk action — `POST /dashboard/news/bulk_action`

```python
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
                    r.published_at = datetime.utcnow().isoformat()
            elif op == "unpublish":
                r.status = "pending"
            elif op == "delete":
                db.delete(r)
            affected += 1
        db.commit()
    return jsonify(ok=True, affected=affected)
```

Notes:
- `NewsArticleModel.id` is `Unicode(255)`, so coerce to `str`, not `int`.
- Publishing a never-published article sets `published_at` (mirrors existing `news_publish` logic at app.py:4291).
- App uses `status="pending"` for drafts (not `"draft"`).

### 2. Guide reorder — `POST /dashboard/admin/guides/reorder`

```python
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
```

### 3. Guide inline publish toggle — `POST /dashboard/admin/guides/<id>/toggle`

```python
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
```

All three endpoints register inside the `create_app()` function alongside the existing news/guide routes (around app.py:2470 and app.py:2620).

## Template wiring

Each template ships with a feature-flag stub at the top of its `<script>` block. We replace each stub with the real `url_for(...)` value so the flag becomes effectively "on":

**news_manage.html** — replace:
```js
var BULK_ENABLED = false;
var BULK_URL = "";
```
with:
```js
var BULK_ENABLED = true;
var BULK_URL = "{{ url_for('news_bulk_action') }}";
```

**guide_manage.html** — replace:
```js
var REORDER_URL = "";
var TOGGLE_URL_TEMPLATE = "";
```
with:
```js
var REORDER_URL = "{{ url_for('admin_guides_reorder') }}";
var TOGGLE_URL_TEMPLATE = "{{ url_for('admin_guide_toggle_published', guide_id=0) | replace('/0/', '/{id}/') }}";
```

(The `replace` trick produces a URL template the JS can `.replace('{id}', actualId)` against, without hardcoding the `/dashboard/admin/guides/...` prefix.)

Implementation step verifies these are the exact variable names by reading the handoff template `<script>` blocks before swap-in.

## CSRF

The codebase has no Flask-WTF CSRF protection on existing JSON POST endpoints (verified at app.py:2408 `news_categories_add`, etc.). Match this convention — no CSRF tokens on the new endpoints. (If/when the team adds global CSRF, these routes get the same treatment as the existing JSON routes.)

## i18n

The new templates introduce roughly 40 new `{{ _('...') }}` strings (search input placeholders, chip labels, bulk action labels, empty-state copy, toast messages, etc.). Steps:

1. After dropping templates in, extract messages with `pybabel extract -F babel.cfg -o messages.pot .` (or whatever the existing flow uses — check `tools/compile_translations.py`).
2. Merge into `translations/zh/LC_MESSAGES/messages.po` and add Chinese translations for the new strings. English uses the source text.
3. Run `python tools/compile_translations.py` to produce `.mo` files.
4. JS-emitted toast strings: the handoff template renders these via `{{ _('...') | tojson }}` server-side and the JS reads them as a `const STRINGS = { ... }` block, so they go through the same i18n flow — no separate JS translation system needed. Verify this pattern matches what handoff uses; if it inlines plain English strings in JS, wrap them via the same `tojson` trick.

## Tests

New file `tests/test_manage_pages_contract.py` with three checks (all following the existing AST/Jinja contract test style):

1. **Routes are registered**: parse `app.py` with `ast`, walk for `@app.route("/dashboard/news/bulk_action", ...)`, `@app.route("/dashboard/admin/guides/reorder", ...)`, `@app.route("/dashboard/admin/guides/<int:guide_id>/toggle", ...)`. Each must exist exactly once.
2. **Templates render**: instantiate a Jinja2 environment pointed at `templates/` with the same filters/globals as the real app (`_`, `url_for` stubbed to return a sentinel string), render both templates with realistic mock `articles` / `categories` / `guides` data including the optional `word_count` and `updated_at` attributes. Assert no exception and that the rendered HTML contains expected anchors (`kp-toolbar`, `kp-table`, the JS feature-flag constants point at non-empty URLs).
3. **Feature flags wired**: rendered `news_manage.html` HTML contains `BULK_ENABLED = true` and a non-empty `BULK_URL`; rendered `guide_manage.html` HTML contains non-empty `REORDER_URL` and `TOGGLE_URL_TEMPLATE` with `{id}` placeholder.

## Manual verification checklist

Before declaring done, run the dev server (`./start_local.sh`) and walk through:

- Open `/dashboard/news/manage` as a level-2 user. Verify search, status chips, category filter, date range, sort all filter the visible rows live.
- Select rows → bulk action bar appears → "Publish" / "Move to draft" / "Delete" each hit the new endpoint and the table updates.
- Open category management modal → add / rename / delete a category still works (existing endpoints unchanged).
- Open `/dashboard/admin/guides` as a level-3 user. Verify rows are grouped by category, drag handle reorders within a category, dragging across categories changes `category`, both are persisted across reload.
- Toggle the inline publish switch on a guide → state persists across reload.
- Copy-link button copies the `/guides/<slug>` URL; preview link opens the public page.
- Resize browser window narrow (< 820px wide) → guide rows reflow to stacked layout via container queries.
- Switch language between EN and ZH → all new strings render in the active locale.

## Risk notes

- **Drag-to-reorder race**: rapid successive drag operations could send overlapping POSTs. The endpoint is idempotent (last-write-wins on `sort_order`), but the client should debounce. Verify the handoff JS already debounces (it appears to — quick scan shows a `setTimeout` around the persist call); if not, add a 300ms debounce.
- **Bulk delete is not transactional across HTTP failures**: if the network drops mid-bulk-delete, some rows may be deleted and the client won't know which. Acceptable — the user can refresh and try again on the survivors. Not worth a multi-step protocol.
- **`scheduled` status chip**: handoff template shows it only when `sched_count > 0`. App code never emits `"scheduled"`, so this chip stays hidden until somebody adds scheduling. Harmless.
- **Legacy redirects**: existing `/news/manage` and `/admin/guides` legacy redirects (app.py:2481, app.py:2660) keep working untouched.

## Out-of-scope follow-ups (not in this spec)

- Scheduled-publish workflow (the chip exists but no backend).
- Server-side filtering/pagination (everything is client-side; fine until article count grows past a few hundred).
- Bulk operations on guides (not requested, not in handoff).
