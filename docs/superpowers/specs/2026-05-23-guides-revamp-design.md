# Guides revamp — design spec

**Date:** 2026-05-23
**Scope:** Reskin the three guides templates (`guides.html`, `guide_article.html`, `guide_publish.html`) to match the design in `templates/Keydion-Guides-revamp.html`. Leave `guide_manage.html` on its current Bootstrap look.

## Source of the design

`templates/Keydion-Guides-revamp.html` is a bundled Claude-artifact mock containing three React-rendered artboards:

- **A · `GuidesIndex`** — public `/guides` index.
- **B · `GuideArticle`** — public `/guides/<slug>` article page.
- **C · `GuidePublish`** — admin edit form embedded in the dashboard shell.

The mock defines:

- A scholarly palette: cream (`#faf8f5`), dark red (`#8b1a1a`), gold accent, thin borders.
- A type system: Cormorant Garamond (display, italic-leaning), Source Serif 4 (body), JetBrains Mono (eyebrows, meta, code).
- Component classes prefixed `kd-*` covering header, page shell, headlines, category rows, guide list, article body, callouts, figures, prev/next, admin form, editor cards, Quill toolbar reskin, sticky form footer, toggle.

The mock is a visual reference. The implementation extends `base.html` (keeping the existing site chrome) and the existing `_dashboard_shell.html`. The `Keydion-Guides-revamp.html` file is deleted once the new templates land.

## Goals

1. Replace the visual treatment of the three guide pages with the mock's design.
2. Add three behavioral features on the admin edit form that the current form lacks:
   - per-language "status" indicator (title / summary / body filled),
   - sticky form footer with "● Unsaved changes" dirty tracking and `beforeunload` warning,
   - "Preview" button that opens the in-draft article in a new tab.
3. Add custom Quill blots for `kd-callout` and `kd-fig` so authors can compose the design's full body vocabulary.
4. Add prev/next navigation between published guides on the article page.

## Non-goals

- No redesign of `guide_manage.html` (admin list).
- No new admin actions (no bulk edit, no scheduled publishing).
- No DB schema change. Guide bodies remain HTML strings in `body_en` / `body_zh`.
- No sitewide chrome change. `base.html` header/footer/login modal stay as today.
- No rich text inside callouts/figures beyond plain text + an image src.

## File layout

**New files**

- `static/css/guides.css` — design tokens (`:root` CSS vars), `.kd-*` component classes, Quill toolbar reskin (overrides `quill.snow.css`). ~600 lines.
- `static/js/guides-editor.js` — Quill init, image upload, `CalloutBlot`, `FigureBlot`, toolbar handlers, per-language status pill, dirty tracker, `beforeunload`, slug auto-suggest, Preview submit, Delete confirm. One IIFE, no globals.

**Rewritten templates**

- `templates/guides.html` — new index markup (eyebrow + display headline + lede; two-column sticky category rows; numbered guide list with thin rules).
- `templates/guide_article.html` — new article markup (back link, single-locale title, meta strip with category pill + updated date + `EN · 中文` label, body wrapped in `.kd-body`, prev/next nav, optional preview banner).
- `templates/guide_publish.html` — now extends `_dashboard_shell.html` instead of `base.html`. Panel head with crumbs + back; meta row (category / sort order / published toggle / slug with `/guides/` prefix); EN and ZH `EditorCard`s each with status pill, reskinned Quill toolbar, callout + figure buttons; sticky form footer with delete · unsaved · cancel · preview · save.

**Touched (small, surgical)**

- `templates/base.html` — add one Google Fonts `<link>` to `<head>`: Cormorant Garamond 400/500 italic, Source Serif 4 400/500/600, JetBrains Mono 400/500. Also add the `guides.css` link so any page can use the tokens without re-loading.
- `app.py`:
  - `guides()` route: compute `total = len(all_guides)` and pass to template.
  - `guide_article()` route: load the full flat list of published guides ordered by category and `sort_order`, find current by slug, pass `prev_guide` and `next_guide` (each `None` if absent) to template.
  - New endpoint `admin_guide_preview` (POST, role 3) — reads form data via shared `_read_guide_form()` helper, renders `guide_article.html` with `preview_mode=True`, `prev_guide=None`, `next_guide=None`.
  - New helper `_read_guide_form(form)` — extracts the dict currently built inline in `admin_guide_publish`; called from both publish and preview.

**Untouched**

- `templates/guide_manage.html`.
- `static/vendor/quill/*`.
- DB schema; `_load_papers`, `_load_guide_categories`, `load_guides` and all other guide helpers.

**Deleted**

- `templates/Keydion-Guides-revamp.html` (reference artifact, not a template).

## Page-by-page treatment

### Public index — `templates/guides.html`

Structure inside `{% block content %}`: a single `<div class="kd-page">` containing a `<main class="kd-main">` containing a `<div class="kd-wrap">` (max-width 880px). Inside that:

1. **Hero block** — eyebrow row reading `{{ _('Guides') }} · NN ARTICLES`, then `<h1 class="kd-h-display">` with the second word wrapped in `<em>` (italic, accent-colored), then `<p class="kd-lede">` with a one-line description. All copy goes through gettext.
2. **Category rows** — `{% for category, items in grouped %}` producing `<section class="kd-cat-row">` with a sticky left column showing the category label in italic display type plus a sub-line `NN Articles`, and a right column containing `<ol class="kd-guide-list">` of `<li class="kd-guide-item">` items. Each item has a two-digit padded counter, title, optional summary, and a `→` arrow.
3. **Empty state** — when `not grouped`, render hero plus a single mono-styled "No guides published yet" line. Mock doesn't cover this; we add it defensively.

Route changes:
- Add `total=len(all_guides)` to the `render_template` call.
- Category label localization: if `_load_guide_categories()` already returns locale-keyed labels, use directly; otherwise add a small `_localize_category(label, locale)` helper.

### Public article — `templates/guide_article.html`

Structure: `<div class="kd-page">` → `<main class="kd-main">` → `<div class="kd-wrap" style="max-width:760px;">`. Inside:

1. **Preview banner** (when `preview_mode` is true) — single eyebrow-style line on `var(--accent-tint)` background reading "Preview · not yet published".
2. **Back link** — `<a class="kd-back">` with `←` glyph linking to `/guides`.
3. **Title** — `<h1 class="kd-h-page">` with single-locale title (zh if `current_locale == 'zh'` and `title_zh` set, else `title_en or title_zh`).
4. **Meta strip** — `<div class="kd-article-meta">` with category pill (`.kd-cat-pill`), separator dot, "Last updated YYYY-MM-DD", separator dot, static `EN · 中文` label.
5. **Body** — `<article class="kd-body">{{ body | safe }}</article>`. All styling lives in `guides.css`.
6. **Prev/Next nav** — `<nav class="kd-prevnext">` with two grid cells. Each cell either an `<a>` with label and title, or a `<span class="disabled">` with "No previous guide" / "No next guide". Suppressed entirely when both are absent (preview mode).

Route changes:
- `guide_article(slug)` loads the full ordered list of published guides, finds current by slug, computes `prev_guide`/`next_guide`, passes them and `preview_mode=False` to the template.

Removed: the existing inline `<style>` block at the bottom of `guide_article.html`. Its rules are superseded by `.kd-body *` selectors in `guides.css`.

### Admin edit form — `templates/guide_publish.html`

Top of file changes from `{% extends "base.html" %}` to `{% extends "_dashboard_shell.html" %}`, content moves into `{% block panel %}`. Loads `quill.snow.css` for the editor (overridden by `guides.css`).

Structure:

1. **Panel head** — `.kd-panel-head` with breadcrumb (`Manage guides / <slug or "new">`), display-type title (`Edit guide` / `New guide` with second word in `<em>`), sub-line description, and a back-button on the right.
2. **Form** — single `<form id="guideForm" method="post">`:
   - **Meta row** (`.kd-form-meta`, 4-column grid): Category select, Sort order number input, Published custom toggle (clickable label flipping a hidden checkbox), Slug input with `/guides/` prefix decoration.
   - **EN editor card** (`.kd-editor-card[data-lang="en"]`): header with language label and status pill, fields section with Title (required) and Summary inputs, body section with `#editorEn` Quill mount and `#bodyEnField` hidden input.
   - **ZH editor card** — same shape, `_zh` fields and `#editorZh` / `#bodyZhField`.
   - **Sticky form footer** (`.kd-form-footer`): left = Delete button (only when `editing`), right = dirty state label + Cancel + Preview + Save.
3. **Hidden delete form** (only when `editing`) — separate top-level form with `id="deleteGuideForm"` posting to `admin_guide_delete`. The visible "Delete" button in the footer submits this via JS, avoiding nested forms.

#### Wired behaviors in `guides-editor.js`

A single IIFE registers blots, initializes editors, then wires:

1. **Quill init + image upload** — ports lines 113-149 of current `guide_publish.html` verbatim. Init order is strict: `Quill.register(blots)` → `new Quill(...)` → `editor.clipboard.dangerouslyPasteHTML(0, hidden.value)`. Otherwise existing kd-callout / kd-fig markup won't materialize back into blot instances on edit.
2. **Status pill** — per `.kd-editor-card`: gather inputs with `[data-required]` plus the Quill instance. Recompute status on every input/change/`text-change`. Status logic: empty title → "Title missing" (amber), empty summary → "Summary missing" (amber), empty body → "Body missing" (amber), else "All fields filled" (green). Body emptiness checked via `editor.getText().trim().length === 0`.
3. **Dirty tracker** — snapshot the entire form (FormData + both editors' HTML) at load. Recompute on every input/change/`text-change`. When `current !== initial`: set `[data-dirty-state]` text to `● Unsaved changes`, install `window.onbeforeunload` handler. On form `submit`, clear handler. See known limitation below regarding server-side errors.
4. **Published toggle** — clicking `.kd-toggle` flips the `.on` class and the hidden `#publishedCheck` checked attribute. Status label switches between "Live" and "Draft".
5. **Delete button** — `confirm()` then `document.getElementById('deleteGuideForm').submit()`.
6. **Preview button** — build a transient `<form method="post" action="{{ admin_guide_preview }}" target="_blank">` containing copies of every visible form field + the two hidden body fields (synced from Quill first), append to body, submit, remove. Opens in a new tab.
7. **Slug auto-suggest** — preserved verbatim from current template.

#### Custom Quill blots

Both extend `Quill.import('blots/block/embed')` and use `static blotName` / `static tagName` / `static className`.

**`CalloutBlot`**:

```js
class CalloutBlot extends BlockEmbed {
  static blotName = 'callout';
  static tagName = 'div';
  static className = 'kd-callout';
  static create(value) {
    const node = super.create();
    const label = document.createElement('div');
    label.className = 'kd-callout-label';
    label.contentEditable = 'true';
    label.textContent = (value && value.label) || 'Note';
    const body = document.createElement('div');
    body.className = 'kd-callout-body';
    body.contentEditable = 'true';
    body.innerHTML = '<p>' + escapeHtml((value && value.body) || 'Type your callout here.') + '</p>';
    node.appendChild(label);
    node.appendChild(body);
    return node;
  }
  static value(node) {
    return {
      label: (node.querySelector('.kd-callout-label')?.textContent || '').trim(),
      body:  (node.querySelector('.kd-callout-body')?.textContent || '').trim(),
    };
  }
}
```

**Sanitization decision:** `value()` returns `textContent` (not `innerHTML`) for both label and body. `create()` always wraps body in `<p>` and escapes HTML. Trade-off: no `<strong>` or `<em>` inside callouts. Benefit: zero XSS surface from authors. Recommended.

**`FigureBlot`**: analogous, materializes `<div class="kd-fig"><div class="kd-fig-img" data-src="…" style="background-image:url(…)">SCREENSHOT</div><div class="kd-fig-caption"><span class="num">Fig. NN</span><span class="caption-text">…</span></div></div>`. `value()` returns `{src, num, caption}`. Image insertion reuses `admin_guides_upload_image` POST endpoint. Image src is validated client-side: must be either same-origin `/static/uploads/guides/...` or `https://`-scheme; anything else falls back to a placeholder div.

**Toolbar buttons** — added to Quill toolbar config as `['callout', 'figure']`. Quill renders `<button class="ql-callout">` / `<button class="ql-figure">` with no icon by default. After Quill init, replace innerHTML with inline SVGs to get the mock's iconography.

**Backward compatibility** — existing guide bodies in the DB contain only standard Quill HTML. They load on the new edit page without errors; the new blots simply don't match anything in their content. The public article page picks up the new CSS for h2/h3/p/ol/ul/blockquote/img/a automatically.

#### Server-side changes

`_read_guide_form(form)` extracted from current `admin_guide_publish` (app.py:2338-2400 region — the dict assembled today inline). Returns the same dict shape. Single source of truth for both publish and preview.

`admin_guide_preview` endpoint (auth-guard pattern must match `admin_guide_publish` — the spec sketch below uses `isinstance(user, dict)` but the implementation should mirror whatever the existing `require_login(3)` callers do):

```python
@app.route("/dashboard/admin/guides/preview", methods=["POST"], endpoint="admin_guide_preview")
def admin_guide_preview():
    user = require_login(3)
    if not isinstance(user, dict): return user
    data = _read_guide_form(request.form)
    guide = {
        "slug": data["slug"] or "preview",
        "category": data["category"],
        "title_en": data["title_en"], "title_zh": data["title_zh"],
        "summary_en": data["summary_en"], "summary_zh": data["summary_zh"],
        "body_en": data["body_en"], "body_zh": data["body_zh"],
        "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "published": data["published"],
    }
    return render_template(
        "guide_article.html",
        guide=guide,
        prev_guide=None,
        next_guide=None,
        preview_mode=True,
    )
```

No persistence, no DB writes. The route exists only to render the in-draft form state through the same template the published page uses.

## Testing

The repo's convention is contract tests (AST parse `app.py` + render Jinja2 with mock data). No browser integration.

**New test files**

1. **`tests/test_guides_revamp_templates.py`** — render each template with mock data:
   - `guides.html` with grouped data of two categories, three guides each. Assert two-digit padded counters present, `.kd-cat-row` and `.kd-guide-list` present, summary line present when given, empty state renders when grouped is empty.
   - `guide_article.html` with a guide dict, both with and without `prev_guide`/`next_guide`. Assert back link, category pill, body wrapped in `<article class="kd-body">`. With `preview_mode=True`, assert preview banner present.
   - `guide_publish.html` for both new and edit modes. Assert both `[data-lang="en"]` and `[data-lang="zh"]` editor cards present, `[data-status]` indicators present, `.kd-form-footer` present. Delete button absent when not editing.

2. **`tests/test_guide_dom_contract.py`** — read `static/js/guides-editor.js` as text, regex-collect every `document.getElementById('X')`, `querySelector('#X')`, `querySelector('[data-X]')`, and assert each selector matches at least one element in the rendered `guide_publish.html`. Mirrors the DOM-contract pattern called out in CLAUDE.md.

3. **`tests/test_guide_route_contract.py`** — AST-parse `app.py`:
   - `guides()` passes `total` to `render_template`.
   - `guide_article()` passes `prev_guide` and `next_guide` to `render_template`.
   - `admin_guide_preview` endpoint exists with `methods=["POST"]` and a `require_login(3)` call.
   - `_read_guide_form` helper defined and called from both `admin_guide_publish` and `admin_guide_preview`.

4. **`tests/test_guides_css_contract.py`** — read `static/css/guides.css`, assert it defines the CSS vars (`--cream`, `--accent`, `--serif`, `--display`, `--mono`, `--ink`, etc.) and every class the new templates reference (`kd-page`, `kd-main`, `kd-wrap`, `kd-cat-row`, `kd-guide-list`, `kd-editor-card`, `kd-form-footer`, `kd-callout`, `kd-fig`, `kd-h-display`, `kd-h-page`, `kd-eyebrow`, `kd-lede`, …). Catches class typos without a browser.

**Explicitly not tested**

- Blot round-trip in Quill (manual smoke).
- Sticky-footer visual behavior (manual smoke).
- `beforeunload` firing (manual smoke).

## Manual smoke checklist (for PR description)

- [ ] `/guides` index renders with seeded guides; sticky category labels stay in place on scroll.
- [ ] `/guides/<slug>` renders with prev/next, callout, figure, ordered list, blockquote all styled per mock.
- [ ] EN ↔ ZH language switch on both public pages shows the right locale's title/summary/body.
- [ ] Status pill on edit form flips to amber when summary is cleared, green when refilled.
- [ ] Sticky footer dirty state flips to `● Unsaved changes` on edit; resets on save.
- [ ] `beforeunload` warning fires on dirty navigation; doesn't fire after save.
- [ ] Insert callout → save → reload → callout renders editable with same text.
- [ ] Insert figure with uploaded image → save → reload → figure renders with image + caption.
- [ ] Preview button opens new tab with the article as currently drafted; banner present.
- [ ] Existing guides (no callout/figure markup) render correctly on both public and edit pages.
- [ ] Delete button on edit page asks for confirmation, removes the guide.

## Rollout

Single PR, single deploy. No feature flag (small site, internal authors, visual refactor of three templates plus one new route). Risk profile: edit-form regression > public-page regression, so the smoke checklist is weighted toward the admin flow.

No data migration. No DB schema change. The new blot markup only appears in guides edited after deploy; existing bodies render under the new CSS unchanged.

## Open questions / known limitations

- **Dirty-tracker snapshot after server-side errors:** if the server returns the form with errors (e.g., slug collision), the page reloads with `form_data` reflecting the submitted-but-not-saved state. The dirty tracker takes a fresh snapshot on load and reports "saved", even though changes haven't been persisted to the DB. Acceptable for v1; document in the PR.
- **Categories.json localization:** unclear from current code whether `_load_guide_categories()` returns locale-aware labels. To verify and either use directly or add a `_localize_category()` helper during implementation.
- **Existing guide bodies under the new `<ol>` styling:** any existing guide that uses an ordered list inline will pick up the dramatic stepped-rule treatment. Authors should be told this is the new house style for ordered lists; if a guide needs a non-narrated numbered list, use bullets or rewrite as prose.
