# Guides Revamp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reskin the three guide templates (`guides.html`, `guide_article.html`, `guide_publish.html`) to match the design in `templates/Keydion-Guides-revamp.html`, add Preview / dirty-tracking / per-language status behaviors, and add custom Quill blots for callouts and figures.

**Architecture:** Templates continue to extend `base.html` (public) and `_dashboard_shell.html` (admin). Shared design tokens and `.kd-*` component classes live in one new `static/css/guides.css`. Quill blots, status pill, dirty tracker, and Preview/Delete wiring live in one new `static/js/guides-editor.js`. Server side gets one new POST route (`admin_guide_preview`), one new helper (`_read_guide_form`), small additions to `guides()` and `guide_article()`, and a small extension to the bleach allowlist for the new `<img class="kd-fig-img">` markup. No DB schema change.

**Tech Stack:** Flask + Jinja2 + Bootstrap (existing chrome), Quill 1.3 editor (existing), bleach sanitizer (existing). New typography from Google Fonts (Cormorant Garamond, Source Serif 4, JetBrains Mono).

**Spec:** `docs/superpowers/specs/2026-05-23-guides-revamp-design.md`. Read it before starting Task 1.

**Key conventions:**
- Conventional commits: `feat:`, `fix:`, `test:`, `style:`, `chore:`, with optional scope like `feat(guides):`.
- Tests live in `tests/`, use `unittest`, are run with `python -m unittest discover -s tests -p "test_*.py" -v` or a single file with `python -m unittest tests/test_FILE.py -v`.
- Repo CLAUDE.md is at the root; the worktree path is `.claude/worktrees/guides-revamp-spec/`. All paths below are repo-relative.
- After every task: run the full test suite at minimum once per task to catch regressions early.

---

## Task 1: Create `static/css/guides.css` skeleton and wire it into `base.html`

**Files:**
- Create: `static/css/guides.css`
- Modify: `templates/base.html` (head section, after existing stylesheets)
- Test: `tests/test_guides_css_contract.py` (new)

The CSS file starts with only design tokens. Subsequent tasks add component classes as they're needed by templates. This avoids "all 600 lines of CSS in one commit" and lets contract tests track which classes exist as we go.

- [ ] **Step 1: Write the failing test**

Create `tests/test_guides_css_contract.py`:

```python
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GuidesCssContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        css_path = ROOT / "static" / "css" / "guides.css"
        cls.css = css_path.read_text(encoding="utf-8") if css_path.exists() else ""

    def test_defines_design_tokens(self):
        # CSS custom properties under :root
        for var in [
            "--cream", "--cream-2", "--paper", "--border", "--border-soft",
            "--ink", "--ink-soft", "--muted", "--muted-2",
            "--accent", "--accent-hover", "--accent-tint", "--gold",
            "--serif", "--display", "--sans", "--mono",
        ]:
            self.assertIn(var, self.css, f"guides.css missing token {var}")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guides_css_contract.py -v`
Expected: FAIL with `AssertionError: guides.css missing token --cream` (file is empty or absent).

- [ ] **Step 3: Create `static/css/guides.css` with design tokens**

```css
/* Keydion — Guides design tokens and components.
   Ported from templates/Keydion-Guides-revamp.html (Claude artifact mock).
   Loaded sitewide via base.html so any page can opt into the typography. */

:root {
  --cream:        #faf8f5;
  --cream-2:      #f3efe8;
  --paper:        #ffffff;
  --border:       #e0dbd0;
  --border-soft:  #ebe6dc;
  --ink:          #1a1a1a;
  --ink-soft:     #3d3d3d;
  --muted:        #6b6b6b;
  --muted-2:      #8e857a;
  --accent:       #8b1a1a;
  --accent-hover: #6b1212;
  --accent-tint:  #fdf6f6;
  --gold:         #a07a2a;

  --serif:        "Source Serif 4", "Source Serif Pro", Georgia, "Noto Serif SC", serif;
  --display:      "Cormorant Garamond", "Source Serif 4", Georgia, serif;
  --sans:         -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
  --mono:         "JetBrains Mono", "IBM Plex Mono", ui-monospace, Menlo, monospace;
}
```

- [ ] **Step 4: Add Google Fonts and guides.css to `templates/base.html`**

In `templates/base.html`, after line 9 (`<link rel="stylesheet" href="{{ url_for('static', filename='vendor/bootstrap.min.css') }}">`) and before line 10 (`<link rel="stylesheet" href="{{ url_for('static', filename='css/styles.css') }}">`), insert:

```html
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,500;0,600;1,400;1,500&family=Source+Serif+4:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap">
  <link rel="stylesheet" href="{{ url_for('static', filename='css/guides.css') }}">
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m unittest tests/test_guides_css_contract.py -v`
Expected: PASS (1 test).

Also run full suite to ensure nothing broke: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add static/css/guides.css templates/base.html tests/test_guides_css_contract.py
git commit -m "feat(guides): add CSS tokens skeleton and load Google Fonts"
```

---

## Task 2: Port full `.kd-*` component classes into `guides.css`

**Files:**
- Modify: `static/css/guides.css` (append after `:root`)
- Modify: `tests/test_guides_css_contract.py` (add class-coverage assertion)

This is the bulk-CSS step. The source of truth is the embedded `<style>` block in `templates/Keydion-Guides-revamp.html`. Extract the post-`:root` rules (everything from `.kd { ... }` through the end of the design-system style block) verbatim, with three modifications noted in Step 3.

- [ ] **Step 1: Extend the failing test**

Add a second test to `tests/test_guides_css_contract.py` after `test_defines_design_tokens`:

```python
    def test_defines_component_classes(self):
        for cls in [
            "kd", "kd-header", "kd-footer", "kd-page", "kd-main", "kd-wrap",
            "kd-eyebrow", "kd-h-display", "kd-h-page", "kd-lede", "kd-meta",
            "kd-cat-row", "kd-cat-label", "kd-cat-count",
            "kd-guide-list", "kd-guide-item", "kd-guide-num",
            "kd-guide-link", "kd-guide-title", "kd-guide-summary", "kd-guide-arrow",
            "kd-back", "kd-article-meta", "kd-cat-pill", "kd-body",
            "kd-callout", "kd-callout-label", "kd-callout-body",
            "kd-fig", "kd-fig-img", "kd-fig-caption",
            "kd-prevnext",
            "kd-form-head", "kd-form-meta", "kd-field", "kd-field-label", "kd-field-hint",
            "kd-input", "kd-select", "kd-input-prefix",
            "kd-toggle", "kd-toggle-track", "kd-toggle-status",
            "kd-editor-card", "kd-editor-head", "kd-editor-lang", "kd-editor-status",
            "kd-editor-fields",
            "kd-ql-toolbar", "kd-ql-group", "kd-ql-btn", "kd-ql-select", "kd-ql-canvas",
            "kd-form-footer", "kd-saved",
            "kd-btn", "kd-btn-primary", "kd-btn-ghost", "kd-btn-danger",
            "kd-hairline", "kd-panel", "kd-panel-head",
        ]:
            self.assertIn(f".{cls}", self.css, f"guides.css missing class .{cls}")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guides_css_contract.py::GuidesCssContractTest::test_defines_component_classes -v`
Expected: FAIL on the first missing class.

- [ ] **Step 3: Append all `.kd-*` rules to `guides.css`**

Open `templates/Keydion-Guides-revamp.html`. Find the second `<style>` block (begins with `/* Keydion — Guides revamp design system */`) and copy everything from `.kd { ... }` through the end of that style block (`}` before `</style>`) into `guides.css`.

**Three modifications during the paste:**

1. **The `.kd-header`, `.kd-header-inner`, `.kd-logo`, `.kd-nav`, `.kd-signin`, `.kd-lang`, `.kd-footer`, `.kd-footer-inner`** rules go in but are NOT used by the production templates (which keep base.html's chrome). Keeping them does no harm — they're scoped to `.kd-header` and `.kd-footer` which the production templates don't render. Include them anyway for completeness.

2. **`.kd-fig-img`**: in the mock this is a striped placeholder div. In production it will be an `<img>` element (because bleach's allowlist permits `img src` but not `div data-src` or `div style`). Replace the rule with:

   ```css
   .kd-fig-img {
     display: block;
     width: 100%;
     height: auto;
     max-height: 480px;
     object-fit: cover;
     border: 1px solid var(--border);
   }
   ```

   This makes `kd-fig-img` work on a real `<img>`. The mock's diagonal-stripe placeholder treatment is dropped (only shown when there's no image, which never happens in production).

3. **Append the dashboard-shell port** — the third `<style>` block in the mock (begins with `/* Keydion · Dashboard shell — ported from static/css/dashboard.css */`) is mocked because the artifact had to fake the shell. The production form is inside the real `_dashboard_shell.html`, which loads `static/css/dashboard.css`. So **do NOT copy that third style block**. Skip it entirely.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests/test_guides_css_contract.py -v`
Expected: PASS (2 tests).

Also run: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add static/css/guides.css tests/test_guides_css_contract.py
git commit -m "feat(guides): port .kd-* component classes from design mock"
```

---

## Task 3: Extract `_read_guide_form(form)` helper

**Files:**
- Modify: `app.py` (refactor `admin_guide_publish`, app.py:2362-2380)
- Test: `tests/test_guide_routes_contract.py` (add helper-existence assertion)

Extract the dict built inside the `if request.method == "POST":` branch into a module-level helper so the new Preview route can use the same form-parsing logic.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_routes_contract.py` inside the `GuideRoutesContractTest` class:

```python
    def test_read_guide_form_helper_exists(self):
        # A helper that maps request.form -> the canonical guide form dict.
        self.assertIn("def _read_guide_form(", self.app_source,
                      "expected module-level helper _read_guide_form(form)")

    def test_admin_guide_publish_uses_read_guide_form(self):
        src = self._function_source("admin_guide_publish")
        self.assertIn("_read_guide_form(request.form)", src)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_routes_contract.py::GuideRoutesContractTest::test_read_guide_form_helper_exists -v`
Expected: FAIL (`_read_guide_form` not defined).

- [ ] **Step 3: Add the helper to `app.py`**

Find a good module-level home for the helper — adjacent to other guide helpers, before `def _load_guide_categories()` (currently around line 3864). Insert:

```python
def _read_guide_form(form) -> dict:
    """Parse a guide POST form into the canonical form_data dict.

    Called from admin_guide_publish (which then validates and persists) and
    admin_guide_preview (which renders the article template without persisting).
    Slug normalization stays in admin_guide_publish since preview tolerates
    a blank or invalid slug.
    """
    return {
        "slug": form.get("slug", "").strip(),
        "category": form.get("category", "").strip(),
        "sort_order": form.get("sort_order", "100").strip() or "100",
        "published": form.get("published") == "1",
        "title_en": form.get("title_en", "").strip(),
        "title_zh": form.get("title_zh", "").strip(),
        "summary_en": form.get("summary_en", "").strip(),
        "summary_zh": form.get("summary_zh", "").strip(),
        "body_en": form.get("body_en", "").strip(),
        "body_zh": form.get("body_zh", "").strip(),
    }
```

- [ ] **Step 4: Refactor `admin_guide_publish` to use it**

In `app.py` around line 2362-2374, replace the inline dict:

```python
        if request.method == "POST":
            form_data = {
                "slug": request.form.get("slug", "").strip(),
                # ... 10 more lines ...
                "body_zh": request.form.get("body_zh", "").strip(),
            }
```

with:

```python
        if request.method == "POST":
            form_data = _read_guide_form(request.form)
```

Leave the subsequent slug-normalization and validation logic exactly as it is.

- [ ] **Step 5: Run tests**

Run: `python -m unittest tests/test_guide_routes_contract.py -v`
Expected: PASS.

Also run: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add app.py tests/test_guide_routes_contract.py
git commit -m "refactor(guides): extract _read_guide_form helper"
```

---

## Task 4: Add `admin_guide_preview` route

**Files:**
- Modify: `app.py` (add new route adjacent to `admin_guide_delete`, around line 2425)
- Test: `tests/test_guide_routes_contract.py` (add route assertion)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_routes_contract.py`:

```python
    def test_admin_guide_preview_route(self):
        decs = self._route_decorators("admin_guide_preview")
        self.assertEqual(
            decs,
            [{"path": "/dashboard/admin/guides/preview", "methods": ["POST"]}],
        )
        src = self._function_source("admin_guide_preview")
        self.assertIn("require_login(level=3)", src)
        self.assertIn("_read_guide_form(request.form)", src)
        self.assertIn('render_template("guide_article.html"', src)
        self.assertIn("preview_mode=True", src)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_routes_contract.py::GuideRoutesContractTest::test_admin_guide_preview_route -v`
Expected: FAIL.

- [ ] **Step 3: Implement the route**

In `app.py`, locate `admin_guide_delete` (around line 2416). Add the preview route directly below it, inside the same `register_routes(app)` block (or wherever the other guide routes are registered — the existing routes will show the pattern):

```python
    @app.route("/dashboard/admin/guides/preview", methods=["POST"], endpoint="admin_guide_preview")
    def admin_guide_preview():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        data = _read_guide_form(request.form)
        # Sanitize bodies the same way the persisted save path would, so the
        # preview reflects exactly what would end up in the DB.
        data["body_en"] = _sanitize_guide_html(data.get("body_en", ""))
        data["body_zh"] = _sanitize_guide_html(data.get("body_zh", ""))
        guide = {
            "slug": data["slug"] or "preview",
            "category": data["category"],
            "title_en": data["title_en"],
            "title_zh": data["title_zh"],
            "summary_en": data["summary_en"],
            "summary_zh": data["summary_zh"],
            "body_en": data["body_en"],
            "body_zh": data["body_zh"],
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

Verify `datetime` is already imported at the top of `app.py`. If not, add `from datetime import datetime`.

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_routes_contract.py -v`
Expected: PASS.

Also: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_guide_routes_contract.py
git commit -m "feat(guides): add admin_guide_preview route for in-draft preview"
```

---

## Task 5: Pass prev/next guides from `guide_article` route

**Files:**
- Modify: `app.py` (the `guide_article` route, around line 2307)
- Test: `tests/test_guide_routes_contract.py` (add render_template assertion)

The route currently passes only `guide`. The new template expects `prev_guide`, `next_guide`, `preview_mode` (which defaults to False on the public path).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_routes_contract.py`:

```python
    def test_guide_article_passes_prev_next(self):
        src = self._function_source("guide_article")
        self.assertIn("prev_guide=", src)
        self.assertIn("next_guide=", src)
        self.assertIn("preview_mode=False", src)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_routes_contract.py::GuideRoutesContractTest::test_guide_article_passes_prev_next -v`
Expected: FAIL.

- [ ] **Step 3: Modify the `guide_article` route**

In `app.py`, find the `guide_article` function (around line 2307). Replace:

```python
    @app.route("/guides/<slug>")
    def guide_article(slug):
        guide = get_guide_by_slug(slug)
        if not guide or not guide.get("published"):
            abort(404)
        return render_template("guide_article.html", guide=guide)
```

with:

```python
    @app.route("/guides/<slug>")
    def guide_article(slug):
        guide = get_guide_by_slug(slug)
        if not guide or not guide.get("published"):
            abort(404)
        # Compute prev/next from the same ordered list the index uses.
        flat = load_guides(published_only=True)
        idx = next((i for i, g in enumerate(flat) if g.get("slug") == slug), -1)
        prev_guide = flat[idx - 1] if idx > 0 else None
        next_guide = flat[idx + 1] if 0 <= idx < len(flat) - 1 else None
        return render_template(
            "guide_article.html",
            guide=guide,
            prev_guide=prev_guide,
            next_guide=next_guide,
            preview_mode=False,
        )
```

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_routes_contract.py -v`
Expected: PASS.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_guide_routes_contract.py
git commit -m "feat(guides): pass prev/next neighbors to article template"
```

---

## Task 6: Pass `total` from `guides()` route

**Files:**
- Modify: `app.py` (the `guides` route, around line 2284)
- Test: `tests/test_guide_routes_contract.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_routes_contract.py`:

```python
    def test_guides_index_passes_total(self):
        src = self._function_source("guides")
        self.assertIn("total=", src)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_routes_contract.py::GuideRoutesContractTest::test_guides_index_passes_total -v`
Expected: FAIL.

- [ ] **Step 3: Modify the route**

In the existing `guides()` route, find the `render_template("guides.html", grouped=grouped)` call and change it to `render_template("guides.html", grouped=grouped, total=len(all_guides))`.

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_routes_contract.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_guide_routes_contract.py
git commit -m "feat(guides): pass total guide count to index template"
```

---

## Task 7: Extend sanitizer allowlist for figure images

**Files:**
- Modify: `app.py` (around line 3798, `GUIDE_ALLOWED_ATTRS`)
- Modify: `tests/test_guide_sanitization_contract.py` (add round-trip tests)

The new figure markup uses `<img class="kd-fig-img">`, but `GUIDE_ALLOWED_ATTRS["img"]` is currently `["src", "alt", "width", "height"]` — `class` is missing. Without this fix, every figure save would strip the class and the CSS wouldn't apply on the public page.

Callout markup (`<div class="kd-callout">`, `<span class="num">`, etc.) already survives sanitization because `div` and `span` already allow `class`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_guide_sanitization_contract.py`:

```python
    def test_preserves_callout_markup(self):
        html = (
            '<div class="kd-callout">'
            '<div class="kd-callout-label">Note</div>'
            '<div class="kd-callout-body"><p>Be careful.</p></div>'
            '</div>'
        )
        result = _sanitize_guide_html(html)
        self.assertIn('class="kd-callout"', result)
        self.assertIn('class="kd-callout-label"', result)
        self.assertIn('class="kd-callout-body"', result)
        self.assertIn("Be careful.", result)

    def test_preserves_figure_markup_with_image_class(self):
        html = (
            '<div class="kd-fig">'
            '<img class="kd-fig-img" src="/static/uploads/guides/x.png" alt="">'
            '<div class="kd-fig-caption">'
            '<span class="num">Fig. 01</span>'
            '<span class="caption-text">An example.</span>'
            '</div></div>'
        )
        result = _sanitize_guide_html(html)
        self.assertIn('class="kd-fig"', result)
        self.assertIn('class="kd-fig-img"', result)
        self.assertIn('src="/static/uploads/guides/x.png"', result)
        self.assertIn('class="num"', result)
        self.assertIn("Fig. 01", result)
        self.assertIn("An example.", result)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests/test_guide_sanitization_contract.py -v`
Expected: `test_preserves_callout_markup` passes (existing rules cover it), `test_preserves_figure_markup_with_image_class` FAILS (`class="kd-fig-img"` stripped from `<img>`).

- [ ] **Step 3: Extend the allowlist**

In `app.py` around line 3798, replace:

```python
GUIDE_ALLOWED_ATTRS = {
    "a": ["href", "title", "target", "rel"],
    "img": ["src", "alt", "width", "height"],
    "span": ["class"],
    "div": ["class"],
}
```

with:

```python
GUIDE_ALLOWED_ATTRS = {
    "a": ["href", "title", "target", "rel"],
    "img": ["src", "alt", "width", "height", "class"],
    "span": ["class"],
    "div": ["class"],
    "p": ["class"],
}
```

(`p` gains `class` so future `<p class="...">` inside a callout body would survive — defensive, no current consumer.)

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_sanitization_contract.py -v`
Expected: all pass.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add app.py tests/test_guide_sanitization_contract.py
git commit -m "feat(guides): allow class on img and p in sanitizer for figures"
```

---

## Task 8: Rewrite `templates/guides.html` with new markup

**Files:**
- Rewrite: `templates/guides.html`
- Modify: `tests/test_guide_template_contract.py` (update index-template assertions)

- [ ] **Step 1: Update the failing tests**

In `tests/test_guide_template_contract.py`, the existing `test_index_template_links_to_articles_by_slug` already asserts `"slug=g.slug"` is present. Add a new test below it:

```python
    def test_index_template_uses_new_design(self):
        self.assertIn("kd-page", self.index_tpl)
        self.assertIn("kd-eyebrow", self.index_tpl)
        self.assertIn("kd-h-display", self.index_tpl)
        self.assertIn("kd-lede", self.index_tpl)
        self.assertIn("kd-cat-row", self.index_tpl)
        self.assertIn("kd-cat-label", self.index_tpl)
        self.assertIn("kd-guide-list", self.index_tpl)
        self.assertIn("kd-guide-item", self.index_tpl)
        self.assertIn("kd-guide-num", self.index_tpl)
        # Two-digit padded counter format used somewhere
        self.assertIn("'%02d'", self.index_tpl)
        # Empty-state message preserved
        self.assertIn("No guides published yet", self.index_tpl)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests/test_guide_template_contract.py::GuideTemplateContractTest::test_index_template_uses_new_design -v`
Expected: FAIL.

- [ ] **Step 3: Rewrite `templates/guides.html`**

Replace the entire file with:

```jinja2
{% extends "base.html" %}
{% block title %}{{ _('Guides') }} · Keydion{% endblock %}
{% block content %}
<div class="kd-page">
  <div class="kd-wrap">
    <div style="margin-bottom: 72px;">
      <div class="kd-eyebrow" style="margin-bottom: 24px;">
        {{ _('Guides') }}
        <span style="color: var(--accent); margin: 0 8px;">·</span>
        {{ '%02d'|format(total or 0) }} {{ _('ARTICLES') }}
      </div>
      <h1 class="kd-h-display" style="margin-bottom: 24px;">
        {{ _('Step-by-step') }} <em>{{ _('help.') }}</em>
      </h1>
      <p class="kd-lede">{{ _('How to upload, publish, and curate work on Keydion.') }}</p>
    </div>

    {% if not grouped %}
      <div style="padding: 48px 0; color: var(--muted); font-family: var(--mono); font-size: 11.5px; letter-spacing: 0.06em; text-transform: uppercase;">
        {{ _('No guides published yet. Check back soon.') }}
      </div>
    {% else %}
      {% for category, items in grouped %}
        <section class="kd-cat-row">
          <div class="kd-cat-label">
            {{ category }}
            <span class="kd-cat-count">{{ '%02d'|format(items|length) }} {{ _('Articles') }}</span>
          </div>
          <ol class="kd-guide-list">
            {% for g in items %}
              {% set title = g.title_zh if current_locale == 'zh' and g.title_zh else g.title_en or g.title_zh %}
              {% set summary = g.summary_zh if current_locale == 'zh' and g.summary_zh else g.summary_en or g.summary_zh %}
              <li class="kd-guide-item">
                <span class="kd-guide-num">{{ '%02d'|format(loop.index) }}</span>
                <a class="kd-guide-link" href="{{ url_for('guide_article', slug=g.slug) }}">
                  <div class="kd-guide-title">{{ title }}</div>
                  {% if summary %}<div class="kd-guide-summary">{{ summary }}</div>{% endif %}
                </a>
                <span class="kd-guide-arrow">→</span>
              </li>
            {% endfor %}
          </ol>
        </section>
      {% endfor %}
    {% endif %}
  </div>
</div>
{% endblock %}
```

Note: the outer `.kd-page` is wrapped *inside* base.html's `<main class="py-4"><div class="container">…`. The mock's outer chrome (`<main class="kd-main">`) isn't needed because base.html already supplies the page wrapper. The `.kd-wrap` div constrains width.

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_template_contract.py -v`
Expected: PASS.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add templates/guides.html tests/test_guide_template_contract.py
git commit -m "feat(guides): redesign /guides index with new design tokens"
```

---

## Task 9: Rewrite `templates/guide_article.html` with prev/next + preview banner

**Files:**
- Rewrite: `templates/guide_article.html`
- Modify: `tests/test_guide_template_contract.py`

- [ ] **Step 1: Update the failing tests**

Replace the existing `test_article_template_renders_body_safe` in `tests/test_guide_template_contract.py` with:

```python
    def test_article_template_renders_body_safe(self):
        self.assertIn("| safe", self.article_tpl)
        self.assertIn("guide.body_en", self.article_tpl)
        self.assertIn("guide.body_zh", self.article_tpl)

    def test_article_template_uses_new_design(self):
        self.assertIn("kd-page", self.article_tpl)
        self.assertIn("kd-back", self.article_tpl)
        self.assertIn("kd-h-page", self.article_tpl)
        self.assertIn("kd-article-meta", self.article_tpl)
        self.assertIn("kd-cat-pill", self.article_tpl)
        self.assertIn('article class="kd-body"', self.article_tpl)
        self.assertIn("kd-prevnext", self.article_tpl)
        # prev/next rendered, optional via if/else
        self.assertIn("prev_guide", self.article_tpl)
        self.assertIn("next_guide", self.article_tpl)
        # preview banner shown when preview_mode is true
        self.assertIn("preview_mode", self.article_tpl)
        self.assertIn("Preview", self.article_tpl)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests/test_guide_template_contract.py::GuideTemplateContractTest::test_article_template_uses_new_design -v`
Expected: FAIL.

- [ ] **Step 3: Rewrite `templates/guide_article.html`**

Replace the entire file with:

```jinja2
{% extends "base.html" %}
{% set title = guide.title_zh if current_locale == 'zh' and guide.title_zh else (guide.title_en or guide.title_zh) %}
{% set body = guide.body_zh if current_locale == 'zh' and guide.body_zh else (guide.body_en or guide.body_zh) %}
{% block title %}{{ title }} · Keydion{% endblock %}
{% block content %}
<div class="kd-page">
  <div class="kd-wrap" style="max-width: 760px;">
    {% if preview_mode %}
      <div class="kd-eyebrow" style="text-align: center; padding: 10px 0; background: var(--accent-tint); margin-bottom: 32px;">
        {{ _('Preview · not yet published') }}
      </div>
    {% endif %}

    <a class="kd-back" href="{{ url_for('guides') }}">
      <span>←</span> {{ _('Back to guides') }}
    </a>

    <h1 class="kd-h-page" style="margin-top: 32px; margin-bottom: 0;">{{ title }}</h1>

    <div class="kd-article-meta">
      {% if guide.category %}
        <span class="kd-cat-pill">{{ guide.category }}</span>
        <span class="sep">·</span>
      {% endif %}
      {% if guide.updated_at %}
        <span>{{ _('Last updated') }} {{ guide.updated_at[:10] }}</span>
        <span class="sep">·</span>
      {% endif %}
      <span>EN&nbsp;·&nbsp;中文</span>
    </div>

    <article class="kd-body">{{ body | safe }}</article>

    {% if prev_guide or next_guide %}
      <nav class="kd-prevnext">
        {% if prev_guide %}
          <a href="{{ url_for('guide_article', slug=prev_guide.slug) }}">
            <div class="label">← {{ _('Previous') }}</div>
            <div class="title">{{ prev_guide.title_zh if current_locale == 'zh' and prev_guide.title_zh else (prev_guide.title_en or prev_guide.title_zh) }}</div>
          </a>
        {% else %}
          <span class="disabled" style="padding: 28px 32px;">
            <div class="label">← {{ _('Previous') }}</div>
            <div class="title">{{ _('No previous guide') }}</div>
          </span>
        {% endif %}
        {% if next_guide %}
          <a href="{{ url_for('guide_article', slug=next_guide.slug) }}" class="right">
            <div class="label">{{ _('Next') }} →</div>
            <div class="title">{{ next_guide.title_zh if current_locale == 'zh' and next_guide.title_zh else (next_guide.title_en or next_guide.title_zh) }}</div>
          </a>
        {% else %}
          <span class="disabled right" style="padding: 28px 32px;">
            <div class="label">{{ _('Next') }} →</div>
            <div class="title">{{ _('No next guide') }}</div>
          </span>
        {% endif %}
      </nav>
    {% endif %}
  </div>
</div>
{% endblock %}
```

The old inline `<style>` block (lines 24-34 of the old file) is gone — all body styling now lives in `guides.css`.

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_template_contract.py -v`
Expected: PASS (all).

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add templates/guide_article.html tests/test_guide_template_contract.py
git commit -m "feat(guides): redesign article page with prev/next and preview banner"
```

---

## Task 10: Update existing publish-template tests for JS extraction

**Files:**
- Modify: `tests/test_guide_template_contract.py`

The current `test_publish_template_loads_quill` and `test_publish_template_wires_image_upload` assert that `"new Quill("` and `"admin_guides_upload_image"` appear in the publish template. In the next tasks both move to `static/js/guides-editor.js`. Update those tests now so the next task's rewrite doesn't trigger spurious failures.

- [ ] **Step 1: Update the two tests in place**

Replace `test_publish_template_loads_quill` and `test_publish_template_wires_image_upload` with versions that target the new JS file:

```python
    def test_publish_template_loads_quill(self):
        # Quill CSS still loaded from the template; init moves to the JS module.
        self.assertIn("vendor/quill/quill.snow.css", self.publish_tpl)
        self.assertIn("vendor/quill/quill.min.js", self.publish_tpl)
        self.assertIn("js/guides-editor.js", self.publish_tpl)

    def test_publish_template_wires_image_upload(self):
        # Image upload endpoint is referenced from the publish template (so the
        # JS module can read it as a data-* attribute) AND used in the JS module.
        self.assertIn("admin_guides_upload_image", self.publish_tpl)
        js_path = ROOT / "static" / "js" / "guides-editor.js"
        js = js_path.read_text(encoding="utf-8") if js_path.exists() else ""
        self.assertIn("admin_guides_upload_image", js + self.publish_tpl)
```

The template must still emit the URL somehow so the JS can use it. The pattern we'll use in Task 11: the form root carries `data-upload-image-url="{{ url_for('admin_guides_upload_image') }}"`, which keeps `admin_guides_upload_image` in the template text and gives the JS a way to read the URL without templating into the JS file.

- [ ] **Step 2: Run tests — they should still pass (Quill init currently in template)**

Run: `python -m unittest tests/test_guide_template_contract.py::GuideTemplateContractTest::test_publish_template_loads_quill -v`
Expected: FAIL (no `js/guides-editor.js` reference yet). This is intentional — we'll satisfy these once Task 11/12 land.

Actually: `test_publish_template_loads_quill` now requires `js/guides-editor.js` which doesn't exist yet. Don't run the full suite green here; instead skip those two tests via `@unittest.skip` until Task 13 completes. Add the decorator now:

```python
    @unittest.skip("Re-enabled after Task 13 lands guides-editor.js")
    def test_publish_template_loads_quill(self):
        ...

    @unittest.skip("Re-enabled after Task 13 lands guides-editor.js")
    def test_publish_template_wires_image_upload(self):
        ...
```

- [ ] **Step 3: Verify full suite still passes**

Run: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass (skipped tests show as skipped, not failed).

- [ ] **Step 4: Commit**

```bash
git add tests/test_guide_template_contract.py
git commit -m "test(guides): retarget publish-template assertions to upcoming JS module"
```

---

## Task 11: Rewrite `templates/guide_publish.html` to use dashboard shell + new markup

**Files:**
- Rewrite: `templates/guide_publish.html`
- Modify: `app.py` (`admin_guide_publish` render_template call to pass `user`)
- Modify: `tests/test_guide_template_contract.py`

The template moves from extending `base.html` to extending `_dashboard_shell.html`. The form keeps the same field names (no breaking change on POST). JS extraction comes in Task 12 — for now leave a minimal inline script that only auto-suggests the slug (because Quill init has to move into the external module, but a minimal template is needed first so Task 12 has DOM to bind to).

Actually, simpler: in this task, keep the OLD inline JS verbatim at the bottom of the file (Quill init + slug suggest + image handler). Task 12 moves it out. This way the form stays functional after each task.

- [ ] **Step 1: Add new template-shape tests**

Append to `tests/test_guide_template_contract.py`:

```python
    def test_publish_template_uses_dashboard_shell(self):
        self.assertIn('extends "_dashboard_shell.html"', self.publish_tpl)
        self.assertIn("{% block panel %}", self.publish_tpl)

    def test_publish_template_has_new_form_structure(self):
        # Editor cards per language
        self.assertIn('data-lang="en"', self.publish_tpl)
        self.assertIn('data-lang="zh"', self.publish_tpl)
        # Slug prefix decoration
        self.assertIn("kd-input-prefix", self.publish_tpl)
        # Custom published toggle
        self.assertIn("kd-toggle", self.publish_tpl)
        # Sticky form footer with dirty state
        self.assertIn("kd-form-footer", self.publish_tpl)
        self.assertIn("data-dirty-state", self.publish_tpl)
        # Preview and delete buttons
        self.assertIn("data-preview-guide", self.publish_tpl)
        # Status pill
        self.assertIn("data-status", self.publish_tpl)
        # URL exposed for JS image upload
        self.assertIn("data-upload-image-url", self.publish_tpl)

    def test_publish_template_keeps_field_names(self):
        # Backward compat: the POST handler must still see these names
        for name in ("slug", "category", "sort_order", "published",
                     "title_en", "title_zh", "summary_en", "summary_zh",
                     "body_en", "body_zh"):
            self.assertIn(f'name="{name}"', self.publish_tpl)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests/test_guide_template_contract.py -v`
Expected: the four new tests FAIL; existing ones pass.

- [ ] **Step 3: Rewrite the template**

Replace `templates/guide_publish.html` with:

```jinja2
{% extends "_dashboard_shell.html" %}
{% block title %}
  {% if editing %}{{ _('Edit Guide') }}{% else %}{{ _('New Guide') }}{% endif %} · Keydion
{% endblock %}
{% block panel %}
<link rel="stylesheet" href="{{ url_for('static', filename='vendor/quill/quill.snow.css') }}">

<div class="kd-panel"
     data-upload-image-url="{{ url_for('admin_guides_upload_image') }}"
     data-preview-url="{{ url_for('admin_guide_preview') }}"
     {% if editing %}data-delete-url="{{ url_for('admin_guide_delete', guide_id=guide_id) }}"{% endif %}>
  <div class="kd-panel-head">
    <div class="kd-panel-head__left">
      <div class="kd-panel-head__crumb">
        <a href="{{ url_for('admin_guides_manage') }}">{{ _('Manage guides') }}</a>
        <span class="sep">/</span>
        <span>{{ form_data.slug or _('new') }}</span>
      </div>
      <h1 class="kd-panel-head__title">
        {% if editing %}{{ _('Edit') }} <em>{{ _('guide') }}</em>{% else %}{{ _('New') }} <em>{{ _('guide') }}</em>{% endif %}
      </h1>
      <p class="kd-panel-head__sub">{{ _('Publish dual-language guides with rich text.') }}</p>
    </div>
    <div class="kd-panel-head__actions">
      <a href="{{ url_for('admin_guides_manage') }}" class="kd-panel-head__back">
        ← {{ _('Manage guides') }}
      </a>
    </div>
  </div>

  <form method="post" id="guideForm">
    <div class="kd-form-meta">
      <div class="kd-field">
        <label class="kd-field-label">
          {{ _('Category') }} <span class="req">*</span>
        </label>
        <select name="category" class="kd-select">
          <option value="">{{ _('— Select —') }}</option>
          {% for cat in categories %}
            <option value="{{ cat }}" {% if form_data.category == cat %}selected{% endif %}>{{ cat }}</option>
          {% endfor %}
        </select>
      </div>
      <div class="kd-field">
        <label class="kd-field-label">{{ _('Sort order') }}</label>
        <input class="kd-input" type="number" name="sort_order"
               value="{{ form_data.sort_order }}" min="0" max="9999">
        <span class="kd-field-hint">{{ _('Lower numbers appear first.') }}</span>
      </div>
      <div class="kd-field">
        <label class="kd-field-label">{{ _('Published') }}</label>
        <label class="kd-toggle {% if form_data.published %}on{% endif %}" data-toggle-published>
          <span class="kd-toggle-track"></span>
          <span class="kd-toggle-status">
            {% if form_data.published %}{{ _('Live') }}{% else %}{{ _('Draft') }}{% endif %}
          </span>
        </label>
        <input type="checkbox" name="published" id="publishedCheck"
               value="1" hidden {% if form_data.published %}checked{% endif %}>
      </div>
      <div class="kd-field">
        <label class="kd-field-label">{{ _('Slug') }}</label>
        <div class="kd-input-prefix">
          <span class="prefix">/guides/</span>
          <input class="kd-input" type="text" name="slug" id="slugInput"
                 value="{{ form_data.slug }}"
                 placeholder="{{ _('auto-generated from EN title') }}">
        </div>
        <span class="kd-field-hint">{{ _('Lowercase letters, numbers, dashes only.') }}</span>
      </div>
    </div>

    {# EN editor card #}
    <div class="kd-editor-card" data-lang="en">
      <div class="kd-editor-head">
        <span class="kd-editor-lang"><span class="dot"></span> English</span>
        <span class="kd-editor-status" data-status>
          <span class="dot"></span><span data-status-label>—</span>
        </span>
      </div>
      <div class="kd-editor-fields">
        <div class="kd-field">
          <label class="kd-field-label">
            {{ _('Title') }} <span class="req">*</span>
          </label>
          <input class="kd-input" type="text" name="title_en"
                 value="{{ form_data.title_en }}" data-required>
        </div>
        <div class="kd-field">
          <label class="kd-field-label">{{ _('Summary') }}</label>
          <input class="kd-input" type="text" name="summary_en"
                 value="{{ form_data.summary_en }}" data-required>
          <span class="kd-field-hint">{{ _('One-line description shown on the guides index.') }}</span>
        </div>
      </div>
      <div class="kd-field" style="gap: 0;">
        <div style="padding: 16px 22px 0;">
          <label class="kd-field-label">{{ _('Body') }}</label>
        </div>
        <div id="editorEn" data-body style="min-height: 280px; background: #fff;"></div>
        <input type="hidden" name="body_en" id="bodyEnField" value="{{ form_data.body_en }}">
      </div>
    </div>

    {# ZH editor card #}
    <div class="kd-editor-card" data-lang="zh">
      <div class="kd-editor-head">
        <span class="kd-editor-lang"><span class="dot"></span> 中文</span>
        <span class="kd-editor-status" data-status>
          <span class="dot"></span><span data-status-label>—</span>
        </span>
      </div>
      <div class="kd-editor-fields">
        <div class="kd-field">
          <label class="kd-field-label">
            {{ _('Title') }} <span class="req">*</span>
          </label>
          <input class="kd-input" type="text" name="title_zh"
                 value="{{ form_data.title_zh }}" data-required>
        </div>
        <div class="kd-field">
          <label class="kd-field-label">{{ _('Summary') }}</label>
          <input class="kd-input" type="text" name="summary_zh"
                 value="{{ form_data.summary_zh }}" data-required>
          <span class="kd-field-hint">{{ _('One-line description shown on the guides index.') }}</span>
        </div>
      </div>
      <div class="kd-field" style="gap: 0;">
        <div style="padding: 16px 22px 0;">
          <label class="kd-field-label">{{ _('Body') }}</label>
        </div>
        <div id="editorZh" data-body style="min-height: 280px; background: #fff;"></div>
        <input type="hidden" name="body_zh" id="bodyZhField" value="{{ form_data.body_zh }}">
      </div>
    </div>

    <div class="kd-form-footer">
      <div class="left">
        {% if editing %}
          <button class="kd-btn kd-btn-danger" type="button" data-delete-guide>
            {{ _('Delete guide') }}
          </button>
        {% endif %}
      </div>
      <div class="right">
        <span class="kd-saved" data-dirty-state>{{ _('All changes saved') }}</span>
        <a href="{{ url_for('admin_guides_manage') }}" class="kd-btn kd-btn-ghost">
          {{ _('Cancel') }}
        </a>
        <button class="kd-btn" type="button" data-preview-guide>{{ _('Preview') }}</button>
        <button class="kd-btn kd-btn-primary" type="submit">{{ _('Save') }}</button>
      </div>
    </div>
  </form>

  {% if editing %}
    <form method="post" action="{{ url_for('admin_guide_delete', guide_id=guide_id) }}"
          id="deleteGuideForm" hidden></form>
  {% endif %}
</div>

<script src="{{ url_for('static', filename='vendor/quill/quill.min.js') }}"></script>
<script>
  /* Temporary inline init — moved to static/js/guides-editor.js in Task 12. */
  (function () {
    var toolbar = [
      [{ header: [1, 2, 3, false] }],
      ['bold', 'italic', 'underline', 'strike'],
      [{ list: 'ordered' }, { list: 'bullet' }],
      ['blockquote', 'code-block'],
      ['link', 'image'],
      ['clean'],
    ];

    function makeEditor(elId, hiddenId) {
      var hidden = document.getElementById(hiddenId);
      var editor = new Quill('#' + elId, { theme: 'snow', modules: { toolbar: toolbar } });
      if (hidden.value) editor.clipboard.dangerouslyPasteHTML(0, hidden.value);
      var uploadUrl = document.querySelector('.kd-panel').dataset.uploadImageUrl;
      editor.getModule('toolbar').addHandler('image', function () {
        var input = document.createElement('input');
        input.type = 'file';
        input.accept = 'image/png,image/jpeg,image/gif,image/webp';
        input.onchange = function () {
          var file = input.files && input.files[0];
          if (!file) return;
          var fd = new FormData(); fd.append('file', file);
          fetch(uploadUrl, { method: 'POST', body: fd })
            .then(function (r) { return r.json(); })
            .then(function (data) {
              if (data.url) {
                var range = editor.getSelection(true);
                editor.insertEmbed(range.index, 'image', data.url, 'user');
                editor.setSelection(range.index + 1);
              } else { alert(data.error || 'Upload failed'); }
            });
        };
        input.click();
      });
      editor.on('text-change', function () { hidden.value = editor.root.innerHTML; });
      return { editor: editor, hidden: hidden };
    }

    var pairEn = makeEditor('editorEn', 'bodyEnField');
    var pairZh = makeEditor('editorZh', 'bodyZhField');

    document.getElementById('guideForm').addEventListener('submit', function () {
      pairEn.hidden.value = pairEn.editor.root.innerHTML;
      pairZh.hidden.value = pairZh.editor.root.innerHTML;
    });

    var slugInput = document.getElementById('slugInput');
    var titleEn = document.querySelector('input[name="title_en"]');
    titleEn.addEventListener('blur', function () {
      if (!slugInput.value && titleEn.value) {
        slugInput.value = titleEn.value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
      }
    });
  })();
</script>
{% endblock %}
```

- [ ] **Step 4: Pass `user` to the publish template**

In `app.py`, the `admin_guide_publish` route's `render_template` call (around line 2400) currently passes `form_data, categories, editing, guide_id`. The dashboard shell needs `user`. Update the call to also pass `user=user`:

```python
        return render_template(
            "guide_publish.html",
            form_data=form_data,
            categories=_load_guide_categories(),
            editing=editing,
            guide_id=guide_id,
            user=user,
        )
```

- [ ] **Step 5: Run tests**

Run: `python -m unittest tests/test_guide_template_contract.py -v`
Expected: PASS (new tests). The two skipped tests stay skipped.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass (skips OK).

- [ ] **Step 6: Commit**

```bash
git add templates/guide_publish.html app.py tests/test_guide_template_contract.py
git commit -m "feat(guides): redesign publish form inside dashboard shell"
```

---

## Task 12: Create `static/js/guides-editor.js` with Quill init + image upload

**Files:**
- Create: `static/js/guides-editor.js`
- Modify: `templates/guide_publish.html` (replace inline `<script>` block with external src)

Move the temporary inline init from Task 11 into the external file, unchanged in behavior. This is a pure refactor; the form should still work identically.

- [ ] **Step 1: Write the failing DOM-contract test**

Create `tests/test_guide_dom_contract.py`:

```python
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GuideDomContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        js_path = ROOT / "static" / "js" / "guides-editor.js"
        cls.js = js_path.read_text(encoding="utf-8") if js_path.exists() else ""
        cls.publish_tpl = (ROOT / "templates" / "guide_publish.html").read_text(encoding="utf-8")

    def test_js_module_exists(self):
        self.assertTrue(self.js, "static/js/guides-editor.js must exist")

    def test_js_referenced_element_ids_exist_in_template(self):
        # Collect every getElementById('X') used in the JS
        ids = set(re.findall(r"getElementById\(['\"]([\w-]+)['\"]\)", self.js))
        # Plus QuerySelector('#X') uses
        ids |= set(re.findall(r"querySelector\(['\"]#([\w-]+)['\"]\)", self.js))
        for el_id in ids:
            self.assertIn(f'id="{el_id}"', self.publish_tpl,
                          f"JS references #{el_id} but template has no id={el_id}")

    def test_js_referenced_data_attrs_exist_in_template(self):
        # querySelector('[data-foo]'), querySelectorAll, dataset.foo
        attrs = set(re.findall(r"\[data-([\w-]+)", self.js))
        # Skip ones we know come from `.dataset.fooBar` (camelCase access)
        for attr in attrs:
            self.assertIn(f"data-{attr}", self.publish_tpl,
                          f"JS references [data-{attr}] but template has no such attribute")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_dom_contract.py -v`
Expected: FAIL on `test_js_module_exists` (file absent).

- [ ] **Step 3: Create `static/js/guides-editor.js`**

Create the file with the exact same logic that's currently inline in `guide_publish.html`:

```js
/* Keydion guides editor wiring.
   Owns: Quill init for EN/ZH editor cards, image upload, slug auto-suggest,
   published toggle, status pill, dirty tracker, Preview, Delete, callout/figure blots.
   Tasks add features incrementally — this baseline is feature-equivalent to
   the inline init that lived in guide_publish.html before Task 12.        */
(function () {
  var panel = document.querySelector('.kd-panel');
  if (!panel) return; /* not on the publish page */

  var uploadUrl = panel.dataset.uploadImageUrl;
  var toolbar = [
    [{ header: [1, 2, 3, false] }],
    ['bold', 'italic', 'underline', 'strike'],
    [{ list: 'ordered' }, { list: 'bullet' }],
    ['blockquote', 'code-block'],
    ['link', 'image'],
    ['clean'],
  ];

  function makeEditor(elId, hiddenId) {
    var hidden = document.getElementById(hiddenId);
    var editor = new Quill('#' + elId, { theme: 'snow', modules: { toolbar: toolbar } });
    if (hidden.value) editor.clipboard.dangerouslyPasteHTML(0, hidden.value);
    editor.getModule('toolbar').addHandler('image', function () {
      var input = document.createElement('input');
      input.type = 'file';
      input.accept = 'image/png,image/jpeg,image/gif,image/webp';
      input.onchange = function () {
        var file = input.files && input.files[0];
        if (!file) return;
        var fd = new FormData(); fd.append('file', file);
        fetch(uploadUrl, { method: 'POST', body: fd })
          .then(function (r) { return r.json(); })
          .then(function (data) {
            if (data.url) {
              var range = editor.getSelection(true);
              editor.insertEmbed(range.index, 'image', data.url, 'user');
              editor.setSelection(range.index + 1);
            } else { alert(data.error || 'Upload failed'); }
          })
          .catch(function () { alert('Image upload failed.'); });
      };
      input.click();
    });
    editor.on('text-change', function () { hidden.value = editor.root.innerHTML; });
    return { editor: editor, hidden: hidden };
  }

  var pairEn = makeEditor('editorEn', 'bodyEnField');
  var pairZh = makeEditor('editorZh', 'bodyZhField');

  document.getElementById('guideForm').addEventListener('submit', function () {
    pairEn.hidden.value = pairEn.editor.root.innerHTML;
    pairZh.hidden.value = pairZh.editor.root.innerHTML;
  });

  /* Slug auto-suggest from EN title (preserved verbatim from old inline init). */
  var slugInput = document.getElementById('slugInput');
  var titleEn = document.querySelector('input[name="title_en"]');
  titleEn.addEventListener('blur', function () {
    if (!slugInput.value && titleEn.value) {
      slugInput.value = titleEn.value.toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
    }
  });

  /* Hooks for the next tasks — exported on a namespace so each task can
     reach in without forcing another rewrite. */
  window.__guidesEditor = {
    pairs: { en: pairEn, zh: pairZh },
    uploadUrl: uploadUrl,
    panel: panel,
  };
})();
```

- [ ] **Step 4: Replace inline script in template**

In `templates/guide_publish.html`, delete the entire `<script>...</script>` block at the bottom of `{% block panel %}` (the temporary inline init). Replace it with:

```html
<script src="{{ url_for('static', filename='vendor/quill/quill.min.js') }}"></script>
<script src="{{ url_for('static', filename='js/guides-editor.js') }}"></script>
```

- [ ] **Step 5: Un-skip the two previously skipped tests**

In `tests/test_guide_template_contract.py`, remove the `@unittest.skip(...)` decorators added in Task 10 from `test_publish_template_loads_quill` and `test_publish_template_wires_image_upload`.

- [ ] **Step 6: Run tests**

Run: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass — the un-skipped tests now pass because the template references `js/guides-editor.js` and the new file contains `admin_guides_upload_image`... wait, it doesn't. Let me recheck the test:

The test `test_publish_template_wires_image_upload` asserts `admin_guides_upload_image` is in `js + publish_tpl`. The template has `data-upload-image-url="{{ url_for('admin_guides_upload_image') }}"` — that satisfies the assertion (the endpoint name appears in the template source).

OK, tests should pass.

- [ ] **Step 7: Commit**

```bash
git add static/js/guides-editor.js templates/guide_publish.html tests/test_guide_template_contract.py tests/test_guide_dom_contract.py
git commit -m "refactor(guides): move publish-form JS into static/js/guides-editor.js"
```

---

## Task 13: Add `CalloutBlot` to `guides-editor.js`

**Files:**
- Modify: `static/js/guides-editor.js`

The blot must register before `new Quill(...)` runs, otherwise existing saved callout HTML won't materialize on load. Restructure the IIFE so blot registration happens first.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_dom_contract.py`:

```python
    def test_js_registers_callout_blot(self):
        self.assertIn("CalloutBlot", self.js)
        self.assertIn("blotName = 'callout'", self.js.replace('"', "'"))
        self.assertIn("kd-callout", self.js)
        # Toolbar handler for callout
        self.assertIn("'callout'", self.js)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_dom_contract.py::GuideDomContractTest::test_js_registers_callout_blot -v`
Expected: FAIL.

- [ ] **Step 3: Add the blot and toolbar handler**

In `static/js/guides-editor.js`, near the top of the IIFE (before the `toolbar` array), insert:

```js
  /* ─── Callout blot ─────────────────────────────────────────────── */
  var BlockEmbed = Quill.import('blots/block/embed');

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function CalloutBlot() { BlockEmbed.apply(this, arguments); }
  CalloutBlot.prototype = Object.create(BlockEmbed.prototype);
  CalloutBlot.prototype.constructor = CalloutBlot;
  CalloutBlot.blotName = 'callout';
  CalloutBlot.tagName = 'div';
  CalloutBlot.className = 'kd-callout';
  CalloutBlot.create = function (value) {
    var node = BlockEmbed.create.call(this);
    node.setAttribute('class', 'kd-callout');
    var label = document.createElement('div');
    label.className = 'kd-callout-label';
    label.setAttribute('contenteditable', 'true');
    label.textContent = (value && value.label) || 'Note';
    var body = document.createElement('div');
    body.className = 'kd-callout-body';
    body.setAttribute('contenteditable', 'true');
    body.innerHTML = '<p>' + escapeHtml((value && value.body) || 'Type your callout here.') + '</p>';
    node.appendChild(label);
    node.appendChild(body);
    return node;
  };
  CalloutBlot.value = function (node) {
    var lbl = node.querySelector('.kd-callout-label');
    var bdy = node.querySelector('.kd-callout-body');
    return {
      label: lbl ? lbl.textContent.trim() : '',
      body: bdy ? bdy.textContent.trim() : '',
    };
  };
  Quill.register(CalloutBlot);
```

Update the `toolbar` array to include the callout button:

```js
  var toolbar = [
    [{ header: [1, 2, 3, false] }],
    ['bold', 'italic', 'underline', 'strike'],
    [{ list: 'ordered' }, { list: 'bullet' }],
    ['blockquote', 'code-block'],
    ['link', 'image'],
    ['callout'],
    ['clean'],
  ];
```

Inside `makeEditor()`, after the image handler is added, add a callout handler:

```js
    editor.getModule('toolbar').addHandler('callout', function () {
      var range = editor.getSelection(true);
      editor.insertEmbed(range.index, 'callout',
        { label: 'Note', body: 'Type your callout here.' }, 'user');
      editor.setSelection(range.index + 1);
    });
```

After both pairs are constructed, decorate the callout buttons with an icon. Append:

```js
  document.querySelectorAll('button.ql-callout').forEach(function (btn) {
    btn.setAttribute('title', 'Insert callout');
    btn.innerHTML = '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="3" width="12" height="10"/><line x1="2" y1="6" x2="14" y2="6"/><circle cx="5" cy="9" r="0.6" fill="currentColor"/></svg>';
  });
```

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_dom_contract.py -v`
Expected: PASS.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add static/js/guides-editor.js tests/test_guide_dom_contract.py
git commit -m "feat(guides): add Quill CalloutBlot and toolbar button"
```

---

## Task 14: Add `FigureBlot` to `guides-editor.js`

**Files:**
- Modify: `static/js/guides-editor.js`

Figure inserts an `<img class="kd-fig-img">` (bleach allows class on img after Task 7), with a separate caption block.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_dom_contract.py`:

```python
    def test_js_registers_figure_blot(self):
        self.assertIn("FigureBlot", self.js)
        self.assertIn("blotName = 'figure'", self.js.replace('"', "'"))
        self.assertIn("kd-fig", self.js)
        self.assertIn("kd-fig-img", self.js)
        self.assertIn("kd-fig-caption", self.js)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_dom_contract.py::GuideDomContractTest::test_js_registers_figure_blot -v`
Expected: FAIL.

- [ ] **Step 3: Add the blot**

In `static/js/guides-editor.js`, after the `CalloutBlot` definition, add:

```js
  /* ─── Figure blot ──────────────────────────────────────────────── */
  function isSafeImageSrc(src) {
    if (!src) return false;
    /* Same-origin /static/uploads/guides/... — relative URL */
    if (src.indexOf('/static/uploads/guides/') === 0) return true;
    /* Otherwise must be https:// */
    try {
      var u = new URL(src, window.location.origin);
      return u.protocol === 'https:';
    } catch (e) { return false; }
  }

  function FigureBlot() { BlockEmbed.apply(this, arguments); }
  FigureBlot.prototype = Object.create(BlockEmbed.prototype);
  FigureBlot.prototype.constructor = FigureBlot;
  FigureBlot.blotName = 'figure';
  FigureBlot.tagName = 'div';
  FigureBlot.className = 'kd-fig';
  FigureBlot.create = function (value) {
    var node = BlockEmbed.create.call(this);
    node.setAttribute('class', 'kd-fig');
    var src = (value && isSafeImageSrc(value.src)) ? value.src : '';
    var num = (value && value.num) || 'Fig.';
    var cap = (value && value.caption) || '';
    if (src) {
      var img = document.createElement('img');
      img.className = 'kd-fig-img';
      img.src = src;
      img.alt = cap;
      node.appendChild(img);
    } else {
      var placeholder = document.createElement('div');
      placeholder.className = 'kd-fig-img';
      placeholder.style.cssText = 'height:200px;display:flex;align-items:center;justify-content:center;color:var(--muted-2);font-family:var(--mono);font-size:11px;letter-spacing:.08em;text-transform:uppercase;background:var(--cream-2);';
      placeholder.textContent = 'No image';
      node.appendChild(placeholder);
    }
    var caption = document.createElement('div');
    caption.className = 'kd-fig-caption';
    caption.setAttribute('contenteditable', 'true');
    var numSpan = document.createElement('span');
    numSpan.className = 'num';
    numSpan.textContent = num;
    var capSpan = document.createElement('span');
    capSpan.className = 'caption-text';
    capSpan.textContent = cap;
    caption.appendChild(numSpan);
    caption.appendChild(document.createTextNode(' '));
    caption.appendChild(capSpan);
    node.appendChild(caption);
    return node;
  };
  FigureBlot.value = function (node) {
    var img = node.querySelector('img.kd-fig-img');
    var numSpan = node.querySelector('.kd-fig-caption .num');
    var capSpan = node.querySelector('.kd-fig-caption .caption-text');
    return {
      src: img ? img.getAttribute('src') : '',
      num: numSpan ? numSpan.textContent.trim() : 'Fig.',
      caption: capSpan ? capSpan.textContent.trim() : '',
    };
  };
  Quill.register(FigureBlot);
```

Update `toolbar` array to include figure:

```js
    ['callout', 'figure'],
```

Inside `makeEditor()`, add the figure handler after the callout handler:

```js
    editor.getModule('toolbar').addHandler('figure', function () {
      var input = document.createElement('input');
      input.type = 'file';
      input.accept = 'image/png,image/jpeg,image/gif,image/webp';
      input.onchange = function () {
        var file = input.files && input.files[0];
        if (!file) return;
        var fd = new FormData(); fd.append('file', file);
        fetch(uploadUrl, { method: 'POST', body: fd })
          .then(function (r) { return r.json(); })
          .then(function (data) {
            if (!data.url) { alert(data.error || 'Upload failed'); return; }
            var caption = window.prompt('Figure caption (optional):', '') || '';
            var num = window.prompt('Figure label:', 'Fig. 01') || 'Fig.';
            var range = editor.getSelection(true);
            editor.insertEmbed(range.index, 'figure',
              { src: data.url, num: num, caption: caption }, 'user');
            editor.setSelection(range.index + 1);
          });
      };
      input.click();
    });
```

Decorate the figure toolbar button after editor construction:

```js
  document.querySelectorAll('button.ql-figure').forEach(function (btn) {
    btn.setAttribute('title', 'Insert figure');
    btn.innerHTML = '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="3" width="12" height="10"/><circle cx="6" cy="7" r="1.2"/><path d="M2 11 L6 8 L9 10 L14 6"/></svg>';
  });
```

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_dom_contract.py -v`
Expected: PASS.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add static/js/guides-editor.js tests/test_guide_dom_contract.py
git commit -m "feat(guides): add Quill FigureBlot with image+caption insertion"
```

---

## Task 15: Add per-language status pill

**Files:**
- Modify: `static/js/guides-editor.js`

For each `.kd-editor-card`, watch title/summary inputs and the Quill `text-change` event. Compute status: "All fields filled" (green dot) or the first missing piece (amber dot).

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_dom_contract.py`:

```python
    def test_js_wires_status_pill(self):
        self.assertIn("data-status-label", self.js)
        # Status states surface as text in the JS for translation/observability
        for phrase in ("All fields filled", "Title missing",
                       "Summary missing", "Body missing"):
            self.assertIn(phrase, self.js)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_dom_contract.py::GuideDomContractTest::test_js_wires_status_pill -v`
Expected: FAIL.

- [ ] **Step 3: Add the status pill logic**

In `static/js/guides-editor.js`, after both `pairEn`/`pairZh` are constructed but before the export at the bottom, add:

```js
  /* ─── Per-language status pill ───────────────────────────────────── */
  function updateStatus(lang, pair) {
    var card = document.querySelector('.kd-editor-card[data-lang="' + lang + '"]');
    if (!card) return;
    var dot = card.querySelector('[data-status] .dot');
    var label = card.querySelector('[data-status-label]');
    var titleInput = card.querySelector('input[name="title_' + lang + '"]');
    var summaryInput = card.querySelector('input[name="summary_' + lang + '"]');
    var bodyText = pair.editor.getText().trim();
    var msg, ok;
    if (!titleInput.value.trim()) { msg = 'Title missing'; ok = false; }
    else if (!summaryInput.value.trim()) { msg = 'Summary missing'; ok = false; }
    else if (!bodyText) { msg = 'Body missing'; ok = false; }
    else { msg = 'All fields filled'; ok = true; }
    label.textContent = msg;
    dot.style.background = ok ? '#2a9d5f' : '#c98a1a';
  }

  ['en', 'zh'].forEach(function (lang) {
    var pair = (lang === 'en') ? pairEn : pairZh;
    var card = document.querySelector('.kd-editor-card[data-lang="' + lang + '"]');
    if (!card) return;
    var inputs = card.querySelectorAll('input[data-required]');
    inputs.forEach(function (inp) {
      inp.addEventListener('input', function () { updateStatus(lang, pair); });
    });
    pair.editor.on('text-change', function () { updateStatus(lang, pair); });
    updateStatus(lang, pair); /* initial */
  });
```

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_dom_contract.py -v`
Expected: PASS.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add static/js/guides-editor.js tests/test_guide_dom_contract.py
git commit -m "feat(guides): per-language status pill on publish form"
```

---

## Task 16: Add dirty tracker + sticky-footer state

**Files:**
- Modify: `static/js/guides-editor.js`

Snapshot the form on load; recompute on input/change/text-change; flip `[data-dirty-state]` between "All changes saved" and "● Unsaved changes". Install `beforeunload` when dirty.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_dom_contract.py`:

```python
    def test_js_wires_dirty_tracker(self):
        self.assertIn("data-dirty-state", self.js)
        self.assertIn("beforeunload", self.js)
        for phrase in ("All changes saved", "Unsaved changes"):
            self.assertIn(phrase, self.js)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_dom_contract.py::GuideDomContractTest::test_js_wires_dirty_tracker -v`
Expected: FAIL.

- [ ] **Step 3: Add the dirty tracker**

In `static/js/guides-editor.js`, after the status pill block, add:

```js
  /* ─── Dirty tracker + beforeunload ───────────────────────────────── */
  var form = document.getElementById('guideForm');
  var dirtyEl = document.querySelector('[data-dirty-state]');

  function snapshot() {
    var fd = new FormData(form);
    var parts = [];
    fd.forEach(function (v, k) { parts.push(k + '=' + v); });
    parts.push('__body_en=' + pairEn.editor.root.innerHTML);
    parts.push('__body_zh=' + pairZh.editor.root.innerHTML);
    return parts.join('|');
  }

  var initial = snapshot();
  var isDirty = false;

  function beforeUnloadHandler(e) {
    e.preventDefault();
    e.returnValue = '';
    return '';
  }

  function checkDirty() {
    var now = snapshot();
    var nextDirty = (now !== initial);
    if (nextDirty === isDirty) return;
    isDirty = nextDirty;
    if (isDirty) {
      dirtyEl.textContent = '● Unsaved changes';
      dirtyEl.style.color = 'var(--accent)';
      window.addEventListener('beforeunload', beforeUnloadHandler);
    } else {
      dirtyEl.textContent = 'All changes saved';
      dirtyEl.style.color = '';
      window.removeEventListener('beforeunload', beforeUnloadHandler);
    }
  }

  form.addEventListener('input', checkDirty);
  form.addEventListener('change', checkDirty);
  pairEn.editor.on('text-change', checkDirty);
  pairZh.editor.on('text-change', checkDirty);

  form.addEventListener('submit', function () {
    window.removeEventListener('beforeunload', beforeUnloadHandler);
  });
```

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_dom_contract.py -v`
Expected: PASS.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add static/js/guides-editor.js tests/test_guide_dom_contract.py
git commit -m "feat(guides): sticky-footer dirty tracker with beforeunload warning"
```

---

## Task 17: Wire published toggle, Preview button, Delete button

**Files:**
- Modify: `static/js/guides-editor.js`

Three small wiring jobs in one task — each is a few lines and they share no state.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_guide_dom_contract.py`:

```python
    def test_js_wires_toggle_preview_delete(self):
        self.assertIn("data-toggle-published", self.js)
        self.assertIn("data-preview-guide", self.js)
        self.assertIn("data-delete-guide", self.js)
        self.assertIn("publishedCheck", self.js)
        self.assertIn("deleteGuideForm", self.js)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests/test_guide_dom_contract.py::GuideDomContractTest::test_js_wires_toggle_preview_delete -v`
Expected: FAIL.

- [ ] **Step 3: Add the wiring**

In `static/js/guides-editor.js`, after the dirty-tracker block, add:

```js
  /* ─── Published toggle ───────────────────────────────────────────── */
  var toggle = document.querySelector('[data-toggle-published]');
  var publishedCheck = document.getElementById('publishedCheck');
  if (toggle && publishedCheck) {
    toggle.addEventListener('click', function (e) {
      e.preventDefault();
      var nowOn = !publishedCheck.checked;
      publishedCheck.checked = nowOn;
      toggle.classList.toggle('on', nowOn);
      var statusEl = toggle.querySelector('.kd-toggle-status');
      if (statusEl) statusEl.textContent = nowOn ? 'Live' : 'Draft';
      checkDirty();
    });
  }

  /* ─── Preview button ─────────────────────────────────────────────── */
  var previewBtn = document.querySelector('[data-preview-guide]');
  if (previewBtn) {
    previewBtn.addEventListener('click', function () {
      /* Sync editors into hidden fields first */
      pairEn.hidden.value = pairEn.editor.root.innerHTML;
      pairZh.hidden.value = pairZh.editor.root.innerHTML;
      var transient = document.createElement('form');
      transient.method = 'POST';
      transient.action = panel.dataset.previewUrl;
      transient.target = '_blank';
      var fd = new FormData(form);
      fd.forEach(function (v, k) {
        var input = document.createElement('input');
        input.type = 'hidden';
        input.name = k;
        input.value = v;
        transient.appendChild(input);
      });
      document.body.appendChild(transient);
      transient.submit();
      document.body.removeChild(transient);
    });
  }

  /* ─── Delete button ──────────────────────────────────────────────── */
  var deleteBtn = document.querySelector('[data-delete-guide]');
  var deleteForm = document.getElementById('deleteGuideForm');
  if (deleteBtn && deleteForm) {
    deleteBtn.addEventListener('click', function () {
      if (window.confirm('Delete this guide? This cannot be undone.')) {
        window.removeEventListener('beforeunload', beforeUnloadHandler);
        deleteForm.submit();
      }
    });
  }
```

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests/test_guide_dom_contract.py -v`
Expected: PASS.

Full suite: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add static/js/guides-editor.js tests/test_guide_dom_contract.py
git commit -m "feat(guides): wire published toggle, Preview, and Delete buttons"
```

---

## Task 18: Delete `templates/Keydion-Guides-revamp.html`

**Files:**
- Delete: `templates/Keydion-Guides-revamp.html`

The reference artifact has done its job. Removing it keeps the templates directory clean and avoids any chance Flask discovers it via Jinja2's autoescape probing.

- [ ] **Step 1: Delete the file**

Run:

```bash
git rm templates/Keydion-Guides-revamp.html
```

- [ ] **Step 2: Run full suite to confirm nothing referenced it**

Run: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git commit -m "chore(guides): remove reference design artifact"
```

---

## Task 19: Add translation entries and compile

**Files:**
- Modify: `translations/en/LC_MESSAGES/messages.po`
- Modify: `translations/zh/LC_MESSAGES/messages.po`
- Run: `python tools/compile_translations.py`

The new templates introduce several new `_()` strings that need translations: "Step-by-step", "help.", "How to upload, publish, and curate work on Keydion.", "ARTICLES", "Articles", "No guides published yet. Check back soon.", "Last updated", "Previous", "Next", "No previous guide", "No next guide", "Preview · not yet published", "Manage guides", "Edit", "guide", "New", "Publish dual-language guides with rich text.", "Sort order", "Lower numbers appear first.", "Published", "Live", "Draft", "Slug", "Lowercase letters, numbers, dashes only.", "auto-generated from EN title", "— Select —", "Title", "Summary", "One-line description shown on the guides index.", "Body", "Delete guide", "All changes saved", "Cancel", "Preview", "Save".

(Some of these may already exist from previous templates — Babel's `pybabel extract` won't duplicate. If a fully automated workflow exists for extract+compile, prefer it.)

- [ ] **Step 1: Extract message catalog**

If the repo has a `pybabel extract` invocation (check `tools/` for an `extract_translations.py` or similar), run it. Otherwise:

```bash
pybabel extract -F babel.cfg -o messages.pot --omit-header app.py templates/
```

Then for each locale:

```bash
pybabel update -i messages.pot -d translations -l en --no-fuzzy-matching
pybabel update -i messages.pot -d translations -l zh --no-fuzzy-matching
```

(If `babel.cfg` doesn't exist, this step is skipped and translations are added manually in Step 2.)

- [ ] **Step 2: Add Chinese translations for new strings**

Open `translations/zh/LC_MESSAGES/messages.po`. For every new `msgid` whose `msgstr ""` is empty, fill in:

```po
msgid "Step-by-step"
msgstr "循序渐进的"

msgid "help."
msgstr "帮助。"

msgid "How to upload, publish, and curate work on Keydion."
msgstr "如何在 Keydion 上传、发布与策展作品。"

msgid "ARTICLES"
msgstr "篇文章"

msgid "Articles"
msgstr "篇"

msgid "No guides published yet. Check back soon."
msgstr "暂无指南,稍后再来。"

msgid "Last updated"
msgstr "最后更新"

msgid "Previous"
msgstr "上一篇"

msgid "Next"
msgstr "下一篇"

msgid "No previous guide"
msgstr "没有上一篇"

msgid "No next guide"
msgstr "没有下一篇"

msgid "Preview · not yet published"
msgstr "预览 · 尚未发布"

msgid "Manage guides"
msgstr "管理指南"

msgid "Edit"
msgstr "编辑"

msgid "guide"
msgstr "指南"

msgid "New"
msgstr "新建"

msgid "Publish dual-language guides with rich text."
msgstr "发布双语指南,支持富文本格式。"

msgid "Sort order"
msgstr "排序"

msgid "Lower numbers appear first."
msgstr "数字越小越靠前。"

msgid "Published"
msgstr "已发布"

msgid "Live"
msgstr "已上线"

msgid "Draft"
msgstr "草稿"

msgid "Slug"
msgstr "短链接"

msgid "Lowercase letters, numbers, dashes only."
msgstr "仅限小写字母、数字与连字符。"

msgid "auto-generated from EN title"
msgstr "根据英文标题自动生成"

msgid "— Select —"
msgstr "— 选择 —"

msgid "Title"
msgstr "标题"

msgid "Summary"
msgstr "摘要"

msgid "One-line description shown on the guides index."
msgstr "在指南列表页显示的一句话描述。"

msgid "Body"
msgstr "正文"

msgid "Delete guide"
msgstr "删除指南"

msgid "All changes saved"
msgstr "已保存所有更改"

msgid "Cancel"
msgstr "取消"

msgid "Preview"
msgstr "预览"

msgid "Save"
msgstr "保存"
```

(Some of these may already have entries — leave existing translations alone.)

- [ ] **Step 3: Add English translations**

In `translations/en/LC_MESSAGES/messages.po`, fill `msgstr` for new entries with the same text as `msgid` (English is the source locale).

- [ ] **Step 4: Compile**

Run: `python tools/compile_translations.py`
Expected: outputs that en and zh `.mo` files were written.

- [ ] **Step 5: Run full suite**

Run: `python -m unittest discover -s tests -p "test_*.py"`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add translations/en/LC_MESSAGES/messages.po translations/zh/LC_MESSAGES/messages.po translations/*/LC_MESSAGES/messages.mo
git commit -m "i18n(guides): translate new strings for redesigned templates"
```

---

## Task 20: Manual smoke

**Files:** none (verification only)

Run the app locally and walk the checklist from the spec. Fix anything that surfaces.

- [ ] **Step 1: Start the dev server**

```bash
./start_local.sh
```

Open `http://localhost:5000` (or whatever port the script prints).

- [ ] **Step 2: Run the smoke checklist**

Verify each item, fixing any failures inline before moving on:

- [ ] `/guides` renders with the seeded guides; sticky category labels stay in place when scrolling within a category.
- [ ] `/guides/<slug>` renders an existing published guide with prev/next, headings, ordered list, blockquote all styled per mock.
- [ ] EN ↔ ZH language switch on both pages flips title/summary/body to the right locale (use the existing `?locale=zh` switcher in the header).
- [ ] Sign in as a Curator (role 3) and visit `/dashboard/admin/guides`, then click Edit on an existing guide.
- [ ] Status pill on the EN card shows "All fields filled" (green); blank the summary input and it flips to "Summary missing" (amber).
- [ ] Sticky footer at the bottom of the form shows "All changes saved". Type into the EN title — flips to "● Unsaved changes" in accent color.
- [ ] Navigate away — browser warns about unsaved changes.
- [ ] Hit Save — the form submits, redirect lands on `/dashboard/admin/guides`.
- [ ] Edit again. In the EN body, click the callout button (rectangle icon). A callout block appears editable with "Note" label and "Type your callout here." body. Edit both, save.
- [ ] Reload the edit page — callout renders with your edited text, still editable.
- [ ] Click the figure button. Upload a PNG. Caption prompt → fill it. Figure label prompt → accept default. Figure appears with image, "Fig. 01", caption.
- [ ] Save. Reload. Figure persists with image, label, caption.
- [ ] Click Preview. New tab opens showing the article as currently drafted, with a "Preview · not yet published" banner across the top. The drafted (unsaved) edits show in the preview if you made any.
- [ ] Open an existing guide that has no callout/figure markup. It still renders cleanly on both the public page and the edit page.
- [ ] Click Delete guide. Confirmation dialog → confirm. Redirect to manage page, guide gone.

- [ ] **Step 3: If everything passes, commit the manual-smoke completion**

(If the checklist exposed fixes, those should have been committed as small follow-ups during Step 2. If the smoke passes clean, no commit needed for this task.)

```bash
# Only if you made fixes during the smoke walk:
git status
# Stage any forgotten files, then:
git commit -m "fix(guides): <description of smoke-pass fix>"
```

---

## Done

At this point the guides revamp is feature-complete:
- Three templates redesigned (`guides.html`, `guide_article.html`, `guide_publish.html`).
- One new file each for shared CSS and JS (`static/css/guides.css`, `static/js/guides-editor.js`).
- One new server route (`admin_guide_preview`) + one new helper (`_read_guide_form`).
- `guide_article()` route augmented with prev/next.
- `guides()` route augmented with `total`.
- Bleach allowlist extended for `<img class>` on figures.
- Quill custom blots for callout and figure with sanitization-safe markup.
- Per-language status pill, sticky-footer dirty tracker with `beforeunload`, Preview button, Delete confirm, custom Published toggle.
- Reference design artifact removed.
- Translations updated and compiled.
- Manual smoke run.

Push the branch and open a PR. Use the manual smoke checklist from Task 20 as the PR description's "Test plan" section.
