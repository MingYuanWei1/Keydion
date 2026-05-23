# Dashboard URL Nesting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Nest every curator-side route under `/dashboard/*` and render the dashboard shell on direct visits so refreshing, bookmarking, or post-redirect navigation never escapes the dashboard.

**Architecture:** A new `templates/_dashboard_shell.html` becomes the canonical shell (sidebar + main panel) extending `base.html` with a `{% block panel %}` slot. Each sidebar-destination template extends `_bare.html` when the request carries `X-Partial-Content: 1` and `_dashboard_shell.html` otherwise — so direct visits to `/dashboard/upload` etc. render with the sidebar wrapped around them. Route decorators move from `/upload`, `/manage`, `/news/publish`, … to `/dashboard/upload`, `/dashboard/manage`, `/dashboard/news/publish`, … with the original endpoint names kept (so every existing `url_for(...)` automatically resolves to the new path). Old paths get GET-only 301-redirect aliases so external bookmarks survive. `dashboard.js`'s "redirected off-route → full nav" fallback is refined to only kick in when the redirect leaves `/dashboard/*`.

**Tech Stack:** Flask 3 + Jinja2, Flask-Babel for i18n, vanilla JS for partial loading (`static/js/dashboard.js`), `unittest` + `ast` contract tests.

---

## File Structure

**Created:**

- `templates/_dashboard_shell.html` — full dashboard shell (sidebar + main) with `{% block panel %}` slot; previously inlined in `dashboard.html`.
- `tests/test_dashboard_url_nesting_contract.py` — asserts every moved endpoint's `app.url_map` rule starts with `/dashboard/`, asserts legacy GET redirects exist, asserts `_dashboard_shell.html` renders correctly.

**Modified:**

- `templates/dashboard.html` — slims down to `{% extends "_dashboard_shell.html" %}` + override `{% block panel %}` with the overview include. Sidebar markup moves out into the shell.
- `templates/_bare.html` — `{% block content %}` renamed to `{% block panel %}` to match the new convention.
- 11 sidebar-destination templates (`upload.html`, `my_submissions.html`, `review_list.html`, `review_paper.html`, `delete.html`, `paper_manage.html`, `news_publish.html`, `news_manage.html`, `admin_users.html`, `guide_manage.html`, `change_password.html`) — first-line extends becomes `{% extends "_bare.html" if partial else "_dashboard_shell.html" %}`, body block renamed to `{% block panel %}`.
- `app.py` — route decorator paths for ~35 endpoints flipped from `/foo` → `/dashboard/foo`. Endpoint names unchanged. Legacy-path GET redirects added for the ~15 GET-accessible routes.
- `static/js/dashboard.js` — `submit` handler's "off-route redirect → full nav" check refined to `!redirPath.startsWith('/dashboard') && origPath.startsWith('/dashboard')`; pushState updated to follow the resolved redirect URL.
- `tests/test_partial_request_contract.py` — `PartialAwareTemplatesContractTest` expected first-line updated.
- `tests/test_dashboard_revamp_contract.py` — `DashboardAssetsContractTest` asserts new leave-dashboard detection regex.

---

## Task 1: Extract `_dashboard_shell.html` and rewrite `dashboard.html`

**Files:**
- Create: `templates/_dashboard_shell.html`
- Modify: `templates/dashboard.html`
- Test: extends-existing test in `tests/test_dashboard_revamp_contract.py` (`DashboardShellTemplateContractTest`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dashboard_revamp_contract.py` inside `DashboardShellTemplateContractTest`:

```python
    def test_shell_template_file_exists(self):
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[1]
        self.assertTrue((ROOT / "templates" / "_dashboard_shell.html").exists())

    def test_dashboard_extends_shell(self):
        # dashboard.html is now a thin wrapper that fills the shell's panel slot.
        self.assertIn('{% extends "_dashboard_shell.html" %}', self.src)
        self.assertIn("{% block panel %}", self.src)
        self.assertIn('include "_dashboard/overview.html"', self.src)

    def test_shell_exposes_panel_block(self):
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[1]
        shell = (ROOT / "templates" / "_dashboard_shell.html").read_text(encoding="utf-8")
        # Shell must extend base and expose a {% block panel %} slot inside the main panel.
        self.assertIn('{% extends "base.html" %}', shell)
        self.assertRegex(shell, r'id="dashboardMain".*?\{%\s*block\s+panel\s*%\}', flags=__import__('re').DOTALL)
        # Sidebar must still be in the shell (moved out of dashboard.html).
        self.assertIn("dashboard-sidebar", shell)
        self.assertIn("data-cycle-sidebar", shell)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_dashboard_revamp_contract.DashboardShellTemplateContractTest -v`
Expected: FAIL on `test_shell_template_file_exists` (file does not exist yet).

- [ ] **Step 3: Create `templates/_dashboard_shell.html`**

Create the file with this content (the sidebar block is copied verbatim from `templates/dashboard.html` lines 14-180, with the `{% include "_dashboard/overview.html" %}` line replaced by `{% block panel %}{% endblock %}`):

```jinja
{# =============================================================
   templates/_dashboard_shell.html — canonical dashboard shell.

   Extends base.html and exposes a single {% block panel %} slot
   where the active page's content goes. Used by dashboard.html
   (which fills the slot with the overview partial) and by every
   sidebar-destination page on direct visits.

   Required context:
     user                          — current user object
     role_label(role)              — helper already in scope
     dashboard_stats               — dict (any keys may be missing)
   ============================================================= #}
{% extends "base.html" %}
{% block title %}{{ _('User Hub · Keydion') }}{% endblock %}

{% block content %}
<style>
  main.py-4 > .container { max-width: none !important; padding: 0 !important; }
  main.py-4 { padding: 0 !important; }
  body { padding-bottom: 0 !important; }
  footer { display: none !important; }
</style>

<link rel="stylesheet" href="{{ url_for('static', filename='css/dashboard.css') }}">

{% set role = user.role|int %}
{% set display_name = user.display_name or (user.first_name ~ ' ' ~ user.last_name)|trim or user.email or user.username %}

<div class="dashboard-shell" data-sidebar-state="full">
  <aside class="dashboard-sidebar" aria-label="{{ _('User hub navigation') }}">
    <div class="sidebar-head">
      <div class="sidebar-head__id">
        <div class="sidebar-head__avatar" aria-hidden="true">
          <img src="{{ url_for('static', filename='usricon.png') }}" alt="">
        </div>
        <div class="sidebar-head__who">
          <div class="sidebar-head__name">{{ display_name }}</div>
          <div class="sidebar-head__role">{{ role_label(role) }}</div>
        </div>
      </div>
      <button class="sidebar-toggle" type="button" aria-label="{{ _('Cycle sidebar') }}" data-cycle-sidebar>
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M9 4v16"/><path d="M3 6h18"/><path d="M3 18h18"/></svg>
      </button>
    </div>

    <nav class="sidebar-nav" aria-label="{{ _('Functions') }}">
      <a href="{{ url_for('dashboard') }}" class="nav-item" data-partial-href="{{ url_for('dashboard') }}">
        <span class="nav-item__icon" aria-hidden="true">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12l9-9 9 9"/><path d="M5 10v10h14V10"/></svg>
        </span>
        <span class="nav-item__label">{{ _('Overview') }}</span>
      </a>

      <div class="nav-group">
        <div class="nav-group__label">{{ _('Workspace') }}</div>
        <a href="{{ url_for('upload') }}" class="nav-item" data-partial-href="{{ url_for('upload') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M12 16V4"/><path d="M6 10l6-6 6 6"/><path d="M4 20h16"/></svg>
          </span>
          <span class="nav-item__label">{{ _('Upload research') }}</span>
        </a>
        {% if role == 1 %}
        <a href="{{ url_for('my_submissions') }}" class="nav-item" data-partial-href="{{ url_for('my_submissions') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/><path d="M9 13h6"/><path d="M9 17h6"/></svg>
          </span>
          <span class="nav-item__label">{{ _('My submissions') }}</span>
        </a>
        {% endif %}
      </div>

      {% if role >= 2 %}
      <div class="nav-group">
        <div class="nav-group__label">{{ _('Review') }}</div>
        <a href="{{ url_for('review_list') }}" class="nav-item" data-partial-href="{{ url_for('review_list') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/></svg>
          </span>
          <span class="nav-item__label">{{ _('Review submissions') }}</span>
          {% if dashboard_stats and dashboard_stats.pending_reviews %}
          <span class="nav-item__badge">{{ dashboard_stats.pending_reviews }}</span>
          {% endif %}
        </a>
      </div>
      {% endif %}

      {% if role >= 3 %}
      <div class="nav-group">
        <div class="nav-group__label">{{ _('Collection') }}</div>
        <a href="{{ url_for('manage') }}" class="nav-item" data-partial-href="{{ url_for('manage') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h6v6H4z"/><path d="M14 4h6v6h-6z"/><path d="M4 14h6v6H4z"/><path d="M14 14h6v6h-6z"/></svg>
          </span>
          <span class="nav-item__label">{{ _('Manage papers') }}</span>
        </a>
        <a href="{{ url_for('paper_manage') }}" class="nav-item" data-partial-href="{{ url_for('paper_manage') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7h18"/><path d="M3 12h18"/><path d="M3 17h12"/></svg>
          </span>
          <span class="nav-item__label">{{ _('Categories & journals') }}</span>
        </a>
        <a href="{{ url_for('admin_guides_manage') }}" class="nav-item" data-partial-href="{{ url_for('admin_guides_manage') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>
          </span>
          <span class="nav-item__label">{{ _('Manage guides') }}</span>
        </a>
      </div>
      {% endif %}

      {% if role >= 2 %}
      <div class="nav-group">
        <div class="nav-group__label">{{ _('News') }}</div>
        <a href="{{ url_for('news_publish') }}" class="nav-item" data-partial-href="{{ url_for('news_publish') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4z"/></svg>
          </span>
          <span class="nav-item__label">{{ _('Write an article') }}</span>
        </a>
        <a href="{{ url_for('news_manage') }}" class="nav-item" data-partial-href="{{ url_for('news_manage') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6h16"/><path d="M4 12h16"/><path d="M4 18h10"/><circle cx="19" cy="18" r="2"/></svg>
          </span>
          <span class="nav-item__label">{{ _('Manage news') }}</span>
          {% if dashboard_stats and dashboard_stats.pending_news %}
          <span class="nav-item__badge nav-item__badge--quiet">{{ dashboard_stats.pending_news }}</span>
          {% endif %}
        </a>
      </div>
      {% endif %}

      {% if role >= 3 %}
      <div class="nav-group">
        <div class="nav-group__label">{{ _('Admin') }}</div>
        <a href="{{ url_for('admin_users') }}" class="nav-item" data-partial-href="{{ url_for('admin_users') }}">
          <span class="nav-item__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H7a4 4 0 0 0-4 4v2"/><circle cx="10" cy="7" r="4"/><path d="M21 21v-2a4 4 0 0 0-3-3.87"/><path d="M17 3.13a4 4 0 0 1 0 7.75"/></svg>
          </span>
          <span class="nav-item__label">{{ _('Manage users') }}</span>
        </a>
      </div>
      {% endif %}
    </nav>

    <div class="sidebar-foot">
      <div class="nav-group__label">{{ _('Account') }}</div>
      <a href="{{ url_for('change_password') }}" class="nav-item" data-partial-href="{{ url_for('change_password') }}">
        <span class="nav-item__icon" aria-hidden="true">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
        </span>
        <span class="nav-item__label">{{ _('Change password') }}</span>
      </a>
      <a href="{{ url_for('logout') }}" class="nav-item nav-item--signout">
        <span class="nav-item__icon" aria-hidden="true">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><path d="M16 17l5-5-5-5"/><path d="M21 12H9"/></svg>
        </span>
        <span class="nav-item__label">{{ _('Sign out') }}</span>
      </a>
    </div>
  </aside>

  <button class="sidebar-reveal" type="button" data-cycle-sidebar aria-label="{{ _('Show sidebar') }}" title="{{ _('Show sidebar') }}">
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M3 12h18"/><path d="M3 18h18"/></svg>
  </button>

  <main class="dashboard-main" id="dashboardMain">
    {% block panel %}{% endblock %}
  </main>
</div>

<script src="{{ url_for('static', filename='js/dashboard.js') }}"></script>
{% endblock %}
```

- [ ] **Step 4: Rewrite `templates/dashboard.html` as a thin wrapper**

Replace the entire file with:

```jinja
{# =============================================================
   templates/dashboard.html — Overview page.

   Now just fills the shell's panel slot with the overview partial.
   All sidebar markup lives in _dashboard_shell.html so other
   pages can re-use it via {% extends "_dashboard_shell.html" %}.

   Required context (provided by the dashboard() route in app.py):
     user, role_label, dashboard_stats — see _dashboard_shell.html
   ============================================================= #}
{% extends "_dashboard_shell.html" %}
{% block panel %}
  {% include "_dashboard/overview.html" %}
{% endblock %}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_dashboard_revamp_contract -v`
Expected: All `DashboardShellTemplateContractTest` tests PASS, including the three new ones. The existing `test_shell_extends_base` assertion still passes because `dashboard.html` transitively extends `base.html` through `_dashboard_shell.html` — but the literal string `'{% extends "base.html" %}'` no longer appears in `dashboard.html`. Update the existing assertion:

In `tests/test_dashboard_revamp_contract.py` at `test_shell_extends_base`, change:

```python
    def test_shell_extends_base(self):
        self.assertIn('{% extends "base.html" %}', self.src)
```

to:

```python
    def test_shell_extends_base(self):
        # dashboard.html now extends the shared shell, which itself extends base.html.
        self.assertIn('{% extends "_dashboard_shell.html" %}', self.src)
```

Re-run: `python3 -m unittest tests.test_dashboard_revamp_contract -v`
Expected: All tests PASS.

- [ ] **Step 6: Smoke-render dashboard.html manually**

Start the server (`./start_local.sh`), log in as admin, visit `/dashboard`. Confirm the sidebar + overview render identically to the pre-refactor version. There should be NO visual difference — this task is purely a code reorganization.

- [ ] **Step 7: Commit**

```bash
git add templates/_dashboard_shell.html templates/dashboard.html tests/test_dashboard_revamp_contract.py
git commit -m "refactor(dashboard): extract _dashboard_shell.html with panel slot"
```

---

## Task 2: Switch `_bare.html` and 11 sidebar templates to `panel` block

**Files:**
- Modify: `templates/_bare.html`
- Modify: 11 sidebar-destination templates (`upload.html`, `my_submissions.html`, `review_list.html`, `review_paper.html`, `delete.html`, `paper_manage.html`, `news_publish.html`, `news_manage.html`, `admin_users.html`, `guide_manage.html`, `change_password.html`)
- Test: `tests/test_partial_request_contract.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_partial_request_contract.py`, replace the `PartialAwareTemplatesContractTest` class with:

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
        # On a partial fetch, the page extends _bare.html; on a direct visit,
        # it extends the shared shell so the sidebar wraps it server-side.
        expected = '{% extends "_bare.html" if partial else "_dashboard_shell.html" %}'
        for name in self.TEMPLATES:
            path = ROOT / "templates" / name
            self.assertTrue(path.exists(), f"{name} missing")
            first_line = path.read_text(encoding="utf-8").splitlines()[0].strip()
            self.assertEqual(
                first_line,
                expected,
                f"{name} first line should be conditional extends, got: {first_line!r}",
            )

    def test_all_sidebar_destinations_use_panel_block(self):
        # The shell expects {% block panel %}, not the old {% block content %}.
        import re
        for name in self.TEMPLATES:
            src = (ROOT / "templates" / name).read_text(encoding="utf-8")
            self.assertRegex(
                src,
                r"\{%\s*block\s+panel\s*%\}",
                f"{name} must define a {{% block panel %}} block",
            )
            # The old content block must be gone (otherwise direct visits would
            # render an empty panel: dashboard_shell.html doesn't override content).
            self.assertNotRegex(
                src,
                r"\{%\s*block\s+content\s*%\}",
                f"{name} should no longer use {{% block content %}} — rename to panel",
            )

    def test_bare_template_uses_panel_block(self):
        src = (ROOT / "templates" / "_bare.html").read_text(encoding="utf-8")
        self.assertIn("{% block panel %}{% endblock %}", src)
```

Also update `BareTemplateContractTest.test_bare_template_has_content_block` to match:

```python
    def test_bare_template_has_panel_block(self):
        src = self.path.read_text(encoding="utf-8")
        self.assertIn("{% block panel %}{% endblock %}", src)
```

(Delete the old `test_bare_template_has_content_block` method.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_partial_request_contract -v`
Expected: FAIL on every sidebar template (`first line should be conditional extends, got: '{% extends "_bare.html" if partial else "base.html" %}'`) and on `_bare.html` (still uses `content`).

- [ ] **Step 3: Update `templates/_bare.html`**

Replace the last line:

```jinja
{% block content %}{% endblock %}
```

with:

```jinja
{% block panel %}{% endblock %}
```

- [ ] **Step 4: Update each of the 11 sidebar templates**

For each template in the `TEMPLATES` list, make two edits:

(a) Change the first line from
`{% extends "_bare.html" if partial else "base.html" %}`
to
`{% extends "_bare.html" if partial else "_dashboard_shell.html" %}`

(b) Change `{% block content %}` to `{% block panel %}` (single occurrence per file — `endblock` lines don't need changing).

Exact files and shell command to verify each:

```
templates/upload.html
templates/my_submissions.html
templates/review_list.html
templates/review_paper.html
templates/delete.html
templates/paper_manage.html
templates/news_publish.html
templates/news_manage.html
templates/admin_users.html
templates/guide_manage.html
templates/change_password.html
```

After editing, run:

```
grep -c "{% block panel %}" templates/upload.html templates/my_submissions.html templates/review_list.html templates/review_paper.html templates/delete.html templates/paper_manage.html templates/news_publish.html templates/news_manage.html templates/admin_users.html templates/guide_manage.html templates/change_password.html
```

Expected: each line shows `:1`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_partial_request_contract -v`
Expected: All tests PASS.

Run the full suite as a regression check:

```
python3 -m unittest discover -s tests -p "test_*.py" -v
```

Expected: 74+ tests PASS (existing 74 plus the new partial-aware assertions).

- [ ] **Step 6: Smoke-render each sidebar page manually**

Start the server, log in as admin, click each sidebar link in turn (Upload research, Review, Manage papers, Categories & journals, Manage guides, Write an article, Manage news, Manage users, Change password). Each should swap into the panel without a full reload — confirming the partial path still works after the block rename.

Then refresh the browser on each (Cmd-R / F5). Confirm the dashboard shell re-renders with the same panel still active — this exercises the new direct-visit shell extension. Currently the URLs are still `/upload`, `/manage`, etc. (Task 3+ moves them); the shell-on-direct-visit refresh test confirms that even at the old URL, refreshing now renders inside the shell.

- [ ] **Step 7: Commit**

```bash
git add templates/_bare.html templates/upload.html templates/my_submissions.html templates/review_list.html templates/review_paper.html templates/delete.html templates/paper_manage.html templates/news_publish.html templates/news_manage.html templates/admin_users.html templates/guide_manage.html templates/change_password.html tests/test_partial_request_contract.py
git commit -m "refactor(dashboard): render sidebar shell on direct visits to sidebar pages"
```

---

## Task 3: Add the URL-nesting contract test (move-list spec)

**Files:**
- Create: `tests/test_dashboard_url_nesting_contract.py`

This test is created before any routes are moved so that subsequent tasks (4–9) are TDD-driven against this single contract.

- [ ] **Step 1: Write the failing test**

Create `tests/test_dashboard_url_nesting_contract.py`:

```python
"""Contract: every curator-side endpoint lives under /dashboard/* and every
moved endpoint has a GET-only legacy redirect at its old path.

The endpoint -> (new_path, legacy_path) map below is the single source of
truth for the URL-nesting refactor. Each task that moves a route family
flips entries from xfail to pass."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Endpoint name -> (expected new URL pattern, old URL pattern for redirect).
# old=None means POST-only route, no legacy redirect needed.
MOVED_ROUTES = {
    # Task 4: workspace
    "upload":                          ("/dashboard/upload",                          "/upload"),
    "upload_success":                  ("/dashboard/upload/success",                  "/upload/success"),
    "my_submissions":                  ("/dashboard/my-submissions",                  "/my-submissions"),
    "my_submission_view":              ("/dashboard/my-submissions/<sub_id>",         "/my-submissions/<sub_id>"),
    "my_submission_file":              ("/dashboard/my-submissions/<sub_id>/file",    "/my-submissions/<sub_id>/file"),
    "my_submission_delete":            ("/dashboard/my-submissions/<sub_id>/delete",  None),

    # Task 5: collection management
    "manage":                          ("/dashboard/manage",                          "/manage"),
    "paper_modify":                    ("/dashboard/paper/<path:filename>/modify",    "/paper/<path:filename>/modify"),
    "paper_delete":                    ("/dashboard/paper/<path:filename>/delete",    None),
    "paper_manage":                    ("/dashboard/admin/paper-manage",              "/admin/paper-manage"),
    "admin_paper_categories_add":      ("/dashboard/admin/paper-categories/add",      None),
    "admin_paper_categories_rename":   ("/dashboard/admin/paper-categories/rename",   None),
    "admin_paper_categories_delete":   ("/dashboard/admin/paper-categories/delete",   None),
    "admin_ee_subjects_add":           ("/dashboard/admin/ee-subjects/add",           None),
    "admin_ee_subjects_delete":        ("/dashboard/admin/ee-subjects/delete",        None),
    "admin_journals_add":              ("/dashboard/admin/journals/add",              None),
    "admin_journals_delete":           ("/dashboard/admin/journals/delete",           None),
    "admin_journal_edit":              ("/dashboard/admin/journal/<journal_id>/edit", "/admin/journal/<journal_id>/edit"),

    # Task 6: review
    "review_list":                     ("/dashboard/review",                          "/review"),
    "review_paper":                    ("/dashboard/review/<sub_id>",                 "/review/<sub_id>"),
    "review_accept":                   ("/dashboard/review/<sub_id>/accept",          None),
    "review_reject":                   ("/dashboard/review/<sub_id>/reject",          None),

    # Task 7: news editor
    "news_publish":                    ("/dashboard/news/publish",                    "/news/publish"),
    "news_edit":                       ("/dashboard/news/<news_id>/edit",             "/news/<news_id>/edit"),
    "news_delete":                     ("/dashboard/news/<news_id>/delete",           None),
    "news_manage":                     ("/dashboard/news/manage",                     "/news/manage"),
    "news_categories_add":             ("/dashboard/news/categories/add",             None),
    "news_categories_rename":          ("/dashboard/news/categories/rename",          None),
    "news_categories_delete":          ("/dashboard/news/categories/delete",          None),
    "news_upload_inline_image":        ("/dashboard/news/upload-inline-image",        None),

    # Task 8: admin (users + guides)
    "admin_users":                     ("/dashboard/admin/users",                     "/admin/users"),
    "admin_users_roles":               ("/dashboard/admin/users/roles",               None),
    "admin_users_add":                 ("/dashboard/admin/users/add",                 None),
    "admin_user_role":                 ("/dashboard/admin/users/<path:username>/role",            None),
    "admin_user_reset_password":       ("/dashboard/admin/users/<path:username>/reset-password",  None),
    "admin_user_delete":               ("/dashboard/admin/users/<path:username>/delete",          None),
    "admin_ms_user_role":              ("/dashboard/admin/ms-users/<path:ms_id>/role",            None),
    "admin_ms_user_delete":            ("/dashboard/admin/ms-users/<path:ms_id>/delete",          None),
    "admin_ms_user_set_password":      ("/dashboard/admin/ms-users/<path:ms_id>/set-password",    None),
    "admin_guides_manage":             ("/dashboard/admin/guides",                    "/admin/guides"),
    "admin_guide_new":                 ("/dashboard/admin/guides/new",                "/admin/guides/new"),
    "admin_guide_edit":                ("/dashboard/admin/guides/<int:guide_id>/edit", "/admin/guides/<int:guide_id>/edit"),
    "admin_guide_delete":              ("/dashboard/admin/guides/<int:guide_id>/delete", None),
    "admin_guides_upload_image":       ("/dashboard/admin/guides/upload-image",       None),

    # Task 9: account
    "change_password":                 ("/dashboard/account/change-password",         "/account/change-password"),
}


def _build_app():
    """Construct the Flask app once; the test reads its url_map only."""
    import os
    os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")
    os.environ.setdefault("PAPERQUERY_DATABASE_URL", "sqlite:///:memory:")
    import importlib
    import app as app_module
    importlib.reload(app_module)
    return app_module.create_app()


class DashboardUrlNestingContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = _build_app()
        cls.rules_by_endpoint = {}
        for rule in cls.app.url_map.iter_rules():
            cls.rules_by_endpoint.setdefault(rule.endpoint, []).append(rule.rule)

    def test_every_moved_endpoint_is_under_dashboard(self):
        missing = []
        for endpoint, (new_path, _old) in MOVED_ROUTES.items():
            rules = self.rules_by_endpoint.get(endpoint, [])
            if not any(r == new_path for r in rules):
                missing.append(f"{endpoint}: expected {new_path!r}, got {rules!r}")
        self.assertEqual(missing, [], "Endpoints not at expected /dashboard/ paths:\n" + "\n".join(missing))

    def test_legacy_redirect_endpoints_exist_for_get_routes(self):
        # Each moved endpoint with a non-None old_path must have a paired
        # *_legacy endpoint serving the old URL.
        missing = []
        for endpoint, (_new, old_path) in MOVED_ROUTES.items():
            if old_path is None:
                continue
            legacy_endpoint = f"{endpoint}_legacy"
            rules = self.rules_by_endpoint.get(legacy_endpoint, [])
            if not any(r == old_path for r in rules):
                missing.append(f"{legacy_endpoint}: expected {old_path!r}, got {rules!r}")
        self.assertEqual(missing, [], "Legacy redirect endpoints missing:\n" + "\n".join(missing))

    def test_legacy_routes_actually_redirect(self):
        client = self.app.test_client()
        skipped = []
        for endpoint, (new_path, old_path) in MOVED_ROUTES.items():
            if old_path is None:
                continue
            # Substitute fake values for path parameters so the URL is concrete.
            concrete = (old_path
                        .replace("<sub_id>", "abc")
                        .replace("<news_id>", "abc")
                        .replace("<path:filename>", "x.pdf")
                        .replace("<path:username>", "alice")
                        .replace("<path:ms_id>", "ms-1")
                        .replace("<journal_id>", "j1")
                        .replace("<int:guide_id>", "1"))
            resp = client.get(concrete, follow_redirects=False)
            if resp.status_code not in (301, 302, 308):
                skipped.append(f"{old_path} -> got {resp.status_code}, expected redirect")
        self.assertEqual(skipped, [], "Legacy routes didn't redirect:\n" + "\n".join(skipped))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: FAIL on `test_every_moved_endpoint_is_under_dashboard` (all current rules are at old paths) and on `test_legacy_redirect_endpoints_exist_for_get_routes` (no legacy endpoints exist yet).

This test stays red until Task 9 is complete. Each of Tasks 4–9 will turn one family from red to green without touching other tasks' rows.

- [ ] **Step 3: Commit**

```bash
git add tests/test_dashboard_url_nesting_contract.py
git commit -m "test(dashboard): add URL-nesting contract spec (currently red)"
```

---

## Task 4: Move `/upload` and `/my-submissions` to `/dashboard/*`

**Files:**
- Modify: `app.py` (`upload`, `upload_success`, `my_submissions`, `my_submission_view`, `my_submission_file`, `my_submission_delete` routes around lines 1277, 1596, 2701, 2711, 2734, 2763)

- [ ] **Step 1: Run the URL-nesting test, focused on this task's rows**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: still RED — confirm `upload`, `upload_success`, `my_submissions`, `my_submission_view`, `my_submission_file`, `my_submission_delete` are in the failure list.

- [ ] **Step 2: Change the route paths in `app.py`**

For each of these handlers, change the `@app.route(...)` path from the old to the new value. Keep methods and endpoint kwargs as-is.

```python
# Line ~1277
@app.route("/dashboard/upload", methods=["GET", "POST"])

# Line ~1596
@app.route("/dashboard/upload/success")

# Line ~2701
@app.route("/dashboard/my-submissions")

# Line ~2711
@app.route("/dashboard/my-submissions/<sub_id>/delete", methods=["POST"])

# Line ~2734
@app.route("/dashboard/my-submissions/<sub_id>")

# Line ~2763
@app.route("/dashboard/my-submissions/<sub_id>/file")
```

- [ ] **Step 3: Add GET-only legacy redirect handlers**

Append immediately after the `upload` route definition (i.e. after the upload function body ends):

```python
@app.route("/upload", endpoint="upload_legacy")
def upload_legacy():
    return redirect(url_for("upload"), code=301)

@app.route("/upload/success", endpoint="upload_success_legacy")
def upload_success_legacy():
    return redirect(url_for("upload_success"), code=301)
```

Add after the `my_submission_file` route:

```python
@app.route("/my-submissions", endpoint="my_submissions_legacy")
def my_submissions_legacy():
    return redirect(url_for("my_submissions"), code=301)

@app.route("/my-submissions/<sub_id>", endpoint="my_submission_view_legacy")
def my_submission_view_legacy(sub_id):
    return redirect(url_for("my_submission_view", sub_id=sub_id), code=301)

@app.route("/my-submissions/<sub_id>/file", endpoint="my_submission_file_legacy")
def my_submission_file_legacy(sub_id):
    return redirect(url_for("my_submission_file", sub_id=sub_id), code=301)
```

- [ ] **Step 4: Run the URL-nesting test**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: still failing, but only on Tasks 5–9 endpoints. The six Task-4 endpoints should now appear in neither failure list.

- [ ] **Step 5: Run the full suite as a regression check**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: No regressions. Existing tests that hit upload-related endpoints (if any) should still pass because `url_for` resolves to the new path.

- [ ] **Step 6: Smoke-test in the browser**

Start the server, log in as a contributor (role ≥ 2). Click the Upload research sidebar item — URL should now read `/dashboard/upload`. Submit a file — should redirect to `/dashboard/upload/success` and stay in the dashboard shell. Type `/upload` directly in the address bar — should 301 to `/dashboard/upload`.

- [ ] **Step 7: Commit**

```bash
git add app.py
git commit -m "feat(dashboard): nest /upload and /my-submissions under /dashboard"
```

---

## Task 5: Move `/manage`, `/paper/<f>/modify|delete`, and admin paper-management routes

**Files:**
- Modify: `app.py` (`manage`, `paper_modify`, `paper_delete`, `paper_manage`, `admin_paper_categories_*`, `admin_ee_subjects_*`, `admin_journals_*`, `admin_journal_edit` around lines 1605, 1659, 1807, 2362, 2373, 2388, 2416, 2431, 2451, 2473, 2497, 2523)

- [ ] **Step 1: Confirm the URL-nesting test is red on these rows**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: Task 5 endpoints appear in the failure list.

- [ ] **Step 2: Change the route paths in `app.py`**

```python
# Line ~1605
@app.route("/dashboard/manage")

# Line ~1659
@app.route("/dashboard/paper/<path:filename>/modify", methods=["GET", "POST"])

# Line ~1807
@app.route("/dashboard/paper/<path:filename>/delete", methods=["POST"])

# Line ~2362
@app.route("/dashboard/admin/paper-manage")

# Line ~2373
@app.route("/dashboard/admin/paper-categories/add", methods=["POST"])

# Line ~2388
@app.route("/dashboard/admin/paper-categories/rename", methods=["POST"])

# Line ~2416
@app.route("/dashboard/admin/paper-categories/delete", methods=["POST"])

# Line ~2431
@app.route("/dashboard/admin/ee-subjects/add", methods=["POST"])

# Line ~2451
@app.route("/dashboard/admin/ee-subjects/delete", methods=["POST"])

# Line ~2473
@app.route("/dashboard/admin/journals/add", methods=["POST"])

# Line ~2497
@app.route("/dashboard/admin/journals/delete", methods=["POST"])

# Line ~2523
@app.route("/dashboard/admin/journal/<journal_id>/edit", methods=["GET", "POST"])
```

- [ ] **Step 3: Add GET-only legacy redirects**

After the `paper_modify` block:

```python
@app.route("/manage", endpoint="manage_legacy")
def manage_legacy():
    return redirect(url_for("manage"), code=301)

@app.route("/paper/<path:filename>/modify", endpoint="paper_modify_legacy")
def paper_modify_legacy(filename):
    return redirect(url_for("paper_modify", filename=filename), code=301)
```

After the `admin_journal_edit` block:

```python
@app.route("/admin/paper-manage", endpoint="paper_manage_legacy")
def paper_manage_legacy():
    return redirect(url_for("paper_manage"), code=301)

@app.route("/admin/journal/<journal_id>/edit", endpoint="admin_journal_edit_legacy")
def admin_journal_edit_legacy(journal_id):
    return redirect(url_for("admin_journal_edit", journal_id=journal_id), code=301)
```

- [ ] **Step 4: Run the URL-nesting test**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: Task 5 endpoints have left the failure list.

- [ ] **Step 5: Run the full suite**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: No regressions.

- [ ] **Step 6: Smoke-test**

Log in as admin. Click Manage papers, Categories & journals — confirm in-shell. Edit a paper — submit — confirm redirect stays in shell. Type `/admin/paper-manage` directly → 301 to `/dashboard/admin/paper-manage`.

- [ ] **Step 7: Commit**

```bash
git add app.py
git commit -m "feat(dashboard): nest /manage and paper/admin routes under /dashboard"
```

---

## Task 6: Move `/review/*` routes

**Files:**
- Modify: `app.py` (`review_list`, `review_paper`, `review_accept`, `review_reject` around lines 2775, 2792, 2805, 2859)

- [ ] **Step 1: Confirm red rows**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract.DashboardUrlNestingContractTest -v`
Expected: review_list/review_paper appear in the failure list.

- [ ] **Step 2: Change route paths**

```python
# Line ~2775
@app.route("/dashboard/review")

# Line ~2792
@app.route("/dashboard/review/<sub_id>")

# Line ~2805
@app.route("/dashboard/review/<sub_id>/accept", methods=["POST"])

# Line ~2859
@app.route("/dashboard/review/<sub_id>/reject", methods=["POST"])
```

- [ ] **Step 3: Add legacy redirects**

After the `review_reject` block:

```python
@app.route("/review", endpoint="review_list_legacy")
def review_list_legacy():
    return redirect(url_for("review_list"), code=301)

@app.route("/review/<sub_id>", endpoint="review_paper_legacy")
def review_paper_legacy(sub_id):
    return redirect(url_for("review_paper", sub_id=sub_id), code=301)
```

- [ ] **Step 4: Run the URL-nesting test**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: review_* endpoints have left the failure list.

- [ ] **Step 5: Run the full suite**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: No regressions.

- [ ] **Step 6: Smoke-test**

Submit a paper as role-1, switch to role-2, click Review submissions — confirm in-shell. Open a submission, click Accept — confirm in-shell redirect.

- [ ] **Step 7: Commit**

```bash
git add app.py
git commit -m "feat(dashboard): nest /review/* under /dashboard"
```

---

## Task 7: Move `/news/publish`, `/news/manage`, `/news/<id>/edit|delete`, `/news/categories/*`, `/news/upload-inline-image`

**Files:**
- Modify: `app.py` (`news_publish`, `news_edit`, `news_delete`, `news_manage`, `news_categories_add`, `news_categories_rename`, `news_categories_delete`, `news_upload_inline_image` around lines 1981, 2056, 2132, 2143, 2152, 2167, 2201, 1963)

**Note:** `/news` (public list at line 1946) and `/news/<news_id>` (public detail at line 2604) **stay** at the root. Only curator-side news routes move.

- [ ] **Step 1: Confirm red rows**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: news_publish, news_edit, news_delete, news_manage, news_categories_*, news_upload_inline_image appear in the failure list.

- [ ] **Step 2: Change route paths**

```python
# Line ~1963
@app.route("/dashboard/news/upload-inline-image", methods=["POST"])

# Line ~1981
@app.route("/dashboard/news/publish", methods=["GET", "POST"])

# Line ~2056
@app.route("/dashboard/news/<news_id>/edit", methods=["GET", "POST"])

# Line ~2132
@app.route("/dashboard/news/<news_id>/delete", methods=["POST"])

# Line ~2143
@app.route("/dashboard/news/manage")

# Line ~2152
@app.route("/dashboard/news/categories/add", methods=["POST"])

# Line ~2167
@app.route("/dashboard/news/categories/rename", methods=["POST"])

# Line ~2201
@app.route("/dashboard/news/categories/delete", methods=["POST"])
```

- [ ] **Step 3: Add legacy redirects**

After the `news_categories_delete` block:

```python
@app.route("/news/publish", endpoint="news_publish_legacy")
def news_publish_legacy():
    return redirect(url_for("news_publish"), code=301)

@app.route("/news/<news_id>/edit", endpoint="news_edit_legacy")
def news_edit_legacy(news_id):
    return redirect(url_for("news_edit", news_id=news_id), code=301)

@app.route("/news/manage", endpoint="news_manage_legacy")
def news_manage_legacy():
    return redirect(url_for("news_manage"), code=301)
```

- [ ] **Step 4: Verify the public news routes did NOT move**

Run:

```
grep -n '@app\.route("/news' app.py | grep -v dashboard
```

Expected output (exactly these — no other curator routes):

```
@app.route("/news")                         # public list
@app.route("/news/<news_id>")               # public detail
@app.route("/news/publish", endpoint=...)   # legacy redirect
@app.route("/news/<news_id>/edit", endpoint=...)
@app.route("/news/manage", endpoint=...)
```

- [ ] **Step 5: Run the URL-nesting test**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: news editor endpoints have left the failure list. Only Task 8 + 9 rows remain.

- [ ] **Step 6: Run the full suite**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: No regressions.

- [ ] **Step 7: Smoke-test**

Log in as moderator. Click Write an article — confirm in-shell. Save as draft — should land on `/dashboard/news/manage` with the draft pill. Edit, then click Publish — should land on `/dashboard/news/manage` with the published pill. Visit `/news` as a reader — confirm only the published article appears.

- [ ] **Step 8: Commit**

```bash
git add app.py
git commit -m "feat(dashboard): nest curator news routes under /dashboard"
```

---

## Task 8: Move `/admin/users/*`, `/admin/ms-users/*`, `/admin/guides/*`

**Files:**
- Modify: `app.py` (`admin_users`, `admin_users_roles`, `admin_users_add`, `admin_user_role`, `admin_user_reset_password`, `admin_user_delete`, `admin_ms_user_role`, `admin_ms_user_delete`, `admin_ms_user_set_password`, `admin_guides_manage`, `admin_guide_new`, `admin_guide_edit`, `admin_guide_delete`, `admin_guides_upload_image` around lines 950–1062, 2218–2351)

- [ ] **Step 1: Confirm red rows**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: all Task 8 endpoints appear in the failure list.

- [ ] **Step 2: Change route paths for users**

```python
# Line ~950
@app.route("/dashboard/admin/users")

# Line ~965
@app.route("/dashboard/admin/users/roles", methods=["POST"])

# Line ~983
@app.route("/dashboard/admin/users/add", methods=["POST"])

# Line ~1001
@app.route("/dashboard/admin/users/<path:username>/role", methods=["POST"])

# Line ~1013
@app.route("/dashboard/admin/users/<path:username>/reset-password", methods=["POST"])

# Line ~1028
@app.route("/dashboard/admin/users/<path:username>/delete", methods=["POST"])

# Line ~1039
@app.route("/dashboard/admin/ms-users/<path:ms_id>/role", methods=["POST"])

# Line ~1051
@app.route("/dashboard/admin/ms-users/<path:ms_id>/delete", methods=["POST"])

# Line ~1062
@app.route("/dashboard/admin/ms-users/<path:ms_id>/set-password", methods=["POST"])
```

- [ ] **Step 3: Change route paths for guides**

```python
# Line ~2248
@app.route("/dashboard/admin/guides/upload-image", methods=["POST"])

# Line ~2270 (admin_guide_new)
@app.route("/dashboard/admin/guides/new", methods=["GET", "POST"], endpoint="admin_guide_new")

# Line ~2271 (admin_guide_edit — same handler as new)
@app.route("/dashboard/admin/guides/<int:guide_id>/edit", methods=["GET", "POST"], endpoint="admin_guide_edit")

# Line ~2342
@app.route("/dashboard/admin/guides")

# Line ~2350
@app.route("/dashboard/admin/guides/<int:guide_id>/delete", methods=["POST"])
```

Public guide reader routes (`/guides`, `/guides/<slug>`) at lines 2218 and 2241 **stay** at the root.

- [ ] **Step 4: Add legacy redirects**

After the `admin_user_delete` block:

```python
@app.route("/admin/users", endpoint="admin_users_legacy")
def admin_users_legacy():
    return redirect(url_for("admin_users"), code=301)
```

After the `admin_guide_delete` block:

```python
@app.route("/admin/guides", endpoint="admin_guides_manage_legacy")
def admin_guides_manage_legacy():
    return redirect(url_for("admin_guides_manage"), code=301)

@app.route("/admin/guides/new", endpoint="admin_guide_new_legacy")
def admin_guide_new_legacy():
    return redirect(url_for("admin_guide_new"), code=301)

@app.route("/admin/guides/<int:guide_id>/edit", endpoint="admin_guide_edit_legacy")
def admin_guide_edit_legacy(guide_id):
    return redirect(url_for("admin_guide_edit", guide_id=guide_id), code=301)
```

- [ ] **Step 5: Run the URL-nesting test**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: Task 8 endpoints have left the failure list.

- [ ] **Step 6: Run the full suite**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: No regressions. Existing guide-related tests (`tests/test_guide_*.py`) should still pass because they use `url_for`.

- [ ] **Step 7: Smoke-test**

Log in as admin. Click Manage users — confirm in-shell, change a role, confirm in-shell. Click Manage guides — confirm in-shell, create a guide, confirm in-shell.

- [ ] **Step 8: Commit**

```bash
git add app.py
git commit -m "feat(dashboard): nest /admin/users/* and /admin/guides/* under /dashboard"
```

---

## Task 9: Move `/account/change-password`

**Files:**
- Modify: `app.py` (`change_password` around line 908)

- [ ] **Step 1: Change route path**

```python
# Line ~908
@app.route("/dashboard/account/change-password", methods=["GET", "POST"])
```

- [ ] **Step 2: Add legacy redirect**

After the `change_password` block (before the next route):

```python
@app.route("/account/change-password", endpoint="change_password_legacy")
def change_password_legacy():
    return redirect(url_for("change_password"), code=301)
```

- [ ] **Step 3: Run the URL-nesting test — should now be fully green**

Run: `python3 -m unittest tests.test_dashboard_url_nesting_contract -v`
Expected: ALL tests PASS (test_every_moved_endpoint_is_under_dashboard, test_legacy_redirect_endpoints_exist_for_get_routes, test_legacy_routes_actually_redirect).

- [ ] **Step 4: Run the full suite**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: All 75+ tests PASS.

- [ ] **Step 5: Smoke-test**

Click Change password — confirm in-shell. Submit a password change — confirm flash + redirect stays in shell.

- [ ] **Step 6: Commit**

```bash
git add app.py
git commit -m "feat(dashboard): nest /account/change-password under /dashboard"
```

---

## Task 10: Refine `dashboard.js` off-route redirect handling

**Files:**
- Modify: `static/js/dashboard.js` (lines 65-104, the `loadPartial` function)
- Test: `tests/test_dashboard_revamp_contract.py`

**Why:** Today the JS does `window.location.href = res.url` whenever the server redirects to a path different from the requested one. After Tasks 4–9, many in-shell POSTs redirect within `/dashboard/*` (e.g. `/dashboard/news/publish` → `/dashboard/news/manage`). The current logic would treat that as "off-route" and full-nav out, defeating the whole refactor. Refine it: only full-nav when the redirect **leaves** `/dashboard/*`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dashboard_revamp_contract.py` inside `DashboardAssetsContractTest`:

```python
    def test_dashboard_js_only_full_navs_when_leaving_dashboard(self):
        # The redirect-fallback in loadPartial must check that we *leave*
        # /dashboard/* before doing window.location.href. A naive path-mismatch
        # check would kick the user out on every in-shell redirect after the
        # URL-nesting refactor.
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertRegex(
            src,
            r"!.*startsWith\(\s*['\"]/dashboard['\"]\s*\)",
            "dashboard.js must guard the full-nav fallback with a /dashboard prefix check",
        )

    def test_dashboard_js_pushes_resolved_redirect_url(self):
        # When a redirect resolves in-shell, the address bar must show the
        # final URL (e.g. /dashboard/news/manage), not the originally posted one.
        src = (ROOT / "static" / "js" / "dashboard.js").read_text(encoding="utf-8")
        self.assertIn("res.url", src)
        self.assertRegex(src, r"history\.pushState\([^;]*res\.url|history\.pushState\([^;]*resolvedUrl")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_dashboard_revamp_contract.DashboardAssetsContractTest -v`
Expected: FAIL on the two new tests.

- [ ] **Step 3: Update `static/js/dashboard.js`**

Replace lines 53-104 (the entire `loadPartial` function) with:

```javascript
  /* ── Core partial loader ───────────────────────────────────────────── */
  function loadPartial(url, opts) {
    opts = opts || {};
    main.classList.add('is-swapping');

    var init = {
      method: opts.method || 'GET',
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
      credentials: 'same-origin',
      redirect: 'follow'
    };
    init.headers[PARTIAL_HEADER] = '1';
    if (opts.body) init.body = opts.body;

    fetch(url, init)
      .then(function (res) {
        // If the server redirected us OUT of /dashboard/* (e.g. login wall,
        // session expiry), fall back to a full navigation. In-shell redirects
        // (POST /dashboard/news/publish → /dashboard/news/manage) stay partial.
        if (res.redirected && res.url) {
          var redirPath = pathOf(res.url);
          var origPath  = pathOf(url);
          var leftDashboard = origPath.indexOf('/dashboard') === 0 && !redirPath.startsWith('/dashboard');
          if (leftDashboard) {
            window.location.href = res.url;
            return null;
          }
          // Otherwise carry the resolved URL forward so pushState writes
          // /dashboard/news/manage instead of /dashboard/news/publish.
          opts.resolvedUrl = res.url;
        }
        return res.text();
      })
      .then(function (html) {
        if (html === null) return;
        main.innerHTML = html;
        main.classList.remove('is-swapping');
        main.scrollTop = 0;

        // Run any <script> tags inside the swapped HTML.
        main.querySelectorAll('script').forEach(function (oldScript) {
          var s = document.createElement('script');
          for (var i = 0; i < oldScript.attributes.length; i++) {
            s.setAttribute(oldScript.attributes[i].name, oldScript.attributes[i].value);
          }
          s.textContent = oldScript.textContent;
          oldScript.parentNode.replaceChild(s, oldScript);
        });

        var finalUrl = opts.resolvedUrl || url;
        activateNavForPath(pathOf(finalUrl));
        if (opts.push !== false) {
          history.pushState({ partial: finalUrl }, '', finalUrl);
        }
        document.dispatchEvent(new CustomEvent('keydion:partial-loaded', { detail: { url: finalUrl } }));
      })
      .catch(function (err) {
        main.classList.remove('is-swapping');
        main.innerHTML = '<div class="panel"><div class="panel-placeholder">' +
          '<h3>Could not load this section.</h3><p>' + (err && err.message || err) + '</p></div></div>';
      });
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_dashboard_revamp_contract.DashboardAssetsContractTest -v`
Expected: All tests in the class PASS, including the two new ones.

Run the full suite:

```
python3 -m unittest discover -s tests -p "test_*.py" -v
```

Expected: All tests PASS.

- [ ] **Step 5: Smoke-test in the browser**

Log in as moderator. Open Write an article, click Save as Draft — URL bar should read `/dashboard/news/manage` (not stuck at `/dashboard/news/publish`). Same for Publish. Try clicking Sign out — should still full-nav to `/logout` because that path doesn't start with `/dashboard`.

- [ ] **Step 6: Commit**

```bash
git add static/js/dashboard.js tests/test_dashboard_revamp_contract.py
git commit -m "fix(dashboard): keep in-shell on /dashboard redirects, push resolved URL"
```

---

## Task 11: Remove orphaned old-path templates if any, audit residual `url_for` strings

**Files:**
- Audit: `app.py`, `templates/`

This task is a final sweep. After moving routes, hand-written URL strings (not produced by `url_for`) are the only remaining risk.

- [ ] **Step 1: Search the codebase for hard-coded old paths**

Run:

```
grep -rn '"/upload\b\|"/manage\b\|"/news/publish\|"/news/manage\|"/news/[^"]*/edit\|"/news/[^"]*/delete\|"/admin/users\|"/admin/guides\|"/admin/paper-manage\|"/admin/journal/\|"/admin/paper-categories\|"/admin/ee-subjects\|"/admin/journals\|"/admin/ms-users\|"/account/change-password\|"/review\b\|"/my-submissions\|"/paper/[^"]*/modify\|"/paper/[^"]*/delete' app.py templates static
```

Expected: Hits should ONLY be in:
- The `@app.route("/...")` legacy redirect decorators we added (these are intentional).
- The `endpoint="..._legacy"` argument strings.

Any other occurrence is a hard-coded URL that needs to be replaced with `url_for(...)`.

- [ ] **Step 2: Fix any unexpected hits**

For each unexpected hit, replace the literal URL with the corresponding `url_for(...)` call. Example pattern:

```jinja
<!-- Before: -->
<a href="/upload">Upload</a>
<!-- After: -->
<a href="{{ url_for('upload') }}">Upload</a>
```

If you find none, skip to Step 3.

- [ ] **Step 3: Final regression run**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: All tests PASS.

- [ ] **Step 4: Commit (only if Step 2 made changes)**

```bash
git add app.py templates
git commit -m "fix(dashboard): replace remaining hard-coded URLs with url_for"
```

If Step 2 made no changes, skip the commit.

---

## Task 12: Manual end-to-end verification

**This task does not produce code changes.** It is the final user-facing validation that the refactor works.

- [ ] **Step 1: Reset dev environment**

Run: `./start_local.sh`
Open: `http://localhost:8123` (or whatever port the script prints) in a private browser window so localStorage / cookies don't carry sidebar state from earlier sessions.

- [ ] **Step 2: Role 1 (Reader) walkthrough**

Log in as a role-1 user. Verify:
- `/dashboard` → in-shell, stats row hidden, two quick actions visible (Upload + Change password).
- Click Upload research → URL becomes `/dashboard/upload`, in-shell.
- Click Change password → URL becomes `/dashboard/account/change-password`, in-shell.
- Refresh on Change password page → stays in shell (was the original symptom).
- Type `/upload` in the address bar → 301 to `/dashboard/upload`, in-shell.

- [ ] **Step 3: Role 2 (Moderator) walkthrough**

Log in as a role-2 user. Verify all of role-1 PLUS:
- Click Review submissions → `/dashboard/review`, in-shell.
- Click Write an article → `/dashboard/news/publish`, in-shell. Save as draft → lands on `/dashboard/news/manage`, in-shell.
- Click Manage news → `/dashboard/news/manage`, in-shell.
- Type `/news/publish` directly → 301 to `/dashboard/news/publish`.

- [ ] **Step 4: Role 3 (Admin) walkthrough**

Log in as a role-3 user. Verify all of role-2 PLUS:
- Click Manage papers → `/dashboard/manage`, in-shell. Edit a paper → submit → stays in shell.
- Click Categories & journals → `/dashboard/admin/paper-manage`, in-shell. Add a category → stays in shell.
- Click Manage guides → `/dashboard/admin/guides`, in-shell. Create a guide → submit → stays in shell.
- Click Manage users → `/dashboard/admin/users`, in-shell. Change a user's role → stays in shell.

- [ ] **Step 5: Cross-locale verification**

In ZH: repeat steps 2–4 with the language switched to 中文. Verify every page loads without 500 errors and the dashboard shell labels are translated.

- [ ] **Step 6: Public route sanity check**

Visit `/`, `/news`, `/news/<some-published-id>`, `/guides`, `/guides/<slug>`, `/papers/<filename>`, `/login`. Each should render under the public top-nav `base.html` shell, NOT the dashboard shell.

- [ ] **Step 7: Bookmark survival check**

For each of these old paths, paste it in the address bar and confirm a 301 to the new path: `/upload`, `/manage`, `/news/publish`, `/news/manage`, `/news/<id>/edit`, `/admin/users`, `/admin/guides`, `/admin/paper-manage`, `/account/change-password`, `/review`, `/my-submissions`, `/paper/<f>/modify`.

- [ ] **Step 8: Sign-off**

If all of steps 2–7 pass: mark this task complete. If any step fails: file a follow-up commit fixing the specific case (most likely a hard-coded URL that escaped Task 11's audit) and re-run that step.

---

## Self-Review

**Spec coverage:**
- "Upload (and others) escape the dashboard shell" → fixed by Task 2 (extends `_dashboard_shell.html` on direct visit) + Task 10 (in-shell redirects stay in shell).
- "Make them subpages of dashboard like `/dashboard/upload`" → covered by Tasks 4–9 with one task per route family.
- Backward compat for external bookmarks → covered by per-task legacy redirects + verified in Task 12 step 7.
- No regression for public routes → public news/guides/papers explicitly listed as NOT moved in Tasks 7, 8.
- Test coverage → Task 3 lays down the single contract that 4–9 turn green, plus Tasks 1, 2, 10 add localized tests for the template / JS changes.

**Placeholder scan:** No "TBD", no "implement later", no "etc.". Every step has either a code block or an exact command + expected output.

**Type consistency:** `_dashboard_shell.html` is the layout name used in Tasks 1, 2, 3, and the assertion in `test_partial_request_contract.py`. The block name `panel` is consistent across the shell, `_bare.html`, and all 11 sidebar templates. Endpoint names (`upload`, `manage`, etc.) are stable across all tasks; only URL patterns change. Legacy endpoint naming convention is `<endpoint>_legacy` consistently across Tasks 4–9 and asserted by `test_dashboard_url_nesting_contract.py`.

**Known risk:** The URL-nesting test in Task 3 imports and reloads `app.py` to read its `url_map`. If `create_app()` has hidden side effects (writes to disk, opens DB connections requiring a real backend), this test may need a fixture. The `os.environ.setdefault` calls in `_build_app()` address the two known env requirements. If a different env var turns out to be needed, add it there — do not skip the test.