# Login-free Paper Viewing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let visitors preview the full PDF, download, and open the raw file of papers without logging in, gated by a single `PAPERQUERY_OPEN_ACCESS` environment flag that defaults off in committed code.

**Architecture:** A module-level `OPEN_ACCESS` boolean is read from the environment in `app.py` and exposed to templates via the existing `inject_global_vars` context processor. Two viewing routes (`paper_file`, `download`) skip their `require_login()` gate when the flag is on, the preview page serves the full PDF instead of the 2-page truncated copy, and two templates show the full-access UI instead of "Sign in" prompts. The flag is turned on for the dev and prod deployments by adding it to the gitignored `.env` and `.env.prod` files.

**Tech Stack:** Python 3 / Flask, Jinja2 templates, `python-dotenv`, `unittest` contract tests (AST source inspection + Jinja render + template-text assertions).

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `app.py` | Modify | Define `OPEN_ACCESS`; expose it to templates; make `paper_file`/`download`/`preview_paper` open-access aware |
| `templates/preview.html` | Modify | Show Download + "Open in new tab" buttons and hide the truncation banner when open access is on |
| `templates/search.html` | Modify | Show the "Download" link instead of "Sign in to Download" when open access is on |
| `tests/test_open_access_contract.py` | Create | Contract tests for the flag, route gates, and template gating |
| `.env` | Modify (local, gitignored) | Enable open access for dev / `start_local.sh` |
| `.env.prod` | Modify (local, gitignored) | Enable open access for prod / `run_prod.sh` |

All `app.py` route functions named below are nested inside `create_app()`; `ast.walk` + `ast.FunctionDef` still finds them by name (the existing `test_guide_routes_contract.py` relies on this), and they read the module-level `OPEN_ACCESS` global without any extra wiring.

---

## Task 1: Add the `OPEN_ACCESS` flag and expose it to templates

**Files:**
- Modify: `app.py` (constant near line 260; context processor `inject_global_vars` near line 755)
- Test: `tests/test_open_access_contract.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_open_access_contract.py`:

```python
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class OpenAccessContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)

    def _function_source(self, name):
        for node in ast.walk(self.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return ast.get_source_segment(self.app_source, node)
        return ""

    def test_open_access_flag_defined_from_env(self):
        self.assertIn("PAPERQUERY_OPEN_ACCESS", self.app_source)
        self.assertRegex(
            self.app_source,
            r'OPEN_ACCESS\s*=\s*os\.environ\.get\(\s*"PAPERQUERY_OPEN_ACCESS"\s*,\s*"0"\s*\)',
            "OPEN_ACCESS must read PAPERQUERY_OPEN_ACCESS and default to \"0\"",
        )

    def test_context_processor_exposes_open_access(self):
        src = self._function_source("inject_global_vars")
        self.assertIn('"open_access": OPEN_ACCESS', src)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python3 -m unittest tests.test_open_access_contract -v`
Expected: FAIL — `test_open_access_flag_defined_from_env` (PAPERQUERY_OPEN_ACCESS not in source) and `test_context_processor_exposes_open_access` (key not in return dict).

- [ ] **Step 3: Add the `OPEN_ACCESS` constant**

In `app.py`, immediately after the line:
```python
SESSION_TIMEOUT_SECONDS = int(os.environ.get("PAPERQUERY_SESSION_TIMEOUT", "3600"))
```
add:
```python
OPEN_ACCESS = os.environ.get("PAPERQUERY_OPEN_ACCESS", "0").strip().lower() in ("1", "true", "yes", "on")
```

- [ ] **Step 4: Expose it in the context processor**

In `app.py`, in the `inject_global_vars` context processor, change the return dict from:
```python
        return {
            "current_year": datetime.utcnow().year,
            "site_name": "Keydion",
            "ms_enabled": is_ms_configured(),
        }
```
to:
```python
        return {
            "current_year": datetime.utcnow().year,
            "site_name": "Keydion",
            "ms_enabled": is_ms_configured(),
            "open_access": OPEN_ACCESS,
        }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `python3 -m unittest tests.test_open_access_contract -v`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add app.py tests/test_open_access_contract.py
git commit -m "feat(auth): add PAPERQUERY_OPEN_ACCESS flag exposed to templates"
```

---

## Task 2: Make the viewing routes open-access aware

**Files:**
- Modify: `app.py` — `preview_paper` (line 2135), `paper_file` (lines 2202-2204), `download` (lines 2212-2214)
- Test: `tests/test_open_access_contract.py` (append)

- [ ] **Step 1: Write the failing tests**

Append these methods to `OpenAccessContractTest` in `tests/test_open_access_contract.py`:

```python
    def test_paper_file_gate_is_conditional(self):
        src = self._function_source("paper_file")
        self.assertIn("if not OPEN_ACCESS:", src)
        self.assertIn("require_login()", src)

    def test_download_gate_is_conditional(self):
        src = self._function_source("download")
        self.assertIn("if not OPEN_ACCESS:", src)
        self.assertIn("require_login()", src)

    def test_preview_serves_full_pdf_when_open_access(self):
        src = self._function_source("preview_paper")
        self.assertIn("not is_guest or OPEN_ACCESS", src)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest tests.test_open_access_contract -v`
Expected: FAIL — the three new tests (`OPEN_ACCESS` / `if not OPEN_ACCESS:` not yet in those functions).

- [ ] **Step 3: Update `preview_paper`**

In `app.py`, change the `pdf_url` line in `preview_paper` from:
```python
        pdf_url = url_for("paper_preview", filename=filename) if is_guest else url_for("paper_file", filename=filename)
```
to:
```python
        pdf_url = url_for("paper_file", filename=filename) if (not is_guest or OPEN_ACCESS) else url_for("paper_preview", filename=filename)
```

- [ ] **Step 4: Update `paper_file`**

In `app.py`, change the body of `paper_file` from:
```python
    @app.route("/papers/raw/<path:filename>")
    def paper_file(filename: str):
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        pdf_path = PAPERS_DIR / filename
        if not pdf_path.exists():
            abort(404)
        return send_from_directory(PAPERS_DIR, filename, as_attachment=False)
```
to:
```python
    @app.route("/papers/raw/<path:filename>")
    def paper_file(filename: str):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        pdf_path = PAPERS_DIR / filename
        if not pdf_path.exists():
            abort(404)
        return send_from_directory(PAPERS_DIR, filename, as_attachment=False)
```

- [ ] **Step 5: Update `download`**

In `app.py`, change the body of `download` from:
```python
    @app.route("/papers/<path:filename>")
    def download(filename: str):
        user = require_login()
        if not user:
            return redirect(url_for("login"))
        return send_from_directory(PAPERS_DIR, filename, as_attachment=True)
```
to:
```python
    @app.route("/papers/<path:filename>")
    def download(filename: str):
        if not OPEN_ACCESS:
            user = require_login()
            if not user:
                return redirect(url_for("login"))
        return send_from_directory(PAPERS_DIR, filename, as_attachment=True)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python3 -m unittest tests.test_open_access_contract -v`
Expected: PASS (5 tests total).

- [ ] **Step 7: Commit**

```bash
git add app.py tests/test_open_access_contract.py
git commit -m "feat(auth): skip login on paper view/download when open access is on"
```

---

## Task 3: Gate the preview and search UI on `open_access`

**Files:**
- Modify: `templates/preview.html` (the two `{% if is_guest %}` blocks at lines ~20 and ~86), `templates/search.html` (the `{% if is_guest %}` block at line ~239)
- Test: `tests/test_open_access_contract.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_open_access_contract.py`. Add this import at the top of the file (next to the existing imports):

```python
from jinja2 import Environment, FileSystemLoader
from types import SimpleNamespace
```

Then add these methods to `OpenAccessContractTest`:

```python
    def test_preview_template_gates_buttons_and_banner(self):
        text = (ROOT / "templates" / "preview.html").read_text(encoding="utf-8")
        self.assertEqual(
            text.count("{% if is_guest and not open_access %}"),
            2,
            "preview.html must gate both the button row and the banner on open_access",
        )
        self.assertNotIn("{% if is_guest %}", text)

    def test_search_template_gates_download(self):
        text = (ROOT / "templates" / "search.html").read_text(encoding="utf-8")
        self.assertIn("{% if is_guest and not open_access %}", text)
        self.assertNotIn("{% if is_guest %}", text)

    def _render_search(self, is_guest, open_access):
        env = Environment(loader=FileSystemLoader(ROOT / "templates"))
        template = env.get_template("search.html")
        records = [{
            "filename": "p.pdf", "title": "T", "category": "History",
            "author_name": "Jane", "author_school": "S",
            "published_at": "2026-05-21", "abstract": "", "is_ib_sample": "",
        }]
        return template.render(
            _=lambda value, **kwargs: value % kwargs if kwargs else value,
            ngettext=lambda s, p, n, **k: s if n == 1 else p,
            url_for=lambda endpoint, **kwargs: f"/{endpoint}",
            get_flashed_messages=lambda with_categories=False: [],
            request=SimpleNamespace(full_path="/search", args={}),
            session={}, current_locale="en", current_year=2026, ms_enabled=False,
            user=None, query="", category_filter="", language_filter="",
            date_filter="", paper_type_filter="", ee_subject_filter="",
            cp_context_filter="", ee_subjects_list=[], cp_contexts=[],
            filtered=False, records=records,
            pagination=SimpleNamespace(page=1, pages=1, has_prev=False, has_next=False),
            is_guest=is_guest, open_access=open_access, total_matches=1,
            paper_categories=[], journal_id_map={},
        )

    def test_search_shows_download_for_guest_when_open(self):
        html = self._render_search(is_guest=True, open_access=True)
        self.assertNotIn("Sign in to Download", html)
        self.assertIn("/download", html)

    def test_search_shows_signin_for_guest_when_closed(self):
        html = self._render_search(is_guest=True, open_access=False)
        self.assertIn("Sign in to Download", html)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m unittest tests.test_open_access_contract -v`
Expected: FAIL — templates still contain `{% if is_guest %}`; the guest-when-open render still shows "Sign in to Download".

- [ ] **Step 3: Update `preview.html`**

In `templates/preview.html`, replace both occurrences of:
```
{% if is_guest %}
```
with:
```
{% if is_guest and not open_access %}
```
(There are exactly two: the button row at line ~20 and the truncation banner at line ~86. Use a replace-all on the exact string `{% if is_guest %}`.)

- [ ] **Step 4: Update `search.html`**

In `templates/search.html`, replace the single occurrence of:
```
{% if is_guest %}
```
with:
```
{% if is_guest and not open_access %}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python3 -m unittest tests.test_open_access_contract -v`
Expected: PASS (9 tests total).

- [ ] **Step 6: Commit**

```bash
git add templates/preview.html templates/search.html tests/test_open_access_contract.py
git commit -m "feat(auth): show full-access paper UI to guests when open access is on"
```

---

## Task 4: Enable the flag in the env files (local, not committed)

**Files:**
- Modify: `.env` (gitignored), `.env.prod` (gitignored)

No tests — these files are gitignored and never committed. Both contain secrets; only **append** the new line, do not alter existing values.

- [ ] **Step 1: Add the flag to `.env`**

Read `.env`, then append a new line at the end:
```
PAPERQUERY_OPEN_ACCESS=1
```

- [ ] **Step 2: Add the flag to `.env.prod`**

Read `.env.prod`, then append a new line at the end:
```
PAPERQUERY_OPEN_ACCESS=1
```

- [ ] **Step 3: Confirm git ignores them**

Run: `git status --porcelain .env .env.prod`
Expected: no output (both ignored — nothing staged or shown).

- [ ] **Step 4: No commit**

Nothing to commit for this task (both files are gitignored). Do not force-add them.

---

## Task 5: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass (the prior 178 plus the 9 new ones), no failures or errors.

- [ ] **Step 2: Manual smoke test — open access ON**

Run the dev server with the flag on:
```bash
PAPERQUERY_OPEN_ACCESS=1 ./start_local.sh
```
In a fresh browser (no login / incognito):
- Open a paper's preview page → the embedded PDF shows the **full** document (not just 2 pages), and the Download + "Open in new tab" buttons are visible (no "Sign in to view full papers" banner).
- Click Download → the PDF downloads without a login redirect.
- Click "Open in new tab" → the raw PDF opens without a login redirect.
- On the search results page → each result shows a "Download" link (not "Sign in to Download").
Stop the server.

- [ ] **Step 3: Manual smoke test — open access OFF (regression)**

Run the dev server with the flag off:
```bash
PAPERQUERY_OPEN_ACCESS=0 ./start_local.sh
```
In a fresh browser (no login):
- Preview page shows the truncation banner and "Sign in to download"; the embedded PDF is the 2-page preview.
- Visiting `/papers/<a-real-filename>.pdf` redirects to login.
Stop the server.

- [ ] **Step 4: Confirm login still works for gated actions**

With the flag ON, confirm `/dashboard/upload` and `/dashboard/review` still redirect un-logged-in visitors to login (these are intentionally still gated), and the navbar "Sign in" button is present.

---

## Self-Review

**Spec coverage:**
- Flag (spec §1) → Task 1.
- Backend gates: `preview_paper`, `paper_file`, `download` (spec §2) → Task 2.
- Template gating: `preview.html` ×2, `search.html` (spec §3) → Task 3.
- Env files `.env` + `.env.prod` (spec §4) → Task 4.
- "What stays gated" + navbar login (spec §5) → verified in Task 5 Step 4.
- Testing (spec §6): existing suite + new contract tests → Tasks 1–3 add tests, Task 5 runs the full suite.

**Placeholder scan:** No TBD/TODO/"handle edge cases"; every code step shows the full before/after code or exact line to add.

**Type / name consistency:** `OPEN_ACCESS` (module global) and `open_access` (template var) are used consistently across all tasks. Route names (`preview_paper`, `paper_file`, `download`, `inject_global_vars`) match `app.py`. The template gate string `{% if is_guest and not open_access %}` is identical everywhere it appears.
