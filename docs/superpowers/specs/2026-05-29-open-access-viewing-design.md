# Login-free paper viewing via `PAPERQUERY_OPEN_ACCESS`

**Date:** 2026-05-29
**Status:** Approved for planning

## Goal

Let visitors view, download, and open the raw file of papers **without logging in**, matching what a logged-in Reader (role 1) gets today. Controlled by a single environment flag so the behavior is reversible and the committed default stays secure. Implemented on `main` (no separate branch) and turned on through the env files.

## Scope

**In scope (login-free when the flag is on):**
- Full PDF preview in the preview page (not the 2-page truncated preview).
- Download (`/papers/<filename>`).
- Open raw file in a new tab (`/papers/raw/<filename>`).
- The preview-page and search-result UI that currently gates these behind "Sign in".

**Out of scope (still requires login):**
- Upload (`/dashboard/upload`), review queue, all admin / user / guide / paper-category management, EE-extract route, news editing — every existing `require_login(level=…)` call other than the two viewing routes below stays untouched.
- The navbar "Sign in" button remains so admins can still log in to reach the gated functions.

## Background — current mechanism

Browsing is already open to guests (homepage, search, and the preview *page* all use `get_active_user()`), so the only things that gate un-logged-in visitors today are:

1. `preview_paper` (`app.py:2135`) picks the PDF URL: guests get `paper_preview` (2-page truncated via `build_preview_pdf`), logged-in users get `paper_file` (full raw).
2. `paper_file` `/papers/raw/<filename>` (`app.py:2202`) calls `require_login()`.
3. `download` `/papers/<filename>` (`app.py:2212`) calls `require_login()`.
4. `preview.html:20` shows a "Sign in to download" button (guest) vs Download + "Open in new tab" (logged-in); `preview.html:86` shows a "Guest preview is limited to the first two pages" banner.
5. `search.html:239` shows "Sign in to Download" (guest) vs "Download" (logged-in).

No existing contract test asserts these specific gates. The `require_login` references in tests cover admin guide routes (level 3) and the EE-extract route (level 2), none of which change.

## Design

### 1. The flag

Add a module-level constant in `app.py`, alongside the other config reads (near `SESSION_TIMEOUT_SECONDS`, ~line 260):

```python
OPEN_ACCESS = os.environ.get("PAPERQUERY_OPEN_ACCESS", "0").strip().lower() in ("1", "true", "yes", "on")
```

- Default `"0"` (off): the committed `main` code keeps today's login-required behavior.
- Truthy values accepted: `1`, `true`, `yes`, `on` (case-insensitive).
- `app.py` already calls `load_dotenv()` at import, and `load_dotenv` does not override variables already present in the environment — so a value set in `.env.prod` (sourced by `run_prod.sh`) wins in production.

Expose it to all templates by adding one key to the existing `inject_global_vars` context processor (`app.py:755`):

```python
return {
    "current_year": datetime.utcnow().year,
    "site_name": "Keydion",
    "ms_enabled": is_ms_configured(),
    "open_access": OPEN_ACCESS,
}
```

### 2. Backend gate changes (`app.py`)

- **`preview_paper` (line 2135).** Serve the full PDF to everyone when open access is on:
  ```python
  pdf_url = url_for("paper_file", filename=filename) if (not is_guest or OPEN_ACCESS) else url_for("paper_preview", filename=filename)
  ```
- **`paper_file` `/papers/raw/<filename>` (line 2202).** Skip the login gate when open access is on:
  ```python
  if not OPEN_ACCESS:
      user = require_login()
      if not user:
          return redirect(url_for("login"))
  ```
- **`download` `/papers/<filename>` (line 2212).** Same conditional wrap around its `require_login()` gate.

### 3. Template changes

- **`preview.html:20`** (Download / "Open in new tab" buttons) and **`preview.html:86`** (guest "limited to first two pages" banner): change `{% if is_guest %}` → `{% if is_guest and not open_access %}` so guests get the full-access buttons and no limitation banner when the flag is on.
- **`search.html:239`** (per-result "Sign in to Download" vs "Download"): same `{% if is_guest and not open_access %}` change.

No new translatable strings are introduced; the "Download" and "Open in new tab" strings already exist in the catalogs.

### 4. Environment files (local only — gitignored, not committed)

Add the flag to both env files so the running deployments turn the feature on:

- **`.env`** (dev / `start_local.sh`): add `PAPERQUERY_OPEN_ACCESS=1`.
- **`.env.prod`** (prod / sourced by `run_prod.sh`): add `PAPERQUERY_OPEN_ACCESS=1`.

Both files are listed in `.gitignore`, so these edits stay out of version control; the committed code default remains off.

## What stays the same

- The 2-page truncated preview route (`paper_preview`) and `build_preview_pdf` remain in place for when the flag is off.
- All other `require_login(level=…)` gates are untouched.
- The navbar login affordance (`base.html`) is unchanged.

## Testing

- Run the existing suite (`python3 -m unittest discover -s tests -p "test_*.py"`) — all 178 should still pass, since no test asserts the viewing-route gates.
- Add a small contract test (AST/source-based, matching the repo's style) asserting:
  - `paper_file` and `download` function sources reference `OPEN_ACCESS` (the gate is conditional, not unconditional).
  - `preview.html` and `search.html` reference `open_access`.

## Risks / notes

- This makes every published paper's full PDF and download fully reachable by anyone who can reach the deployment when the flag is on. That is the intended behavior for the internal/open deployment; the secure default and env-only enablement keep `main` and any other deployment locked down unless explicitly turned on.
- Author emails are already shown to guests on the preview page today, so no new personal-data exposure is introduced by this change.
