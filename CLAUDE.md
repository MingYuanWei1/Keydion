# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Build & Run

```bash
# Install dependencies (use python3/pip3 on macOS — there is no `python` binary)
pip3 install -r requirements.txt

# Start dev server (macOS/Linux)
./start_local.sh

# Start with Docker Compose
docker-compose up -d

# Run all tests (178 contract tests, ~4s)
python3 -m unittest discover -s tests -p "test_*.py" -v

# Run a single test file
python3 -m unittest tests/test_ee_total_grade_contract.py -v

# Compile translations after editing .po files (restart dev server to pick up new .mo)
python3 tools/compile_translations.py

# Manage users (CLI)
python3 tools/manage_passwords.py set --username <name> --password <pw> --role 3
python3 tools/manage_passwords.py list
```

Environment variables (set via `.env` or shell):
- `PAPERQUERY_SECRET` — Flask secret key
- `PAPERQUERY_DATABASE_URL` — SQLAlchemy connection string (MySQL)
- `PAPERQUERY_DATA_DIR` / `PAPERQUERY_UPLOAD_DIR` — override data/papers paths

## Architecture

**Single-file Flask app** — all routes, models, and business logic live in `app.py` (~4,400 lines). The app is created via `create_app()` which sets up Flask, Babel (i18n), and SQLAlchemy. EE PDF parsing is the one exception, factored out into `ee_pdf_extractor.py`.

**Dashboard URL nesting** — authenticated admin routes live under `/dashboard/...` (e.g. `/dashboard/admin/users`, `/dashboard/admin/guides`). Bare `/admin/*` paths exist only as 301-redirect legacy endpoints. Enforced by `test_dashboard_url_nesting_contract.py`.

**Partial rendering** — most dashboard templates start with `{% extends "_bare.html" if partial else "_dashboard_shell.html" %}`. Sidebar nav links carry `data-partial-href` so client JS fetches `?partial=1` and swaps the panel without a full page reload. Routes pass `partial=request.args.get("partial")` to `render_template`.

**Data layer** — MySQL via SQLAlchemy ORM with models defined at module level in `app.py`:
- `LocalUser` / `MsUser` — local password auth and Microsoft Graph OAuth users
- `PaperMetadataModel` — published papers (JSON fields stored as text: `ib_ee_data`, `cp_data`)
- `SubmissionModel` — user-submitted papers pending review
- `NewsArticleModel` — news/articles with block-based body (JSON array of text/image blocks)
- `JournalModel` — academic journals
- `SessionModel` — server-side session tokens

**Roles** (stored as int in `role` column): 1 = Reader, 2 = Contributor (can upload), 3 = Curator/Admin. Enforced via `require_login(level)`.

**Paper types** — three mutually exclusive categories: independent papers, IB Extended Essay (EE, `is_ib_ee` flag + `ib_ee_data` JSON), and IB Community Project (CP, `is_cp_paper` flag + `cp_data` JSON). Legacy IB sample papers identified by `author_name == "IB SAMPLE"`.

**i18n** — Flask-Babel with `en`/`zh` locales. Translation catalogs in `translations/<locale>/LC_MESSAGES/messages.po`. The `_()` gettext function and `_l()` lazy_gettext are used throughout `app.py`.

**Auth** — dual system: local PBKDF2-hashed passwords and MS Graph OAuth via the `msal` library. Session tokens are stored server-side in the `sessions` table with a configurable timeout.

## Key directories

| Path | Purpose |
|------|---------|
| `app.py` | Entire application: models, routes, auth, helpers |
| `ee_pdf_extractor.py` | Standalone PDF parser for IB EE auto-fill (imported by `app.py`) |
| `templates/` | Jinja2 templates (~36 files) |
| `static/css/` | Per-page stylesheets (`styles.css`, `dashboard.css`, `guides.css`, `manage.css`, `upload.css`) — no build step |
| `static/vendor/` | Bootstrap CSS/JS (manually vendored) |
| `data/` | JSON configs for categories, EE subjects, guide categories; runtime `pending_papers/` |
| `papers/` | Uploaded PDF storage (gitignored) |
| `tools/` | CLI scripts: user management, translation compilation |
| `tests/` | Contract tests using `unittest` — parse app.py with AST + render Jinja2 templates |
| `deploy/keydion.nginx.conf` | Production nginx config (host-managed, not docker) |

## Testing approach

Tests are **contract tests**, not integration tests. They parse `app.py` with Python's `ast` module and render Jinja2 templates with mock data to verify structural invariants:
- DOM contracts (element IDs match between HTML and JS `getElementById` calls)
- Data round-trip contracts (fields are carried through load/write functions)
- Server-side logic contracts (EE total grade is calculated server-side, not trusted from the form)

Conventional commits (`feat:`, `fix:`, with optional scope like `fix(i18n):`) are used.

## Noteworthy patterns

- **EE total grade** is computed server-side in `build_ib_ee_data_from_form()` — the form field `ibTotalGradeNumber` is `readonly` and its submitted value is ignored
- **Legacy IB sample papers** (`author_name == "IB SAMPLE"`) hide the school field in search results and maintain backward-compat logic scattered across templates and routes
- **News body** supports a JSON block format: `[{"type": "text", "content": "..."}, {"type": "image", "url": "...", "caption": "..."}]`, with a `parse_body_blocks` template filter for backward compat with plain-text bodies
- DB migrations are ad-hoc in `init_db()` — ALTER TABLE statements wrapped in try/except for idempotency
- Paper metadata is both in the DB (`papers_metadata` table) and optionally in the filesystem (`data/paper_metadata.json`); routes read from DB via `_load_papers()` which queries `PaperMetadataModel`
- **Production deploy** uses host-managed nginx + systemd gunicorn (`gunicorn.conf.py` + `run_prod.sh`). The bundled `docker-compose.prod.yml` exists but is **not** what runs in prod — don't propose Docker-based deploy fixes.
- **Translation cache**: Flask-Babel loads `.mo` files at startup. After `tools/compile_translations.py`, the dev server must be restarted for new translations to appear.
