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

# Start dev container (Flask debug on :4000, MySQL expected on the host at 127.0.0.1:3306)
docker-compose up -d

# Run all tests (~594 contract tests, ~5s — requires a reachable MySQL 9.x, see Testing approach)
python3 -m unittest discover -s tests -p "test_*.py" -v

# Run a single test file
python3 -m unittest tests/test_ee_total_grade_contract.py -v

# Compile translations after editing .po files (restart dev server to pick up new .mo)
python3 tools/compile_translations.py

# Backfill / rebuild RAG chunk embeddings for published papers
python3 tools/build_embeddings.py

# One-time migration: backfill papers_chunks JSON embeddings into the VECTOR column
python3 tools/migrate_chunk_vectors.py            # then --drop-json after verifying

# Manage users (CLI)
python3 tools/manage_passwords.py set --username <name> --password <pw> --role 3
python3 tools/manage_passwords.py list
```

**Dev container gotcha**: `docker-compose.yml` bind-mounts only `app.py`, `config.py`, `db.py`, `models.py`, `routes/`, `services/`, `library_tools.py`, `ee_pdf_extractor.py`, `templates/`, `static/`, `data/`, `translations/`, `papers/`. Changes to the other Python modules (`llm_client.py`, `rag_index.py`, `web_search.py`, `llm_metadata.py`, `pdf_text.py`) and to `tests/` need an image rebuild — don't trust test runs inside the container. Single-file bind mounts track the inode, so if a tool rewrites `app.py` the container may serve a stale copy — `docker restart keydion-web` fixes it.

Environment variables: see `.env.example` for the full annotated list. The important ones:
- `PAPERQUERY_SECRET` — Flask secret key
- `PAPERQUERY_DATABASE_URL` — SQLAlchemy connection string (MySQL)
- `PAPERQUERY_MS_CLIENT_ID` / `PAPERQUERY_MS_CLIENT_SECRET` / `PAPERQUERY_MS_REDIRECT_URI` — MS OAuth
- `LLM_API_KEY` / `LLM_BASE_URL` — OpenAI-compatible chat provider; **empty key disables all AI features**
- `LLM_DEFAULT_FLASH` / `LLM_DEFAULT_THINK` — model tiers (cheap/fast vs. reasoning)
- `LLM_EMBED_API_KEY` / `LLM_EMBED_BASE_URL` / `LLM_EMBED_MODEL` — separate embedding provider (defaults to Gemini's OpenAI-compatible endpoint; falls back to chat credentials when unset)
- `WEB_SEARCH_PROVIDER` / `WEB_SEARCH_API_KEY` — Ask-the-Library web access (Tavily default); empty key hides the web toggle
- `PAPERQUERY_DATA_DIR` / `PAPERQUERY_UPLOAD_DIR` / `PAPERQUERY_RESOURCES_DIR` — path overrides

## Architecture

**App factory core + domain packages** — `app.py` (~800 lines) is the factory core: `create_app()`, context processors and template filters, the auth/account/profile/dashboard/admin-users/category/EE-subject routes, legacy redirects, and a back-compat re-export block so `from app import X` still works for moved names. `app = create_app()` runs at module level and calls `init_db()`, so **importing `app` connects to MySQL** — this affects tests and CLI scripts (the extracted modules below all import standalone without a DB). The rest of the application is split into:
- `config.py` — env loading + constants; **must be imported before reading `os.environ`**
- `db.py` — engine/session setup; `get_engine()` is what gunicorn `post_fork` uses
- `models.py` — the 13 ORM classes + `init_db()`
- `routes/<domain>.py` — HTTP routes per domain (resources, guides, news, journals, upload, submissions, ask, papers), each exposing `register_routes(app)`; `routes/shared.py` holds cross-domain HTTP helpers; endpoint names are unchanged from the monolith — these are **not** blueprints
- `services/<domain>.py` — domain logic (DB/storage helpers)
- Hard rule: `routes/` and `services/` modules never import `app` (enforced by `tests/test_split_imports_contract.py`)

Self-contained concerns remain factored into satellite modules:
- `ee_pdf_extractor.py` — PDF parser for IB EE metadata auto-fill
- `pdf_text.py` — shared PDF→text extraction (PyPDF2 first, Tesseract OCR fallback for scanned PDFs)
- `llm_client.py` — central LLM client + model resolution (flash/think tiers, separate embedding provider)
- `llm_metadata.py` — abstract + keyword drafting from a paper PDF
- `rag_index.py` — RAG index: chunking, embeddings stored in MySQL 9 binary `VECTOR` columns (`papers_chunks.embedding_vec`), numpy cosine (normalized mat-vec) over a per-process snapshot that auto-refreshes when the `rag_index_meta.chunks_version` stamp moves (any process's write invalidates all gunicorn workers within one request)
- `library_tools.py` — tool-calling core for Ask-the-Library agentic mode (tool schemas + dispatch)
- `web_search.py` — pluggable web search for Ask-the-Library (disabled when unconfigured)

**LLM features** (all degrade gracefully when `LLM_API_KEY` is unset): Ask-the-Library RAG chat at `/ask` + `/api/ask` (conversations, citations, PDF attachments, optional agentic web/document tools), semantic search + semantic "related papers", abstract/keyword auto-fill (`/api/upload/generate-abstract-keywords`), and EE metadata extraction (`/api/upload/extract-ee-metadata`). The idea backlog and implementation status live in `LLM_DEPLOYMENT_IDEAS.md`.

**Dashboard URL nesting** — authenticated admin routes live under `/dashboard/...` (e.g. `/dashboard/admin/users`, `/dashboard/admin/guides`). Bare `/admin/*` paths exist only as 301-redirect legacy endpoints. Enforced by `test_dashboard_url_nesting_contract.py`.

**Partial rendering** — most dashboard templates start with `{% extends "_bare.html" if partial else "_dashboard_shell.html" %}`. Sidebar nav links carry `data-partial-href` so client JS fetches `?partial=1` and swaps the panel without a full page reload. Routes pass `partial=request.args.get("partial")` to `render_template`.

**Data layer** — MySQL via SQLAlchemy ORM with models defined at module level in `models.py`:
- `LocalUser` / `MsUser` — local password auth and Microsoft Graph OAuth users
- `PaperMetadataModel` — published papers (JSON fields stored as text: `ib_ee_data`, `cp_data`)
- `PaperChunkModel` — RAG chunk embeddings per published paper (vectors stored as JSON text)
- `ConversationModel` / `ChatMessageModel` — Ask-the-Library chat history
- `AttachmentChunkModel` — embeddings for per-conversation uploaded attachments
- `SubmissionModel` — user-submitted papers pending review
- `NewsArticleModel` — news/articles with block-based body (JSON array of text/image blocks)
- `GuideModel` — published guides
- `JournalModel` — academic journals
- `ResourceNode` — Academic Resources folder/file tree (slug-based public URLs)
- `SessionModel` — server-side session tokens

**Roles** (stored as int in `role` column): 1 = Reader, 2 = Contributor (can upload), 3 = Curator/Admin. Enforced via `require_login(level)`.

**Paper types** — three mutually exclusive categories: independent papers, IB Extended Essay (EE, `is_ib_ee` flag + `ib_ee_data` JSON), and IB Community Project (CP, `is_cp_paper` flag + `cp_data` JSON). Legacy IB sample papers identified by `author_name == "IB SAMPLE"`.

**i18n** — Flask-Babel with `en`/`zh` locales. Translation catalogs in `translations/<locale>/LC_MESSAGES/messages.po`. The `_()` gettext function and `_l()` lazy_gettext are used throughout `app.py`. All user-facing LLM output must be bilingual too.

**Auth** — dual system: local PBKDF2-hashed passwords and MS Graph OAuth via the `msal` library. Session tokens are stored server-side in the `sessions` table with a configurable timeout.

## Key directories

| Path | Purpose |
|------|---------|
| `app.py` | App factory core: create_app, auth/admin routes, back-compat re-exports |
| `routes/` | Domain HTTP routes — `register_routes(app)` per domain, endpoint-preserving |
| `services/` | Domain logic — DB/storage helpers per domain; never imports `app` |
| `ee_pdf_extractor.py`, `pdf_text.py` | PDF parsing/extraction modules |
| `llm_client.py`, `llm_metadata.py`, `rag_index.py`, `library_tools.py`, `web_search.py` | LLM/RAG layer (see Architecture) |
| `templates/` | Jinja2 templates (~38 files; dashboard panels in `templates/_dashboard/`) |
| `static/css/` | Per-page stylesheets (`styles.css`, `dashboard.css`, `ask.css`, `guides.css`, `manage.css`, `resources.css`, `upload.css`) — no build step |
| `static/js/` | Per-page scripts (`ask.js`, `dashboard.js`, `guides-editor.js`, `upload-wizard.js`) |
| `static/vendor/` | Bootstrap CSS/JS (manually vendored) |
| `data/` | JSON configs (paper/news/guide categories, EE subjects); runtime `pending_papers/` |
| `papers/` | Uploaded PDF storage (gitignored) |
| `resource_files/` | Academic Resources file storage |
| `tools/` | CLI scripts: user management, translation compilation, embedding backfill |
| `tests/` | Contract tests using `unittest` — parse app.py with AST + render Jinja2 templates |
| `deploy/keydion.nginx.conf` | Production nginx config (host-managed, not docker) |
| `docs/superpowers/` | Local spec/plan docs — gitignored on purpose, never commit |

## Testing approach

Tests are **contract tests**, not integration tests. They locate source via `tests/support.py` (`find_function` / `source_of` / `all_sources`, searching `app.py` + `routes/` + `services/`), parse it with Python's `ast` module, and render Jinja2 templates with mock data to verify structural invariants:
- DOM contracts (element IDs match between HTML and JS `getElementById` calls)
- Data round-trip contracts (fields are carried through load/write functions)
- Server-side logic contracts (EE total grade is calculated server-side, not trusted from the form)

20 of the 55 test files `import app` (directly or via `from app import ...`), which connects to MySQL at import time — **the full suite needs a reachable database**; without one those modules fail at import with `OperationalError`. This caveat applies to `import app` only — the extracted modules (`config`, `db`, `models`, `routes/*`, `services/*`) import without a DB. The remaining files are pure AST/template tests and run standalone.

Conventional commits (`feat:`, `fix:`, with optional scope like `fix(i18n):`) are used.

## Noteworthy patterns

- **EE total grade** is computed server-side in `build_ib_ee_data_from_form()` — the form field `ibTotalGradeNumber` is `readonly` and its submitted value is ignored
- **Legacy IB sample papers** (`author_name == "IB SAMPLE"`) hide the school field in search results and maintain backward-compat logic scattered across templates and routes
- **News body** supports a JSON block format: `[{"type": "text", "content": "..."}, {"type": "image", "url": "...", "caption": "..."}]`, with a `parse_body_blocks` template filter for backward compat with plain-text bodies
- **RAG lifecycle**: papers are chunked + embedded on publish and purged on delete (`rag_index.py`); chunk vectors live in a binary `VECTOR(3072)` column (**requires MySQL 9.x**; dimension from `RAG_EMBED_DIM`). Every store write bumps `rag_index_meta.chunks_version` in the same transaction; each worker's in-memory numpy snapshot is stamp-checked per query, so re-embeds/purges propagate to all workers without restarts. One-time JSON→VECTOR backfill: `tools/migrate_chunk_vectors.py` (then `--drop-json`).
- DB migrations are ad-hoc in `init_db()` — ALTER TABLE statements wrapped in try/except for idempotency
- Paper metadata is both in the DB (`papers_metadata` table) and optionally in the filesystem (`data/paper_metadata.json`); routes read from DB via `_load_papers()` which queries `PaperMetadataModel`
- **Production deploy** uses host-managed nginx + systemd gunicorn (`gunicorn.conf.py` + `run_prod.sh`). The bundled `docker-compose.prod.yml` exists but is **not** what runs in prod — don't propose Docker-based deploy fixes.
- **Translation cache**: Flask-Babel loads `.mo` files at startup. After `tools/compile_translations.py`, the dev server must be restarted for new translations to appear.
- **Split layout invariants** — endpoint names are a cross-module contract (`url_for("preview_paper")` is called from the ask domain); `routes/` and `services/` modules must not import `app`; new code goes in `services/<domain>` + `routes/<domain>`, not `app.py`.
