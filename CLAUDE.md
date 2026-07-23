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
pip3 install --require-hashes -r requirements.lock

# Start dev server (macOS/Linux). start_local.sh is gitignored (local-only); raw equivalent:
PAPERQUERY_ALLOW_DEV_SECRET=1 PAPERQUERY_COOKIE_SECURE=0 python3 -m flask --app wsgi run --debug --port 4000

# Start dev container (Flask debug on :4000, MySQL expected on the host at 127.0.0.1:3306).
# docker-compose.yml is gitignored (local-only); the tracked prod stack is docker-compose.prod.yml.
docker-compose up -d

# Run all tests (~820 contract tests, ~2min — requires a reachable MySQL 9.x, see Testing approach)
python3 -m unittest discover -s tests -p "test_*.py" -v

# Run a single test file
python3 -m unittest tests/test_ee_total_grade_contract.py -v

# Sync translations after changing any _()/_l() source string (then restart dev server for new .mo):
python3 tools/extract_translations.py   # source -> messages.pot
python3 tools/update_translations.py    # merge .pot into each .po: keeps existing translations, adds new as empty, marks changed #, fuzzy
python3 tools/compile_translations.py   # .po -> .mo
# (If you only hand-edited .po msgstr values, just run compile_translations.py.)

# Backfill / rebuild RAG chunk embeddings for published papers
python3 tools/build_embeddings.py

# One-time migration: backfill papers_chunks JSON embeddings into the VECTOR column
python3 tools/migrate_chunk_vectors.py            # then --drop-json after verifying

# Manage users (CLI)
python3 tools/manage_passwords.py set --username <name> --password <pw> --role 3
python3 tools/manage_passwords.py list
```

**Dev container gotcha** (the dev `docker-compose.yml` is gitignored / local-only): `docker-compose.yml` bind-mounts only `app.py`, `config.py`, `db.py`, `models.py`, `routes/`, `services/`, `library_tools.py`, `ee_pdf_extractor.py`, `templates/`, `static/`, `data/`, `translations/`, `papers/`. Changes to the other Python modules (`llm_client.py`, `rag_index.py`, `web_search.py`, `llm_metadata.py`, `pdf_text.py`) and to `tests/` need an image rebuild — don't trust test runs inside the container. Single-file bind mounts track the inode, so if a tool rewrites `app.py` the container may serve a stale copy — `docker restart keydion-web` fixes it.

Environment variables: see `.env.example` for the full annotated list. **Gotcha:** `config.py` loads `.env.prod` in preference to `.env` when both exist — so locally the *prod* file is usually the active one. The important ones:
- `PAPERQUERY_SECRET` — Flask secret key; **`create_app()` refuses to boot if unset or `dev-secret-key`** unless `PAPERQUERY_ALLOW_DEV_SECRET=1` (SEC-09)
- `PAPERQUERY_ALLOW_DEV_SECRET` — dev-only opt-in to the insecure default secret
- `PAPERQUERY_COOKIE_SECURE` — `Secure` flag on the session cookie (default `1`; set `0` for plain-HTTP dev)
- `PAPERQUERY_DATABASE_URL` — SQLAlchemy connection string (MySQL)
- `PAPERQUERY_MS_CLIENT_ID` / `PAPERQUERY_MS_CLIENT_SECRET` / `PAPERQUERY_MS_REDIRECT_URI` — MS OAuth
- `LLM_API_KEY` / `LLM_BASE_URL` — OpenAI-compatible chat provider; **empty key disables all AI features**
- `LLM_DEFAULT_FLASH` / `LLM_DEFAULT_THINK` — model tiers (cheap/fast vs. reasoning)
- `LLM_EMBED_API_KEY` / `LLM_EMBED_BASE_URL` / `LLM_EMBED_MODEL` — separate embedding provider (defaults to Gemini's OpenAI-compatible endpoint; falls back to chat credentials when unset)
- `LLM_VISION` / `LLM_VISION_API_KEY` / `LLM_VISION_BASE_URL` — separate **vision** (multimodal) provider for reading rendered PDF pages; falls back to chat credentials when the `*_VISION_*` values are unset. Empty `LLM_VISION` ⇒ `vision_enabled()` is false and everything uses the legacy OCR/text path
- `WEB_SEARCH_PROVIDER` / `WEB_SEARCH_API_KEY` — Ask-the-Library web access (Tavily default); empty key hides the web toggle
- `PAPERQUERY_DATA_DIR` / `PAPERQUERY_UPLOAD_DIR` / `PAPERQUERY_RESOURCES_DIR` — path overrides

## Architecture

**App factory core + domain packages** — `app.py` is the factory core: `create_app()`, context processors and template filters, the auth/account/profile/dashboard/admin-users/category/EE-subject routes, legacy redirects, and a lazy back-compat surface for older imports. Importing `app` does not construct the Flask application or connect to MySQL. `wsgi.py` is the explicit serving entry point and contains `app = create_app()`. Startup verifies the Alembic state; schema creation is available only through the confirmed empty-database bootstrap command. The rest of the application is split into:
- `config.py` — env loading + constants; **must be imported before reading `os.environ`**
- `db.py` — engine/session setup; `get_engine()` is what gunicorn `post_fork` uses
- `models.py` — ORM mappings plus explicit empty-database bootstrap and schema verification
- `routes/<domain>.py` — HTTP routes per domain (resources, guides, news, journals, upload, submissions, ask, papers), each exposing `register_routes(app)`; `routes/shared.py` holds cross-domain HTTP helpers; endpoint names are unchanged from the monolith — these are **not** blueprints
- `services/<domain>.py` — domain logic (DB/storage helpers)
- Hard rule: `routes/` and `services/` modules never import `app` (enforced by `tests/test_split_imports_contract.py`)

Self-contained concerns remain factored into satellite modules:
- `ee_pdf_extractor.py` — PDF parser for IB EE metadata auto-fill (vision-first when `vision_enabled()`, else the local regex/pdfplumber path)
- `pdf_text.py` — shared PDF→text extraction (pypdf first, Tesseract OCR fallback for scanned PDFs). Stays a **leaf module** (never imports `llm_client`); for scanned pages it calls an optional caller-**injected** `vision_fallback(file_bytes, max_pages)` callable when one is passed, else Tesseract. Also exposes `render_pdf_pages()` (PyMuPDF rasterizer → PNG bytes per page)
- `llm_client.py` — central LLM client + model resolution: **three providers** — chat (flash/think tiers, `LLM_*`), embeddings (`LLM_EMBED_*`), and vision (`LLM_VISION_*`); `vision_enabled()` gates the vision path independently of `llm_enabled()`
- `llm_metadata.py` — abstract + keyword drafting from a paper PDF (vision-first when `vision_enabled()`; OCR+text-LLM fallback)
- `vision_read.py` — vision-model PDF reading: `transcribe_pdf()` (vision-as-OCR, `""` on failure) and `extract_with_vision()` (structured `json_object` extraction over page images, raises `VisionError`). Used by the extractors and the scanned-page RAG ingestion fallback
- `rag_index.py` — RAG index: chunking, embeddings stored in MySQL 9 binary `VECTOR` columns (`papers_chunks.embedding_vec`), numpy cosine (normalized mat-vec) over a per-process snapshot that auto-refreshes when the `rag_index_meta.chunks_version` stamp moves (any process's write invalidates all gunicorn workers within one request)
- `library_tools.py` — tool-calling core for Ask-the-Library agentic mode (tool schemas + dispatch)
- `web_search.py` — pluggable web search for Ask-the-Library (disabled when unconfigured)

**LLM features** (all degrade gracefully when `LLM_API_KEY` is unset): Ask-the-Library RAG chat at `/ask` + `/api/ask` (conversations, citations, PDF attachments, optional agentic web/document tools), semantic search + semantic "related papers", abstract/keyword auto-fill (`/api/upload/generate-abstract-keywords`), EE metadata extraction (`/api/upload/extract-ee-metadata`), and IA score/comment extraction (`/api/upload/extract-ia-metadata`). The three PDF-reading extractors are **vision-first**: when `vision_enabled()` they read rendered page images via the vision model, otherwise they fall back to the legacy path (abstract/IA → OCR+text-LLM; EE → local regex/pdfplumber). The abstract/IA auto-extract buttons show when `(vision_enabled() or llm_enabled())` for a contributor (the EE button is always on); RAG ingestion transcribes **scanned** pages with the vision model when configured, else Tesseract. The idea backlog and implementation status live in `LLM_DEPLOYMENT_IDEAS.md`.

**Dashboard URL nesting** — authenticated admin routes live under `/dashboard/...` (e.g. `/dashboard/admin/users`, `/dashboard/admin/guides`). Bare `/admin/*` paths exist only as 301-redirect legacy endpoints. Enforced by `test_dashboard_url_nesting_contract.py`.

**Partial rendering** — most dashboard templates start with `{% extends "_bare.html" if partial else "_dashboard_shell.html" %}`. Sidebar nav links carry `data-partial-href`; `static/js/dashboard.js` fetches the URL with an **`X-Partial-Content: 1` header** (not a `?partial=1` query param) and swaps only `#dashboardMain`. The `partial` flag is injected **globally** by the `inject_partial_flag` context processor (`app.py`) via `is_partial_request()` (which reads that header) — so routes must **NOT** pass an explicit `partial=` to `render_template`. Doing so overrides the context processor (Flask re-applies the passed context over processor values); if it reads `request.args.get("partial")` it is always `None` on a partial fetch, so the route returns the full `_dashboard_shell.html`, which the loader nests inside the panel (a second `position:fixed` shell + sidebar). Enforced by `test_partial_flag_contract.py`.

**Data layer** — MySQL via SQLAlchemy ORM with models defined at module level in `models.py`:
- `LocalUser` / `MsUser` — local password auth and Microsoft Graph OAuth users
- `PaperMetadataModel` — published papers (JSON fields stored as text: `ib_ee_data`, `cp_data`, `ia_data`)
- `PaperChunkModel` — RAG chunk embeddings per published paper (vectors stored as JSON text)
- `ConversationModel` / `ChatMessageModel` — Ask-the-Library chat history
- `AttachmentChunkModel` — embeddings for per-conversation uploaded attachments
- `SubmissionModel` — user-submitted papers pending review
- `NewsArticleModel` — news/articles with block-based body (JSON array of text/image blocks)
- `GuideModel` — published guides
- `JournalModel` — academic journals
- `ResourceNode` — Academic Resources folder/file tree (slug-based public URLs)
- `SessionModel` — server-side session tokens

**Roles** (stored as int in `role` column): 1 = Reader, 2 = Contributor (publishes directly; Readers may submit for review), 3 = Curator/Admin. Enforced via `require_login(level)`.

**Paper types** — four mutually exclusive categories: independent papers, IB Extended Essay (EE, `is_ib_ee` flag + `ib_ee_data` JSON), IB Community Project (CP, `is_cp_paper` flag + `cp_data` JSON), and IB Internal Assessment (IA, `is_ia` flag + `ia_data` JSON). Like EE/CP, the `is_ia` discriminator lives **inside** the JSON blob (no `is_ia` DB column). IA differs from EE in that **marking criteria vary by subject**: each IA subject in `data/ia_subjects.json` owns its own `criteria` list (`{name, max}`), the total max is the sum of those maxes, and the grade is numeric-only (no A–E letter). Criteria are snapshotted into each paper's `ia_data` at submit time; per-criterion `max` + total are computed server-side in `build_ia_data_from_form` (form values ignored — same server-trust rule as EE). Legacy IB sample papers identified by `author_name == "IB SAMPLE"`. Two mutually exclusive author-bypass flags: `is_ib_sample` (non-`standard` types only — EE/CP/IA — displays an "IB SAMPLE" placeholder) and `is_anonymous` (any type, stores empty author fields and hides the author row everywhere); IB Sample wins server-side if both arrive.

**i18n** — Flask-Babel with `en`/`zh` locales. Translation catalogs in `translations/<locale>/LC_MESSAGES/messages.po`. The `_()` gettext function and `_l()` lazy_gettext are used throughout `app.py`. All user-facing LLM output must be bilingual too.

**Auth & web hardening** — dual system: local PBKDF2-hashed passwords and MS Graph OAuth via the `msal` library. Session tokens are stored server-side in the `sessions` table with a configurable timeout (cookie lifetime = `SESSION_TIMEOUT`, not 365d). Hardening added in the security pass:
- **CSRF: Flask-WTF `CSRFProtect` is global.** Every `<form method="post">` must include `{{ csrf_token() }}`; JSON/`fetch` calls must send an `X-CSRFToken` header read from `<meta name="csrf-token">` (injected at the `dashboard.js` fetch chokepoint + per-page fetches in `ai.js`/`upload-wizard.js`/`ee-subjects.js`/`ia-subjects.js`/`guides-editor.js`). The standalone `ai.html` carries its own meta tag.
- **`logout` is POST-only**; sign-out controls are forms.
- Session cookies: `SameSite=Lax`, `HttpOnly`, `Secure` (gated by `PAPERQUERY_COOKIE_SECURE`).
- `after_request` adds `nosniff` / `X-Frame-Options: DENY` / `Referrer-Policy` / `Permissions-Policy` / HSTS (HTTPS only) + a **CSP in Report-Only mode** (not enforcing — the app has many inline scripts/handlers).
- Login `next` and OAuth `state` are validated: `_is_safe_redirect_target()` (same-host/relative only; rejects `\`, `//`, control chars) and `ms_callback` pops+requires `ms_state`.

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
| `data/` | JSON configs (paper/news/guide categories, EE subjects, IA subjects + per-subject criteria); runtime `pending_papers/` |
| `papers/` | Uploaded PDF storage (gitignored) |
| `resource_files/` | Academic Resources file storage |
| `tools/` | CLI scripts: user management, translation compilation, embedding backfill |
| `tests/` | Contract tests using `unittest` — parse app.py with AST + render Jinja2 templates |
| `deploy/keydion.nginx.conf` | Production nginx config (host-managed, not docker) |
| `docs/superpowers/` | Local spec/plan docs — use this gitignored area for planning artifacts; never stage or commit any plan/spec |

## Testing approach

Tests are **contract tests**, not integration tests. They locate source via `tests/support.py` (`find_function` / `source_of` / `all_sources`, searching `app.py` + `routes/` + `services/`), parse it with Python's `ast` module, and render Jinja2 templates with mock data to verify structural invariants:
- DOM contracts (element IDs match between HTML and JS `getElementById` calls)
- Data round-trip contracts (fields are carried through load/write functions)
- Server-side logic contracts (EE total grade is calculated server-side, not trusted from the form)

Importing `app`, `routes/*`, or `services/*` is side-effect free with respect to database initialization. Tests that call `create_app()` still require an already migrated database, while the isolated-test runner creates and bootstraps its disposable database explicitly.

**CSRF test gotcha:** global `CSRFProtect` breaks naive tests — Flask test-client tests that POST must set `app.config["WTF_CSRF_ENABLED"] = False`, and standalone Jinja-render tests must stub `env.globals["csrf_token"] = lambda: ""` (else templates calling `{{ csrf_token() }}` raise). Existing test files already do this; follow the pattern when adding tests.

Conventional commits (`feat:`, `fix:`, with optional scope like `fix(i18n):`) are used.

## Noteworthy patterns

- **EE total grade** is computed server-side in `build_ib_ee_data_from_form()` — the form field `ibTotalGradeNumber` is `readonly` and its submitted value is ignored
- **Legacy IB sample papers** (`author_name == "IB SAMPLE"`) hide the school field in search results and maintain backward-compat logic scattered across templates and routes
- **News body** supports a JSON block format: `[{"type": "text", "content": "..."}, {"type": "image", "url": "...", "caption": "..."}]`, with a `parse_body_blocks` template filter for backward compat with plain-text bodies. Each text block's `content` is sanitized server-side on publish/edit via `sanitize_news_body` (reuses the guides bleach allowlist `_sanitize_guide_html`); rendered `|safe` only after sanitization.
- **Path containment** — every user-controlled `PAPERS_DIR / <filename>` sink must `resolve()` + `is_relative_to(PAPERS_DIR.resolve())` before any FS op (idiom from `papers_bulk_action`): `paper_preview`, `preview_paper`, `paper_delete`, `paper_modify`, and the upload draft `pending_filename`. The agentic `read_paper` guard lives in `_lib_full_text` (basename of the model-supplied filename), **not** `_rag_paper_text` — that function doubles as the RAG indexer's text extractor, so guarding it there breaks first-time indexing of new papers.
- **RAG lifecycle**: papers are chunked + embedded on publish and purged on delete (`rag_index.py`); chunk vectors live in a binary `VECTOR(3072)` column (**requires MySQL 9.x**; dimension from `RAG_EMBED_DIM`). Every store write bumps `rag_index_meta.chunks_version` in the same transaction; each worker's in-memory numpy snapshot is stamp-checked per query, so re-embeds/purges propagate to all workers without restarts. One-time JSON→VECTOR backfill: `tools/migrate_chunk_vectors.py` (then `--drop-json`).
- **Schema lifecycle**: Alembic owns every schema change. Runtime startup is verification-only and refuses empty, stale, or divergent schemas. `python3 -m tools.bootstrap_database --confirm-empty-bootstrap` is the only application command that creates and stamps a verified-empty database; `python3 -m tools.verify_alembic_state` checks the deployed revision and migration drift.
- Paper metadata is both in the DB (`papers_metadata` table) and optionally in the filesystem (`data/paper_metadata.json`); routes read from DB via `_load_papers()` which queries `PaperMetadataModel`
- **Production deploy** uses host-managed nginx + systemd gunicorn (`wsgi:app`, `gunicorn.conf.py`, and `run_prod.sh`) plus independent publishing and attachment workers and the scheduled Paper-integrity scanner. The bundled `docker-compose.prod.yml` is a reference stack, not the authoritative production procedure.
- **Translation cache**: Flask-Babel loads `.mo` files at startup. After `tools/compile_translations.py`, the dev server must be restarted for new translations to appear.
- **Split layout invariants** — endpoint names are a cross-module contract (`url_for("preview_paper")` is called from the ask domain); `routes/` and `services/` modules must not import `app`; new code goes in `services/<domain>` + `routes/<domain>`, not `app.py`.

## Agent skills

### Issue tracker

Issues are tracked in this repo's GitHub Issues via the `gh` CLI; external PRs are not a triage surface. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles use their default label strings (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context — one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
