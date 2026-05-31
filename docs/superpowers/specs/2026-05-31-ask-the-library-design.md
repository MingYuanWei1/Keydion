# Ask the Library — Design Spec

**Date:** 2026-05-31
**Idea:** #6 in `LLM_DEPLOYMENT_IDEAS.md` — "Ask the library" RAG chat.
**Status:** All five phases IMPLEMENTED (Phases 1–3 on `worktree-ask-the-library`; Phases 4–5 on `ask-the-library-phases-4-5`). See §0 for per-item status.

A Q&A assistant grounded in the published paper corpus. Students ask questions and
get cited answers that link back to the source papers. This spec is comprehensive
(all five phases) but **implementation is phased** — each phase is independently
shippable and verifiable.

---

## 0. Implementation status (updated 2026-06-01)

Legend: ✅ done & verified · 🟡 done with deviation/known gap · ⏳ pending (not built)

| Area | Status | Notes |
|------|--------|-------|
| **Foundation** | | |
| 2.1 Env var migration (`LLM_MODEL`→`LLM_DEFAULT_FLASH`/`THINK`, `LLM_EMBED_*`) | ✅ | `llm_client.py`; README updated; no `LLM_MODEL` refs remain |
| 2.2 `llm_client.py` (client + model resolution, embed/chat split) | ✅ | flash/think/embed + `build_client`/`build_embed_client`/`llm_enabled` |
| 2.3 `rag_index.py` + `PaperChunkModel` | 🟡 | built & unit-tested; **`retrieve()` applies `min_sim` after `scored[:k]`** so it can return fewer than k hits (minor bug, not fixed) |
| 2.4 Index lifecycle (`tools/build_embeddings.py` + publish/delete hooks) | ✅ | CLI + `review_accept` upsert + `paper_delete` purge |
| 2.5 Guardrails (rate limit, length cap, `llm_enabled` gate) | 🟡 | done; **rate limit is per-gunicorn-worker** (not per-deployment); length cap = `MAX_QUESTION_CHARS=2000` (spec said ~1000) |
| **Phase 1 — core chat** | | |
| 3.1 `GET /ask` page route | 🟡 | **DEVIATION (user-approved): standalone full-screen page, NOT partial-aware** — `ask.html` is its own `<!doctype html>`; the `partial=` arg is now unused |
| 3.2 `POST /api/ask` SSE streaming + citations | ✅ | token/citations/done frames; grounding via `retrieve()`/forced |
| 3.3 UI (`ask.html` + `ask.css`) | ✅ | full mockup port: header, rail, thread (centered via `.kd-thread__inner`), rich composer, cite modal; footer removed → "© <year> Keydion" in rail foot |
| 3.4 Entry points (nav links + landing CTA) | ✅ | gated on `llm_enabled` in `base.html` + `landing.html`; contract test added (`test_ask_entrypoint_gating_contract.py`) |
| **Phase 2 — saved history** | | |
| Models + owner key + CRUD + persistence | ✅ | `ConversationModel`/`ChatMessageModel`, `_ask_owner_key`, `/api/conversations[/<id>]` |
| Rail UI (list/create/switch/rename/delete) | ✅ | works; conversation list date-grouped (Today/Yesterday/older); rail search wired; owner key on 365-day cookie (conversations survive browser close) |
| **Phase 3 — cite from library** | | |
| Cite list endpoint `/api/ask/papers` | ✅ | search/list published papers, capped 50 |
| Cite modal + chips + forced grounding | ✅ | modal + selection + chips + subject filters + abstract preview pane all wired; `_forced_grounding` now applies `min_sim` |
| **Phase 4 — attach a document** | ✅ | `attachment_chunks` transient table; `extract_text_from_upload` (PDF/DOCX/TXT/MD); `POST/DELETE /api/ask/attach` upload + purge; attachment grounding merged into `api_ask` (attached docs highest priority, capped to 6 combined hits); conversation DELETE purges chunks; conversation GET returns `attachments`; file chips (`kd-chip--file`) in composer; chips restore on conversation switch; `python-docx>=1.1` added to `requirements.txt` |
| **Phase 5 — web access** | ✅ | `web_search.py` (default Tavily, off when `WEB_SEARCH_API_KEY` unset; toggle hidden when unconfigured); `web` flag in `POST /api/ask`; `_build_ask_prompt(web_results=...)` blends web sources as continuation-numbered entries; `web` SSE frame; "Searched the web" note (`kd-webnote`) rendered in JS; `WEB_SEARCH_PROVIDER`/`WEB_SEARCH_API_KEY` documented in README |
| **Cross-cutting** | | |
| 8.1 i18n (en/zh) | ✅ | all ask UI strings extracted + zh filled + compiled, including Phase 4/5 strings (`Attached document`, `Searched the web`, file-error strings) |
| 8.2 Testing | ✅ | 282 tests pass; `test_ask_entrypoint_gating_contract.py`, `test_attachment_contract.py`, `test_web_search_contract.py` added on this branch |
| 8.3 Deploy | ✅ | env vars documented in README; `tools/build_embeddings.py` present; `python-docx>=1.1` in `requirements.txt` |

**Known open items (deliberate deviations; reviewed and accepted):**
- Web results are **ephemeral** — not persisted to `chat_messages`, so they do not reappear on conversation reload. This is intentional: web results are time-sensitive and should not be treated as authoritative library sources.
- `retrieve()` filter-before-slice (`min_sim` applied after `scored[:k]`) — MINOR; fixed for `_forced_grounding` and `_attachment_grounding` but not for the base `retrieve()` path (low impact in practice).
- Rate limit is per-gunicorn-worker (not per-deployment) — MINOR; acceptable for current traffic.
- `GET /ask` is a standalone full-screen page (not partial-aware) — user-approved deviation from §3.1.

**Branch history:** Phases 1–3 + foundation on `worktree-ask-the-library` (merged to `main`). Phases 4–5 + hardening on `ask-the-library-phases-4-5` (Tasks 1–12, 282 tests pass).

---

## 1. Guiding principles

- **Match the codebase, not the mockup's tech.** The mockup (`Keydion_AI.html`) is
  bundled React/Babel. This app is server-rendered Jinja2 + vanilla JS + Bootstrap
  with **no build step** and per-page CSS in `static/css/`. We reproduce the
  mockup's *visual design* (layout, `kd-*` class names, tokens) using a per-page
  stylesheet `static/css/ask.css` and vanilla JS. We do **not** introduce React,
  a bundler, or any build step.
- **Surgical, simplicity-first.** Follow `CLAUDE.md`: minimum code that solves the
  problem; touch only what the feature requires; match existing patterns
  (`require_login`/`get_active_user`, `render_template` with `partial`,
  `init_db()` ad-hoc migrations, per-page CSS, `_()` i18n).
- **One spec, phased build.** Five phases. A cross-cutting foundation is built
  first and used by every phase.
- **Human-in-the-loop / honesty.** The assistant answers only from retrieved
  sources and says so when it has nothing relevant; every answer carries its
  citations.

### Design tokens (from the mockup)

- Brand blue: `#1f44b8` (the Keydion "K")
- Page background: `#faf9f5` (warm off-white)
- Fonts: system UI stack for body; `Cormorant Garamond` for display headings
  (already loaded in `base.html`)
- BEM-style `kd-*` class names from the mockup are reused verbatim so the CSS maps
  cleanly to the reference design.

---

## 2. Cross-cutting foundation (built first; all phases depend on it)

### 2.1 Environment variable migration (explicit user requirement)

**Remove** `LLM_MODEL` everywhere it appears:
- `llm_metadata.py:109` (`os.environ.get("LLM_MODEL", DEFAULT_MODEL)`)
- `README.md` LLM/AI-assist section
- any `.env` example / docker-compose / docs references
- the contract test(s) that assert on `LLM_MODEL`

**Add** these variables:

| Variable | Purpose | Default |
|----------|---------|---------|
| `LLM_DEFAULT_FLASH` | Fast/cheap chat + summarization model | `gpt-4o-mini` |
| `LLM_DEFAULT_THINK` | Deeper reasoning model for the "Thinking" toggle | unset → falls back to `LLM_DEFAULT_FLASH` |
| `LLM_EMBED_MODEL` | Embedding model for the RAG index | `gemini-embedding-001` |
| `LLM_EMBED_BASE_URL` | Base URL for the **embedding** provider (separate so embeddings can use Gemini while chat uses another provider) | falls back to `LLM_BASE_URL` |
| `LLM_EMBED_API_KEY` | API key for the embedding provider (Gemini key differs from the chat key) | falls back to `LLM_API_KEY` |

`LLM_API_KEY` / `LLM_BASE_URL` drive the **chat** models. Embeddings use the
`LLM_EMBED_*` vars when set, falling back to the chat vars otherwise — so a
Gemini embedding endpoint (e.g. base URL
`https://generativelanguage.googleapis.com/v1beta/openai/`, model
`gemini-embedding-001`) can run alongside a different chat provider.

### 2.2 New module `llm_client.py`

Centralizes client + model selection so `llm_metadata.py`, the chat endpoint, and
the RAG index all share one source of truth.

```
build_client()       -> OpenAI-compatible CHAT client from LLM_API_KEY / LLM_BASE_URL
build_embed_client() -> EMBEDDING client from LLM_EMBED_API_KEY/LLM_EMBED_BASE_URL,
                        each falling back to LLM_API_KEY/LLM_BASE_URL when unset
flash_model()        -> os.environ["LLM_DEFAULT_FLASH"] or "gpt-4o-mini"
think_model()        -> os.environ.get("LLM_DEFAULT_THINK") or flash_model()
embed_model()        -> os.environ.get("LLM_EMBED_MODEL") or "gemini-embedding-001"
llm_enabled()        -> bool(os.environ.get("LLM_API_KEY"))
```

`llm_metadata.py` is refactored to import `build_client` and `flash_model` from
this module. Its only behavior change: it uses `flash_model()` instead of
`LLM_MODEL`. The `LLMMetadataError` surface and the abstract/keyword route are
otherwise untouched.

`app.py:1407` (`llm_metadata_enabled`) switches to `llm_enabled() and _role >= 2`.

### 2.3 Embeddings / RAG core — new module `rag_index.py`

Pure-Python, no new heavy dependency (cosine done by hand).

```
chunk_text(text, size=800, overlap=120) -> list[str]
    overlapping character chunks; bounds prompt/token cost like MAX_PDF_CHARS does.

embed_texts(client, texts) -> list[list[float]]
    batched embeddings via build_embed_client() (the Gemini embedding endpoint).

build_index(filenames=None) -> stats
    For each published paper (gather_paper_records + extract_pdf_text):
    chunk -> embed -> upsert PaperChunkModel rows. filenames=None rebuilds all.

retrieve(query, k=6, min_sim=0.20) -> list[Chunk]
    embed query; pure-Python cosine over an in-memory cache of stored embeddings;
    return top-k chunks above min_sim, each carrying paper metadata
    (filename, title, author_name, url).
```

**Storage** — new model `PaperChunkModel` (table `papers_chunks`):

| Column | Type | Notes |
|--------|------|-------|
| `id` | int PK autoincrement | |
| `filename` | Unicode(255) | FK-by-convention to `papers_metadata.filename` |
| `chunk_index` | int | order within the paper |
| `content` | UnicodeText | the chunk text (for prompt grounding + snippet display) |
| `embedding` | UnicodeText | JSON-encoded `list[float]` |
| `lang` | Unicode(10) | paper language, for optional locale filtering |

Table created via the `init_db()` ad-hoc `CREATE TABLE` / try-except pattern
(`app.py:623`), consistent with existing migrations.

**In-memory cache:** embeddings are loaded once per process into a module-level
list and reused across requests; invalidated on index rebuild/upsert. Brute-force
cosine over hundreds–low-thousands of chunks is well within request latency.

### 2.4 Index lifecycle

- **CLI** `tools/build_embeddings.py` — full (re)build; run once by the operator
  after deploy. Prints per-paper stats.
- **Incremental upsert** on publish: hook where metadata rows are created
  (`app.py:~4022`, reached via `review_accept` at `app.py:3291`). Best-effort,
  wrapped so a failed embed never blocks publishing.
- **Purge** on paper delete (`paper_delete`, `app.py:2094`): delete the paper's
  `papers_chunks` rows and invalidate the cache.

### 2.5 Public-access guardrails (chat is public / no login)

- Per-IP, in-memory sliding-window rate limit (best-effort per gunicorn worker)
  on `POST /api/ask`.
- Question length cap (~1000 chars) and a retrieval/context token budget.
- All chat endpoints check `llm_enabled()` and return a JSON error (mirroring
  `api_generate_abstract_keywords`, `app.py:2292`) when AI is not configured.

---

## 3. Phase 1 — Core chat (visible MVP)

The end-to-end usable single-session assistant.

### 3.1 Page route — `GET /ask`

```
@app.route("/ask")            # endpoint: ask_library
def ask_library():
    user = get_active_user()          # public; None for guests
    return render_template(
        "ask.html",
        partial=...,                  # is_partial_request()
        llm_enabled=llm_enabled(),
        suggestions=[...],            # translated suggested prompts
        i18n={...},                   # JS-side strings (wizard_boot pattern)
    )
```

`ask.html` uses `{% extends "_bare.html" if partial else "base.html" %}` so it
works both as a full page and as a partial swap.

### 3.2 Streaming answer endpoint — `POST /api/ask`

1. Validate: `llm_enabled()`; non-empty question within length cap; rate limit.
2. `mode` in `{"flash","think"}` selects `flash_model()` / `think_model()`.
3. `retrieve(question, k)` → top-k grounding chunks (+ their papers).
4. Build a grounded prompt: numbered sources block; system instruction =
   *"Answer only from the numbered sources. Cite claims as `[n]`. Answer in
   {locale language}. If the sources don't contain the answer, say so."*
5. Stream: `Response(stream_with_context(gen), mimetype="text/event-stream")`.
   The generator yields SSE frames:
   - `token` frames as the model streams,
   - one `citations` frame: `[{n, filename, title, authors, url}]`
     (url = `url_for("paper_info", filename=...)`, `app.py:1917`),
   - a final `done` frame.
6. Client consumes the stream via `fetch` + `ReadableStream` and renders tokens
   live; on `citations`, renders the "Cited from your library" grid.

This is the app's first streaming endpoint; `stream_with_context` is the standard
Flask approach and needs no new dependency.

### 3.3 UI — `templates/ask.html` + `static/css/ask.css` + inline/vendored vanilla JS

Reproduces the mockup design at the structural level needed for Phase 1:

- **Shell:** `kd-app` with main column (rail is present but minimal until Phase 2).
- **Empty state:** `kd-empty` with display heading ("Ask Keydion") and
  `kd-suggest` suggested-prompt cards.
- **Thread:** `kd-thread` with `kd-msg--user` / `kd-msg--ai` bubbles; AI messages
  show a `kd-ai__avatar` "K", streamed prose in `kd-prose`, and a `kd-sources`
  grid of `kd-source` links to paper pages.
- **Composer:** `kd-composer` with auto-growing `<textarea>`
  (placeholder "Message Keydion AI…"), the **Flash / Thinking** `kd-agent`
  selector, and a `kd-send` button. Enter sends; Shift+Enter newlines.
- **Message actions:** copy, regenerate (`kd-iconbtn`).
- **Loading:** `kd-typing` indicator until the first token arrives.
- Composer hint: "AI can be wrong — verify citations against the source."

All user-visible strings are translated (server-rendered) or passed in the `i18n`
JS dict for client-rendered ones.

### 3.4 Entry points (explicit user requirement)

- **Header nav link on all pages:** add an "Ask the Library" `nav-link` in
  `base.html` (the shared `<nav>`, ~line 65) **and** in `landing.html`'s
  standalone header (landing does not extend base). Gated on `llm_enabled`.
- **Landing CTA button:** add a button linking to `/ask` in `landing.html`'s
  hero/CTA section, reusing `.btn-search` / `.btn-outline-light` styling.

---

## 4. Phase 2 — Saved conversation history

- **Models:** `ConversationModel` (`id`, `owner_key`, `title`, `created_at`,
  `updated_at`) and `ChatMessageModel` (`id`, `conversation_id`, `role`,
  `content`, `citations` JSON, `created_at`). Tables via `init_db()` pattern.
- **Ownership:** chat is public, so conversations are scoped by a stable
  per-browser key (session id / signed cookie), not a user account.
- **Endpoints:** list (grouped Today / Yesterday / Previous 7 days), create,
  rename, delete. Owner-scoped.
- **Rail UI:** `kd-rail` with `kd-newchat`, conversation search, grouped
  `kd-convo` items with rename (inline input) and delete (`kd-convo__menu`).
- **Persistence:** the user message is saved on send; the assistant message +
  citations are saved when the stream completes.

---

## 5. Phase 3 — Cite from library

- **Modal** (`kd-overlay` / `kd-modal`): paper list with search + subject filter
  and a preview pane (title, authors, journal, abstract) — per mockup.
- **Listing endpoint:** search/list published papers (reuse `gather_paper_records`
  / `search_papers`, `app.py:4041` / `3907`).
- **Forced grounding:** selected filenames are sent to `POST /api/ask` and used as
  the grounding set — augmenting or overriding automatic `retrieve()` — so the
  answer is constrained to the chosen papers. Selected papers appear as chips in
  the composer (`kd-chip--paper`).

---

## 6. Phase 4 — Attach a document

- **Composer "+" menu** → upload an ad-hoc file (PDF / DOCX / TXT / MD).
- **New dependency:** `python-docx` (added to `requirements.txt`) for `.docx`
  text extraction.
- **Transient ingestion:** extract text (PDF via existing PyPDF2 path; DOCX via
  `python-docx`; TXT/MD read directly) → chunk → embed into a
  **per-conversation transient scope** (not the published index).
- **Grounding:** the attached doc's chunks are included in that conversation's
  retrieval and purged when the conversation is deleted.
- Size cap enforced; files shown as `kd-chip--file` chips.

---

## 7. Phase 5 — Web access toggle

- **Toggle** (`kd-tool` "Web access") blends live web results into grounding.
- **Provider interface:** a small `web_search(query) -> list[result]` behind a
  configurable backend. **Default = Tavily** (purpose-built for LLM grounding),
  keyed by a new env var (e.g. `WEB_SEARCH_API_KEY` / `WEB_SEARCH_PROVIDER`);
  swappable, off when unset.
- **Rendering:** web sources show in a "Searched the web" note (`kd-webnote`),
  visually distinct from the "Cited from your library" grid.

---

## 8. Cross-cutting concerns

### 8.1 Internationalization (en / zh)

- All server-side strings via `_()` / `_l()`.
- Client-rendered (streamed/JS) strings passed in a per-route `i18n` JS dict
  (the existing `wizard_boot["i18n"]` convention).
- Workflow: `pybabel extract` / `update`, fill `translations/en` + `translations/zh`,
  then `python3 tools/compile_translations.py` and restart the dev server.

### 8.2 Testing (contract-test style, consistent with `tests/`)

- **Env migration:** no `LLM_MODEL` references remain anywhere; `llm_metadata`
  resolves its model from `LLM_DEFAULT_FLASH` via `llm_client`.
- **Routes:** `/ask` exists, is public, partial-aware; `POST /api/ask` validates
  input and gates on `llm_enabled()`; returns JSON errors (not HTML).
- **DOM contract:** the `kd-*` IDs/classes the JS targets exist in `ask.html`
  (mirrors existing DOM-id contracts).
- **Models:** `PaperChunkModel` (and Phase 2 `ConversationModel`/`ChatMessageModel`)
  column contracts.
- **`rag_index`:** unit tests with monkeypatched deterministic embeddings (no
  network) — chunking bounds, cosine ranking, `min_sim` filtering.
- **Entry points:** nav link present in `base.html` and `landing.html`; landing
  CTA button present; both conditional on `llm_enabled`.
- **Existing contracts still pass:** dashboard-URL-nesting (these routes are
  public, not under `/dashboard`) and partial-request contracts.

### 8.3 Deploy

- Document the new env vars (`LLM_DEFAULT_FLASH`, `LLM_DEFAULT_THINK`,
  `LLM_EMBED_MODEL`, `LLM_EMBED_BASE_URL`, `LLM_EMBED_API_KEY`, and Phase-5
  web-search vars) in `README.md`.
- New Python dependency `python-docx` (Phase 4) added to `requirements.txt`.
- Operator runs `tools/build_embeddings.py` once after deploy; the index stays
  current via the publish hook and delete purge.
- The embedding provider must expose an OpenAI-compatible embeddings endpoint
  (Gemini's `…/v1beta/openai/` endpoint qualifies). Host-managed nginx + systemd
  gunicorn deploy is unchanged (no Docker, per project deploy shape).

---

## 9. Decisions of record

1. **Vanilla JS** (no React/build step) — required by the no-build codebase.
2. **Routes:** `GET /ask` (`ask_library`) + `POST /api/ask`; citations link to the
   paper detail page (`paper_info`).
3. **Models:** `LLM_DEFAULT_FLASH=gpt-4o-mini`; `LLM_DEFAULT_THINK` (fallback →
   flash); embeddings on Gemini via `LLM_EMBED_MODEL=gemini-embedding-001` with a
   **separate** `LLM_EMBED_BASE_URL` / `LLM_EMBED_API_KEY` (fall back to the chat
   `LLM_BASE_URL` / `LLM_API_KEY`). `LLM_MODEL` removed.
4. **Vector store:** in-process — embeddings in MySQL (`papers_chunks`), pure-Python
   cosine over an in-memory cache. No new service, no new heavy dependency.
5. **Access:** public (no login), with per-IP rate limiting + length caps.
6. **Streaming:** live token streaming via `text/event-stream` +
   `stream_with_context`.
7. **Web search provider (Phase 5):** default Tavily, swappable, off when unset.
8. **Attach (Phase 4):** PDF / DOCX / TXT / MD; DOCX via the new `python-docx`
   dependency. Transient, per-conversation scope.

---

## 10. Phase summary

| Phase | Delivers | Key new artifacts |
|-------|----------|-------------------|
| Foundation | env migration, RAG core, guardrails | `llm_client.py`, `rag_index.py`, `PaperChunkModel`, `tools/build_embeddings.py` |
| 1 | core chat (MVP) | `GET /ask`, `POST /api/ask` (streaming), `ask.html`, `ask.css`, nav links, landing button |
| 2 | saved history | `ConversationModel`, `ChatMessageModel`, rail CRUD |
| 3 | cite from library | cite modal + forced grounding |
| 4 | attach a document | transient per-conversation ingestion |
| 5 | web access | pluggable web-search provider (default Tavily) |
