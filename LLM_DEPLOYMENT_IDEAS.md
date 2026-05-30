# LLM Deployment Ideas — Keydion

Candidate places to apply an LLM in this repository app. #2 is implemented
(abstract + keyword auto-fill on upload). The rest are recorded for future work.
All user-facing output must be bilingual (en/zh) via Flask-Babel.

| # | Idea | Effort | Risk | Status |
|---|------|--------|------|--------|
| 1 | Grading assist (EE/CP draft scores + comments) | M | High (subjective IB grading) | Idea |
| 2 | Abstract + keyword auto-fill on upload | S | Low | Implemented |
| 3 | Reviewer triage / quality flags | M | Medium | Idea |
| 4 | Semantic search & "related papers" | L | Medium | Idea |
| 5 | Bilingual content assist (en↔zh) | S–M | Low–Medium | Idea |
| 6 | "Ask the library" RAG chat | L | Medium | Idea |

## 1. Grading assist (EE / CP)
Draft per-criterion scores + comments into the Curator review form for an
*ungraded* uploaded paper. Plugs into the review flow (`/dashboard/review/<id>`,
`review_accept` / `review_reject`) and the existing `ib_ee_data` criteria A–E /
`cp_data` criteria A–D structures (score + comment fields already exist).
**Always draft-only** — a Curator must edit and approve; never auto-publish. IB
grading is subjective and high-stakes, so this is the riskiest idea and needs
clear "AI-suggested, human-confirmed" UX.

## 2. Abstract + keyword auto-fill on upload  (implemented)
LLM reads the uploaded PDF and drafts the Abstract + Keywords on the Standard-
paper Metadata step; the uploader edits before submit. Mirrors the EE auto-fill
button. Endpoint `/api/upload/generate-abstract-keywords` -> `llm_metadata.py`.
Future extension: add abstract/keyword fields + generation for EE/CP papers too.

## 3. Reviewer triage / quality flags
Summarize each pending submission and flag issues (off-topic, missing sections,
possibly AI-generated, language mismatch) to help Curators prioritize the
`/dashboard/review` queue. Output is advisory metadata shown in the review list.

## 4. Semantic search & "related papers"
Replace/augment the current substring + full-text search (`search_papers`,
capped at 20 results) with embeddings-based semantic search and a "related
papers" block on paper pages. Needs an embedding store / vector index — the
largest infrastructure lift here.

## 5. Bilingual content assist (en ↔ zh)
Auto-draft translations of titles, abstracts, and news/journal bodies so content
is available in both `en` and `zh`. Fits the existing i18n workflow and the
block-based `NewsArticleModel.body`. Human reviews translations before publish.

## 6. "Ask the library" RAG chat
A Q&A assistant grounded in the published corpus (retrieval over paper text +
metadata). Students ask questions and get cited answers. Depends on the same
embedding/index infrastructure as #4; largest lift, highest product value.

## Cross-cutting notes
- **Provider:** OpenAI-compatible client (`openai` SDK with configurable
  `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL`); works with OpenAI, a local
  model (Ollama/vLLM), or any OpenAI-style provider. Cheap model `gpt-4o-mini`
  is the default for summarization-class tasks.
- **Privacy:** cloud providers receive paper text. For sensitive data, point
  `LLM_BASE_URL` at a self-hosted model so nothing leaves your network.
- **Cost control:** gate cost-incurring endpoints behind `require_login(level>=2)`,
  truncate input text, and prefer explicit user-triggered buttons over automatic
  background calls.
- **Human-in-the-loop:** every user-facing generation is a *draft* a human edits
  before anything is saved or published.
