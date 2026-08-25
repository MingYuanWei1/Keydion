# AI, RAG, and PDF Reading

Before changing metadata auto-fill, read [ADR-0001](../adr/0001-vision-first-extractor-template-method.md) and [ADR-0004](../adr/0004-metadata-auto-fill-is-model-read.md); they are authoritative.

## Capability gates

Use `llm_enabled()` for Ask/chat and the text-model metadata fallback, `embedding_enabled()` for embeddings and semantic search, and `vision_enabled()` for vision-first PDF reading. Ask web access additionally requires `web_search_enabled()` and per-turn opt-in. Never treat `llm_enabled()` as a global AI gate: an empty `LLM_API_KEY` does not disable independently configured embeddings or vision. `.env.example` owns the credential and fallback mapping.

## Model and PDF boundaries

- Send one-shot model calls through `llm_client.chat()` or `chat_json()`. The streaming Ask turn is the deliberate exception.
- Keep `pdf_text.py` provider-free: it must not import `llm_client`; callers opt scanned PDFs into vision by injecting `vision_fallback`, otherwise it uses Tesseract.

## RAG storage

Paper chunk embeddings live in MySQL `VECTOR(RAG_EMBED_DIM)`. Python binds JSON-text vectors through `STRING_TO_VECTOR()`, but the database column is binary `VECTOR`; only attachment chunk embeddings remain JSON text.

Publishing and deletion update Paper chunks through the publishing lifecycle and bump `rag_index_meta.chunks_version` in the same transaction. Each retrieval process checks that stamp on every query and rebuilds its in-memory snapshot when it changes. Changing `RAG_EMBED_DIM` requires an Alembic migration and a full Paper re-index.
