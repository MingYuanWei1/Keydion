# Cloudflare model gateway

Keydion keeps prompts, PDF rendering, RAG retrieval, tool execution, and existing
business fallbacks. The Worker owns OpenAI-compatible model routing and provider
credentials. Tavily continues to run directly from Keydion. No database migration
is required when the embedding provider, model, and dimensions stay the same.

## Deployed instance

The `keydion-llm` Worker was created through the Cloudflare MCP on 2026-09-05
and serves `https://api.keydion.com`. Cloudflare manages the custom domain and
TLS certificate. The `workers.dev` endpoint and preview URLs are disabled.
The checked-in Wrangler configuration mirrors this deployment:

| Purpose | Provider | Model |
| --- | --- | --- |
| Flash | Google AI Studio | `gemini-3.8-flash` |
| Thinking | Google AI Studio | `gemini-3.8-flash`, high reasoning effort |
| Vision | Google AI Studio | `gemini-3.8-flash` |
| Embeddings | Google AI Studio | `gemini-embedding-001`, 3072 dimensions |

The shared token and connection settings are saved locally in the gitignored
`local/llm-worker.env` with owner-only permissions. Provider credentials are
Cloudflare secret bindings. Keydion uses the Worker exclusively; direct-provider settings and the provider
editor have been removed.

Live tests verified TLS, authenticated capability discovery, rejection of
unauthenticated requests, both chat models, JSON output, completed SSE streaming,
automatic tool selection, image input, and embeddings. All model credentials are Cloudflare secret bindings. The local provider keys
were removed after verification.

Keydion identifies Worker requests with `User-Agent: Keydion/llm-worker`; the
zone blocked the OpenAI SDK's default user-agent before requests reached the
Worker. Cloudflare's zone-wide security settings were not weakened. The Worker
sets its configured `reasoning_effort` for Thinking, and Ask
preserves Gemini's opaque tool-call signatures when continuing a tool round.
The live Thinking Ask test completed two model requests, executed its synthetic
Paper tool, and produced a final answer without errors. Worker verification
passed 14 tests and type checking. After removing direct transport, 169 focused
Python tests passed. The rebuilt Docker environment passed 53 client/Ask tests
with its locked OpenAI 3 and httpx2 dependencies, served the homepage with HTTP
200, and completed a live 3072-dimension embedding request.

The full isolated MySQL run executed 1547 tests: 1542 passed, one filesystem test
was skipped, and four tests encountered runner environment errors (Node was not
installed in the app image, and resource storage was mounted read-only). Both
JavaScript test modules subsequently passed on the host, and all 12 resource
tests passed in a separate isolated run with temporary writable storage.
The Google embedding test used both English and Chinese inputs through the real
Keydion client. The Worker restores Google's omitted first `index=0` field while
retaining the remaining vector count, order, and dimension checks.

The local `.env` and `.env.prod` contain the Worker URL, token, and verified
Google embedding identity from `local/llm-worker.env`. These files belong to
the local workspace, not the production server. The local database and index
rebuild are outside this deployment's scope; no temporary embedding disablement
is applied for them. No production environment or database was changed.

## The Worker

The Worker's source, configuration, deployment steps, demo token and wire
contract live in the separate `keydion-api` repository (`~/Desktop/Project/keydion-api`).

## Configure Keydion

In the active environment (`.env.prod` when present, otherwise `.env`), set:

```dotenv
LLM_WORKER_URL=https://api.keydion.com
LLM_WORKER_TOKEN=THE_SAME_VALUE_AS_KEYDION_TOKEN
LLM_WORKER_EMBED_ID=
```

The URL is an origin without `/v1`. Keep the embedding pin empty until the index
matches the Worker model. After a verified rebuild, set it to
`purposes.embed.embedding_id` from authenticated `GET /v1/capabilities`.
An empty or mismatched identity disables embeddings independently of chat and
vision. The capability endpoint reports configuration, not inference health.

Restart the web, publishing, and attachment processes after changing their
connection settings or embedding pin. Later Worker-only chat or vision model
changes need no app restart. `/dashboard/admin/models` shows Worker status and
embedding compatibility, and retains Tavily settings. Provider editing and
probe routes are removed. Capability discovery is cached for 15 seconds and
fails closed when discovery is unavailable. There is no direct transport or
local model-provider credential configuration.

Verify Flash and Thinking Ask turns, streamed tool use, image extraction and
JSON responses. After the index cutover, verify semantic search, attachment
retrieval, and publishing-worker indexing before resuming traffic.

## Embedding changes

The Worker computes embedding identity from normalized endpoint, model, and
output dimensions. Every embedding request carries the pinned identity in its
model alias and the server's expected dimensions in a header. A mismatch returns
409 before contacting a provider. Returned float vectors are checked for count,
index order, finite values, and dimensions before Keydion receives them. Chat
capabilities do not depend on embedding configuration.

Changing even a same-dimension embedding model requires a maintenance cutover:
stop semantic reads and indexing writers, take recoverable backups, change the
Worker route and the server pin together, rebuild all Paper embeddings with
`python3 tools/build_embeddings.py --rebuild`, and rebuild or invalidate existing
attachment embeddings using the application's attachment lifecycle. Restart all
processes to clear query-vector caches, then verify retrieval before reopening
traffic. If dimensions change, also follow the Alembic schema migration process
for `RAG_EMBED_DIM`. Do not expose a partially rebuilt index, and do not roll back
to an old embedding model without restoring or rebuilding the matching vectors.
