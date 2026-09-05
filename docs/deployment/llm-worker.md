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

## Configure and validate the Worker

Use Node.js 24+ in `workers/llm/`:

```bash
npm ci
npm run types
npm run check
npm test
npm run dry-run
```

`npm test` includes an actual local Workers runtime with mocked upstream requests;
it needs permission to bind loopback sockets. The other gateway tests exercise
forwarding, authentication, cancellation, errors, and embedding validation.
`npm run dry-run` builds without publishing. These are Worker checks; the Flask
application still has no frontend build step.

Edit `workers/llm/wrangler.jsonc`:

- `MODEL_ROUTES.flash`, `think`, and `vision` each select an OpenAI-compatible
  `base_url`, actual `model`, `key_secret`, and boolean `enabled`.
- `MODEL_ROUTES.embed` also declares the expected output `dimensions`. For an
  existing index, copy its current endpoint, model, and dimensions exactly.
  Use a versioned model where the provider offers one. The gateway cannot detect
  a provider changing the weights behind an unchanged model name.
- `key_secret` selects `CHAT_API_KEY`, `VISION_API_KEY`, or `EMBED_API_KEY`.
  Purposes may share a credential or use different providers.
- Optional `timeout_ms` sets the entire upstream request deadline, including
  streaming, from 1 to 600000 ms (default 90000). Keydion also retains its own
  client/publishing deadlines; the earlier deadline wins.
- Optional `reasoning_effort` selects `minimal`, `low`, `medium`, or `high` for
  models supporting the OpenAI-compatible reasoning parameter. It replaces
  Keydion's older provider-specific `thinking` field. The deployed Thinking route
  uses `high`.

The checked-in routes are enabled for the deployed instance. Disable any purpose
that should not accept requests. Non-secret routing settings are deployed with
the Worker. Store credentials as [Cloudflare Secrets](https://developers.cloudflare.com/workers/configuration/secrets/),
never in `wrangler.jsonc`. For an operator-approved deployment:

```bash
npx wrangler deploy
npx wrangler secret put KEYDION_TOKEN
npx wrangler secret put CHAT_API_KEY
npx wrangler secret put VISION_API_KEY
npx wrangler secret put EMBED_API_KEY
```

Use an independently generated random shared token for `KEYDION_TOKEN`, and set
provider secrets for the enabled routes. Enable the verified routes and redeploy.
For local testing, copy `.dev.vars.example` to the gitignored `.dev.vars`, supply
test credentials, and run `npm run dev`. Only local development may use an HTTP
Worker origin. Keep the shared token server-side.

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

## Wire contract and operational behavior

| Endpoint | Request | Behavior |
| --- | --- | --- |
| `GET /v1/capabilities` | Shared bearer token | Per-purpose enabled state and model; embedding identity and dimensions; no provider URLs or keys |
| `POST /v1/chat/completions` | OpenAI chat body with `model` = `flash`, `think`, or `vision` | Resolves the real model; preserves messages, tools, image parts, JSON format, and streaming |
| `POST /v1/embeddings` | `model` = `embed:<pinned ID>`, `X-Keydion-Embed-Dim` header | Resolves the fixed embedding model, returns validated float vectors |

Only these routes are forwarded. Neither caller-selected destinations nor
provider redirects are followed. The shared Worker token is replaced with the
selected provider credential upstream. Upstream HTTP failures preserve their
error status but return a generic error code and request ID; provider bodies,
redirects, cookies, and credentials are not echoed. A midstream failure aborts
the stream so Keydion can report a failed Ask turn.

Requests are limited to 32 MiB; embedding responses to 16 MiB. Chat responses are
streamed with backpressure and cancellation. Structured application logs contain
only purpose, actual model, duration, status, and request ID. Stream duration
runs until completion, cancellation, or error. Automatic invocation logs are
disabled; do not enable request/response body logging in other observability
systems. See [Cloudflare's streaming guidance](https://developers.cloudflare.com/workers/runtime-apis/streams/).
