---
status: accepted
---

# Centralize model configuration and credentials in a Cloudflare Worker

Keydion sends model requests through a Cloudflare Worker that owns provider credentials, model configuration, and request forwarding. The purpose is to change providers and models without modifying or restarting Keydion; Keydion keeps the Worker endpoint and an access credential, and continues to own prompts, RAG retrieval, and tool execution.

The implementation includes code, tests, and deployment instructions. Actual deployment and production cutover are a separate operation.

## Agreed scope

Chat, vision, and embedding requests go through the Worker; Tavily web search remains separate. Provider configuration and secrets are managed in Cloudflare. Keydion's administration interface shows AI capability configuration status and no longer edits provider credentials or model configuration; the first version adds no separate configuration panel.

Keydion requests models by purpose, and the Worker maps each purpose to an actual model and controls whether it is enabled. Embeddings use a fixed model version: switching models requires a coordinated index update even when vector dimensions match.

Keydion authenticates to the Worker with a dedicated shared access token sent only by the server. Provider failures return explicit errors while preserving existing application fallback behavior; the Worker does not automatically switch providers.

The Worker is the only model transport. The temporary direct transport and provider editor were removed after live validation of chat, vision, and embeddings.

The OpenAI-compatible `model` field carries `flash`, `think`, `vision`, or an embedding purpose with a pinned identity. The Worker derives the embedding identity from the provider endpoint, model, and dimensions; mismatches fail before inference. Capability discovery is authenticated and cached in Keydion for 15 seconds, failing closed on discovery failure. Missing Worker settings disable model capabilities; there is no direct-provider fallback.

The first version supports OpenAI-compatible provider APIs. Non-sensitive routing configuration is deployed with the Worker, and provider credentials use Cloudflare Secrets; configuration changes may redeploy the Worker without restarting Keydion. Worker logs contain only purpose, model, duration, status, and request ID, excluding request bodies, response bodies, and credentials.

## Considered options

- Keep provider configuration in Keydion and use the Worker only as a network proxy. This retains application configuration changes when switching providers and does not meet the central management goal.
- Move prompts, retrieval, and tool orchestration into the Worker. This splits application behavior across deployments without being necessary for central model configuration.
