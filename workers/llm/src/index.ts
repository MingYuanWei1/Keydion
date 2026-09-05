import { timingSafeEqual } from "node:crypto";

const PURPOSES = ["flash", "think", "vision", "embed"] as const;
type Purpose = typeof PURPOSES[number];
type Route = {
  enabled: boolean;
  base_url: string;
  model: string;
  key_secret: "CHAT_API_KEY" | "VISION_API_KEY" | "EMBED_API_KEY";
  dimensions?: number;
  timeout_ms?: number;
  reasoning_effort?: "minimal" | "low" | "medium" | "high";
};

class GatewayError extends Error {
  status: number;
  code: string;
  constructor(status: number, code: string) { super(code); this.status = status; this.code = code; }
}

function routeFor(env: Env, purpose: Purpose): Route {
  const route: Route = env.MODEL_ROUTES[purpose];
  const url = new URL(route.base_url);
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash
      || !route.model || !["CHAT_API_KEY", "VISION_API_KEY", "EMBED_API_KEY"].includes(route.key_secret)
      || (purpose === "embed" && (!Number.isInteger(route.dimensions) || route.dimensions! < 1))
      || (route.reasoning_effort !== undefined && !["minimal", "low", "medium", "high"].includes(route.reasoning_effort))
      || (route.timeout_ms !== undefined && (!Number.isInteger(route.timeout_ms)
          || route.timeout_ms < 1 || route.timeout_ms > 600000))) {
    throw new GatewayError(503, "invalid_route_configuration");
  }
  return route;
}

async function embeddingId(route: Route): Promise<string> {
  // Changing the endpoint, model, or dimensions requires an explicit index cutover.
  const identity = JSON.stringify([route.base_url.replace(/\/+$/, ""), route.model, route.dimensions]);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(identity));
  return Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, "0")).join("");
}

async function readJson(body: ReadableStream<Uint8Array> | null, maxBytes: number): Promise<Record<string, unknown>> {
  if (!body) throw new GatewayError(400, "invalid_json");
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maxBytes) throw new GatewayError(413, "payload_too_large");
      chunks.push(value);
    }
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error;
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  try {
    const data: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (!data || typeof data !== "object" || Array.isArray(data)) throw new Error();
    return data as Record<string, unknown>;
  } catch { throw new GatewayError(400, "invalid_json"); }
}

export async function handle(request: Request, env: Env, upstreamFetch: typeof fetch = fetch): Promise<Response> {
  const started = Date.now();
  const requestId = crypto.randomUUID();
  let purpose: Purpose | null = null;
  let model: string | null = null;
  let logged = false;
  const log = (status: number) => {
    if (logged) return;
    logged = true;
    console.log(JSON.stringify({ purpose, model, duration_ms: Date.now() - started, status, request_id: requestId }));
  };
  const headers = { "x-request-id": requestId, "cache-control": "no-store" };
  let timer: ReturnType<typeof setTimeout> | undefined;
  const controller = new AbortController();
  const cancel = () => controller.abort();
  const cleanup = () => {
    if (timer !== undefined) clearTimeout(timer);
    request.signal.removeEventListener("abort", cancel);
  };
  try {
    const supplied = new TextEncoder().encode(request.headers.get("authorization") || "");
    const expected = new TextEncoder().encode(`Bearer ${env.KEYDION_TOKEN}`);
    if (!env.KEYDION_TOKEN || supplied.byteLength !== expected.byteLength || !timingSafeEqual(supplied, expected)) {
      throw new GatewayError(401, "unauthorized");
    }
    const path = new URL(request.url).pathname;
    if (path === "/v1/capabilities" && request.method === "GET") {
      const purposes: Record<string, object> = {};
      for (const name of PURPOSES) {
        // A malformed/disabled purpose does not disable independently configured purposes.
        try {
          const route = routeFor(env, name);
          purposes[name] = {
            enabled: route.enabled === true && Boolean(env[route.key_secret]), model: route.model,
            ...(name === "embed" ? { embedding_id: await embeddingId(route), dimensions: route.dimensions } : {}),
          };
        } catch { purposes[name] = { enabled: false, model: "" }; }
      }
      log(200);
      return Response.json({ purposes }, { headers });
    }
    if (!["/v1/chat/completions", "/v1/embeddings"].includes(path)) throw new GatewayError(404, "not_found");
    if (request.method !== "POST") throw new GatewayError(405, "method_not_allowed");
    const payload = await readJson(request.body, 32 * 1024 * 1024);
    const alias = payload.model;
    if (typeof alias !== "string") throw new GatewayError(400, "invalid_purpose");
    if (path === "/v1/embeddings") {
      if (!/^embed:[a-f0-9]{64}$/.test(alias)) throw new GatewayError(409, "embedding_identity_mismatch");
      purpose = "embed";
    } else {
      if (!["flash", "think", "vision"].includes(alias)) throw new GatewayError(400, "invalid_purpose");
      purpose = alias as Purpose;
    }
    const route = routeFor(env, purpose);
    model = route.model;
    if (route.enabled !== true || !env[route.key_secret]) throw new GatewayError(503, "purpose_unavailable");
    if (purpose === "embed") {
      if (alias !== `embed:${await embeddingId(route)}`
          || request.headers.get("x-keydion-embed-dim") !== String(route.dimensions)
          || (payload.dimensions !== undefined && payload.dimensions !== route.dimensions)) {
        throw new GatewayError(409, "embedding_identity_mismatch");
      }
      payload.encoding_format = "float";
    }
    payload.model = model;
    if (route.reasoning_effort !== undefined) {
      // Replace Keydion's legacy provider-specific flag with this route's
      // OpenAI-compatible reasoning setting.
      delete payload.thinking;
      payload.reasoning_effort = route.reasoning_effort;
    }
    request.signal.addEventListener("abort", cancel, { once: true });
    if (request.signal.aborted) cancel();
    timer = setTimeout(cancel, route.timeout_ms ?? 90000);
    const upstream = await upstreamFetch(route.base_url.replace(/\/+$/, "") +
      (purpose === "embed" ? "/embeddings" : "/chat/completions"), {
      method: "POST", redirect: "manual", signal: controller.signal,
      headers: { "Authorization": `Bearer ${env[route.key_secret]}`, "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!upstream.ok) {
      await upstream.body?.cancel();
      // Never return a provider's body, cookies, credentials or redirect location.
      throw new GatewayError(upstream.status >= 400 ? upstream.status : 502, "upstream_error");
    }
    if (purpose === "embed") {
      const data = await readJson(upstream.body, 16 * 1024 * 1024);
      const expectedCount = Array.isArray(payload.input) ? payload.input.length : 1;
      // Google's OpenAI endpoint omits the default zero index. Restore only
      // that first index; all remaining order/count/vector checks still apply.
      if (new URL(route.base_url).hostname === "generativelanguage.googleapis.com"
          && Array.isArray(data.data) && data.data[0]
          && typeof data.data[0] === "object" && data.data[0].index == null) {
        data.data[0].index = 0;
      }
      if (!Array.isArray(data.data) || data.data.length !== expectedCount
          || !data.data.every((item, index) => item?.index === index
            && Array.isArray(item.embedding) && item.embedding.length === route.dimensions
            && item.embedding.every((n: unknown) => typeof n === "number" && Number.isFinite(n)))) {
        throw new GatewayError(502, "invalid_embedding_response");
      }
      cleanup();
      log(200);
      return Response.json(data, { headers });
    }
    if (!upstream.body) throw new GatewayError(502, "empty_upstream_response");
    const reader = upstream.body.getReader();
    const body = new ReadableStream<Uint8Array>({
      async pull(stream) {
        try {
          const { done, value } = await reader.read();
          if (done) { cleanup(); log(200); stream.close(); }
          else stream.enqueue(value);
        } catch {
          cleanup(); controller.abort(); log(502);
          stream.error(new Error("upstream_stream_error"));
        }
      },
      async cancel() {
        cleanup(); controller.abort(); log(499);
        await reader.cancel().catch(() => {});
      },
    });
    return new Response(body, { headers: {
      ...headers, "content-type": payload.stream === true ? "text/event-stream" : "application/json",
    } });
  } catch (error) {
    cleanup(); controller.abort();
    const status = error instanceof GatewayError ? error.status : 502;
    const code = error instanceof GatewayError ? error.code : "upstream_unavailable";
    log(status);
    return Response.json({ error: { message: code, type: "gateway_error", code } }, { status, headers });
  }
}

export default { fetch(request: Request, env: Env) { return handle(request, env); } } satisfies ExportedHandler<Env>;
