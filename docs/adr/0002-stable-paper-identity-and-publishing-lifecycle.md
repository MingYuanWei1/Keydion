---
status: accepted
---

# Papers use stable identity and one deep publishing lifecycle

A Paper has an immutable opaque UUID independent of its title, authors, filename, and revisions. Historical filename aliases redirect to the canonical Paper-ID URL while the Paper exists and are removed on Paper deletion; PDF content is stored by Paper ID and revision. A Submission remains a permanent, independent review record: acceptance links it to exactly one Paper, rejection links none, and Paper deletion removes that link without deleting the Submission.

A deep `PublishingLifecycle` module owns direct publication, Submission acceptance and rejection, revision, restoration, Paper deletion, and index recovery. It presents one structured, intent-based interface to HTTP and worker adapters; database, PDF storage, RAG, and durable-job adapters sit at internal seams in its implementation. Metadata auto-fill stays outside this seam because it is advisory input reviewed before publication, preserving ADR-0001.

A Paper becomes visible only after both its database record and PDF are durable. RAG data is derived and nonblocking: publication records durable indexing work, makes one post-publication indexing attempt, and returns a structured `indexing_failed` outcome if that attempt fails. The HTTP adapter then warns that the Paper was uploaded successfully but RAG indexing failed, while a database-backed worker retries idempotently with backoff. RAG chunks carry the Paper ID and revision, and retrieval accepts only chunks belonging to the current visible revision.

Earlier revisions remain private. Restoring earlier content appends a new revision rather than rewriting history. Paper writes use optimistic concurrency so stale edits cannot replace newer state. Submission decisions are idempotent: repeating the same decision returns its original outcome, while a conflicting later decision is rejected. Direct publication is also idempotent, so retrying the same request cannot create a second Paper.

Paper deletion first makes the Paper inaccessible, then durably retries until every Paper-owned record, PDF revision, filename alias, RAG chunk, and job is removed. It reports success only after cleanup completes. The originating Submission remains, unlinked from the deleted Paper.

## Considered options

- **Keep filename as Paper identity.** Rejected because a rename then changes identity and couples public links, storage, database relationships, and RAG data.
- **Keep publication implementation in each HTTP route.** Rejected because database, PDF, Submission, and indexing invariants would remain duplicated across direct publication, acceptance, revision, and deletion, with poor locality and a route-specific test surface.
- **Require RAG success before publication succeeds.** Rejected because RAG is derived data and an unavailable embedding dependency must not invalidate an otherwise durable Paper.
- **Archive a removed Paper.** Rejected because removal is defined as permanent deletion of all Paper-owned data; only the independent Submission review record survives.

## Consequences

Existing Papers require UUIDs, revision paths, Submission links, and filename aliases to be backfilled. A durable-job worker becomes required infrastructure, and retained private revisions increase storage use. Tests cross the `PublishingLifecycle` interface using production and test adapters, giving every publishing entry point one high-leverage test surface.
