---
status: accepted
---

# Schema evolution uses Alembic migrations

`init_db()` currently combines `create_all()` with ad-hoc `ALTER TABLE` statements whose errors are ignored, so upgrades are unordered and their outcomes cannot be verified reliably. We will use Alembic for ordered schema migrations and backfills; `init_db()` may initialize a fresh database but will no longer evolve an existing schema.

The adoption rollout preserves every existing Paper and Submission, backfills stable UUID identifiers, assigns each existing Paper an initial revision, migrates its PDF to ID-and-revision storage, and rekeys its RAG data. It automatically links a Submission to a Paper only when the relationship is unambiguous; ambiguous relationships or missing files are reported for review rather than guessed or fabricated. Existing filename-based URLs remain aliases so published links continue to resolve.

## Considered options

- **Continue extending `init_db()`.** Rejected because ignored errors conceal partial upgrades, there is no applied-version history, and backfills and constraints cannot be verified as an ordered sequence.
- **Use custom one-off migration scripts.** Rejected because separate scripts fragment schema history and ordering, making deployed database state harder to determine and reproduce.

## Consequences

Alembic adds migration discipline and an explicit deployment step; a planned maintenance window for the initial upgrade is acceptable. Real MySQL CI must verify upgrades from the current schema, UUID, revision, relationship, and RAG backfills, PDF migration, data preservation, and final constraints. Faster lifecycle tests may use SQLite, but they do not replace MySQL migration verification.
