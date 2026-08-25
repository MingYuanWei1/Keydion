# Operations

## Schema lifecycle

Alembic owns every schema change and migration-coupled data backfill. Runtime startup verifies the current revision and refuses empty, stale, or divergent schemas; it does not create or upgrade them.

Bootstrap only a confirmed-empty database, then verify it:

```bash
python3 -m tools.bootstrap_database --confirm-empty-bootstrap
python3 -m tools.verify_alembic_state
```

Use Alembic and the [publishing migration runbook](../deployment/paper-publishing-migration.md) for existing databases. `tools/migrate_chunk_vectors.py` is retired; do not use standalone migration scripts or ad-hoc `create_all()` calls for application schema evolution.

MySQL 9.x is required for Paper chunk `VECTOR` columns.

## Production

Host-managed nginx and the tracked systemd units are authoritative; `docker-compose.prod.yml` is only a reference stack. Follow [README.md](../../README.md) for schema-neutral deployment and the migration runbook for every schema-changing release.
