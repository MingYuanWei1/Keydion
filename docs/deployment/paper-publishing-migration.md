# Paper publishing migration runbook

This is the production procedure for moving a legacy Keydion database and its
flat PDF storage to the stable Paper identity schema. Application startup only
validates the Alembic revision; it never performs this offline migration.
`docker-compose.prod.yml` is not authoritative for production. These commands
assume the tracked host units and the concrete `/Keydion` deployment.

Use one maintenance window and one operator for the whole procedure. The
database, Paper storage, pending Submission storage, and checked-out release
form one restore boundary: never restore or advance only one of them.
Begin while `/Keydion` still contains the release currently serving production:
fetch the candidate commit, but do not check it out and do not run `git pull`.
Open one root Bash, enable `set -euo pipefail`, and keep that shell open for all
steps through restart. A failed command or any failed member of a logging or
backup pipeline then closes the shell and stops the procedure.

## 1. Verify the host and tracked units

Set operator-only variables in a root Bash. The MySQL option file must contain
the backup account credentials and must be mode `0600`; do not put credentials
in shell history or this repository. Enter the candidate as its full, lowercase
40-character commit SHA, not a branch, tag, or abbreviated SHA.

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077

export KEYDION_ROOT=/Keydion
export KEYDION_DATABASE=keydion
export KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
export KEYDION_BACKUP_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
read -r -p "Candidate full commit SHA: " KEYDION_NEW_RELEASE
export KEYDION_NEW_RELEASE

[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify \
  "${KEYDION_NEW_RELEASE}^{commit}")" = "$KEYDION_NEW_RELEASE"
export KEYDION_OLD_RELEASE="$(sudo -u keydion git -C \
  "$KEYDION_ROOT" rev-parse --verify HEAD)"
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
test "$KEYDION_OLD_RELEASE" != "$KEYDION_NEW_RELEASE"
test -z "$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=no)"

test "$(realpath "$KEYDION_ROOT")" = /Keydion
id keydion
test "$(stat -c '%a' "$KEYDION_MYSQL_DEFAULTS")" = 600
test -f "$KEYDION_ROOT/.env.prod"
chown root:keydion "$KEYDION_ROOT/.env.prod"
chmod 0640 "$KEYDION_ROOT/.env.prod"

# Treat EnvironmentFile values as data. Never source or eval a service-owned
# file in this privileged shell. This deliberately supports only one exact
# KEY=value assignment per requested key, with an optional matching quote pair.
read_dotenv_value() {
  local key="$1"
  local required="${2:-1}"
  local line value="" match_count=0

  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      "$key="*)
        match_count=$((match_count + 1))
        value="${line#*=}"
        ;;
    esac
  done < "$KEYDION_ROOT/.env.prod"

  if test "$match_count" -eq 0; then
    if test "$required" -eq 0; then
      printf ''
      return 0
    fi
    printf '%s is required in .env.prod\n' "$key" >&2
    return 1
  fi
  if test "$match_count" -ne 1; then
    printf '%s must appear exactly once in .env.prod\n' "$key" >&2
    return 1
  fi

  if test "${value:0:1}" = '"'; then
    test "${#value}" -ge 2 && test "${value: -1}" = '"' || {
      printf '%s has unmatched double quotes in .env.prod\n' "$key" >&2
      return 1
    }
    value="${value:1:${#value}-2}"
  elif test "${value:0:1}" = "'"; then
    test "${#value}" -ge 2 && test "${value: -1}" = "'" || {
      printf '%s has unmatched single quotes in .env.prod\n' "$key" >&2
      return 1
    }
    value="${value:1:${#value}-2}"
  elif [[ "$value" =~ [[:space:]] ]]; then
    printf '%s must quote values containing whitespace in .env.prod\n' \
      "$key" >&2
    return 1
  fi

  if test "$required" -eq 1 && test -z "$value"; then
    printf '%s must not be empty in .env.prod\n' "$key" >&2
    return 1
  fi
  printf '%s' "$value"
}

export PAPERQUERY_DATABASE_URL="$(read_dotenv_value \
  PAPERQUERY_DATABASE_URL)"
export PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
export PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"

case "${PAPERQUERY_DATA_DIR:-}" in
  ""|/Keydion/data) ;;
  *) printf '%s\n' \
       'PAPERQUERY_DATA_DIR must be empty or /Keydion/data' >&2; exit 1 ;;
esac
case "${PAPERQUERY_UPLOAD_DIR:-}" in
  ""|/Keydion/papers) ;;
  *) printf '%s\n' \
       'PAPERQUERY_UPLOAD_DIR must be empty or /Keydion/papers' >&2; exit 1 ;;
esac

export KEYDION_DATA_DIR="$(realpath -m "$KEYDION_ROOT/data")"
export KEYDION_PAPERS_DIR="$(realpath -m "$KEYDION_ROOT/papers")"
export KEYDION_PENDING_DIR="$(realpath -m \
  "$KEYDION_DATA_DIR/pending_papers")"
test "$KEYDION_DATA_DIR" = /Keydion/data
test "$KEYDION_PAPERS_DIR" = /Keydion/papers
test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers

sudo install -d -o keydion -g keydion -m 0750 \
  "$KEYDION_PAPERS_DIR" "$KEYDION_PENDING_DIR" \
  "$KEYDION_ROOT/resource_files" "$KEYDION_ROOT/static/uploads" \
  /var/log/keydion
sudo -u keydion test -r "$KEYDION_ROOT/.env.prod"
sudo -u keydion test -x "$KEYDION_ROOT/.venv/bin/python"
sudo -u keydion test -r "$KEYDION_PAPERS_DIR"
sudo -u keydion test -w "$KEYDION_PAPERS_DIR"
sudo -u keydion test -r "$KEYDION_PENDING_DIR"
sudo -u keydion test -w "$KEYDION_PENDING_DIR"
sudo -u keydion test -w /var/log/keydion
df -h "$KEYDION_PAPERS_DIR" "$KEYDION_PENDING_DIR"
df -i "$KEYDION_PAPERS_DIR" "$KEYDION_PENDING_DIR"
findmnt -T "$KEYDION_PAPERS_DIR"
findmnt -T "$KEYDION_PENDING_DIR"
stat -c '%d %n' "$KEYDION_PAPERS_DIR" "$KEYDION_PENDING_DIR"
systemctl cat keydion.service >/dev/null
sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
  "$KEYDION_NEW_RELEASE:deploy/keydion.service"
sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
  "$KEYDION_NEW_RELEASE:deploy/keydion-publishing-worker.service"

# Resolve the application's target through its own SQLAlchemy stack as the
# unprivileged service account. Resolve the backup target independently through
# the root-only MySQL option file, then require the same server UUID and schema.
export KEYDION_APPLICATION_DB_IDENTITY="$(
  sudo -u keydion \
    --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
    "$KEYDION_ROOT/.venv/bin/python" - <<'PY'
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
expected_database = os.environ["KEYDION_DATABASE"]
if url.get_backend_name() != "mysql":
    raise SystemExit("PAPERQUERY_DATABASE_URL must use MySQL")
if url.database != expected_database:
    raise SystemExit(
        "PAPERQUERY_DATABASE_URL must select the keydion database"
    )

engine = create_engine(url, pool_pre_ping=True)
try:
    with engine.connect() as connection:
        server_uuid, database = connection.execute(
            text("SELECT @@GLOBAL.server_uuid, DATABASE()")
        ).one()
    print(f"{server_uuid}\t{database}")
finally:
    engine.dispose()
PY
)"
export KEYDION_BACKUP_DB_IDENTITY="$(
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()'
)"

validate_db_identity() {
  local label="$1" identity="$2"
  local server_uuid database extra
  test "${identity//$'\n'/}" = "$identity"
  IFS=$'\t' read -r server_uuid database extra <<< "$identity"
  [[ "$server_uuid" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]] || {
    printf '%s returned an invalid MySQL server UUID\n' "$label" >&2
    return 1
  }
  test "$database" = "$KEYDION_DATABASE"
  test -z "$extra"
}
validate_db_identity application "$KEYDION_APPLICATION_DB_IDENTITY"
validate_db_identity backup "$KEYDION_BACKUP_DB_IDENTITY"
test "$KEYDION_APPLICATION_DB_IDENTITY" = "$KEYDION_BACKUP_DB_IDENTITY"
```

Expected checkpoint: the account is exactly `keydion`; the repository is
exactly `/Keydion`; `KEYDION_OLD_RELEASE` is the clean, currently checked-out
production commit; `KEYDION_NEW_RELEASE` is a different, already-fetched full
commit SHA; and neither checkout nor the installed units have changed. The
environment file must leave `PAPERQUERY_DATA_DIR` and
`PAPERQUERY_UPLOAD_DIR` empty or set them to the exact defaults named above.
Non-default storage is not supported by this runbook. `.env.prod`, the shared
`.venv`, both resolved storage roots, and logs have the tested access; disk
space and inodes cover the database dump plus another full PDF copy. The
root-controlled environment file was parsed without executing it; and the
application and backup credential paths reported the same MySQL server UUID
and exact `keydion` schema. The preflight must later report no
`insufficient_disk` or `cross_device_staging` blocker.

## 2. Stop the worker and web service

Close new traffic at the load balancer/nginx maintenance gate, allow active
requests to drain, then stop the job claimant before Gunicorn:

```bash
if systemctl cat keydion-publishing-worker.service >/dev/null 2>&1; then
  sudo systemctl stop keydion-publishing-worker
  test "$(systemctl is-active keydion-publishing-worker)" = inactive
fi
sudo systemctl stop keydion
test "$(systemctl is-active keydion)" = inactive
```

Expected checkpoint: both commands report `inactive`, no Gunicorn process is
serving traffic, and no publishing worker can claim or reconcile a job.

## 3. Take coordinated database and filesystem backups

The stopped services make the following sequential snapshots one coordinated
point. Record every artifact location before continuing.

```bash
sudo install -d -m 0700 "$KEYDION_BACKUP_DIR"
sudo install -d -m 0700 "$KEYDION_BACKUP_DIR/systemd"
printf '%s\n' "$KEYDION_OLD_RELEASE" \
  > "$KEYDION_BACKUP_DIR/old-release.txt"
printf '%s\n' "$KEYDION_NEW_RELEASE" \
  > "$KEYDION_BACKUP_DIR/new-release.txt"
printf '%s\n' "$KEYDION_APPLICATION_DB_IDENTITY" \
  > "$KEYDION_BACKUP_DIR/database-identity.txt"

if test -e /etc/systemd/system/keydion.service; then
  cp --dereference --preserve=mode,ownership,timestamps \
    /etc/systemd/system/keydion.service \
    "$KEYDION_BACKUP_DIR/systemd/keydion.service"
else
  : > "$KEYDION_BACKUP_DIR/systemd/keydion.service.absent"
fi
if test -e /etc/systemd/system/keydion-publishing-worker.service; then
  cp --dereference --preserve=mode,ownership,timestamps \
    /etc/systemd/system/keydion-publishing-worker.service \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
else
  : > "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service.absent"
fi

sudo mysqldump \
  --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  --single-transaction --routines --triggers --events \
  --set-gtid-purged=OFF "$KEYDION_DATABASE" \
  | gzip -1 > "$KEYDION_BACKUP_DIR/database.sql.gz"
sudo tar --acls --xattrs --numeric-owner -cpf \
  "$KEYDION_BACKUP_DIR/papers.tar" \
  -C "$(dirname "$KEYDION_PAPERS_DIR")" \
  "$(basename "$KEYDION_PAPERS_DIR")"
sudo tar --acls --xattrs --numeric-owner -cpf \
  "$KEYDION_BACKUP_DIR/pending-papers.tar" \
  -C "$(dirname "$KEYDION_PENDING_DIR")" \
  "$(basename "$KEYDION_PENDING_DIR")"

test -s "$KEYDION_BACKUP_DIR/database.sql.gz"
test -s "$KEYDION_BACKUP_DIR/papers.tar"
test -s "$KEYDION_BACKUP_DIR/pending-papers.tar"
gzip -t "$KEYDION_BACKUP_DIR/database.sql.gz"
tar -tf "$KEYDION_BACKUP_DIR/papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/pending-papers.tar" >/dev/null
(
  cd "$KEYDION_BACKUP_DIR"
  find database.sql.gz papers.tar pending-papers.tar \
    database-identity.txt old-release.txt new-release.txt systemd \
    -type f -print0 \
    | sort -z | xargs -0 sha256sum > SHA256SUMS
  sha256sum -c SHA256SUMS
)
```

Expected checkpoint: the database backup, `papers.tar`, `pending-papers.tar`,
the verified database identity, old and candidate release identifiers, prior
web/worker unit files or explicit `.absent` markers, and checksums exist
together under the recorded backup directory. Copy the whole directory to the
approved off-host backup destination and run `sha256sum -c SHA256SUMS` there.
Do not continue until the off-host verification succeeds.

Only after that coordinated backup is verified may the checkout and installed
units move to the candidate release:

```bash
sudo -u keydion git -C "$KEYDION_ROOT" checkout --detach "$KEYDION_NEW_RELEASE"
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_NEW_RELEASE"
sudo install -m 0644 "$KEYDION_ROOT/deploy/keydion.service" \
  /etc/systemd/system/keydion.service
sudo install -m 0644 \
  "$KEYDION_ROOT/deploy/keydion-publishing-worker.service" \
  /etc/systemd/system/keydion-publishing-worker.service
sudo systemctl daemon-reload
sudo systemd-analyze verify /etc/systemd/system/keydion.service \
  /etc/systemd/system/keydion-publishing-worker.service
```

Expected checkpoint: `HEAD` is exactly the recorded candidate SHA and both
installed units validate. A checkout, install, or verification failure stops
the strict shell; do not run Alembic. Restore the snapshot boundary instead.

## 4. Run the read-only preflight

Load the same environment used by both units and run the inventory as the
service account. The tool reads SQL and files but does not mutate either.

```bash
cd "$KEYDION_ROOT"
sudo -u keydion \
  --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" \
  tools/preflight_publishing_migration.py \
  --papers-dir "$KEYDION_PAPERS_DIR" \
  2>&1 | tee "$KEYDION_BACKUP_DIR/preflight.txt"
```

Expected checkpoint:

- MySQL is 9.x, the schema is the exact recognized legacy or resumable shape,
  all relevant tables use InnoDB, and identity/text columns satisfy utf8mb4.
- `metadata_count`, `flat_pdf_count`, `total_pdf_bytes`, Submission-state
  counts, `chunk_count`, and `vector_count` are recorded for later comparison.
- The PDF inventory was hashed during preflight; the stage and Paper directory
  are on the same device; disk and inode capacity are sufficient.
- `blockers=0`. Any exit `2`, `blocking=yes`, `wrong_mysql_version`,
  `unexpected_legacy_schema`, `non_innodb_table`, `non_utf8mb4_column`,
  `insufficient_disk`, or `cross_device_staging` stops the procedure.

Blocking data findings also stop the procedure: `missing_pdf`, unsafe or
unresolved filenames, normalized `alias_collision` findings, and unmapped or
`duplicate_chunk` rows must be repaired before contraction. Take a new
coordinated backup and rerun preflight after repair.

`submission_unmatched` and `submission_ambiguous` are the only nonblocking
findings. They describe migrated accepted Submissions whose historical link
cannot be proven. They remain audit issues with `paper_id = NULL`; never guess
the Paper relationship merely to make the report empty.

## 5. Stamp only a validated legacy baseline

Inspect the current revision before stamping:

```bash
cd "$KEYDION_ROOT"
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic current
```

If and only if the command reports no revision, the preflight exited `0`, and
its schema checkpoint says this is the exact legacy baseline, stamp it:

```bash
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" \
  -m alembic stamp 0000_legacy_baseline
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic current
```

Expected checkpoint: `0000_legacy_baseline`. Never stamp over an existing
Alembic revision, a partially expanded schema, or a failed migration. Restore
the coordinated snapshots instead of changing the version marker to bypass a
shape error.

## 6. Upgrade or safely resume

Open the explicit maintenance fence and upgrade through expand, resumable
backfill, and contract:

```bash
cd "$KEYDION_ROOT"
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  env PAPERQUERY_DATA_DIR="$KEYDION_DATA_DIR" \
  PAPERQUERY_UPLOAD_DIR="$KEYDION_PAPERS_DIR" \
  PAPERQUERY_PUBLISHING_MAINTENANCE=1 \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic upgrade head \
  2>&1 | tee "$KEYDION_BACKUP_DIR/alembic-upgrade.txt"
```

The backfill journals verified source hashes, copied revisions, chunk mapping,
and database completion. If the process is interrupted, keep traffic closed,
preserve the logs, rerun the read-only preflight, and review
`publishing_migration_issues`. When preflight still recognizes the resumable
shape with no blockers, rerun the identical `PAPERQUERY_PUBLISHING_MAINTENANCE=1`
`alembic upgrade head` command. If it reports an unsafe partial shape or changed
file/hash, do not improvise; use the rollback procedure.

## 7. Validate the migrated state

First verify the revision and ORM drift:

```bash
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic current
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic check
```

Expected output is exactly `0003_publishing_contract (head)` and
`No new upgrade operations detected.`

Use the backup account to inspect counts, issues, hashes, and vector bytes:

```bash
mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  --batch --raw "$KEYDION_DATABASE" <<'SQL'
SELECT name, paper_count, submission_count, chunk_count, vector_count,
       ddl_phase
FROM publishing_migration_state;

SELECT kind, legacy_key, paper_id, blocking, resolved_at, details
FROM publishing_migration_issues
ORDER BY blocking DESC, kind, legacy_key;

SELECT COUNT(*) AS journal_count,
       SUM(checkpoint = 'db_complete') AS completed_count,
       SUM(source_sha256 IS NOT NULL) AS source_hash_count,
       SUM(source_size_bytes IS NOT NULL) AS source_size_count,
       SUM(legacy_chunk_fingerprint IS NOT NULL) AS chunk_fingerprint_count
FROM publishing_migration_journal;

SELECT COUNT(*) AS hash_or_size_mismatches
FROM publishing_migration_journal j
JOIN paper_revisions r
  ON r.paper_id = j.paper_id AND r.revision_number = j.revision_number
WHERE r.sha256 <> j.source_sha256 OR r.size_bytes <> j.source_size_bytes;

SELECT COUNT(*) AS paper_count FROM papers_metadata;
SELECT COUNT(*) AS revision_count FROM paper_revisions;
SELECT COUNT(*) AS chunk_count,
       SUM(embedding_vec IS NOT NULL) AS vector_count,
       COALESCE(SUM(OCTET_LENGTH(embedding_vec)), 0) AS vector_bytes
FROM papers_chunks;
SELECT COUNT(*) AS aliases_without_papers
FROM paper_filename_aliases a
LEFT JOIN papers_metadata p ON p.id = a.paper_id
WHERE p.id IS NULL;
SELECT COUNT(*) AS chunks_without_revisions
FROM papers_chunks c
LEFT JOIN paper_revisions r
  ON r.paper_id = c.paper_id AND r.revision_number = c.revision_number
WHERE r.paper_id IS NULL;
SQL
```

Expected checkpoint: `ddl_phase=complete`; journal and completed counts match;
`hash_or_size_mismatches`, `aliases_without_papers`, and
`chunks_without_revisions` are zero; Paper/Submission/chunk/`vector_count`
values reconcile with the preflight and `publishing_migration_state`; every
legacy chunk fingerprint and source SHA-256 is present where its original data
existed. Only reviewed nonblocking Submission issues may remain, with null Paper
links.

## 8. Smoke-test the lifecycle

With production services still stopped, run the route contracts as `keydion`
with an explicit disposable SQLite URL and disposable data/upload roots. This
environment is set before Python imports application configuration, so even an
unexpected database initialization cannot target the production MySQL schema.
The tests exercise canonical UUID routes, legacy redirects, direct publication,
accepted Submissions, and indexing-failure feedback:

```bash
cd "$KEYDION_ROOT"
(
  set -euo pipefail
  KEYDION_TEST_ROOT="$(mktemp -d \
    /tmp/keydion-publishing-smoke.XXXXXX)"
  trap 'rm -rf -- "$KEYDION_TEST_ROOT"' EXIT
  chown keydion:keydion "$KEYDION_TEST_ROOT"
  install -d -o keydion -g keydion -m 0700 \
    "$KEYDION_TEST_ROOT/data" "$KEYDION_TEST_ROOT/papers"
  sudo -u keydion env \
    PAPERQUERY_DATABASE_URL="sqlite:///$KEYDION_TEST_ROOT/routes.sqlite" \
    PAPERQUERY_DATA_DIR="$KEYDION_TEST_ROOT/data" \
    PAPERQUERY_UPLOAD_DIR="$KEYDION_TEST_ROOT/papers" \
    PAPERQUERY_SECRET=isolated-route-contract-secret \
    PAPERQUERY_ALLOW_DEV_SECRET=1 \
    PAPERQUERY_COOKIE_SECURE=0 \
    "$KEYDION_ROOT/.venv/bin/python" -m unittest \
    tests/test_paper_identity_routes.py \
    tests/test_publishing_mutation_routes.py -v
)
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" \
  -m tools.publishing_worker --status
```

Expected checkpoint: tests report `OK`; worker status prints
`pending=<n> running=<n> oldest_age_seconds=<n|none>` without mutation. The
contract proves `/paper/<paper_id>` is canonical and
`/preview/<legacy_filename>` returns a permanent redirect. It also proves that
publication remains successful when indexing fails and reports exactly:

- `%(paper_name)s uploaded successfully, but RAG indexing failed.`
- `%(paper_name)s published successfully, but RAG indexing failed.`

Do not point a generic unittest command at the production application schema.

## 9. Restart worker and web, then reopen traffic

Enable and start the two units independently, worker first:

```bash
sudo systemctl enable keydion-publishing-worker
sudo systemctl start keydion-publishing-worker
sudo systemctl enable keydion
sudo systemctl start keydion
sudo systemctl status keydion-publishing-worker --no-pager
sudo systemctl status keydion --no-pager
sudo journalctl -u keydion-publishing-worker -n 50 --no-pager
sudo journalctl -u keydion -n 50 --no-pager
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" \
  -m tools.publishing_worker --status
```

While the maintenance gate remains closed, perform one read-only live request
to a recorded canonical UUID URL and its recorded legacy alias. Confirm the
canonical response succeeds, the legacy response is `301` to that UUID, and no
new migration or worker error appears. Then reopen traffic and monitor both
journals and queue age. Gunicorn `post_fork` warms only the RAG snapshot; it
does not start a publishing worker.

## Rollback

Rollback is snapshot restoration, never `alembic downgrade`. Close traffic and
restore the old release, database backup, both filesystem snapshots, and both
prior systemd-unit presence states together from the same backup directory.
If the original strict root Bash is no longer open, start a new one, enter the
recorded backup ID, and initialize only these fixed paths:

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077
export KEYDION_ROOT=/Keydion
export KEYDION_DATABASE=keydion
export KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
read -r -p "Backup ID (UTC timestamp): " KEYDION_BACKUP_ID
[[ "$KEYDION_BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
export KEYDION_DATA_DIR="$(realpath -m "$KEYDION_ROOT/data")"
export KEYDION_PAPERS_DIR="$(realpath -m "$KEYDION_ROOT/papers")"
export KEYDION_PENDING_DIR="$(realpath -m \
  "$KEYDION_DATA_DIR/pending_papers")"
test "$KEYDION_PAPERS_DIR" = /Keydion/papers
test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers

if systemctl cat keydion-publishing-worker.service >/dev/null 2>&1; then
  sudo systemctl stop keydion-publishing-worker
fi
sudo systemctl stop keydion
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c SHA256SUMS
)
gzip -t "$KEYDION_BACKUP_DIR/database.sql.gz"
tar -tf "$KEYDION_BACKUP_DIR/papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/pending-papers.tar" >/dev/null

KEYDION_OLD_RELEASE="$(cat "$KEYDION_BACKUP_DIR/old-release.txt")"
KEYDION_NEW_RELEASE="$(cat "$KEYDION_BACKUP_DIR/new-release.txt")"
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
KEYDION_CURRENT_RELEASE="$(sudo -u keydion git -C \
  "$KEYDION_ROOT" rev-parse --verify HEAD)"
case "$KEYDION_CURRENT_RELEASE" in
  "$KEYDION_OLD_RELEASE"|"$KEYDION_NEW_RELEASE") ;;
  *)
    printf 'Current release is neither recorded rollback boundary: %s\n' \
      "$KEYDION_CURRENT_RELEASE" >&2
    exit 1
    ;;
esac
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify \
  "${KEYDION_OLD_RELEASE}^{commit}")" = "$KEYDION_OLD_RELEASE"
sudo -u keydion git -C "$KEYDION_ROOT" checkout --detach \
  "$KEYDION_OLD_RELEASE"
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_OLD_RELEASE"

KEYDION_EXPECTED_DB_IDENTITY="$(cat \
  "$KEYDION_BACKUP_DIR/database-identity.txt")"
KEYDION_BACKUP_DB_IDENTITY="$(
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()'
)"
test "$KEYDION_BACKUP_DB_IDENTITY" = "$KEYDION_EXPECTED_DB_IDENTITY"

mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  -e "DROP DATABASE IF EXISTS \`${KEYDION_DATABASE}\`; CREATE DATABASE \`${KEYDION_DATABASE}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
gzip -dc "$KEYDION_BACKUP_DIR/database.sql.gz" \
  | mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" "$KEYDION_DATABASE"

test ! -e "${KEYDION_PAPERS_DIR}.failed-${KEYDION_BACKUP_ID}"
test ! -e "${KEYDION_PENDING_DIR}.failed-${KEYDION_BACKUP_ID}"
sudo mv "$KEYDION_PAPERS_DIR" \
  "${KEYDION_PAPERS_DIR}.failed-${KEYDION_BACKUP_ID}"
sudo mv "$KEYDION_PENDING_DIR" \
  "${KEYDION_PENDING_DIR}.failed-${KEYDION_BACKUP_ID}"
sudo tar --acls --xattrs --numeric-owner -xpf \
  "$KEYDION_BACKUP_DIR/papers.tar" \
  -C "$(dirname "$KEYDION_PAPERS_DIR")"
sudo tar --acls --xattrs --numeric-owner -xpf \
  "$KEYDION_BACKUP_DIR/pending-papers.tar" \
  -C "$(dirname "$KEYDION_PENDING_DIR")"

if test -f "$KEYDION_BACKUP_DIR/systemd/keydion.service.absent"; then
  sudo rm -f /etc/systemd/system/keydion.service
else
  sudo cp --preserve=mode,ownership,timestamps \
    "$KEYDION_BACKUP_DIR/systemd/keydion.service" \
    /etc/systemd/system/keydion.service
fi
if test -f \
  "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service.absent"; then
  sudo systemctl disable keydion-publishing-worker.service
  sudo rm -f /etc/systemd/system/keydion-publishing-worker.service
else
  sudo cp --preserve=mode,ownership,timestamps \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service" \
    /etc/systemd/system/keydion-publishing-worker.service
fi
sudo systemctl daemon-reload
```

Every checksum, archive-integrity check, and MySQL server/schema identity check
occurs before the database is dropped. Rollback accepts `HEAD` at either the
recorded old release (for an early candidate-checkout failure) or the recorded
candidate release, and rejects every other checkout. Because rollback also uses
`set -euo pipefail`, any checkout, import, extraction, unit restoration, or
`daemon-reload` failure exits immediately; leave traffic closed and do not
start either service. Verify the restored revision, Paper/pending counts and
hashes, `HEAD`, and the recorded old unit presence before independently
starting only the services that existed in the snapshot. Reopen traffic only
after the database, both filesystem snapshots, old release, and old unit state
are confirmed to belong together.

## Retention boundary

Keep legacy flat PDFs and legacy filename/vector columns for **one verified release** after migration. Their later cleanup is out of scope for this change
and requires its own backup, migration, compatibility, and rollback plan.
