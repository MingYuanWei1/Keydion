# Paper publishing migration runbook

This is the production procedure for moving a legacy Keydion database and its
flat PDF storage to the stable Paper identity schema. Application startup only
validates the Alembic revision; it never performs this offline migration.
`docker-compose.prod.yml` is not authoritative for production. These commands
assume the tracked host units and the concrete `/Keydion` deployment.
If the production account or path differs from `keydion` and `/Keydion`, stop:
update both tracked units together, adapt this runbook in the same change, and
rerun the deployment contract before scheduling the migration. Never edit only
one installed unit or substitute paths ad hoc during the maintenance window.
`deploy/keydion-legacy.service` is a reviewed first-rollout artifact derived
from the former README unit, not a forward service to install. It is consulted
only when the old release does not track `deploy/keydion.service`, and then only
to allowlist an exact byte-for-byte match of the already-installed web unit.
Any mismatch is a hard stop: update the reviewed fixture, this runbook, and its
deployment contracts before the maintenance window. Never edit or copy
installed host bytes to make them match during the window.

Use one maintenance window and one operator for the whole procedure. The
database, Paper storage, pending Submission storage, Python virtual environment,
and checked-out release form one restore boundary: never restore or advance
only one of them.
Begin while `/Keydion` still contains the release currently serving production:
fetch the candidate commit, but do not check it out and do not run `git pull`.
Open one root Bash, enable `set -euo pipefail`, and keep that shell open for all
steps through restart. A failed command or any failed member of a logging or
backup pipeline then closes the shell and stops the procedure.

## 1. Verify the host and tracked units

Set operator-only variables in a root Bash. The MySQL option file must contain
the backup/restore administrative account credentials and must be mode `0600`;
do not put credentials in shell history or this repository. That account must
be able to dump and replay all DDL, DML, routines, triggers, and events and to
create/drop the named restore targets; Section 3 proves those rights with an
isolated restore rehearsal before cutover. Enter the candidate as its full,
lowercase 40-character commit SHA, not a branch, tag, or abbreviated SHA.

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077

KEYDION_RECOVERY_LOCK=/run/lock/keydion-paper-publishing.lock
test -d /run/lock
test ! -L /run/lock
test "$(stat -c '%U:%G' /run/lock)" = root:root
if ! test -e "$KEYDION_RECOVERY_LOCK"; then
  (set -o noclobber; : > "$KEYDION_RECOVERY_LOCK") 2>/dev/null \
    || test -f "$KEYDION_RECOVERY_LOCK"
fi
test -f "$KEYDION_RECOVERY_LOCK"
test ! -L "$KEYDION_RECOVERY_LOCK"
test "$(stat -c '%U:%G:%a' "$KEYDION_RECOVERY_LOCK")" = root:root:600
exec {KEYDION_RECOVERY_LOCK_FD}<>"$KEYDION_RECOVERY_LOCK"
flock --exclusive --nonblock "$KEYDION_RECOVERY_LOCK_FD"

export KEYDION_ROOT=/Keydion
export KEYDION_DATABASE=keydion
export KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
KEYDION_BACKUP_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export KEYDION_BACKUP_ID
export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
read -r -p "Candidate full commit SHA: " KEYDION_NEW_RELEASE
export KEYDION_NEW_RELEASE

[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify \
  "${KEYDION_NEW_RELEASE}^{commit}")" = "$KEYDION_NEW_RELEASE"
KEYDION_OLD_RELEASE="$(sudo -u keydion git -C \
  "$KEYDION_ROOT" rev-parse --verify HEAD)"
export KEYDION_OLD_RELEASE
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
test "$KEYDION_OLD_RELEASE" != "$KEYDION_NEW_RELEASE"
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"

test "$(realpath "$KEYDION_ROOT")" = /Keydion
id keydion
test "$(stat -c '%a' "$KEYDION_MYSQL_DEFAULTS")" = 600
test -f "$KEYDION_ROOT/.env.prod"
test ! -L "$KEYDION_ROOT/.env.prod"
test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
  = root:keydion:640

# Treat EnvironmentFile values as data. Never source or eval a service-owned
# file in this privileged shell. This deliberately supports only one exact
# KEY=value assignment per requested key, with an optional matching quote pair.
read_dotenv_value() {
  local key="$1"
  local required="${2:-1}"
  local line value="" match_count=0

  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] || return 1

  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      *\\)
        printf 'EnvironmentFile continuations are not supported here\n' >&2
        return 1
        ;;
    esac
    if [[ "$line" =~ ^[[:space:]]*${key}[[:space:]]*= ]] && \
       [[ "$line" != "$key="* ]]; then
      printf '%s must use exact KEY=value spelling in .env.prod\n' \
        "$key" >&2
      return 1
    fi
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

  if [[ "$value" == *\\* ]]; then
    printf '%s must not use backslash escapes in .env.prod\n' "$key" >&2
    return 1
  fi

  if test "${value:0:1}" = '"'; then
    test "${#value}" -ge 2 && test "${value: -1}" = '"' || {
      printf '%s has unmatched double quotes in .env.prod\n' "$key" >&2
      return 1
    }
    value="${value:1:${#value}-2}"
    if [[ "$value" == *\"* ]]; then
      printf '%s must use one whole quoted value in .env.prod\n' "$key" >&2
      return 1
    fi
  elif test "${value:0:1}" = "'"; then
    test "${#value}" -ge 2 && test "${value: -1}" = "'" || {
      printf '%s has unmatched single quotes in .env.prod\n' "$key" >&2
      return 1
    }
    value="${value:1:${#value}-2}"
    if [[ "$value" == *\'* ]]; then
      printf '%s must use one whole quoted value in .env.prod\n' "$key" >&2
      return 1
    fi
  elif [[ "$value" =~ [[:space:]] ]] || \
       [[ "$value" == *\"* ]] || [[ "$value" == *\'* ]]; then
    printf '%s must use one unquoted or wholly quoted value in .env.prod\n' \
      "$key" >&2
    return 1
  fi

  if test "$required" -eq 1 && test -z "$value"; then
    printf '%s must not be empty in .env.prod\n' "$key" >&2
    return 1
  fi
  printf '%s' "$value"
}

PAPERQUERY_DATABASE_URL="$(read_dotenv_value PAPERQUERY_DATABASE_URL)"
PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"
GUNICORN_BIND="$(read_dotenv_value GUNICORN_BIND)"
export PAPERQUERY_DATABASE_URL PAPERQUERY_DATA_DIR PAPERQUERY_UPLOAD_DIR
KEYDION_JOB_LEASE_SECONDS="$(read_dotenv_value \
  PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS 0)"
KEYDION_JOB_LEASE_SECONDS="${KEYDION_JOB_LEASE_SECONDS:-1800}"
if ! [[ "$KEYDION_JOB_LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  printf '%s\n' \
    'PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS must be a positive integer' >&2
  exit 1
fi
if ! test "$KEYDION_JOB_LEASE_SECONDS" -le 1800; then
  printf '%s\n' \
    'PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS must not exceed 1800 seconds' >&2
  exit 1
fi

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
test "$GUNICORN_BIND" = 127.0.0.1:5000

KEYDION_DATA_DIR="$(realpath -m "$KEYDION_ROOT/data")"
KEYDION_PAPERS_DIR="$(realpath -m "$KEYDION_ROOT/papers")"
KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
export KEYDION_DATA_DIR KEYDION_PAPERS_DIR KEYDION_PENDING_DIR
test "$KEYDION_DATA_DIR" = /Keydion/data
test "$KEYDION_PAPERS_DIR" = /Keydion/papers
test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers

test -d "$KEYDION_PAPERS_DIR"
test ! -L "$KEYDION_PAPERS_DIR"
test "$(realpath "$KEYDION_PAPERS_DIR")" = "$KEYDION_PAPERS_DIR"
test "$(stat -c '%U:%G:%a' "$KEYDION_PAPERS_DIR")" \
  = keydion:keydion:750
test -d "$KEYDION_PENDING_DIR"
test ! -L "$KEYDION_PENDING_DIR"
test "$(realpath "$KEYDION_PENDING_DIR")" = "$KEYDION_PENDING_DIR"
test "$(stat -c '%U:%G:%a' "$KEYDION_PENDING_DIR")" \
  = keydion:keydion:750

read -r -p "Expected Paper storage mount source: " \
  KEYDION_EXPECTED_PAPERS_SOURCE
read -r -p "Expected pending storage mount source: " \
  KEYDION_EXPECTED_PENDING_SOURCE
test -n "$KEYDION_EXPECTED_PAPERS_SOURCE"
test -n "$KEYDION_EXPECTED_PENDING_SOURCE"
KEYDION_PAPERS_SOURCE="$(findmnt --noheadings --raw --output SOURCE \
  --target "$KEYDION_PAPERS_DIR")"
KEYDION_PENDING_SOURCE="$(findmnt --noheadings --raw --output SOURCE \
  --target "$KEYDION_PENDING_DIR")"
test "$KEYDION_PAPERS_SOURCE" = "$KEYDION_EXPECTED_PAPERS_SOURCE"
test "$KEYDION_PENDING_SOURCE" = "$KEYDION_EXPECTED_PENDING_SOURCE"
KEYDION_PAPERS_MOUNT_TARGET="$(findmnt --noheadings --raw --output TARGET \
  --target "$KEYDION_PAPERS_DIR")"
KEYDION_PENDING_MOUNT_TARGET="$(findmnt --noheadings --raw --output TARGET \
  --target "$KEYDION_PENDING_DIR")"
KEYDION_PAPERS_DEVICE="$(stat -c '%d' "$KEYDION_PAPERS_DIR")"
KEYDION_PENDING_DEVICE="$(stat -c '%d' "$KEYDION_PENDING_DIR")"
test "$KEYDION_PAPERS_DEVICE" \
  = "$(stat -c '%d' "$KEYDION_PAPERS_MOUNT_TARGET")"
test "$KEYDION_PENDING_DEVICE" \
  = "$(stat -c '%d' "$KEYDION_PENDING_MOUNT_TARGET")"

test ! -L "$KEYDION_ROOT/resource_files"
if test -e "$KEYDION_ROOT/resource_files"; then
  test -d "$KEYDION_ROOT/resource_files"
  test "$(realpath "$KEYDION_ROOT/resource_files")" \
    = "$KEYDION_ROOT/resource_files"
fi
test -d "$KEYDION_ROOT/static"
test ! -L "$KEYDION_ROOT/static"
test "$(realpath "$KEYDION_ROOT/static")" = "$KEYDION_ROOT/static"
test ! -L "$KEYDION_ROOT/static/uploads"
if test -e "$KEYDION_ROOT/static/uploads"; then
  test -d "$KEYDION_ROOT/static/uploads"
  test "$(realpath "$KEYDION_ROOT/static/uploads")" \
    = "$KEYDION_ROOT/static/uploads"
fi
sudo install -d -o keydion -g keydion -m 0750 \
  "$KEYDION_ROOT/resource_files" "$KEYDION_ROOT/static/uploads" \
  /var/log/keydion
test -d "$KEYDION_ROOT/resource_files"
test ! -L "$KEYDION_ROOT/resource_files"
test "$(realpath "$KEYDION_ROOT/resource_files")" \
  = "$KEYDION_ROOT/resource_files"
test -d "$KEYDION_ROOT/static/uploads"
test ! -L "$KEYDION_ROOT/static/uploads"
test "$(realpath "$KEYDION_ROOT/static/uploads")" \
  = "$KEYDION_ROOT/static/uploads"
sudo -u keydion test -r "$KEYDION_ROOT/.env.prod"
sudo -u keydion test -d "$KEYDION_ROOT/.venv"
test ! -L "$KEYDION_ROOT/.venv"
test "$(realpath "$KEYDION_ROOT/.venv")" = "$KEYDION_ROOT/.venv"
sudo -u keydion test -x "$KEYDION_ROOT/.venv/bin/python"
sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" -m pip check
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

KEYDION_MOUNT_TARGETS="$(findmnt --raw --noheadings --output TARGET)"
assert_rename_tree_not_mounted() {
  local tree="$1"
  local mount_target
  while IFS= read -r mount_target; do
    case "$mount_target" in
      "$tree"|"$tree"/*)
        printf '%s is or contains a mount and cannot use rename rollback\n' \
          "$tree" >&2
        return 1
        ;;
    esac
  done <<< "$KEYDION_MOUNT_TARGETS"
}
assert_rename_tree_not_mounted "$KEYDION_ROOT/.venv"
assert_rename_tree_not_mounted "$KEYDION_PAPERS_DIR"
assert_rename_tree_not_mounted "$KEYDION_PENDING_DIR"
systemctl cat keydion.service >/dev/null

assert_no_systemd_dropins() {
  local unit="$1"
  local dropins
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  if test -n "$dropins"; then
    printf '%s has unexpected systemd DropInPaths: %s\n' \
      "$unit" "$dropins" >&2
    return 1
  fi
}

assert_tracked_unit_fragment() {
  local unit="$1"
  local expected="/etc/systemd/system/$unit"
  local fragment dropins
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  test "$fragment" = "$expected"
  test -f "$expected"
  test ! -L "$expected"
  test -z "$dropins"
}

assert_old_release_unit_provenance() {
  local unit="$1" expected="/etc/systemd/system/$1"
  local fragment dropins release_blob installed_blob
  systemctl cat "$unit" >/dev/null
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  test "$fragment" = "$expected"
  test -z "$dropins"
  test -f "$expected"
  test ! -L "$expected"
  test "$(stat -c '%U:%G:%a' "$expected")" = root:root:644
  sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
    "${KEYDION_OLD_RELEASE}:deploy/$unit"
  release_blob="$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse \
    "${KEYDION_OLD_RELEASE}:deploy/$unit")"
  installed_blob="$(git -C "$KEYDION_ROOT" hash-object "$expected")"
  test "$installed_blob" = "$release_blob"
}

resolve_web_unit_provenance() {
  local unit=keydion.service expected=/etc/systemd/system/keydion.service
  local fragment dropins old_path release_blob installed_blob
  systemctl cat "$unit" >/dev/null
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  test "$fragment" = "$expected"
  test -z "$dropins"
  test -f "$expected"
  test ! -L "$expected"
  test "$(stat -c '%U:%G:%a' "$expected")" = root:root:644

  old_path="$(sudo -u keydion git -C "$KEYDION_ROOT" \
    ls-tree --name-only "$KEYDION_OLD_RELEASE" -- deploy/keydion.service)"
  case "$old_path" in
    deploy/keydion.service)
      KEYDION_WEB_UNIT_ORIGIN=old-release
      KEYDION_WEB_UNIT_SOURCE_RELEASE="$KEYDION_OLD_RELEASE"
      KEYDION_WEB_UNIT_SOURCE_PATH=deploy/keydion.service
      ;;
    "" )
      KEYDION_WEB_UNIT_ORIGIN=candidate-legacy-allowlist
      KEYDION_WEB_UNIT_SOURCE_RELEASE="$KEYDION_NEW_RELEASE"
      KEYDION_WEB_UNIT_SOURCE_PATH=deploy/keydion-legacy.service
      ;;
    *)
      printf 'Unexpected old-release web unit path: %s\n' "$old_path" >&2
      return 1
      ;;
  esac
  sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
    "${KEYDION_WEB_UNIT_SOURCE_RELEASE}:${KEYDION_WEB_UNIT_SOURCE_PATH}"
  release_blob="$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse \
    "${KEYDION_WEB_UNIT_SOURCE_RELEASE}:${KEYDION_WEB_UNIT_SOURCE_PATH}")"
  installed_blob="$(git -C "$KEYDION_ROOT" hash-object "$expected")"
  test "$installed_blob" = "$release_blob"
  KEYDION_WEB_UNIT_GIT_BLOB="$release_blob"
  KEYDION_WEB_UNIT_SHA256="$(sha256sum "$expected" | awk '{print $1}')"
  [[ "$KEYDION_WEB_UNIT_SHA256" =~ ^[0-9a-f]{64}$ ]]
}
assert_no_systemd_dropins keydion.service
assert_tracked_unit_fragment keydion.service

prove_optional_worker_presence() {
  local unit=keydion-publishing-worker.service
  local expected="/etc/systemd/system/$unit"
  local load_state active_state active_status enabled_state enabled_status

  if load_state="$(systemctl show --property=LoadState --value "$unit")"; then
    :
  else
    printf '%s LoadState query failed\n' "$unit" >&2
    return 1
  fi

  case "$load_state" in
    loaded)
      assert_old_release_unit_provenance keydion-publishing-worker.service
      KEYDION_WORKER_UNIT_PRESENT=1
      ;;
    not-found)
      test ! -e "$expected"
      test ! -L "$expected"
      if active_state="$(systemctl is-active "$unit" 2>/dev/null)"; then
        active_status=0
      else
        active_status=$?
      fi
      test "$active_state" = inactive
      test "$active_status" -ne 0
      if enabled_state="$(systemctl is-enabled "$unit" 2>/dev/null)"; then
        enabled_status=0
      else
        enabled_status=$?
      fi
      test "$enabled_state" = not-found
      test "$enabled_status" -ne 0
      KEYDION_WORKER_UNIT_PRESENT=0
      ;;
    *)
      printf '%s has unsupported LoadState: %s\n' \
        "$unit" "$load_state" >&2
      return 1
      ;;
  esac
}

read_unit_enabled_state() {
  local unit="$1"
  local state status
  if state="$(systemctl is-enabled "$unit" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$state" in
    enabled) test "$status" -eq 0 ;;
    disabled) test "$status" -ne 0 ;;
    *) printf '%s has unsupported enabled state: %s\n' \
         "$unit" "$state" >&2; return 1 ;;
  esac
  printf '%s' "$state"
}

read_unit_active_state() {
  local unit="$1"
  local state status
  if state="$(systemctl is-active "$unit" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$state" in
    active) test "$status" -eq 0 ;;
    inactive) test "$status" -ne 0 ;;
    *) printf '%s has unsupported active state: %s\n' \
         "$unit" "$state" >&2; return 1 ;;
  esac
  printf '%s' "$state"
}

sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
  "$KEYDION_NEW_RELEASE:deploy/keydion.service"
sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
  "$KEYDION_NEW_RELEASE:deploy/keydion-publishing-worker.service"

# Resolve the application's target through its own SQLAlchemy stack as the
# unprivileged service account. Resolve the backup target independently through
# the root-only MySQL option file, then require the same server UUID and schema.
KEYDION_APPLICATION_DB_IDENTITY="$(
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
export KEYDION_APPLICATION_DB_IDENTITY
KEYDION_BACKUP_DB_IDENTITY="$(
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()'
)"
export KEYDION_BACKUP_DB_IDENTITY

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
IFS=$'\t' read -r KEYDION_RESTORE_PROBE_SERVER_UUID \
  KEYDION_RESTORE_PROBE_SOURCE_DATABASE KEYDION_RESTORE_PROBE_IDENTITY_EXTRA \
  <<< "$KEYDION_APPLICATION_DB_IDENTITY"
[[ "$KEYDION_RESTORE_PROBE_SERVER_UUID" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
test "$KEYDION_RESTORE_PROBE_SOURCE_DATABASE" = "$KEYDION_DATABASE"
test -z "$KEYDION_RESTORE_PROBE_IDENTITY_EXTRA"
KEYDION_RESTORE_PROBE_SUFFIX="$(
  od -An -N16 -tx1 /dev/urandom | tr -d '[:space:]'
)"
KEYDION_RESTORE_PROBE_TOKEN="$(
  od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]'
)"
[[ "$KEYDION_RESTORE_PROBE_SUFFIX" =~ ^[0-9a-f]{32}$ ]]
[[ "$KEYDION_RESTORE_PROBE_TOKEN" =~ ^[0-9a-f]{64}$ ]]
KEYDION_RESTORE_PROBE_DATABASE="keydion_restore_probe_${KEYDION_RESTORE_PROBE_SUFFIX}"
[[ "$KEYDION_RESTORE_PROBE_DATABASE" =~ ^keydion_restore_probe_[0-9a-f]{32}$ ]]
test "${#KEYDION_RESTORE_PROBE_DATABASE}" -le 64

assert_fresh_database_identity() {
  local application_identity backup_identity expected_identity
  application_identity="$(
    sudo -u keydion \
      --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
      "$KEYDION_ROOT/.venv/bin/python" - <<'PY'
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
if url.get_backend_name() != "mysql" or url.database != os.environ["KEYDION_DATABASE"]:
    raise SystemExit("PAPERQUERY_DATABASE_URL must select the keydion MySQL database")
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
  backup_identity="$(
    mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
      --batch --skip-column-names "$KEYDION_DATABASE" \
      -e 'SELECT @@GLOBAL.server_uuid, DATABASE()'
  )"
  expected_identity="$(cat "$KEYDION_BACKUP_DIR/database-identity.txt")"
  test "$application_identity" = "$expected_identity"
  test "$backup_identity" = "$expected_identity"
}

KEYDION_DATABASE_DEFAULTS="$(
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = DATABASE()"
)"
test "${KEYDION_DATABASE_DEFAULTS//$'\n'/}" = "$KEYDION_DATABASE_DEFAULTS"
IFS=$'\t' read -r KEYDION_DATABASE_CHARACTER_SET \
  KEYDION_DATABASE_COLLATION KEYDION_DATABASE_DEFAULTS_EXTRA \
  <<< "$KEYDION_DATABASE_DEFAULTS"
[[ "$KEYDION_DATABASE_CHARACTER_SET" =~ ^[A-Za-z0-9_]+$ ]]
[[ "$KEYDION_DATABASE_COLLATION" =~ ^[A-Za-z0-9_]+$ ]]
test -z "$KEYDION_DATABASE_DEFAULTS_EXTRA"

KEYDION_DATABASE_ESTIMATED_BYTES="$(
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e "SELECT COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${KEYDION_DATABASE}'"
)"
[[ "$KEYDION_DATABASE_ESTIMATED_BYTES" =~ ^[0-9]+$ ]]
read -r -p \
  "DBA capacity evidence/ticket for one extra ${KEYDION_DATABASE_ESTIMATED_BYTES}-byte schema: " \
  KEYDION_MYSQL_CAPACITY_EVIDENCE
[[ "$KEYDION_MYSQL_CAPACITY_EVIDENCE" =~ ^[A-Za-z0-9._:/-]{3,128}$ ]]

tree_bytes() {
  local tree="$1"
  local bytes
  if ! bytes="$(set -o pipefail; \
      du --bytes --summarize --one-file-system "$tree" \
        | awk '{print $1}')"; then
    printf 'Could not measure bytes for %s\n' "$tree" >&2
    return 1
  fi
  [[ "$bytes" =~ ^[0-9]+$ ]]
  printf '%s' "$bytes"
}

tree_inodes() {
  local tree="$1"
  local inodes
  if ! inodes="$(set -o pipefail; \
      find "$tree" -xdev -printf . | wc -c)"; then
    printf 'Could not count inodes for %s\n' "$tree" >&2
    return 1
  fi
  inodes="${inodes//[[:space:]]/}"
  [[ "$inodes" =~ ^[0-9]+$ ]]
  printf '%s' "$inodes"
}

available_bytes() {
  local path="$1"
  local bytes
  if ! bytes="$(set -o pipefail; \
      df --output=avail -B1 "$path" | awk 'NR == 2 {print $1}')"; then
    printf 'Could not read available bytes for %s\n' "$path" >&2
    return 1
  fi
  [[ "$bytes" =~ ^[0-9]+$ ]]
  printf '%s' "$bytes"
}

available_inodes() {
  local path="$1"
  local inodes
  if ! inodes="$(set -o pipefail; \
      df --output=iavail "$path" | awk 'NR == 2 {print $1}')"; then
    printf 'Could not read available inodes for %s\n' "$path" >&2
    return 1
  fi
  [[ "$inodes" =~ ^[0-9]+$ ]]
  printf '%s' "$inodes"
}

assert_restore_capacity() {
  local path="$1"
  local KEYDION_AVAILABLE_BYTES KEYDION_AVAILABLE_INODES
  KEYDION_AVAILABLE_BYTES="$(available_bytes "$path")"
  KEYDION_AVAILABLE_INODES="$(available_inodes "$path")"
  test "$KEYDION_AVAILABLE_BYTES" \
    -ge "$KEYDION_TOTAL_LOCAL_WORST_CASE_BYTES"
  test "$KEYDION_AVAILABLE_INODES" \
    -ge "$KEYDION_TOTAL_LOCAL_WORST_CASE_INODES"
}

install -d -o root -g root -m 0700 /srv/keydion-backups
test "$(realpath /srv/keydion-backups)" = /srv/keydion-backups
test ! -L /srv/keydion-backups
test ! -e /srv/keydion-backups/paper-publishing-active
test ! -L /srv/keydion-backups/paper-publishing-active
KEYDION_VENV_BYTES="$(tree_bytes "$KEYDION_ROOT/.venv")"
KEYDION_PAPERS_BYTES="$(tree_bytes "$KEYDION_PAPERS_DIR")"
KEYDION_PENDING_BYTES="$(tree_bytes "$KEYDION_PENDING_DIR")"
KEYDION_VENV_INODES="$(tree_inodes "$KEYDION_ROOT/.venv")"
KEYDION_PAPERS_INODES="$(tree_inodes "$KEYDION_PAPERS_DIR")"
KEYDION_PENDING_INODES="$(tree_inodes "$KEYDION_PENDING_DIR")"
KEYDION_BACKUP_REQUIRED_BYTES=$((
  KEYDION_DATABASE_ESTIMATED_BYTES * 2 +
  KEYDION_VENV_BYTES + KEYDION_PAPERS_BYTES + KEYDION_PENDING_BYTES +
  1073741824
))
KEYDION_TOTAL_LOCAL_WORST_CASE_BYTES=$((
  KEYDION_BACKUP_REQUIRED_BYTES + KEYDION_VENV_BYTES +
  KEYDION_PAPERS_BYTES + KEYDION_PENDING_BYTES + 805306368
))
KEYDION_TOTAL_LOCAL_WORST_CASE_INODES=$((
  KEYDION_VENV_INODES + KEYDION_PAPERS_INODES +
  KEYDION_PENDING_INODES + 4096
))
KEYDION_BACKUP_AVAILABLE_BYTES="$(available_bytes /srv/keydion-backups)"
KEYDION_BACKUP_AVAILABLE_INODES="$(available_inodes /srv/keydion-backups)"
test "$KEYDION_BACKUP_AVAILABLE_BYTES" \
  -ge "$KEYDION_TOTAL_LOCAL_WORST_CASE_BYTES"
test "$KEYDION_BACKUP_AVAILABLE_INODES" \
  -ge "$KEYDION_TOTAL_LOCAL_WORST_CASE_INODES"
assert_restore_capacity /srv/keydion-backups
assert_restore_capacity "$KEYDION_ROOT/.venv"
assert_restore_capacity "$KEYDION_PAPERS_DIR"
assert_restore_capacity "$KEYDION_PENDING_DIR"

# Persist the release-bound original service state before the first disable.
# The exclusive active-boundary file prevents a restarted Section 1 from
# recapturing the maintenance state after an interruption.
prove_optional_worker_presence
KEYDION_PROVEN_WORKER_UNIT_PRESENT="$KEYDION_WORKER_UNIT_PRESENT"
resolve_web_unit_provenance
if test "$KEYDION_WORKER_UNIT_PRESENT" -eq 1; then
  assert_old_release_unit_provenance keydion-publishing-worker.service
  KEYDION_WORKER_UNIT_ORIGIN=old-release
  KEYDION_WORKER_UNIT_SOURCE_RELEASE="$KEYDION_OLD_RELEASE"
  KEYDION_WORKER_UNIT_SOURCE_PATH=deploy/keydion-publishing-worker.service
  KEYDION_WORKER_UNIT_GIT_BLOB="$(sudo -u keydion git -C \
    "$KEYDION_ROOT" rev-parse \
    "$KEYDION_OLD_RELEASE:deploy/keydion-publishing-worker.service")"
  KEYDION_WORKER_UNIT_SHA256="$(sha256sum \
    /etc/systemd/system/keydion-publishing-worker.service | awk '{print $1}')"
else
  test ! -e /etc/systemd/system/keydion-publishing-worker.service
  test ! -L /etc/systemd/system/keydion-publishing-worker.service
  KEYDION_WORKER_UNIT_ORIGIN=absent
  KEYDION_WORKER_UNIT_SOURCE_RELEASE=-
  KEYDION_WORKER_UNIT_SOURCE_PATH=-
  KEYDION_WORKER_UNIT_GIT_BLOB=-
  KEYDION_WORKER_UNIT_SHA256=-
fi
mkdir --mode=0700 -- "$KEYDION_BACKUP_DIR"
install -d -m 0700 "$KEYDION_BACKUP_DIR/systemd"
if test "$KEYDION_WORKER_UNIT_PRESENT" -eq 1; then
  cp --preserve=mode,ownership,timestamps \
    /etc/systemd/system/keydion-publishing-worker.service \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
  test "$(stat -c '%U:%G:%a' \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service")" \
    = root:root:644
  test "$(git -C "$KEYDION_ROOT" hash-object \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service")" \
    = "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse \
      "$KEYDION_OLD_RELEASE:deploy/keydion-publishing-worker.service")"
else
  : > "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service.absent"
fi
cp --preserve=mode,ownership,timestamps \
  /etc/systemd/system/keydion.service \
  "$KEYDION_BACKUP_DIR/systemd/keydion.service"
test "$(stat -c '%U:%G:%a' \
  "$KEYDION_BACKUP_DIR/systemd/keydion.service")" = root:root:644
test "$(git -C "$KEYDION_ROOT" hash-object \
  "$KEYDION_BACKUP_DIR/systemd/keydion.service")" \
  = "$KEYDION_WEB_UNIT_GIT_BLOB"
test "$(sha256sum "$KEYDION_BACKUP_DIR/systemd/keydion.service" \
  | awk '{print $1}')" = "$KEYDION_WEB_UNIT_SHA256"
prove_optional_worker_presence
test "$KEYDION_WORKER_UNIT_PRESENT" -eq \
  "$KEYDION_PROVEN_WORKER_UNIT_PRESENT"
KEYDION_WEB_ENABLED_STATE="$(read_unit_enabled_state keydion.service)"
KEYDION_WEB_ACTIVE_STATE="$(read_unit_active_state keydion.service)"
if test "$KEYDION_WORKER_UNIT_PRESENT" -eq 1; then
  KEYDION_WORKER_ENABLED_STATE="$(
    read_unit_enabled_state keydion-publishing-worker.service
  )"
  KEYDION_WORKER_ACTIVE_STATE="$(
    read_unit_active_state keydion-publishing-worker.service
  )"
else
  KEYDION_WORKER_ENABLED_STATE=absent
  KEYDION_WORKER_ACTIVE_STATE=absent
fi
printf 'keydion.service\t%s\t%s\nkeydion-publishing-worker.service\t%s\t%s\n' \
  "$KEYDION_WEB_ENABLED_STATE" "$KEYDION_WEB_ACTIVE_STATE" \
  "$KEYDION_WORKER_ENABLED_STATE" "$KEYDION_WORKER_ACTIVE_STATE" \
  > "$KEYDION_BACKUP_DIR/systemd/unit-state.tsv"
printf 'keydion.service\t%s\t%s\t%s\t%s\t%s\nkeydion-publishing-worker.service\t%s\t%s\t%s\t%s\t%s\n' \
  "$KEYDION_WEB_UNIT_ORIGIN" "$KEYDION_WEB_UNIT_SOURCE_RELEASE" \
  "$KEYDION_WEB_UNIT_SOURCE_PATH" "$KEYDION_WEB_UNIT_GIT_BLOB" \
  "$KEYDION_WEB_UNIT_SHA256" \
  "$KEYDION_WORKER_UNIT_ORIGIN" "$KEYDION_WORKER_UNIT_SOURCE_RELEASE" \
  "$KEYDION_WORKER_UNIT_SOURCE_PATH" "$KEYDION_WORKER_UNIT_GIT_BLOB" \
  "$KEYDION_WORKER_UNIT_SHA256" \
  > "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv"
test "$(stat -c '%U:%G:%a' \
  "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv")" = root:root:600
printf '%s\n' "$KEYDION_OLD_RELEASE" \
  > "$KEYDION_BACKUP_DIR/old-release.txt"
printf '%s\n' "$KEYDION_NEW_RELEASE" \
  > "$KEYDION_BACKUP_DIR/new-release.txt"
printf '%s\n' "$KEYDION_ROOT/.venv" \
  > "$KEYDION_BACKUP_DIR/venv-path.txt"
printf '%s\n' "$KEYDION_APPLICATION_DB_IDENTITY" \
  > "$KEYDION_BACKUP_DIR/database-identity.txt"
printf '%s\t%s\t%s\n' "$KEYDION_RESTORE_PROBE_SERVER_UUID" \
  "$KEYDION_RESTORE_PROBE_DATABASE" "$KEYDION_RESTORE_PROBE_TOKEN" \
  > "$KEYDION_BACKUP_DIR/restore-probe.tsv"
test -f "$KEYDION_BACKUP_DIR/restore-probe.tsv"
test ! -L "$KEYDION_BACKUP_DIR/restore-probe.tsv"
test "$(stat -c '%U:%G:%a' "$KEYDION_BACKUP_DIR/restore-probe.tsv")" \
  = root:root:600
printf '%s\t%s\t%s\n' "$KEYDION_RESTORE_PROBE_SERVER_UUID" \
  "$KEYDION_RESTORE_PROBE_DATABASE" "$KEYDION_RESTORE_PROBE_TOKEN" \
  | cmp --silent - "$KEYDION_BACKUP_DIR/restore-probe.tsv"
printf '%s\t%s\n' "$KEYDION_DATABASE_CHARACTER_SET" \
  "$KEYDION_DATABASE_COLLATION" \
  > "$KEYDION_BACKUP_DIR/database-defaults.txt"
printf '%s\n' "$KEYDION_MYSQL_CAPACITY_EVIDENCE" \
  > "$KEYDION_BACKUP_DIR/mysql-capacity-evidence.txt"
printf 'papers\t%s\npending-papers\t%s\n' \
  "$KEYDION_PAPERS_SOURCE" "$KEYDION_PENDING_SOURCE" \
  > "$KEYDION_BACKUP_DIR/storage-sources.tsv"
printf '%s\n' "$KEYDION_BACKUP_ID" \
  > "$KEYDION_BACKUP_DIR/active-boundary"
(
  cd "$KEYDION_BACKUP_DIR"
  find old-release.txt new-release.txt venv-path.txt \
    database-identity.txt database-defaults.txt restore-probe.tsv \
    mysql-capacity-evidence.txt storage-sources.tsv active-boundary systemd \
    -type f -print0 | sort -z | xargs -0 sha256sum \
    > pre-snapshot.sha256
  sha256sum -c pre-snapshot.sha256
)
sync -f "$KEYDION_BACKUP_DIR/active-boundary"
sync -f "$KEYDION_BACKUP_DIR"
ln "$KEYDION_BACKUP_DIR/active-boundary" \
  /srv/keydion-backups/paper-publishing-active
sync -f /srv/keydion-backups/paper-publishing-active
sync -f /srv/keydion-backups
```

Expected checkpoint: the account is exactly `keydion`; the repository is
exactly `/Keydion`; `KEYDION_OLD_RELEASE` is the clean, currently checked-out
production commit; `KEYDION_NEW_RELEASE` is a different, already-fetched full
commit SHA; no tracked or untracked repository files are present; and neither
checkout nor the installed units have changed. Both effective units have no
systemd drop-ins, and their original enabled/active states are captured for
rollback. The old virtual environment is executable and passes
`pip check`, proving the environment later archived for rollback is internally
consistent. The environment file must leave `PAPERQUERY_DATA_DIR` and
`PAPERQUERY_UPLOAD_DIR` empty or set them to the exact defaults named above.
Non-default storage is not supported by this runbook. `.env.prod`, the shared
`.venv`, both resolved storage roots, and logs have the tested access. `.venv`
is a real contained directory; none of the three rename-restored trees is or
contains a mount. Executable byte/inode gates cover both database dumps, three
archives, each temporary restore tree, and safety margins on the local backup
and tree filesystems. The conservative aggregate worst case is required on
every involved local filesystem, so shared devices cannot pass three
independent underestimates. Because MySQL may be remote, its restore-probe
schema headroom has a recorded DBA evidence reference and Section 3 proves it
with an actual restore rehearsal on that server rather than a local-host `df`.
The root-controlled environment file was parsed without executing it; the
database defaults were captured; and the application and backup credential
paths reported the same MySQL server UUID and exact `keydion` schema. The
configured job lease is at most 1,800 seconds, so the worker unit's 1,900-second
stop timeout never SIGKILLs a still-valid default-bound lease. The preflight
must later report no `insufficient_disk` or `cross_device_staging` blocker.
The Paper and pending roots already existed as real, owned directories on the
operator-confirmed mount sources; Section 1 did not create or repair them. The
release IDs, virtual-environment path, random restore-probe name/token and
recorded MySQL server UUID, original unit files/presence, exact unit source
release/path/Git blob/SHA-256 provenance, and original enabled/active states are
now protected by `pre-snapshot.sha256` and the exclusive
`paper-publishing-active` boundary before any unit is disabled.

## 2. Stop the worker and web service

Close new traffic at the load balancer/nginx maintenance gate, allow active
requests to drain, then stop the job claimant before Gunicorn:

```bash
if test "$KEYDION_WORKER_UNIT_PRESENT" -eq 1; then
  sudo systemctl disable keydion-publishing-worker
  sudo systemctl stop keydion-publishing-worker
  test "$(systemctl is-active keydion-publishing-worker)" = inactive
  test "$(systemctl is-enabled keydion-publishing-worker)" = disabled
fi
sudo systemctl disable keydion
sudo systemctl stop keydion
test "$(systemctl is-active keydion)" = inactive
test "$(systemctl is-enabled keydion)" = disabled
```

Expected checkpoint: every present unit reports both `inactive` and `disabled`,
so a reboot cannot restart Gunicorn or a publishing claimant during the offline
boundary. No process is serving traffic or claiming/reconciling a job.

### Continue after an interruption before Section 3

If the original shell disconnects, exits, or the host reboots after the
pre-snapshot boundary was created but before Section 3 completed, do not restart
Section 1: that would risk treating a partially fenced unit as the original
state. Use this standalone continuation with the existing backup ID. It accepts
only the release-bound, checksummed original state, revalidates every present
unit's fragment provenance, and idempotently completes the service fence. After
its checkpoint, continue at Section 3 with the variables from this shell.

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077
KEYDION_RECOVERY_LOCK=/run/lock/keydion-paper-publishing.lock
test -d /run/lock
test ! -L /run/lock
test "$(stat -c '%U:%G' /run/lock)" = root:root
if ! test -e "$KEYDION_RECOVERY_LOCK"; then
  (set -o noclobber; : > "$KEYDION_RECOVERY_LOCK") 2>/dev/null \
    || test -f "$KEYDION_RECOVERY_LOCK"
fi
test -f "$KEYDION_RECOVERY_LOCK"
test ! -L "$KEYDION_RECOVERY_LOCK"
test "$(stat -c '%U:%G:%a' "$KEYDION_RECOVERY_LOCK")" = root:root:600
exec {KEYDION_RECOVERY_LOCK_FD}<>"$KEYDION_RECOVERY_LOCK"
flock --exclusive --nonblock "$KEYDION_RECOVERY_LOCK_FD"
export KEYDION_ROOT=/Keydion
export KEYDION_DATABASE=keydion
export KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
test -f "$KEYDION_MYSQL_DEFAULTS"
test ! -L "$KEYDION_MYSQL_DEFAULTS"
test "$(stat -c '%U:%G:%a' "$KEYDION_MYSQL_DEFAULTS")" = root:root:600
load_active_boundary() {
  local selector=/srv/keydion-backups/paper-publishing-active
  local boundary selector_inode boundary_inode
  test -f "$selector"
  test ! -L "$selector"
  test "$(stat -c '%U:%G:%a' "$selector")" = root:root:600
  KEYDION_BACKUP_ID="$(cat "$selector")"
  [[ "$KEYDION_BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
  export KEYDION_BACKUP_ID
  export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
  boundary="$KEYDION_BACKUP_DIR/active-boundary"
  test -f "$boundary"
  test ! -L "$boundary"
  test "$(stat -c '%U:%G:%a' "$boundary")" = root:root:600
  selector_inode="$(stat -c '%d:%i' "$selector")"
  boundary_inode="$(stat -c '%d:%i' "$boundary")"
  test "$selector_inode" = "$boundary_inode"
  printf '%s\n' "$KEYDION_BACKUP_ID" | cmp --silent - "$boundary"
  (
    cd "$KEYDION_BACKUP_DIR"
    sha256sum -c pre-snapshot.sha256
  )
}
load_active_boundary
# A crash after link(2), but before the original shell's sync, is repaired
# before any service is disabled in this continuation.
sync -f /srv/keydion-backups/paper-publishing-active
sync -f /srv/keydion-backups
KEYDION_OLD_RELEASE="$(cat "$KEYDION_BACKUP_DIR/old-release.txt")"
KEYDION_NEW_RELEASE="$(cat "$KEYDION_BACKUP_DIR/new-release.txt")"
export KEYDION_OLD_RELEASE KEYDION_NEW_RELEASE
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_OLD_RELEASE"
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"

test -f "$KEYDION_ROOT/.env.prod"
test ! -L "$KEYDION_ROOT/.env.prod"
test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
  = root:keydion:640
read_dotenv_value() {
  local key="$1"
  local required="${2:-1}"
  local line value="" match_count=0
  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] || return 1
  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      *\\) printf '%s\n' 'EnvironmentFile continuations are unsupported' >&2; return 1 ;;
    esac
    if [[ "$line" =~ ^[[:space:]]*${key}[[:space:]]*= ]] && \
       [[ "$line" != "$key="* ]]; then
      printf '%s must use exact KEY=value spelling\n' "$key" >&2
      return 1
    fi
    case "$line" in
      "$key="*) match_count=$((match_count + 1)); value="${line#*=}" ;;
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
  test "$match_count" -eq 1
  [[ "$value" != *\\* ]]
  if test "${value:0:1}" = '"'; then
    test "${#value}" -ge 2
    test "${value: -1}" = '"'
    value="${value:1:${#value}-2}"
    [[ "$value" != *\"* ]]
  elif test "${value:0:1}" = "'"; then
    test "${#value}" -ge 2
    test "${value: -1}" = "'"
    value="${value:1:${#value}-2}"
    [[ "$value" != *\'* ]]
  else
    [[ ! "$value" =~ [[:space:]] ]]
    [[ "$value" != *\"* ]]
    [[ "$value" != *\'* ]]
  fi
  if test "$required" -eq 1; then
    test -n "$value"
  fi
  printf '%s' "$value"
}
PAPERQUERY_DATABASE_URL="$(read_dotenv_value PAPERQUERY_DATABASE_URL)"
PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"
KEYDION_JOB_LEASE_SECONDS="$(read_dotenv_value \
  PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS 0)"
KEYDION_JOB_LEASE_SECONDS="${KEYDION_JOB_LEASE_SECONDS:-1800}"
if ! [[ "$KEYDION_JOB_LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]] || \
   ! test "$KEYDION_JOB_LEASE_SECONDS" -le 1800; then
  printf '%s\n' \
    'PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS must be 1..1800' >&2
  exit 1
fi
case "$PAPERQUERY_DATA_DIR" in
  ""|/Keydion/data) ;;
  *) printf '%s\n' \
       'PAPERQUERY_DATA_DIR must be empty or /Keydion/data' >&2; exit 1 ;;
esac
case "$PAPERQUERY_UPLOAD_DIR" in
  ""|/Keydion/papers) ;;
  *) printf '%s\n' \
       'PAPERQUERY_UPLOAD_DIR must be empty or /Keydion/papers' >&2; exit 1 ;;
esac
export PAPERQUERY_DATABASE_URL PAPERQUERY_DATA_DIR PAPERQUERY_UPLOAD_DIR

assert_fresh_database_identity() {
  local application_identity backup_identity expected_identity
  application_identity="$(
    sudo -u keydion \
      --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
      "$KEYDION_ROOT/.venv/bin/python" - <<'PY'
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
if url.get_backend_name() != "mysql" or url.database != os.environ["KEYDION_DATABASE"]:
    raise SystemExit("PAPERQUERY_DATABASE_URL must select keydion MySQL")
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
  backup_identity="$(mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()')"
  expected_identity="$(cat "$KEYDION_BACKUP_DIR/database-identity.txt")"
  test "$application_identity" = "$expected_identity"
  test "$backup_identity" = "$expected_identity"
}

KEYDION_DATA_DIR="$(realpath -m "$KEYDION_ROOT/data")"
KEYDION_PAPERS_DIR="$(realpath -m "$KEYDION_ROOT/papers")"
KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
export KEYDION_DATA_DIR KEYDION_PAPERS_DIR KEYDION_PENDING_DIR
test "$KEYDION_PAPERS_DIR" = /Keydion/papers
test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers
test -d "$KEYDION_PAPERS_DIR"
test ! -L "$KEYDION_PAPERS_DIR"
test "$(realpath "$KEYDION_PAPERS_DIR")" = "$KEYDION_PAPERS_DIR"
test -d "$KEYDION_PENDING_DIR"
test ! -L "$KEYDION_PENDING_DIR"
test "$(realpath "$KEYDION_PENDING_DIR")" = "$KEYDION_PENDING_DIR"
test -f "$KEYDION_BACKUP_DIR/storage-sources.tsv"
test ! -L "$KEYDION_BACKUP_DIR/storage-sources.tsv"
mapfile -t KEYDION_STORAGE_SOURCE_LINES \
  < "$KEYDION_BACKUP_DIR/storage-sources.tsv"
test "${#KEYDION_STORAGE_SOURCE_LINES[@]}" -eq 2
IFS=$'\t' read -r KEYDION_PAPERS_LABEL KEYDION_EXPECTED_PAPERS_SOURCE \
  KEYDION_PAPERS_SOURCE_EXTRA <<< "${KEYDION_STORAGE_SOURCE_LINES[0]}"
IFS=$'\t' read -r KEYDION_PENDING_LABEL KEYDION_EXPECTED_PENDING_SOURCE \
  KEYDION_PENDING_SOURCE_EXTRA <<< "${KEYDION_STORAGE_SOURCE_LINES[1]}"
test "$KEYDION_PAPERS_LABEL" = papers
test "$KEYDION_PENDING_LABEL" = pending-papers
test -z "$KEYDION_PAPERS_SOURCE_EXTRA"
test -z "$KEYDION_PENDING_SOURCE_EXTRA"
test "$(findmnt --noheadings --raw --output SOURCE \
  --target "$KEYDION_PAPERS_DIR")" = "$KEYDION_EXPECTED_PAPERS_SOURCE"
test "$(findmnt --noheadings --raw --output SOURCE \
  --target "$KEYDION_PENDING_DIR")" = "$KEYDION_EXPECTED_PENDING_SOURCE"
KEYDION_APPLICATION_DB_IDENTITY="$(cat \
  "$KEYDION_BACKUP_DIR/database-identity.txt")"
KEYDION_DATABASE_DEFAULTS="$(cat \
  "$KEYDION_BACKUP_DIR/database-defaults.txt")"
IFS=$'\t' read -r KEYDION_DATABASE_CHARACTER_SET \
  KEYDION_DATABASE_COLLATION KEYDION_DATABASE_DEFAULTS_EXTRA \
  <<< "$KEYDION_DATABASE_DEFAULTS"
[[ "$KEYDION_DATABASE_CHARACTER_SET" =~ ^[A-Za-z0-9_]+$ ]]
[[ "$KEYDION_DATABASE_COLLATION" =~ ^[A-Za-z0-9_]+$ ]]
test -z "$KEYDION_DATABASE_DEFAULTS_EXTRA"
KEYDION_MYSQL_CAPACITY_EVIDENCE="$(cat \
  "$KEYDION_BACKUP_DIR/mysql-capacity-evidence.txt")"

mapfile -t KEYDION_UNIT_STATE_LINES \
  < "$KEYDION_BACKUP_DIR/systemd/unit-state.tsv"
test "${#KEYDION_UNIT_STATE_LINES[@]}" -eq 2
IFS=$'\t' read -r KEYDION_WEB_UNIT_NAME KEYDION_WEB_ENABLED_STATE \
  KEYDION_WEB_ACTIVE_STATE KEYDION_WEB_STATE_EXTRA \
  <<< "${KEYDION_UNIT_STATE_LINES[0]}"
IFS=$'\t' read -r KEYDION_WORKER_UNIT_NAME KEYDION_WORKER_ENABLED_STATE \
  KEYDION_WORKER_ACTIVE_STATE KEYDION_WORKER_STATE_EXTRA \
  <<< "${KEYDION_UNIT_STATE_LINES[1]}"
test "$KEYDION_WEB_UNIT_NAME" = keydion.service
test "$KEYDION_WORKER_UNIT_NAME" = keydion-publishing-worker.service
test -z "$KEYDION_WEB_STATE_EXTRA"
test -z "$KEYDION_WORKER_STATE_EXTRA"
case "$KEYDION_WEB_ENABLED_STATE:$KEYDION_WEB_ACTIVE_STATE" in
  enabled:active|enabled:inactive|disabled:active|disabled:inactive) ;;
  *) printf '%s\n' 'Invalid recorded web unit state' >&2; exit 1 ;;
esac
case "$KEYDION_WORKER_ENABLED_STATE:$KEYDION_WORKER_ACTIVE_STATE" in
  enabled:active|enabled:inactive|disabled:active|disabled:inactive|absent:absent) ;;
  *) printf '%s\n' 'Invalid recorded worker unit state' >&2; exit 1 ;;
esac

load_recorded_unit_provenance() {
  local -a lines=()
  test -f "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv"
  test ! -L "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv"
  test "$(stat -c '%U:%G:%a' \
    "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv")" = root:root:600
  mapfile -t lines < "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv"
  test "${#lines[@]}" -eq 2
  IFS=$'\t' read -r KEYDION_WEB_PROVENANCE_UNIT \
    KEYDION_WEB_UNIT_ORIGIN KEYDION_WEB_UNIT_SOURCE_RELEASE \
    KEYDION_WEB_UNIT_SOURCE_PATH KEYDION_WEB_UNIT_GIT_BLOB \
    KEYDION_WEB_UNIT_SHA256 KEYDION_WEB_PROVENANCE_EXTRA <<< "${lines[0]}"
  IFS=$'\t' read -r KEYDION_WORKER_PROVENANCE_UNIT \
    KEYDION_WORKER_UNIT_ORIGIN KEYDION_WORKER_UNIT_SOURCE_RELEASE \
    KEYDION_WORKER_UNIT_SOURCE_PATH KEYDION_WORKER_UNIT_GIT_BLOB \
    KEYDION_WORKER_UNIT_SHA256 KEYDION_WORKER_PROVENANCE_EXTRA <<< "${lines[1]}"
  test "$KEYDION_WEB_PROVENANCE_UNIT" = keydion.service
  test "$KEYDION_WORKER_PROVENANCE_UNIT" \
    = keydion-publishing-worker.service
  test -z "$KEYDION_WEB_PROVENANCE_EXTRA"
  test -z "$KEYDION_WORKER_PROVENANCE_EXTRA"
}

assert_recorded_unit_source() {
  local unit="$1" origin source_release source_path recorded_blob
  local recorded_sha256 snapshot current_blob current_sha256 live_blob
  case "$unit" in
    keydion.service)
      origin="$KEYDION_WEB_UNIT_ORIGIN"
      source_release="$KEYDION_WEB_UNIT_SOURCE_RELEASE"
      source_path="$KEYDION_WEB_UNIT_SOURCE_PATH"
      recorded_blob="$KEYDION_WEB_UNIT_GIT_BLOB"
      recorded_sha256="$KEYDION_WEB_UNIT_SHA256"
      snapshot="$KEYDION_BACKUP_DIR/systemd/keydion.service"
      case "$origin:$source_release:$source_path" in
        "old-release:$KEYDION_OLD_RELEASE:deploy/keydion.service") ;;
        "candidate-legacy-allowlist:$KEYDION_NEW_RELEASE:deploy/keydion-legacy.service") ;;
        *) printf '%s has invalid recorded source\n' "$unit" >&2; return 1 ;;
      esac
      ;;
    keydion-publishing-worker.service)
      origin="$KEYDION_WORKER_UNIT_ORIGIN"
      source_release="$KEYDION_WORKER_UNIT_SOURCE_RELEASE"
      source_path="$KEYDION_WORKER_UNIT_SOURCE_PATH"
      recorded_blob="$KEYDION_WORKER_UNIT_GIT_BLOB"
      recorded_sha256="$KEYDION_WORKER_UNIT_SHA256"
      snapshot="$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
      if test "$origin" = absent; then
        test "$source_release:$source_path:$recorded_blob:$recorded_sha256" \
          = -:-:-:-
        test -f "${snapshot}.absent"
        test ! -L "${snapshot}.absent"
        test ! -e "$snapshot"
        test ! -L "$snapshot"
        return 0
      fi
      test "$origin:$source_release:$source_path" \
        = "old-release:$KEYDION_OLD_RELEASE:deploy/keydion-publishing-worker.service"
      ;;
    *) return 1 ;;
  esac
  [[ "$recorded_blob" =~ ^[0-9a-f]{40}$ ]]
  [[ "$recorded_sha256" =~ ^[0-9a-f]{64}$ ]]
  test -f "$snapshot"
  test ! -L "$snapshot"
  test "$(stat -c '%U:%G:%a' "$snapshot")" = root:root:644
  sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
    "${source_release}:${source_path}"
  live_blob="$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse \
    "${source_release}:${source_path}")"
  test "$live_blob" = "$recorded_blob"
  current_blob="$(git -C "$KEYDION_ROOT" hash-object "$snapshot")"
  test "$current_blob" = "$recorded_blob"
  current_sha256="$(sha256sum "$snapshot" | awk '{print $1}')"
  test "$current_sha256" = "$recorded_sha256"
}
load_recorded_unit_provenance
assert_recorded_unit_source keydion.service
assert_recorded_unit_source keydion-publishing-worker.service

assert_tracked_unit_fragment() {
  local unit="$1"
  local expected="/etc/systemd/system/$unit"
  local fragment
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  test "$fragment" = "$expected"
  test -f "$expected"
  test ! -L "$expected"
}
assert_no_systemd_dropins() {
  local unit="$1" dropins
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  test -z "$dropins"
}
assert_no_systemd_dropins keydion.service
assert_tracked_unit_fragment keydion.service
cmp --silent "$KEYDION_BACKUP_DIR/systemd/keydion.service" \
  /etc/systemd/system/keydion.service
if test "$KEYDION_WORKER_ENABLED_STATE:$KEYDION_WORKER_ACTIVE_STATE" \
    = absent:absent; then
  ! systemctl cat keydion-publishing-worker.service >/dev/null 2>&1
else
  assert_no_systemd_dropins keydion-publishing-worker.service
  assert_tracked_unit_fragment keydion-publishing-worker.service
  cmp --silent \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service" \
    /etc/systemd/system/keydion-publishing-worker.service
  sudo systemctl disable keydion-publishing-worker.service
  sudo systemctl stop keydion-publishing-worker.service
  test "$(systemctl is-enabled keydion-publishing-worker.service)" = disabled
  test "$(systemctl is-active keydion-publishing-worker.service)" = inactive
fi
sudo systemctl disable keydion.service
sudo systemctl stop keydion.service
test "$(systemctl is-enabled keydion.service)" = disabled
test "$(systemctl is-active keydion.service)" = inactive
```

## 3. Take coordinated database and filesystem backups

The stopped services make the following sequential snapshots one coordinated
point. Record every artifact location before continuing. Never reuse a backup ID or backup directory: Section 1 already created the boundary exclusively, and
this block refuses any boundary not named by the active pre-snapshot record.

```bash
sudo install -d -o root -g root -m 0700 /srv/keydion-backups
test "$(cat /srv/keydion-backups/paper-publishing-active)" \
  = "$KEYDION_BACKUP_ID"
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c pre-snapshot.sha256
)

load_restore_probe_metadata() {
  local metadata="$KEYDION_BACKUP_DIR/restore-probe.tsv"
  local identity="$KEYDION_BACKUP_DIR/database-identity.txt"
  local identity_server_uuid identity_database identity_extra
  local -a KEYDION_RESTORE_PROBE_LINES=()
  local -a KEYDION_RESTORE_PROBE_IDENTITY_LINES=()

  test -f "$metadata"
  test ! -L "$metadata"
  test "$(stat -c '%U:%G:%a' "$metadata")" = root:root:600
  mapfile -t KEYDION_RESTORE_PROBE_LINES < "$metadata"
  test "${#KEYDION_RESTORE_PROBE_LINES[@]}" -eq 1
  IFS=$'\t' read -r KEYDION_RESTORE_PROBE_SERVER_UUID \
    KEYDION_RESTORE_PROBE_DATABASE KEYDION_RESTORE_PROBE_TOKEN \
    KEYDION_RESTORE_PROBE_EXTRA <<< "${KEYDION_RESTORE_PROBE_LINES[0]}"
  [[ "$KEYDION_RESTORE_PROBE_SERVER_UUID" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
  [[ "$KEYDION_RESTORE_PROBE_DATABASE" =~ ^keydion_restore_probe_[0-9a-f]{32}$ ]]
  test "${#KEYDION_RESTORE_PROBE_DATABASE}" -le 64
  [[ "$KEYDION_RESTORE_PROBE_TOKEN" =~ ^[0-9a-f]{64}$ ]]
  test -z "$KEYDION_RESTORE_PROBE_EXTRA"
  printf '%s\t%s\t%s\n' "$KEYDION_RESTORE_PROBE_SERVER_UUID" \
    "$KEYDION_RESTORE_PROBE_DATABASE" "$KEYDION_RESTORE_PROBE_TOKEN" \
    | cmp --silent - "$metadata"

  test -f "$identity"
  test ! -L "$identity"
  mapfile -t KEYDION_RESTORE_PROBE_IDENTITY_LINES < "$identity"
  test "${#KEYDION_RESTORE_PROBE_IDENTITY_LINES[@]}" -eq 1
  IFS=$'\t' read -r identity_server_uuid identity_database identity_extra \
    <<< "${KEYDION_RESTORE_PROBE_IDENTITY_LINES[0]}"
  [[ "$identity_server_uuid" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
  test "$identity_database" = "$KEYDION_DATABASE"
  test -z "$identity_extra"
  printf '%s\t%s\n' "$identity_server_uuid" "$identity_database" \
    | cmp --silent - "$identity"
  test "$KEYDION_RESTORE_PROBE_SERVER_UUID" = "$identity_server_uuid"

  KEYDION_RESTORE_PROBE_MARKER_TABLE=_keydion_restore_probe_owner
  [[ "$KEYDION_RESTORE_PROBE_MARKER_TABLE" =~ ^_[a-z0-9_]+$ ]]
  test "${#KEYDION_RESTORE_PROBE_MARKER_TABLE}" -le 64
}

restore_probe_current_server_uuid() {
  local current_server_uuid
  current_server_uuid="$(mysql \
    --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e 'SELECT @@GLOBAL.server_uuid')"
  test "${current_server_uuid//$'\n'/}" = "$current_server_uuid"
  [[ "$current_server_uuid" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
  printf '%s' "$current_server_uuid"
}

assert_restore_probe_server_uuid() {
  local current_server_uuid
  current_server_uuid="$(restore_probe_current_server_uuid)"
  if ! test "$current_server_uuid" = \
      "$KEYDION_RESTORE_PROBE_SERVER_UUID"; then
    printf 'Restore probe server UUID mismatch; refusing mutation\n' >&2
    return 1
  fi
}

restore_probe_marker_row() {
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_RESTORE_PROBE_DATABASE" \
    -e "SELECT ownership_token, server_uuid FROM \`${KEYDION_RESTORE_PROBE_MARKER_TABLE}\` ORDER BY singleton"
}

assert_restore_probe_ownership() {
  local marker_row expected_marker_row
  assert_restore_probe_server_uuid
  if ! marker_row="$(restore_probe_marker_row)"; then
    printf 'Restore probe ownership marker is absent; refusing DROP\n' >&2
    return 1
  fi
  test "${marker_row//$'\n'/}" = "$marker_row"
  expected_marker_row="$(printf '%s\t%s' \
    "$KEYDION_RESTORE_PROBE_TOKEN" \
    "$KEYDION_RESTORE_PROBE_SERVER_UUID")"
  if ! test "$marker_row" = "$expected_marker_row"; then
    printf 'Restore probe ownership marker mismatch; refusing DROP\n' >&2
    return 1
  fi
}

restore_probe_schema_count() {
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '${KEYDION_RESTORE_PROBE_DATABASE}'"
}

create_restore_probe() {
  assert_restore_probe_server_uuid
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    -e "CREATE DATABASE \`${KEYDION_RESTORE_PROBE_DATABASE}\` CHARACTER SET ${KEYDION_DATABASE_CHARACTER_SET} COLLATE ${KEYDION_DATABASE_COLLATION};"
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    "$KEYDION_RESTORE_PROBE_DATABASE" \
    -e "CREATE TABLE \`${KEYDION_RESTORE_PROBE_MARKER_TABLE}\` (singleton TINYINT UNSIGNED NOT NULL PRIMARY KEY, ownership_token CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, server_uuid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, CONSTRAINT \`keydion_restore_probe_singleton\` CHECK (singleton = 1)) ENGINE=InnoDB AS SELECT CAST(1 AS UNSIGNED) AS singleton, '${KEYDION_RESTORE_PROBE_TOKEN}' AS ownership_token, '${KEYDION_RESTORE_PROBE_SERVER_UUID}' AS server_uuid;"
  assert_restore_probe_ownership
}

drop_owned_restore_probe() {
  assert_restore_probe_ownership
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    -e "DROP DATABASE \`${KEYDION_RESTORE_PROBE_DATABASE}\`;"
}

prepare_restore_probe() {
  local schema_count
  assert_restore_probe_server_uuid
  if ! schema_count="$(restore_probe_schema_count)"; then
    printf 'Could not determine restore probe schema state\n' >&2
    return 1
  fi
  test "${schema_count//$'\n'/}" = "$schema_count"
  case "$schema_count" in
    0) ;;
    1)
      if ! assert_restore_probe_ownership; then
        printf '%s\n' \
          'Existing restore probe is not owned by this boundary; refusing DROP' >&2
        return 1
      fi
      drop_owned_restore_probe
      ;;
    *)
      printf 'Unexpected restore probe schema count: %s\n' \
        "$schema_count" >&2
      return 1
      ;;
  esac
  create_restore_probe
}
# End restore-probe ownership helpers.

load_restore_probe_metadata
assert_restore_probe_server_uuid
KEYDION_SOURCE_MARKER_TABLE_COUNT="$(mysql \
  --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  --batch --skip-column-names "$KEYDION_DATABASE" \
  -e "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '${KEYDION_RESTORE_PROBE_MARKER_TABLE}'")"
test "$KEYDION_SOURCE_MARKER_TABLE_COUNT" = 0
mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  --batch --skip-column-names "$KEYDION_DATABASE" \
  -e "SELECT 'papers_metadata', COUNT(*), 0 FROM papers_metadata UNION ALL SELECT 'papers_chunks', COUNT(*), COALESCE(SUM(OCTET_LENGTH(embedding_vec)), 0) FROM papers_chunks UNION ALL SELECT 'submissions', COUNT(*), 0 FROM submissions ORDER BY 1" \
  > "$KEYDION_BACKUP_DIR/database-source-metrics.txt"
mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  --batch --raw --skip-column-names "$KEYDION_DATABASE" \
  -e 'SELECT id, HEX(embedding_vec) FROM papers_chunks ORDER BY id' \
  | sha256sum | awk '{print $1}' \
  > "$KEYDION_BACKUP_DIR/database-source-vectors.sha256"

assert_fresh_database_identity
sudo mysqldump \
  --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  --single-transaction --quick --hex-blob \
  --default-character-set="$KEYDION_DATABASE_CHARACTER_SET" \
  --routines --triggers --events \
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
sudo tar --acls --xattrs --numeric-owner -cpf \
  "$KEYDION_BACKUP_DIR/venv.tar" \
  -C "$KEYDION_ROOT" .venv

test -s "$KEYDION_BACKUP_DIR/database.sql.gz"
test -s "$KEYDION_BACKUP_DIR/papers.tar"
test -s "$KEYDION_BACKUP_DIR/pending-papers.tar"
test -s "$KEYDION_BACKUP_DIR/venv.tar"
gzip -t "$KEYDION_BACKUP_DIR/database.sql.gz"
tar -tf "$KEYDION_BACKUP_DIR/papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/pending-papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/venv.tar" >/dev/null

# The checksummed random name is collision-refusing on first use. A prior probe
# may be dropped and recreated only when its exact token/server marker proves it
# belongs to this active pre-snapshot boundary.
prepare_restore_probe
KEYDION_RESTORE_PROBE_SUCCEEDED=0
if gzip -dc "$KEYDION_BACKUP_DIR/database.sql.gz" \
  | mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
      "$KEYDION_RESTORE_PROBE_DATABASE"; then
  if mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
      --batch --skip-column-names "$KEYDION_RESTORE_PROBE_DATABASE" \
      -e "SELECT 'papers_metadata', COUNT(*), 0 FROM papers_metadata UNION ALL SELECT 'papers_chunks', COUNT(*), COALESCE(SUM(OCTET_LENGTH(embedding_vec)), 0) FROM papers_chunks UNION ALL SELECT 'submissions', COUNT(*), 0 FROM submissions ORDER BY 1" \
      > "$KEYDION_BACKUP_DIR/restore-rehearsal-metrics.txt" \
    && mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
      --batch --raw --skip-column-names "$KEYDION_RESTORE_PROBE_DATABASE" \
      -e 'SELECT id, HEX(embedding_vec) FROM papers_chunks ORDER BY id' \
      | sha256sum | awk '{print $1}' \
      > "$KEYDION_BACKUP_DIR/restore-rehearsal-vectors.sha256" \
    && cmp --silent "$KEYDION_BACKUP_DIR/database-source-metrics.txt" \
      "$KEYDION_BACKUP_DIR/restore-rehearsal-metrics.txt" \
    && cmp --silent "$KEYDION_BACKUP_DIR/database-source-vectors.sha256" \
      "$KEYDION_BACKUP_DIR/restore-rehearsal-vectors.sha256"; then
    KEYDION_RESTORE_PROBE_SUCCEEDED=1
  fi
fi
drop_owned_restore_probe
test "$KEYDION_RESTORE_PROBE_SUCCEEDED" -eq 1
printf 'restore rehearsal passed for %s and the probe was dropped\n' \
  "$KEYDION_RESTORE_PROBE_DATABASE" \
  > "$KEYDION_BACKUP_DIR/restore-rehearsal.txt"
(
  cd "$KEYDION_BACKUP_DIR"
  find database.sql.gz papers.tar pending-papers.tar venv.tar \
    database-defaults.txt database-identity.txt restore-probe.tsv \
    database-source-metrics.txt mysql-capacity-evidence.txt \
    database-source-vectors.sha256 \
    old-release.txt new-release.txt venv-path.txt pre-snapshot.sha256 \
    storage-sources.tsv \
    restore-rehearsal-metrics.txt restore-rehearsal-vectors.sha256 \
    restore-rehearsal.txt systemd \
    -type f -print0 \
    | sort -z | xargs -0 sha256sum > SHA256SUMS
  sha256sum -c SHA256SUMS
)
```

Expected checkpoint: the database backup, `papers.tar`, `pending-papers.tar`,
`venv.tar`, the verified database identity, old and candidate release
identifiers, prior web/worker unit files or explicit `.absent` markers, and
checksums exist together under the recorded backup directory. A schema collision
without the exact checksummed token/server marker is a hard stop and is never
automatically dropped. An exact marked residue on the same recorded server is a
resumable probe owned by this boundary: Section 3 freshly rechecks its marker,
drops it, recreates it, and writes the marker before replay. Final cleanup makes
the same fresh ownership check immediately before its drop. The original
character set/collation, representative row counts, total vector bytes, and
ordered raw-vector digest matched the source, proving the binary-safe dump can
be replayed faithfully before the production schema is touched. Copy the whole
directory to the approved off-host backup destination and run
`sha256sum -c SHA256SUMS` there. Do not continue until the off-host verification
succeeds.

### Guarded human recovery for an unmarked restore probe

`CREATE DATABASE` and the atomic InnoDB marker CTAS are necessarily separate
statements. If the host is killed between them, Section 3 finds an empty schema
with no marker and correctly refuses to infer ownership. Do not use this path
for a nonempty schema, an existing marker table, a marker mismatch, or a server
UUID mismatch. Keep both services fenced, obtain MySQL audit evidence that the
interrupted Section 3 invocation created this exact random schema, and obtain an
incident-owner approval reference. Run the standalone continuation above to
reacquire the process lock and checked boundary, then run this block in that
same root shell:

```bash
KEYDION_GUARDED_RECOVERY_BACKUP_ID="$KEYDION_BACKUP_ID"
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c pre-snapshot.sha256
)

load_guarded_restore_probe_metadata() {
  local metadata="$KEYDION_BACKUP_DIR/restore-probe.tsv"
  local identity="$KEYDION_BACKUP_DIR/database-identity.txt"
  local identity_server_uuid identity_database identity_extra
  local -a KEYDION_GUARDED_PROBE_LINES=()
  local -a KEYDION_GUARDED_IDENTITY_LINES=()
  test -f "$metadata"
  test ! -L "$metadata"
  test "$(stat -c '%U:%G:%a' "$metadata")" = root:root:600
  mapfile -t KEYDION_GUARDED_PROBE_LINES < "$metadata"
  test "${#KEYDION_GUARDED_PROBE_LINES[@]}" -eq 1
  IFS=$'\t' read -r KEYDION_RESTORE_PROBE_SERVER_UUID \
    KEYDION_RESTORE_PROBE_DATABASE KEYDION_RESTORE_PROBE_TOKEN \
    KEYDION_RESTORE_PROBE_EXTRA <<< "${KEYDION_GUARDED_PROBE_LINES[0]}"
  [[ "$KEYDION_RESTORE_PROBE_SERVER_UUID" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
  [[ "$KEYDION_RESTORE_PROBE_DATABASE" =~ ^keydion_restore_probe_[0-9a-f]{32}$ ]]
  test "${#KEYDION_RESTORE_PROBE_DATABASE}" -le 64
  [[ "$KEYDION_RESTORE_PROBE_TOKEN" =~ ^[0-9a-f]{64}$ ]]
  test -z "$KEYDION_RESTORE_PROBE_EXTRA"
  printf '%s\t%s\t%s\n' "$KEYDION_RESTORE_PROBE_SERVER_UUID" \
    "$KEYDION_RESTORE_PROBE_DATABASE" "$KEYDION_RESTORE_PROBE_TOKEN" \
    | cmp --silent - "$metadata"

  test -f "$identity"
  test ! -L "$identity"
  mapfile -t KEYDION_GUARDED_IDENTITY_LINES < "$identity"
  test "${#KEYDION_GUARDED_IDENTITY_LINES[@]}" -eq 1
  IFS=$'\t' read -r identity_server_uuid identity_database identity_extra \
    <<< "${KEYDION_GUARDED_IDENTITY_LINES[0]}"
  [[ "$identity_server_uuid" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
  test "$identity_database" = "$KEYDION_DATABASE"
  test -z "$identity_extra"
  printf '%s\t%s\n' "$identity_server_uuid" "$identity_database" \
    | cmp --silent - "$identity"
  test "$KEYDION_RESTORE_PROBE_SERVER_UUID" = "$identity_server_uuid"
  KEYDION_RESTORE_PROBE_MARKER_TABLE=_keydion_restore_probe_owner
}

guarded_restore_probe_current_server_uuid() {
  local current_server_uuid
  current_server_uuid="$(mysql \
    --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e 'SELECT @@GLOBAL.server_uuid')"
  test "${current_server_uuid//$'\n'/}" = "$current_server_uuid"
  [[ "$current_server_uuid" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]
  printf '%s' "$current_server_uuid"
}

assert_guarded_restore_probe_server_uuid() {
  test "$(guarded_restore_probe_current_server_uuid)" = \
    "$KEYDION_RESTORE_PROBE_SERVER_UUID"
}

guarded_restore_probe_schema_count() {
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '${KEYDION_RESTORE_PROBE_DATABASE}'"
}

guarded_restore_probe_table_count() {
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${KEYDION_RESTORE_PROBE_DATABASE}'"
}

guarded_restore_probe_routine_count() {
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = '${KEYDION_RESTORE_PROBE_DATABASE}'"
}

guarded_restore_probe_event_count() {
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e "SELECT COUNT(*) FROM information_schema.EVENTS WHERE EVENT_SCHEMA = '${KEYDION_RESTORE_PROBE_DATABASE}'"
}

guarded_restore_probe_trigger_count() {
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '${KEYDION_RESTORE_PROBE_DATABASE}'"
}

load_guarded_restore_probe_metadata
assert_guarded_restore_probe_server_uuid
test "$(guarded_restore_probe_schema_count)" = 1
test "$(guarded_restore_probe_table_count)" = 0
test "$(guarded_restore_probe_routine_count)" = 0
test "$(guarded_restore_probe_event_count)" = 0
test "$(guarded_restore_probe_trigger_count)" = 0
read -r -p "MySQL audit evidence/ticket for interrupted CREATE: " \
  KEYDION_RESTORE_PROBE_AUDIT_EVIDENCE
[[ "$KEYDION_RESTORE_PROBE_AUDIT_EVIDENCE" =~ ^[A-Za-z0-9._:/-]{3,128}$ ]]
read -r -p "Incident-owner approval reference: " \
  KEYDION_RESTORE_PROBE_INCIDENT_APPROVAL
[[ "$KEYDION_RESTORE_PROBE_INCIDENT_APPROVAL" =~ ^[A-Za-z0-9._:/-]{3,128}$ ]]
read -r -p \
  "Type DROP ${KEYDION_RESTORE_PROBE_DATABASE} ON ${KEYDION_RESTORE_PROBE_SERVER_UUID}: " \
  KEYDION_RESTORE_PROBE_DROP_APPROVAL

# The prompts are a human-duration pause. Rebind and reparse the exact
# checksummed boundary, then repeat every live proof immediately before DROP.
load_active_boundary
test "$KEYDION_BACKUP_ID" = "$KEYDION_GUARDED_RECOVERY_BACKUP_ID"
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c pre-snapshot.sha256
)
load_guarded_restore_probe_metadata
test "$KEYDION_RESTORE_PROBE_DROP_APPROVAL" = \
  "DROP ${KEYDION_RESTORE_PROBE_DATABASE} ON ${KEYDION_RESTORE_PROBE_SERVER_UUID}"
test "$(guarded_restore_probe_schema_count)" = 1
test "$(guarded_restore_probe_table_count)" = 0
test "$(guarded_restore_probe_routine_count)" = 0
test "$(guarded_restore_probe_event_count)" = 0
test "$(guarded_restore_probe_trigger_count)" = 0
assert_guarded_restore_probe_server_uuid
mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  -e "DROP DATABASE \`${KEYDION_RESTORE_PROBE_DATABASE}\`;"
```

After the guarded drop, rerun Section 3 from its first command. Do not continue
to candidate checkout until the restore rehearsal succeeds and its owned probe
is removed normally. If any guard fails, leave the schema untouched and
escalate to the DBA/incident owner.

Only after that coordinated backup is verified may the checkout and installed
units move to the candidate release:

```bash
sudo -u keydion git -C "$KEYDION_ROOT" checkout --detach "$KEYDION_NEW_RELEASE"
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_NEW_RELEASE"
cd "$KEYDION_ROOT"
sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" -m pip install \
  --disable-pip-version-check \
  --requirement "$KEYDION_ROOT/requirements.txt"
sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" -m pip check
test "$(sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" \
  -m alembic heads)" = "0003_publishing_contract (head)"
sudo install -m 0644 "$KEYDION_ROOT/deploy/keydion.service" \
  /etc/systemd/system/keydion.service
sudo install -m 0644 \
  "$KEYDION_ROOT/deploy/keydion-publishing-worker.service" \
  /etc/systemd/system/keydion-publishing-worker.service
sudo systemctl daemon-reload
assert_no_systemd_dropins keydion.service
assert_no_systemd_dropins keydion-publishing-worker.service
assert_tracked_unit_fragment keydion.service
assert_tracked_unit_fragment keydion-publishing-worker.service
sudo systemd-analyze verify /etc/systemd/system/keydion.service \
  /etc/systemd/system/keydion-publishing-worker.service
```

Expected checkpoint: `HEAD` is exactly the recorded candidate SHA; every
candidate requirement is installed consistently; `pip check` succeeds; the
candidate exposes exactly `0003_publishing_contract (head)`; neither unit has
an untracked systemd drop-in; and both installed units validate. A checkout,
dependency installation, unit installation, or verification failure stops the
strict shell; do not run a database-facing Alembic command. Restore the snapshot
boundary, including `venv.tar`, instead.

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
`duplicate_chunk` rows must be repaired before contraction. Do not repair data
or rerun Section 3 from the candidate checkout. Run the Rollback section in
full before repairing, and verify that it restored the recorded old checkout,
virtual environment, unit-presence state, database, and storage snapshots.
Perform the approved repair against that old-release boundary. Then close
traffic and start again at Section 1 in a new root Bash; it must generate a new
`KEYDION_BACKUP_ID` whose exclusive backup directory does not exist. Repeat the
stop, coordinated backup, candidate checkout, dependency sync, and preflight
steps from the beginning.

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
assert_fresh_database_identity
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
KEYDION_UPGRADE_ATTEMPT="$(date -u +%Y%m%dT%H%M%SZ)"
KEYDION_UPGRADE_LOG="$KEYDION_BACKUP_DIR/alembic-upgrade-${KEYDION_UPGRADE_ATTEMPT}.txt"
(
  set -o noclobber
  : > "$KEYDION_UPGRADE_LOG"
)
printf 'Alembic upgrade attempt %s\n' "$KEYDION_UPGRADE_ATTEMPT" \
  | tee -a "$KEYDION_UPGRADE_LOG"
assert_fresh_database_identity
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  env PAPERQUERY_DATA_DIR="$KEYDION_DATA_DIR" \
  PAPERQUERY_UPLOAD_DIR="$KEYDION_PAPERS_DIR" \
  PAPERQUERY_PUBLISHING_MAINTENANCE=1 \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic upgrade head \
  2>&1 | tee -a "$KEYDION_UPGRADE_LOG"
```

The backfill journals verified source hashes, copied revisions, chunk mapping,
and database completion. If the process is interrupted or exits nonzero, the
strict shell may close. Keep traffic closed and preserve its per-attempt log;
never rerun Section 1, reuse Section 3, or truncate an earlier log. Use the
standalone resume entry below. If its preflight reports an unsafe partial shape,
changed file/hash, or any blocker, do not improvise; use the rollback procedure.

### Resume after an interrupted upgrade

Use this entry only after the original attempt completed the coordinated backup
and candidate checkout, then reached Section 6. It reopens that exact backup
boundary and refuses any different checkout, active service, installed unit,
dependency set, database target, or storage path. Enter the existing backup ID;
do not generate a new one for a resumable attempt.

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077

KEYDION_RECOVERY_LOCK=/run/lock/keydion-paper-publishing.lock
test -d /run/lock
test ! -L /run/lock
test "$(stat -c '%U:%G' /run/lock)" = root:root
if ! test -e "$KEYDION_RECOVERY_LOCK"; then
  (set -o noclobber; : > "$KEYDION_RECOVERY_LOCK") 2>/dev/null \
    || test -f "$KEYDION_RECOVERY_LOCK"
fi
test -f "$KEYDION_RECOVERY_LOCK"
test ! -L "$KEYDION_RECOVERY_LOCK"
test "$(stat -c '%U:%G:%a' "$KEYDION_RECOVERY_LOCK")" = root:root:600
exec {KEYDION_RECOVERY_LOCK_FD}<>"$KEYDION_RECOVERY_LOCK"
flock --exclusive --nonblock "$KEYDION_RECOVERY_LOCK_FD"

export KEYDION_ROOT=/Keydion
export KEYDION_DATABASE=keydion
export KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
test "$(stat -c '%a' "$KEYDION_MYSQL_DEFAULTS")" = 600
load_active_boundary() {
  local selector=/srv/keydion-backups/paper-publishing-active
  local boundary selector_inode boundary_inode
  test -f "$selector"
  test ! -L "$selector"
  test "$(stat -c '%U:%G:%a' "$selector")" = root:root:600
  KEYDION_BACKUP_ID="$(cat "$selector")"
  [[ "$KEYDION_BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
  export KEYDION_BACKUP_ID
  export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
  boundary="$KEYDION_BACKUP_DIR/active-boundary"
  test -f "$boundary"
  test ! -L "$boundary"
  test "$(stat -c '%U:%G:%a' "$boundary")" = root:root:600
  selector_inode="$(stat -c '%d:%i' "$selector")"
  boundary_inode="$(stat -c '%d:%i' "$boundary")"
  test "$selector_inode" = "$boundary_inode"
  printf '%s\n' "$KEYDION_BACKUP_ID" | cmp --silent - "$boundary"
  (
    cd "$KEYDION_BACKUP_DIR"
    sha256sum -c pre-snapshot.sha256
  )
}
load_active_boundary
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c SHA256SUMS
)
gzip -t "$KEYDION_BACKUP_DIR/database.sql.gz"
tar -tf "$KEYDION_BACKUP_DIR/papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/pending-papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/venv.tar" >/dev/null

KEYDION_OLD_RELEASE="$(cat "$KEYDION_BACKUP_DIR/old-release.txt")"
KEYDION_NEW_RELEASE="$(cat "$KEYDION_BACKUP_DIR/new-release.txt")"
export KEYDION_OLD_RELEASE KEYDION_NEW_RELEASE
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
test "$KEYDION_OLD_RELEASE" != "$KEYDION_NEW_RELEASE"
KEYDION_CURRENT_RELEASE="$(sudo -u keydion git -C \
  "$KEYDION_ROOT" rev-parse --verify HEAD)"
test "$KEYDION_CURRENT_RELEASE" = "$KEYDION_NEW_RELEASE"
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"

read_unit_enabled_state() {
  local unit="$1"
  local state status
  if state="$(systemctl is-enabled "$unit" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$state" in
    enabled) test "$status" -eq 0 ;;
    disabled) test "$status" -ne 0 ;;
    *) printf '%s has unsupported enabled state: %s\n' \
         "$unit" "$state" >&2; return 1 ;;
  esac
  printf '%s' "$state"
}

read_unit_active_state() {
  local unit="$1"
  local state status
  if state="$(systemctl is-active "$unit" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$state" in
    active) test "$status" -eq 0 ;;
    inactive) test "$status" -ne 0 ;;
    *) printf '%s has unsupported active state: %s\n' \
         "$unit" "$state" >&2; return 1 ;;
  esac
  printf '%s' "$state"
}

assert_tracked_unit_fragment() {
  local unit="$1"
  local expected="/etc/systemd/system/$unit"
  local fragment
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  test "$fragment" = "$expected"
  test -f "$expected"
  test ! -L "$expected"
}
assert_tracked_unit_fragment keydion.service
assert_tracked_unit_fragment keydion-publishing-worker.service

test "$(read_unit_enabled_state keydion-publishing-worker.service)" \
  = disabled
test "$(read_unit_enabled_state keydion.service)" = disabled
test "$(read_unit_active_state keydion-publishing-worker.service)" \
  = inactive
test "$(read_unit_active_state keydion.service)" = inactive
cmp --silent "$KEYDION_ROOT/deploy/keydion.service" \
  /etc/systemd/system/keydion.service
cmp --silent "$KEYDION_ROOT/deploy/keydion-publishing-worker.service" \
  /etc/systemd/system/keydion-publishing-worker.service

assert_no_systemd_dropins() {
  local unit="$1"
  local dropins
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  if test -n "$dropins"; then
    printf '%s has unexpected systemd DropInPaths: %s\n' \
      "$unit" "$dropins" >&2
    return 1
  fi
}
assert_no_systemd_dropins keydion.service
assert_no_systemd_dropins keydion-publishing-worker.service
systemd-analyze verify /etc/systemd/system/keydion.service \
  /etc/systemd/system/keydion-publishing-worker.service

test -f "$KEYDION_ROOT/.env.prod"
test ! -L "$KEYDION_ROOT/.env.prod"
test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
  = root:keydion:640

# This is the same non-executing EnvironmentFile parser as Section 1. Keep the
# two copies identical when the accepted value grammar changes.
read_dotenv_value() {
  local key="$1"
  local required="${2:-1}"
  local line value="" match_count=0

  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] || return 1

  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      *\\)
        printf 'EnvironmentFile continuations are not supported here\n' >&2
        return 1
        ;;
    esac
    if [[ "$line" =~ ^[[:space:]]*${key}[[:space:]]*= ]] && \
       [[ "$line" != "$key="* ]]; then
      printf '%s must use exact KEY=value spelling in .env.prod\n' \
        "$key" >&2
      return 1
    fi
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

  if [[ "$value" == *\\* ]]; then
    printf '%s must not use backslash escapes in .env.prod\n' "$key" >&2
    return 1
  fi

  if test "${value:0:1}" = '"'; then
    test "${#value}" -ge 2 && test "${value: -1}" = '"' || {
      printf '%s has unmatched double quotes in .env.prod\n' "$key" >&2
      return 1
    }
    value="${value:1:${#value}-2}"
    if [[ "$value" == *\"* ]]; then
      printf '%s must use one whole quoted value in .env.prod\n' "$key" >&2
      return 1
    fi
  elif test "${value:0:1}" = "'"; then
    test "${#value}" -ge 2 && test "${value: -1}" = "'" || {
      printf '%s has unmatched single quotes in .env.prod\n' "$key" >&2
      return 1
    }
    value="${value:1:${#value}-2}"
    if [[ "$value" == *\'* ]]; then
      printf '%s must use one whole quoted value in .env.prod\n' "$key" >&2
      return 1
    fi
  elif [[ "$value" =~ [[:space:]] ]] || \
       [[ "$value" == *\"* ]] || [[ "$value" == *\'* ]]; then
    printf '%s must use one unquoted or wholly quoted value in .env.prod\n' \
      "$key" >&2
    return 1
  fi

  if test "$required" -eq 1 && test -z "$value"; then
    printf '%s must not be empty in .env.prod\n' "$key" >&2
    return 1
  fi
  printf '%s' "$value"
}

PAPERQUERY_DATABASE_URL="$(read_dotenv_value PAPERQUERY_DATABASE_URL)"
export PAPERQUERY_DATABASE_URL
PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"
KEYDION_JOB_LEASE_SECONDS="$(read_dotenv_value \
  PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS 0)"
KEYDION_JOB_LEASE_SECONDS="${KEYDION_JOB_LEASE_SECONDS:-1800}"
if ! [[ "$KEYDION_JOB_LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]] || \
   ! test "$KEYDION_JOB_LEASE_SECONDS" -le 1800; then
  printf '%s\n' \
    'PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS must be 1..1800 on this unit' >&2
  exit 1
fi
case "$PAPERQUERY_DATA_DIR" in
  ""|/Keydion/data) ;;
  *) printf '%s\n' \
       'PAPERQUERY_DATA_DIR must be empty or /Keydion/data' >&2; exit 1 ;;
esac
case "$PAPERQUERY_UPLOAD_DIR" in
  ""|/Keydion/papers) ;;
  *) printf '%s\n' \
       'PAPERQUERY_UPLOAD_DIR must be empty or /Keydion/papers' >&2; exit 1 ;;
esac
KEYDION_DATA_DIR="$(realpath -m "$KEYDION_ROOT/data")"
KEYDION_PAPERS_DIR="$(realpath -m "$KEYDION_ROOT/papers")"
KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
export KEYDION_DATA_DIR KEYDION_PAPERS_DIR KEYDION_PENDING_DIR
test "$KEYDION_DATA_DIR" = /Keydion/data
test "$KEYDION_PAPERS_DIR" = /Keydion/papers
test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers
test -d "$KEYDION_ROOT/.venv"
test ! -L "$KEYDION_ROOT/.venv"
test "$(realpath "$KEYDION_ROOT/.venv")" = "$KEYDION_ROOT/.venv"

assert_recorded_storage_provenance() {
  local papers_label expected_papers_source papers_extra
  local pending_label expected_pending_source pending_extra
  local KEYDION_PAPERS_MOUNT_TARGET KEYDION_PENDING_MOUNT_TARGET
  test -f "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test ! -L "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  mapfile -t KEYDION_STORAGE_SOURCE_LINES \
    < "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test "${#KEYDION_STORAGE_SOURCE_LINES[@]}" -eq 2
  IFS=$'\t' read -r papers_label expected_papers_source papers_extra \
    <<< "${KEYDION_STORAGE_SOURCE_LINES[0]}"
  IFS=$'\t' read -r pending_label expected_pending_source pending_extra \
    <<< "${KEYDION_STORAGE_SOURCE_LINES[1]}"
  test "$papers_label" = papers
  test "$pending_label" = pending-papers
  test -z "$papers_extra"
  test -z "$pending_extra"
  test -d "$KEYDION_PAPERS_DIR"
  test ! -L "$KEYDION_PAPERS_DIR"
  test "$(realpath "$KEYDION_PAPERS_DIR")" = "$KEYDION_PAPERS_DIR"
  test -d "$KEYDION_PENDING_DIR"
  test ! -L "$KEYDION_PENDING_DIR"
  test "$(realpath "$KEYDION_PENDING_DIR")" = "$KEYDION_PENDING_DIR"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PAPERS_DIR")" = "$expected_papers_source"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PENDING_DIR")" = "$expected_pending_source"
  KEYDION_PAPERS_MOUNT_TARGET="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PAPERS_DIR")"
  KEYDION_PENDING_MOUNT_TARGET="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PENDING_DIR")"
  test "$(stat -c '%d' "$KEYDION_PAPERS_DIR")" \
    = "$(stat -c '%d' "$KEYDION_PAPERS_MOUNT_TARGET")"
  test "$(stat -c '%d' "$KEYDION_PENDING_DIR")" \
    = "$(stat -c '%d' "$KEYDION_PENDING_MOUNT_TARGET")"
  sudo -u keydion test -r "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -w "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -r "$KEYDION_PENDING_DIR"
  sudo -u keydion test -w "$KEYDION_PENDING_DIR"
}
assert_recorded_storage_provenance

KEYDION_MOUNT_TARGETS="$(findmnt --raw --noheadings --output TARGET)"
assert_rename_tree_not_mounted() {
  local tree="$1"
  local mount_target
  while IFS= read -r mount_target; do
    case "$mount_target" in
      "$tree"|"$tree"/*)
        printf '%s is or contains a mount and cannot use rename rollback\n' \
          "$tree" >&2
        return 1
        ;;
    esac
  done <<< "$KEYDION_MOUNT_TARGETS"
}
assert_rename_tree_not_mounted "$KEYDION_ROOT/.venv"
assert_rename_tree_not_mounted "$KEYDION_PAPERS_DIR"
assert_rename_tree_not_mounted "$KEYDION_PENDING_DIR"

cd "$KEYDION_ROOT"
sudo -u keydion test -x "$KEYDION_ROOT/.venv/bin/python"
sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" -m pip check
test "$(sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" \
  -m alembic heads)" = "0003_publishing_contract (head)"

KEYDION_APPLICATION_DB_IDENTITY="$(
  sudo -u keydion \
    --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
    "$KEYDION_ROOT/.venv/bin/python" - <<'PY'
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
expected_database = os.environ["KEYDION_DATABASE"]
if url.get_backend_name() != "mysql" or url.database != expected_database:
    raise SystemExit("PAPERQUERY_DATABASE_URL must select the keydion MySQL database")

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
export KEYDION_APPLICATION_DB_IDENTITY
KEYDION_BACKUP_DB_IDENTITY="$(
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()'
)"
export KEYDION_BACKUP_DB_IDENTITY
KEYDION_EXPECTED_DB_IDENTITY="$(cat \
  "$KEYDION_BACKUP_DIR/database-identity.txt")"
test "$KEYDION_APPLICATION_DB_IDENTITY" = "$KEYDION_EXPECTED_DB_IDENTITY"
test "$KEYDION_BACKUP_DB_IDENTITY" = "$KEYDION_EXPECTED_DB_IDENTITY"

assert_fresh_database_identity() {
  local application_identity backup_identity
  application_identity="$(
    sudo -u keydion \
      --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
      "$KEYDION_ROOT/.venv/bin/python" - <<'PY'
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
if url.get_backend_name() != "mysql" or url.database != os.environ["KEYDION_DATABASE"]:
    raise SystemExit("PAPERQUERY_DATABASE_URL must select keydion MySQL")
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
  backup_identity="$(mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()')"
  test "$application_identity" = "$KEYDION_EXPECTED_DB_IDENTITY"
  test "$backup_identity" = "$KEYDION_EXPECTED_DB_IDENTITY"
}

KEYDION_RESUME_ATTEMPT="$(date -u +%Y%m%dT%H%M%SZ)"
KEYDION_PREFLIGHT_LOG="$KEYDION_BACKUP_DIR/preflight-resume-${KEYDION_RESUME_ATTEMPT}.txt"
(
  set -o noclobber
  : > "$KEYDION_PREFLIGHT_LOG"
)
cd "$KEYDION_ROOT"
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" \
  tools/preflight_publishing_migration.py \
  --papers-dir "$KEYDION_PAPERS_DIR" \
  2>&1 | tee -a "$KEYDION_PREFLIGHT_LOG"
KEYDION_ISSUES_TABLE_COUNT="$(
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'publishing_migration_issues'"
)"
case "$KEYDION_ISSUES_TABLE_COUNT" in
  0) printf '%s\n' 'No publishing_migration_issues table exists yet' ;;
  1)
    mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
      --batch --raw "$KEYDION_DATABASE" \
      -e 'SELECT kind, legacy_key, paper_id, blocking, resolved_at, details FROM publishing_migration_issues ORDER BY blocking DESC, kind, legacy_key'
    ;;
  *) printf '%s\n' 'Could not prove migration-issues table state' >&2; exit 1 ;;
esac

KEYDION_UPGRADE_LOG="$KEYDION_BACKUP_DIR/alembic-upgrade-${KEYDION_RESUME_ATTEMPT}.txt"
(
  set -o noclobber
  : > "$KEYDION_UPGRADE_LOG"
)
printf 'Alembic resume attempt %s\n' "$KEYDION_RESUME_ATTEMPT" \
  | tee -a "$KEYDION_UPGRADE_LOG"
assert_recorded_storage_provenance
assert_fresh_database_identity
sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  env PAPERQUERY_DATA_DIR="$KEYDION_DATA_DIR" \
  PAPERQUERY_UPLOAD_DIR="$KEYDION_PAPERS_DIR" \
  PAPERQUERY_PUBLISHING_MAINTENANCE=1 \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic upgrade head \
  2>&1 | tee -a "$KEYDION_UPGRADE_LOG"
```

Expected checkpoint: the manifest and all archives validate; the old/candidate
SHAs come only from that manifest-bound directory; `HEAD`, dependencies, and
installed units are still the candidate; the repository has no tracked or
untracked changes; neither effective unit has a drop-in; both services remain
inactive; both credential paths still reach the recorded MySQL server/schema;
resumed preflight reports the recognized resumable shape with `blockers=0`;
reviewed issues are safe; and the newly named Alembic attempt reaches the
contract head. Every attempt has a different exclusive log, so no partial
evidence is overwritten.

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
reload_validated_prestart_config() {
  test -f "$KEYDION_ROOT/.env.prod"
  test ! -L "$KEYDION_ROOT/.env.prod"
  test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
  test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
    = root:keydion:640
  PAPERQUERY_DATABASE_URL="$(read_dotenv_value PAPERQUERY_DATABASE_URL)"
  PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
  PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"
  GUNICORN_BIND="$(read_dotenv_value GUNICORN_BIND)"
  KEYDION_JOB_LEASE_SECONDS="$(read_dotenv_value \
    PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS 0)"
  KEYDION_JOB_LEASE_SECONDS="${KEYDION_JOB_LEASE_SECONDS:-1800}"
  [[ "$KEYDION_JOB_LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]]
  test "$KEYDION_JOB_LEASE_SECONDS" -le 1800
  case "${PAPERQUERY_DATA_DIR:-}" in
    ""|/Keydion/data) ;;
    *) printf '%s\n' \
         'PAPERQUERY_DATA_DIR must be empty or /Keydion/data' >&2; return 1 ;;
  esac
  case "${PAPERQUERY_UPLOAD_DIR:-}" in
    ""|/Keydion/papers) ;;
    *) printf '%s\n' \
         'PAPERQUERY_UPLOAD_DIR must be empty or /Keydion/papers' >&2; return 1 ;;
  esac
  test "$GUNICORN_BIND" = 127.0.0.1:5000
  export PAPERQUERY_DATABASE_URL PAPERQUERY_DATA_DIR PAPERQUERY_UPLOAD_DIR
  KEYDION_DATA_DIR="$(realpath -m \
    "${PAPERQUERY_DATA_DIR:-$KEYDION_ROOT/data}")"
  KEYDION_PAPERS_DIR="$(realpath -m \
    "${PAPERQUERY_UPLOAD_DIR:-$KEYDION_ROOT/papers}")"
  KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
  test "$KEYDION_DATA_DIR" = /Keydion/data
  test "$KEYDION_PAPERS_DIR" = /Keydion/papers
  test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers
}

assert_active_boundary_current() {
  local selector=/srv/keydion-backups/paper-publishing-active
  local boundary="$KEYDION_BACKUP_DIR/active-boundary"
  test -f "$selector"
  test ! -L "$selector"
  test "$(stat -c '%U:%G:%a' "$selector")" = root:root:600
  test -f "$boundary"
  test ! -L "$boundary"
  test "$(stat -c '%U:%G:%a' "$boundary")" = root:root:600
  test "$(stat -c '%d:%i' "$selector")" = "$(stat -c '%d:%i' "$boundary")"
  printf '%s\n' "$KEYDION_BACKUP_ID" | cmp --silent - "$boundary"
  (cd "$KEYDION_BACKUP_DIR"; sha256sum -c pre-snapshot.sha256)
}

assert_recorded_storage_provenance() {
  local papers_label papers_source papers_extra
  local pending_label pending_source pending_extra papers_mount pending_mount
  local -a storage_source_lines=()
  test -f "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test ! -L "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  mapfile -t storage_source_lines \
    < "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test "${#storage_source_lines[@]}" -eq 2
  IFS=$'\t' read -r papers_label papers_source papers_extra \
    <<< "${storage_source_lines[0]}"
  IFS=$'\t' read -r pending_label pending_source pending_extra \
    <<< "${storage_source_lines[1]}"
  test "$papers_label" = papers
  test "$pending_label" = pending-papers
  test -z "$papers_extra"
  test -z "$pending_extra"
  test -d "$KEYDION_PAPERS_DIR"
  test ! -L "$KEYDION_PAPERS_DIR"
  test "$(realpath "$KEYDION_PAPERS_DIR")" = "$KEYDION_PAPERS_DIR"
  test -d "$KEYDION_PENDING_DIR"
  test ! -L "$KEYDION_PENDING_DIR"
  test "$(realpath "$KEYDION_PENDING_DIR")" = "$KEYDION_PENDING_DIR"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PAPERS_DIR")" = "$papers_source"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PENDING_DIR")" = "$pending_source"
  papers_mount="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PAPERS_DIR")"
  pending_mount="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PENDING_DIR")"
  test "$(stat -c '%d' "$KEYDION_PAPERS_DIR")" \
    = "$(stat -c '%d' "$papers_mount")"
  test "$(stat -c '%d' "$KEYDION_PENDING_DIR")" \
    = "$(stat -c '%d' "$pending_mount")"
  sudo -u keydion test -r "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -w "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -r "$KEYDION_PENDING_DIR"
  sudo -u keydion test -w "$KEYDION_PENDING_DIR"
}

assert_candidate_unit_prestart() {
  local unit="$1" expected="/etc/systemd/system/$1"
  local fragment dropins release_blob installed_blob
  systemctl cat "$unit" >/dev/null
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  test "$fragment" = "$expected"
  test -z "$dropins"
  test -f "$expected"
  test ! -L "$expected"
  test "$(stat -c '%U:%G:%a' "$expected")" = root:root:644
  sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
    "$KEYDION_NEW_RELEASE:deploy/$unit"
  release_blob="$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse \
    "$KEYDION_NEW_RELEASE:deploy/$unit")"
  installed_blob="$(git -C "$KEYDION_ROOT" hash-object "$expected")"
  test "$installed_blob" = "$release_blob"
}

reload_validated_prestart_config
assert_active_boundary_current
(cd "$KEYDION_BACKUP_DIR"; sha256sum -c SHA256SUMS)
assert_recorded_storage_provenance
assert_fresh_database_identity
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_NEW_RELEASE"
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"
test -d "$KEYDION_ROOT/.venv"
test ! -L "$KEYDION_ROOT/.venv"
test "$(realpath "$KEYDION_ROOT/.venv")" = "$KEYDION_ROOT/.venv"
sudo -u keydion test -x "$KEYDION_ROOT/.venv/bin/python"
sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" -m pip check
for unit in keydion-publishing-worker.service keydion.service; do
  assert_candidate_unit_prestart "$unit"
  test "$(read_unit_enabled_state "$unit")" = disabled
  test "$(read_unit_active_state "$unit")" = inactive
done
test "$(sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic current | tail -n 1)" \
  = '0003_publishing_contract (head)'
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
printf '%s\n' "$KEYDION_BACKUP_ID" \
  | cmp --silent - /srv/keydion-backups/paper-publishing-active
exit
```

While the maintenance gate remains closed, perform one read-only live request
to a recorded canonical UUID URL and its recorded legacy alias. Confirm the
canonical response succeeds, the legacy response is `301` to that UUID, and no
new migration or worker error appears. The final `exit` above releases the process-lifetime lock;
confirm that root shell has ended before starting the
following standalone finalizer. Use it to perform and bind those checks before
reopening traffic. Gunicorn `post_fork`
warms only the RAG snapshot; it does not start a publishing worker. Keep the checksummed
`paper-publishing-active` selector throughout the documented one verified
release rollback window; Section 9 must not remove it.

### Finalize the validated forward release

Run this entry with the maintenance gate still closed. It freshly proves the
candidate checkout, database target, storage mounts, tracked units, healthy
services, Alembic head, and both live identity routes. Only then does it publish
the immutable forward-success terminal. Reopen traffic after its checkpoint.

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077
KEYDION_RECOVERY_LOCK=/run/lock/keydion-paper-publishing.lock
test -d /run/lock
test ! -L /run/lock
test "$(stat -c '%U:%G' /run/lock)" = root:root
if ! test -e "$KEYDION_RECOVERY_LOCK"; then
  (set -o noclobber; : > "$KEYDION_RECOVERY_LOCK") 2>/dev/null \
    || test -f "$KEYDION_RECOVERY_LOCK"
fi
test -f "$KEYDION_RECOVERY_LOCK"
test ! -L "$KEYDION_RECOVERY_LOCK"
test "$(stat -c '%U:%G:%a' "$KEYDION_RECOVERY_LOCK")" = root:root:600
exec {KEYDION_RECOVERY_LOCK_FD}<>"$KEYDION_RECOVERY_LOCK"
flock --exclusive --nonblock "$KEYDION_RECOVERY_LOCK_FD"

export KEYDION_ROOT=/Keydion
export KEYDION_DATABASE=keydion
export KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
load_active_boundary() {
  local selector=/srv/keydion-backups/paper-publishing-active
  local boundary
  test -f "$selector"
  test ! -L "$selector"
  test "$(stat -c '%U:%G:%a' "$selector")" = root:root:600
  KEYDION_BACKUP_ID="$(cat "$selector")"
  [[ "$KEYDION_BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
  export KEYDION_BACKUP_ID
  export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
  boundary="$KEYDION_BACKUP_DIR/active-boundary"
  test -f "$boundary"
  test ! -L "$boundary"
  test "$(stat -c '%U:%G:%a' "$boundary")" = root:root:600
  test "$(stat -c '%d:%i' "$selector")" = "$(stat -c '%d:%i' "$boundary")"
  printf '%s\n' "$KEYDION_BACKUP_ID" | cmp --silent - "$boundary"
  (cd "$KEYDION_BACKUP_DIR"; sha256sum -c pre-snapshot.sha256)
}
load_active_boundary
(cd "$KEYDION_BACKUP_DIR"; sha256sum -c SHA256SUMS)
test ! -e "$KEYDION_BACKUP_DIR/rollback-started"
test ! -L "$KEYDION_BACKUP_DIR/rollback-started"
KEYDION_NEW_RELEASE="$(cat "$KEYDION_BACKUP_DIR/new-release.txt")"
[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_NEW_RELEASE"
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"

read_dotenv_value() {
  local key="$1" required="${2:-1}" line value="" count=0
  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] || return 1
  while IFS= read -r line || test -n "$line"; do
    case "$line" in *\\) return 1 ;; esac
    if [[ "$line" =~ ^[[:space:]]*${key}[[:space:]]*= ]] && \
       [[ "$line" != "$key="* ]]; then
      return 1
    fi
    case "$line" in "$key="*) count=$((count + 1)); value="${line#*=}" ;; esac
  done < "$KEYDION_ROOT/.env.prod"
  if test "$count" -eq 0; then
    test "$required" -eq 0
    printf ''
    return 0
  fi
  test "$count" -eq 1
  [[ "$value" != *\\* ]]
  if test "${value:0:1}" = '"'; then
    test "${#value}" -ge 2
    test "${value: -1}" = '"'
    value="${value:1:${#value}-2}"
    [[ "$value" != *\"* ]]
  elif test "${value:0:1}" = "'"; then
    test "${#value}" -ge 2
    test "${value: -1}" = "'"
    value="${value:1:${#value}-2}"
    [[ "$value" != *\'* ]]
  else
    [[ ! "$value" =~ [[:space:]] ]]
    [[ "$value" != *\"* ]]
    [[ "$value" != *\'* ]]
  fi
  if test "$required" -eq 1; then test -n "$value"; fi
  printf '%s' "$value"
}
reload_validated_runtime_config() {
  test -f "$KEYDION_ROOT/.env.prod"
  test ! -L "$KEYDION_ROOT/.env.prod"
  test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
  test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
    = root:keydion:640
  PAPERQUERY_DATABASE_URL="$(read_dotenv_value PAPERQUERY_DATABASE_URL)"
  PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
  PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"
  GUNICORN_BIND="$(read_dotenv_value GUNICORN_BIND)"
  KEYDION_JOB_LEASE_SECONDS="$(read_dotenv_value \
    PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS 0)"
  KEYDION_JOB_LEASE_SECONDS="${KEYDION_JOB_LEASE_SECONDS:-1800}"
  [[ "$KEYDION_JOB_LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]]
  if ! test "$KEYDION_JOB_LEASE_SECONDS" -le 1800; then
    printf '%s\n' \
      'PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS must not exceed 1800 seconds' >&2
    return 1
  fi
  case "${PAPERQUERY_DATA_DIR:-}" in
    ""|/Keydion/data) ;;
    *) printf '%s\n' 'PAPERQUERY_DATA_DIR must be empty or /Keydion/data' >&2; return 1 ;;
  esac
  case "${PAPERQUERY_UPLOAD_DIR:-}" in
    ""|/Keydion/papers) ;;
    *) printf '%s\n' 'PAPERQUERY_UPLOAD_DIR must be empty or /Keydion/papers' >&2; return 1 ;;
  esac
  test "$GUNICORN_BIND" = 127.0.0.1:5000
  export PAPERQUERY_DATABASE_URL PAPERQUERY_DATA_DIR PAPERQUERY_UPLOAD_DIR
  KEYDION_DATA_DIR="$(realpath -m "${PAPERQUERY_DATA_DIR:-$KEYDION_ROOT/data}")"
  KEYDION_PAPERS_DIR="$(realpath -m \
    "${PAPERQUERY_UPLOAD_DIR:-$KEYDION_ROOT/papers}")"
  KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
  test "$KEYDION_DATA_DIR" = /Keydion/data
  test "$KEYDION_PAPERS_DIR" = /Keydion/papers
  test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers
}
reload_validated_runtime_config

assert_recorded_storage_provenance() {
  local papers_label papers_source papers_extra
  local pending_label pending_source pending_extra papers_mount pending_mount
  test -f "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test ! -L "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  mapfile -t lines < "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test "${#lines[@]}" -eq 2
  IFS=$'\t' read -r papers_label papers_source papers_extra <<< "${lines[0]}"
  IFS=$'\t' read -r pending_label pending_source pending_extra <<< "${lines[1]}"
  test "$papers_label" = papers
  test "$pending_label" = pending-papers
  test -z "$papers_extra"; test -z "$pending_extra"
  test -d "$KEYDION_PAPERS_DIR"; test ! -L "$KEYDION_PAPERS_DIR"
  test -d "$KEYDION_PENDING_DIR"; test ! -L "$KEYDION_PENDING_DIR"
  test "$(realpath "$KEYDION_PAPERS_DIR")" = "$KEYDION_PAPERS_DIR"
  test "$(realpath "$KEYDION_PENDING_DIR")" = "$KEYDION_PENDING_DIR"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PAPERS_DIR")" = "$papers_source"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PENDING_DIR")" = "$pending_source"
  papers_mount="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PAPERS_DIR")"
  pending_mount="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PENDING_DIR")"
  test "$(stat -c '%d' "$KEYDION_PAPERS_DIR")" = "$(stat -c '%d' "$papers_mount")"
  test "$(stat -c '%d' "$KEYDION_PENDING_DIR")" = "$(stat -c '%d' "$pending_mount")"
  sudo -u keydion test -r "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -w "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -r "$KEYDION_PENDING_DIR"
  sudo -u keydion test -w "$KEYDION_PENDING_DIR"
}

assert_fresh_database_identity() {
  local application admin expected
  application="$(sudo -u keydion \
    --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
    "$KEYDION_ROOT/.venv/bin/python" - <<'PY'
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
if url.get_backend_name() != "mysql" or url.database != os.environ["KEYDION_DATABASE"]:
    raise SystemExit("unexpected database target")
engine = create_engine(url, pool_pre_ping=True)
try:
    with engine.connect() as connection:
        print("%s\t%s" % connection.execute(
            text("SELECT @@GLOBAL.server_uuid, DATABASE()")
        ).one())
finally:
    engine.dispose()
PY
  )"
  admin="$(mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()')"
  expected="$(cat "$KEYDION_BACKUP_DIR/database-identity.txt")"
  test "$application" = "$expected"
  test "$admin" = "$expected"
}

assert_recorded_storage_provenance
assert_fresh_database_identity
for unit in keydion-publishing-worker.service keydion.service; do
  test "$(systemctl show --property=FragmentPath --value "$unit")" \
    = "/etc/systemd/system/$unit"
  test -z "$(systemctl show --property=DropInPaths --value "$unit")"
  cmp --silent "$KEYDION_ROOT/deploy/$unit" "/etc/systemd/system/$unit"
  test "$(systemctl is-enabled "$unit")" = enabled
  test "$(systemctl is-active "$unit")" = active
done
test "$(sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic current | tail -n 1)" \
  = '0003_publishing_contract (head)'

read -r -p "Canonical Paper UUID for live validation: " KEYDION_LIVE_PAPER_ID
[[ "$KEYDION_LIVE_PAPER_ID" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$ ]]
reload_validated_runtime_config
assert_recorded_storage_provenance
assert_fresh_database_identity
KEYDION_LIVE_LEGACY_NAME="$(mysql \
  --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" --batch --skip-column-names \
  "$KEYDION_DATABASE" -e "SELECT filename FROM paper_filename_aliases WHERE paper_id = '${KEYDION_LIVE_PAPER_ID}' ORDER BY filename LIMIT 1")"
test -n "$KEYDION_LIVE_LEGACY_NAME"
test "${KEYDION_LIVE_LEGACY_NAME//$'\n'/}" = "$KEYDION_LIVE_LEGACY_NAME"
KEYDION_LIVE_LEGACY_ENCODED="$("$KEYDION_ROOT/.venv/bin/python" -c \
  'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' \
  "$KEYDION_LIVE_LEGACY_NAME")"
assert_recorded_storage_provenance
assert_fresh_database_identity
KEYDION_CANONICAL_STATUS="$(curl --silent --show-error --output /dev/null \
  --write-out '%{http_code}' \
  "http://127.0.0.1:5000/paper/$KEYDION_LIVE_PAPER_ID")"
test "$KEYDION_CANONICAL_STATUS" = 200
KEYDION_LEGACY_RESULT="$(curl --silent --show-error --output /dev/null \
  --max-redirs 0 --write-out '%{http_code}\t%{redirect_url}' \
  "http://127.0.0.1:5000/preview/$KEYDION_LIVE_LEGACY_ENCODED")"
IFS=$'\t' read -r KEYDION_LEGACY_STATUS KEYDION_LEGACY_REDIRECT \
  KEYDION_LEGACY_EXTRA <<< "$KEYDION_LEGACY_RESULT"
test "$KEYDION_LEGACY_STATUS" = 301
test -z "$KEYDION_LEGACY_EXTRA"
case "$KEYDION_LEGACY_REDIRECT" in
  */paper/"$KEYDION_LIVE_PAPER_ID") ;;
  *) printf '%s\n' 'Legacy alias did not redirect to the canonical UUID' >&2; exit 1 ;;
esac
reload_validated_runtime_config
assert_recorded_storage_provenance
assert_fresh_database_identity
for unit in keydion-publishing-worker.service keydion.service; do
  KEYDION_UNIT_FRAGMENT="$(systemctl show \
    --property=FragmentPath --value "$unit")"
  KEYDION_UNIT_DROPINS="$(systemctl show \
    --property=DropInPaths --value "$unit")"
  test "$KEYDION_UNIT_FRAGMENT" = "/etc/systemd/system/$unit"
  test -z "$KEYDION_UNIT_DROPINS"
  test -f "/etc/systemd/system/$unit"
  test ! -L "/etc/systemd/system/$unit"
  cmp --silent "$KEYDION_ROOT/deploy/$unit" "/etc/systemd/system/$unit"
  test "$(systemctl is-enabled "$unit")" = enabled
  test "$(systemctl is-active "$unit")" = active
done
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_NEW_RELEASE"
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"
test "$(sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
  "$KEYDION_ROOT/.venv/bin/python" -m alembic current | tail -n 1)" \
  = '0003_publishing_contract (head)'

publish_marker_once() {
  local target="$1" expected="$2" parent base partial stale
  local -a stale_partials=()
  parent="$(dirname "$target")"
  base="$(basename "$target")"
  test -d "$parent"
  test ! -L "$parent"
  test -n "$expected"
  test "${expected//$'\n'/}" = "$expected"
  test ! -L "$target"
  shopt -s nullglob
  stale_partials=("$parent/.${base}.partial."*)
  shopt -u nullglob
  for stale in "${stale_partials[@]}"; do
    test -f "$stale"
    test ! -L "$stale"
    rm -f -- "$stale"
  done
  sync -f "$parent"
  if test -e "$target"; then
    test -f "$target"
    test ! -L "$target"
    printf '%s\n' "$expected" | cmp --silent - "$target"
    sync -f "$target"
    sync -f "$parent"
    return 0
  fi
  partial="$(mktemp "$parent/.${base}.partial.XXXXXXXX")"
  test -f "$partial"
  test ! -L "$partial"
  printf '%s\n' "$expected" > "$partial"
  sync -f "$partial"
  if ! ln -- "$partial" "$target" 2>/dev/null; then
    test -f "$target"
    test ! -L "$target"
    printf '%s\n' "$expected" | cmp --silent - "$target"
  fi
  printf '%s\n' "$expected" | cmp --silent - "$target"
  sync -f "$target"
  sync -f "$parent"
  rm -f -- "$partial"
  sync -f "$parent"
  printf '%s\n' "$expected" | cmp --silent - "$target"
}
KEYDION_MANIFEST_SHA="$(sha256sum "$KEYDION_BACKUP_DIR/SHA256SUMS" \
  | awk '{print $1}')"
KEYDION_DB_IDENTITY_SHA="$(sha256sum \
  "$KEYDION_BACKUP_DIR/database-identity.txt" | awk '{print $1}')"
KEYDION_FORWARD_SUCCESS="$KEYDION_BACKUP_DIR/forward-success.complete"
KEYDION_FORWARD_SUCCESS_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" forward-success "$KEYDION_NEW_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"
publish_marker_once "$KEYDION_FORWARD_SUCCESS" \
  "$KEYDION_FORWARD_SUCCESS_EXPECTED"
exit
```

Expected checkpoint: the candidate release is live and healthy, the canonical
request returned `200`, the recorded alias returned `301` to that UUID, and the
durable `forward-success.complete` terminal binds this backup ID, candidate
release, database identity, and immutable manifest. Reopen traffic now.
The final `exit` releases the process-lifetime lock; confirm that root shell
has ended before opening any rollback entry.

## Rollback

Rollback is snapshot restoration, never `alembic downgrade`. Close traffic and
restore the old release, database backup, virtual environment, both storage
snapshots, and both prior systemd-unit presence states together from the same
backup directory.
The Section 3 snapshot is the rollback RPO. If traffic was reopened—or any
manual write occurred after that snapshot—restoring it discards all
post-snapshot writes in MySQL, published PDFs, and pending Submission storage.
Before authorizing rollback, keep traffic closed, inventory and export every
post-snapshot write that must survive, and obtain explicit incident-owner
authorization for the loss or later reconciliation. Do not run this block until
that write set has been accounted for.
If the original strict root Bash is no longer open, start a new one, enter the
recorded backup ID, and initialize only these fixed paths:

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077
KEYDION_RECOVERY_LOCK=/run/lock/keydion-paper-publishing.lock
test -d /run/lock
test ! -L /run/lock
test "$(stat -c '%U:%G' /run/lock)" = root:root
if ! test -e "$KEYDION_RECOVERY_LOCK"; then
  (set -o noclobber; : > "$KEYDION_RECOVERY_LOCK") 2>/dev/null \
    || test -f "$KEYDION_RECOVERY_LOCK"
fi
test -f "$KEYDION_RECOVERY_LOCK"
test ! -L "$KEYDION_RECOVERY_LOCK"
test "$(stat -c '%U:%G:%a' "$KEYDION_RECOVERY_LOCK")" = root:root:600
exec {KEYDION_RECOVERY_LOCK_FD}<>"$KEYDION_RECOVERY_LOCK"
flock --exclusive --nonblock "$KEYDION_RECOVERY_LOCK_FD"
export KEYDION_ROOT=/Keydion
export KEYDION_DATABASE=keydion
export KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
load_active_boundary() {
  local selector=/srv/keydion-backups/paper-publishing-active
  local boundary selector_inode boundary_inode
  test -f "$selector"
  test ! -L "$selector"
  test "$(stat -c '%U:%G:%a' "$selector")" = root:root:600
  KEYDION_BACKUP_ID="$(cat "$selector")"
  [[ "$KEYDION_BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
  export KEYDION_BACKUP_ID
  export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
  boundary="$KEYDION_BACKUP_DIR/active-boundary"
  test -f "$boundary"
  test ! -L "$boundary"
  test "$(stat -c '%U:%G:%a' "$boundary")" = root:root:600
  selector_inode="$(stat -c '%d:%i' "$selector")"
  boundary_inode="$(stat -c '%d:%i' "$boundary")"
  test "$selector_inode" = "$boundary_inode"
  printf '%s\n' "$KEYDION_BACKUP_ID" | cmp --silent - "$boundary"
  (
    cd "$KEYDION_BACKUP_DIR"
    sha256sum -c pre-snapshot.sha256
  )
}
load_active_boundary
publish_marker_once() {
  local target="$1" expected="$2" parent base partial stale
  local -a stale_partials=()
  parent="$(dirname "$target")"
  base="$(basename "$target")"
  test -d "$parent"
  test ! -L "$parent"
  test -n "$expected"
  test "${expected//$'\n'/}" = "$expected"
  test ! -L "$target"
  shopt -s nullglob
  stale_partials=("$parent/.${base}.partial."*)
  shopt -u nullglob
  for stale in "${stale_partials[@]}"; do
    test -f "$stale"
    test ! -L "$stale"
    rm -f -- "$stale"
  done
  sync -f "$parent"
  if test -e "$target"; then
    test -f "$target"
    test ! -L "$target"
    printf '%s\n' "$expected" | cmp --silent - "$target"
    sync -f "$target"
    sync -f "$parent"
    return 0
  fi
  partial="$(mktemp "$parent/.${base}.partial.XXXXXXXX")"
  test -f "$partial"
  test ! -L "$partial"
  printf '%s\n' "$expected" > "$partial"
  sync -f "$partial"
  if ! ln -- "$partial" "$target" 2>/dev/null; then
    test -f "$target"
    test ! -L "$target"
    printf '%s\n' "$expected" | cmp --silent - "$target"
  fi
  printf '%s\n' "$expected" | cmp --silent - "$target"
  sync -f "$target"
  sync -f "$parent"
  rm -f -- "$partial"
  sync -f "$parent"
  printf '%s\n' "$expected" | cmp --silent - "$target"
}
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c SHA256SUMS
)
gzip -t "$KEYDION_BACKUP_DIR/database.sql.gz"
tar -tf "$KEYDION_BACKUP_DIR/papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/pending-papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/venv.tar" >/dev/null
KEYDION_OLD_RELEASE="$(cat "$KEYDION_BACKUP_DIR/old-release.txt")"
KEYDION_NEW_RELEASE="$(cat "$KEYDION_BACKUP_DIR/new-release.txt")"
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
load_recorded_unit_provenance() {
  local -a lines=()
  test -f "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv"
  test ! -L "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv"
  test "$(stat -c '%U:%G:%a' \
    "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv")" = root:root:600
  mapfile -t lines < "$KEYDION_BACKUP_DIR/systemd/unit-provenance.tsv"
  test "${#lines[@]}" -eq 2
  IFS=$'\t' read -r KEYDION_WEB_PROVENANCE_UNIT \
    KEYDION_WEB_UNIT_ORIGIN KEYDION_WEB_UNIT_SOURCE_RELEASE \
    KEYDION_WEB_UNIT_SOURCE_PATH KEYDION_WEB_UNIT_GIT_BLOB \
    KEYDION_WEB_UNIT_SHA256 KEYDION_WEB_PROVENANCE_EXTRA <<< "${lines[0]}"
  IFS=$'\t' read -r KEYDION_WORKER_PROVENANCE_UNIT \
    KEYDION_WORKER_UNIT_ORIGIN KEYDION_WORKER_UNIT_SOURCE_RELEASE \
    KEYDION_WORKER_UNIT_SOURCE_PATH KEYDION_WORKER_UNIT_GIT_BLOB \
    KEYDION_WORKER_UNIT_SHA256 KEYDION_WORKER_PROVENANCE_EXTRA <<< "${lines[1]}"
  test "$KEYDION_WEB_PROVENANCE_UNIT" = keydion.service
  test "$KEYDION_WORKER_PROVENANCE_UNIT" \
    = keydion-publishing-worker.service
  test -z "$KEYDION_WEB_PROVENANCE_EXTRA"
  test -z "$KEYDION_WORKER_PROVENANCE_EXTRA"
}

assert_recorded_unit_source() {
  local unit="$1" origin source_release source_path recorded_blob
  local recorded_sha256 snapshot current_blob current_sha256 live_blob
  case "$unit" in
    keydion.service)
      origin="$KEYDION_WEB_UNIT_ORIGIN"
      source_release="$KEYDION_WEB_UNIT_SOURCE_RELEASE"
      source_path="$KEYDION_WEB_UNIT_SOURCE_PATH"
      recorded_blob="$KEYDION_WEB_UNIT_GIT_BLOB"
      recorded_sha256="$KEYDION_WEB_UNIT_SHA256"
      snapshot="$KEYDION_BACKUP_DIR/systemd/keydion.service"
      case "$origin:$source_release:$source_path" in
        "old-release:$KEYDION_OLD_RELEASE:deploy/keydion.service") ;;
        "candidate-legacy-allowlist:$KEYDION_NEW_RELEASE:deploy/keydion-legacy.service") ;;
        *) printf '%s has invalid recorded source\n' "$unit" >&2; return 1 ;;
      esac
      ;;
    keydion-publishing-worker.service)
      origin="$KEYDION_WORKER_UNIT_ORIGIN"
      source_release="$KEYDION_WORKER_UNIT_SOURCE_RELEASE"
      source_path="$KEYDION_WORKER_UNIT_SOURCE_PATH"
      recorded_blob="$KEYDION_WORKER_UNIT_GIT_BLOB"
      recorded_sha256="$KEYDION_WORKER_UNIT_SHA256"
      snapshot="$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
      if test "$origin" = absent; then
        test "$source_release:$source_path:$recorded_blob:$recorded_sha256" \
          = -:-:-:-
        test -f "${snapshot}.absent"
        test ! -L "${snapshot}.absent"
        test ! -e "$snapshot"
        test ! -L "$snapshot"
        return 0
      fi
      test "$origin:$source_release:$source_path" \
        = "old-release:$KEYDION_OLD_RELEASE:deploy/keydion-publishing-worker.service"
      ;;
    *) return 1 ;;
  esac
  [[ "$recorded_blob" =~ ^[0-9a-f]{40}$ ]]
  [[ "$recorded_sha256" =~ ^[0-9a-f]{64}$ ]]
  test -f "$snapshot"
  test ! -L "$snapshot"
  test "$(stat -c '%U:%G:%a' "$snapshot")" = root:root:644
  sudo -u keydion git -C "$KEYDION_ROOT" cat-file -e \
    "${source_release}:${source_path}"
  live_blob="$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse \
    "${source_release}:${source_path}")"
  test "$live_blob" = "$recorded_blob"
  current_blob="$(git -C "$KEYDION_ROOT" hash-object "$snapshot")"
  test "$current_blob" = "$recorded_blob"
  current_sha256="$(sha256sum "$snapshot" | awk '{print $1}')"
  test "$current_sha256" = "$recorded_sha256"
}

assert_recorded_unit_live() {
  local unit="$1" expected="/etc/systemd/system/$1" origin snapshot
  assert_recorded_unit_source "$unit"
  case "$unit" in
    keydion.service)
      origin="$KEYDION_WEB_UNIT_ORIGIN"
      snapshot="$KEYDION_BACKUP_DIR/systemd/keydion.service"
      ;;
    keydion-publishing-worker.service)
      origin="$KEYDION_WORKER_UNIT_ORIGIN"
      snapshot="$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
      ;;
    *) return 1 ;;
  esac
  if test "$origin" = absent; then
    ! systemctl cat "$unit" >/dev/null 2>&1
    test ! -e "$expected"
    test ! -L "$expected"
    return 0
  fi
  systemctl cat "$unit" >/dev/null
  test "$(systemctl show --property=FragmentPath --value "$unit")" \
    = "$expected"
  test -z "$(systemctl show --property=DropInPaths --value "$unit")"
  test -f "$expected"
  test ! -L "$expected"
  test "$(stat -c '%U:%G:%a' "$expected")" = root:root:644
  cmp --silent "$snapshot" "$expected"
}
load_recorded_unit_provenance
assert_recorded_unit_source keydion.service
assert_recorded_unit_source keydion-publishing-worker.service
KEYDION_MANIFEST_SHA="$(sha256sum "$KEYDION_BACKUP_DIR/SHA256SUMS" \
  | awk '{print $1}')"
KEYDION_DB_IDENTITY_SHA="$(sha256sum \
  "$KEYDION_BACKUP_DIR/database-identity.txt" | awk '{print $1}')"
KEYDION_ROLLBACK_STARTED="$KEYDION_BACKUP_DIR/rollback-started"
KEYDION_ROLLBACK_RESTORED="$KEYDION_BACKUP_DIR/rollback-restored.complete"
KEYDION_ROLLBACK_ACTIVATED="$KEYDION_BACKUP_DIR/rollback-activated.complete"
KEYDION_ROLLBACK_ARCHIVES="$KEYDION_BACKUP_DIR/rollback-archives.complete"
KEYDION_ROLLBACK_STARTED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-started "$KEYDION_OLD_RELEASE" \
  "$KEYDION_NEW_RELEASE" "$KEYDION_MANIFEST_SHA")"
KEYDION_ROLLBACK_RESTORED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-restored "$KEYDION_OLD_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"
for KEYDION_IMPOSSIBLE_LATER_TERMINAL in \
    "$KEYDION_ROLLBACK_ACTIVATED" "$KEYDION_ROLLBACK_ARCHIVES"; do
  test ! -e "$KEYDION_IMPOSSIBLE_LATER_TERMINAL"
  test ! -L "$KEYDION_IMPOSSIBLE_LATER_TERMINAL"
done
if test -e "$KEYDION_ROLLBACK_RESTORED" || \
   test -L "$KEYDION_ROLLBACK_RESTORED"; then
  test -f "$KEYDION_ROLLBACK_STARTED"
  test ! -L "$KEYDION_ROLLBACK_STARTED"
  printf '%s\n' "$KEYDION_ROLLBACK_STARTED_EXPECTED" \
    | cmp --silent - "$KEYDION_ROLLBACK_STARTED"
  test -f "$KEYDION_ROLLBACK_RESTORED"
  test ! -L "$KEYDION_ROLLBACK_RESTORED"
  printf '%s\n' "$KEYDION_ROLLBACK_RESTORED_EXPECTED" \
    | cmp --silent - "$KEYDION_ROLLBACK_RESTORED"
  publish_marker_once "$KEYDION_ROLLBACK_STARTED" \
    "$KEYDION_ROLLBACK_STARTED_EXPECTED"
  publish_marker_once "$KEYDION_ROLLBACK_RESTORED" \
    "$KEYDION_ROLLBACK_RESTORED_EXPECTED"
  printf '%s\n' \
    'Restoration already completed; refusing destructive rollback rerun' >&2
  printf '%s\n' 'Use the standalone rollback activation entry instead' >&2
  exit 1
fi
publish_marker_once "$KEYDION_ROLLBACK_STARTED" \
  "$KEYDION_ROLLBACK_STARTED_EXPECTED"
KEYDION_DATA_DIR="$(realpath -m "$KEYDION_ROOT/data")"
KEYDION_PAPERS_DIR="$(realpath -m "$KEYDION_ROOT/papers")"
KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
export KEYDION_DATA_DIR KEYDION_PAPERS_DIR KEYDION_PENDING_DIR
test "$KEYDION_PAPERS_DIR" = /Keydion/papers
test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers

KEYDION_MOUNT_TARGETS="$(findmnt --raw --noheadings --output TARGET)"
assert_rename_tree_not_mounted() {
  local tree="$1"
  local mount_target
  while IFS= read -r mount_target; do
    case "$mount_target" in
      "$tree"|"$tree"/*)
        printf '%s is or contains a mount and cannot use rename rollback\n' \
          "$tree" >&2
        return 1
        ;;
    esac
  done <<< "$KEYDION_MOUNT_TARGETS"
}
assert_rename_tree_not_mounted "$KEYDION_ROOT/.venv"
assert_rename_tree_not_mounted "$KEYDION_PAPERS_DIR"
assert_rename_tree_not_mounted "$KEYDION_PENDING_DIR"

assert_tracked_unit_fragment() {
  local unit="$1"
  local expected="/etc/systemd/system/$unit"
  local fragment
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  test "$fragment" = "$expected"
  test -f "$expected"
  test ! -L "$expected"
}
assert_tracked_unit_fragment keydion.service
if systemctl cat keydion-publishing-worker.service >/dev/null 2>&1; then
  assert_tracked_unit_fragment keydion-publishing-worker.service
  sudo systemctl disable keydion-publishing-worker
  sudo systemctl stop keydion-publishing-worker
  test "$(systemctl is-active keydion-publishing-worker)" = inactive
  test "$(systemctl is-enabled keydion-publishing-worker)" = disabled
fi
sudo systemctl disable keydion
sudo systemctl stop keydion
test "$(systemctl is-active keydion)" = inactive
test "$(systemctl is-enabled keydion)" = disabled
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c SHA256SUMS
)
gzip -t "$KEYDION_BACKUP_DIR/database.sql.gz"
tar -tf "$KEYDION_BACKUP_DIR/papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/pending-papers.tar" >/dev/null
tar -tf "$KEYDION_BACKUP_DIR/venv.tar" >/dev/null

assert_recorded_storage_provenance() {
  local papers_label expected_papers_source papers_extra
  local pending_label expected_pending_source pending_extra
  local KEYDION_PAPERS_MOUNT_TARGET KEYDION_PENDING_MOUNT_TARGET
  test -f "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test ! -L "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  mapfile -t KEYDION_STORAGE_SOURCE_LINES \
    < "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test "${#KEYDION_STORAGE_SOURCE_LINES[@]}" -eq 2
  IFS=$'\t' read -r papers_label expected_papers_source papers_extra \
    <<< "${KEYDION_STORAGE_SOURCE_LINES[0]}"
  IFS=$'\t' read -r pending_label expected_pending_source pending_extra \
    <<< "${KEYDION_STORAGE_SOURCE_LINES[1]}"
  test "$papers_label" = papers
  test "$pending_label" = pending-papers
  test -z "$papers_extra"
  test -z "$pending_extra"
  test -d "$KEYDION_PAPERS_DIR"
  test ! -L "$KEYDION_PAPERS_DIR"
  test "$(realpath "$KEYDION_PAPERS_DIR")" = "$KEYDION_PAPERS_DIR"
  test -d "$KEYDION_PENDING_DIR"
  test ! -L "$KEYDION_PENDING_DIR"
  test "$(realpath "$KEYDION_PENDING_DIR")" = "$KEYDION_PENDING_DIR"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PAPERS_DIR")" = "$expected_papers_source"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PENDING_DIR")" = "$expected_pending_source"
  KEYDION_PAPERS_MOUNT_TARGET="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PAPERS_DIR")"
  KEYDION_PENDING_MOUNT_TARGET="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PENDING_DIR")"
  test "$(stat -c '%d' "$KEYDION_PAPERS_DIR")" \
    = "$(stat -c '%d' "$KEYDION_PAPERS_MOUNT_TARGET")"
  test "$(stat -c '%d' "$KEYDION_PENDING_DIR")" \
    = "$(stat -c '%d' "$KEYDION_PENDING_MOUNT_TARGET")"
  sudo -u keydion test -r "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -w "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -r "$KEYDION_PENDING_DIR"
  sudo -u keydion test -w "$KEYDION_PENDING_DIR"
}
assert_recorded_storage_provenance

mapfile -t KEYDION_UNIT_STATE_LINES \
  < "$KEYDION_BACKUP_DIR/systemd/unit-state.tsv"
test "${#KEYDION_UNIT_STATE_LINES[@]}" -eq 2
IFS=$'\t' read -r KEYDION_WEB_UNIT_NAME KEYDION_WEB_ENABLED_STATE \
  KEYDION_WEB_ACTIVE_STATE KEYDION_WEB_STATE_EXTRA \
  <<< "${KEYDION_UNIT_STATE_LINES[0]}"
IFS=$'\t' read -r KEYDION_WORKER_UNIT_NAME KEYDION_WORKER_ENABLED_STATE \
  KEYDION_WORKER_ACTIVE_STATE KEYDION_WORKER_STATE_EXTRA \
  <<< "${KEYDION_UNIT_STATE_LINES[1]}"
test "$KEYDION_WEB_UNIT_NAME" = keydion.service
test "$KEYDION_WORKER_UNIT_NAME" = keydion-publishing-worker.service
test -z "$KEYDION_WEB_STATE_EXTRA"
test -z "$KEYDION_WORKER_STATE_EXTRA"
case "$KEYDION_WEB_ENABLED_STATE:$KEYDION_WEB_ACTIVE_STATE" in
  enabled:active|enabled:inactive|disabled:active|disabled:inactive) ;;
  *) printf '%s\n' 'Invalid recorded web unit state' >&2; exit 1 ;;
esac
case "$KEYDION_WORKER_ENABLED_STATE:$KEYDION_WORKER_ACTIVE_STATE" in
  enabled:active|enabled:inactive|disabled:active|disabled:inactive|absent:absent) ;;
  *) printf '%s\n' 'Invalid recorded worker unit state' >&2; exit 1 ;;
esac

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
assert_boundary_checkout_safe() {
  local status_file entry
  status_file="$(mktemp)"
  if ! sudo -u keydion git -C "$KEYDION_ROOT" status \
      --porcelain=v1 -z --untracked-files=all > "$status_file"; then
    rm -f -- "$status_file"
    return 1
  fi
  while IFS= read -r -d '' entry; do
    case "$entry" in
      "?? .venv.failed-${KEYDION_BACKUP_ID}"|\
      "?? .venv.failed-${KEYDION_BACKUP_ID}/"*|\
      "?? papers.failed-${KEYDION_BACKUP_ID}"|\
      "?? papers.failed-${KEYDION_BACKUP_ID}/"*|\
      "?? data/pending_papers.failed-${KEYDION_BACKUP_ID}"|\
      "?? data/pending_papers.failed-${KEYDION_BACKUP_ID}/"*) ;;
      *)
        printf 'Unsafe checkout entry for rollback: %q\n' "$entry" >&2
        rm -f -- "$status_file"
        return 1
        ;;
    esac
  done < "$status_file"
  rm -f -- "$status_file"
}
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
assert_boundary_checkout_safe
sudo -u keydion git -C "$KEYDION_ROOT" checkout --detach \
  "$KEYDION_OLD_RELEASE"
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_OLD_RELEASE"
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
assert_boundary_checkout_safe
load_recorded_unit_provenance
assert_recorded_unit_source keydion.service
assert_recorded_unit_source keydion-publishing-worker.service

restore_tree_once() {
  local label="$1"
  local current failed stage archive entry started complete archive_sha
  local current_parent failed_parent expected_marker path

  case "$label" in
    venv)
      current="$KEYDION_ROOT/.venv"
      failed="$KEYDION_ROOT/.venv.failed-${KEYDION_BACKUP_ID}"
      stage="$KEYDION_ROOT/.rollback-venv-${KEYDION_BACKUP_ID}"
      archive="$KEYDION_BACKUP_DIR/venv.tar"
      entry=.venv
      started="$KEYDION_BACKUP_DIR/venv-restore.started"
      complete="$KEYDION_BACKUP_DIR/venv-restore.complete"
      ;;
    papers)
      current="$KEYDION_PAPERS_DIR"
      failed="${KEYDION_PAPERS_DIR}.failed-${KEYDION_BACKUP_ID}"
      stage="$KEYDION_ROOT/.rollback-papers-${KEYDION_BACKUP_ID}"
      archive="$KEYDION_BACKUP_DIR/papers.tar"
      entry=papers
      started="$KEYDION_BACKUP_DIR/papers-restore.started"
      complete="$KEYDION_BACKUP_DIR/papers-restore.complete"
      ;;
    pending-papers)
      current="$KEYDION_PENDING_DIR"
      failed="${KEYDION_PENDING_DIR}.failed-${KEYDION_BACKUP_ID}"
      stage="$KEYDION_DATA_DIR/.rollback-pending-papers-${KEYDION_BACKUP_ID}"
      archive="$KEYDION_BACKUP_DIR/pending-papers.tar"
      entry=pending_papers
      started="$KEYDION_BACKUP_DIR/pending-papers-restore.started"
      complete="$KEYDION_BACKUP_DIR/pending-papers-restore.complete"
      ;;
    *) printf 'Unknown restore tree: %s\n' "$label" >&2; return 1 ;;
  esac
  current_parent="$(dirname "$current")"
  failed_parent="$(dirname "$failed")"
  test -f "$archive"
  test ! -L "$archive"
  archive_sha="$(sha256sum "$archive" | awk '{print $1}')"
  [[ "$archive_sha" =~ ^[0-9a-f]{64}$ ]]
  expected_marker="$(printf '%s\t%s\t%s' \
    "$KEYDION_BACKUP_ID" "$label" "$archive_sha")"

  # Symlinks are never valid recovery state, even when dangling.
  for path in "$current" "$failed" "$stage" "$started" "$complete"; do
    if test -L "$path"; then
      printf 'Symlink in %s restore state: %s\n' "$label" "$path" >&2
      return 1
    fi
  done

  if test -e "$complete" || test -L "$complete"; then
    test -f "$complete"
    test ! -L "$complete"
    if ! test -f "$started" || test -L "$started"; then
      printf '%s\n' 'complete without started' >&2
      return 1
    fi
    publish_marker_once "$started" "$expected_marker"
    publish_marker_once "$complete" "$expected_marker"
    test -d "$current"
    test ! -L "$current"
    test -d "$failed"
    test ! -L "$failed"
    test ! -e "$stage"
    tar --compare --acls --xattrs --numeric-owner -f "$archive" \
      -C "$current_parent" "$entry"
    return 0
  fi

  if { test -e "$failed" || test -L "$failed"; } && \
     ! test -f "$started"; then
    printf '%s\n' 'failed tree without started' >&2
    return 1
  fi

  if test -e "$started" || test -L "$started"; then
    publish_marker_once "$started" "$expected_marker"
  else
    test -d "$current"
    test ! -L "$current"
    test ! -e "$failed"
    test ! -L "$failed"
    test ! -e "$stage"
    publish_marker_once "$started" "$expected_marker"
  fi

  if ! test -e "$failed" && ! test -L "$failed"; then
    test -d "$current"
    test ! -L "$current"
    # An interruption while extracting leaves only the trusted current tree
    # plus a disposable stage. Rebuild the stage from the immutable archive.
    if test -e "$stage"; then
      rm -rf -- "$stage"
      sync -f "$current_parent"
    fi
    mkdir --mode=0700 -- "$stage"
    tar --acls --xattrs --numeric-owner -xpf "$archive" -C "$stage"
    test -d "$stage/$entry"
    test ! -L "$stage/$entry"
    sync -f "$stage/$entry"
    sync -f "$stage"
    mv -- "$current" "$failed"
    sync -f "$failed_parent"
  elif test -e "$current" || test -L "$current"; then
    test -d "$current"
    test ! -L "$current"
    test -d "$failed"
    test ! -L "$failed"
    if test -e "$stage"; then
      rmdir -- "$stage"
      sync -f "$current_parent"
    fi
    tar --compare --acls --xattrs --numeric-owner -f "$archive" \
      -C "$current_parent" "$entry"
    publish_marker_once "$complete" "$expected_marker"
    return 0
  fi

  test -d "$failed"
  test ! -L "$failed"
  test ! -e "$current"
  test ! -L "$current"
  if ! test -d "$stage" || test -L "$stage"; then
    printf '%s\n' 'missing current and stage' >&2
    return 1
  fi
  test -d "$stage/$entry"
  test ! -L "$stage/$entry"
  mv -- "$stage/$entry" "$current"
  sync -f "$current_parent"
  rmdir -- "$stage"
  sync -f "$current_parent"
  tar --compare --acls --xattrs --numeric-owner -f "$archive" \
    -C "$current_parent" "$entry"
  publish_marker_once "$complete" "$expected_marker"
}

restore_tree_once venv
sudo -u keydion test -x "$KEYDION_ROOT/.venv/bin/python"
sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" -m pip check

read_dotenv_value() {
  local key="$1" required="${2:-1}" line value="" match_count=0
  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] || return 1
  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      *\\) return 1 ;;
    esac
    if [[ "$line" =~ ^[[:space:]]*${key}[[:space:]]*= ]] && \
       [[ "$line" != "$key="* ]]; then
      return 1
    fi
    case "$line" in
      "$key="*) match_count=$((match_count + 1)); value="${line#*=}" ;;
    esac
  done < "$KEYDION_ROOT/.env.prod"
  if test "$match_count" -eq 0; then
    test "$required" -eq 0
    printf ''
    return 0
  fi
  test "$match_count" -eq 1
  [[ "$value" != *\\* ]]
  if test "${value:0:1}" = '"'; then
    test "${#value}" -ge 2
    test "${value: -1}" = '"'
    value="${value:1:${#value}-2}"
    [[ "$value" != *\"* ]]
  elif test "${value:0:1}" = "'"; then
    test "${#value}" -ge 2
    test "${value: -1}" = "'"
    value="${value:1:${#value}-2}"
    [[ "$value" != *\'* ]]
  else
    [[ ! "$value" =~ [[:space:]] ]]
    [[ "$value" != *\"* ]]
    [[ "$value" != *\'* ]]
  fi
  if test "$required" -eq 1; then test -n "$value"; fi
  printf '%s' "$value"
}
reload_validated_runtime_config() {
  test -f "$KEYDION_ROOT/.env.prod"
  test ! -L "$KEYDION_ROOT/.env.prod"
  test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
  test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
    = root:keydion:640
  PAPERQUERY_DATABASE_URL="$(read_dotenv_value PAPERQUERY_DATABASE_URL)"
  PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
  PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"
  KEYDION_JOB_LEASE_SECONDS="$(read_dotenv_value \
    PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS 0)"
  KEYDION_JOB_LEASE_SECONDS="${KEYDION_JOB_LEASE_SECONDS:-1800}"
  [[ "$KEYDION_JOB_LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]]
  if ! test "$KEYDION_JOB_LEASE_SECONDS" -le 1800; then
    printf '%s\n' \
      'PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS must not exceed 1800 seconds' >&2
    return 1
  fi
  case "${PAPERQUERY_DATA_DIR:-}" in
    ""|/Keydion/data) ;;
    *) printf '%s\n' 'PAPERQUERY_DATA_DIR must be empty or /Keydion/data' >&2; return 1 ;;
  esac
  case "${PAPERQUERY_UPLOAD_DIR:-}" in
    ""|/Keydion/papers) ;;
    *) printf '%s\n' 'PAPERQUERY_UPLOAD_DIR must be empty or /Keydion/papers' >&2; return 1 ;;
  esac
  export PAPERQUERY_DATABASE_URL PAPERQUERY_DATA_DIR PAPERQUERY_UPLOAD_DIR
  KEYDION_DATA_DIR="$(realpath -m "${PAPERQUERY_DATA_DIR:-$KEYDION_ROOT/data}")"
  KEYDION_PAPERS_DIR="$(realpath -m \
    "${PAPERQUERY_UPLOAD_DIR:-$KEYDION_ROOT/papers}")"
  KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
  test "$KEYDION_DATA_DIR" = /Keydion/data
  test "$KEYDION_PAPERS_DIR" = /Keydion/papers
  test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers
}
reload_validated_runtime_config
assert_fresh_database_identity() {
  local application_identity backup_identity expected_identity
  application_identity="$(sudo -u keydion \
    --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
    "$KEYDION_ROOT/.venv/bin/python" - <<'PY'
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
if url.get_backend_name() != "mysql" or url.database != os.environ["KEYDION_DATABASE"]:
    raise SystemExit("unexpected database target")
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
  backup_identity="$(mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()')"
  expected_identity="$(cat "$KEYDION_BACKUP_DIR/database-identity.txt")"
  test "$application_identity" = "$expected_identity"
  test "$backup_identity" = "$expected_identity"
}

KEYDION_EXPECTED_DB_IDENTITY="$(cat \
  "$KEYDION_BACKUP_DIR/database-identity.txt")"
test "${KEYDION_EXPECTED_DB_IDENTITY//$'\n'/}" \
  = "$KEYDION_EXPECTED_DB_IDENTITY"
IFS=$'\t' read -r KEYDION_EXPECTED_SERVER_UUID \
  KEYDION_EXPECTED_DATABASE KEYDION_EXPECTED_EXTRA \
  <<< "$KEYDION_EXPECTED_DB_IDENTITY"
[[ "$KEYDION_EXPECTED_SERVER_UUID" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]]
test "$KEYDION_EXPECTED_DATABASE" = "$KEYDION_DATABASE"
test -z "$KEYDION_EXPECTED_EXTRA"

# A retry may begin after the prior attempt dropped the application schema.
# Prove the same MySQL server without selecting the application schema, then
# bind the explicit keydion schema constant back to the recorded identity.
KEYDION_BACKUP_SERVER_UUID="$(
  mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names \
    -e 'SELECT @@GLOBAL.server_uuid'
)"
printf -v KEYDION_BACKUP_DB_IDENTITY '%s\t%s' \
  "$KEYDION_BACKUP_SERVER_UUID" "$KEYDION_DATABASE"
test "$KEYDION_BACKUP_DB_IDENTITY" = "$KEYDION_EXPECTED_DB_IDENTITY"

KEYDION_DATABASE_DEFAULTS="$(cat \
  "$KEYDION_BACKUP_DIR/database-defaults.txt")"
test "${KEYDION_DATABASE_DEFAULTS//$'\n'/}" = "$KEYDION_DATABASE_DEFAULTS"
IFS=$'\t' read -r KEYDION_DATABASE_CHARACTER_SET \
  KEYDION_DATABASE_COLLATION KEYDION_DATABASE_DEFAULTS_EXTRA \
  <<< "$KEYDION_DATABASE_DEFAULTS"
[[ "$KEYDION_DATABASE_CHARACTER_SET" =~ ^[A-Za-z0-9_]+$ ]]
[[ "$KEYDION_DATABASE_COLLATION" =~ ^[A-Za-z0-9_]+$ ]]
test -z "$KEYDION_DATABASE_DEFAULTS_EXTRA"

preserve_failed_database_once() {
  local dump="$KEYDION_BACKUP_DIR/failed-current-database.sql.gz"
  local checksum="$KEYDION_BACKUP_DIR/failed-current-database.sha256"
  local partial="$KEYDION_BACKUP_DIR/failed-current-database.sql.gz.partial"
  local checksum_partial="$KEYDION_BACKUP_DIR/failed-current-database.sha256.partial"
  local estimated_bytes available_backup_bytes

  if test -e "$dump" || test -L "$dump"; then
    test -f "$dump"
    test ! -L "$dump"
    if test -e "$partial" || test -L "$partial"; then
      test -f "$partial"
      test ! -L "$partial"
      rm -f -- "$partial"
      sync -f "$KEYDION_BACKUP_DIR"
    fi
    gzip -t "$dump"
    sync -f "$dump"
    sync -f "$KEYDION_BACKUP_DIR"
    if test -e "$checksum" || test -L "$checksum"; then
      test -f "$checksum"
      test ! -L "$checksum"
    else
      if test -e "$checksum_partial" || test -L "$checksum_partial"; then
        test -f "$checksum_partial"
        test ! -L "$checksum_partial"
        rm -f -- "$checksum_partial"
      fi
      (
        cd "$KEYDION_BACKUP_DIR"
        set -o noclobber
        sha256sum failed-current-database.sql.gz \
          > failed-current-database.sha256.partial
      )
      sync -f "$checksum_partial"
      mv -- "$checksum_partial" "$checksum"
    fi
    sync -f "$checksum"
    sync -f "$KEYDION_BACKUP_DIR"
    (
      cd "$KEYDION_BACKUP_DIR"
      sha256sum -c failed-current-database.sha256
    )
    return 0
  fi

  test ! -e "$checksum"
  test ! -L "$checksum"
  if test -e "$checksum_partial" || test -L "$checksum_partial"; then
    test -f "$checksum_partial"
    test ! -L "$checksum_partial"
    rm -f -- "$checksum_partial"
    sync -f "$KEYDION_BACKUP_DIR"
  fi
  estimated_bytes="$(
    mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
      --batch --skip-column-names \
      -e "SELECT COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '${KEYDION_DATABASE}'"
  )"
  available_backup_bytes="$(
    df --output=avail -B1 "$KEYDION_BACKUP_DIR" \
      | awk 'NR == 2 {print $1}'
  )"
  [[ "$estimated_bytes" =~ ^[0-9]+$ ]]
  [[ "$available_backup_bytes" =~ ^[0-9]+$ ]]
  test "$available_backup_bytes" -ge "$((estimated_bytes + 268435456))"
  if test -e "$partial" || test -L "$partial"; then
    test -f "$partial"
    test ! -L "$partial"
    rm -f -- "$partial"
  fi
  (
    set -o noclobber
    mysqldump --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
      --single-transaction --quick --hex-blob \
      --default-character-set="$KEYDION_DATABASE_CHARACTER_SET" \
      --routines --triggers --events \
      --set-gtid-purged=OFF "$KEYDION_DATABASE" \
      | gzip -1 > "$partial"
  )
  gzip -t "$partial"
  sync -f "$partial"
  mv -- "$partial" "$dump"
  sync -f "$dump"
  sync -f "$KEYDION_BACKUP_DIR"
  (
    cd "$KEYDION_BACKUP_DIR"
    set -o noclobber
    sha256sum failed-current-database.sql.gz \
      > failed-current-database.sha256.partial
  )
  sync -f "$checksum_partial"
  mv -- "$checksum_partial" "$checksum"
  sync -f "$checksum"
  sync -f "$KEYDION_BACKUP_DIR"
  (
    cd "$KEYDION_BACKUP_DIR"
    sha256sum -c failed-current-database.sha256
  )
}

preserve_failed_database_once
mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  -e "DROP DATABASE IF EXISTS \`${KEYDION_DATABASE}\`; CREATE DATABASE \`${KEYDION_DATABASE}\` CHARACTER SET ${KEYDION_DATABASE_CHARACTER_SET} COLLATE ${KEYDION_DATABASE_COLLATION};"
gzip -dc "$KEYDION_BACKUP_DIR/database.sql.gz" \
  | mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" "$KEYDION_DATABASE"
mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  --batch --skip-column-names "$KEYDION_DATABASE" \
  -e "SELECT 'papers_metadata', COUNT(*), 0 FROM papers_metadata UNION ALL SELECT 'papers_chunks', COUNT(*), COALESCE(SUM(OCTET_LENGTH(embedding_vec)), 0) FROM papers_chunks UNION ALL SELECT 'submissions', COUNT(*), 0 FROM submissions ORDER BY 1" \
  > "$KEYDION_BACKUP_DIR/database-restored-metrics.txt"
mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
  --batch --raw --skip-column-names "$KEYDION_DATABASE" \
  -e 'SELECT id, HEX(embedding_vec) FROM papers_chunks ORDER BY id' \
  | sha256sum | awk '{print $1}' \
  > "$KEYDION_BACKUP_DIR/database-restored-vectors.sha256"
cmp --silent "$KEYDION_BACKUP_DIR/database-source-metrics.txt" \
  "$KEYDION_BACKUP_DIR/database-restored-metrics.txt"
cmp --silent "$KEYDION_BACKUP_DIR/database-source-vectors.sha256" \
  "$KEYDION_BACKUP_DIR/database-restored-vectors.sha256"

assert_recorded_storage_provenance
restore_tree_once papers
assert_recorded_storage_provenance
restore_tree_once pending-papers

if test -f "$KEYDION_BACKUP_DIR/systemd/keydion.service.absent"; then
  if systemctl cat keydion.service >/dev/null 2>&1; then
    assert_tracked_unit_fragment keydion.service
  fi
  sudo rm -f /etc/systemd/system/keydion.service
else
  if systemctl cat keydion.service >/dev/null 2>&1; then
    assert_tracked_unit_fragment keydion.service
  fi
  sudo cp --preserve=mode,ownership,timestamps \
    "$KEYDION_BACKUP_DIR/systemd/keydion.service" \
    /etc/systemd/system/keydion.service
fi
if test -f \
  "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service.absent"; then
  if systemctl cat keydion-publishing-worker.service >/dev/null 2>&1; then
    assert_tracked_unit_fragment keydion-publishing-worker.service
    sudo systemctl disable keydion-publishing-worker.service
  fi
  sudo rm -f /etc/systemd/system/keydion-publishing-worker.service
else
  if systemctl cat keydion-publishing-worker.service >/dev/null 2>&1; then
    assert_tracked_unit_fragment keydion-publishing-worker.service
  fi
  sudo cp --preserve=mode,ownership,timestamps \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service" \
    /etc/systemd/system/keydion-publishing-worker.service
fi
sudo systemctl daemon-reload
if test -f "$KEYDION_BACKUP_DIR/systemd/keydion.service"; then
  assert_tracked_unit_fragment keydion.service
fi
if test -f \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"; then
  assert_tracked_unit_fragment keydion-publishing-worker.service
fi
assert_recorded_unit_live keydion.service
assert_recorded_unit_live keydion-publishing-worker.service

restore_recorded_unit_state() {
  local unit="$1" enabled_state="$2" active_state="$3"
  if test "$enabled_state:$active_state" = absent:absent; then
    if systemctl cat "$unit" >/dev/null 2>&1; then
      printf '%s should be absent after rollback\n' "$unit" >&2
      return 1
    fi
    return 0
  fi

  case "$enabled_state" in
    enabled|disabled) ;;
    *) printf 'Invalid enabled state for %s: %s\n' \
         "$unit" "$enabled_state" >&2; return 1 ;;
  esac
  case "$active_state" in
    active|inactive) ;;
    *) printf 'Invalid active state for %s: %s\n' \
         "$unit" "$active_state" >&2; return 1 ;;
  esac
  assert_tracked_unit_fragment "$unit"
  sudo systemctl disable "$unit"
  sudo systemctl stop "$unit"
  test "$(systemctl is-enabled "$unit")" = disabled
  test "$(systemctl is-active "$unit")" = inactive
}

restore_recorded_unit_state keydion-publishing-worker.service \
  "$KEYDION_WORKER_ENABLED_STATE" "$KEYDION_WORKER_ACTIVE_STATE"
restore_recorded_unit_state keydion.service \
  "$KEYDION_WEB_ENABLED_STATE" "$KEYDION_WEB_ACTIVE_STATE"

# Re-prove the complete restored boundary immediately before publishing its
# terminal. No later activation entry may infer this state from HEAD alone.
restore_tree_once venv
restore_tree_once papers
restore_tree_once pending-papers
cmp --silent "$KEYDION_BACKUP_DIR/database-source-metrics.txt" \
  "$KEYDION_BACKUP_DIR/database-restored-metrics.txt"
cmp --silent "$KEYDION_BACKUP_DIR/database-source-vectors.sha256" \
  "$KEYDION_BACKUP_DIR/database-restored-vectors.sha256"
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_OLD_RELEASE"
sudo -u keydion "$KEYDION_ROOT/.venv/bin/python" -m pip check
assert_recorded_storage_provenance
assert_fresh_database_identity
load_recorded_unit_provenance
assert_recorded_unit_live keydion.service
assert_recorded_unit_live keydion-publishing-worker.service
if test -f "$KEYDION_BACKUP_DIR/systemd/keydion.service"; then
  cmp --silent "$KEYDION_BACKUP_DIR/systemd/keydion.service" \
    /etc/systemd/system/keydion.service
  test "$(systemctl is-enabled keydion.service)" = disabled
  test "$(systemctl is-active keydion.service)" = inactive
fi
if test -f \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"; then
  cmp --silent \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service" \
    /etc/systemd/system/keydion-publishing-worker.service
  test "$(systemctl is-enabled keydion-publishing-worker.service)" = disabled
  test "$(systemctl is-active keydion-publishing-worker.service)" = inactive
fi
publish_marker_once "$KEYDION_ROLLBACK_RESTORED" \
  "$KEYDION_ROLLBACK_RESTORED_EXPECTED"
exit
```

The final `exit` releases the process-lifetime lock. Confirm that destructive
rollback shell has ended before opening the standalone activation entry.

Every checksum, archive-integrity check, virtual-environment restore/`pip check`,
and MySQL server identity check occurs before the database is dropped. An
exclusive `failed-current-database.sql.gz` plus its separate checksum preserves
the current/partial database for reconciliation and diagnosis before any
destructive SQL. The server check deliberately works when `keydion` is absent
or half-imported, so a retry can repeat the explicit drop/create/replay sequence
using the manifest's original character set and collation. Representative row
counts, binary/vector bytes, and the ordered vector-content digest must match
the original snapshot after import.
Rollback accepts `HEAD` at either the recorded old release (for an early
candidate-checkout failure) or the recorded candidate release, and rejects
every other checkout. The candidate virtual environment remains at the
`.venv.failed-<backup-id>` path for diagnosis until rollback is fully verified.

The root-owned `*.started` and `*.complete` phase markers make each filesystem
swap resumable without overwriting the saved candidate tree. Because rollback
uses `set -euo pipefail`, any checkout, import, extraction, unit restoration, or
`daemon-reload` failure exits immediately. Leave traffic closed, correct only
the external cause, and rerun this same rollback block with the same backup ID
only while `rollback-restored.complete` is absent. Once that terminal exists,
this destructive entry refuses to run again; use the standalone activation
entry below. Before the restored terminal, this block revalidates the immutable
manifest, safely restarts database recreation, and resumes or verifies each
tree swap from its phase markers. It validates and reuses the failed-current
database dump rather than overwriting it. Do not
remove or edit the markers, safety dump, or `.failed-<backup-id>` trees.

Verify the restored revision, virtual environment, Paper/pending counts and
hashes, `HEAD`, and the recorded old unit presence/state while the maintenance
gate remains closed. The destructive block deliberately leaves every present
service disabled and inactive, so neither a reboot nor the worker can mutate
the restored snapshot during reconciliation. Reopen traffic only after the
whole boundary is confirmed to belong together and the authorized
post-snapshot write reconciliation is complete.

Only after those checks and reconciliation succeed, use this self-contained
activation entry to restore recorded enablement and activity, worker first. It
re-reads the checksummed state by backup ID and never touches the database or
storage, so after a reboot or disconnected shell do not rerun destructive
rollback merely to reconstruct shell variables. Keep the traffic gate closed
until both commands finish:

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077
KEYDION_RECOVERY_LOCK=/run/lock/keydion-paper-publishing.lock
test -d /run/lock
test ! -L /run/lock
test "$(stat -c '%U:%G' /run/lock)" = root:root
if ! test -e "$KEYDION_RECOVERY_LOCK"; then
  (set -o noclobber; : > "$KEYDION_RECOVERY_LOCK") 2>/dev/null \
    || test -f "$KEYDION_RECOVERY_LOCK"
fi
test -f "$KEYDION_RECOVERY_LOCK"
test ! -L "$KEYDION_RECOVERY_LOCK"
test "$(stat -c '%U:%G:%a' "$KEYDION_RECOVERY_LOCK")" = root:root:600
exec {KEYDION_RECOVERY_LOCK_FD}<>"$KEYDION_RECOVERY_LOCK"
flock --exclusive --nonblock "$KEYDION_RECOVERY_LOCK_FD"
export KEYDION_ROOT=/Keydion
export KEYDION_DATABASE=keydion
export KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
test -f "$KEYDION_MYSQL_DEFAULTS"
test ! -L "$KEYDION_MYSQL_DEFAULTS"
test "$(stat -c '%U:%G:%a' "$KEYDION_MYSQL_DEFAULTS")" = root:root:600
load_active_boundary() {
  local selector=/srv/keydion-backups/paper-publishing-active
  local boundary selector_inode boundary_inode
  test -f "$selector"
  test ! -L "$selector"
  test "$(stat -c '%U:%G:%a' "$selector")" = root:root:600
  KEYDION_BACKUP_ID="$(cat "$selector")"
  [[ "$KEYDION_BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
  export KEYDION_BACKUP_ID
  export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
  boundary="$KEYDION_BACKUP_DIR/active-boundary"
  test -f "$boundary"
  test ! -L "$boundary"
  test "$(stat -c '%U:%G:%a' "$boundary")" = root:root:600
  selector_inode="$(stat -c '%d:%i' "$selector")"
  boundary_inode="$(stat -c '%d:%i' "$boundary")"
  test "$selector_inode" = "$boundary_inode"
  printf '%s\n' "$KEYDION_BACKUP_ID" | cmp --silent - "$boundary"
  (
    cd "$KEYDION_BACKUP_DIR"
    sha256sum -c pre-snapshot.sha256
  )
}
load_active_boundary
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c SHA256SUMS
)

publish_marker_once() {
  local target="$1" expected="$2" parent base partial stale
  local -a stale_partials=()
  parent="$(dirname "$target")"
  base="$(basename "$target")"
  test -d "$parent"
  test ! -L "$parent"
  test -n "$expected"
  test "${expected//$'\n'/}" = "$expected"
  test ! -L "$target"
  shopt -s nullglob
  stale_partials=("$parent/.${base}.partial."*)
  shopt -u nullglob
  for stale in "${stale_partials[@]}"; do
    test -f "$stale"
    test ! -L "$stale"
    rm -f -- "$stale"
  done
  sync -f "$parent"
  if test -e "$target"; then
    test -f "$target"
    test ! -L "$target"
    printf '%s\n' "$expected" | cmp --silent - "$target"
    sync -f "$target"
    sync -f "$parent"
    return 0
  fi
  partial="$(mktemp "$parent/.${base}.partial.XXXXXXXX")"
  test -f "$partial"
  test ! -L "$partial"
  printf '%s\n' "$expected" > "$partial"
  sync -f "$partial"
  if ! ln -- "$partial" "$target" 2>/dev/null; then
    test -f "$target"
    test ! -L "$target"
    printf '%s\n' "$expected" | cmp --silent - "$target"
  fi
  printf '%s\n' "$expected" | cmp --silent - "$target"
  sync -f "$target"
  sync -f "$parent"
  rm -f -- "$partial"
  sync -f "$parent"
  printf '%s\n' "$expected" | cmp --silent - "$target"
}
KEYDION_OLD_RELEASE="$(cat "$KEYDION_BACKUP_DIR/old-release.txt")"
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
KEYDION_MANIFEST_SHA="$(sha256sum "$KEYDION_BACKUP_DIR/SHA256SUMS" \
  | awk '{print $1}')"
KEYDION_DB_IDENTITY_SHA="$(sha256sum \
  "$KEYDION_BACKUP_DIR/database-identity.txt" | awk '{print $1}')"
KEYDION_ROLLBACK_STARTED="$KEYDION_BACKUP_DIR/rollback-started"
KEYDION_ROLLBACK_RESTORED="$KEYDION_BACKUP_DIR/rollback-restored.complete"
KEYDION_ROLLBACK_ACTIVATED="$KEYDION_BACKUP_DIR/rollback-activated.complete"
KEYDION_ROLLBACK_STARTED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-started "$KEYDION_OLD_RELEASE" \
  "$(cat "$KEYDION_BACKUP_DIR/new-release.txt")" "$KEYDION_MANIFEST_SHA")"
KEYDION_ROLLBACK_RESTORED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-restored "$KEYDION_OLD_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"
KEYDION_ROLLBACK_ACTIVATED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-activated "$KEYDION_OLD_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"
for KEYDION_REQUIRED_TERMINAL in \
    "$KEYDION_ROLLBACK_STARTED:$KEYDION_ROLLBACK_STARTED_EXPECTED" \
    "$KEYDION_ROLLBACK_RESTORED:$KEYDION_ROLLBACK_RESTORED_EXPECTED"; do
  KEYDION_REQUIRED_TERMINAL_PATH="${KEYDION_REQUIRED_TERMINAL%%:*}"
  KEYDION_REQUIRED_TERMINAL_PAYLOAD="${KEYDION_REQUIRED_TERMINAL#*:}"
  test -f "$KEYDION_REQUIRED_TERMINAL_PATH"
  test ! -L "$KEYDION_REQUIRED_TERMINAL_PATH"
  printf '%s\n' "$KEYDION_REQUIRED_TERMINAL_PAYLOAD" \
    | cmp --silent - "$KEYDION_REQUIRED_TERMINAL_PATH"
  publish_marker_once "$KEYDION_REQUIRED_TERMINAL_PATH" \
    "$KEYDION_REQUIRED_TERMINAL_PAYLOAD"
done
if ! test -e "$KEYDION_ROLLBACK_ACTIVATED"; then
  test ! -L "$KEYDION_ROLLBACK_ACTIVATED"
  # A retry without terminal activation evidence immediately re-establishes
  # the inactive fence before parsing configuration or waiting for approval.
  for KEYDION_REFENCE_UNIT in \
      keydion-publishing-worker.service keydion.service; do
    if systemctl cat "$KEYDION_REFENCE_UNIT" >/dev/null 2>&1; then
      sudo systemctl disable "$KEYDION_REFENCE_UNIT"
      sudo systemctl stop "$KEYDION_REFENCE_UNIT"
      test "$(systemctl is-active "$KEYDION_REFENCE_UNIT")" = inactive
      test "$(systemctl is-enabled "$KEYDION_REFENCE_UNIT")" = disabled
    fi
  done
else
  test -f "$KEYDION_ROLLBACK_ACTIVATED"
  test ! -L "$KEYDION_ROLLBACK_ACTIVATED"
  publish_marker_once "$KEYDION_ROLLBACK_ACTIVATED" \
    "$KEYDION_ROLLBACK_ACTIVATED_EXPECTED"
fi

read_dotenv_value() {
  local key="$1" required="${2:-1}" line value="" match_count=0
  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] || return 1
  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      *\\) printf '%s\n' 'EnvironmentFile continuations are unsupported' >&2; return 1 ;;
    esac
    if [[ "$line" =~ ^[[:space:]]*${key}[[:space:]]*= ]] && \
       [[ "$line" != "$key="* ]]; then
      printf '%s must use exact KEY=value spelling\n' "$key" >&2
      return 1
    fi
    case "$line" in
      "$key="*) match_count=$((match_count + 1)); value="${line#*=}" ;;
    esac
  done < "$KEYDION_ROOT/.env.prod"
  if test "$match_count" -eq 0; then
    test "$required" -eq 0
    printf ''
    return 0
  fi
  test "$match_count" -eq 1
  [[ "$value" != *\\* ]]
  if test "${value:0:1}" = '"'; then
    test "${#value}" -ge 2
    test "${value: -1}" = '"'
    value="${value:1:${#value}-2}"
    [[ "$value" != *\"* ]]
  elif test "${value:0:1}" = "'"; then
    test "${#value}" -ge 2
    test "${value: -1}" = "'"
    value="${value:1:${#value}-2}"
    [[ "$value" != *\'* ]]
  else
    [[ ! "$value" =~ [[:space:]] ]]
    [[ "$value" != *\"* ]]
    [[ "$value" != *\'* ]]
  fi
  if test "$required" -eq 1; then test -n "$value"; fi
  printf '%s' "$value"
}
reload_validated_runtime_config() {
  test -f "$KEYDION_ROOT/.env.prod"
  test ! -L "$KEYDION_ROOT/.env.prod"
  test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
  test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
    = root:keydion:640
  PAPERQUERY_DATABASE_URL="$(read_dotenv_value PAPERQUERY_DATABASE_URL)"
  PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
  PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"
  KEYDION_JOB_LEASE_SECONDS="$(read_dotenv_value \
    PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS 0)"
  KEYDION_JOB_LEASE_SECONDS="${KEYDION_JOB_LEASE_SECONDS:-1800}"
  [[ "$KEYDION_JOB_LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]]
  if ! test "$KEYDION_JOB_LEASE_SECONDS" -le 1800; then
    printf '%s\n' \
      'PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS must not exceed 1800 seconds' >&2
    return 1
  fi
  case "${PAPERQUERY_DATA_DIR:-}" in
    ""|/Keydion/data) ;;
    *) printf '%s\n' 'PAPERQUERY_DATA_DIR must be empty or /Keydion/data' >&2; return 1 ;;
  esac
  case "${PAPERQUERY_UPLOAD_DIR:-}" in
    ""|/Keydion/papers) ;;
    *) printf '%s\n' 'PAPERQUERY_UPLOAD_DIR must be empty or /Keydion/papers' >&2; return 1 ;;
  esac
  export PAPERQUERY_DATABASE_URL PAPERQUERY_DATA_DIR PAPERQUERY_UPLOAD_DIR
  KEYDION_DATA_DIR="$(realpath -m "${PAPERQUERY_DATA_DIR:-$KEYDION_ROOT/data}")"
  KEYDION_PAPERS_DIR="$(realpath -m \
    "${PAPERQUERY_UPLOAD_DIR:-$KEYDION_ROOT/papers}")"
  KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
  test "$KEYDION_DATA_DIR" = /Keydion/data
  test "$KEYDION_PAPERS_DIR" = /Keydion/papers
  test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers
}
reload_validated_runtime_config

assert_fresh_database_identity() {
  local application_identity backup_identity expected_identity
  application_identity="$(
    sudo -u keydion \
      --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
      "$KEYDION_VENV_PATH/bin/python" - <<'PY'
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
if url.get_backend_name() != "mysql" or url.database != os.environ["KEYDION_DATABASE"]:
    raise SystemExit("PAPERQUERY_DATABASE_URL must select keydion MySQL")
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
  backup_identity="$(mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()')"
  expected_identity="$(cat "$KEYDION_BACKUP_DIR/database-identity.txt")"
  test "$application_identity" = "$expected_identity"
  test "$backup_identity" = "$expected_identity"
}

assert_recorded_storage_provenance() {
  local papers_label expected_papers_source papers_extra
  local pending_label expected_pending_source pending_extra
  local KEYDION_PAPERS_MOUNT_TARGET KEYDION_PENDING_MOUNT_TARGET
  test -f "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test ! -L "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  mapfile -t KEYDION_STORAGE_SOURCE_LINES \
    < "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test "${#KEYDION_STORAGE_SOURCE_LINES[@]}" -eq 2
  IFS=$'\t' read -r papers_label expected_papers_source papers_extra \
    <<< "${KEYDION_STORAGE_SOURCE_LINES[0]}"
  IFS=$'\t' read -r pending_label expected_pending_source pending_extra \
    <<< "${KEYDION_STORAGE_SOURCE_LINES[1]}"
  test "$papers_label" = papers
  test "$pending_label" = pending-papers
  test -z "$papers_extra"
  test -z "$pending_extra"
  test -d "$KEYDION_PAPERS_DIR"
  test ! -L "$KEYDION_PAPERS_DIR"
  test "$(realpath "$KEYDION_PAPERS_DIR")" = "$KEYDION_PAPERS_DIR"
  test -d "$KEYDION_PENDING_DIR"
  test ! -L "$KEYDION_PENDING_DIR"
  test "$(realpath "$KEYDION_PENDING_DIR")" = "$KEYDION_PENDING_DIR"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PAPERS_DIR")" = "$expected_papers_source"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PENDING_DIR")" = "$expected_pending_source"
  KEYDION_PAPERS_MOUNT_TARGET="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PAPERS_DIR")"
  KEYDION_PENDING_MOUNT_TARGET="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PENDING_DIR")"
  test "$(stat -c '%d' "$KEYDION_PAPERS_DIR")" \
    = "$(stat -c '%d' "$KEYDION_PAPERS_MOUNT_TARGET")"
  test "$(stat -c '%d' "$KEYDION_PENDING_DIR")" \
    = "$(stat -c '%d' "$KEYDION_PENDING_MOUNT_TARGET")"
  sudo -u keydion test -r "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -w "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -r "$KEYDION_PENDING_DIR"
  sudo -u keydion test -w "$KEYDION_PENDING_DIR"
}
assert_recorded_storage_provenance

KEYDION_OLD_RELEASE="$(cat "$KEYDION_BACKUP_DIR/old-release.txt")"
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
test "$(sudo -u keydion git -C "$KEYDION_ROOT" rev-parse --verify HEAD)" \
  = "$KEYDION_OLD_RELEASE"
assert_boundary_checkout_safe() {
  local status_file entry
  status_file="$(mktemp)"
  if ! sudo -u keydion git -C "$KEYDION_ROOT" status \
      --porcelain=v1 -z --untracked-files=all > "$status_file"; then
    rm -f -- "$status_file"
    return 1
  fi
  while IFS= read -r -d '' entry; do
    case "$entry" in
      "?? .venv.failed-${KEYDION_BACKUP_ID}"|\
      "?? .venv.failed-${KEYDION_BACKUP_ID}/"*|\
      "?? papers.failed-${KEYDION_BACKUP_ID}"|\
      "?? papers.failed-${KEYDION_BACKUP_ID}/"*|\
      "?? data/pending_papers.failed-${KEYDION_BACKUP_ID}"|\
      "?? data/pending_papers.failed-${KEYDION_BACKUP_ID}/"*) ;;
      *)
        printf 'Unsafe checkout entry for activation: %q\n' "$entry" >&2
        rm -f -- "$status_file"
        return 1
        ;;
    esac
  done < "$status_file"
  rm -f -- "$status_file"
}
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
assert_boundary_checkout_safe
KEYDION_VENV_PATH="$(cat "$KEYDION_BACKUP_DIR/venv-path.txt")"
test "$KEYDION_VENV_PATH" = "$KEYDION_ROOT/.venv"
test -d "$KEYDION_VENV_PATH"
test ! -L "$KEYDION_VENV_PATH"
test "$(realpath "$KEYDION_VENV_PATH")" = "$KEYDION_VENV_PATH"
sudo -u keydion test -x "$KEYDION_VENV_PATH/bin/python"
sudo -u keydion "$KEYDION_VENV_PATH/bin/python" -m pip check

mapfile -t KEYDION_UNIT_STATE_LINES \
  < "$KEYDION_BACKUP_DIR/systemd/unit-state.tsv"
test "${#KEYDION_UNIT_STATE_LINES[@]}" -eq 2
IFS=$'\t' read -r KEYDION_WEB_UNIT_NAME KEYDION_WEB_ENABLED_STATE \
  KEYDION_WEB_ACTIVE_STATE KEYDION_WEB_STATE_EXTRA \
  <<< "${KEYDION_UNIT_STATE_LINES[0]}"
IFS=$'\t' read -r KEYDION_WORKER_UNIT_NAME KEYDION_WORKER_ENABLED_STATE \
  KEYDION_WORKER_ACTIVE_STATE KEYDION_WORKER_STATE_EXTRA \
  <<< "${KEYDION_UNIT_STATE_LINES[1]}"
test "$KEYDION_WEB_UNIT_NAME" = keydion.service
test "$KEYDION_WORKER_UNIT_NAME" = keydion-publishing-worker.service
test -z "$KEYDION_WEB_STATE_EXTRA"
test -z "$KEYDION_WORKER_STATE_EXTRA"
case "$KEYDION_WEB_ENABLED_STATE:$KEYDION_WEB_ACTIVE_STATE" in
  enabled:active|enabled:inactive|disabled:active|disabled:inactive) ;;
  *) printf '%s\n' 'Invalid recorded web unit state' >&2; exit 1 ;;
esac
case "$KEYDION_WORKER_ENABLED_STATE:$KEYDION_WORKER_ACTIVE_STATE" in
  enabled:active|enabled:inactive|disabled:active|disabled:inactive|absent:absent) ;;
  *) printf '%s\n' 'Invalid recorded worker unit state' >&2; exit 1 ;;
esac

read_unit_enabled_state() {
  local unit="$1"
  local state status
  if state="$(systemctl is-enabled "$unit" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$state" in
    enabled) test "$status" -eq 0 ;;
    disabled) test "$status" -ne 0 ;;
    *) printf '%s has unsupported enabled state: %s\n' \
         "$unit" "$state" >&2; return 1 ;;
  esac
  printf '%s' "$state"
}

read_unit_active_state() {
  local unit="$1"
  local state status
  if state="$(systemctl is-active "$unit" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$state" in
    active) test "$status" -eq 0 ;;
    inactive) test "$status" -ne 0 ;;
    *) printf '%s has unsupported active state: %s\n' \
         "$unit" "$state" >&2; return 1 ;;
  esac
  printf '%s' "$state"
}

assert_tracked_unit_fragment() {
  local unit="$1"
  local expected="/etc/systemd/system/$unit"
  local fragment dropins
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  test "$fragment" = "$expected"
  test -f "$expected"
  test ! -L "$expected"
  test -z "$dropins"
}

assert_unit_activation_fenced() {
  local unit="$1" enabled_state="$2" active_state="$3" snapshot="$4"
  local dropins current_enabled current_active current_state
  if test "$enabled_state:$active_state" = absent:absent; then
    if systemctl cat "$unit" >/dev/null 2>&1; then
      printf '%s should remain absent\n' "$unit" >&2
      return 1
    fi
    return 0
  fi
  systemctl cat "$unit" >/dev/null
  assert_tracked_unit_fragment "$unit"
  cmp --silent "$snapshot" "/etc/systemd/system/$unit"
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  test -z "$dropins"
  current_enabled="$(read_unit_enabled_state "$unit")"
  current_active="$(read_unit_active_state "$unit")"
  current_state="$current_enabled:$current_active"
  if test "$current_state" = disabled:inactive || \
     test "$current_state" = "$enabled_state:$active_state" || \
     { test "$enabled_state" = enabled \
       && test "$current_state" = enabled:inactive; }; then
    return 0
  fi
  printf '%s has unsafe partial activation state: %s\n' \
    "$unit" "$current_state" >&2
  return 1
}

assert_unit_activation_fenced keydion-publishing-worker.service \
  "$KEYDION_WORKER_ENABLED_STATE" "$KEYDION_WORKER_ACTIVE_STATE" \
  "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
assert_unit_activation_fenced keydion.service \
  "$KEYDION_WEB_ENABLED_STATE" "$KEYDION_WEB_ACTIVE_STATE" \
  "$KEYDION_BACKUP_DIR/systemd/keydion.service"

assert_recorded_unit_final() {
  local unit="$1" enabled_state="$2" active_state="$3" snapshot="$4"
  if test "$enabled_state:$active_state" = absent:absent; then
    ! systemctl cat "$unit" >/dev/null 2>&1
    return 0
  fi
  assert_tracked_unit_fragment "$unit"
  cmp --silent "$snapshot" "/etc/systemd/system/$unit"
  test "$(read_unit_enabled_state "$unit")" = "$enabled_state"
  test "$(read_unit_active_state "$unit")" = "$active_state"
}

if test -e "$KEYDION_ROLLBACK_ACTIVATED"; then
  assert_recorded_storage_provenance
  assert_fresh_database_identity
  assert_recorded_unit_final keydion-publishing-worker.service \
    "$KEYDION_WORKER_ENABLED_STATE" "$KEYDION_WORKER_ACTIVE_STATE" \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
  assert_recorded_unit_final keydion.service \
    "$KEYDION_WEB_ENABLED_STATE" "$KEYDION_WEB_ACTIVE_STATE" \
    "$KEYDION_BACKUP_DIR/systemd/keydion.service"
  exit 0
fi
read -r -p "Type RECONCILED to restore recorded service states: " \
  KEYDION_RECONCILIATION_APPROVAL
test "$KEYDION_RECONCILIATION_APPROVAL" = RECONCILED

restore_recorded_unit_activity() {
  local unit="$1" enabled_state="$2" active_state="$3"
  if test "$enabled_state:$active_state" = absent:absent; then
    return 0
  fi
  assert_tracked_unit_fragment "$unit"
  case "$enabled_state" in
    enabled) sudo systemctl enable "$unit" ;;
    disabled) sudo systemctl disable "$unit" ;;
    *) printf 'Invalid enabled state for %s: %s\n' \
         "$unit" "$enabled_state" >&2; return 1 ;;
  esac
  test "$(systemctl is-enabled "$unit")" = "$enabled_state"
  case "$active_state" in
    active) sudo systemctl start "$unit" ;;
    inactive) sudo systemctl stop "$unit" ;;
    *) printf 'Invalid active state for %s: %s\n' \
         "$unit" "$active_state" >&2; return 1 ;;
  esac
  test "$(systemctl is-active "$unit")" = "$active_state"
}

reload_validated_runtime_config
assert_recorded_storage_provenance
assert_fresh_database_identity
restore_recorded_unit_activity keydion-publishing-worker.service \
  "$KEYDION_WORKER_ENABLED_STATE" "$KEYDION_WORKER_ACTIVE_STATE"
restore_recorded_unit_activity keydion.service \
  "$KEYDION_WEB_ENABLED_STATE" "$KEYDION_WEB_ACTIVE_STATE"
assert_recorded_unit_final keydion-publishing-worker.service \
  "$KEYDION_WORKER_ENABLED_STATE" "$KEYDION_WORKER_ACTIVE_STATE" \
  "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
assert_recorded_unit_final keydion.service \
  "$KEYDION_WEB_ENABLED_STATE" "$KEYDION_WEB_ACTIVE_STATE" \
  "$KEYDION_BACKUP_DIR/systemd/keydion.service"
reload_validated_runtime_config
assert_recorded_storage_provenance
assert_fresh_database_identity
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
assert_boundary_checkout_safe
publish_marker_once "$KEYDION_ROLLBACK_ACTIVATED" \
  "$KEYDION_ROLLBACK_ACTIVATED_EXPECTED"
printf '%s\n' "$KEYDION_BACKUP_ID" \
  | cmp --silent - /srv/keydion-backups/paper-publishing-active
exit
```

Expected checkpoint: only units recorded as `active` are running; units
recorded as `inactive` remain stopped; the worker was started before the web;
and the original enabled/disabled states remain unchanged. Reopen traffic only
after this checkpoint. The final `exit` releases the process-lifetime lock;
confirm that activation shell has ended before opening the archive entry.

### Clear retained failed trees before a later migration

The diagnostic `.venv.failed-<backup-id>`, `papers.failed-<backup-id>`, and
`data/pending_papers.failed-<backup-id>` trees intentionally remain inside the
checkout through rollback verification. They are not ignored by Git, so before
restarting Section 1 for a later migration, obtain incident-owner approval and
relocate them into the root-only backup boundary. Do not weaken the untracked
cleanliness gate, and do not discard these trees until their retention expires.

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077
KEYDION_RECOVERY_LOCK=/run/lock/keydion-paper-publishing.lock
test -d /run/lock
test ! -L /run/lock
test "$(stat -c '%U:%G' /run/lock)" = root:root
if ! test -e "$KEYDION_RECOVERY_LOCK"; then
  (set -o noclobber; : > "$KEYDION_RECOVERY_LOCK") 2>/dev/null \
    || test -f "$KEYDION_RECOVERY_LOCK"
fi
test -f "$KEYDION_RECOVERY_LOCK"
test ! -L "$KEYDION_RECOVERY_LOCK"
test "$(stat -c '%U:%G:%a' "$KEYDION_RECOVERY_LOCK")" = root:root:600
exec {KEYDION_RECOVERY_LOCK_FD}<>"$KEYDION_RECOVERY_LOCK"
flock --exclusive --nonblock "$KEYDION_RECOVERY_LOCK_FD"
export KEYDION_ROOT=/Keydion
load_active_boundary() {
  local selector=/srv/keydion-backups/paper-publishing-active
  local boundary selector_inode boundary_inode
  test -f "$selector"
  test ! -L "$selector"
  test "$(stat -c '%U:%G:%a' "$selector")" = root:root:600
  KEYDION_BACKUP_ID="$(cat "$selector")"
  [[ "$KEYDION_BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
  export KEYDION_BACKUP_ID
  export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
  boundary="$KEYDION_BACKUP_DIR/active-boundary"
  test -f "$boundary"
  test ! -L "$boundary"
  test "$(stat -c '%U:%G:%a' "$boundary")" = root:root:600
  selector_inode="$(stat -c '%d:%i' "$selector")"
  boundary_inode="$(stat -c '%d:%i' "$boundary")"
  test "$selector_inode" = "$boundary_inode"
  printf '%s\n' "$KEYDION_BACKUP_ID" | cmp --silent - "$boundary"
  (
    cd "$KEYDION_BACKUP_DIR"
    sha256sum -c pre-snapshot.sha256
  )
}
load_active_boundary
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c SHA256SUMS
)
KEYDION_OLD_RELEASE="$(cat "$KEYDION_BACKUP_DIR/old-release.txt")"
KEYDION_NEW_RELEASE="$(cat "$KEYDION_BACKUP_DIR/new-release.txt")"
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
KEYDION_MANIFEST_SHA="$(sha256sum "$KEYDION_BACKUP_DIR/SHA256SUMS" \
  | awk '{print $1}')"
KEYDION_DB_IDENTITY_SHA="$(sha256sum \
  "$KEYDION_BACKUP_DIR/database-identity.txt" | awk '{print $1}')"
KEYDION_ROLLBACK_STARTED="$KEYDION_BACKUP_DIR/rollback-started"
KEYDION_ROLLBACK_RESTORED="$KEYDION_BACKUP_DIR/rollback-restored.complete"
KEYDION_ROLLBACK_ACTIVATED="$KEYDION_BACKUP_DIR/rollback-activated.complete"
KEYDION_ROLLBACK_ARCHIVES="$KEYDION_BACKUP_DIR/rollback-archives.complete"
KEYDION_ROLLBACK_STARTED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-started "$KEYDION_OLD_RELEASE" \
  "$KEYDION_NEW_RELEASE" "$KEYDION_MANIFEST_SHA")"
KEYDION_ROLLBACK_RESTORED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-restored "$KEYDION_OLD_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"
KEYDION_ROLLBACK_ACTIVATED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-activated "$KEYDION_OLD_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"
for KEYDION_REQUIRED_TERMINAL in \
    "$KEYDION_ROLLBACK_STARTED:$KEYDION_ROLLBACK_STARTED_EXPECTED" \
    "$KEYDION_ROLLBACK_RESTORED:$KEYDION_ROLLBACK_RESTORED_EXPECTED" \
    "$KEYDION_ROLLBACK_ACTIVATED:$KEYDION_ROLLBACK_ACTIVATED_EXPECTED"; do
  KEYDION_REQUIRED_TERMINAL_PATH="${KEYDION_REQUIRED_TERMINAL%%:*}"
  KEYDION_REQUIRED_TERMINAL_PAYLOAD="${KEYDION_REQUIRED_TERMINAL#*:}"
  test -f "$KEYDION_REQUIRED_TERMINAL_PATH"
  test ! -L "$KEYDION_REQUIRED_TERMINAL_PATH"
  printf '%s\n' "$KEYDION_REQUIRED_TERMINAL_PAYLOAD" \
    | cmp --silent - "$KEYDION_REQUIRED_TERMINAL_PATH"
  sync -f "$KEYDION_REQUIRED_TERMINAL_PATH"
  sync -f "$KEYDION_BACKUP_DIR"
done
read -r -p "Type ARCHIVE_FAILED_TREES after retention approval: " \
  KEYDION_FAILED_TREE_APPROVAL
test "$KEYDION_FAILED_TREE_APPROVAL" = ARCHIVE_FAILED_TREES
KEYDION_FAILED_TREE_DIR="$KEYDION_BACKUP_DIR/failed-trees"
if ! test -d "$KEYDION_FAILED_TREE_DIR"; then
  mkdir --mode=0700 -- "$KEYDION_FAILED_TREE_DIR"
fi
test ! -L "$KEYDION_FAILED_TREE_DIR"

publish_marker_once() {
  local target="$1" expected="$2" parent base partial stale
  local -a stale_partials=()
  parent="$(dirname "$target")"
  base="$(basename "$target")"
  test -d "$parent"
  test ! -L "$parent"
  test -n "$expected"
  test "${expected//$'\n'/}" = "$expected"
  test ! -L "$target"

  # The entry-wide lock makes stale unique partials crash residue, never a
  # concurrent publisher. Reject symlinks and remove only regular partials.
  shopt -s nullglob
  stale_partials=("$parent/.${base}.partial."*)
  shopt -u nullglob
  for stale in "${stale_partials[@]}"; do
    test -f "$stale"
    test ! -L "$stale"
    rm -f -- "$stale"
  done
  sync -f "$parent"

  if test -e "$target"; then
    test -f "$target"
    test ! -L "$target"
    printf '%s\n' "$expected" | cmp --silent - "$target"
    sync -f "$target"
    sync -f "$parent"
    return 0
  fi

  partial="$(mktemp "$parent/.${base}.partial.XXXXXXXX")"
  test -f "$partial"
  test ! -L "$partial"
  printf '%s\n' "$expected" > "$partial"
  sync -f "$partial"
  if ! ln -- "$partial" "$target" 2>/dev/null; then
    # Accept only an exact, regular no-replace winner (EEXIST semantics).
    test -f "$target"
    test ! -L "$target"
    printf '%s\n' "$expected" | cmp --silent - "$target"
  fi
  printf '%s\n' "$expected" | cmp --silent - "$target"
  sync -f "$target"
  sync -f "$parent"
  rm -f -- "$partial"
  sync -f "$parent"
  printf '%s\n' "$expected" | cmp --silent - "$target"
}

archive_failed_tree_once() {
  local label="$1" source="$2"
  local expected_source
  local destination="$KEYDION_FAILED_TREE_DIR/$label.tar"
  local partial="$KEYDION_FAILED_TREE_DIR/$label.tar.partial"
  local checksum="$KEYDION_FAILED_TREE_DIR/$label.sha256"
  local checksum_partial="$KEYDION_FAILED_TREE_DIR/$label.sha256.partial"
  local source_identity="$KEYDION_FAILED_TREE_DIR/$label.source-identity"
  local committed="$KEYDION_FAILED_TREE_DIR/$label.archive-complete"
  local removed="$KEYDION_FAILED_TREE_DIR/$label.source-delete-complete"
  local source_device_inode source_digest identity extra expected_marker path

  assert_failed_tree_unmounted() {
    local tree="$1" mount_targets mount_target
    if ! mount_targets="$(findmnt --raw --noheadings --output TARGET)"; then
      printf 'Could not read mount targets before archiving %s\n' "$tree" >&2
      return 1
    fi
    while IFS= read -r mount_target; do
      case "$mount_target" in
        "$tree"|"$tree"/*)
          printf '%s is or contains a mount and cannot be archived\n' \
            "$tree" >&2
          return 1
          ;;
      esac
    done <<< "$mount_targets"
  }

  case "$label" in
    venv) expected_source="$KEYDION_ROOT/.venv.failed-${KEYDION_BACKUP_ID}" ;;
    papers) expected_source="$KEYDION_ROOT/papers.failed-${KEYDION_BACKUP_ID}" ;;
    pending-papers)
      expected_source="$KEYDION_ROOT/data/pending_papers.failed-${KEYDION_BACKUP_ID}"
      ;;
    *) printf 'Unsupported failed-tree label: %s\n' "$label" >&2; return 1 ;;
  esac
  test "$source" = "$expected_source"
  test "$(realpath -m -- "$source")" = "$expected_source"

  for path in "$source" "$destination" "$partial" "$checksum" \
      "$checksum_partial" "$source_identity" "$committed" "$removed"; do
    if test -L "$path"; then
      printf 'Symlink in failed-tree archive state: %s\n' "$path" >&2
      return 1
    fi
  done
  assert_failed_tree_unmounted "$source"

  if test -f "$source_identity"; then
    identity="$(cat "$source_identity")"
    test "${identity//$'\n'/}" = "$identity"
    IFS=$'\t' read -r source_device_inode source_digest extra \
      <<< "$identity"
    [[ "$source_device_inode" =~ ^[0-9]+:[0-9]+$ ]]
    [[ "$source_digest" =~ ^[0-9a-f]{64}$ ]]
    test -z "$extra"
    publish_marker_once "$source_identity" \
      "$(printf '%s\t%s' "$source_device_inode" "$source_digest")"
  else
    test -d "$source"
    test ! -L "$source"
    assert_failed_tree_unmounted "$source"
    source_device_inode="$(stat -c '%d:%i' "$source")"
    source_digest="$(tar --sort=name --acls --xattrs --numeric-owner \
      -C "$(dirname "$source")" -cpf - "$(basename "$source")" \
      | sha256sum | awk '{print $1}')"
    [[ "$source_digest" =~ ^[0-9a-f]{64}$ ]]
    publish_marker_once "$source_identity" \
      "$(printf '%s\t%s' "$source_device_inode" "$source_digest")"
  fi
  expected_marker="$(printf '%s\t%s\t%s\t%s' "$KEYDION_BACKUP_ID" \
    "$label" "$source_device_inode" "$source_digest")"

  if test -e "$removed"; then
    test -f "$committed"
    publish_marker_once "$committed" "$expected_marker"
    publish_marker_once "$removed" "$expected_marker"
    test ! -e "$source"
    test -f "$destination"
    test -f "$checksum"
    test "$(sha256sum "$destination" | awk '{print $1}')" = "$source_digest"
    (cd "$KEYDION_FAILED_TREE_DIR"; sha256sum -c "$label.sha256")
    tar -tf "$destination" >/dev/null
    return 0
  fi

  if test -f "$committed"; then
    publish_marker_once "$committed" "$expected_marker"
    test -f "$destination"
    test -f "$checksum"
    test "$(sha256sum "$destination" | awk '{print $1}')" = "$source_digest"
    (cd "$KEYDION_FAILED_TREE_DIR"; sha256sum -c "$label.sha256")
  else
    test -d "$source"
    test ! -L "$source"
    test "$(stat -c '%d:%i' "$source")" = "$source_device_inode"
    assert_failed_tree_unmounted "$source"
    test "$(tar --sort=name --acls --xattrs --numeric-owner \
      -C "$(dirname "$source")" -cpf - "$(basename "$source")" \
      | sha256sum | awk '{print $1}')" = "$source_digest"
    rm -f -- "$destination" "$partial" "$checksum" "$checksum_partial"
    assert_failed_tree_unmounted "$source"
    tar --sort=name --acls --xattrs --numeric-owner \
      -C "$(dirname "$source")" -cpf "$partial" "$(basename "$source")"
    tar -tf "$partial" >/dev/null
    test "$(sha256sum "$partial" | awk '{print $1}')" = "$source_digest"
    printf '%s  %s\n' "$source_digest" "$label.tar" > "$checksum_partial"
    sync -f "$partial"
    sync -f "$checksum_partial"
    sync -f "$KEYDION_FAILED_TREE_DIR"
    mv -- "$partial" "$destination"
    mv -- "$checksum_partial" "$checksum"
    sync -f "$KEYDION_FAILED_TREE_DIR"
    (cd "$KEYDION_FAILED_TREE_DIR"; sha256sum -c "$label.sha256")
    publish_marker_once "$committed" "$expected_marker"
  fi

  if test -e "$source"; then
    test -d "$source"
    test ! -L "$source"
    test "$(stat -c '%d:%i' "$source")" = "$source_device_inode"
    assert_failed_tree_unmounted "$source"
    rm -rf --one-file-system -- "$source"
  fi
  sync -f "$(dirname "$source")"
  publish_marker_once "$removed" "$expected_marker"
}

archive_failed_tree_once venv \
  "$KEYDION_ROOT/.venv.failed-${KEYDION_BACKUP_ID}"
archive_failed_tree_once papers \
  "$KEYDION_ROOT/papers.failed-${KEYDION_BACKUP_ID}"
archive_failed_tree_once pending-papers \
  "$KEYDION_ROOT/data/pending_papers.failed-${KEYDION_BACKUP_ID}"
(
  cd "$KEYDION_FAILED_TREE_DIR"
  test ! -L TREE-SHA256
  test ! -L TREE-SHA256.partial
  sha256sum venv.tar papers.tar pending-papers.tar > TREE-SHA256.partial
  sync -f TREE-SHA256.partial
  mv -- TREE-SHA256.partial TREE-SHA256
  sync -f "$KEYDION_FAILED_TREE_DIR"
  sha256sum -c TREE-SHA256
)
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"
test ! -e "$KEYDION_ROOT/.venv.failed-${KEYDION_BACKUP_ID}"
test ! -e "$KEYDION_ROOT/papers.failed-${KEYDION_BACKUP_ID}"
test ! -e "$KEYDION_ROOT/data/pending_papers.failed-${KEYDION_BACKUP_ID}"
for KEYDION_ARCHIVE_LABEL in venv papers pending-papers; do
  test -f "$KEYDION_FAILED_TREE_DIR/$KEYDION_ARCHIVE_LABEL.source-identity"
  test -f "$KEYDION_FAILED_TREE_DIR/$KEYDION_ARCHIVE_LABEL.archive-complete"
  test -f \
    "$KEYDION_FAILED_TREE_DIR/$KEYDION_ARCHIVE_LABEL.source-delete-complete"
done
KEYDION_TREE_SHA256="$(sha256sum "$KEYDION_FAILED_TREE_DIR/TREE-SHA256" \
  | awk '{print $1}')"
KEYDION_ROLLBACK_ARCHIVES_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-archives "$KEYDION_OLD_RELEASE" \
  "$KEYDION_TREE_SHA256" "$KEYDION_MANIFEST_SHA")"
publish_marker_once "$KEYDION_ROLLBACK_ARCHIVES" \
  "$KEYDION_ROLLBACK_ARCHIVES_EXPECTED"
exit
```

Expected checkpoint: each deterministic archive is checksum-verified and
durably committed before its source tree is deleted. Its source identity binds
the original root's device, inode, and archive-stream digest, so a retry can
resume a partial one-filesystem deletion but cannot accept a replaced source.
The aggregate `TREE-SHA256` covers all three committed archives, their former
in-checkout paths are absent, and Git status is clean. A later migration must
still start in a new root Bash at Section 1 with a new backup ID. The final
`exit` releases the process-lifetime lock; confirm that archive shell has ended
before opening the rollback-window close entry.

### Close the rollback window before a later migration

Only after one verified release and after the incident owner has ended this
backup ID's rollback window may the trusted selector be removed. The forward
branch requires its exact success terminal and the live candidate boundary.
If rollback ever started, the rollback branch instead requires every exact
rollback terminal through activation and failed-tree archival, plus the live
old-release boundary. A historical forward-success terminal may coexist with a
later completed rollback; `rollback-started` makes the rollback chain
authoritative. This entry refuses all repository changes and never deletes
backup artifacts.

```bash
sudo /bin/bash
set -euo pipefail
test "$(id -u)" -eq 0
umask 077
KEYDION_RECOVERY_LOCK=/run/lock/keydion-paper-publishing.lock
test -d /run/lock
test ! -L /run/lock
test "$(stat -c '%U:%G' /run/lock)" = root:root
if ! test -e "$KEYDION_RECOVERY_LOCK"; then
  (set -o noclobber; : > "$KEYDION_RECOVERY_LOCK") 2>/dev/null \
    || test -f "$KEYDION_RECOVERY_LOCK"
fi
test -f "$KEYDION_RECOVERY_LOCK"
test ! -L "$KEYDION_RECOVERY_LOCK"
test "$(stat -c '%U:%G:%a' "$KEYDION_RECOVERY_LOCK")" = root:root:600
exec {KEYDION_RECOVERY_LOCK_FD}<>"$KEYDION_RECOVERY_LOCK"
flock --exclusive --nonblock "$KEYDION_RECOVERY_LOCK_FD"
export KEYDION_ROOT=/Keydion
load_active_boundary() {
  local selector=/srv/keydion-backups/paper-publishing-active
  local boundary selector_inode boundary_inode
  test -f "$selector"
  test ! -L "$selector"
  test "$(stat -c '%U:%G:%a' "$selector")" = root:root:600
  KEYDION_BACKUP_ID="$(cat "$selector")"
  [[ "$KEYDION_BACKUP_ID" =~ ^[0-9]{8}T[0-9]{6}Z$ ]]
  export KEYDION_BACKUP_ID
  export KEYDION_BACKUP_DIR="/srv/keydion-backups/${KEYDION_BACKUP_ID}"
  boundary="$KEYDION_BACKUP_DIR/active-boundary"
  test -f "$boundary"
  test ! -L "$boundary"
  test "$(stat -c '%U:%G:%a' "$boundary")" = root:root:600
  selector_inode="$(stat -c '%d:%i' "$selector")"
  boundary_inode="$(stat -c '%d:%i' "$boundary")"
  test "$selector_inode" = "$boundary_inode"
  printf '%s\n' "$KEYDION_BACKUP_ID" | cmp --silent - "$boundary"
  (
    cd "$KEYDION_BACKUP_DIR"
    sha256sum -c pre-snapshot.sha256
  )
}
load_active_boundary
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c SHA256SUMS
)
KEYDION_OLD_RELEASE="$(cat "$KEYDION_BACKUP_DIR/old-release.txt")"
KEYDION_NEW_RELEASE="$(cat "$KEYDION_BACKUP_DIR/new-release.txt")"
[[ "$KEYDION_OLD_RELEASE" =~ ^[0-9a-f]{40}$ ]]
[[ "$KEYDION_NEW_RELEASE" =~ ^[0-9a-f]{40}$ ]]
KEYDION_MANIFEST_SHA="$(sha256sum "$KEYDION_BACKUP_DIR/SHA256SUMS" \
  | awk '{print $1}')"
KEYDION_DB_IDENTITY_SHA="$(sha256sum \
  "$KEYDION_BACKUP_DIR/database-identity.txt" | awk '{print $1}')"
[[ "$KEYDION_MANIFEST_SHA" =~ ^[0-9a-f]{64}$ ]]
[[ "$KEYDION_DB_IDENTITY_SHA" =~ ^[0-9a-f]{64}$ ]]

KEYDION_FORWARD_SUCCESS="$KEYDION_BACKUP_DIR/forward-success.complete"
KEYDION_ROLLBACK_STARTED="$KEYDION_BACKUP_DIR/rollback-started"
KEYDION_ROLLBACK_RESTORED="$KEYDION_BACKUP_DIR/rollback-restored.complete"
KEYDION_ROLLBACK_ACTIVATED="$KEYDION_BACKUP_DIR/rollback-activated.complete"
KEYDION_ROLLBACK_ARCHIVES="$KEYDION_BACKUP_DIR/rollback-archives.complete"
KEYDION_FORWARD_SUCCESS_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" forward-success "$KEYDION_NEW_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"
KEYDION_ROLLBACK_STARTED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-started "$KEYDION_OLD_RELEASE" \
  "$KEYDION_NEW_RELEASE" "$KEYDION_MANIFEST_SHA")"
KEYDION_ROLLBACK_RESTORED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-restored "$KEYDION_OLD_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"
KEYDION_ROLLBACK_ACTIVATED_EXPECTED="$(printf '%s\t%s\t%s\t%s\t%s' \
  "$KEYDION_BACKUP_ID" rollback-activated "$KEYDION_OLD_RELEASE" \
  "$KEYDION_DB_IDENTITY_SHA" "$KEYDION_MANIFEST_SHA")"

assert_exact_terminal() {
  local path="$1" expected="$2"
  test -f "$path"
  test ! -L "$path"
  printf '%s\n' "$expected" | cmp --silent - "$path"
  sync -f "$path"
  sync -f "$(dirname "$path")"
}

export KEYDION_DATABASE=keydion
KEYDION_MYSQL_DEFAULTS=/root/.my.cnf
KEYDION_VENV_PATH="$KEYDION_ROOT/.venv"
test -f "$KEYDION_MYSQL_DEFAULTS"
test ! -L "$KEYDION_MYSQL_DEFAULTS"
test "$(stat -c '%U:%G:%a' "$KEYDION_MYSQL_DEFAULTS")" = root:root:600
test -f "$KEYDION_ROOT/.env.prod"
test ! -L "$KEYDION_ROOT/.env.prod"
test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
  = root:keydion:640
test -d "$KEYDION_VENV_PATH"
test ! -L "$KEYDION_VENV_PATH"
test "$(realpath "$KEYDION_VENV_PATH")" = "$KEYDION_VENV_PATH"
sudo -u keydion test -x "$KEYDION_VENV_PATH/bin/python"

read_dotenv_value() {
  local key="$1" required="${2:-1}" line value="" match_count=0
  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] || return 1
  while IFS= read -r line || test -n "$line"; do
    case "$line" in
      *\\) printf '%s\n' 'EnvironmentFile continuations are unsupported' >&2; return 1 ;;
    esac
    if [[ "$line" =~ ^[[:space:]]*${key}[[:space:]]*= ]] && \
       [[ "$line" != "$key="* ]]; then
      printf '%s must use exact KEY=value spelling\n' "$key" >&2
      return 1
    fi
    case "$line" in
      "$key="*) match_count=$((match_count + 1)); value="${line#*=}" ;;
    esac
  done < "$KEYDION_ROOT/.env.prod"
  if test "$match_count" -eq 0; then
    test "$required" -eq 0
    printf ''
    return 0
  fi
  test "$match_count" -eq 1
  [[ "$value" != *\\* ]]
  if test "${value:0:1}" = '"'; then
    test "${#value}" -ge 2
    test "${value: -1}" = '"'
    value="${value:1:${#value}-2}"
    [[ "$value" != *\"* ]]
  elif test "${value:0:1}" = "'"; then
    test "${#value}" -ge 2
    test "${value: -1}" = "'"
    value="${value:1:${#value}-2}"
    [[ "$value" != *\'* ]]
  else
    [[ ! "$value" =~ [[:space:]] ]]
    [[ "$value" != *\"* ]]
    [[ "$value" != *\'* ]]
  fi
  if test "$required" -eq 1; then test -n "$value"; fi
  printf '%s' "$value"
}
reload_validated_runtime_config() {
  test -f "$KEYDION_ROOT/.env.prod"
  test ! -L "$KEYDION_ROOT/.env.prod"
  test "$(realpath "$KEYDION_ROOT/.env.prod")" = "$KEYDION_ROOT/.env.prod"
  test "$(stat -c '%U:%G:%a' "$KEYDION_ROOT/.env.prod")" \
    = root:keydion:640
  PAPERQUERY_DATABASE_URL="$(read_dotenv_value PAPERQUERY_DATABASE_URL)"
  PAPERQUERY_DATA_DIR="$(read_dotenv_value PAPERQUERY_DATA_DIR 0)"
  PAPERQUERY_UPLOAD_DIR="$(read_dotenv_value PAPERQUERY_UPLOAD_DIR 0)"
  KEYDION_JOB_LEASE_SECONDS="$(read_dotenv_value \
    PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS 0)"
  KEYDION_JOB_LEASE_SECONDS="${KEYDION_JOB_LEASE_SECONDS:-1800}"
  [[ "$KEYDION_JOB_LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]]
  if ! test "$KEYDION_JOB_LEASE_SECONDS" -le 1800; then
    printf '%s\n' \
      'PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS must not exceed 1800 seconds' >&2
    return 1
  fi
  case "${PAPERQUERY_DATA_DIR:-}" in
    ""|/Keydion/data) ;;
    *) printf '%s\n' 'PAPERQUERY_DATA_DIR must be empty or /Keydion/data' >&2; return 1 ;;
  esac
  case "${PAPERQUERY_UPLOAD_DIR:-}" in
    ""|/Keydion/papers) ;;
    *) printf '%s\n' 'PAPERQUERY_UPLOAD_DIR must be empty or /Keydion/papers' >&2; return 1 ;;
  esac
  export PAPERQUERY_DATABASE_URL PAPERQUERY_DATA_DIR PAPERQUERY_UPLOAD_DIR
  KEYDION_DATA_DIR="$(realpath -m "${PAPERQUERY_DATA_DIR:-$KEYDION_ROOT/data}")"
  KEYDION_PAPERS_DIR="$(realpath -m \
    "${PAPERQUERY_UPLOAD_DIR:-$KEYDION_ROOT/papers}")"
  KEYDION_PENDING_DIR="$(realpath -m "$KEYDION_DATA_DIR/pending_papers")"
  test "$KEYDION_DATA_DIR" = /Keydion/data
  test "$KEYDION_PAPERS_DIR" = /Keydion/papers
  test "$KEYDION_PENDING_DIR" = /Keydion/data/pending_papers
}
reload_validated_runtime_config

assert_fresh_database_identity() {
  local application_identity admin_identity expected_identity
  application_identity="$(sudo -u keydion \
    --preserve-env=PAPERQUERY_DATABASE_URL,KEYDION_DATABASE \
    "$KEYDION_VENV_PATH/bin/python" - <<'PY'
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
url = make_url(os.environ["PAPERQUERY_DATABASE_URL"])
if url.get_backend_name() != "mysql" or url.database != os.environ["KEYDION_DATABASE"]:
    raise SystemExit("PAPERQUERY_DATABASE_URL must select keydion MySQL")
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
  admin_identity="$(mysql --defaults-extra-file="$KEYDION_MYSQL_DEFAULTS" \
    --batch --skip-column-names "$KEYDION_DATABASE" \
    -e 'SELECT @@GLOBAL.server_uuid, DATABASE()')"
  expected_identity="$(cat "$KEYDION_BACKUP_DIR/database-identity.txt")"
  test "$application_identity" = "$expected_identity"
  test "$admin_identity" = "$expected_identity"
}

assert_recorded_storage_provenance() {
  local papers_label expected_papers_source papers_extra
  local pending_label expected_pending_source pending_extra
  local papers_mount_target pending_mount_target
  local -a storage_source_lines=()
  test -f "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test ! -L "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  mapfile -t storage_source_lines \
    < "$KEYDION_BACKUP_DIR/storage-sources.tsv"
  test "${#storage_source_lines[@]}" -eq 2
  IFS=$'\t' read -r papers_label expected_papers_source papers_extra \
    <<< "${storage_source_lines[0]}"
  IFS=$'\t' read -r pending_label expected_pending_source pending_extra \
    <<< "${storage_source_lines[1]}"
  test "$papers_label" = papers
  test "$pending_label" = pending-papers
  test -z "$papers_extra"
  test -z "$pending_extra"
  test -d "$KEYDION_PAPERS_DIR"
  test ! -L "$KEYDION_PAPERS_DIR"
  test "$(realpath "$KEYDION_PAPERS_DIR")" = "$KEYDION_PAPERS_DIR"
  test -d "$KEYDION_PENDING_DIR"
  test ! -L "$KEYDION_PENDING_DIR"
  test "$(realpath "$KEYDION_PENDING_DIR")" = "$KEYDION_PENDING_DIR"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PAPERS_DIR")" = "$expected_papers_source"
  test "$(findmnt --noheadings --raw --output SOURCE \
    --target "$KEYDION_PENDING_DIR")" = "$expected_pending_source"
  papers_mount_target="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PAPERS_DIR")"
  pending_mount_target="$(findmnt --noheadings --raw --output TARGET \
    --target "$KEYDION_PENDING_DIR")"
  test "$(stat -c '%d' "$KEYDION_PAPERS_DIR")" \
    = "$(stat -c '%d' "$papers_mount_target")"
  test "$(stat -c '%d' "$KEYDION_PENDING_DIR")" \
    = "$(stat -c '%d' "$pending_mount_target")"
  sudo -u keydion test -r "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -w "$KEYDION_PAPERS_DIR"
  sudo -u keydion test -r "$KEYDION_PENDING_DIR"
  sudo -u keydion test -w "$KEYDION_PENDING_DIR"
}

read_unit_enabled_state() {
  local unit="$1" state status
  if state="$(systemctl is-enabled "$unit" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$state" in
    enabled) test "$status" -eq 0 ;;
    disabled) test "$status" -ne 0 ;;
    *) printf '%s has unsupported enabled state: %s\n' \
         "$unit" "$state" >&2; return 1 ;;
  esac
  printf '%s' "$state"
}

read_unit_active_state() {
  local unit="$1" state status
  if state="$(systemctl is-active "$unit" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$state" in
    active) test "$status" -eq 0 ;;
    inactive) test "$status" -ne 0 ;;
    *) printf '%s has unsupported active state: %s\n' \
         "$unit" "$state" >&2; return 1 ;;
  esac
  printf '%s' "$state"
}

assert_tracked_unit_fragment() {
  local unit="$1" expected="/etc/systemd/system/$1" fragment dropins
  fragment="$(systemctl show --property=FragmentPath --value "$unit")"
  dropins="$(systemctl show --property=DropInPaths --value "$unit")"
  test "$fragment" = "$expected"
  test -f "$expected"
  test ! -L "$expected"
  test -z "$dropins"
}

assert_recorded_unit_final() {
  local unit="$1" enabled_state="$2" active_state="$3" snapshot="$4"
  if test "$enabled_state:$active_state" = absent:absent; then
    test -f "${snapshot}.absent"
    test ! -L "${snapshot}.absent"
    test ! -e "$snapshot"
    test ! -L "$snapshot"
    ! systemctl cat "$unit" >/dev/null 2>&1
    return 0
  fi
  case "$enabled_state:$active_state" in
    enabled:active|enabled:inactive|disabled:active|disabled:inactive) ;;
    *) printf '%s has invalid recorded final state\n' "$unit" >&2; return 1 ;;
  esac
  test -f "$snapshot"
  test ! -L "$snapshot"
  assert_tracked_unit_fragment "$unit"
  cmp --silent "$snapshot" "/etc/systemd/system/$unit"
  test "$(read_unit_enabled_state "$unit")" = "$enabled_state"
  test "$(read_unit_active_state "$unit")" = "$active_state"
}

mapfile -t KEYDION_UNIT_STATE_LINES \
  < "$KEYDION_BACKUP_DIR/systemd/unit-state.tsv"
test "${#KEYDION_UNIT_STATE_LINES[@]}" -eq 2
IFS=$'\t' read -r KEYDION_WEB_UNIT_NAME KEYDION_WEB_ENABLED_STATE \
  KEYDION_WEB_ACTIVE_STATE KEYDION_WEB_STATE_EXTRA \
  <<< "${KEYDION_UNIT_STATE_LINES[0]}"
IFS=$'\t' read -r KEYDION_WORKER_UNIT_NAME KEYDION_WORKER_ENABLED_STATE \
  KEYDION_WORKER_ACTIVE_STATE KEYDION_WORKER_STATE_EXTRA \
  <<< "${KEYDION_UNIT_STATE_LINES[1]}"
test "$KEYDION_WEB_UNIT_NAME" = keydion.service
test "$KEYDION_WORKER_UNIT_NAME" = keydion-publishing-worker.service
test -z "$KEYDION_WEB_STATE_EXTRA"
test -z "$KEYDION_WORKER_STATE_EXTRA"

assert_rollback_restore_markers() {
  local label archive started complete archive_sha expected_marker
  for label in venv papers pending-papers; do
    case "$label" in
      venv)
        archive="$KEYDION_BACKUP_DIR/venv.tar"
        started="$KEYDION_BACKUP_DIR/venv-restore.started"
        complete="$KEYDION_BACKUP_DIR/venv-restore.complete"
        ;;
      papers)
        archive="$KEYDION_BACKUP_DIR/papers.tar"
        started="$KEYDION_BACKUP_DIR/papers-restore.started"
        complete="$KEYDION_BACKUP_DIR/papers-restore.complete"
        ;;
      pending-papers)
        archive="$KEYDION_BACKUP_DIR/pending-papers.tar"
        started="$KEYDION_BACKUP_DIR/pending-papers-restore.started"
        complete="$KEYDION_BACKUP_DIR/pending-papers-restore.complete"
        ;;
    esac
    test -f "$archive"
    test ! -L "$archive"
    archive_sha="$(sha256sum "$archive" | awk '{print $1}')"
    expected_marker="$(printf '%s\t%s\t%s' \
      "$KEYDION_BACKUP_ID" "$label" "$archive_sha")"
    assert_exact_terminal "$started" "$expected_marker"
    assert_exact_terminal "$complete" "$expected_marker"
  done
}

assert_rollback_archive_chain() {
  local failed_tree_dir="$KEYDION_BACKUP_DIR/failed-trees"
  local tree_manifest="$KEYDION_BACKUP_DIR/failed-trees/TREE-SHA256"
  local tree_sha expected_archives label source_identity committed removed
  local destination checksum identity source_device_inode source_digest extra
  local expected_marker source
  test -d "$failed_tree_dir"
  test ! -L "$failed_tree_dir"
  test -f "$tree_manifest"
  test ! -L "$tree_manifest"
  (
    cd "$failed_tree_dir"
    sha256sum -c TREE-SHA256
  )
  tree_sha="$(sha256sum "$tree_manifest" | awk '{print $1}')"
  expected_archives="$(printf '%s\t%s\t%s\t%s\t%s' \
    "$KEYDION_BACKUP_ID" rollback-archives "$KEYDION_OLD_RELEASE" \
    "$tree_sha" "$KEYDION_MANIFEST_SHA")"
  assert_exact_terminal "$KEYDION_ROLLBACK_ARCHIVES" "$expected_archives"

  for label in venv papers pending-papers; do
    destination="$failed_tree_dir/$label.tar"
    checksum="$failed_tree_dir/$label.sha256"
    source_identity="$failed_tree_dir/$label.source-identity"
    committed="$failed_tree_dir/$label.archive-complete"
    removed="$failed_tree_dir/$label.source-delete-complete"
    for KEYDION_ARCHIVE_EVIDENCE in "$destination" "$checksum" \
        "$source_identity" "$committed" "$removed"; do
      test -f "$KEYDION_ARCHIVE_EVIDENCE"
      test ! -L "$KEYDION_ARCHIVE_EVIDENCE"
    done
    identity="$(cat "$source_identity")"
    test "${identity//$'\n'/}" = "$identity"
    IFS=$'\t' read -r source_device_inode source_digest extra <<< "$identity"
    [[ "$source_device_inode" =~ ^[0-9]+:[0-9]+$ ]]
    [[ "$source_digest" =~ ^[0-9a-f]{64}$ ]]
    test -z "$extra"
    assert_exact_terminal "$source_identity" \
      "$(printf '%s\t%s' "$source_device_inode" "$source_digest")"
    test "$(sha256sum "$destination" | awk '{print $1}')" = "$source_digest"
    (cd "$failed_tree_dir"; sha256sum -c "$label.sha256")
    tar -tf "$destination" >/dev/null
    expected_marker="$(printf '%s\t%s\t%s\t%s' \
      "$KEYDION_BACKUP_ID" "$label" "$source_device_inode" \
      "$source_digest")"
    assert_exact_terminal "$committed" "$expected_marker"
    assert_exact_terminal "$removed" "$expected_marker"
    case "$label" in
      venv) source="$KEYDION_ROOT/.venv.failed-${KEYDION_BACKUP_ID}" ;;
      papers) source="$KEYDION_ROOT/papers.failed-${KEYDION_BACKUP_ID}" ;;
      pending-papers)
        source="$KEYDION_ROOT/data/pending_papers.failed-${KEYDION_BACKUP_ID}"
        ;;
    esac
    test ! -e "$source"
    test ! -L "$source"
  done
}

assert_forward_units() {
  assert_recorded_unit_final keydion-publishing-worker.service enabled active \
    "$KEYDION_ROOT/deploy/keydion-publishing-worker.service"
  assert_recorded_unit_final keydion.service enabled active \
    "$KEYDION_ROOT/deploy/keydion.service"
  test "$(sudo -u keydion --preserve-env=PAPERQUERY_DATABASE_URL \
    "$KEYDION_VENV_PATH/bin/python" -m alembic current | tail -n 1)" \
    = '0003_publishing_contract (head)'
}

assert_rollback_units() {
  assert_recorded_unit_final keydion-publishing-worker.service \
    "$KEYDION_WORKER_ENABLED_STATE" "$KEYDION_WORKER_ACTIVE_STATE" \
    "$KEYDION_BACKUP_DIR/systemd/keydion-publishing-worker.service"
  assert_recorded_unit_final keydion.service \
    "$KEYDION_WEB_ENABLED_STATE" "$KEYDION_WEB_ACTIVE_STATE" \
    "$KEYDION_BACKUP_DIR/systemd/keydion.service"
}

KEYDION_CURRENT_RELEASE="$(sudo -u keydion git -C \
  "$KEYDION_ROOT" rev-parse --verify HEAD)"
if test -e "$KEYDION_ROLLBACK_STARTED"; then
  KEYDION_TERMINAL_MODE=rollback
  assert_exact_terminal "$KEYDION_ROLLBACK_STARTED" \
    "$KEYDION_ROLLBACK_STARTED_EXPECTED"
  assert_exact_terminal "$KEYDION_ROLLBACK_RESTORED" \
    "$KEYDION_ROLLBACK_RESTORED_EXPECTED"
  assert_exact_terminal "$KEYDION_ROLLBACK_ACTIVATED" \
    "$KEYDION_ROLLBACK_ACTIVATED_EXPECTED"
  assert_rollback_restore_markers
  assert_rollback_archive_chain
  test "$KEYDION_CURRENT_RELEASE" = "$KEYDION_OLD_RELEASE"
  assert_rollback_units
else
  KEYDION_TERMINAL_MODE=forward
  test ! -L "$KEYDION_ROLLBACK_STARTED"
  for KEYDION_UNEXPECTED_ROLLBACK_TERMINAL in \
      "$KEYDION_ROLLBACK_RESTORED" "$KEYDION_ROLLBACK_ACTIVATED" \
      "$KEYDION_ROLLBACK_ARCHIVES"; do
    test ! -e "$KEYDION_UNEXPECTED_ROLLBACK_TERMINAL"
    test ! -L "$KEYDION_UNEXPECTED_ROLLBACK_TERMINAL"
  done
  assert_exact_terminal "$KEYDION_FORWARD_SUCCESS" \
    "$KEYDION_FORWARD_SUCCESS_EXPECTED"
  test "$KEYDION_CURRENT_RELEASE" = "$KEYDION_NEW_RELEASE"
  assert_forward_units
fi
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"
assert_recorded_storage_provenance
assert_fresh_database_identity
read -r -p "Type CLOSE_ROLLBACK_WINDOW after one verified release: " \
  KEYDION_CLOSE_APPROVAL
test "$KEYDION_CLOSE_APPROVAL" = CLOSE_ROLLBACK_WINDOW
reload_validated_runtime_config

# Approval is a human-duration pause. Re-bind the selector and repeat every
# mutable live-boundary proof immediately before removing it.
KEYDION_CLOSING_BACKUP_ID="$KEYDION_BACKUP_ID"
load_active_boundary
test "$KEYDION_BACKUP_ID" = "$KEYDION_CLOSING_BACKUP_ID"
(
  cd "$KEYDION_BACKUP_DIR"
  sha256sum -c SHA256SUMS
)
KEYDION_CURRENT_RELEASE="$(sudo -u keydion git -C \
  "$KEYDION_ROOT" rev-parse --verify HEAD)"
if test "$KEYDION_TERMINAL_MODE" = rollback; then
  assert_exact_terminal "$KEYDION_ROLLBACK_STARTED" \
    "$KEYDION_ROLLBACK_STARTED_EXPECTED"
  assert_exact_terminal "$KEYDION_ROLLBACK_RESTORED" \
    "$KEYDION_ROLLBACK_RESTORED_EXPECTED"
  assert_exact_terminal "$KEYDION_ROLLBACK_ACTIVATED" \
    "$KEYDION_ROLLBACK_ACTIVATED_EXPECTED"
  assert_rollback_restore_markers
  assert_rollback_archive_chain
  test "$KEYDION_CURRENT_RELEASE" = "$KEYDION_OLD_RELEASE"
  assert_rollback_units
else
  test "$KEYDION_TERMINAL_MODE" = forward
  test ! -e "$KEYDION_ROLLBACK_STARTED"
  test ! -L "$KEYDION_ROLLBACK_STARTED"
  assert_exact_terminal "$KEYDION_FORWARD_SUCCESS" \
    "$KEYDION_FORWARD_SUCCESS_EXPECTED"
  test "$KEYDION_CURRENT_RELEASE" = "$KEYDION_NEW_RELEASE"
  assert_forward_units
fi
sudo -u keydion git -C "$KEYDION_ROOT" diff --quiet --
sudo -u keydion git -C "$KEYDION_ROOT" diff --cached --quiet --
KEYDION_GIT_STATUS="$(sudo -u keydion git -C "$KEYDION_ROOT" \
  status --porcelain --untracked-files=all)"
test -z "$KEYDION_GIT_STATUS"
reload_validated_runtime_config
assert_recorded_storage_provenance
assert_fresh_database_identity
rm -f -- /srv/keydion-backups/paper-publishing-active
sync -f /srv/keydion-backups
exit
```

## Retention boundary

Keep legacy flat PDFs and legacy filename/vector columns for **one verified release** after migration. Their later cleanup is out of scope for this change
and requires its own backup, migration, compatibility, and rollback plan.
