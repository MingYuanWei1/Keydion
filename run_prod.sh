#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load production env (PAPERQUERY_SECRET, DATABASE_URL, MS_*, GUNICORN_* …)
if [[ ! -f .env.prod ]]; then
  echo "Error: .env.prod not found. Copy it from .env.prod template and fill in values." >&2
  exit 1
fi
set -a
# shellcheck source=/dev/null
source .env.prod
set +a

# Activate venv (override with VENV_PATH=… ./run_prod.sh)
VENV_PATH="${VENV_PATH:-.venv}"
if [[ -d "${VENV_PATH}/bin" ]]; then
  # shellcheck source=/dev/null
  source "${VENV_PATH}/bin/activate"
elif [[ -n "${VENV_PATH}" && "${VENV_PATH}" != ".venv" ]]; then
  echo "Warning: virtual environment not found at ${VENV_PATH}" >&2
fi

# Default bind: TCP on 0.0.0.0:4000 — assumes nginx (or another reverse proxy)
# sits in front. For a Unix-socket setup set GUNICORN_BIND in .env.prod, e.g.
#   GUNICORN_BIND=unix:/var/run/keydion/keydion.sock
export GUNICORN_BIND="${GUNICORN_BIND:-0.0.0.0:5000}"

# Safety net: refuse to boot with the dev secret in production.
if [[ "${PAPERQUERY_SECRET:-}" == "dev-secret-key" || -z "${PAPERQUERY_SECRET:-}" ]]; then
  echo "Error: PAPERQUERY_SECRET is unset or still set to the dev default." >&2
  echo "       Generate one with: python -c 'import secrets; print(secrets.token_urlsafe(64))'" >&2
  exit 1
fi

echo "Launching Keydion (prod)..."
echo "  BIND:        ${GUNICORN_BIND}"
echo "  WORKERS:     ${GUNICORN_WORKERS:-auto}"
echo "  TIMEOUT:     ${GUNICORN_TIMEOUT:-60}s"
[[ -n "${PAPERQUERY_DATA_DIR:-}" ]]   && echo "  DATA DIR:    ${PAPERQUERY_DATA_DIR}"
[[ -n "${PAPERQUERY_UPLOAD_DIR:-}" ]] && echo "  UPLOAD DIR:  ${PAPERQUERY_UPLOAD_DIR}"

PY_BIN="${PYTHON_BIN:-}"
if [[ -z "${PY_BIN}" ]]; then
  if command -v python >/dev/null 2>&1; then
    PY_BIN="python"
  elif command -v python3 >/dev/null 2>&1; then
    PY_BIN="python3"
  else
    echo "Error: Python interpreter not found. Please install Python or set PYTHON_BIN." >&2
    exit 1
  fi
fi

exec "${PY_BIN}" -m gunicorn -c gunicorn.conf.py 'app:create_app()'
