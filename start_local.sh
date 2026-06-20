#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

export PAPERQUERY_SECRET="${PAPERQUERY_SECRET:-dev-secret-key}"
# Local dev opts in to the insecure default secret; prod must set a strong one (SEC-09).
export PAPERQUERY_ALLOW_DEV_SECRET="${PAPERQUERY_ALLOW_DEV_SECRET:-1}"
# Cookies are not Secure over the plain-HTTP dev server (SEC-07/08).
export PAPERQUERY_COOKIE_SECURE="${PAPERQUERY_COOKIE_SECURE:-0}"
export FLASK_APP="app"

# 激活虚拟环境（默认 .venv，可通过 VENV_PATH 覆盖）
VENV_PATH="${VENV_PATH:-.venv}"
if [[ -d "${VENV_PATH}/bin" ]]; then
  # shellcheck source=/dev/null
  source "${VENV_PATH}/bin/activate"
elif [[ -n "${VENV_PATH}" && "${VENV_PATH}" != ".venv" ]]; then
  echo "Warning: virtual environment not found at ${VENV_PATH}" >&2
fi

if [[ -n "${PAPERQUERY_DATA_DIR:-}" ]]; then
  export PAPERQUERY_DATA_DIR
fi

if [[ -n "${PAPERQUERY_UPLOAD_DIR:-}" ]]; then
  export PAPERQUERY_UPLOAD_DIR
fi

echo "Launching Keydion..."
echo "  SECRET:      ${PAPERQUERY_SECRET}"
[[ -n "${PAPERQUERY_DATA_DIR:-}" ]] && echo "  DATA DIR:    ${PAPERQUERY_DATA_DIR}"
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

# exec "${PY_BIN}" -m flask --app app run --debug
exec "${PY_BIN}" -m flask --app app run --debug --host="${HOST:-127.0.0.1}" --port="${PORT:-4000}"
