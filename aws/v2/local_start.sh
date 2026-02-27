#!/usr/bin/env bash
set -euo pipefail

# Local MDF v2 FastAPI server startup.
# Usage: STORE_BACKEND=sqlite SQLITE_PATH=/tmp/mdf_connect_v2.db ./local_start.sh

export STORE_BACKEND=${STORE_BACKEND:-sqlite}
export SQLITE_PATH=${SQLITE_PATH:-/tmp/mdf_connect_v2.db}
export STORAGE_BACKEND=${STORAGE_BACKEND:-local}
export ASYNC_DISPATCH_MODE=${ASYNC_DISPATCH_MODE:-inline}
export AUTH_MODE=${AUTH_MODE:-dev}
export ALLOW_ALL_CURATORS=${ALLOW_ALL_CURATORS:-true}
export CURATOR_GROUP_IDS=${CURATOR_GROUP_IDS:-}
export REQUIRED_GROUP_MEMBERSHIP=${REQUIRED_GROUP_MEMBERSHIP:-}
export USE_MOCK_DATACITE=${USE_MOCK_DATACITE:-true}
export LOCAL_HOST=${LOCAL_HOST:-127.0.0.1}
export LOCAL_PORT=${LOCAL_PORT:-8080}
export FORCE_RESTART=${FORCE_RESTART:-false}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PID_FILE="${SCRIPT_DIR}/.local_server.pid"
LOG_FILE="${SCRIPT_DIR}/.local_server.log"

if [[ -f "${PID_FILE}" ]]; then
  PID=$(cat "${PID_FILE}")
  if ps -p "${PID}" > /dev/null 2>&1; then
    if [[ "${FORCE_RESTART}" == "true" || "${FORCE_RESTART}" == "1" ]]; then
      echo "Restarting local server (pid ${PID})..."
      kill "${PID}" || true
      rm -f "${PID_FILE}"
    else
      echo "Local server already running (pid ${PID})."
      echo "Stop it with: kill ${PID} && rm -f ${PID_FILE}"
      echo "Or restart with: FORCE_RESTART=true ./local_start.sh"
      exit 0
    fi
  fi
fi

(
  cd "${ROOT_DIR}"
  python3 -m v2.app.main
) >"${LOG_FILE}" 2>&1 &
PID=$!
sleep 1
if ! kill -0 "${PID}" >/dev/null 2>&1; then
  echo "Failed to start local server. Last log lines:"
  tail -n 40 "${LOG_FILE}" || true
  exit 1
fi

printf "%s" "${PID}" > "${PID_FILE}"

cat <<MSG
Local MDF v2 server running:
  http://${LOCAL_HOST}:${LOCAL_PORT}

Config:
  STORE_BACKEND=${STORE_BACKEND}
  SQLITE_PATH=${SQLITE_PATH}
  STORAGE_BACKEND=${STORAGE_BACKEND}
  ASYNC_DISPATCH_MODE=${ASYNC_DISPATCH_MODE}
  AUTH_MODE=${AUTH_MODE}
  ALLOW_ALL_CURATORS=${ALLOW_ALL_CURATORS}
  USE_MOCK_DATACITE=${USE_MOCK_DATACITE}
  FORCE_RESTART=${FORCE_RESTART}
  LOG_FILE=${LOG_FILE}

Stop with:
  kill ${PID} && rm -f ${PID_FILE}
MSG
