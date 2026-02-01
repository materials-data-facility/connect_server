#!/usr/bin/env bash
set -euo pipefail

# Local MDF v2 server startup
# Usage: STORE_BACKEND=sqlite SQLITE_PATH=/tmp/mdf_connect_v2.db ./local_start.sh

export STORE_BACKEND=${STORE_BACKEND:-sqlite}
export SQLITE_PATH=${SQLITE_PATH:-/tmp/mdf_connect_v2.db}
export TINYDB_PATH=${TINYDB_PATH:-/tmp/mdf_connect_v2.json}
export USE_MOCK_FLOW=${USE_MOCK_FLOW:-true}
export LOCAL_HOST=${LOCAL_HOST:-127.0.0.1}
export LOCAL_PORT=${LOCAL_PORT:-8080}
export START_FLOW_SIM=${START_FLOW_SIM:-false}
export FORCE_RESTART=${FORCE_RESTART:-false}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${SCRIPT_DIR}/.local_server.pid"
FLOW_PID_FILE="${SCRIPT_DIR}/.flow_sim.pid"

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

python "${SCRIPT_DIR}/local_server.py" &
PID=$!

printf "%s" "${PID}" > "${PID_FILE}"

FLOW_PID=""
if [[ "${START_FLOW_SIM}" == "true" || "${START_FLOW_SIM}" == "1" ]]; then
  python "${SCRIPT_DIR}/flow_simulator.py" &
  FLOW_PID=$!
  printf "%s" "${FLOW_PID}" > "${FLOW_PID_FILE}"
fi

cat <<MSG
Local MDF v2 server running:
  http://${LOCAL_HOST}:${LOCAL_PORT}

Config:
  STORE_BACKEND=${STORE_BACKEND}
  SQLITE_PATH=${SQLITE_PATH}
  TINYDB_PATH=${TINYDB_PATH}
  USE_MOCK_FLOW=${USE_MOCK_FLOW}
  START_FLOW_SIM=${START_FLOW_SIM}
  FORCE_RESTART=${FORCE_RESTART}

Stop with:
  kill ${PID} && rm -f ${PID_FILE}
  ${FLOW_PID:+kill ${FLOW_PID} && rm -f ${FLOW_PID_FILE}}
MSG
