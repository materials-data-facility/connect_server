#!/usr/bin/env bash
set -euo pipefail

# Local MDF v2 test script (expects server running)
# Usage: ./local_test.sh

HOST=${LOCAL_HOST:-127.0.0.1}
PORT=${LOCAL_PORT:-8080}
BASE_URL="http://${HOST}:${PORT}"

TMP_PAYLOAD="/tmp/mdf_payload.json"

cat <<'JSON' > "${TMP_PAYLOAD}"
{
  "dc": {
    "titles": [{"title": "Local Test Dataset"}],
    "creators": [{"creatorName": "Doe, Jane"}]
  },
  "data_sources": ["globus://example/collection"],
  "test": true
}
JSON

echo "Submitting test dataset..."
SUBMIT_RES=$(curl -s -X POST "${BASE_URL}/submit" \
  -H "Content-Type: application/json" \
  -d @"${TMP_PAYLOAD}")

echo "Response:"
echo "${SUBMIT_RES}"

SOURCE_ID=$(python - <<'PY'
import json,sys
res=json.loads(sys.stdin.read())
print(res["source_id"])
PY
<<< "${SUBMIT_RES}")

if [[ -z "${SOURCE_ID}" ]]; then
  echo "Failed to parse source_id from submit response" >&2
  exit 1
fi

echo "Source ID: ${SOURCE_ID}"

echo "\nFetching status..."
curl -s "${BASE_URL}/status/${SOURCE_ID}"

echo "\nUpdating status to processing..."
curl -s -X POST "${BASE_URL}/status/update" \
  -H "Content-Type: application/json" \
  -d "{\"source_id\":\"${SOURCE_ID}\",\"version\":\"1.0\",\"status\":\"processing\"}"

echo "\nFetching status again..."
curl -s "${BASE_URL}/status/${SOURCE_ID}"

echo "\nListing submissions..."
curl -s "${BASE_URL}/submissions"

echo "\nDone."
