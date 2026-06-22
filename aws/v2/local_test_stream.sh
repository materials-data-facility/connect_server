#!/usr/bin/env bash
set -euo pipefail

HOST=${LOCAL_HOST:-127.0.0.1}
PORT=${LOCAL_PORT:-8080}
BASE_URL="http://${HOST}:${PORT}"

TMP_FILES="/tmp/mdf_stream_files.json"
cat <<'JSON' > "${TMP_FILES}"
{
  "files": [
    {"path": "file1.csv", "size": 1234},
    {"path": "file2.csv", "size": 5678}
  ]
}
JSON

echo "Creating stream..."
CREATE_RES=$(curl -s -X POST "${BASE_URL}/stream/create" \
  -H "Content-Type: application/json" \
  -d '{"title":"Test Stream","lab_id":"lab-1"}')

echo "Response:"
echo "${CREATE_RES}"

STREAM_ID=$(python - <<'PY'
import json,sys
res=json.loads(sys.stdin.read())
print(res["stream_id"])
PY
<<< "${CREATE_RES}")

if [[ -z "${STREAM_ID}" ]]; then
  echo "Failed to parse stream_id" >&2
  exit 1
fi

echo "Stream ID: ${STREAM_ID}"

echo "Appending files..."
curl -s -X POST "${BASE_URL}/stream/${STREAM_ID}/append" \
  -H "Content-Type: application/json" \
  -d @"${TMP_FILES}"

echo "\nFetching stream status..."
curl -s "${BASE_URL}/stream/${STREAM_ID}"

echo "\nClosing stream..."
curl -s -X POST "${BASE_URL}/stream/${STREAM_ID}/close" \
  -H "Content-Type: application/json" \
  -d '{}'

echo "\nSnapshotting stream..."
curl -s -X POST "${BASE_URL}/stream/${STREAM_ID}/snapshot" \
  -H "Content-Type: application/json" \
  -d "{\"stream_id\":\"${STREAM_ID}\"}"

echo "\nDone."
