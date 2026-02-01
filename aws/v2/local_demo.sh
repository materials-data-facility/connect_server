#!/usr/bin/env bash
set -euo pipefail

# Combined local demo: dataset + stream flow using mdf backend commands

API_URL=${MDF_API_URL:-http://127.0.0.1:8080}

SUBMIT_TMP=$(mktemp)
STREAM_TMP=$(mktemp)
trap 'rm -f "${SUBMIT_TMP}" "${STREAM_TMP}"' EXIT

TMP_PAYLOAD="/tmp/mdf_payload.json"
TMP_FILES="/tmp/mdf_stream_files.json"

cat <<'JSON' > "${TMP_PAYLOAD}"
{
  "dc": {
    "titles": [{"title": "Local Demo Dataset"}],
    "creators": [{"creatorName": "Doe, Jane"}]
  },
  "data_sources": ["globus://example/collection"],
  "test": true
}
JSON

cat <<'JSON' > "${TMP_FILES}"
{
  "files": [
    {"path": "file1.csv", "size": 1234},
    {"path": "file2.csv", "size": 5678}
  ]
}
JSON

echo "Submitting dataset via mdf backend..."
mdf backend submit --payload "${TMP_PAYLOAD}" --api-url "${API_URL}" | tee "${SUBMIT_TMP}"

SOURCE_ID=$(python - "$SUBMIT_TMP" <<'PY'
import json,sys
path=sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)
print(data.get("source_id", ""))
PY
)

if [[ -z "${SOURCE_ID}" ]]; then
  echo "Failed to parse source_id" >&2
  exit 1
fi

echo "Checking status..."
mdf backend status --source-id "${SOURCE_ID}" --api-url "${API_URL}"

echo "Updating status to processing..."
mdf backend update-status --source-id "${SOURCE_ID}" --version "1.0" --status "processing" --api-url "${API_URL}"

echo "Checking status again..."
mdf backend status --source-id "${SOURCE_ID}" --api-url "${API_URL}"

echo "\nCreating stream..."
mdf backend stream-create --title "Demo Stream" --lab-id "lab-1" --api-url "${API_URL}" | tee "${STREAM_TMP}"

STREAM_ID=$(python - "$STREAM_TMP" <<'PY'
import json,sys
path=sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)
stream = data.get("stream", {}) or {}
print(stream.get("stream_id", ""))
PY
)

if [[ -z "${STREAM_ID}" ]]; then
  echo "Failed to parse stream_id" >&2
  exit 1
fi

echo "Appending files to stream..."
mdf backend stream-append --stream-id "${STREAM_ID}" --files "${TMP_FILES}" --api-url "${API_URL}"

echo "Stream status..."
mdf backend stream-status --stream-id "${STREAM_ID}" --api-url "${API_URL}"

echo "Closing stream..."
mdf backend stream-close --stream-id "${STREAM_ID}" --api-url "${API_URL}"

echo "Snapshotting stream into dataset..."
mdf backend stream-snapshot --stream-id "${STREAM_ID}" --api-url "${API_URL}"

echo "Done."
