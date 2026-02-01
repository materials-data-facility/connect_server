from datetime import datetime

from v2.request import parse_json_body
from v2.responses import bad_request, ok
from v2.stream_store import get_stream_store


def _count_files(payload):
    files = payload.get("files")
    if isinstance(files, list) and files:
        total_bytes = 0
        for entry in files:
            try:
                total_bytes += int(entry.get("size", 0))
            except Exception:
                continue
        return len(files), total_bytes, files[-1]

    file_count = payload.get("file_count")
    total_bytes = payload.get("total_bytes")
    try:
        file_count = int(file_count) if file_count is not None else 0
    except Exception:
        file_count = 0
    try:
        total_bytes = int(total_bytes) if total_bytes is not None else 0
    except Exception:
        total_bytes = 0
    return file_count, total_bytes, payload.get("last_file")


def lambda_handler(event, context):
    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    if not payload:
        return bad_request("Missing request payload")
    if isinstance(payload, list):
        payload = {"files": payload}

    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id") or payload.get("stream_id")
    if not stream_id:
        return bad_request("stream_id is required")

    file_count, total_bytes, last_file = _count_files(payload)
    if file_count <= 0 and total_bytes <= 0:
        return bad_request("Provide files list or file_count/total_bytes")

    store = get_stream_store()
    updated = store.append_stream(stream_id, file_count, total_bytes, last_file=last_file)
    if not updated:
        return bad_request("Stream not found")

    return ok({"success": True, "stream": updated})
