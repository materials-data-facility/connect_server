import uuid
from datetime import datetime

from v2.request import parse_authorizer, parse_json_body
from v2.responses import bad_request, ok
from v2.stream_store import get_stream_store


def lambda_handler(event, context):
    auth = parse_authorizer(event)
    user_id = auth.get("user_id")

    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    if payload is None:
        payload = {}

    title = payload.get("title")
    lab_id = payload.get("lab_id")
    organization = payload.get("organization")
    metadata = payload.get("metadata")

    if not title:
        return bad_request("title is required")

    now = datetime.utcnow().isoformat("T") + "Z"
    stream_id = payload.get("stream_id") or f"stream-{uuid.uuid4().hex}"

    record = {
        "stream_id": stream_id,
        "lab_id": lab_id,
        "title": title,
        "status": "open",
        "file_count": 0,
        "total_bytes": 0,
        "last_append_at": None,
        "created_at": now,
        "updated_at": now,
        "user_id": user_id,
        "organization": organization,
        "metadata": metadata,
    }

    store = get_stream_store()
    store.create_stream(record)

    return ok({"success": True, "stream_id": stream_id, "stream": record})
