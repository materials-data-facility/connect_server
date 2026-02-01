import json
from typing import Any, Dict

from v2.request import parse_json_body
from v2.responses import bad_request, ok
from v2.store import get_store


ALLOWED_STATUSES = {
    "submitted",
    "transferring",
    "processing",
    "indexing",
    "complete",
    "failed",
}


def lambda_handler(event, context):
    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    if not payload:
        return bad_request("Missing request payload")

    source_id = payload.get("source_id")
    version = payload.get("version")
    status = payload.get("status")

    if not source_id or not version or not status:
        return bad_request("source_id, version, and status are required")
    if status not in ALLOWED_STATUSES:
        return bad_request("status must be one of: {}".format(", ".join(sorted(ALLOWED_STATUSES)))
        )

    store = get_store()
    store.update_status(source_id, version, status)

    return ok({"success": True, "source_id": source_id, "version": version, "status": status})
