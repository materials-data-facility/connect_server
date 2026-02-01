import json
from typing import Any, Dict

from v2.store import get_store
from v2.responses import bad_request, ok


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    if not record:
        return {}
    if "dataset_mdata" in record and isinstance(record["dataset_mdata"], str):
        try:
            record["dataset_mdata"] = json.loads(record["dataset_mdata"])
        except Exception:
            pass
    return record


def lambda_handler(event, context):
    source_id = None
    version = None

    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    source_id = path_params.get("source_id") or query_params.get("source_id")
    version = query_params.get("version")

    if not source_id:
        return bad_request("Missing source_id")

    store = get_store()

    if version:
        record = store.get_submission(source_id, version)
        if not record:
            return ok({"success": False, "error": "Submission not found"})
        return ok({"success": True, "submission": _normalize_record(record)})

    versions = store.list_versions(source_id)
    if not versions:
        return ok({"success": False, "error": "Submission not found"})

    def sort_key(item):
        version_value = item.get("version", "")
        parts = []
        for part in version_value.split("."):
            if part.isdigit():
                parts.append(int(part))
            else:
                parts.append(part)
        return parts

    latest = sorted(versions, key=sort_key)[-1]
    return ok({"success": True, "submission": _normalize_record(latest)})
