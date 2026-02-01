from v2.responses import bad_request, ok
from v2.stream_store import get_stream_store


def lambda_handler(event, context):
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    stream_id = path_params.get("stream_id") or query_params.get("stream_id")
    if not stream_id:
        return bad_request("stream_id is required")

    store = get_stream_store()
    record = store.get_stream(stream_id)
    if not record:
        return ok({"success": False, "error": "Stream not found"})

    return ok({"success": True, "stream": record})
