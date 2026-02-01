import json

from v2.store import get_store, parse_pagination_key, serialize_pagination_key
from v2.request import parse_authorizer
from v2.responses import bad_request, ok


def lambda_handler(event, context):
    auth = parse_authorizer(event)
    user_id = auth.get("user_id")

    if not user_id:
        return bad_request("Missing user identity")

    query_params = event.get("queryStringParameters") or {}
    organization = query_params.get("organization")
    limit_param = query_params.get("limit")
    start_key_param = query_params.get("start_key")

    try:
        limit = int(limit_param) if limit_param else 50
    except Exception:
        limit = 50

    start_key = parse_pagination_key(start_key_param)

    store = get_store()

    if organization:
        items, last_key = store.list_by_org(organization, limit=limit, start_key=start_key)
    else:
        items, last_key = store.list_by_user(user_id, limit=limit, start_key=start_key)

    for item in items:
        if isinstance(item.get("dataset_mdata"), str):
            try:
                item["dataset_mdata"] = json.loads(item["dataset_mdata"])
            except Exception:
                pass

    return ok({
        "success": True,
        "submissions": items,
        "next_key": serialize_pagination_key(last_key),
    })
