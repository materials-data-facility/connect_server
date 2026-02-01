import ast
import base64
import json


def parse_json_body(event):
    body = event.get("body")
    if body is None:
        return None, "Missing request body"

    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode("utf-8")
        except Exception:
            return None, "Unable to decode base64 body"

    try:
        return json.loads(body), None
    except Exception:
        return None, "Submission must be valid JSON"


def parse_authorizer(event):
    context = event.get("requestContext", {}).get("authorizer", {})
    user_id = context.get("user_id") or context.get("sub")
    name = context.get("name") or context.get("principalId")
    user_email = context.get("user_email") or context.get("email")

    identities = _maybe_literal_eval(context.get("identities"))
    group_info = _maybe_literal_eval(context.get("group_info"))
    dependent_token = _maybe_literal_eval(context.get("globus_dependent_token"))

    return {
        "user_id": user_id,
        "name": name,
        "user_email": user_email,
        "identities": identities,
        "group_info": group_info,
        "dependent_token": dependent_token,
    }


def _maybe_literal_eval(value):
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:
            return value
    return value
