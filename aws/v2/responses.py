import json


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def ok(body):
    return response(200, body)


def bad_request(message):
    return response(400, {"success": False, "error": message})


def forbidden(message):
    return response(403, {"success": False, "error": message})


def not_found(message):
    return response(404, {"success": False, "error": message})


def server_error(message):
    return response(500, {"success": False, "error": message})
