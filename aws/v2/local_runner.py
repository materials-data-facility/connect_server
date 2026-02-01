import argparse
import json
import os
import sys
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR.parent))

from v2 import submit as submit_handler  # noqa: E402
from v2 import status as status_handler  # noqa: E402
from v2 import submissions as submissions_handler  # noqa: E402
from v2 import status_update as status_update_handler  # noqa: E402
from v2 import stream_create as stream_create_handler  # noqa: E402
from v2 import stream_append as stream_append_handler  # noqa: E402
from v2 import stream_status as stream_status_handler  # noqa: E402
from v2 import stream_close as stream_close_handler  # noqa: E402
from v2 import stream_snapshot as stream_snapshot_handler  # noqa: E402


def _build_event(body=None, query=None, path=None, user_id="local-user", user_email="local@example.com", name="Local User"):
    return {
        "body": json.dumps(body) if body is not None else None,
        "isBase64Encoded": False,
        "headers": {"authorization": "local"},
        "requestContext": {
            "authorizer": {
                "user_id": user_id,
                "user_email": user_email,
                "name": name,
                "identities": "[]",
                "group_info": "{}",
                "globus_dependent_token": "{}",
            }
        },
        "queryStringParameters": query,
        "pathParameters": path,
    }


def run_submit(args):
    body = json.loads(Path(args.body).read_text(encoding="utf-8")) if args.body else {}
    event = _build_event(body=body, user_id=args.user_id, user_email=args.user_email, name=args.name)
    return submit_handler.lambda_handler(event, None)


def run_status(args):
    query = {"version": args.version} if args.version else {}
    path = {"source_id": args.source_id}
    event = _build_event(query=query, path=path, user_id=args.user_id, user_email=args.user_email, name=args.name)
    return status_handler.lambda_handler(event, None)


def run_submissions(args):
    query = {}
    if args.organization:
        query["organization"] = args.organization
    event = _build_event(query=query, user_id=args.user_id, user_email=args.user_email, name=args.name)
    return submissions_handler.lambda_handler(event, None)


def run_update_status(args):
    body = {
        "source_id": args.source_id,
        "version": args.version,
        "status": args.status,
    }
    event = _build_event(body=body, user_id=args.user_id, user_email=args.user_email, name=args.name)
    return status_update_handler.lambda_handler(event, None)


def run_stream_create(args):
    body = {
        "title": args.title,
        "lab_id": args.lab_id,
        "organization": args.organization,
    }
    event = _build_event(body=body, user_id=args.user_id, user_email=args.user_email, name=args.name)
    return stream_create_handler.lambda_handler(event, None)


def run_stream_append(args):
    body = {"stream_id": args.stream_id}
    if args.files:
        payload = json.loads(Path(args.files).read_text(encoding="utf-8"))
        if isinstance(payload, dict) and "files" in payload:
            body["files"] = payload["files"]
        else:
            body["files"] = payload
    if args.file_count is not None:
        body["file_count"] = args.file_count
    if args.total_bytes is not None:
        body["total_bytes"] = args.total_bytes
    event = _build_event(body=body, user_id=args.user_id, user_email=args.user_email, name=args.name)
    event["pathParameters"] = {"stream_id": args.stream_id}
    return stream_append_handler.lambda_handler(event, None)


def run_stream_status(args):
    event = _build_event(query=None, path={"stream_id": args.stream_id}, user_id=args.user_id, user_email=args.user_email, name=args.name)
    return stream_status_handler.lambda_handler(event, None)


def run_stream_close(args):
    body = {"stream_id": args.stream_id}
    event = _build_event(body=body, user_id=args.user_id, user_email=args.user_email, name=args.name)
    event["pathParameters"] = {"stream_id": args.stream_id}
    return stream_close_handler.lambda_handler(event, None)


def run_stream_snapshot(args):
    body = {
        "stream_id": args.stream_id,
        "title": args.title,
        "update": args.update,
    }
    event = _build_event(body=body, user_id=args.user_id, user_email=args.user_email, name=args.name)
    event["pathParameters"] = {"stream_id": args.stream_id}
    return stream_snapshot_handler.lambda_handler(event, None)


def main():
    parser = argparse.ArgumentParser(description="Local runner for MDF v2 handlers")
    parser.add_argument(
        "command",
        choices=[
            "submit",
            "status",
            "submissions",
            "update-status",
            "stream-create",
            "stream-append",
        "stream-status",
        "stream-close",
        "stream-snapshot",
        ],
    )
    parser.add_argument("--body", help="Path to JSON submission payload")
    parser.add_argument("--source-id", help="Source ID for status")
    parser.add_argument("--version", help="Version for status")
    parser.add_argument("--status", help="Status for update-status")
    parser.add_argument("--organization", help="Organization filter")
    parser.add_argument("--title", help="Stream title")
    parser.add_argument("--lab-id", help="Lab ID for stream")
    parser.add_argument("--stream-id", help="Stream ID")
    parser.add_argument("--files", help="Path to JSON list of file entries")
    parser.add_argument("--file-count", type=int, help="Number of files to append")
    parser.add_argument("--total-bytes", type=int, help="Total bytes to append")
    parser.add_argument("--update", action="store_true", help="Update existing dataset on snapshot")
    parser.add_argument("--user-id", default="local-user")
    parser.add_argument("--user-email", default="local@example.com")
    parser.add_argument("--name", default="Local User")

    args = parser.parse_args()

    os.environ.setdefault("STORE_BACKEND", "sqlite")
    os.environ.setdefault("SQLITE_PATH", "/tmp/mdf_connect_v2.db")
    os.environ.setdefault("USE_MOCK_FLOW", "true")

    if args.command == "submit":
        response = run_submit(args)
    elif args.command == "status":
        response = run_status(args)
    elif args.command == "update-status":
        response = run_update_status(args)
    elif args.command == "stream-create":
        response = run_stream_create(args)
    elif args.command == "stream-append":
        response = run_stream_append(args)
    elif args.command == "stream-status":
        response = run_stream_status(args)
    elif args.command == "stream-close":
        response = run_stream_close(args)
    elif args.command == "stream-snapshot":
        response = run_stream_snapshot(args)
    else:
        response = run_submissions(args)

    print(json.dumps(response, indent=2))


if __name__ == "__main__":
    main()
