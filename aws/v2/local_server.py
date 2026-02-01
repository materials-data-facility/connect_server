import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR.parent))

from v2 import status as status_handler
from v2 import status_update as status_update_handler
from v2 import submissions as submissions_handler
from v2 import submit as submit_handler
from v2 import stream_create as stream_create_handler
from v2 import stream_append as stream_append_handler
from v2 import stream_close as stream_close_handler
from v2 import stream_snapshot as stream_snapshot_handler
from v2 import stream_status as stream_status_handler
from v2 import stream_upload as stream_upload_handler
from v2 import search as search_handler
from v2 import dataset_card as card_handler
from v2 import citation as citation_handler
from v2 import preview as preview_handler
from v2 import curation as curation_handler


def _read_body(handler):
    length = int(handler.headers.get("Content-Length", "0"))
    if length <= 0:
        return None
    return handler.rfile.read(length).decode("utf-8")


def _authorizer_context(handler):
    user_id = handler.headers.get("X-User-Id") or os.environ.get("LOCAL_USER_ID", "local-user")
    user_email = handler.headers.get("X-User-Email") or os.environ.get("LOCAL_USER_EMAIL", "local@example.com")
    name = handler.headers.get("X-User-Name") or os.environ.get("LOCAL_USER_NAME", "Local User")
    return {
        "user_id": user_id,
        "user_email": user_email,
        "name": name,
        "identities": "[]",
        "group_info": "{}",
        "globus_dependent_token": "{}",
    }


def _make_event(handler, body=None, query=None, path_params=None):
    return {
        "body": body,
        "isBase64Encoded": False,
        "headers": {k.lower(): v for k, v in handler.headers.items()},
        "requestContext": {"authorizer": _authorizer_context(handler)},
        "queryStringParameters": query,
        "pathParameters": path_params,
    }


class LocalHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        if os.environ.get("LOCAL_SERVER_SILENT", "").lower() in {"1", "true", "yes"}:
            return
        super().log_message(format, *args)

    def _send_response(self, payload):
        self.send_response(payload.get("statusCode", 200))
        for header, value in payload.get("headers", {}).items():
            self.send_header(header, value)
        self.end_headers()
        self.wfile.write(payload.get("body", "").encode("utf-8"))

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            body = _read_body(self)
            event = _make_event(self, body=body, query=None, path_params=None)

            if parsed.path == "/submit":
                response = submit_handler.lambda_handler(event, None)
            elif parsed.path == "/status/update":
                response = status_update_handler.lambda_handler(event, None)
            elif parsed.path == "/stream/create":
                response = stream_create_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/stream/") and parsed.path.endswith("/append"):
                stream_id = parsed.path.split("/stream/")[-1].split("/append")[0]
                event["pathParameters"] = {"stream_id": stream_id}
                response = stream_append_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/stream/") and parsed.path.endswith("/close"):
                stream_id = parsed.path.split("/stream/")[-1].split("/close")[0]
                event["pathParameters"] = {"stream_id": stream_id}
                response = stream_close_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/stream/") and parsed.path.endswith("/snapshot"):
                stream_id = parsed.path.split("/stream/")[-1].split("/snapshot")[0]
                event["pathParameters"] = {"stream_id": stream_id}
                response = stream_snapshot_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/stream/") and parsed.path.endswith("/upload"):
                stream_id = parsed.path.split("/stream/")[-1].split("/upload")[0]
                event["pathParameters"] = {"stream_id": stream_id}
                response = stream_upload_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/stream/") and parsed.path.endswith("/upload-url"):
                stream_id = parsed.path.split("/stream/")[-1].split("/upload-url")[0]
                event["pathParameters"] = {"stream_id": stream_id}
                response = stream_upload_handler.upload_url_handler(event, None)
            elif parsed.path.startswith("/stream/") and parsed.path.endswith("/upload-confirm"):
                stream_id = parsed.path.split("/stream/")[-1].split("/upload-confirm")[0]
                event["pathParameters"] = {"stream_id": stream_id}
                response = stream_upload_handler.confirm_upload_handler(event, None)
            elif parsed.path.startswith("/stream/") and parsed.path.endswith("/download-url"):
                stream_id = parsed.path.split("/stream/")[-1].split("/download-url")[0]
                event["pathParameters"] = {"stream_id": stream_id}
                response = stream_upload_handler.download_url_handler(event, None)
            elif parsed.path.startswith("/curation/") and parsed.path.endswith("/approve"):
                source_id = parsed.path.split("/curation/")[-1].split("/approve")[0]
                event["pathParameters"] = {"source_id": source_id}
                response = curation_handler.approve_handler(event, None)
            elif parsed.path.startswith("/curation/") and parsed.path.endswith("/reject"):
                source_id = parsed.path.split("/curation/")[-1].split("/reject")[0]
                event["pathParameters"] = {"source_id": source_id}
                response = curation_handler.reject_handler(event, None)
            else:
                response = {
                    "statusCode": 404,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"success": False, "error": "Not found"}),
                }
        except Exception as exc:
            response = {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"success": False, "error": str(exc)}),
            }

        self._send_response(response)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            query = {k: v[0] for k, v in parse_qs(parsed.query).items()}

            if parsed.path == "/search":
                event = _make_event(self, body=None, query=query, path_params=None)
                response = search_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/card/"):
                source_id = parsed.path.split("/card/")[-1]
                event = _make_event(self, body=None, query=query, path_params={"source_id": source_id})
                response = card_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/citation/"):
                source_id = parsed.path.split("/citation/")[-1]
                event = _make_event(self, body=None, query=query, path_params={"source_id": source_id})
                response = citation_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/status/"):
                source_id = parsed.path.split("/status/")[-1]
                event = _make_event(self, body=None, query=query, path_params={"source_id": source_id})
                response = status_handler.lambda_handler(event, None)
            elif parsed.path.startswith("/stream/") and parsed.path.endswith("/files"):
                stream_id = parsed.path.split("/stream/")[-1].split("/files")[0]
                event = _make_event(self, body=None, query=query, path_params={"stream_id": stream_id})
                response = stream_upload_handler.list_files_handler(event, None)
            elif parsed.path.startswith("/stream/") and "/preview" in parsed.path:
                # Handle both /stream/{id}/preview and /stream/{id}/files/{filename}/preview
                parts = parsed.path.split("/")
                stream_id = parts[2]
                filename = None
                if len(parts) >= 5 and parts[3] == "files":
                    filename = parts[4]
                event = _make_event(self, body=None, query=query, path_params={"stream_id": stream_id, "filename": filename})
                if filename:
                    response = preview_handler.lambda_handler(event, None)
                else:
                    response = preview_handler.preview_stream_handler(event, None)
            elif parsed.path.startswith("/stream/"):
                stream_id = parsed.path.split("/stream/")[-1]
                event = _make_event(self, body=None, query=query, path_params={"stream_id": stream_id})
                response = stream_status_handler.lambda_handler(event, None)
            elif parsed.path == "/status":
                event = _make_event(self, body=None, query=query, path_params=None)
                response = status_handler.lambda_handler(event, None)
            elif parsed.path == "/submissions":
                event = _make_event(self, body=None, query=query, path_params=None)
                response = submissions_handler.lambda_handler(event, None)
            elif parsed.path == "/health":
                response = {
                    "statusCode": 200,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"status": "ok", "service": "mdf-v2-local"}),
                }
            elif parsed.path == "/curation/pending":
                event = _make_event(self, body=None, query=query, path_params=None)
                response = curation_handler.list_pending_handler(event, None)
            elif parsed.path.startswith("/curation/"):
                source_id = parsed.path.split("/curation/")[-1]
                event = _make_event(self, body=None, query=query, path_params={"source_id": source_id})
                response = curation_handler.get_curation_handler(event, None)
            else:
                response = {
                    "statusCode": 404,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"success": False, "error": "Not found"}),
                }
        except Exception as exc:
            response = {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"success": False, "error": str(exc)}),
            }

        self._send_response(response)


def main():
    os.environ.setdefault("STORE_BACKEND", "sqlite")
    os.environ.setdefault("SQLITE_PATH", "/tmp/mdf_connect_v2.db")
    os.environ.setdefault("USE_MOCK_FLOW", "true")

    host = os.environ.get("LOCAL_HOST", "127.0.0.1")
    port = int(os.environ.get("LOCAL_PORT", "8080"))

    try:
        from http.server import ThreadingHTTPServer

        server = ThreadingHTTPServer((host, port), LocalHandler)
    except Exception:
        server = HTTPServer((host, port), LocalHandler)
    print(f"Local MDF v2 server running on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
