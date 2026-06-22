#!/usr/bin/env python3
"""Command-line runner for MDF v2 local FastAPI server."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

import httpx


def _request(
    method: str,
    base_url: str,
    path: str,
    payload: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    with httpx.Client(timeout=30.0) as client:
        response = client.request(method, url, json=payload, params=params)
    try:
        return response.json()
    except Exception:
        return {
            "success": False,
            "status_code": response.status_code,
            "error": response.text,
        }


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MDF v2 local API commands")
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
    parser.add_argument("--api-url", default="http://127.0.0.1:8080")
    parser.add_argument("--payload", type=Path, help="Path to JSON payload")
    parser.add_argument("--source-id", help="Source ID")
    parser.add_argument("--version", help="Dataset version")
    parser.add_argument("--status", help="Submission status")
    parser.add_argument("--organization", help="Organization filter")
    parser.add_argument("--title", help="Stream title")
    parser.add_argument("--lab-id", help="Stream lab ID")
    parser.add_argument("--stream-id", help="Stream ID")
    parser.add_argument("--files", type=Path, help="Path to JSON file list")
    parser.add_argument("--file-count", type=int, help="Number of files appended")
    parser.add_argument("--total-bytes", type=int, help="Bytes appended")
    parser.add_argument("--update", action="store_true", help="Snapshot as update")

    args = parser.parse_args()

    payload: Dict[str, Any]
    if args.command == "submit":
        payload = _load_json(args.payload) if args.payload else {}
        result = _request("POST", args.api_url, "/submit", payload=payload)
    elif args.command == "status":
        params = {"version": args.version} if args.version else None
        result = _request("GET", args.api_url, f"/status/{args.source_id}", params=params)
    elif args.command == "submissions":
        params = {"organization": args.organization} if args.organization else None
        result = _request("GET", args.api_url, "/submissions", params=params)
    elif args.command == "update-status":
        payload = {"source_id": args.source_id, "version": args.version, "status": args.status}
        result = _request("POST", args.api_url, "/status/update", payload=payload)
    elif args.command == "stream-create":
        payload = {"title": args.title, "lab_id": args.lab_id, "organization": args.organization}
        result = _request("POST", args.api_url, "/stream/create", payload=payload)
    elif args.command == "stream-append":
        payload = {}
        if args.files:
            files_data = _load_json(args.files)
            payload["files"] = files_data.get("files", files_data)
        if args.file_count is not None:
            payload["file_count"] = args.file_count
        if args.total_bytes is not None:
            payload["total_bytes"] = args.total_bytes
        result = _request("POST", args.api_url, f"/stream/{args.stream_id}/append", payload=payload)
    elif args.command == "stream-status":
        result = _request("GET", args.api_url, f"/stream/{args.stream_id}")
    elif args.command == "stream-close":
        result = _request("POST", args.api_url, f"/stream/{args.stream_id}/close", payload={})
    else:
        payload = {"title": args.title, "update": args.update}
        result = _request("POST", args.api_url, f"/stream/{args.stream_id}/snapshot", payload=payload)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
