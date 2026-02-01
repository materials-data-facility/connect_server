"""Stream file upload handler for MDF v2.

This endpoint handles file uploads to streams.
Supports multiple storage backends: Globus (primary), S3, local.

Upload modes:
1. Base64-encoded content in JSON body (small files)
2. Pre-signed URL for direct upload (large files)
"""

import base64
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from v2.request import parse_authorizer, parse_json_body
from v2.responses import bad_request, ok, server_error
from v2.stream_store import get_stream_store
from v2.storage import get_storage_backend


def lambda_handler(event, context):
    """Handle file upload to a stream.

    POST /stream/{stream_id}/upload

    JSON body format (small files, < 6MB):
    {
        "filename": "sample_001.csv",
        "content_base64": "base64-encoded-content",
        "content_type": "text/csv",
        "metadata": {"sample_id": "001", "temperature": 300}
    }

    Or for multiple files:
    {
        "files": [
            {"filename": "a.csv", "content_base64": "...", "metadata": {...}},
            {"filename": "b.csv", "content_base64": "...", "metadata": {...}}
        ]
    }

    For large files, first get an upload URL:
    POST /stream/{stream_id}/upload-url
    """
    auth = parse_authorizer(event)
    user_id = auth.get("user_id")

    # Get user's Globus token for storage operations
    # This allows actions to be performed on behalf of the user
    headers = event.get("headers") or {}
    user_token = headers.get("x-globus-token") or auth.get("globus_dependent_token")
    if isinstance(user_token, str) and user_token.startswith("{"):
        # It's a JSON object - extract the HTTPS token if present
        try:
            tokens = json.loads(user_token)
            # Look for HTTPS scope token (endpoint UUID as key)
            user_token = next(iter(tokens.values()), {}).get("access_token") if tokens else None
        except Exception:
            user_token = None

    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id")

    if not stream_id:
        return bad_request("stream_id is required in path")

    # Check stream exists and is open
    stream_store = get_stream_store()
    stream = stream_store.get_stream(stream_id)
    if not stream:
        return bad_request("Stream not found")
    if stream.get("status") != "open":
        return bad_request("Stream is not open for uploads")

    # Parse body
    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    if not payload:
        return bad_request("Request body required")

    # Handle single file or multiple files
    files_to_upload: List[Dict[str, Any]] = []

    if "files" in payload:
        files_to_upload = payload["files"]
    elif "filename" in payload:
        files_to_upload = [payload]
    else:
        return bad_request("Either 'filename' or 'files' required")

    if not files_to_upload:
        return bad_request("No files provided")

    # Get storage backend
    storage = get_storage_backend()

    uploaded = []
    total_bytes = 0
    errors = []

    for file_data in files_to_upload:
        filename = file_data.get("filename")
        content_b64 = file_data.get("content_base64")
        content_type = file_data.get("content_type", "application/octet-stream")
        metadata = file_data.get("metadata", {})

        if not filename:
            errors.append({"error": "filename required"})
            continue

        if not content_b64:
            errors.append({"filename": filename, "error": "content_base64 required"})
            continue

        try:
            content = base64.b64decode(content_b64)
        except Exception as e:
            errors.append({"filename": filename, "error": f"Invalid base64: {e}"})
            continue

        try:
            # Pass user_token for Globus backend (ignored by local/S3)
            store_kwargs = {
                "stream_id": stream_id,
                "filename": filename,
                "content": content,
                "content_type": content_type,
                "metadata": metadata,
            }
            if user_token:
                store_kwargs["user_token"] = user_token

            file_meta = storage.store_file(**store_kwargs)
            uploaded.append(file_meta.to_dict())
            total_bytes += file_meta.size_bytes
        except Exception as e:
            errors.append({"filename": filename, "error": str(e)})

    if not uploaded and errors:
        return bad_request(f"All uploads failed: {errors}")

    # Update stream with new file count and bytes
    last_file = uploaded[-1] if uploaded else None
    stream_store.append_stream(
        stream_id=stream_id,
        file_count=len(uploaded),
        total_bytes=total_bytes,
        last_file=last_file,
    )

    return ok({
        "success": True,
        "stream_id": stream_id,
        "storage_backend": storage.backend_name,
        "uploaded": len(uploaded),
        "total_bytes": total_bytes,
        "files": uploaded,
        "errors": errors if errors else None,
    })


def upload_url_handler(event, context):
    """Get a pre-signed URL for direct upload.

    POST /stream/{stream_id}/upload-url
    {
        "filename": "large_file.hdf5",
        "content_type": "application/x-hdf5",
        "size_bytes": 1073741824
    }

    Returns:
    {
        "url": "https://...",
        "method": "PUT",
        "headers": {...},
        "path": "streams/{stream_id}/2026-01-31/large_file.hdf5",
        "expires_in": 3600
    }
    """
    auth = parse_authorizer(event)
    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id")

    if not stream_id:
        return bad_request("stream_id is required in path")

    # Check stream exists and is open
    stream_store = get_stream_store()
    stream = stream_store.get_stream(stream_id)
    if not stream:
        return bad_request("Stream not found")
    if stream.get("status") != "open":
        return bad_request("Stream is not open for uploads")

    # Parse body
    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    if not payload:
        return bad_request("Request body required")

    filename = payload.get("filename")
    if not filename:
        return bad_request("filename is required")

    content_type = payload.get("content_type", "application/octet-stream")
    expires_in = min(payload.get("expires_in", 3600), 86400)  # Max 24 hours

    # Get upload URL from storage backend
    storage = get_storage_backend()
    upload_info = storage.get_upload_url(
        stream_id=stream_id,
        filename=filename,
        content_type=content_type,
        expires_in=expires_in,
    )

    if not upload_info:
        return bad_request("This storage backend does not support direct uploads")

    return ok({
        "success": True,
        "stream_id": stream_id,
        "storage_backend": storage.backend_name,
        **upload_info,
    })


def confirm_upload_handler(event, context):
    """Confirm a direct upload completed.

    POST /stream/{stream_id}/upload-confirm
    {
        "path": "streams/{stream_id}/2026-01-31/large_file.hdf5",
        "size_bytes": 1073741824,
        "checksum_md5": "d41d8cd98f00b204e9800998ecf8427e",
        "metadata": {"experiment_id": "exp-123"}
    }

    This records the file in the stream after direct upload.
    """
    auth = parse_authorizer(event)
    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id")

    if not stream_id:
        return bad_request("stream_id is required in path")

    # Check stream exists and is open
    stream_store = get_stream_store()
    stream = stream_store.get_stream(stream_id)
    if not stream:
        return bad_request("Stream not found")
    if stream.get("status") != "open":
        return bad_request("Stream is not open for uploads")

    # Parse body
    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    if not payload:
        return bad_request("Request body required")

    path = payload.get("path")
    if not path:
        return bad_request("path is required")

    size_bytes = payload.get("size_bytes", 0)
    checksum = payload.get("checksum_md5", "")
    metadata = payload.get("metadata", {})

    # Extract filename from path
    filename = path.split("/")[-1]

    # Record the file
    file_info = {
        "filename": filename,
        "path": path,
        "size_bytes": size_bytes,
        "checksum_md5": checksum,
        "stored_at": datetime.utcnow().isoformat() + "Z",
        "storage_backend": get_storage_backend().backend_name,
        "metadata": metadata,
    }

    # Update stream
    stream_store.append_stream(
        stream_id=stream_id,
        file_count=1,
        total_bytes=size_bytes,
        last_file=file_info,
    )

    return ok({
        "success": True,
        "stream_id": stream_id,
        "file": file_info,
    })


def list_files_handler(event, context):
    """List files in a stream.

    GET /stream/{stream_id}/files
    """
    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id")

    if not stream_id:
        return bad_request("stream_id is required in path")

    stream_store = get_stream_store()
    stream = stream_store.get_stream(stream_id)
    if not stream:
        return bad_request("Stream not found")

    storage = get_storage_backend()
    files = storage.list_files(stream_id)

    return ok({
        "success": True,
        "stream_id": stream_id,
        "storage_backend": storage.backend_name,
        "file_count": len(files),
        "files": [f.to_dict() for f in files],
    })


def download_url_handler(event, context):
    """Get a download URL for a file.

    GET /stream/{stream_id}/files/{filename}/download
    or
    POST /stream/{stream_id}/download-url
    {"path": "streams/.../file.csv"}
    """
    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id")

    if not stream_id:
        return bad_request("stream_id is required in path")

    # Get path from body or path params
    payload, _ = parse_json_body(event)
    path = None

    if payload:
        path = payload.get("path")
    elif path_params.get("filename"):
        # Build path from filename (assumes current date - may need adjustment)
        filename = path_params.get("filename")
        # Try to find the file in storage
        storage = get_storage_backend()
        files = storage.list_files(stream_id)
        for f in files:
            if f.filename == filename:
                path = f.path
                break

    if not path:
        return bad_request("Could not determine file path")

    storage = get_storage_backend()
    download_url = storage.get_download_url(path)

    if not download_url:
        return bad_request("File not found or download not available")

    return ok({
        "success": True,
        "stream_id": stream_id,
        "path": path,
        "download_url": download_url,
        "storage_backend": storage.backend_name,
    })
