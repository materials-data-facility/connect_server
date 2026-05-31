import base64
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException

logger = logging.getLogger(__name__)

from v2.app.auth import ensure_stream_owner_or_curator, get_auth
from v2.app.deps import get_storage, get_stream_store_dep, get_submission_store
from v2.app.models import (
    AuthContext,
    ConfirmUploadRequest,
    DownloadUrlRequest,
    FileUploadRequest,
    UploadUrlRequest,
)
from v2.storage import StorageBackend
from v2.store import SubmissionStore
from v2.stream_store import StreamStore

router = APIRouter()


def _path_belongs_to_stream(storage: StorageBackend, stream_id: str, path: str) -> bool:
    normalized = (path or "").replace("\\", "/").strip()
    if not normalized:
        return False
    if ".." in normalized.split("/"):
        return False
    if storage.backend_name == "local":
        return normalized.startswith(f"streams/{stream_id}/")
    if storage.backend_name == "s3":
        return normalized.startswith(f"streams/{stream_id}/") or normalized.startswith(f"{stream_id}/")
    if storage.backend_name == "globus":
        return normalized.startswith(f"{stream_id}_")
    return False


@router.post("/stream/{stream_id}/upload")
async def upload_files(
    stream_id: str,
    payload: FileUploadRequest,
    auth: AuthContext = Depends(get_auth),
    stream_store: StreamStore = Depends(get_stream_store_dep),
    storage: StorageBackend = Depends(get_storage),
    x_globus_token: Optional[str] = Header(None),
):
    # Check stream exists and is open
    stream = stream_store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)
    if stream.get("status") != "open":
        raise HTTPException(400, "Stream is not open for uploads")

    # Get user token for Globus backend
    logger.info("upload_files: stream=%s, x_globus_token=%s", stream_id, "present" if x_globus_token else "NONE")
    user_token = x_globus_token
    if not user_token and auth.dependent_token:
        dep = auth.dependent_token
        if isinstance(dep, str) and dep.startswith("{"):
            try:
                tokens = json.loads(dep)
                user_token = next(iter(tokens.values()), {}).get("access_token") if tokens else None
            except Exception:
                pass
        elif isinstance(dep, dict) and dep:
            user_token = next(iter(dep.values()), {}).get("access_token") if dep else None

    # Build file list
    files_to_upload: List[Dict[str, Any]] = []
    if payload.files:
        files_to_upload = [f.model_dump() for f in payload.files]
    elif payload.filename:
        files_to_upload = [{
            "filename": payload.filename,
            "content_base64": payload.content_base64,
            "content_type": payload.content_type,
            "metadata": payload.metadata,
        }]
    else:
        raise HTTPException(400, "Either 'filename' or 'files' required")

    if not files_to_upload:
        raise HTTPException(400, "No files provided")

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
        raise HTTPException(400, f"All uploads failed: {errors}")

    # Update stream with new file count and bytes
    last_file = uploaded[-1] if uploaded else None
    stream_store.append_stream(
        stream_id=stream_id,
        file_count=len(uploaded),
        total_bytes=total_bytes,
        last_file=last_file,
    )

    return {
        "success": True,
        "stream_id": stream_id,
        "storage_backend": storage.backend_name,
        "uploaded": len(uploaded),
        "total_bytes": total_bytes,
        "files": uploaded,
        "errors": errors if errors else None,
    }


@router.post("/stream/{stream_id}/upload-url")
async def get_upload_url(
    stream_id: str,
    payload: UploadUrlRequest,
    auth: AuthContext = Depends(get_auth),
    stream_store: StreamStore = Depends(get_stream_store_dep),
    storage: StorageBackend = Depends(get_storage),
):
    stream = stream_store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)
    if stream.get("status") != "open":
        raise HTTPException(400, "Stream is not open for uploads")

    expires_in = min(payload.expires_in or 3600, 86400)

    try:
        upload_info = storage.get_upload_url(
            stream_id=stream_id,
            filename=payload.filename,
            content_type=payload.content_type,
            expires_in=expires_in,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))

    if not upload_info:
        raise HTTPException(400, "This storage backend does not support direct uploads")

    return {
        "success": True,
        "stream_id": stream_id,
        "storage_backend": storage.backend_name,
        **upload_info,
    }


@router.post("/stream/{stream_id}/upload-confirm")
async def confirm_upload(
    stream_id: str,
    payload: ConfirmUploadRequest,
    auth: AuthContext = Depends(get_auth),
    stream_store: StreamStore = Depends(get_stream_store_dep),
    storage: StorageBackend = Depends(get_storage),
):
    stream = stream_store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)
    if stream.get("status") != "open":
        raise HTTPException(400, "Stream is not open for uploads")
    if not _path_belongs_to_stream(storage, stream_id, payload.path):
        raise HTTPException(400, "Upload path does not belong to this stream")
    # Ensure the external upload is present before mutating stream accounting.
    if storage.get_file(payload.path) is None:
        raise HTTPException(400, "Uploaded file path does not exist")

    filename = payload.path.split("/")[-1]

    file_info = {
        "filename": filename,
        "path": payload.path,
        "size_bytes": payload.size_bytes,
        "checksum_md5": payload.checksum_md5,
        "stored_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "storage_backend": storage.backend_name,
        "metadata": payload.metadata,
    }

    stream_store.append_stream(
        stream_id=stream_id,
        file_count=1,
        total_bytes=payload.size_bytes or 0,
        last_file=file_info,
    )

    return {
        "success": True,
        "stream_id": stream_id,
        "file": file_info,
    }


@router.post("/stream/{stream_id}/download-url")
async def get_download_url(
    stream_id: str,
    payload: DownloadUrlRequest,
    auth: AuthContext = Depends(get_auth),
    stream_store: StreamStore = Depends(get_stream_store_dep),
    storage: StorageBackend = Depends(get_storage),
    submission_store: SubmissionStore = Depends(get_submission_store),
):
    path = payload.path if payload else None

    if not path:
        raise HTTPException(400, "Could not determine file path")
    stream = stream_store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)
    if not _path_belongs_to_stream(storage, stream_id, path):
        raise HTTPException(400, "File path does not belong to this stream")

    download_url = storage.get_download_url(path)
    if not download_url:
        raise HTTPException(400, "File not found or download not available")

    # Fire-and-forget download count increment on parent submission
    parent_source_id = stream.get("source_id")
    parent_version = stream.get("version")
    if parent_source_id and parent_version:
        try:
            submission_store.increment_counter(parent_source_id, parent_version, "download_count")
        except Exception:
            logger.debug("Failed to increment download_count for %s", parent_source_id, exc_info=True)

    return {
        "success": True,
        "stream_id": stream_id,
        "path": path,
        "download_url": download_url,
        "storage_backend": storage.backend_name,
    }


@router.get("/stream/{stream_id}/files")
async def list_files(
    stream_id: str,
    auth: AuthContext = Depends(get_auth),
    stream_store: StreamStore = Depends(get_stream_store_dep),
    storage: StorageBackend = Depends(get_storage),
):
    stream = stream_store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)

    files = storage.list_files(stream_id)

    return {
        "success": True,
        "stream_id": stream_id,
        "storage_backend": storage.backend_name,
        "file_count": len(files),
        "files": [f.to_dict() for f in files],
    }
