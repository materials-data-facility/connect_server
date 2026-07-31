import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException

from v2.async_jobs import enqueue_profile_job, enqueue_stream_doi_job
from v2.app.auth import ensure_stream_owner_or_curator, get_auth
from v2.app.deps import get_stream_store_dep, get_submission_store
from v2.app.models import (
    AuthContext,
    StreamAppendRequest,
    StreamCloseRequest,
    StreamCreateRequest,
    StreamSnapshotRequest,
)
from v2.app.routers.submissions import ensure_dataset_update_permitted
from v2.doi_utils import mint_doi_for_stream
from v2.store import SubmissionStore
from v2.stream_store import StreamStore
from v2.submission_utils import generate_source_id, increment_version, latest_version

router = APIRouter()

MAX_STREAM_APPEND_COUNT = int(os.environ.get("MAX_STREAM_APPEND_COUNT", "10000"))
MAX_STREAM_APPEND_BYTES = int(os.environ.get("MAX_STREAM_APPEND_BYTES", str(50 * 1024 * 1024 * 1024)))


@router.post("/stream/create")
async def stream_create(
    payload: StreamCreateRequest,
    auth: AuthContext = Depends(get_auth),
    store: StreamStore = Depends(get_stream_store_dep),
):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    stream_id = payload.stream_id or f"stream-{uuid.uuid4().hex}"

    record = {
        "stream_id": stream_id,
        "lab_id": payload.lab_id,
        "title": payload.title,
        "status": "open",
        "file_count": 0,
        "total_bytes": 0,
        "last_append_at": None,
        "created_at": now,
        "updated_at": now,
        "user_id": auth.user_id,
        "organization": payload.organization,
        "metadata": payload.metadata,
    }

    store.create_stream(record)
    return {"success": True, "stream_id": stream_id, "stream": record}


@router.post("/stream/{stream_id}/append")
async def stream_append(
    stream_id: str,
    payload: StreamAppendRequest,
    auth: AuthContext = Depends(get_auth),
    store: StreamStore = Depends(get_stream_store_dep),
):
    stream = store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)

    # Handle files list or direct file_count/total_bytes
    if payload.files:
        file_count = len(payload.files)
        total_bytes = 0
        for entry in payload.files:
            try:
                total_bytes += int(entry.size or 0)
            except Exception:
                continue
        last_file = payload.files[-1].model_dump() if payload.files else None
    else:
        try:
            file_count = int(payload.file_count) if payload.file_count is not None else 0
        except Exception:
            file_count = 0
        try:
            total_bytes = int(payload.total_bytes) if payload.total_bytes is not None else 0
        except Exception:
            total_bytes = 0
        last_file = payload.last_file

    if file_count <= 0 and total_bytes <= 0:
        raise HTTPException(400, "Provide files list or file_count/total_bytes")
    if file_count > MAX_STREAM_APPEND_COUNT:
        raise HTTPException(413, f"file_count exceeds max {MAX_STREAM_APPEND_COUNT}")
    if total_bytes > MAX_STREAM_APPEND_BYTES:
        raise HTTPException(413, f"total_bytes exceeds max {MAX_STREAM_APPEND_BYTES}")

    updated = store.append_stream(stream_id, file_count, total_bytes, last_file=last_file)
    if not updated:
        raise HTTPException(400, "Stream not found")

    return {"success": True, "stream": updated}


@router.get("/stream/{stream_id}")
async def stream_status(
    stream_id: str,
    auth: AuthContext = Depends(get_auth),
    store: StreamStore = Depends(get_stream_store_dep),
):
    record = store.get_stream(stream_id)
    if not record:
        return {"success": False, "error": "Stream not found"}
    ensure_stream_owner_or_curator(auth, record)
    return {"success": True, "stream": record}


@router.post("/stream/{stream_id}/close")
async def stream_close(
    stream_id: str,
    payload: StreamCloseRequest,
    auth: AuthContext = Depends(get_auth),
    store: StreamStore = Depends(get_stream_store_dep),
):
    stream = store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)
    if stream.get("status") == "closed":
        raise HTTPException(400, "Stream is already closed")

    mint_doi = payload.mint_doi or False

    record = store.close_stream(stream_id)
    if not record:
        raise HTTPException(500, "Failed to close stream")

    result = {
        "success": True,
        "stream_id": stream_id,
        "status": "closed",
        "stream": record,
    }

    if mint_doi:
        doi_job = enqueue_stream_doi_job(stream_id, payload.model_dump())
        result["doi_job"] = doi_job
        if not doi_job.get("queued"):
            result["doi"] = doi_job.get("result")
            refreshed_stream = store.get_stream(stream_id)
            if refreshed_stream:
                result["stream"] = refreshed_stream

    return result


@router.post("/stream/{stream_id}/snapshot")
async def stream_snapshot(
    stream_id: str,
    payload: StreamSnapshotRequest,
    auth: AuthContext = Depends(get_auth),
    stream_store: StreamStore = Depends(get_stream_store_dep),
    sub_store: SubmissionStore = Depends(get_submission_store),
):
    stream = stream_store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)

    stream_meta = stream.get("metadata") or {}
    if isinstance(stream_meta, str):
        try:
            stream_meta = json.loads(stream_meta)
        except Exception:
            stream_meta = {}

    user_id = stream.get("user_id") or auth.user_id
    title = payload.title or stream.get("title") or f"Stream {stream_id}"

    source_id = payload.source_id or stream_id
    update = bool(payload.update)

    existing_versions = sub_store.list_versions(source_id)

    if update and not existing_versions:
        raise HTTPException(400, "Update requested but no prior submission found")

    # A snapshot with update=true is an alternate writer into a dataset's version
    # history, so it needs the same ownership gate as POST /submit — otherwise any
    # stream owner could push a version onto an arbitrary victim's dataset by
    # passing its source_id. Shared helper so the two paths cannot drift.
    if update:
        ensure_dataset_update_permitted(auth, existing_versions, source_id)

    if not update and existing_versions:
        source_id = generate_source_id(prefix=source_id)
        existing_versions = []

    latest = latest_version(existing_versions)
    version = increment_version(latest) if update else "1.0"
    versioned_source_id = f"{source_id}-{version}"

    # Build flat v2 metadata
    operator = stream_meta.get("operator") if isinstance(stream_meta, dict) else None
    dataset = {
        "title": title,
        "authors": [{"name": payload.author or operator or user_id}],
        "description": payload.description or f"Stream snapshot: {stream_id}",
        "data_sources": payload.data_sources or [f"stream://{stream_id}"],
        "organization": stream.get("organization"),
        "tags": stream_meta.get("tags", []) if isinstance(stream_meta, dict) else [],
        "test": payload.test or False,
        "update": update,
        "extensions": {
            "stream": {
                "stream_id": stream_id,
                "file_count": stream.get("file_count"),
                "total_bytes": stream.get("total_bytes"),
            }
        },
    }

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    record = {
        "source_id": source_id,
        "version": version,
        "versioned_source_id": versioned_source_id,
        "user_id": user_id,
        "user_email": None,
        "organization": stream.get("organization"),
        "status": "pending_curation",
        "dataset_mdata": json.dumps(dataset, default=lambda o: int(o) if isinstance(o, Decimal) else str(o)),
        "test": dataset.get("test", False),
        "created_at": now,
        "updated_at": now,
    }

    sub_store.put_submission(record)

    profile_job = enqueue_profile_job(source_id, version, stream_id)

    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "versioned_source_id": versioned_source_id,
        "stream_id": stream_id,
        "profile_job": profile_job,
    }


def _mint_doi_for_stream(stream: Dict, overrides: Dict) -> Dict:
    # Kept for backwards compatibility with existing imports.
    return mint_doi_for_stream(stream, overrides)
