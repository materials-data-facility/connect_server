import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from v2.app.auth import ensure_stream_owner_or_curator, get_auth
from v2.app.deps import get_storage, get_stream_store_dep, get_submission_store
from v2.app.models import AuthContext
from v2.preview import generate_preview
from v2.storage import StorageBackend
from v2.store import SubmissionStore
from v2.stream_store import StreamStore

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Stream-level preview (existing) ────────────────────────────────

@router.get("/stream/{stream_id}/preview")
async def preview_stream(
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

    previews = []
    for f in files:
        content = storage.get_file(f.path)
        if content:
            preview = generate_preview(content, f.filename)
            previews.append({
                "filename": f.filename,
                "path": f.path,
                "size_bytes": f.size_bytes,
                "preview": preview,
            })

    return {
        "success": True,
        "stream_id": stream_id,
        "file_count": len(previews),
        "previews": previews,
    }


@router.get("/stream/{stream_id}/files/{filename}/preview")
async def preview_file(
    stream_id: str,
    filename: str,
    max_rows: Optional[int] = Query(20),
    max_lines: Optional[int] = Query(50),
    auth: AuthContext = Depends(get_auth),
    stream_store: StreamStore = Depends(get_stream_store_dep),
    storage: StorageBackend = Depends(get_storage),
):
    stream = stream_store.get_stream(stream_id)
    if not stream:
        raise HTTPException(400, "Stream not found")
    ensure_stream_owner_or_curator(auth, stream)

    # Try to find the file
    files = storage.list_files(stream_id)
    file_meta = None
    for f in files:
        if f.filename == filename or f.path.endswith(filename):
            file_meta = f
            break

    if not file_meta:
        path = f"streams/{stream_id}/{filename}"
        content = storage.get_file(path)
        if content is None:
            raise HTTPException(400, f"File not found: {filename}")
    else:
        content = storage.get_file(file_meta.path)
        if content is None:
            raise HTTPException(400, f"Could not read file: {filename}")

    preview = generate_preview(
        content=content,
        filename=filename,
        max_rows=max_rows or 20,
        max_lines=max_lines or 50,
    )

    return {
        "success": True,
        "stream_id": stream_id,
        "filename": filename,
        "preview": preview,
    }


# ── Dataset-level preview (new) ────────────────────────────────────

def _get_profile_and_record(source_id: str, store: SubmissionStore):
    """Load the stored DatasetProfile + record for a source_id (published only).

    Returns (profile_dict, record_dict) or (None, None).
    """
    record = store.get(source_id)
    if not record or record.get("status") != "published":
        return None, None
    profile = record.get("dataset_profile")
    if profile is None:
        return None, record
    if isinstance(profile, str):
        try:
            profile = json.loads(profile)
        except Exception:
            return None, record
    return profile, record


def _get_profile(source_id: str, store: SubmissionStore) -> Optional[dict]:
    """Load the stored DatasetProfile for a source_id (published only)."""
    profile, _ = _get_profile_and_record(source_id, store)
    return profile


def _increment_view(source_id: str, record: Optional[dict], store: SubmissionStore) -> None:
    """Fire-and-forget view count increment."""
    if not record:
        return
    try:
        store.increment_counter(source_id, record["version"], "view_count")
    except Exception:
        logger.debug("Failed to increment view_count for %s", source_id, exc_info=True)


@router.get("/preview/{source_id}")
async def dataset_preview(
    source_id: str,
    store: SubmissionStore = Depends(get_submission_store),
):
    """Return the stored DatasetProfile for a dataset."""
    profile, record = _get_profile_and_record(source_id, store)
    if not profile:
        raise HTTPException(404, "No profile found for this dataset")

    _increment_view(source_id, record, store)
    return {"success": True, "profile": profile}


@router.get("/preview/{source_id}/files")
async def dataset_files(
    source_id: str,
    store: SubmissionStore = Depends(get_submission_store),
):
    """List all files in the dataset with metadata."""
    profile, record = _get_profile_and_record(source_id, store)
    if not profile:
        raise HTTPException(404, "No profile found for this dataset")

    _increment_view(source_id, record, store)

    files = []
    for fp in profile.get("files", []):
        files.append({
            "path": fp.get("path"),
            "filename": fp.get("filename"),
            "size_bytes": fp.get("size_bytes"),
            "content_type": fp.get("content_type"),
            "format": fp.get("format"),
        })

    return {"success": True, "source_id": source_id, "files": files}


@router.get("/preview/{source_id}/files/{path:path}")
async def dataset_file_detail(
    source_id: str,
    path: str,
    store: SubmissionStore = Depends(get_submission_store),
):
    """Get detailed profile of a specific file in the dataset."""
    profile, record = _get_profile_and_record(source_id, store)
    if not profile:
        raise HTTPException(404, "No profile found for this dataset")

    _increment_view(source_id, record, store)

    for fp in profile.get("files", []):
        if fp.get("path") == path or fp.get("filename") == path:
            return {"success": True, "source_id": source_id, "file": fp}

    raise HTTPException(404, f"File not found in profile: {path}")


@router.get("/preview/{source_id}/sample")
async def dataset_sample(
    source_id: str,
    store: SubmissionStore = Depends(get_submission_store),
):
    """Quick sample data from the first tabular file in the dataset."""
    profile, record = _get_profile_and_record(source_id, store)
    if not profile:
        raise HTTPException(404, "No profile found for this dataset")

    _increment_view(source_id, record, store)

    # Find the first file with sample_rows
    for fp in profile.get("files", []):
        sample_rows = fp.get("sample_rows", [])
        if sample_rows:
            return {
                "success": True,
                "source_id": source_id,
                "filename": fp.get("filename"),
                "format": fp.get("format"),
                "columns": fp.get("columns", []),
                "n_rows": fp.get("n_rows"),
                "sample_rows": sample_rows,
            }

    # Fallback: return preview_lines from first text file
    for fp in profile.get("files", []):
        preview_lines = fp.get("preview_lines", [])
        if preview_lines:
            return {
                "success": True,
                "source_id": source_id,
                "filename": fp.get("filename"),
                "format": fp.get("format"),
                "preview_lines": preview_lines,
            }

    return {
        "success": True,
        "source_id": source_id,
        "message": "No previewable data found",
    }
