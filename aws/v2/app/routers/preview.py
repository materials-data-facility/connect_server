import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from v2.app.auth import can_view_dataset, get_optional_auth
from v2.app.deps import get_submission_store
from v2.app.models import AuthContext
from v2.store import SubmissionStore

logger = logging.getLogger(__name__)

router = APIRouter()


# Note: stream-level preview endpoints (GET /stream/{id}/preview and
# GET /stream/{id}/files/{filename}/preview) were removed here — disabled
# until the stream feature is ready (see v2/app/__init__.py). They can be
# restored from git history alongside the streams/files router remount.


# ── Dataset-level preview (new) ────────────────────────────────────

def _get_profile_and_record(
    source_id: str,
    store: SubmissionStore,
    auth: Optional[AuthContext] = None,
):
    """Load the stored DatasetProfile + record for a source_id.

    Gated on ``can_view_dataset``: the profile carries file names, column
    names and sample rows, so a restricted-but-published dataset must not be
    previewable by anyone outside its acl.

    Returns (profile_dict, record_dict) or (None, None).
    """
    record = store.get(source_id)
    if not can_view_dataset(auth, record):
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


def _get_profile(
    source_id: str,
    store: SubmissionStore,
    auth: Optional[AuthContext] = None,
) -> Optional[dict]:
    """Load the stored DatasetProfile for a source_id (published + visible)."""
    profile, _ = _get_profile_and_record(source_id, store, auth)
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
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    """Return the stored DatasetProfile for a dataset."""
    profile, record = _get_profile_and_record(source_id, store, auth)
    if not profile:
        raise HTTPException(404, "No profile found for this dataset")

    _increment_view(source_id, record, store)
    return {"success": True, "profile": profile}


@router.get("/preview/{source_id}/files")
async def dataset_files(
    source_id: str,
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    """List all files in the dataset with metadata."""
    profile, record = _get_profile_and_record(source_id, store, auth)
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
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    """Get detailed profile of a specific file in the dataset."""
    profile, record = _get_profile_and_record(source_id, store, auth)
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
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    """Quick sample data from the first tabular file in the dataset."""
    profile, record = _get_profile_and_record(source_id, store, auth)
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
