import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

logger = logging.getLogger(__name__)

from v2.async_jobs import enqueue_publish_job
from v2.app.auth import require_curator
from v2.app.deps import get_submission_store
from v2.app.models import AuthContext, CurationApproveRequest, CurationRejectRequest
from v2.email_utils import notify_submitter_rejected
from v2.metadata import parse_metadata
from v2.store import SubmissionStore
from v2.submission_utils import deep_merge, latest_version

router = APIRouter()


def _resolve_submission_for_curation(
    store: SubmissionStore,
    source_id: str,
    version: Optional[str],
) -> Dict[str, Any]:
    if version:
        submission = store.get_submission(source_id, version)
        if not submission:
            raise HTTPException(404, "Submission not found")
        return submission

    versions = store.list_versions(source_id)
    if not versions:
        raise HTTPException(404, "Submission not found")
    latest = latest_version(versions)
    for item in versions:
        if item.get("version") == latest:
            return item
    return versions[-1]


@router.get("/curation/pending")
async def list_pending(
    limit: Optional[int] = Query(50),
    offset: Optional[int] = Query(0),
    organization: Optional[str] = Query(None),
    auth: AuthContext = Depends(require_curator),
    store: SubmissionStore = Depends(get_submission_store),
):
    limit_val = min(int(limit or 50), 200)
    offset_val = int(offset or 0)

    all_submissions = store.list_by_status(["pending_curation"], limit=1000)

    pending = []
    for sub in all_submissions:
        if organization and sub.get("organization") != organization:
            continue
        try:
            meta = parse_metadata(sub)
        except Exception:
            meta = None
        title = meta.title if meta else "Untitled"
        authors = [a.model_dump() for a in meta.authors] if meta else []
        description = meta.description if meta else None
        data_sources = meta.data_sources if meta else []
        pending.append({
            "source_id": sub.get("source_id"),
            "version": sub.get("version"),
            "title": title,
            "authors": authors,
            "description": description,
            "data_sources": data_sources,
            "organization": sub.get("organization"),
            "submitter": sub.get("user_id"),
            "submitter_email": sub.get("user_email"),
            "submitted_at": sub.get("created_at"),
            "file_count": sub.get("file_count", 0),
            "total_bytes": sub.get("total_bytes", 0),
        })

    pending.sort(key=lambda x: x.get("submitted_at", ""))
    paginated = pending[offset_val:offset_val + limit_val]

    return {
        "success": True,
        "pending_count": len(pending),
        "submissions": paginated,
        "limit": limit_val,
        "offset": offset_val,
    }


@router.get("/curation/{source_id}")
async def get_curation(
    source_id: str,
    version: Optional[str] = Query(None),
    auth: AuthContext = Depends(require_curator),
    store: SubmissionStore = Depends(get_submission_store),
):
    submission = _resolve_submission_for_curation(store, source_id, version)

    curation_history = submission.get("curation_history", [])

    return {
        "success": True,
        "submission": submission,
        "curation_history": curation_history,
        "current_status": submission.get("status"),
        "can_approve": submission.get("status") == "pending_curation",
        "can_reject": submission.get("status") == "pending_curation",
    }


@router.post("/curation/{source_id}/approve")
async def approve(
    source_id: str,
    payload: CurationApproveRequest,
    auth: AuthContext = Depends(require_curator),
    store: SubmissionStore = Depends(get_submission_store),
):
    submission = _resolve_submission_for_curation(store, source_id, payload.version)
    version = submission.get("version")

    if submission.get("status") != "pending_curation":
        raise HTTPException(
            400,
            f"Submission is not pending curation (status: {submission.get('status')})",
        )

    curator_id = auth.user_id
    now = datetime.now(timezone.utc).isoformat()

    curation_record = {
        "action": "approved",
        "curator_id": curator_id,
        "timestamp": now,
        "notes": payload.notes or "",
    }

    curation_history = submission.get("curation_history") or []
    if isinstance(curation_history, str):
        try:
            curation_history = json.loads(curation_history)
        except Exception:
            curation_history = []
    if not isinstance(curation_history, list):
        curation_history = []
    curation_history.append(curation_record)

    if payload.metadata_updates:
        existing_metadata = submission.get("dataset_mdata", {})
        if isinstance(existing_metadata, str):
            try:
                existing_metadata = json.loads(existing_metadata)
            except Exception:
                existing_metadata = {}
        # Deep merge metadata updates into existing flat metadata
        deep_merge(existing_metadata, payload.metadata_updates)
        submission["dataset_mdata"] = existing_metadata
        submission["metadata_updated_at"] = now

    submission["status"] = "approved"
    submission["curation_history"] = curation_history
    submission["approved_at"] = now
    submission["approved_by"] = curator_id
    submission["updated_at"] = now

    store.upsert_submission(submission)

    logger.info("Submission approved source_id=%s version=%s by=%s", source_id, version, curator_id)

    result = {
        "success": True,
        "source_id": source_id,
        "version": version,
        "status": "approved",
        "approved_by": curator_id,
        "approved_at": now,
    }

    # Always trigger publish pipeline (search ingest + status update);
    # mint_doi flag only controls the DOI step
    publish_job = enqueue_publish_job(source_id, version, mint_doi=payload.mint_doi)
    result["publish_job"] = publish_job
    if not publish_job.get("queued"):
        publish_result = publish_job.get("result", {})
        if publish_result.get("doi", {}).get("success"):
            result["doi"] = publish_result["doi"]
        if publish_result.get("status") == "published":
            result["status"] = "published"
        # Refresh to get latest state after inline publish
        refreshed = store.get_submission(source_id, version)
        if refreshed:
            result["status"] = refreshed.get("status", result["status"])

    return result


@router.post("/curation/{source_id}/reject")
async def reject(
    source_id: str,
    payload: CurationRejectRequest,
    auth: AuthContext = Depends(require_curator),
    store: SubmissionStore = Depends(get_submission_store),
):
    reason = payload.reason.strip()
    if not reason:
        raise HTTPException(400, "reason is required for rejection")

    submission = _resolve_submission_for_curation(store, source_id, payload.version)
    version = submission.get("version")

    if submission.get("status") != "pending_curation":
        raise HTTPException(
            400,
            f"Submission is not pending curation (status: {submission.get('status')})",
        )

    curator_id = auth.user_id
    now = datetime.now(timezone.utc).isoformat()

    curation_record = {
        "action": "rejected",
        "curator_id": curator_id,
        "timestamp": now,
        "reason": reason,
        "suggestions": payload.suggestions or "",
    }

    curation_history = submission.get("curation_history") or []
    if isinstance(curation_history, str):
        try:
            curation_history = json.loads(curation_history)
        except Exception:
            curation_history = []
    if not isinstance(curation_history, list):
        curation_history = []
    curation_history.append(curation_record)

    submission["status"] = "rejected"
    submission["curation_history"] = curation_history
    submission["rejected_at"] = now
    submission["rejected_by"] = curator_id
    submission["rejection_reason"] = reason
    submission["updated_at"] = now

    store.upsert_submission(submission)

    logger.info("Submission rejected source_id=%s version=%s by=%s reason=%s", source_id, version, curator_id, reason)

    try:
        notify_submitter_rejected(submission, reason, payload.suggestions or "")
    except Exception:
        logger.warning("Failed to send rejection email for %s", source_id, exc_info=True)

    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "status": "rejected",
        "rejected_by": curator_id,
        "rejected_at": now,
        "reason": reason,
    }


