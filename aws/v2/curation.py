"""Curation handlers for MDF v2.

Provides API endpoints for curators to review, approve, or reject submissions.
This replaces the Globus Automate weboption-based curation workflow.

Curation states:
- pending_curation: Awaiting curator review
- approved: Curator approved, ready for DOI/indexing
- rejected: Curator rejected with reason
- published: DOI minted and indexed

Endpoints:
- GET /curation/pending - List submissions awaiting curation
- GET /curation/{source_id} - Get submission details for curation
- POST /curation/{source_id}/approve - Approve a submission
- POST /curation/{source_id}/reject - Reject a submission
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from v2.request import parse_authorizer, parse_json_body
from v2.responses import bad_request, forbidden, ok, server_error
from v2.store import get_store as get_submission_store


# Curators can be defined by user ID or group
CURATOR_USER_IDS = set(
    os.environ.get("CURATOR_USER_IDS", "").split(",")
) - {""}

CURATOR_GROUP_IDS = set(
    os.environ.get("CURATOR_GROUP_IDS", "").split(",")
) - {""}


def _is_curator(auth: Dict[str, Any]) -> bool:
    """Check if the authenticated user is a curator."""
    user_id = auth.get("user_id", "")

    # Check user ID
    if user_id in CURATOR_USER_IDS:
        return True

    # Check group membership
    group_info = auth.get("group_info", "{}")
    if isinstance(group_info, str):
        try:
            group_info = json.loads(group_info)
        except Exception:
            group_info = {}

    user_groups = set(group_info.keys()) if isinstance(group_info, dict) else set()
    if user_groups & CURATOR_GROUP_IDS:
        return True

    # For local development, allow all users to curate
    if os.environ.get("ALLOW_ALL_CURATORS", "").lower() in ("true", "1", "yes"):
        return True

    return False


def list_pending_handler(event, context):
    """List submissions pending curation.

    GET /curation/pending

    Query params:
    - limit: Max results (default 50)
    - offset: Pagination offset
    - organization: Filter by organization
    """
    auth = parse_authorizer(event)

    if not _is_curator(auth):
        return forbidden("You do not have curator permissions")

    query = event.get("queryStringParameters") or {}
    limit = min(int(query.get("limit", 50)), 200)
    offset = int(query.get("offset", 0))
    organization = query.get("organization")

    store = get_submission_store()

    # Get submissions pending curation
    all_submissions = store.list_by_status(["pending_curation"], limit=1000)

    pending = []
    for sub in all_submissions:
        if organization and sub.get("organization") != organization:
            continue
        pending.append({
            "source_id": sub.get("source_id"),
            "version": sub.get("version"),
            "title": _extract_title(sub),
            "organization": sub.get("organization"),
            "submitter": sub.get("user_id"),
            "submitted_at": sub.get("created_at"),
            "file_count": sub.get("file_count", 0),
            "total_bytes": sub.get("total_bytes", 0),
        })

    # Sort by submitted_at (oldest first for FIFO curation)
    pending.sort(key=lambda x: x.get("submitted_at", ""))

    # Apply pagination
    paginated = pending[offset:offset + limit]

    return ok({
        "success": True,
        "pending_count": len(pending),
        "submissions": paginated,
        "limit": limit,
        "offset": offset,
    })


def get_curation_handler(event, context):
    """Get submission details for curation review.

    GET /curation/{source_id}

    Returns full submission metadata for curator review.
    """
    auth = parse_authorizer(event)

    if not _is_curator(auth):
        return forbidden("You do not have curator permissions")

    path_params = event.get("pathParameters") or {}
    source_id = path_params.get("source_id")

    if not source_id:
        return bad_request("source_id is required")

    query = event.get("queryStringParameters") or {}
    version = query.get("version", "1.0")

    store = get_submission_store()
    submission = store.get_submission(source_id, version)

    if not submission:
        return bad_request("Submission not found")

    # Get curation history if any
    curation_history = submission.get("curation_history", [])

    return ok({
        "success": True,
        "submission": submission,
        "curation_history": curation_history,
        "current_status": submission.get("status"),
        "can_approve": submission.get("status") == "pending_curation",
        "can_reject": submission.get("status") == "pending_curation",
    })


def approve_handler(event, context):
    """Approve a submission for publication.

    POST /curation/{source_id}/approve
    {
        "notes": "Optional curator notes",
        "mint_doi": true,  // Whether to mint DOI (default: true)
        "metadata_updates": {}  // Optional metadata corrections
    }

    On approval:
    1. Updates status to "approved"
    2. If mint_doi=true, mints DOI
    3. Indexes to Globus Search
    4. Updates status to "published"
    """
    auth = parse_authorizer(event)

    if not _is_curator(auth):
        return forbidden("You do not have curator permissions")

    path_params = event.get("pathParameters") or {}
    source_id = path_params.get("source_id")

    if not source_id:
        return bad_request("source_id is required")

    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    payload = payload or {}

    query = event.get("queryStringParameters") or {}
    version = query.get("version") or payload.get("version", "1.0")

    store = get_submission_store()
    submission = store.get_submission(source_id, version)

    if not submission:
        return bad_request("Submission not found")

    if submission.get("status") != "pending_curation":
        return bad_request(f"Submission is not pending curation (status: {submission.get('status')})")

    curator_id = auth.get("user_id")
    curator_notes = payload.get("notes", "")
    mint_doi = payload.get("mint_doi", True)
    metadata_updates = payload.get("metadata_updates", {})

    now = datetime.now(timezone.utc).isoformat()

    # Record approval in curation history
    curation_record = {
        "action": "approved",
        "curator_id": curator_id,
        "timestamp": now,
        "notes": curator_notes,
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

    # Apply any metadata updates from curator
    if metadata_updates:
        existing_metadata = submission.get("dataset_mdata", {})
        if isinstance(existing_metadata, str):
            try:
                existing_metadata = json.loads(existing_metadata)
            except Exception:
                existing_metadata = {}
        _deep_merge(existing_metadata, metadata_updates)
        submission["dataset_mdata"] = existing_metadata

    # Update submission status
    submission["status"] = "approved"
    submission["curation_history"] = curation_history
    submission["approved_at"] = now
    submission["approved_by"] = curator_id
    submission["updated_at"] = now

    # Save the approval
    store.put_submission(submission)

    result = {
        "success": True,
        "source_id": source_id,
        "version": version,
        "status": "approved",
        "approved_by": curator_id,
        "approved_at": now,
    }

    # Mint DOI if requested
    if mint_doi:
        doi_result = _mint_doi_for_submission(submission)
        result["doi"] = doi_result

        if doi_result.get("success"):
            submission["doi"] = doi_result.get("doi")
            submission["status"] = "published"
            submission["published_at"] = datetime.now(timezone.utc).isoformat()
            store.put_submission(submission)
            result["status"] = "published"

    # TODO: Index to Globus Search
    # search_result = _index_to_search(submission)
    # result["search"] = search_result

    return ok(result)


def reject_handler(event, context):
    """Reject a submission.

    POST /curation/{source_id}/reject
    {
        "reason": "Required: reason for rejection",
        "suggestions": "Optional: suggestions for resubmission"
    }
    """
    auth = parse_authorizer(event)

    if not _is_curator(auth):
        return forbidden("You do not have curator permissions")

    path_params = event.get("pathParameters") or {}
    source_id = path_params.get("source_id")

    if not source_id:
        return bad_request("source_id is required")

    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    payload = payload or {}

    reason = payload.get("reason", "").strip()
    if not reason:
        return bad_request("reason is required for rejection")

    query = event.get("queryStringParameters") or {}
    version = query.get("version") or payload.get("version", "1.0")

    store = get_submission_store()
    submission = store.get_submission(source_id, version)

    if not submission:
        return bad_request("Submission not found")

    if submission.get("status") != "pending_curation":
        return bad_request(f"Submission is not pending curation (status: {submission.get('status')})")

    curator_id = auth.get("user_id")
    suggestions = payload.get("suggestions", "")

    now = datetime.now(timezone.utc).isoformat()

    # Record rejection in curation history
    curation_record = {
        "action": "rejected",
        "curator_id": curator_id,
        "timestamp": now,
        "reason": reason,
        "suggestions": suggestions,
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

    # Update submission status
    submission["status"] = "rejected"
    submission["curation_history"] = curation_history
    submission["rejected_at"] = now
    submission["rejected_by"] = curator_id
    submission["rejection_reason"] = reason
    submission["updated_at"] = now

    store.put_submission(submission)

    # TODO: Send rejection email to submitter
    # _send_rejection_email(submission, reason, suggestions)

    return ok({
        "success": True,
        "source_id": source_id,
        "version": version,
        "status": "rejected",
        "rejected_by": curator_id,
        "rejected_at": now,
        "reason": reason,
    })


def _extract_title(submission: Dict[str, Any]) -> str:
    """Extract title from submission metadata."""
    metadata = submission.get("dataset_mdata", {})
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            return "Untitled"

    # Try DataCite format
    dc = metadata.get("dc", {})
    titles = dc.get("titles", [])
    if titles and isinstance(titles, list):
        return titles[0].get("title", "Untitled")

    # Try direct title
    return metadata.get("title", submission.get("title", "Untitled"))


def _deep_merge(base: dict, updates: dict) -> None:
    """Deep merge updates into base dict."""
    for key, value in updates.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def _mint_doi_for_submission(submission: Dict[str, Any]) -> Dict[str, Any]:
    """Mint a DOI for an approved submission."""
    from v2.datacite import get_datacite_client

    try:
        client = get_datacite_client()

        metadata = submission.get("dataset_mdata", {})
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}

        dc = metadata.get("dc", {})

        # Build DataCite metadata
        doi_metadata = {
            "titles": dc.get("titles", [{"title": "Untitled Dataset"}]),
            "creators": dc.get("creators", [{"name": "Materials Data Facility"}]),
            "publisher": dc.get("publisher", "Materials Data Facility"),
            "publication_year": dc.get("publicationYear", datetime.now().year),
        }

        if dc.get("descriptions"):
            doi_metadata["descriptions"] = dc["descriptions"]
        if dc.get("subjects"):
            doi_metadata["subjects"] = dc["subjects"]

        source_id = submission.get("source_id", "unknown")

        result = client.mint_doi(
            source_id=source_id,
            metadata=doi_metadata,
            publish=True,
        )

        client.close()
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}
