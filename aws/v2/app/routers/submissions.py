import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query

from v2.async_jobs import enqueue_profile_job, enqueue_publish_job, enqueue_transfer_job
from v2.transfer import check_transfer_status, cleanup_transfer_acl
from v2.app.auth import (
    ensure_submission_owner_or_curator,
    get_auth,
    get_optional_auth,
    is_curator,
    require_curator,
    require_submitter,
)
from v2.app.deps import get_submission_store
from v2.app.models import (
    AuthContext,
    DeleteSubmissionRequest,
    MetadataEditRequest,
    ResubmitRequest,
    StatusUpdateRequest,
    WithdrawRequest,
)
from v2.config import DEFAULT_ORGANIZATION
from v2.email_utils import notify_curators_new_submission
from v2.metadata import DatasetMetadata, migrate_v1_payload
from v2.store import SubmissionStore, parse_pagination_key, serialize_pagination_key
from v2.submission_utils import deep_merge, generate_source_id, increment_version, latest_version

logger = logging.getLogger(__name__)

router = APIRouter()

MAX_SUBMIT_METADATA_BYTES = int(os.environ.get("MAX_SUBMIT_METADATA_BYTES", "262144"))
MAX_SUBMIT_DATA_SOURCES = int(os.environ.get("MAX_SUBMIT_DATA_SOURCES", "2000"))
MAX_SUBMIT_AUTHORS = int(os.environ.get("MAX_SUBMIT_AUTHORS", "1000"))


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)


def _validate_data_sources(data_sources: List[str]) -> List[str]:
    """Validate data source URL formats. Returns list of error messages."""
    errors: List[str] = []
    for i, src in enumerate(data_sources):
        if src.startswith("globus://"):
            # Must have UUID-like segment and non-empty path
            rest = src[len("globus://"):]
            slash_idx = rest.find("/")
            if slash_idx <= 0:
                errors.append(f"data_sources[{i}]: globus:// URI missing path: {src}")
                continue
            collection_id = rest[:slash_idx]
            path = rest[slash_idx:]
            if not _UUID_RE.match(collection_id):
                errors.append(f"data_sources[{i}]: globus:// URI has invalid collection UUID: {collection_id}")
            if not path or path == "/":
                errors.append(f"data_sources[{i}]: globus:// URI has empty path")
        elif src.startswith("https://") or src.startswith("http://"):
            parsed = urlparse(src)
            if not parsed.hostname:
                errors.append(f"data_sources[{i}]: malformed URL (no hostname): {src}")
        elif src.startswith("stream://"):
            stream_id = src[len("stream://"):]
            if not stream_id.strip():
                errors.append(f"data_sources[{i}]: stream:// URI has empty ID")
        # Other formats (absolute paths, etc.) pass through
    return errors


def _is_v1_payload(metadata: dict) -> bool:
    """Detect old dc/mdf/custom format."""
    dc = metadata.get("dc")
    return isinstance(dc, dict) and ("titles" in dc or "creators" in dc)


def _source_id_from_metadata(metadata: Dict[str, Any]) -> Optional[str]:
    """Extract source_id from either v1 or v2 payload."""
    # v1 format
    mdf = metadata.get("mdf", {})
    sid = mdf.get("source_id") or mdf.get("source_name")
    if sid:
        return sid
    # v2 format: check extensions
    ext = metadata.get("extensions", {})
    return ext.get("mdf_source_id") or ext.get("mdf_source_name")


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    if not record:
        return {}
    if "dataset_mdata" in record and isinstance(record["dataset_mdata"], str):
        try:
            record["dataset_mdata"] = json.loads(record["dataset_mdata"])
        except Exception:
            pass
    return record


def _extract_dependent_transfer_token(auth) -> Optional[str]:
    """Extract the user's Globus Transfer token from dependent tokens.

    The server performs a dependent token exchange on the user's auth token
    to obtain tokens for downstream services (groups, transfer, etc.).
    """
    dep = auth.dependent_token
    if not dep:
        return None

    # dependent_token is a dict keyed by resource server
    transfer_entry = dep.get("transfer.api.globus.org")
    if not transfer_entry:
        return None

    if isinstance(transfer_entry, dict):
        return transfer_entry.get("access_token")
    return getattr(transfer_entry, "access_token", None)


def _flip_latest_on_prior(store: SubmissionStore, prior_record: Dict[str, Any]) -> None:
    """Set latest=false in the prior version's metadata."""
    mdata = prior_record.get("dataset_mdata")
    if isinstance(mdata, str):
        try:
            mdata = json.loads(mdata)
        except Exception:
            return
    if not isinstance(mdata, dict):
        return
    mdata["latest"] = False
    prior_record["dataset_mdata"] = json.dumps(mdata)
    prior_record["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    store.upsert_submission(prior_record)


def _resolve_submission(
    store: SubmissionStore,
    source_id: str,
    version: Optional[str],
) -> Dict[str, Any]:
    """Resolve a submission by source_id and optional version (defaults to latest)."""
    if version:
        record = store.get_submission(source_id, version)
        if not record:
            raise HTTPException(404, "Submission not found")
        return record
    versions = store.list_versions(source_id)
    if not versions:
        raise HTTPException(404, "Submission not found")
    latest_ver = latest_version(versions)
    for item in versions:
        if item.get("version") == latest_ver:
            return item
    return versions[-1]


def _parse_mdata(record: Dict[str, Any]) -> dict:
    """Extract dataset_mdata as a new dict from a submission record.

    Always returns a deep copy so callers can mutate without affecting the
    original record (important since SQLite _row_to_dict auto-parses JSON).
    """
    import copy
    mdata = record.get("dataset_mdata", {})
    if isinstance(mdata, str):
        try:
            mdata = json.loads(mdata)
        except Exception:
            mdata = {}
    if not isinstance(mdata, dict):
        return {}
    return copy.deepcopy(mdata)


@router.post("/submissions/{source_id}/metadata")
async def edit_metadata(
    source_id: str,
    payload: MetadataEditRequest,
    auth: AuthContext = Depends(get_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    submission = _resolve_submission(store, source_id, payload.version)
    ensure_submission_owner_or_curator(auth, submission)

    status = submission.get("status")
    if status not in ("pending_curation", "rejected", "published"):
        raise HTTPException(400, f"Cannot edit metadata when status is '{status}'")

    # Build updates from non-None payload fields (excluding version)
    editable_fields = [
        "title", "authors", "description", "keywords", "license", "funding",
        "related_works", "methods", "facility", "fields_of_science", "domains",
        "ml", "geo_locations", "tags", "extensions",
    ]
    updates = {}
    for field in editable_fields:
        value = getattr(payload, field)
        if value is not None:
            updates[field] = value

    if not updates:
        raise HTTPException(400, "No metadata fields provided")

    existing_mdata = _parse_mdata(submission)
    deep_merge(existing_mdata, updates)

    # Re-validate through Pydantic to catch schema errors
    try:
        DatasetMetadata.model_validate(existing_mdata)
    except Exception as exc:
        raise HTTPException(400, f"Invalid metadata after edit: {exc}")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    version = submission.get("version")

    if status == "published":
        # Auto-create a minor version bump
        new_version = increment_version(version, major=False)
        new_versioned_id = "{}-{}".format(source_id, new_version)

        existing_mdata["version"] = new_version
        existing_mdata["latest"] = True
        existing_mdata["previous_version"] = "{}-{}".format(source_id, version)
        existing_mdata["root_version"] = existing_mdata.get("root_version") or "{}-{}".format(source_id, "1.0")

        new_record = {
            "source_id": source_id,
            "version": new_version,
            "versioned_source_id": new_versioned_id,
            "user_id": auth.user_id,
            "user_email": auth.user_email,
            "organization": submission.get("organization"),
            "status": "published",
            "dataset_mdata": json.dumps(existing_mdata),
            "schema_version": submission.get("schema_version", "2"),
            "test": submission.get("test", False),
            "created_at": now,
            "updated_at": now,
            "published_at": now,
        }

        # Inherit dataset_doi
        dataset_doi = submission.get("dataset_doi") or submission.get("doi")
        if dataset_doi:
            new_record["dataset_doi"] = dataset_doi

        # Flip latest=False on the prior version
        _flip_latest_on_prior(store, submission)

        store.put_submission(new_record)

        # Enqueue publish job to update DataCite metadata and re-index in search
        try:
            enqueue_publish_job(source_id, new_version, mint_doi=False)
        except Exception:
            logger.warning("Failed to enqueue publish job for metadata edit %s v%s", source_id, new_version, exc_info=True)

        return {
            "success": True,
            "source_id": source_id,
            "version": version,
            "new_version": new_version,
            "updated_fields": list(updates.keys()),
        }

    # pending_curation or rejected: update in-place
    submission["dataset_mdata"] = json.dumps(existing_mdata)
    submission["updated_at"] = now
    store.upsert_submission(submission)

    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "updated_fields": list(updates.keys()),
    }


@router.post("/submissions/{source_id}/withdraw")
async def withdraw(
    source_id: str,
    payload: WithdrawRequest,
    auth: AuthContext = Depends(get_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    submission = _resolve_submission(store, source_id, payload.version)
    ensure_submission_owner_or_curator(auth, submission)

    status = submission.get("status")
    if status != "pending_curation":
        raise HTTPException(400, f"Can only withdraw submissions with status 'pending_curation', current status is '{status}'")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    version = submission.get("version")

    curation_history = submission.get("curation_history") or []
    if isinstance(curation_history, str):
        try:
            curation_history = json.loads(curation_history)
        except Exception:
            curation_history = []
    if not isinstance(curation_history, list):
        curation_history = []
    curation_history.append({
        "action": "withdrawn",
        "user_id": auth.user_id,
        "timestamp": now,
        "reason": payload.reason or "",
    })

    submission["status"] = "withdrawn"
    submission["curation_history"] = curation_history
    submission["updated_at"] = now
    store.upsert_submission(submission)

    # If this was latest=True, restore latest on prior version
    mdata = _parse_mdata(submission)
    if mdata.get("latest") and mdata.get("previous_version"):
        prev_vsid = mdata["previous_version"]
        # Extract version from "source_id-version"
        parts = prev_vsid.rsplit("-", 1)
        if len(parts) == 2:
            prev_version = parts[1]
            prev_record = store.get_submission(source_id, prev_version)
            if prev_record:
                prev_mdata = _parse_mdata(prev_record)
                prev_mdata["latest"] = True
                prev_record["dataset_mdata"] = json.dumps(prev_mdata)
                prev_record["updated_at"] = now
                store.upsert_submission(prev_record)

    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "status": "withdrawn",
    }


@router.post("/submissions/{source_id}/resubmit")
async def resubmit(
    source_id: str,
    payload: ResubmitRequest,
    auth: AuthContext = Depends(get_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    submission = _resolve_submission(store, source_id, payload.version)
    ensure_submission_owner_or_curator(auth, submission)

    status = submission.get("status")
    if status != "rejected":
        raise HTTPException(400, f"Can only resubmit submissions with status 'rejected', current status is '{status}'")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    version = submission.get("version")

    curation_history = submission.get("curation_history") or []
    if isinstance(curation_history, str):
        try:
            curation_history = json.loads(curation_history)
        except Exception:
            curation_history = []
    if not isinstance(curation_history, list):
        curation_history = []
    curation_history.append({
        "action": "resubmitted",
        "user_id": auth.user_id,
        "timestamp": now,
        "notes": payload.notes or "",
    })

    submission["status"] = "pending_curation"
    submission["curation_history"] = curation_history
    submission["updated_at"] = now
    store.upsert_submission(submission)

    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "status": "pending_curation",
    }


@router.post("/submissions/{source_id}/delete")
async def delete_submission(
    source_id: str,
    payload: DeleteSubmissionRequest,
    auth: AuthContext = Depends(require_curator),
    store: SubmissionStore = Depends(get_submission_store),
):
    submission = _resolve_submission(store, source_id, payload.version)
    version = submission.get("version")

    if submission.get("status") == "deleted":
        raise HTTPException(400, "Submission is already deleted")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    curation_history = submission.get("curation_history") or []
    if isinstance(curation_history, str):
        try:
            curation_history = json.loads(curation_history)
        except Exception:
            curation_history = []
    if not isinstance(curation_history, list):
        curation_history = []
    curation_history.append({
        "action": "deleted",
        "user_id": auth.user_id,
        "timestamp": now,
        "reason": payload.reason,
    })

    submission["status"] = "deleted"
    submission["deleted_at"] = now
    submission["deleted_by"] = auth.user_id
    submission["curation_history"] = curation_history
    submission["updated_at"] = now
    store.upsert_submission(submission)

    logger.info("Submission deleted source_id=%s version=%s by=%s reason=%s",
                source_id, version, auth.user_id, payload.reason)

    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "status": "deleted",
    }


@router.get("/versions/{source_id}/diff")
async def version_diff(
    source_id: str,
    from_version: str = Query(..., alias="from"),
    to_version: str = Query(..., alias="to"),
    store: SubmissionStore = Depends(get_submission_store),
):
    from_record = store.get_submission(source_id, from_version)
    if not from_record:
        raise HTTPException(404, f"Version {from_version} not found")
    to_record = store.get_submission(source_id, to_version)
    if not to_record:
        raise HTTPException(404, f"Version {to_version} not found")

    from_mdata = _parse_mdata(from_record)
    to_mdata = _parse_mdata(to_record)

    # Skip system/versioning fields from diff
    skip_fields = {"version", "latest", "previous_version", "root_version", "update", "test"}

    all_keys = (set(from_mdata.keys()) | set(to_mdata.keys())) - skip_fields
    added = {}
    removed = {}
    changed = {}
    unchanged = []

    for key in sorted(all_keys):
        in_from = key in from_mdata
        in_to = key in to_mdata
        if in_to and not in_from:
            added[key] = to_mdata[key]
        elif in_from and not in_to:
            removed[key] = from_mdata[key]
        elif from_mdata[key] != to_mdata[key]:
            changed[key] = {"from": from_mdata[key], "to": to_mdata[key]}
        else:
            unchanged.append(key)

    return {
        "success": True,
        "source_id": source_id,
        "from_version": {
            "version": from_version,
            "status": from_record.get("status"),
            "created_at": from_record.get("created_at"),
        },
        "to_version": {
            "version": to_version,
            "status": to_record.get("status"),
            "created_at": to_record.get("created_at"),
        },
        "diff": {
            "added": added,
            "removed": removed,
            "changed": changed,
            "unchanged": unchanged,
        },
    }


@router.post("/submit")
async def submit(
    metadata: dict,
    auth: AuthContext = Depends(require_submitter),
    store: SubmissionStore = Depends(get_submission_store),
):
    user_id = auth.user_id
    user_email = auth.user_email

    if not metadata:
        raise HTTPException(400, "POST data empty or not JSON")

    try:
        serialized = json.dumps(metadata, allow_nan=False)
    except Exception:
        raise HTTPException(400, "Submission may not contain NaN or Infinity")
    if len(serialized.encode("utf-8")) > MAX_SUBMIT_METADATA_BYTES:
        raise HTTPException(413, f"Submission metadata exceeds {MAX_SUBMIT_METADATA_BYTES} bytes")

    # Auto-detect and migrate v1 format
    if _is_v1_payload(metadata):
        metadata = migrate_v1_payload(metadata)

    # Validate through Pydantic (fills defaults, validates types)
    try:
        validated = DatasetMetadata.model_validate(metadata)
    except Exception as exc:
        raise HTTPException(400, f"Invalid metadata: {exc}")

    flat = validated.model_dump()
    if len(flat.get("data_sources", [])) > MAX_SUBMIT_DATA_SOURCES:
        raise HTTPException(413, f"Too many data_sources (max {MAX_SUBMIT_DATA_SOURCES})")
    if len(flat.get("authors", [])) > MAX_SUBMIT_AUTHORS:
        raise HTTPException(413, f"Too many authors (max {MAX_SUBMIT_AUTHORS})")

    data_source_errors = _validate_data_sources(flat.get("data_sources", []))
    if data_source_errors:
        raise HTTPException(400, f"Invalid data_sources: {'; '.join(data_source_errors)}")

    if not flat.get("data_sources") and not metadata.get("update_metadata_only") and not flat.get("update"):
        raise HTTPException(400, "You must provide data_sources before submission")

    organization = flat.get("organization") or DEFAULT_ORGANIZATION
    if isinstance(organization, list):
        organization = organization[0]

    is_test = flat.get("test", False)
    update = flat.get("update", False)

    source_id = _source_id_from_metadata(metadata)
    existing_versions = []
    if source_id:
        existing_versions = store.list_versions(source_id)

    if update and not source_id:
        raise HTTPException(400, "Missing source_id for update submission")

    if not update and not source_id:
        source_id = generate_source_id()

    if not update and source_id and existing_versions:
        source_id = "{}-{}".format(source_id, uuid.uuid4().hex[:8])
        existing_versions = []

    latest_ver = latest_version(existing_versions)
    has_new_data = bool(flat.get("data_sources"))
    version = increment_version(latest_ver, major=has_new_data) if update else "1.0"

    if update and not latest_ver:
        raise HTTPException(400, "Update requested but no prior submission found")

    versioned_source_id = "{}-{}".format(source_id, version)

    # Propagate dataset_doi from prior published versions
    inherited_dataset_doi = None
    previous_version_id = None
    root_version_id = None
    prior_record = None
    if update and existing_versions:
        for v in existing_versions:
            ddoi = v.get("dataset_doi") or v.get("doi")
            if ddoi and v.get("status") == "published":
                inherited_dataset_doi = ddoi
                break
        # Build version chain: previous_version = the latest existing version's id
        previous_version_id = "{}-{}".format(source_id, latest_ver)
        # Root version: inherit from prior, or use the earliest version
        prior_record = next(
            (v for v in existing_versions if v.get("version") == latest_ver), None
        )
        if prior_record:
            prior_mdata = prior_record.get("dataset_mdata")
            if isinstance(prior_mdata, str):
                try:
                    prior_mdata = json.loads(prior_mdata)
                except Exception:
                    prior_mdata = {}
            if isinstance(prior_mdata, dict):
                root_version_id = prior_mdata.get("root_version")
        if not root_version_id:
            # Earliest version is the root
            earliest_ver = sorted(
                existing_versions, key=lambda v: v.get("version", "0")
            )[0]
            root_version_id = "{}-{}".format(source_id, earliest_ver.get("version", "1.0"))

    # Inherit data_sources from prior version for metadata-only updates
    if update and not has_new_data and prior_record:
        prior_mdata = prior_record.get("dataset_mdata")
        if isinstance(prior_mdata, str):
            try:
                prior_mdata = json.loads(prior_mdata)
            except Exception:
                prior_mdata = {}
        if isinstance(prior_mdata, dict):
            flat["data_sources"] = prior_mdata.get("data_sources", [])

    # Populate versioning fields in metadata
    flat["version"] = version
    flat["latest"] = True
    if update:
        flat["previous_version"] = previous_version_id
        flat["root_version"] = root_version_id
    else:
        flat["root_version"] = versioned_source_id

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    record = {
        "source_id": source_id,
        "version": version,
        "versioned_source_id": versioned_source_id,
        "user_id": user_id,
        "user_email": user_email,
        "organization": organization,
        "status": "pending_curation",
        "dataset_mdata": json.dumps(flat),
        "schema_version": "2",
        "test": is_test,
        "created_at": now,
        "updated_at": now,
    }
    if inherited_dataset_doi:
        record["dataset_doi"] = inherited_dataset_doi

    # Flip latest=false on prior version's metadata
    if update and prior_record:
        try:
            _flip_latest_on_prior(store, prior_record)
        except Exception:
            logger.warning("Failed to flip latest on prior version %s", latest_ver, exc_info=True)

    try:
        store.put_submission(record)
    except Exception as exc:
        logger.exception("Failed to store submission")
        raise HTTPException(500, "Internal error while storing submission")

    try:
        notify_curators_new_submission(record)
    except Exception:
        logger.warning("Failed to send new-submission email for %s", source_id, exc_info=True)

    response = {
        "success": True,
        "source_id": source_id,
        "version": version,
        "versioned_source_id": versioned_source_id,
        "organization": organization,
    }

    # Profile jobs for stream-backed sources (inline or async queue depending on mode)
    data_sources = flat.get("data_sources", [])
    profile_jobs = []
    for source in data_sources:
        if source.startswith("stream://"):
            stream_id = source.replace("stream://", "", 1)
            try:
                profile_jobs.append(enqueue_profile_job(source_id, version, stream_id))
            except Exception:
                logger.debug("Profile job dispatch failed for %s", source_id, exc_info=True)
    if profile_jobs:
        response["profile_jobs"] = profile_jobs

    # Transfer jobs for data on external Globus endpoints
    from v2.transfer import NCSA_MDF_COLLECTION_UUID, extract_transfer_sources

    transfer_sources = extract_transfer_sources(data_sources)
    if transfer_sources:
        # Extract user's transfer token from dependent tokens
        user_transfer_token = _extract_dependent_transfer_token(auth)
        if user_transfer_token:
            try:
                transfer_result = enqueue_transfer_job(
                    source_id=source_id,
                    version=version,
                    data_sources=data_sources,
                    user_transfer_token=user_transfer_token,
                    user_identity_id=auth.user_id,
                )
                response["transfer_job"] = transfer_result
            except Exception:
                logger.debug("Transfer job dispatch failed for %s", source_id, exc_info=True)
        else:
            response["transfer_warning"] = (
                "Data sources reference external Globus endpoints but no transfer token "
                "was available. Run 'mdf login' to authenticate with transfer scope."
            )

    return response


@router.get("/versions/{source_id}")
async def list_versions(
    source_id: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    versions = store.list_versions(source_id)
    if not versions:
        return {"success": False, "error": "No versions found for this source_id"}

    # If unauthenticated (or not owner/curator), only show published versions
    is_privileged = False
    if auth:
        owner_id = versions[0].get("user_id") if versions else None
        is_privileged = (owner_id and owner_id == auth.user_id) or is_curator(auth)
    if not is_privileged:
        versions = [v for v in versions if v.get("status") == "published"]
        if not versions:
            return {"success": False, "error": "No versions found for this source_id"}

    sorted_versions = sorted(versions, key=lambda x: x.get("version", "0"))
    total_count = len(sorted_versions)
    paginated = sorted_versions[offset:offset + limit]

    result_versions = []
    for v in paginated:
        mdata = v.get("dataset_mdata")
        if isinstance(mdata, str):
            try:
                mdata = json.loads(mdata)
            except Exception:
                mdata = {}
        if not isinstance(mdata, dict):
            mdata = {}

        result_versions.append({
            "version": v.get("version"),
            "title": mdata.get("title", ""),
            "status": v.get("status", ""),
            "doi": v.get("dataset_doi") or v.get("doi") or mdata.get("doi"),
            "created_at": v.get("created_at", ""),
            "updated_at": v.get("updated_at", ""),
        })

    # Find the dataset-level DOI (from any published version)
    dataset_doi = None
    for v in versions:
        ddoi = v.get("dataset_doi") or v.get("doi")
        if ddoi and v.get("status") == "published":
            dataset_doi = ddoi
            break

    return {
        "success": True,
        "source_id": source_id,
        "versions": result_versions,
        "total_count": total_count,
        "dataset_doi": dataset_doi,
    }


@router.get("/stats/{source_id}")
async def dataset_stats(
    source_id: str,
    store: SubmissionStore = Depends(get_submission_store),
):
    """Public access/download stats for a published dataset."""
    versions = store.list_versions(source_id)
    published = [v for v in versions if v.get("status") == "published"]
    if not published:
        raise HTTPException(404, "No published dataset found")

    total_views = 0
    total_downloads = 0
    first_published = None
    last_updated = None

    for v in published:
        total_views += int(v.get("view_count") or 0)
        total_downloads += int(v.get("download_count") or 0)
        pa = v.get("published_at") or v.get("created_at")
        ua = v.get("updated_at")
        if pa and (first_published is None or pa < first_published):
            first_published = pa
        if ua and (last_updated is None or ua > last_updated):
            last_updated = ua

    return {
        "success": True,
        "source_id": source_id,
        "view_count": total_views,
        "download_count": total_downloads,
        "version_count": len(published),
        "first_published": first_published,
        "last_updated": last_updated,
    }


@router.get("/status/{source_id}")
async def get_status(
    source_id: str,
    version: Optional[str] = Query(None),
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    if version:
        record = store.get_submission(source_id, version)
        if not record:
            return {"success": False, "error": "Submission not found"}
    else:
        versions = store.list_versions(source_id)
        if not versions:
            return {"success": False, "error": "Submission not found"}
        latest_ver = latest_version(versions)
        record = next((item for item in versions if item.get("version") == latest_ver), versions[-1])

    # If unauthenticated (or not owner/curator), only allow published
    is_privileged = False
    if auth:
        owner_id = record.get("user_id")
        is_privileged = (owner_id and owner_id == auth.user_id) or is_curator(auth)
    if not is_privileged and record.get("status") != "published":
        return {"success": False, "error": "Submission not found"}

    # Inline transfer status check — single Globus API call (~200ms)
    if record.get("transfer_status") == "active":
        _inline_transfer_check(record, store)

    normalized = _normalize_record(record)
    result = {"success": True, "submission": normalized}

    # Include transfer status in response when present
    if record.get("transfer_status"):
        result["transfer"] = {
            "status": record.get("transfer_status"),
            "bytes_transferred": record.get("transfer_bytes_transferred", 0),
            "files_transferred": record.get("transfer_files_transferred", 0),
            "destination": record.get("transfer_destination", ""),
        }

    return result


def _inline_transfer_check(record: Dict[str, Any], store: SubmissionStore) -> None:
    """Check Globus transfer status inline and update the submission record."""
    task_ids = record.get("transfer_task_ids", [])
    if not task_ids:
        return

    all_succeeded = True
    any_failed = False
    total_bytes = 0
    total_files = 0

    for task_id in task_ids:
        try:
            status = check_transfer_status(task_id)
            total_bytes += status.get("bytes_transferred", 0)
            total_files += status.get("files_transferred", 0)

            if status["status"] == "SUCCEEDED":
                continue
            elif status["status"] in ("FAILED", "INACTIVE"):
                any_failed = True
                all_succeeded = False
            else:
                all_succeeded = False
        except Exception:
            logger.debug("Inline transfer check failed for task %s", task_id, exc_info=True)
            all_succeeded = False

    record["transfer_bytes_transferred"] = total_bytes
    record["transfer_files_transferred"] = total_files

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    if all_succeeded or any_failed:
        record["transfer_status"] = "succeeded" if all_succeeded else "failed"
        # Clean up ACL rules
        for acl_id in record.get("transfer_acl_rule_ids", []):
            try:
                cleanup_transfer_acl(acl_id)
            except Exception:
                logger.debug("ACL cleanup failed for %s", acl_id, exc_info=True)
        record["updated_at"] = now
        store.upsert_submission(record)
    else:
        # Still active — persist progress
        record["updated_at"] = now
        store.upsert_submission(record)


@router.get("/status")
async def get_status_all(
    source_id: Optional[str] = Query(None),
    version: Optional[str] = Query(None),
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    if not source_id:
        raise HTTPException(400, "Missing source_id")
    if version:
        record = store.get_submission(source_id, version)
        if not record:
            return {"success": False, "error": "Submission not found"}
    else:
        versions = store.list_versions(source_id)
        if not versions:
            return {"success": False, "error": "Submission not found"}
        latest_ver = latest_version(versions)
        record = next((item for item in versions if item.get("version") == latest_ver), versions[-1])

    # Apply same access control as GET /status/{source_id}
    is_privileged = False
    if auth:
        owner_id = record.get("user_id")
        is_privileged = (owner_id and owner_id == auth.user_id) or is_curator(auth)
    if not is_privileged and record.get("status") != "published":
        return {"success": False, "error": "Submission not found"}

    return {"success": True, "submission": _normalize_record(record)}


@router.post("/status/update")
async def update_status(
    payload: StatusUpdateRequest,
    auth: AuthContext = Depends(get_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    if not is_curator(auth):
        raise HTTPException(403, "Only curators may update submission status")

    ALLOWED_STATUSES = {
        "pending_curation", "approved", "published", "rejected",
    }

    if payload.status not in ALLOWED_STATUSES:
        raise HTTPException(
            400,
            "status must be one of: {}".format(", ".join(sorted(ALLOWED_STATUSES))),
        )

    record = store.get_submission(payload.source_id, payload.version)
    if not record:
        raise HTTPException(404, "Submission not found")
    ensure_submission_owner_or_curator(auth, record)

    store.update_status(payload.source_id, payload.version, payload.status)

    return {
        "success": True,
        "source_id": payload.source_id,
        "version": payload.version,
        "status": payload.status,
    }


@router.get("/submissions")
async def list_submissions(
    organization: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    include_counts: bool = Query(False),
    limit: Optional[int] = Query(50),
    start_key: Optional[str] = Query(None),
    auth: AuthContext = Depends(get_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    user_id = auth.user_id
    if not user_id:
        raise HTTPException(400, "Missing user identity")

    try:
        limit = int(limit) if limit else 50
    except Exception:
        limit = 50

    # Parse status filter
    status_filter = set()
    if status:
        status_filter = {s.strip() for s in status.split(",") if s.strip()}

    # When include_counts is requested, fetch a larger batch to compute counts
    fetch_limit = max(limit, 1000) if include_counts else limit

    parsed_key = parse_pagination_key(start_key) if not include_counts else None

    if organization:
        if not is_curator(auth):
            raise HTTPException(403, "Organization-wide listing requires curator permissions")
        items, last_key = store.list_by_org(organization, limit=fetch_limit, start_key=parsed_key)
    else:
        items, last_key = store.list_by_user(user_id, limit=fetch_limit, start_key=parsed_key)

    for item in items:
        if isinstance(item.get("dataset_mdata"), str):
            try:
                item["dataset_mdata"] = json.loads(item["dataset_mdata"])
            except Exception:
                pass

    # Compute counts before filtering (over all fetched items)
    response: Dict[str, Any] = {"success": True}
    if include_counts:
        counts: Dict[str, int] = {}
        for item in items:
            s = item.get("status", "unknown")
            counts[s] = counts.get(s, 0) + 1
        response["counts"] = counts
        response["total"] = len(items)

    # Apply status filter
    if status_filter:
        items = [item for item in items if item.get("status") in status_filter]

    # Apply pagination limit after filtering
    if include_counts:
        items = items[:limit]
        last_key = None

    response["submissions"] = items
    response["next_key"] = serialize_pagination_key(last_key)
    return response
