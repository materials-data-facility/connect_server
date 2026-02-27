import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query

from v2.async_jobs import enqueue_profile_job, enqueue_transfer_job
from v2.transfer import check_transfer_status, cleanup_transfer_acl
from v2.app.auth import (
    ensure_submission_owner_or_curator,
    get_auth,
    is_curator,
)
from v2.app.deps import get_submission_store
from v2.app.models import AuthContext, StatusUpdateRequest
from v2.config import DEFAULT_ORGANIZATION
from v2.metadata import DatasetMetadata, migrate_v1_payload
from v2.store import SubmissionStore, parse_pagination_key, serialize_pagination_key
from v2.submission_utils import generate_source_id, increment_version, latest_version

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


@router.post("/submit")
async def submit(
    metadata: dict,
    auth: AuthContext = Depends(get_auth),
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

    if not flat.get("data_sources") and not metadata.get("update_metadata_only"):
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
    version = increment_version(latest_ver) if update else "1.0"

    if update and not latest_ver:
        raise HTTPException(400, "Update requested but no prior submission found")

    versioned_source_id = "{}-{}".format(source_id, version)

    # Propagate dataset_doi from prior published versions
    inherited_dataset_doi = None
    previous_version_id = None
    root_version_id = None
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
        raise HTTPException(500, str(exc))

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


@router.get("/status/{source_id}")
async def get_status(
    source_id: str,
    version: Optional[str] = Query(None),
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
    store: SubmissionStore = Depends(get_submission_store),
):
    if not source_id:
        raise HTTPException(400, "Missing source_id")
    # Delegate to the same logic
    if version:
        record = store.get_submission(source_id, version)
        if not record:
            return {"success": False, "error": "Submission not found"}
        return {"success": True, "submission": _normalize_record(record)}
    versions = store.list_versions(source_id)
    if not versions:
        return {"success": False, "error": "Submission not found"}
    latest_ver = latest_version(versions)
    latest = next((item for item in versions if item.get("version") == latest_ver), versions[-1])
    return {"success": True, "submission": _normalize_record(latest)}


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

    parsed_key = parse_pagination_key(start_key)

    if organization:
        if not is_curator(auth):
            raise HTTPException(403, "Organization-wide listing requires curator permissions")
        items, last_key = store.list_by_org(organization, limit=limit, start_key=parsed_key)
    else:
        items, last_key = store.list_by_user(user_id, limit=limit, start_key=parsed_key)

    for item in items:
        if isinstance(item.get("dataset_mdata"), str):
            try:
                item["dataset_mdata"] = json.loads(item["dataset_mdata"])
            except Exception:
                pass

    return {
        "success": True,
        "submissions": items,
        "next_key": serialize_pagination_key(last_key),
    }
