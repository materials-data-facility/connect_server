import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query

from v2.async_jobs import dispatch_publish_job, enqueue_profile_job, enqueue_transfer_job
from v2.dataset_card import build_dataset_card
from v2.transfer import check_transfer_status, cleanup_transfer_acl
from v2.app.auth import (
    can_view_dataset,
    ensure_submission_owner_or_curator,
    get_auth,
    get_optional_auth,
    is_curator,
    is_submission_owner_or_curator,
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
from v2.store import (
    PublishLockUnavailable,
    SubmissionStore,
    parse_pagination_key,
    publish_lock,
    serialize_pagination_key,
)
from v2.submission_utils import (
    DEFAULT_ACL,
    DEPRECATED_EXTENSION_ALIASES,
    deep_merge,
    generate_source_id,
    increment_version,
    latest_version,
    record_previous_version,
    record_root_version,
    reserved_extension_keys,
    resolve_record_acl,
    validate_source_id,
    validate_source_id_lenient,
)

logger = logging.getLogger(__name__)

router = APIRouter()

MAX_SUBMIT_METADATA_BYTES = int(os.environ.get("MAX_SUBMIT_METADATA_BYTES", "262144"))
MAX_SUBMIT_DATA_SOURCES = int(os.environ.get("MAX_SUBMIT_DATA_SOURCES", "2000"))
MAX_SUBMIT_AUTHORS = int(os.environ.get("MAX_SUBMIT_AUTHORS", "1000"))

DATA_LOCATION_EDIT_FIELDS = frozenset({"data_sources", "download_url", "external"})

# GET /submissions?include_counts=true computes status counts over a single
# large batch and then pages that batch in-process; the cursor it hands back
# carries an offset into the batch under this key.
COUNTS_SCAN_LIMIT = int(os.environ.get("SUBMISSIONS_COUNTS_SCAN_LIMIT", "1000"))
COUNTS_CURSOR_FIELD = "counts_offset"


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


def _submitted_extensions(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """The ``extensions`` blob of a submit payload ({} when absent/malformed)."""
    ext = metadata.get("extensions")
    return ext if isinstance(ext, dict) else {}


def _identity_from_metadata(
    metadata: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str], List[str]]:
    """Extract ``(source_id, source_name, deprecated_keys_used)`` from a payload.

    Dataset identity is a system attribute, not user metadata (N4): it decides
    the partition key of the record and the search facet grouping key, so it is
    read from a dedicated top-level field instead of the user-editable
    ``extensions`` blob. Preference order:

      1. top-level ``source_id`` / ``source_name`` — the v2.1 contract
      2. v1 ``mdf.source_id`` / ``mdf.source_name`` — auto-migrated payloads
      3. ``extensions.mdf_source_id`` / ``extensions.mdf_source_name`` —
         DEPRECATED; still accepted so released CLIs keep working, copied to the
         top level and stripped from what gets stored.
    """
    deprecated: List[str] = []

    source_id = metadata.get("source_id") or None
    source_name = metadata.get("source_name") or None

    mdf = metadata.get("mdf")
    mdf = mdf if isinstance(mdf, dict) else {}
    source_id = source_id or mdf.get("source_id") or mdf.get("source_name")
    source_name = source_name or mdf.get("source_name")

    ext = _submitted_extensions(metadata)
    for key, target in DEPRECATED_EXTENSION_ALIASES.items():
        value = ext.get(key)
        if not value:
            continue
        deprecated.append(key)
        if target == "source_id" and not source_id:
            source_id = value
        elif target == "source_name" and not source_name:
            source_name = value
    # mdf_source_name doubles as a source_id of last resort (v1 promoted
    # source_name to the canonical identity during migration).
    if not source_id:
        source_id = ext.get("mdf_source_name") or None

    return (
        str(source_id) if source_id else None,
        str(source_name) if source_name else None,
        deprecated,
    )


def _reject_reserved_extensions(
    metadata: Dict[str, Any], allow_deprecated: bool = True
) -> None:
    """400 on ``mdf_*`` keys in ``extensions``.

    ``extensions`` is user metadata and is deep-merged wholesale on edit, so any
    system attribute living there is writable by the submitter. The two
    historical identity keys are grandfathered on submit only (they are copied
    to the top level and stripped); everything else namespaced ``mdf_*`` is
    reserved and rejected outright.
    """
    reserved = reserved_extension_keys(_submitted_extensions(metadata))
    if allow_deprecated:
        reserved = [key for key in reserved if key not in DEPRECATED_EXTENSION_ALIASES]
    if reserved:
        raise HTTPException(
            400,
            "extensions keys {} are reserved for MDF system attributes; "
            "use the top-level source_id/source_name fields instead".format(
                ", ".join(repr(key) for key in reserved)
            ),
        )


def _validate_source_id_path(source_id: str) -> str:
    """Validate a route source ID without disclosing its grammar.

    Deliberately LENIENT: this id addresses an existing record, and the live
    corpus predates the strict grammar (uppercase, non-ASCII and >64-character
    ids all exist). Holding path parameters to the strict grammar would 404
    those datasets. Only genuinely unsafe shapes — path separators, control
    characters, ``..`` traversal — are rejected, and as a 404 so the endpoint
    reveals nothing about the id space.
    """
    try:
        return validate_source_id_lenient(source_id)
    except ValueError:
        raise HTTPException(404, "Submission not found")


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


def _publish_via_job(
    store: SubmissionStore,
    source_id: str,
    version: str,
    mint_doi: bool = False,
) -> Dict[str, Any]:
    """Hand a record to the publish job and report the status it reached.

    The publish job owns the transition to "published": it handles the DOI and
    ingests into Globus Search *before* flipping the status, so a failure can
    never leave a published version the search index never saw. With inline
    dispatch that happens synchronously (and a failure raises a 502); with a
    queue the record stays "approved" until the worker runs.
    """
    publish_job = dispatch_publish_job(source_id, version, mint_doi=mint_doi)
    status = "approved"
    if not publish_job.get("queued"):
        refreshed = store.get_submission(source_id, version)
        if refreshed:
            status = refreshed.get("status", status)
    return {"publish_job": publish_job, "status": status}


def _version_sort_key(record: Dict[str, Any]):
    """Numeric-aware ordering key for a submission version record.

    Mirrors ``submission_utils.latest_version`` (per-component numeric sort so
    "10.0" > "2.0"), with created_at as a stable tie-breaker.
    """
    parts = []
    for part in str(record.get("version") or "0").split("."):
        # Numeric components sort before/among themselves; non-numeric ones
        # sort after, compared as strings. Uniform tuple shape keeps the
        # comparison type-safe.
        parts.append((0, int(part), "") if part.isdigit() else (1, 0, part))
    return (parts, record.get("created_at") or "")


def root_version_record(existing_versions: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return the earliest (root) version record of a dataset's version chain.

    ``store.list_versions()`` makes no ordering guarantee (the SQLite backend
    issues a bare SELECT; DynamoDB orders by the raw string sort key), so the
    caller must sort explicitly rather than trusting list position.
    """
    records = [v for v in existing_versions if v]
    if not records:
        return None
    return sorted(records, key=_version_sort_key)[0]


def ensure_dataset_update_permitted(
    auth: AuthContext,
    existing_versions: List[Dict[str, Any]],
    source_id: str,
) -> None:
    """Only a dataset's original submitter (or a curator) may add a version to it.

    Without this check any member of the submitters group could hijack someone
    else's dataset by submitting update=true with their source_id, which also
    flips latest=false on the real owner's current version.

    Ownership is derived from the ROOT (earliest) version, not the latest one:
    both this route and /stream/{id}/snapshot stamp the *updating* user's
    user_id onto each new version record, so a curator legitimately updating
    someone else's dataset would otherwise (a) lock the original submitter out
    of their own dataset and (b) keep permanent write access for themselves
    after losing curator status.

    Migrated v1 datasets have root user_id="v1-migration", so they are
    curator-only for updates. That is intended.
    """
    root = root_version_record(existing_versions)
    if root is None:
        # Brand-new dataset (no prior versions) — nothing to protect.
        return

    owner_id = root.get("user_id")
    if owner_id and owner_id == auth.user_id:
        return
    if is_curator(auth):
        return

    logger.warning(
        "Blocked update to source_id=%s by non-owner user_id=%s (dataset owner=%s)",
        source_id,
        auth.user_id,
        owner_id,
    )
    raise HTTPException(
        403,
        "You do not have permission to submit a new version of this dataset; "
        "only the original submitter or a curator may update it",
    )


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


def _can_access_submission(auth: Optional[AuthContext], record: Dict[str, Any]) -> bool:
    if not record:
        return False
    if is_submission_owner_or_curator(auth, record):
        return True
    return can_view_dataset(auth, record)


def _is_privileged(auth: Optional[AuthContext], record: Dict[str, Any]) -> bool:
    """True when the caller is the submission's owner or a curator.

    The distinction that matters for reads: a *privileged* caller sees the raw
    record, while a caller who merely satisfies ``_can_access_submission``
    (i.e. the dataset happens to be published) gets the sanitized view.
    """
    return is_submission_owner_or_curator(auth, record)


# Submitter PII, curation internals and transfer plumbing. A caller who can only
# see a record because it is *published* has no business reading any of these.
_SENSITIVE_RECORD_FIELDS = frozenset({
    "user_id", "user_email",
    "curation_history",
    "approved_by", "approved_at",
    "rejected_by", "rejected_at", "rejection_reason",
    "deleted_by", "deleted_at",
    "reviewer", "reviewed_by",
    "publish_error", "publish_error_at",
    "transfer_status", "transfer_destination", "transfer_task_ids",
    "transfer_acl_rule_ids", "transfer_bytes_transferred",
    "transfer_files_transferred",
    "metadata_updated_at",
    # Now a top-level record attribute (v2.1 record shape). Serving it would
    # enumerate the Globus identities a restricted dataset is shared with —
    # exactly what the dataset_mdata.acl scrub below has always prevented.
    "acl",
})


def _public_submission_view(record: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitized view of a published submission for a non-owner, non-curator.

    Strips submitter identity/email, the curation history (curator ids, review
    notes and rejection reasons) and the internal transfer bookkeeping. Also
    drops the ACL — both the top-level ``acl`` attribute and the legacy
    ``dataset_mdata.acl`` on un-backfilled rows — which would otherwise
    enumerate the Globus identities a restricted dataset is shared with.
    """
    rec = _normalize_record(dict(record))
    public = {k: v for k, v in rec.items() if k not in _SENSITIVE_RECORD_FIELDS}
    mdata = public.get("dataset_mdata")
    if isinstance(mdata, dict):
        mdata = dict(mdata)
        mdata.pop("acl", None)
        public["dataset_mdata"] = mdata
    if "link_health" in public:
        # Outsiders get the aggregate only (status + checked_at); the per-URL
        # ``checks`` array with upstream error strings is curator-grade detail.
        from v2.link_health import public_link_health

        public["link_health"] = public_link_health(rec)
    return public


class MetadataEditPayload(MetadataEditRequest):
    """Metadata edit body plus the ACL.

    ``acl`` is a top-level record attribute in the v2.1 shape, not dataset
    metadata, so it is declared here rather than on ``MetadataEditRequest``
    (whose remaining fields all deep-merge into ``dataset_mdata``). Owner edits
    to visibility keep working through this same route.
    """

    acl: Optional[List[str]] = None


def _validated_acl(acl: Optional[List[str]]) -> List[str]:
    """Coerce an edited ACL, rejecting shapes that would fail open."""
    if acl is None:
        return list(DEFAULT_ACL)
    entries = [str(entry).strip() for entry in acl if str(entry or "").strip()]
    if not entries:
        raise HTTPException(
            400,
            'acl may not be empty; use ["public"] to make the dataset public',
        )
    if "public" in entries and len(entries) > 1:
        raise HTTPException(
            400,
            'acl may not mix "public" with specific identities; '
            'use ["public"] or a list of Globus identity ids',
        )
    return entries


@router.post("/submissions/{source_id}/metadata")
async def edit_metadata(
    source_id: str,
    payload: MetadataEditPayload,
    auth: AuthContext = Depends(get_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    _validate_source_id_path(source_id)
    submission = _resolve_submission(store, source_id, payload.version)
    ensure_submission_owner_or_curator(auth, submission)

    status = submission.get("status")
    if status not in ("pending_curation", "rejected", "published"):
        if status == "approved":
            # Approved = the publish job is still running (seconds). Say so
            # instead of presenting a dead end.
            raise HTTPException(
                409,
                "This version is still publishing — try again in a few seconds.",
            )
        raise HTTPException(400, f"Cannot edit metadata when status is '{status}'")

    # Build updates from non-None payload fields (excluding version)
    editable_fields = [
        "title", "authors", "description", "keywords", "license", "funding",
        "related_works", "methods", "facility", "fields_of_science", "domains",
        "ml", "geo_locations", "tags", "extensions", "external", "download_url",
        "data_sources", "publisher", "publication_year",
    ]
    updates = payload.model_dump(
        include=set(editable_fields),
        exclude_none=True,
    )

    # acl is a record attribute, never metadata: it must not deep-merge into
    # dataset_mdata (where it used to live) nor show up as a metadata field.
    acl_update = _validated_acl(payload.acl) if payload.acl is not None else None

    if not updates and acl_update is None:
        raise HTTPException(400, "No metadata fields provided")

    if "extensions" in updates:
        # Stricter than submit: an edit may never touch identity, so even the
        # grandfathered mdf_source_id/mdf_source_name aliases are rejected here
        # rather than silently repointing the record's search facet key.
        _reject_reserved_extensions(updates, allow_deprecated=False)

    if "data_sources" in updates:
        data_sources = updates["data_sources"]
        if len(data_sources) > MAX_SUBMIT_DATA_SOURCES:
            raise HTTPException(
                413,
                f"Too many data_sources (max {MAX_SUBMIT_DATA_SOURCES})",
            )
        data_source_errors = _validate_data_sources(data_sources)
        if data_source_errors:
            raise HTTPException(
                400,
                f"Invalid data_sources: {'; '.join(data_source_errors)}",
            )

    existing_mdata = _parse_mdata(submission)
    deep_merge(existing_mdata, updates)

    try:
        merged_serialized = json.dumps(existing_mdata, allow_nan=False)
    except Exception:
        raise HTTPException(400, "Edited metadata may not contain NaN or Infinity")
    if len(merged_serialized.encode("utf-8")) > MAX_SUBMIT_METADATA_BYTES:
        raise HTTPException(
            413,
            f"Submission metadata exceeds {MAX_SUBMIT_METADATA_BYTES} bytes",
        )

    # Re-validate through Pydantic to catch schema errors
    try:
        DatasetMetadata.model_validate(existing_mdata)
    except Exception as exc:
        raise HTTPException(400, f"Invalid metadata after edit: {exc}")

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    version = submission.get("version")

    # Visibility follows the dataset across an edit. Without this, the new
    # version record (whose dataset_mdata no longer carries acl) would fall back
    # to the ["public"] default and silently publish a restricted dataset.
    existing_acl = resolve_record_acl(submission)
    effective_acl = acl_update or existing_acl
    if effective_acl is None:
        raise HTTPException(
            409,
            "Cannot determine this dataset's access control list; "
            "contact support before editing it",
        )
    updated_fields = list(updates.keys()) + (["acl"] if acl_update else [])

    if status == "published":
        # Auto-create a minor version bump
        new_version = increment_version(version, major=False)
        new_versioned_id = "{}-{}".format(source_id, new_version)
        requires_curation = (
            bool(DATA_LOCATION_EDIT_FIELDS.intersection(updates))
            and not is_curator(auth)
        )

        existing_mdata["version"] = new_version
        existing_mdata["latest"] = True
        # Chain pointers are bare version strings on the record, not in the
        # metadata blob (N3). Inherit the root from the version being edited.
        new_previous_version = version
        new_root_version = record_root_version(submission) or "1.0"

        # Created in the pre-publish state, exactly like a curator-approved
        # submission: the publish job refreshes the DOI and the search entry and
        # only then flips the record to "published". Creating it published here
        # would leave a published-but-unindexed version behind whenever the job
        # failed.
        new_record = {
            "source_id": source_id,
            "version": new_version,
            "versioned_source_id": new_versioned_id,
            # Ownership follows the dataset, not the editor. A curator editing
            # someone else's published dataset must not silently become the
            # owner of the new latest version — that would move the dataset out
            # of the submitter's GET /submissions listing and into the curator's,
            # and misattribute it everywhere ownership is displayed. The actor is
            # still recorded below via approved_by.
            "user_id": submission.get("user_id") or auth.user_id,
            "user_email": submission.get("user_email") or auth.user_email,
            "organization": submission.get("organization"),
            "status": "pending_curation" if requires_curation else "approved",
            "dataset_mdata": json.dumps(existing_mdata),
            "schema_version": submission.get("schema_version", "2"),
            "test": submission.get("test", False),
            "created_at": now,
            "updated_at": now,
            "metadata_updated_at": now,
            "acl": effective_acl,
            "source_name": submission.get("source_name") or source_id,
            "root_version": new_root_version,
            "previous_version": new_previous_version,
        }
        legacy_source_id = submission.get("legacy_source_id")
        if legacy_source_id:
            new_record["legacy_source_id"] = legacy_source_id
        if not requires_curation:
            new_record["approved_at"] = now
            new_record["approved_by"] = auth.user_id

        # Inherit dataset_doi
        dataset_doi = submission.get("dataset_doi") or submission.get("doi")
        if dataset_doi:
            new_record["dataset_doi"] = dataset_doi

        # Flip latest=False on the prior version
        _flip_latest_on_prior(store, submission)

        store.put_submission(new_record)

        if requires_curation:
            try:
                notify_curators_new_submission(new_record)
            except Exception:
                logger.warning(
                    "Failed to send new-submission email for %s",
                    source_id,
                    exc_info=True,
                )
            return {
                "success": True,
                "source_id": source_id,
                "version": version,
                "new_version": new_version,
                "status": "pending_curation",
                "message": "Data-location changes require curator approval before publication.",
                "updated_fields": updated_fields,
                "card": build_dataset_card(new_record),
            }

        # Publish the new version (updates DataCite metadata + search entry).
        # Failures surface as a 502 instead of a success response describing a
        # version that never got indexed; the curator can retry with
        # POST /curation/{source_id}/approve on the new version.
        published = _publish_via_job(store, source_id, new_version, mint_doi=False)

        # Hand the caller the finished card so it never has to race a refetch
        # against replication: the record is in hand right here. Built from a
        # consistent re-read because the publish job just stamped status/DOI
        # onto the row.
        fresh = store.get_submission(source_id, new_version) or new_record
        return {
            "success": True,
            "source_id": source_id,
            "version": version,
            "new_version": new_version,
            "status": published["status"],
            "publish_job": published["publish_job"],
            "updated_fields": updated_fields,
            "card": build_dataset_card(fresh),
        }

    # pending_curation or rejected: update in-place
    submission["dataset_mdata"] = json.dumps(existing_mdata)
    submission["acl"] = effective_acl
    submission["updated_at"] = now
    submission["metadata_updated_at"] = now
    store.upsert_submission(submission)

    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "updated_fields": updated_fields,
        "card": build_dataset_card(submission),
    }


@router.post("/submissions/{source_id}/withdraw")
async def withdraw(
    source_id: str,
    payload: WithdrawRequest,
    auth: AuthContext = Depends(get_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    _validate_source_id_path(source_id)
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

    # If this was latest=True, restore latest on prior version. previous_version
    # is a bare version string (N3), so no composite parsing is needed; the
    # reader still normalizes legacy "{source_id}-{version}" values.
    mdata = _parse_mdata(submission)
    prev_version = record_previous_version(submission)
    if mdata.get("latest") and prev_version:
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
    _validate_source_id_path(source_id)
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
    _validate_source_id_path(source_id)
    submission = _resolve_submission(store, source_id, payload.version)
    version = submission.get("version")

    if submission.get("status") == "deleted":
        raise HTTPException(400, "Submission is already deleted")

    # Captured before the status flip: only a version that was actually
    # published can have contributed to the search index.
    was_published = submission.get("status") == "published"

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

    search_reconcile = None
    if was_published:
        search_reconcile = _reconcile_search_after_removal(store, source_id, submission)

    logger.info("Submission deleted source_id=%s version=%s by=%s reason=%s",
                source_id, version, auth.user_id, payload.reason)

    result = {
        "success": True,
        "source_id": source_id,
        "version": version,
        "status": "deleted",
    }
    if search_reconcile:
        result["search_index"] = search_reconcile
    return result


def _reconcile_search_after_removal(
    store: SubmissionStore,
    source_id: str,
    removed: Dict[str, Any],
) -> Dict[str, Any]:
    """Bring the search index back in line after a published version is removed.

    The index holds exactly ONE entry per dataset, keyed on the version-less
    detail-URL subject, and it always describes the latest published version
    (see ``async_jobs._owns_search_entry``). So removal has two cases:

    - other published versions remain → re-ingest the highest remaining one, so
      the entry stops advertising the removed version's metadata;
    - none remain → delete the entry, otherwise the dataset keeps showing up in
      search results and /detail links 404 (bug B-18).

    The store reads and search write share the per-dataset publish lock with the
    async publish worker. A synchronous delete waits for the lock's short,
    bounded default; if it remains contended, reconciliation is skipped because
    the concurrent publish will re-establish the entry. Never raises: the record
    is already updated in the store, and a search outage or lock contention must
    not turn a completed delete into a 500. Failures are logged and reported in
    the response so the caller can retry.
    """
    try:
        from v2.search_client import get_search_client

        client = get_search_client(test_mode=bool(removed.get("test", False)))
        with publish_lock(store, source_id):
            remaining = [
                v for v in (store.list_versions(source_id) or [])
                if v.get("status") == "published"
            ]
            if remaining:
                latest_published = max(remaining, key=_version_sort_key)
                outcome = client.ingest(latest_published, version_count=len(remaining))
                return {
                    "action": "reingested",
                    "version": latest_published.get("version"),
                    "success": bool(outcome.get("success")),
                }
            outcome = client.delete_entry(source_id)
            return {"action": "deleted", "success": bool(outcome.get("success"))}
    except PublishLockUnavailable:
        warning = (
            "Search reconciliation skipped because a concurrent publish holds "
            "the dataset lock; that publish will re-establish the search entry."
        )
        logger.warning(
            "%s source_id=%s removed_version=%s",
            warning, source_id, removed.get("version"),
        )
        return {"action": "skipped", "success": False, "warning": warning}
    except Exception:
        logger.warning(
            "Failed to reconcile the search index after removing %s v%s",
            source_id, removed.get("version"), exc_info=True,
        )
        return {"action": "failed", "success": False}


@router.get("/versions/{source_id}/diff")
async def version_diff(
    source_id: str,
    from_version: str = Query(..., alias="from"),
    to_version: str = Query(..., alias="to"),
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    _validate_source_id_path(source_id)
    from_record = store.get_submission(source_id, from_version)
    if not from_record:
        raise HTTPException(404, f"Version {from_version} not found")
    to_record = store.get_submission(source_id, to_version)
    if not to_record:
        raise HTTPException(404, f"Version {to_version} not found")
    if not _can_access_submission(auth, from_record) or not _can_access_submission(auth, to_record):
        raise HTTPException(404, "Submission not found")

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

    # Required field checks (Pydantic allows empty strings/lists)
    if not (flat.get("title") or "").strip():
        raise HTTPException(400, "title is required and cannot be empty")
    if not flat.get("authors"):
        raise HTTPException(400, "At least one author is required")
    for i, a in enumerate(flat["authors"]):
        if not (a.get("name") or "").strip():
            raise HTTPException(400, f"Author at position {i + 1} has an empty name")

    # Default publication_year to current year if not provided.
    #
    # No local `from datetime import ...` here: rebinding a name that submit()
    # already uses from module scope (line 6) makes it local for the WHOLE
    # function body, so every request that DID supply publication_year skipped
    # this line and then died on the `datetime.now(...)` further down with
    # "UnboundLocalError: cannot access local variable 'datetime'" — a 500 on the
    # commonest submit path, including the frontend's.
    if not flat.get("publication_year"):
        flat["publication_year"] = datetime.now(timezone.utc).year

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
    if is_test:
        test_index_id = os.environ.get("TEST_SEARCH_INDEX_UUID", "").strip()
        if not test_index_id or test_index_id == "not-configured":
            raise HTTPException(
                400,
                "Test submissions are not supported in this environment",
            )
    update = flat.get("update", False)

    _reject_reserved_extensions(metadata)
    source_id, submitted_source_name, deprecated_keys = _identity_from_metadata(metadata)
    if deprecated_keys:
        logger.warning(
            "Submission used deprecated extensions identity keys %s "
            "(user_id=%s); use the top-level source_id/source_name fields",
            ", ".join(deprecated_keys),
            user_id,
        )
    if source_id:
        # Strict grammar for a NEW id, lenient for one that addresses an
        # existing dataset: `update=True` on a pre-grammar legacy id
        # (uppercase / non-ASCII / >64 chars) must keep working.
        validator = (
            validate_source_id_lenient if flat.get("update") else validate_source_id
        )
        try:
            source_id = validator(source_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
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

    if update:
        ensure_dataset_update_permitted(auth, existing_versions, source_id)

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
        # Version chain pointers are BARE version strings (N3): dataset identity
        # is always the (source_id, version) pair, so a pointer only has to name
        # the version. The old "{source_id}-{version}" composite was ambiguous
        # for legacy ids that themselves end in a version-like suffix.
        previous_version_id = latest_ver
        # Root version: inherit from prior, or use the earliest version
        prior_record = next(
            (v for v in existing_versions if v.get("version") == latest_ver), None
        )
        if prior_record:
            root_version_id = record_root_version(prior_record)
        if not root_version_id:
            # Earliest version is the root. Numeric-aware (and shared with the
            # ownership check above) so a chain that reached 10.0 does not
            # suddenly re-root itself onto 10.0 under a string sort.
            earliest = root_version_record(existing_versions) or {}
            root_version_id = earliest.get("version") or "1.0"

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

    # Populate versioning fields in metadata. version/latest stay in the
    # metadata blob (they are part of the published dataset description); the
    # chain pointers are top-level record attributes, set on the record below.
    flat["version"] = version
    flat["latest"] = True
    if not update:
        root_version_id = version

    # ACL is a top-level record attribute (v2.1 shape), not metadata. A submit
    # carries full metadata, so an explicit acl wins; when the caller omits it
    # on an *update* the prior version's visibility is inherited rather than
    # silently reset to public.
    acl = [str(entry) for entry in (flat.get("acl") or []) if entry]
    if not acl and prior_record is not None:
        acl = resolve_record_acl(prior_record) or list(DEFAULT_ACL)
    if not acl:
        acl = list(DEFAULT_ACL)

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    record = {
        "source_id": source_id,
        "version": version,
        "versioned_source_id": versioned_source_id,
        "source_name": submitted_source_name or source_id,
        "root_version": root_version_id,
        "acl": acl,
        "user_id": user_id,
        "user_email": user_email,
        "organization": organization,
        "status": "pending_curation",
        "dataset_mdata": json.dumps(flat),
        "schema_version": "2",
        "test": is_test,
        "created_at": now,
        "updated_at": now,
        "metadata_updated_at": now,
    }
    if previous_version_id:
        record["previous_version"] = previous_version_id
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
    _validate_source_id_path(source_id)
    versions = store.list_versions(source_id)
    if not versions:
        return {"success": False, "error": "No versions found for this source_id"}

    # If unauthenticated (or not owner/curator), only show published versions
    is_privileged = bool(
        auth and (
            is_curator(auth) or any(v.get("user_id") == auth.user_id for v in versions)
        )
    )
    if not is_privileged:
        versions = [v for v in versions if can_view_dataset(auth, v)]
        if not versions:
            # Same body as the no-such-source_id branch above: a hidden
            # dataset must not be distinguishable from a nonexistent one.
            return {"success": False, "error": "No versions found for this source_id"}

    # Numeric-aware: a plain string sort orders 1.0, 10.0, 2.0 and makes the
    # UI's version picker (and the paginated slice below) wrong past v9.
    sorted_versions = sorted(versions, key=_version_sort_key)
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
            # Bare version strings (N3), normalized on read so rows that still
            # carry the legacy "{source_id}-{version}" composite look the same.
            "root_version": record_root_version(v),
            "previous_version": record_previous_version(v),
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
    auth: Optional[AuthContext] = Depends(get_optional_auth),
    store: SubmissionStore = Depends(get_submission_store),
):
    """Public access/download stats for a published dataset."""
    _validate_source_id_path(source_id)
    versions = store.list_versions(source_id)
    published = [v for v in versions if can_view_dataset(auth, v)]
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
    _validate_source_id_path(source_id)
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

    # If unauthenticated (or not owner/curator), only allow viewable records.
    # Mirror the not-found shape above exactly: a hidden record must be
    # indistinguishable from a nonexistent one (no existence oracle).
    if not _can_access_submission(auth, record):
        return {"success": False, "error": "Submission not found"}

    # A caller who reaches this point only because the dataset is published gets
    # the sanitized public view. This also skips the inline transfer check below,
    # so an anonymous caller cannot drive repeated Globus polling and store
    # writes on someone else's submission.
    if not _is_privileged(auth, record):
        return {"success": True, "submission": _public_submission_view(record)}

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
    if not _can_access_submission(auth, record):
        return {"success": False, "error": "Submission not found"}

    if not _is_privileged(auth, record):
        return {"success": True, "submission": _public_submission_view(record)}

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

    if payload.status == "published":
        # "published" is never a bare status write: that would create a record
        # the search index (and DataCite) never heard about. Move it into the
        # pre-publish state and let the publish job perform the transition, the
        # same way POST /curation/{source_id}/approve does. Use the approve
        # endpoint when a DOI should be minted — this path never mints one.
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        record["status"] = "approved"
        record["approved_at"] = now
        record["approved_by"] = auth.user_id
        record["updated_at"] = now
        store.upsert_submission(record)

        published = _publish_via_job(store, payload.source_id, payload.version, mint_doi=False)
        return {
            "success": True,
            "source_id": payload.source_id,
            "version": payload.version,
            "status": published["status"],
            "publish_job": published["publish_job"],
        }

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
    # across the whole listing. That batch is then paginated in-process, so its
    # cursor is a plain offset into the batch rather than a store cursor.
    fetch_limit = max(limit, COUNTS_SCAN_LIMIT) if include_counts else limit

    parsed_key = parse_pagination_key(start_key)
    counts_offset = 0
    if include_counts:
        try:
            counts_offset = max(0, int((parsed_key or {}).get(COUNTS_CURSOR_FIELD) or 0))
        except (TypeError, ValueError):
            counts_offset = 0
        parsed_key = None

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

    # Apply pagination limit after filtering.
    #
    # In counts mode the page is carved out of the in-memory batch, so the
    # cursor has to be an offset into that batch. Previously this branch threw
    # ``last_key`` away unconditionally, which is why GET /submissions answered
    # ``next_key: null`` even when ``limit`` had clearly truncated the result
    # (limit=2 over 19 rows) — page 2 was unreachable.
    if include_counts:
        page = items[counts_offset:counts_offset + limit]
        has_more = len(items) > counts_offset + limit
        items = page
        last_key = (
            {COUNTS_CURSOR_FIELD: counts_offset + limit} if has_more else None
        )

    response["submissions"] = items
    response["next_key"] = serialize_pagination_key(last_key)
    return response
