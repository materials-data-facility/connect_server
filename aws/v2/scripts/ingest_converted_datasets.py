#!/usr/bin/env python3
"""Ingest converted MDF production datasets into DynamoDB and Globus Search.

Reads converted_datasets.json (output of convert_production_datasets.py),
builds proper v2 submission records, and loads them into both the submission
store and the search index.

Supports:
  - Auto-resolve config from a deployed stack (--env prod)
  - Local mode (SQLite + MockSearch) for testing
  - Dry-run mode to validate without writing
  - Content-hash sync: creates, updates, skips unchanged, and reports conflicts
  - Safe watermark advancement after a complete successful store run

Usage:
    # Dry run — validate all records, write nothing
    cd cs/aws
    PYTHONPATH=. python v2/scripts/ingest_converted_datasets.py --dry-run

    # Local SQLite + mock search
    PYTHONPATH=. STORE_BACKEND=sqlite USE_MOCK_SEARCH=true \
        python v2/scripts/ingest_converted_datasets.py

    # Production — auto-resolve config from deployed stack
    PYTHONPATH=. python v2/scripts/ingest_converted_datasets.py --env prod

    # Staging — DynamoDB only, skip search
    PYTHONPATH=. python v2/scripts/ingest_converted_datasets.py --env staging --skip-search

    # Advance a bridge watermark only after this full ingest succeeds
    PYTHONPATH=. python v2/scripts/ingest_converted_datasets.py \
        --extract-file datasets.json --watermark-file watermark.txt

Watermark candidate precedence is: ``--candidate-watermark``, the direct input
JSON's top-level ``candidate_watermark``, then ``--extract-file``. Advancement
is disabled for dry runs, limited runs, skipped/mock/pending Search, search-only
runs, errors, or conflicts.

Exit codes:
  0 = clean success, or any valid --dry-run preview
  1 = validation, store, Search, or watermark-write error
  2 = invalid command-line invocation (argparse)
  3 = partial conversion or otherwise clean run stalled on dataset conflicts
"""

import argparse
import copy
import hashlib
import json
import os
import random
import re
import socket
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Allow imports from aws/ root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# v2 modules are imported lazily (after env vars are resolved) because
# v2.config reads DYNAMO_SUBMISSIONS_TABLE etc. at import time.

# Environment variables the ingestion script needs from the Lambda config.
_RELEVANT_ENV_KEYS = {
    "STORE_BACKEND",
    "DYNAMO_SUBMISSIONS_TABLE",
    "SEARCH_INDEX_UUID",
    "TEST_SEARCH_INDEX_UUID",
    "USE_MOCK_SEARCH",
    "GLOBUS_CLIENT_ID",
    "GLOBUS_CLIENT_SECRET",
    "AWS_REGION",
    # Needed so --backfill-embeddings can enqueue the rebuild onto the live
    # async queue that the deployed worker consumes.
    "ASYNC_DISPATCH_MODE",
    "ASYNC_QUEUE_URL",
}

REGION = "us-east-1"

# Legacy production Globus Search index (v1). The v1 -> v2 migration is
# one-way; nothing this script does may ever write to this index.
LEGACY_SEARCH_INDEX_UUID = "1a57bbe5-5272-477f-9d31-343b8258b7a5"

MIGRATION_USER_ID = "v1-migration"

# These fields reflect v2-side activity or derived data and must not be erased
# when fresh v1 metadata is synchronized into an existing migration record.
_PRESERVED_SYSTEM_FIELDS = {
    "created_at",
    "published_at",
    "status",
    # Ownership can be granted manually after migration (v1 records carry no
    # submitter identity); a later sync update must not revert it.
    "user_id",
    "view_count",
    "download_count",
    "action_id",
    "transfer_status",
    "transfer_tasks",
    "transfer_task_ids",
    "dataset_profile",
    "curation_history",
    "approved_at",
    "approved_by",
    "rejected_at",
    "rejected_by",
    "rejection_reason",
}

_VOLATILE_HASH_METADATA_FIELDS = {
    # These are computed relative to the converter's current batch. They are
    # not v1 source content, and may differ between full and delta conversion.
    "latest",
    "previous_version",
    "root_version",
}

MAX_SEARCH_ATTEMPTS = 3


def resolve_env_from_stack(env: str) -> Dict[str, str]:
    """Read the deployed Lambda's environment variables from CloudFormation.

    Looks up the ApiFunction in stack mdf-connect-v2-{env}, reads its
    resolved environment, and returns the subset relevant to ingestion.
    This gives us DYNAMO_SUBMISSIONS_TABLE, SEARCH_INDEX_UUID,
    GLOBUS_CLIENT_ID, GLOBUS_CLIENT_SECRET, etc. — exactly matching
    what the live backend uses.
    """
    import boto3

    stack_name = f"mdf-connect-v2-{env}"

    cf = boto3.client("cloudformation", region_name=REGION)
    try:
        resp = cf.describe_stack_resources(StackName=stack_name)
    except cf.exceptions.ClientError as exc:
        raise SystemExit(
            f"Stack '{stack_name}' not found in {REGION}. "
            f"Deploy first with: ./deploy.sh {env}"
        ) from exc

    api_func = None
    for r in resp["StackResources"]:
        if r["LogicalResourceId"] == "ApiFunction":
            api_func = r["PhysicalResourceId"]
            break
    if not api_func:
        raise SystemExit(f"ApiFunction not found in stack {stack_name}")

    lam = boto3.client("lambda", region_name=REGION)
    config = lam.get_function_configuration(FunctionName=api_func)
    lambda_env = config.get("Environment", {}).get("Variables", {})

    resolved = {}
    for key in _RELEVANT_ENV_KEYS:
        if key in lambda_env:
            resolved[key] = lambda_env[key]

    # The Lambda always runs with dynamo store
    resolved.setdefault("STORE_BACKEND", "dynamo")

    return resolved


def apply_env(resolved: Dict[str, str]) -> None:
    """Set resolved config as environment variables (without overriding explicit overrides)."""
    for key, value in resolved.items():
        if key not in os.environ:
            os.environ[key] = value


def guard_against_legacy_index() -> None:
    """Hard-fail if the effective SEARCH_INDEX_UUID is the legacy v1 index.

    Must run after config resolution (resolve_env_from_stack / apply_env,
    and/or pre-set env vars) and before any write to the store or search
    index. Runs in every mode, including --dry-run, since a mis-set
    SearchIndexUUID stack parameter or env var is a config error regardless
    of whether the run actually writes.
    """
    effective = os.environ.get("SEARCH_INDEX_UUID")
    if effective == LEGACY_SEARCH_INDEX_UUID:
        print(
            f"\nFATAL: SEARCH_INDEX_UUID resolved to {effective!r}, which is "
            "the LEGACY v1 production Globus Search index.\n"
            "The v1 -> v2 migration is one-way and must NEVER write to the "
            "legacy index. Refusing to continue.\n"
            "Check the SearchIndexUUID CloudFormation stack parameter and/or "
            "the SEARCH_INDEX_UUID environment variable for this run."
        )
        sys.exit(1)


def compute_sync_content_hash(converted: Dict[str, Any]) -> str:
    """Return the canonical SHA-256 content identity for a converted record.

    The hash covers every top-level converted-record field and every metadata
    field except converter batch-relative chain fields: ``latest``,
    ``previous_version``, and ``root_version``. Excluding them makes full and
    delta conversions of the same v1 source content hash identically.
    """
    hash_input = copy.deepcopy(converted)
    metadata = hash_input.get("metadata")
    if isinstance(metadata, dict):
        for field in _VOLATILE_HASH_METADATA_FIELDS:
            metadata.pop(field, None)
    canonical = json.dumps(hash_input, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_submission_record(converted: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a converted_datasets.json record into a v2 submission record.

    The converted record has:
        source_id, source_name, version (int), ingest_date, doi, endpoint_path,
        metadata (flat v2 dict)

    The submission record needs:
        source_id, version (str), versioned_source_id, user_id, user_email,
        organization, status, dataset_mdata (JSON str), schema_version, test,
        created_at, updated_at, published_at, doi, dataset_doi,
        sync_content_hash
    """
    # Hash the source exactly as loaded, before enriching the submission-only
    # metadata copy with endpoint/legacy traceability fields.
    sync_content_hash = compute_sync_content_hash(converted)
    metadata = copy.deepcopy(converted["metadata"])

    # Populate data_sources from endpoint_path (not present in converted metadata)
    endpoint_path = converted.get("endpoint_path")
    if endpoint_path and not metadata.get("data_sources"):
        metadata["data_sources"] = [endpoint_path]

    version_str = metadata.get("version", "1.0")
    doi = converted.get("doi") or None
    ingest_date = converted.get("ingest_date", "")

    organization = metadata.get("organization") or ""

    # Store legacy_source_id in extensions for traceability
    legacy_source_id = converted.get("legacy_source_id")
    if legacy_source_id:
        metadata.setdefault("extensions", {})["legacy_source_id"] = legacy_source_id

    record = {
        "source_id": converted["source_id"],
        "version": version_str,
        "versioned_source_id": f"{converted['source_id']}-{version_str}",
        "user_id": MIGRATION_USER_ID,
        "status": "published",
        "dataset_mdata": json.dumps(metadata),
        "schema_version": "2",
        "test": False,
        "created_at": ingest_date,
        "updated_at": ingest_date,
        "published_at": ingest_date,
        "sync_content_hash": sync_content_hash,
    }

    # Top-level legacy_source_id powers the legacy-source-id GSI so old v1 ids
    # resolve to this record. Omit when empty/equal — DynamoDB rejects empty
    # strings as GSI key attributes and a self-redirect is pointless.
    if legacy_source_id and legacy_source_id != converted["source_id"]:
        record["legacy_source_id"] = legacy_source_id

    # DynamoDB rejects empty strings for GSI key attributes.
    # organization is the partition key of the org-submissions GSI,
    # so omit it when empty (item won't appear in that index).
    if organization:
        record["organization"] = organization

    if doi:
        record["doi"] = doi
        record["dataset_doi"] = doi

    return record


def validate_record(converted: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate that a converted record can parse into DatasetMetadata.

    Returns (ok, error_message).
    """
    from v2.metadata import DatasetMetadata

    try:
        metadata = dict(converted["metadata"])
        if converted.get("endpoint_path") and not metadata.get("data_sources"):
            metadata["data_sources"] = [converted["endpoint_path"]]
        DatasetMetadata.model_validate(metadata)
        return True, ""
    except Exception as exc:
        return False, str(exc)


SEARCH_BATCH_SIZE = 100  # GMetaList entries per request; well under 10MB limit


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _version_sort_key(version: Any) -> List[Tuple[int, int, str]]:
    """Numeric-aware version ordering ("10.0" sorts after "2.0")."""
    parts = []
    for part in str(version or "0").split("."):
        parts.append((0, int(part), "") if part.isdigit() else (1, 0, part))
    return parts


def _latest_published(
    versions: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Return the store-derived newest published record in a dataset chain."""
    published = [
        record for record in versions
        if record and record.get("status") == "published"
    ]
    if not published:
        return None
    return max(
        published,
        key=lambda record: (
            _version_sort_key(record.get("version")),
            record.get("created_at") or "",
        ),
    )


def _dataset_v2_touch_reasons(
    versions: List[Dict[str, Any]],
) -> List[str]:
    """Describe v2 activity anywhere in a stored dataset version chain."""
    reasons = []
    for record in versions:
        version = str(record.get("version") or "?")
        if record.get("user_id") != MIGRATION_USER_ID:
            reasons.append(
                "version {} user_id={!r}".format(
                    version, record.get("user_id")
                )
            )
        if record.get("metadata_updated_at"):
            reasons.append(
                "version {} metadata_updated_at={!r}".format(
                    version, record.get("metadata_updated_at")
                )
            )
    return reasons


def _merge_migration_update(
    existing: Dict[str, Any],
    submission: Dict[str, Any],
    merge_warnings: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Replace migration-owned content while preserving v2 system state."""
    merged = dict(existing)
    merged.update(submission)
    for field in _PRESERVED_SYSTEM_FIELDS:
        if field in existing and existing[field] is not None:
            merged[field] = existing[field]
    existing_metadata = existing.get("dataset_mdata") or {}
    incoming_metadata = submission.get("dataset_mdata") or {}
    metadata_parse_failed = False
    for label, value in (
        ("stored", existing_metadata),
        ("incoming", incoming_metadata),
    ):
        if not isinstance(value, str):
            continue
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            metadata_parse_failed = True
            if merge_warnings is not None:
                merge_warnings.append(
                    {
                        "source_id": str(submission.get("source_id") or ""),
                        "error": "could not parse {} dataset_mdata; skipped "
                        "chain preservation: {}".format(label, exc),
                    }
                )
            break
        if label == "stored":
            existing_metadata = parsed
        else:
            incoming_metadata = parsed
    if (
        not metadata_parse_failed
        and isinstance(existing_metadata, dict)
        and isinstance(incoming_metadata, dict)
    ):
        for field in ("previous_version", "root_version"):
            incoming_value = incoming_metadata.get(field)
            existing_value = existing_metadata.get(field)
            if incoming_value in (None, "", [], {}) and existing_value not in (
                None, "", [], {}
            ):
                incoming_metadata[field] = existing_value
        merged["dataset_mdata"] = json.dumps(incoming_metadata)
    # An updated v1 payload invalidates embeddings derived from the old title
    # and description. Do not set metadata_updated_at: that field marks v2
    # user edits and would turn the migration's own update into a conflict.
    for field in (
        "title_description_embedding",
        "embedding_model",
        "embedding_generated_at",
    ):
        merged.pop(field, None)
    return merged


def _metadata_dict(value: Any) -> Optional[Dict[str, Any]]:
    """Return dataset metadata as a dict without raising on legacy rows."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _chain_fields_need_repair(
    existing: Dict[str, Any], submission: Dict[str, Any]
) -> bool:
    """Return whether meaningful incoming chain fields differ in storage."""
    incoming = _metadata_dict(submission.get("dataset_mdata"))
    if incoming is None:
        return False
    stored = _metadata_dict(existing.get("dataset_mdata"))
    for field in ("previous_version", "root_version"):
        incoming_value = incoming.get(field)
        if incoming_value not in (None, "", [], {}) and (
            stored is None or stored.get(field) != incoming_value
        ):
            return True
    return False


def _http_status(exc: Exception) -> Optional[int]:
    """Return an HTTP status carried by a common SDK exception shape."""
    for attr in ("http_status", "status_code"):
        status = getattr(exc, attr, None)
        if status is not None:
            try:
                return int(status)
            except (TypeError, ValueError):
                pass
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        status = response.get("status_code") or response.get("status")
    else:
        status = getattr(response, "status_code", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _is_transient_error(exc: Exception) -> bool:
    status = _http_status(exc)
    if status is not None:
        return 500 <= status <= 599
    if isinstance(exc, (TimeoutError, ConnectionError, socket.timeout)):
        return True
    reason = getattr(exc, "reason", None)
    return isinstance(reason, (TimeoutError, ConnectionError, socket.timeout))


def _transient_error_text(value: Any) -> bool:
    """Recognize transport/5xx failures returned as batch error strings."""
    text = str(value or "").lower()
    status_match = re.search(
        r"\b(?:http|status)\D{0,5}(\d{3})\b", text, re.IGNORECASE
    )
    if status_match:
        return 500 <= int(status_match.group(1)) <= 599
    return any(
        marker in text
        for marker in (
            "timed out", "timeout", "connection reset", "connection refused",
            "connection aborted", "connection error", "broken pipe",
            "network unreachable",
        )
    )


def _batch_result_is_transient_failure(result: Dict[str, Any]) -> bool:
    errors = result.get("errors") or []
    if not errors or int(result.get("ingested") or 0) != 0:
        return False
    messages = [
        error.get("error") if isinstance(error, dict) else error
        for error in errors
    ]
    return bool(messages) and all(_transient_error_text(msg) for msg in messages)


def _batch_ingest_with_retry(
    search: Any, submissions: List[Dict[str, Any]], attempts: int = MAX_SEARCH_ATTEMPTS
) -> Dict[str, Any]:
    """Retry one Search batch on transport/5xx failures, never on 4xx."""
    for attempt in range(1, attempts + 1):
        try:
            result = search.batch_ingest(
                submissions, batch_size=SEARCH_BATCH_SIZE
            )
        except Exception as exc:
            if attempt == attempts or not _is_transient_error(exc):
                raise
            result = None
            detail = str(exc)
        else:
            if not _batch_result_is_transient_failure(result) or attempt == attempts:
                return result
            detail = "; ".join(
                str(error.get("error") if isinstance(error, dict) else error)
                for error in result.get("errors") or []
            )
        delay = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
        print(
            "  Search batch failed transiently (attempt {}/{}): {}; "
            "retrying in {:.2f}s".format(attempt, attempts, detail, delay),
            file=sys.stderr,
        )
        time.sleep(delay)
    raise AssertionError("unreachable")


def _stamp_search_sync(
    store: Any, source_id: str, version: str, sync_hash: Optional[str]
) -> None:
    """Atomically stamp only Search sync fields without replacing the row."""
    synced_at = _utc_now()
    table = getattr(store, "table", None)
    if table is not None:
        table.update_item(
            Key={"source_id": source_id, "version": version},
            UpdateExpression=(
                "SET search_synced_hash = :sync_hash, "
                "last_synced_at = :synced_at"
            ),
            ExpressionAttributeValues={
                ":sync_hash": sync_hash,
                ":synced_at": synced_at,
            },
            ConditionExpression=(
                "attribute_exists(source_id) AND attribute_exists(version)"
            ),
        )
        return
    conn = getattr(store, "conn", None)
    if conn is not None:
        with conn:
            cursor = conn.execute(
                "UPDATE submissions SET search_synced_hash = ?, "
                "last_synced_at = ? WHERE source_id = ? AND version = ?",
                (sync_hash, synced_at, source_id, version),
            )
        if cursor.rowcount != 1:
            raise RuntimeError("record disappeared before Search sync stamp")
        return
    raise TypeError("store backend does not support targeted Search sync stamps")


def _prepare_migration_write(
    submission: Dict[str, Any],
    existing: Optional[Dict[str, Any]] = None,
    merge_warnings: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Prepare one atomic store write for a create or migration update."""
    prepared = (
        _merge_migration_update(existing, submission, merge_warnings)
        if existing is not None
        else dict(submission)
    )
    now = _utc_now()
    prepared["updated_at"] = now
    prepared["last_synced_at"] = now
    return prepared


def _is_real_search_client(search: Any) -> bool:
    """Return whether the resolved writer is the real Globus client."""
    if os.environ.get("USE_MOCK_SEARCH", "true").lower() == "true":
        return False
    return (
        search is not None
        and search.__class__.__name__ == "GlobusSearchClient"
        and search.__class__.__module__ == "v2.search_client"
    )


def _parse_watermark(value: str) -> Optional[datetime]:
    """Parse an ISO watermark using Python 3.7-compatible handling."""
    if not value:
        return None
    candidate = str(value).strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    parsed = None
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(candidate, fmt)
                break
            except ValueError:
                continue
    if parsed is not None and parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _resolve_candidate_watermark(
    args: argparse.Namespace,
    input_data: Dict[str, Any],
) -> Optional[str]:
    """Resolve and validate the candidate watermark from the supported inputs."""
    raw = args.candidate_watermark or input_data.get("candidate_watermark")
    if not raw and args.extract_file:
        extract_path = os.path.abspath(args.extract_file)
        with open(extract_path) as f:
            extract_data = json.load(f)
        raw = extract_data.get("candidate_watermark")

    if not raw:
        return None
    parsed = _parse_watermark(str(raw))
    if parsed is None:
        raise ValueError("could not parse candidate watermark {!r}".format(raw))
    return parsed.isoformat()


def _write_watermark(path: str, value: str) -> None:
    """Atomically replace a watermark file with a validated ISO timestamp."""
    absolute = os.path.abspath(path)
    directory = os.path.dirname(absolute) or "."
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=directory,
            prefix=".watermark-",
            delete=False,
        ) as temp_file:
            temp_path = temp_file.name
            temp_file.write(value)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, absolute)
        temp_path = None
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)


def _watermark_blocked_reasons(
    args: argparse.Namespace,
    candidate_watermark: Optional[str],
    stats: Dict[str, Any],
    has_errors: bool,
) -> List[str]:
    """Return every reason a migration watermark cannot safely advance."""
    reasons = []
    if not candidate_watermark:
        reasons.append("no candidate watermark was supplied")
    if args.dry_run:
        reasons.append("dry run")
    if args.limit > 0:
        reasons.append("--limit was used")
    if args.skip_store:
        reasons.append("--skip-store was used")
    if args.skip_search:
        reasons.append("--skip-search was used")
    if has_errors:
        reasons.append("the run had errors")
    if stats["conflicts"]:
        reasons.append(
            "{} unresolved conflict(s)".format(stats["conflicts"])
        )
    if (
        stats["search_required"]
        and not args.skip_search
        and not args.dry_run
        and stats["search_client_is_real"] is not True
    ):
        reasons.append(
            "records needed Search ingestion but the resolved client "
            "was not real Globus Search"
        )
    if stats["search_pending"]:
        reasons.append(
            "{} Search-pending dataset(s) remain".format(
                stats["search_pending"]
            )
        )
    return reasons


def _result_exit_code(
    *, dry_run: bool, has_errors: bool, conflicts: int
) -> int:
    """Map a completed run to its documented exit code."""
    if dry_run:
        return 0
    if has_errors:
        return 1
    if conflicts:
        return 3
    return 0


def ingest_records(
    records: List[Dict[str, Any]],
    *,
    dry_run: bool = False,
    skip_search: bool = False,
    skip_store: bool = False,
) -> Dict[str, Any]:
    """Ingest converted records into the submission store and search index.

    Classification is content-hash based, but conflict ownership and Search
    selection are dataset-chain based. Search receives only the true newest
    published store record, never a batch-relative ``metadata.latest`` choice.

    Returns a summary dict with counts.
    """
    from v2.store import get_store
    from v2.search_client import get_search_client

    # Even dry-run and --skip-store need read access: changeset classification,
    # dataset-level conflict detection, and latest selection are store-derived.
    store = get_store()
    search = None if dry_run or skip_search else get_search_client()

    stats = {
        "total": len(records),
        "validated": 0,
        "validation_errors": [],
        "created": 0,
        "updated": 0,
        "unchanged": 0,
        "conflicts": 0,
        "conflict_records": [],
        "store_errors": [],
        "metadata_merge_warnings": [],
        "search_ingested": 0,
        "search_errors": [],
        "would_search_ingest": 0,
        "search_required": 0,
        "search_client_is_real": (
            _is_real_search_client(search) if search is not None else None
        ),
        "search_pending": 0,
        "search_pending_records": [],
    }

    t0 = time.time()
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for i, converted in enumerate(records):
        source_id = converted.get("source_id", f"unknown-{i}")
        ok, err = validate_record(converted)
        if not ok:
            stats["validation_errors"].append({"source_id": source_id, "error": err})
            continue
        stats["validated"] += 1
        grouped.setdefault(source_id, []).append(converted)

        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            print(f"  [validate {i+1}/{len(records)}] {elapsed:.1f}s elapsed")

    # Projected chains make dry-run a useful changeset preview without writes.
    projected_chains: Dict[str, List[Dict[str, Any]]] = {}
    search_submissions: List[Tuple[str, Dict[str, Any]]] = []

    for source_id, converted_group in grouped.items():
        try:
            stored_chain = store.list_versions(source_id)
        except Exception as exc:
            stats["store_errors"].append(
                {"source_id": source_id, "error": str(exc)}
            )
            continue

        touch_reasons = _dataset_v2_touch_reasons(stored_chain)
        if touch_reasons:
            stats["conflicts"] += 1
            stats["conflict_records"].append(
                {
                    "source_id": source_id,
                    "versions": sorted(
                        {
                            str(record.get("version") or "?")
                            for record in stored_chain
                        },
                        key=_version_sort_key,
                    ),
                    "reason": "; ".join(touch_reasons),
                }
            )
            projected_chains[source_id] = list(stored_chain)
            continue

        projected_by_version = {
            str(record.get("version")): record for record in stored_chain
        }

        if not skip_store:
            for converted in converted_group:
                submission = build_submission_record(converted)
                version = str(submission["version"])
                existing = projected_by_version.get(version)

                if existing is None:
                    prepared = _prepare_migration_write(submission)
                    stats["created"] += 1
                elif (
                    existing.get("sync_content_hash")
                    == submission["sync_content_hash"]
                    and not _chain_fields_need_repair(existing, submission)
                ):
                    prepared = existing
                    stats["unchanged"] += 1
                else:
                    prepared = _prepare_migration_write(
                        submission,
                        existing=existing,
                        merge_warnings=stats["metadata_merge_warnings"],
                    )
                    stats["updated"] += 1

                projected_by_version[version] = prepared
                if not dry_run and prepared is not existing:
                    try:
                        store.upsert_submission(prepared)
                    except Exception as exc:
                        stats["store_errors"].append(
                            {
                                "source_id": source_id,
                                "error": str(exc),
                            }
                        )
                        # Do not use an uncommitted record for Search.
                        if existing is None:
                            projected_by_version.pop(version, None)
                        else:
                            projected_by_version[version] = existing

        if not dry_run and not skip_store:
            try:
                chain_after = store.list_versions(source_id)
            except Exception as exc:
                stats["store_errors"].append(
                    {"source_id": source_id, "error": str(exc)}
                )
                chain_after = list(projected_by_version.values())
        else:
            chain_after = list(projected_by_version.values())

        projected_chains[source_id] = chain_after
        latest = _latest_published(chain_after)
        if (
            latest is not None
            and latest.get("search_synced_hash")
            != latest.get("sync_content_hash")
        ):
            search_submissions.append((source_id, latest))

    stats["search_required"] = len(search_submissions)
    stats["would_search_ingest"] = len(search_submissions)

    # Submit explicit batches so each accepted record can be stamped atomically
    # in the store only after its batch was handed successfully to Search.
    if search is not None and search_submissions:
        total_batches = (
            len(search_submissions) + SEARCH_BATCH_SIZE - 1
        ) // SEARCH_BATCH_SIZE
        print(
            f"\n  Ingesting {len(search_submissions)} records to Globus Search "
            f"({total_batches} batch{'es' if total_batches != 1 else ''} "
            f"of {SEARCH_BATCH_SIZE})..."
        )

        for batch_start in range(0, len(search_submissions), SEARCH_BATCH_SIZE):
            batch = search_submissions[
                batch_start:batch_start + SEARCH_BATCH_SIZE
            ]
            result = _batch_ingest_with_retry(
                search,
                [submission for _, submission in batch],
            )
            stats["search_ingested"] += int(result.get("ingested") or 0)
            batch_errors = result.get("errors") or []
            stats["search_errors"].extend(batch_errors)

            if result.get("task_ids"):
                print(f"  Task IDs: {result['task_ids']}")

            failed_source_ids = {
                error.get("source_id")
                for error in batch_errors
                if isinstance(error, dict) and error.get("source_id")
            }
            generic_error = any(
                not isinstance(error, dict) or not error.get("source_id")
                for error in batch_errors
            )
            accepted = [
                (source_id, submission)
                for source_id, submission in batch
                if not generic_error and source_id not in failed_source_ids
            ]
            if int(result.get("ingested") or 0) < len(accepted):
                # The response does not identify all rejected records, so do
                # not overstate Search state for this batch.
                accepted = []

            if dry_run or skip_store:
                continue
            for source_id, submission in accepted:
                try:
                    _stamp_search_sync(
                        store,
                        source_id,
                        str(submission["version"]),
                        submission.get("sync_content_hash"),
                    )
                except Exception as exc:
                    stats["store_errors"].append(
                        {
                            "source_id": source_id,
                            "error": "could not stamp Search sync: {}".format(
                                exc
                            ),
                        }
                    )

    # Re-read each processed dataset after Search stamping. Only the newest
    # published version owns the version-less Search subject, so only it can
    # make the index pending.
    for source_id in grouped:
        try:
            chain = (
                projected_chains.get(source_id, [])
                if dry_run
                else store.list_versions(source_id)
            )
        except Exception as exc:
            stats["store_errors"].append(
                {"source_id": source_id, "error": str(exc)}
            )
            continue
        latest = _latest_published(chain)
        if (
            latest is not None
            and latest.get("search_synced_hash")
            != latest.get("sync_content_hash")
        ):
            stats["search_pending_records"].append(
                {
                    "source_id": source_id,
                    "version": str(latest.get("version")),
                }
            )

    stats["search_pending"] = len(stats["search_pending_records"])

    stats["elapsed_seconds"] = round(time.time() - t0, 2)
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Ingest converted MDF datasets into DynamoDB and Globus Search."
    )
    parser.add_argument(
        "--env",
        choices=["dev", "staging", "prod"],
        default=None,
        help="Auto-resolve config from a deployed CloudFormation stack "
             "(reads Lambda env vars for table name, search index, Globus creds)",
    )
    parser.add_argument(
        "-i", "--input",
        default=os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "converted_datasets.json"),
        help="Path to converted_datasets.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate all records without writing to any store",
    )
    parser.add_argument(
        "--skip-search",
        action="store_true",
        help="Skip Globus Search ingest (DynamoDB only)",
    )
    parser.add_argument(
        "--skip-store",
        action="store_true",
        help="Skip DynamoDB/SQLite store (search only)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only process first N records (0 = all)",
    )
    parser.add_argument(
        "--watermark-file",
        default=None,
        help="Write the resolved candidate watermark here only after a complete "
             "successful store ingest. The format matches extract --since-file.",
    )
    parser.add_argument(
        "--candidate-watermark",
        default=None,
        help="Candidate ISO watermark to commit after success. Overrides values "
             "from the direct input JSON and --extract-file.",
    )
    parser.add_argument(
        "--extract-file",
        default=None,
        help="Original extract JSON to read candidate_watermark from when the "
             "converted input does not contain it.",
    )
    parser.add_argument(
        "--report-file",
        default=None,
        help="Optionally write the full machine-readable run summary as JSON.",
    )
    parser.add_argument(
        "--backfill-embeddings",
        action="store_true",
        help="After ingest, enqueue the embedding rebuild (generate embeddings "
             "+ snapshot) so migrated datasets are semantically searchable. "
             "Enqueues onto the deployed async queue; the worker does the scan.",
    )
    parser.add_argument(
        "--embedding-limit",
        type=int,
        default=0,
        help="Cap embedding generation to N records (0 = all). Useful for phased "
             "backfills. Only meaningful with --backfill-embeddings.",
    )
    args = parser.parse_args()

    # ── Resolve config from deployed stack ──
    if args.env:
        print(f"Resolving config from stack mdf-connect-v2-{args.env}...")
        resolved = resolve_env_from_stack(args.env)
        apply_env(resolved)
        print(f"  STORE_BACKEND:          {resolved.get('STORE_BACKEND', '(not set)')}")
        print(f"  DYNAMO_SUBMISSIONS_TABLE: {resolved.get('DYNAMO_SUBMISSIONS_TABLE', '(not set)')}")
        print(f"  SEARCH_INDEX_UUID:      {resolved.get('SEARCH_INDEX_UUID', '(not set)')}")
        print(f"  USE_MOCK_SEARCH:        {resolved.get('USE_MOCK_SEARCH', '(not set)')}")
        has_globus = bool(resolved.get("GLOBUS_CLIENT_ID"))
        print(f"  GLOBUS_CLIENT_ID:       {'***' if has_globus else '(not set)'}")
        print(f"  GLOBUS_CLIENT_SECRET:   {'***' if resolved.get('GLOBUS_CLIENT_SECRET') else '(not set)'}")
        print()

    # ── Refuse to run against the legacy v1 production index ──
    # Must happen after config resolution/env overrides and before any
    # write, in every mode (including --dry-run — a mis-set index is a
    # config error regardless of whether this run actually writes).
    guard_against_legacy_index()

    input_path = os.path.abspath(args.input)
    print(f"Loading {input_path}")

    with open(input_path) as f:
        data = json.load(f)

    try:
        candidate_watermark = _resolve_candidate_watermark(args, data)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error("cannot resolve candidate watermark: {}".format(exc))

    records = data.get("records", [])
    conversion_errors = data.get("errors") or []
    print(f"Loaded {len(records)} records")
    if conversion_errors:
        print(
            "  Input reports {} conversion error(s); watermark advancement "
            "will be blocked.".format(len(conversion_errors))
        )

    if args.limit > 0:
        records = records[:args.limit]
        print(f"  Limited to first {args.limit}")

    mode_parts = []
    if args.dry_run:
        backend = os.environ.get("STORE_BACKEND", "dynamo")
        mode_parts.append(f"DRY RUN (read-only store={backend})")
    else:
        if not args.skip_store:
            backend = os.environ.get("STORE_BACKEND", "dynamo")
            mode_parts.append(f"store={backend}")
        if not args.skip_search:
            use_mock = os.environ.get("USE_MOCK_SEARCH", "true").lower() == "true"
            mode_parts.append(f"search={'mock' if use_mock else 'globus'}")
    print(f"Mode: {' + '.join(mode_parts) or 'no-op'}\n")

    stats = ingest_records(
        records,
        dry_run=args.dry_run,
        skip_search=args.skip_search,
        skip_store=args.skip_store,
    )

    # ── Report ──
    print(f"\n{'='*50}")
    print(f"Results ({stats['elapsed_seconds']}s)")
    print(f"{'='*50}")
    print(f"  Total records:      {stats['total']}")
    print(f"  Validated:          {stats['validated']}")

    if conversion_errors:
        print(f"  Conversion errors:  {len(conversion_errors)}")
        for error in conversion_errors[:5]:
            print(
                "    {}: {}".format(
                    error.get("subject", "unknown"),
                    str(error.get("error", ""))[:100],
                )
            )

    if stats["validation_errors"]:
        print(f"  Validation errors:  {len(stats['validation_errors'])}")
        for e in stats["validation_errors"][:5]:
            print(f"    {e['source_id']}: {e['error'][:100]}")
        if len(stats["validation_errors"]) > 5:
            print(f"    ... and {len(stats['validation_errors']) - 5} more")

    if not args.skip_store:
        prefix = "Would " if args.dry_run else ""
        print(f"  {prefix}Created:      {stats['created']}")
        print(f"  {prefix}Updated:      {stats['updated']}")
        print(f"  {prefix}Unchanged:    {stats['unchanged']}")
        print(f"  {prefix}Conflicts:    {stats['conflicts']}")
        for conflict in stats["conflict_records"][:5]:
            print(
                "    {source_id} versions={versions}: {reason}".format(
                    **conflict
                )
            )
        if len(stats["conflict_records"]) > 5:
            print(
                "    ... and {} more".format(
                    len(stats["conflict_records"]) - 5
                )
            )
        print(f"  Store errors:       {len(stats['store_errors'])}")
        if stats["store_errors"]:
            for e in stats["store_errors"][:3]:
                print(f"    {e['source_id']}: {e['error'][:100]}")
        print(
            f"  Metadata merge warnings: "
            f"{len(stats['metadata_merge_warnings'])}"
        )
        for warning in stats["metadata_merge_warnings"][:3]:
            print(f"    {warning['source_id']}: {warning['error'][:100]}")

    if args.dry_run:
        print(
            f"  Would search-ingest:{stats['would_search_ingest']:>6}"
        )
    else:
        if not args.skip_search:
            print(f"  Search ingested:    {stats['search_ingested']}")
            print(f"  Search errors:      {len(stats['search_errors'])}")
            if stats["search_errors"]:
                for e in stats["search_errors"][:3]:
                    print(f"    {e['source_id']}: {e['error'][:100]}")
    print(f"  Search pending:     {stats['search_pending']}")
    for pending in stats["search_pending_records"][:5]:
        print(
            "    {source_id}-{version}".format(**pending)
        )

    # ── Embedding backfill ──
    # Migrated records land published but are NOT yet in the semantic index;
    # something must trigger embedding generation. Either do it here (opt-in)
    # or remind the operator so it isn't forgotten.
    ingested_anything = (
        not args.dry_run
        and not args.skip_store
        and (stats["created"] + stats["updated"] > 0)
    )
    if args.backfill_embeddings and not args.dry_run and not args.skip_store:
        try:
            from v2.async_jobs import enqueue_rebuild_dispatch_job

            limit = args.embedding_limit or None
            result = enqueue_rebuild_dispatch_job(force=False, limit=limit, build_snapshot=True)
            print(f"\n  Embedding backfill enqueued (dispatch={os.environ.get('ASYNC_DISPATCH_MODE', 'inline')}, "
                  f"limit={limit or 'all'}): {result}")
            print("  The async worker will generate embeddings and rebuild the snapshot.")
        except Exception as exc:
            print(f"\n  WARNING: embedding backfill enqueue failed: {exc}")
            print("  Records are stored; run `mdf admin rebuild-embeddings --service <env>` manually.")
    elif ingested_anything:
        print("\n  NOTE: migrated records are NOT yet in the semantic index.")
        print("  Run with --backfill-embeddings, or: mdf admin rebuild-embeddings --service <env>")

    # Exit with error code if any failures
    has_errors = (
        stats["validation_errors"]
        or stats.get("store_errors")
        or stats.get("search_errors")
    )

    # ── Watermark commit ──
    # Conflicts are intentionally non-fatal, but advancing past unresolved v1
    # edits would make the delta bridge skip them permanently.
    if args.watermark_file:
        blocked_reasons = _watermark_blocked_reasons(
            args,
            candidate_watermark,
            stats,
            bool(has_errors or conversion_errors),
        )

        if blocked_reasons:
            print(
                "\n  Watermark NOT advanced: {}.".format(
                    "; ".join(blocked_reasons)
                )
            )
        else:
            try:
                _write_watermark(args.watermark_file, candidate_watermark)
            except OSError as exc:
                print(f"\n  Watermark write failed: {exc}")
                has_errors = True
            else:
                print(
                    "\n  Watermark updated: {} -> {}".format(
                        os.path.abspath(args.watermark_file),
                        candidate_watermark,
                    )
                )
    elif candidate_watermark:
        print(
            "\n  Candidate watermark {} was not written "
            "(no --watermark-file).".format(candidate_watermark)
        )

    if args.report_file:
        report = dict(stats)
        report["conversion_errors"] = conversion_errors
        report["candidate_watermark"] = candidate_watermark
        report["candidate_watermark_suppressed"] = data.get(
            "candidate_watermark_suppressed"
        )
        with open(args.report_file, "w") as report_handle:
            json.dump(report, report_handle, indent=2, default=str)
        print(f"\n  JSON report written: {os.path.abspath(args.report_file)}")

    # A dry-run is a preview: content problems are reported above but never
    # turn it into a gating failure. Argparse/config errors still exit earlier.
    exit_code = _result_exit_code(
        dry_run=args.dry_run,
        has_errors=bool(has_errors),
        conflicts=stats["conflicts"] + len(conversion_errors),
    )
    if exit_code:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
