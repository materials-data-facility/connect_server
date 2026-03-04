#!/usr/bin/env python3
"""Ingest converted MDF production datasets into DynamoDB and Globus Search.

Reads converted_datasets.json (output of convert_production_datasets.py),
builds proper v2 submission records, and loads them into both the submission
store and the search index.

Supports:
  - Auto-resolve config from a deployed stack (--env prod)
  - Local mode (SQLite + MockSearch) for testing
  - Dry-run mode to validate without writing
  - Resume: skips records that already exist in the store

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
"""

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Tuple

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
}

REGION = "us-east-1"


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


def build_submission_record(converted: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a converted_datasets.json record into a v2 submission record.

    The converted record has:
        source_id, source_name, version (int), ingest_date, doi, endpoint_path,
        metadata (flat v2 dict)

    The submission record needs:
        source_id, version (str), versioned_source_id, user_id, user_email,
        organization, status, dataset_mdata (JSON str), schema_version, test,
        created_at, updated_at, published_at, doi, dataset_doi
    """
    metadata = dict(converted["metadata"])

    # Populate data_sources from endpoint_path (not present in converted metadata)
    endpoint_path = converted.get("endpoint_path")
    if endpoint_path and not metadata.get("data_sources"):
        metadata["data_sources"] = [endpoint_path]

    version_str = metadata.get("version", "1.0")
    doi = converted.get("doi") or None
    ingest_date = converted.get("ingest_date", "")

    organization = metadata.get("organization") or ""

    record = {
        "source_id": converted["source_id"],
        "version": version_str,
        "versioned_source_id": converted["source_id"],
        "user_id": "v1-migration",
        "status": "published",
        "dataset_mdata": json.dumps(metadata),
        "schema_version": "2",
        "test": False,
        "created_at": ingest_date,
        "updated_at": ingest_date,
        "published_at": ingest_date,
    }

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


def ingest_records(
    records: List[Dict[str, Any]],
    *,
    dry_run: bool = False,
    skip_search: bool = False,
    skip_store: bool = False,
) -> Dict[str, Any]:
    """Ingest converted records into the submission store and search index.

    Store ingest is record-by-record (skip duplicates).
    Search ingest uses GMetaList batching for speed (~10x faster than individual requests).

    Returns a summary dict with counts.
    """
    from v2.store import get_store
    from v2.search_client import get_search_client

    store = None if dry_run or skip_store else get_store()
    search = None if dry_run or skip_search else get_search_client()

    stats = {
        "total": len(records),
        "validated": 0,
        "validation_errors": [],
        "store_inserted": 0,
        "store_skipped": 0,
        "store_errors": [],
        "search_ingested": 0,
        "search_errors": [],
    }

    t0 = time.time()

    # ── Validate + Store (record-by-record) ──
    valid_submissions: List[Tuple[str, Dict[str, Any]]] = []

    for i, converted in enumerate(records):
        source_id = converted.get("source_id", f"unknown-{i}")
        version_str = converted.get("metadata", {}).get("version", "1.0")

        ok, err = validate_record(converted)
        if not ok:
            stats["validation_errors"].append({"source_id": source_id, "error": err})
            continue
        stats["validated"] += 1

        if dry_run:
            continue

        submission = build_submission_record(converted)
        valid_submissions.append((source_id, submission))

        if store:
            try:
                existing = store.get_submission(source_id, version_str)
                if existing:
                    stats["store_skipped"] += 1
                else:
                    store.upsert_submission(submission)
                    stats["store_inserted"] += 1
            except Exception as exc:
                stats["store_errors"].append({"source_id": source_id, "error": str(exc)})

        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            print(f"  [validate+store {i+1}/{len(records)}] {elapsed:.1f}s elapsed")

    # ── Search ingest: GMetaList batches ──
    if search and valid_submissions:
        all_subs = [sub for _, sub in valid_submissions]
        total_batches = (len(all_subs) + SEARCH_BATCH_SIZE - 1) // SEARCH_BATCH_SIZE
        print(f"\n  Ingesting {len(all_subs)} records to Globus Search "
              f"({total_batches} batch{'es' if total_batches != 1 else ''} of {SEARCH_BATCH_SIZE})...")

        result = search.batch_ingest(all_subs, batch_size=SEARCH_BATCH_SIZE)
        stats["search_ingested"] = result.get("ingested", 0)
        stats["search_errors"] = result.get("errors", [])

        if result.get("task_ids"):
            print(f"  Task IDs: {result['task_ids']}")

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

    input_path = os.path.abspath(args.input)
    print(f"Loading {input_path}")

    with open(input_path) as f:
        data = json.load(f)

    records = data.get("records", [])
    print(f"Loaded {len(records)} records")

    if args.limit > 0:
        records = records[:args.limit]
        print(f"  Limited to first {args.limit}")

    mode_parts = []
    if args.dry_run:
        mode_parts.append("DRY RUN")
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

    if stats["validation_errors"]:
        print(f"  Validation errors:  {len(stats['validation_errors'])}")
        for e in stats["validation_errors"][:5]:
            print(f"    {e['source_id']}: {e['error'][:100]}")
        if len(stats["validation_errors"]) > 5:
            print(f"    ... and {len(stats['validation_errors']) - 5} more")

    if not args.dry_run:
        if not args.skip_store:
            print(f"  Store inserted:     {stats['store_inserted']}")
            print(f"  Store skipped:      {stats['store_skipped']} (already exist)")
            if stats["store_errors"]:
                print(f"  Store errors:       {len(stats['store_errors'])}")
                for e in stats["store_errors"][:3]:
                    print(f"    {e['source_id']}: {e['error'][:100]}")

        if not args.skip_search:
            print(f"  Search ingested:    {stats['search_ingested']}")
            if stats["search_errors"]:
                print(f"  Search errors:      {len(stats['search_errors'])}")
                for e in stats["search_errors"][:3]:
                    print(f"    {e['source_id']}: {e['error'][:100]}")

    # Exit with error code if any failures
    has_errors = (
        stats["validation_errors"]
        or stats.get("store_errors")
        or stats.get("search_errors")
    )
    if has_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
