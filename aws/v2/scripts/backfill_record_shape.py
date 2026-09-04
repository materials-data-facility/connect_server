#!/usr/bin/env python3
"""Rewrite existing submission rows into the v2.1 record shape.

WHAT IT CHANGES

Four system attributes used to live inside the user-editable ``dataset_mdata``
JSON blob. In the v2.1 shape they are top-level record attributes (see
``v2/submission_utils.py`` for the canonical description):

  * ``acl``                — list, default ``["public"]``, entries canonicalized
                             to Globus principals (bare UUID -> identity URN,
                             ``urn:globus:*`` passed through verbatim)
  * ``source_name``        — search facet grouping key, default ``source_id``
  * ``legacy_source_id``   — original v1 id (omitted when equal to ``source_id``)
  * ``root_version`` /
    ``previous_version``   — BARE version strings (``"1.0"``), no longer
                             ``"{source_id}-{version}"`` composites

It also strips the deprecated ``extensions.mdf_source_id`` /
``extensions.mdf_source_name`` aliases out of the stored blob, and stamps the
sparse ``curation_queue`` attribute that ``curation-queue-index`` is keyed on
(present iff status is pending_curation / approved / rejected). The index is
populated only on write, so without this pass the curation queue reads as empty
for the entire pre-existing backlog.

The rewrite is IDEMPOTENT: it is the same ``normalize_record_shape`` every store
write already applies, so a second run reports every row as unchanged.

SAFETY

  * ``--dry-run`` is the default. The script refuses to write without
    ``--execute``.
  * Every write is conditional on the row's ``updated_at`` being unchanged since
    it was read, so a concurrent edit through the API is never clobbered — the
    row is reported as a conflict and left alone.
  * ``updated_at`` itself is NOT bumped: this is a shape migration, not a
    content edit, and bumping it would reorder every user's submission listing
    and invalidate the sync pipeline's conflict detection.

USAGE

    # inspect what would change (no writes)
    python v2/scripts/backfill_record_shape.py --env staging --dry-run

    # first 20 rows only
    python v2/scripts/backfill_record_shape.py --env staging --dry-run --limit 20

    # actually write
    python v2/scripts/backfill_record_shape.py --env staging --execute

A JSON report is always written to stdout after the human-readable summary.
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

# Allow imports from aws/ root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# submission_utils is a leaf module (no config, no boto3), so it is safe to
# import before the environment is resolved. Everything else is imported lazily.
from v2.submission_utils import (  # noqa: E402
    RECORD_SHAPE_VERSION,
    normalize_record_shape,
)

REGION = os.environ.get("AWS_REGION", "us-east-1")

#: Statuses that belong in the sparse curation-queue GSI. Mirrors
#: ``store.CURATION_QUEUE_STATUSES``; duplicated as a literal so a drift between
#: the two shows up as a test failure rather than a silently different backfill.
CURATION_QUEUE_STATUSES = frozenset({"pending_curation", "approved", "rejected"})

#: Attributes this script is allowed to introduce or change. Anything outside
#: this set differing after normalization is a bug, not a migration.
MANAGED_FIELDS = (
    "acl",
    "source_name",
    "legacy_source_id",
    "root_version",
    "previous_version",
    "curation_queue",
    "dataset_mdata",
)


def resolve_env_from_stack(env: str) -> Dict[str, str]:
    """Read DYNAMO_SUBMISSIONS_TABLE from the deployed ApiFunction's config.

    Same credential/config pattern as ``ingest_converted_datasets.py --env``:
    the table name comes from the live Lambda's resolved environment, so the
    script can never be pointed at a table the backend is not actually using.
    """
    import boto3

    stack_name = "mdf-connect-v2-{}".format(env)

    cf = boto3.client("cloudformation", region_name=REGION)
    try:
        resp = cf.describe_stack_resources(StackName=stack_name)
    except Exception as exc:
        raise SystemExit(
            "Stack {!r} not found in {}. Deploy first with: ./deploy.sh {}".format(
                stack_name, REGION, env
            )
        ) from exc

    api_func = None
    for resource in resp["StackResources"]:
        if resource["LogicalResourceId"] == "ApiFunction":
            api_func = resource["PhysicalResourceId"]
            break
    if not api_func:
        raise SystemExit("ApiFunction not found in stack {}".format(stack_name))

    lam = boto3.client("lambda", region_name=REGION)
    config = lam.get_function_configuration(FunctionName=api_func)
    lambda_env = config.get("Environment", {}).get("Variables", {})

    resolved = {"STORE_BACKEND": "dynamo"}
    for key in ("DYNAMO_SUBMISSIONS_TABLE", "AWS_REGION"):
        if key in lambda_env:
            resolved[key] = lambda_env[key]
    if "DYNAMO_SUBMISSIONS_TABLE" not in resolved:
        raise SystemExit(
            "DYNAMO_SUBMISSIONS_TABLE not present in the {} ApiFunction "
            "environment; refusing to guess a table name".format(env)
        )
    return resolved


def apply_env(resolved: Dict[str, str]) -> None:
    """Export resolved config without overriding an explicit override."""
    for key, value in resolved.items():
        if key not in os.environ:
            os.environ[key] = value


def with_curation_queue(record: Dict[str, Any]) -> Dict[str, Any]:
    """Stamp/remove the sparse ``curation_queue`` attribute for ``record``.

    Same rule as ``store._with_curation_queue``: present and equal to status
    while the row is in the curation queue, absent otherwise, so the GSI stays
    sparse and every published row does not pile into one partition.
    """
    item = dict(record)
    status = item.get("status")
    if status in CURATION_QUEUE_STATUSES:
        item["curation_queue"] = status
    else:
        item.pop("curation_queue", None)
    return item


def _comparable(value: Any) -> Any:
    """Canonical form for change detection (blob key order must not matter)."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return _comparable(json.loads(stripped))
            except (TypeError, ValueError):
                return value
        return value
    if isinstance(value, dict):
        return {key: _comparable(val) for key, val in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_comparable(val) for val in value]
    return value


def plan_row(record: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Return ``(normalized_record, changed_fields)`` for one row.

    ``changed_fields`` maps a managed attribute name to ``{"from": ..., "to":
    ...}``. An empty mapping means the row is already in the v2.1 shape.
    """
    normalized = with_curation_queue(normalize_record_shape(record))

    changes: Dict[str, Any] = {}
    for field in MANAGED_FIELDS:
        before = record.get(field)
        after = normalized.get(field)
        if _comparable(before) != _comparable(after):
            changes[field] = {"from": before, "to": after}
    return normalized, changes


def _scan_rows(table, limit: Optional[int]):
    """Yield every submission row, honouring ``--limit``."""
    scanned = 0
    last_key = None
    while True:
        kwargs: Dict[str, Any] = {}
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            # The publish lock shares the table under a reserved version and is
            # not a submission.
            if item.get("version") == "__publish_lock__":
                continue
            yield item
            scanned += 1
            if limit is not None and scanned >= limit:
                return
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            return


def _write_row(table, normalized: Dict[str, Any], expected_updated_at: Any) -> None:
    """Conditionally replace a row, guarding against a concurrent edit."""
    if expected_updated_at is None:
        table.put_item(
            Item=normalized,
            ConditionExpression="attribute_not_exists(updated_at)",
        )
        return
    table.put_item(
        Item=normalized,
        ConditionExpression="updated_at = :expected",
        ExpressionAttributeValues={":expected": expected_updated_at},
    )


def backfill(table, execute: bool, limit: Optional[int], verbose: bool) -> Dict[str, Any]:
    """Run the backfill (or the dry run) and return the JSON report."""
    counts = {"scanned": 0, "changed": 0, "unchanged": 0, "conflicts": 0, "errors": 0}
    changed_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []
    field_totals: Dict[str, int] = {}

    for record in _scan_rows(table, limit):
        counts["scanned"] += 1
        source_id = str(record.get("source_id") or "")
        version = str(record.get("version") or "")
        try:
            normalized, changes = plan_row(record)
        except Exception as exc:  # pragma: no cover - defensive
            counts["errors"] += 1
            errors.append(
                {"source_id": source_id, "version": version, "error": str(exc)}
            )
            continue

        if not changes:
            counts["unchanged"] += 1
            continue

        for field in changes:
            field_totals[field] = field_totals.get(field, 0) + 1

        entry = {
            "source_id": source_id,
            "version": version,
            "fields": sorted(changes),
        }
        if verbose:
            entry["changes"] = changes

        if not execute:
            counts["changed"] += 1
            changed_rows.append(entry)
            continue

        try:
            _write_row(table, normalized, record.get("updated_at"))
        except Exception as exc:
            name = type(exc).__name__
            code = ""
            response = getattr(exc, "response", None)
            if isinstance(response, dict):
                code = response.get("Error", {}).get("Code", "")
            if code == "ConditionalCheckFailedException":
                counts["conflicts"] += 1
                errors.append(
                    {
                        "source_id": source_id,
                        "version": version,
                        "error": "updated_at changed under us; row skipped",
                    }
                )
                continue
            counts["errors"] += 1
            errors.append(
                {
                    "source_id": source_id,
                    "version": version,
                    "error": "{}: {}".format(name, exc),
                }
            )
            continue

        counts["changed"] += 1
        changed_rows.append(entry)

    return {
        "record_shape_version": RECORD_SHAPE_VERSION,
        "mode": "execute" if execute else "dry-run",
        "table": getattr(table, "name", None),
        "counts": counts,
        "fields_changed": dict(sorted(field_totals.items())),
        "rows": changed_rows,
        "errors": errors,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Rewrite submission rows into the v2.1 record shape.",
    )
    parser.add_argument(
        "--env",
        choices=("staging", "prod"),
        help="Resolve the table name from the deployed mdf-connect-v2-{env} stack.",
    )
    parser.add_argument(
        "--table",
        help="Table name override (skips stack resolution; for local/test use).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing (default).",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Actually write the normalized rows.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N rows (useful for a first look).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include per-row before/after values in the JSON report.",
    )
    args = parser.parse_args(argv)

    if not args.env and not args.table:
        parser.error("one of --env or --table is required")

    if args.env:
        apply_env(resolve_env_from_stack(args.env))
    table_name = args.table or os.environ["DYNAMO_SUBMISSIONS_TABLE"]

    # Default to a dry run: --execute is the only way to write. Stated
    # explicitly rather than inferred, because this rewrites every row.
    execute = bool(args.execute)
    if not execute:
        print(
            "DRY RUN — no writes. Re-run with --execute to apply.",
            file=sys.stderr,
        )

    import boto3

    resource = boto3.resource("dynamodb", region_name=REGION)
    table = resource.Table(table_name)

    report = backfill(table, execute=execute, limit=args.limit, verbose=args.verbose)
    counts = report["counts"]

    print(
        "\n{} on {}: scanned={} changed={} unchanged={} conflicts={} errors={}".format(
            report["mode"],
            table_name,
            counts["scanned"],
            counts["changed"],
            counts["unchanged"],
            counts["conflicts"],
            counts["errors"],
        ),
        file=sys.stderr,
    )
    if report["fields_changed"]:
        print("  fields: {}".format(report["fields_changed"]), file=sys.stderr)

    print(json.dumps(report, indent=2, default=str))
    return 1 if counts["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
