"""Backfill: every dataset in the "MDF Open" collection is CC-BY-4.0.

Maintainer rule (2026-09-04): datasets published through the MDF Open
collection were always accepted under CC-BY-4.0, but the v1 records rarely
carried a license field, so the migration left ``license`` empty on most of
them. This script sets ``dataset_mdata.license`` to CC-BY-4.0 on rows whose
organization is "MDF Open" AND whose license is missing/empty. It never
touches a row that already has any license value, and never touches other
collections.

Usage (from aws/):
    python v2/scripts/backfill_license_mdf_open.py --env staging --dry-run
    python v2/scripts/backfill_license_mdf_open.py --env staging --execute

Same credential/table resolution and conditional-write discipline as
``backfill_record_shape.py`` (writes are conditional on ``updated_at`` and do
not bump it). Re-ingest the search index afterwards so ``license`` facets and
cards reflect the change.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Tuple

from v2.scripts.backfill_record_shape import (  # reuse, do not duplicate
    _scan_rows,
    _write_row,
    apply_env,
    resolve_env_from_stack,
)

COLLECTION = "MDF Open"
CC_BY_4 = {
    "name": "CC-BY-4.0",
    "identifier": "CC-BY-4.0",
    "url": "https://creativecommons.org/licenses/by/4.0/",
}


def _mdata(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw = record.get("dataset_mdata")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    return raw if isinstance(raw, dict) else None


def _license_is_empty(value: Any) -> bool:
    if value in (None, "", [], {}):
        return True
    if isinstance(value, dict):
        return not any(str(v or "").strip() for v in value.values())
    if isinstance(value, str):
        return not value.strip()
    return False


def plan_row(record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    """Return (new_record or None, reason)."""
    org = record.get("organization") or (_mdata(record) or {}).get("organization")
    if (org or "").strip() != COLLECTION:
        return None, "other-collection"
    mdata = _mdata(record)
    if mdata is None:
        return None, "unparseable-metadata"
    if not _license_is_empty(mdata.get("license")):
        return None, "has-license"
    new_mdata = dict(mdata)
    new_mdata["license"] = dict(CC_BY_4)
    new_record = dict(record)
    # Preserve the stored encoding (string vs map) so nothing else changes shape.
    new_record["dataset_mdata"] = (
        json.dumps(new_mdata) if isinstance(record.get("dataset_mdata"), str) else new_mdata
    )
    return new_record, "set-cc-by-4.0"


def run(table, execute: bool, limit: Optional[int]) -> Dict[str, Any]:
    counts = {"scanned": 0, "changed": 0, "unchanged": 0, "conflicts": 0, "errors": 0}
    reasons: Dict[str, int] = {}
    rows: List[Dict[str, Any]] = []
    for record in _scan_rows(table, limit):
        counts["scanned"] += 1
        new_record, reason = plan_row(record)
        reasons[reason] = reasons.get(reason, 0) + 1
        if new_record is None:
            counts["unchanged"] += 1
            continue
        rows.append({"source_id": record.get("source_id"), "version": record.get("version")})
        if not execute:
            counts["changed"] += 1
            continue
        try:
            _write_row(table, new_record, record.get("updated_at"))
            counts["changed"] += 1
        except Exception as exc:  # ConditionalCheckFailed or transport error
            name = type(exc).__name__
            if "ConditionalCheckFailed" in name or "ConditionalCheckFailed" in str(exc):
                counts["conflicts"] += 1
            else:
                counts["errors"] += 1
                rows[-1]["error"] = str(exc)[:200]
    return {"counts": counts, "reasons": reasons, "rows": rows, "executed": execute}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--env", required=True, choices=["staging", "prod"])
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args(argv)

    resolved = resolve_env_from_stack(args.env)
    apply_env(resolved)
    import boto3

    table_name = resolved.get("DYNAMO_SUBMISSIONS_TABLE") or f"mdf-submissions-{args.env}"
    table = boto3.resource("dynamodb", region_name="us-east-1").Table(table_name)
    report = run(table, execute=args.execute, limit=args.limit)
    c = report["counts"]
    print(
        f"{'EXECUTE' if args.execute else 'DRY RUN — no writes'} on {table_name}: "
        f"scanned={c['scanned']} changed={c['changed']} unchanged={c['unchanged']} "
        f"conflicts={c['conflicts']} errors={c['errors']} reasons={report['reasons']}",
        file=sys.stderr,
    )
    json.dump(report, sys.stdout, indent=1)
    return 0 if c["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
