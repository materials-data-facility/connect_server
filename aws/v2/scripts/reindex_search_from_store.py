#!/usr/bin/env python3
"""Rebuild the v2 Globus Search index from the DynamoDB submissions store.

The store is the source of truth: every dataset that has a published version
gets exactly one entry (subject = version-less detail URL) describing its
newest published version -- the same rule the publish worker applies
(``async_jobs._owns_search_entry``). Unlike ``rebuild_search_from_converted``,
this includes v2-native datasets and needs no converted legacy JSON.

Typical use after a GMeta shape change (wipe first; run the wipe twice):
  python v2/scripts/backup_search_index.py --env staging -o backup.jsonl.gz
  python v2/scripts/wipe_search_index.py --env staging --execute
  python v2/scripts/reindex_search_from_store.py --env staging            # dry run
  python v2/scripts/reindex_search_from_store.py --env staging --execute

Ingest into an index that still holds entries is also safe: entries are keyed
by subject, so this upserts. The LEGACY v1 production index is always refused,
and prod needs --allow-prod.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from v2.scripts.ingest_converted_datasets import (  # noqa: E402
    LEGACY_SEARCH_INDEX_UUID,
    apply_env,
    resolve_env_from_stack,
)

SEARCH_BATCH_SIZE = 100
STORE_SCAN_LIMIT = 1_000_000
TASK_WAIT_SECONDS = 300


def _version_key(value: str) -> List[Any]:
    return [int(p) if p.isdigit() else p for p in str(value).split(".")]


def select_index_records(
    records: List[Dict[str, Any]],
) -> Tuple[List[Tuple[Dict[str, Any], int]], Dict[str, int]]:
    """Pick the newest published version per source_id.

    Returns ``([(record, version_count), ...], stats)``. ``version_count`` is
    the number of stored versions of the dataset, matching what the publish
    worker passes (``len(all_versions)``). Restricted datasets are included:
    ``build_gmeta_entry`` writes their ``visible_to``, exactly as a publish does.
    """
    by_source: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        source_id = record.get("source_id")
        if not source_id or not record.get("version"):
            continue
        by_source.setdefault(source_id, []).append(record)

    selected: List[Tuple[Dict[str, Any], int]] = []
    unpublished = 0
    for source_id in sorted(by_source):
        versions = by_source[source_id]
        published = [r for r in versions if r.get("status") == "published"]
        if not published:
            unpublished += 1
            continue
        newest = max(published, key=lambda r: _version_key(r["version"]))
        selected.append((newest, len(versions)))

    stats = {
        "store_rows": len(records),
        "datasets": len(by_source),
        "datasets_without_published_version": unpublished,
        "entries_to_ingest": len(selected),
    }
    return selected, stats


def _ingest(search, selected, batch_size: int, wait_seconds: float) -> Dict[str, Any]:
    """Ingest in GMetaList batches and wait for every task to finish."""
    client = search._get_client()
    accepted = 0
    errors: List[Dict[str, Any]] = []
    task_ids: List[str] = []
    for start in range(0, len(selected), batch_size):
        batch = selected[start:start + batch_size]
        gmeta = []
        for record, version_count in batch:
            try:
                gmeta.append(search.build_gmeta_entry(record, version_count=version_count))
            except Exception as exc:  # one bad record must not sink the batch
                errors.append({"source_id": record.get("source_id"), "error": str(exc)})
        if not gmeta:
            continue
        try:
            resp = client.ingest(
                search.index_id,
                {"ingest_type": "GMetaList", "ingest_data": {"gmeta": gmeta}},
            )
            data = getattr(resp, "data", None) or {}
            if data.get("task_id"):
                task_ids.append(data["task_id"])
            accepted += len(gmeta)
        except Exception as exc:
            for record, _ in batch:
                errors.append({"source_id": record.get("source_id"), "error": str(exc)})
        print(f"  batch {start // batch_size + 1}: {accepted} accepted, {len(errors)} errors",
              file=sys.stderr)

    # Acceptance is not completion (B-17): poll each task to a terminal state.
    failed_tasks: List[Dict[str, Any]] = []
    deadline = time.monotonic() + wait_seconds
    for task_id in task_ids:
        task = search._wait_for_task(task_id, deadline)
        state = str(task.get("state") or task.get("task_state") or "UNKNOWN")
        if state != "SUCCESS":
            failed_tasks.append({"task_id": task_id, "state": state})

    return {
        "accepted": accepted,
        "errors": errors,
        "task_ids": task_ids,
        "unconfirmed_tasks": failed_tasks,
    }


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "staging", "prod"], required=True)
    parser.add_argument("--execute", action="store_true",
                        help="Actually ingest (default: dry run that only reports).")
    parser.add_argument("--allow-prod", action="store_true",
                        help="Required to write the prod index.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Ingest at most N datasets (smoke test).")
    parser.add_argument("--no-stack", action="store_true",
                        help="Use env vars as-is instead of reading the deployed stack config.")
    parser.add_argument("--wait-seconds", type=float, default=TASK_WAIT_SECONDS,
                        help="Total time to wait for ingest tasks to finish.")
    parser.add_argument("--json", action="store_true", help="Print the report as JSON.")
    args = parser.parse_args(argv)

    if not args.no_stack:
        apply_env(resolve_env_from_stack(args.env))
    index_uuid = os.environ.get("SEARCH_INDEX_UUID", "")
    if index_uuid == LEGACY_SEARCH_INDEX_UUID:
        print("FATAL: refusing to touch the LEGACY v1 production index.", file=sys.stderr)
        return 2
    if args.env == "prod" and args.execute and not args.allow_prod:
        print("FATAL: writing the prod index requires --allow-prod.", file=sys.stderr)
        return 2
    if args.execute and os.environ.get("USE_MOCK_SEARCH", "").lower() == "true":
        print("FATAL: resolved config uses mock search; a reindex would be a no-op.",
              file=sys.stderr)
        return 2

    from v2.store import get_store

    records = get_store().list_all(limit=STORE_SCAN_LIMIT)
    selected, stats = select_index_records(records)
    if args.limit is not None:
        selected = selected[: args.limit]
    report: Dict[str, Any] = {
        "env": args.env,
        "index": index_uuid,
        "mode": "execute" if args.execute else "dry-run",
        **stats,
        "selected": len(selected),
    }

    exit_code = 0
    if args.execute:
        from v2.search_client import get_search_client

        result = _ingest(get_search_client(), selected, SEARCH_BATCH_SIZE, args.wait_seconds)
        report.update({
            "accepted": result["accepted"],
            "errors": result["errors"][:20],
            "error_count": len(result["errors"]),
            "task_count": len(result["task_ids"]),
            "unconfirmed_tasks": result["unconfirmed_tasks"],
        })
        if result["errors"] or result["unconfirmed_tasks"]:
            exit_code = 1

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        for key, value in report.items():
            print(f"{key}: {value}")
        if not args.execute:
            print("DRY RUN: pass --execute to ingest.")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
