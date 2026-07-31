#!/usr/bin/env python3
"""Wipe ALL entries from a v2 Globus Search index (delete_by_query q=*).

For rebuilding a v2 index from scratch (e.g. after generational pollution —
burndown D-18). The LEGACY v1 production index is refused unconditionally,
in two places. Take a backup_search_index.py snapshot first; this is
destructive for the target index (and only the index — no store access).

Usage:
  python v2/scripts/wipe_search_index.py --env staging --execute
  # prod additionally requires --allow-prod
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from v2.scripts.legacy_auth import get_legacy_search_client  # noqa: E402
from v2.scripts.purge_stale_search_entries import (  # noqa: E402
    LEGACY_SEARCH_INDEX_UUID,
)
from v2.scripts.ingest_converted_datasets import (  # noqa: E402
    apply_env,
    resolve_env_from_stack,
)


def _refuse_legacy(index_uuid: str) -> None:
    if index_uuid == LEGACY_SEARCH_INDEX_UUID:
        print(
            "\nFATAL: refusing to touch the LEGACY v1 production index "
            f"({LEGACY_SEARCH_INDEX_UUID})."
        )
        sys.exit(2)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", choices=["dev", "staging", "prod"], required=True)
    parser.add_argument("--execute", action="store_true",
                        help="Actually wipe (default: print what would happen).")
    parser.add_argument("--allow-prod", action="store_true",
                        help="Second required flag for --execute against prod.")
    args = parser.parse_args(argv)

    resolved = resolve_env_from_stack(args.env)
    apply_env(resolved)
    index_uuid = os.environ.get("SEARCH_INDEX_UUID", "")
    _refuse_legacy(index_uuid)
    if args.env == "prod" and args.execute and not args.allow_prod:
        print("\nFATAL: wiping prod requires both --execute and --allow-prod.")
        return 2

    client = get_legacy_search_client(interactive_ok=False)
    total = client.post_search(index_uuid, {"q": "*", "advanced": False}, limit=1).data.get("total")
    print(f"Index {index_uuid} ({args.env}): {total} entries")

    if not args.execute:
        print("DRY RUN: pass --execute to wipe.")
        return 0

    _refuse_legacy(index_uuid)  # re-assert at the destructive call site
    try:
        resp = client.delete_by_query(index_uuid, {"q": "*", "advanced": False})
        task_id = resp.data.get("task_id")
        print(f"delete_by_query accepted, task {task_id}; polling total...")
    except Exception as exc:
        # delete_by_query needs the index ADMIN role; a writer can still
        # delete per subject (same operation the purge tool uses).
        print(f"delete_by_query unavailable ({str(exc)[:80]}...); "
              "falling back to per-subject deletion")
        from v2.scripts.purge_stale_search_entries import (
            delete_stale_subjects,
            enumerate_index,
        )
        records, meta = enumerate_index(client, index_uuid)
        subjects = sorted({r["subject"] for r in records if r.get("subject")})
        print(f"  enumerated {len(records)} results, {len(subjects)} unique subjects")
        outcome = delete_stale_subjects(client, index_uuid, subjects)
        errs = outcome.get("errors", [])
        if errs:
            print(f"  {len(errs)} delete errors; first: {errs[0]}")
    for _ in range(60):
        time.sleep(5)
        now = client.post_search(index_uuid, {"q": "*", "advanced": False}, limit=1).data.get("total")
        print(f"  total: {now}")
        if now == 0:
            print("Index empty.")
            return 0
    print("WARNING: index not yet empty after polling window; task may still be processing.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
