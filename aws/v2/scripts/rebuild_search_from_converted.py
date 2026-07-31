#!/usr/bin/env python3
"""Rebuild a freshly-wiped v2 Search index from converted legacy datasets.

For the from-scratch index path (burndown D-18): wipe_search_index.py first,
then this — it builds submission records from a converted_datasets JSON
(latest-per-chain records only; filter before calling) with the CURRENT
GMeta builder and batch-ingests them. The store is never touched; store
consistency comes from the fact that the store already holds the same
migrated records. The LEGACY v1 production index is refused.

Usage:
  python v2/scripts/rebuild_search_from_converted.py --env staging \
      -i converted-latest.json --execute
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from v2.scripts.ingest_converted_datasets import (  # noqa: E402
    LEGACY_SEARCH_INDEX_UUID,
    apply_env,
    build_submission_record,
    resolve_env_from_stack,
)

SEARCH_BATCH_SIZE = 100


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", choices=["dev", "staging", "prod"], required=True)
    parser.add_argument("-i", "--input", required=True,
                        help="Converted datasets JSON ({'records': [...]}), latest-only.")
    parser.add_argument("--execute", action="store_true",
                        help="Actually ingest (default: report what would happen).")
    args = parser.parse_args(argv)

    apply_env(resolve_env_from_stack(args.env))
    index_uuid = os.environ.get("SEARCH_INDEX_UUID", "")
    if index_uuid == LEGACY_SEARCH_INDEX_UUID:
        print("\nFATAL: refusing to touch the LEGACY v1 production index.")
        return 2
    if os.environ.get("USE_MOCK_SEARCH", "").lower() == "true":
        print("\nFATAL: resolved config uses mock search; rebuild would be a no-op lie.")
        return 2

    data = json.load(open(args.input))
    records = data.get("records", [])
    print(f"Input: {len(records)} converted records (should be latest-per-chain only)")

    submissions = []
    errors = []
    for converted in records:
        try:
            submissions.append(build_submission_record(converted))
        except Exception as exc:
            errors.append({"source_id": converted.get("source_id"), "error": str(exc)})
    print(f"Built: {len(submissions)} submission records, {len(errors)} errors")
    for e in errors[:5]:
        print("  ERROR", e)

    if not args.execute:
        print("DRY RUN: pass --execute to ingest.")
        return 1 if errors else 0

    from v2.search_client import get_search_client

    search = get_search_client()
    ingested = 0
    search_errors = []
    for start in range(0, len(submissions), SEARCH_BATCH_SIZE):
        batch = submissions[start:start + SEARCH_BATCH_SIZE]
        result = search.batch_ingest(batch, batch_size=SEARCH_BATCH_SIZE)
        ingested += int(result.get("ingested") or 0)
        search_errors.extend(result.get("errors") or [])
        print(f"  batch {start // SEARCH_BATCH_SIZE + 1}: "
              f"{ingested} accepted so far, {len(search_errors)} errors")

    print(f"\nAccepted by Search: {ingested}/{len(submissions)}")
    if search_errors:
        print(f"Errors: {len(search_errors)}; first: {search_errors[0]}")
    return 1 if (errors or search_errors) else 0


if __name__ == "__main__":
    sys.exit(main())
