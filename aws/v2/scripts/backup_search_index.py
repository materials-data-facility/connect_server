#!/usr/bin/env python3
"""Read-only backup of a Globus Search index to gzipped JSON lines.

Enumerates every GMeta result in an index (offset paging through the 10k
window, marker-based scroll beyond it — the same enumeration as
purge_stale_search_entries.py) and writes one JSON object per line:

    {"subject": ..., "entries": [{"entry_id": ..., "content": {...}}, ...]}

Strictly read-only: only search queries are issued; no ingest, delete, or
store access of any kind. Safe against any index, including the legacy v1
production index.

Usage:
  # Back up the deployed staging index (resolves UUID from the stack):
  python v2/scripts/backup_search_index.py --env staging -o staging-index.jsonl.gz

  # Back up an explicit index UUID (e.g. the legacy v1 index):
  python v2/scripts/backup_search_index.py \
      --index 1a57bbe5-5272-477f-9d31-343b8258b7a5 -o legacy-index.jsonl.gz

Auth: confidential client via GLOBUS_CLIENT_ID/GLOBUS_CLIENT_SECRET env vars
(read access on the target index suffices), falling back to interactive
native-app login unless --non-interactive.

To restore, feed the JSON lines back through GlobusSearchClient.batch_ingest
(each line is one GMetaEntry group keyed by subject).
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from v2.scripts.legacy_auth import get_legacy_search_client  # noqa: E402
from v2.scripts.purge_stale_search_entries import (  # noqa: E402
    OFFSET_WINDOW,
    PAGE_SIZE,
)
from v2.scripts.ingest_converted_datasets import (  # noqa: E402
    apply_env,
    resolve_env_from_stack,
)


def _page_query(
    client: Any,
    index_uuid: str,
    offset: int,
    limit: int,
    query: str,
    advanced: bool,
) -> Dict[str, Any]:
    return client.post_search(
        index_uuid,
        {"q": query, "advanced": advanced},
        offset=offset,
        limit=limit,
    ).data


def _scroll_page_sized(
    client: Any,
    index_uuid: str,
    marker: Optional[str],
    page_size: int,
    query: str = "*",
    advanced: bool = False,
) -> Dict[str, Any]:
    data = {"q": query, "advanced": advanced, "limit": page_size}
    if marker is None:
        return client.scroll(index_uuid, data).data
    return client.scroll(index_uuid, data, marker=marker).data


def enumerate_raw(
    client: Any,
    index_uuid: str,
    limit: int = 0,
    page_size: int = 0,
    sink: Optional[Any] = None,
    query: str = "*",
):
    """Yield RAW GMetaResult dicts (subject + full entries) page by page.

    purge_stale_search_entries.enumerate_index flattens results for
    classification; a backup must preserve the complete entry content, so
    this walks the same offset/scroll pagination but yields items verbatim.
    With ``sink``, stream each item instead of accumulating in memory
    (required for multi-million-entry indexes); meta['written'] carries the
    count. ``query`` scopes the backup (advanced syntax when not '*').
    """
    page_size = page_size or PAGE_SIZE
    advanced = query != "*"
    first = _page_query(
        client, index_uuid, 0,
        min(page_size, limit) if limit else page_size,
        query, advanced,
    )
    index_total = int(first.get("total") or 0)
    target = min(index_total, limit) if limit > 0 else index_total
    meta = {"index_total_before": index_total, "complete": True, "warnings": [], "method": "offset"}
    if target == 0:
        return [], meta

    items: list = []
    count = 0

    def take(page_items: list) -> None:
        nonlocal count
        for item in page_items:
            if sink is not None:
                sink(item)
            else:
                items.append(item)
            count += 1

    if target <= OFFSET_WINDOW:
        page = first
        offset = 0
        while count < target:
            page_items = (page.get("gmeta") or [])[: target - count]
            take(page_items)
            if count >= target:
                break
            if not page_items:
                meta["complete"] = False
                meta["warnings"].append("offset pagination returned an empty page early")
                break
            offset += len(page_items)
            page = _page_query(
                client, index_uuid, offset,
                min(page_size, target - offset), query, advanced,
            )
    else:
        meta["method"] = "scroll"
        marker = None
        seen = set()
        max_pages = ((target + page_size - 1) // page_size) + 10
        for _ in range(max_pages):
            page = _scroll_page_sized(client, index_uuid, marker, page_size, query, advanced)
            page_items = page.get("gmeta") or []
            take(page_items)
            if count % 100000 < page_size:
                print(f"  ... {count} / {target}", flush=True)
            marker = page.get("marker")
            if not page.get("has_next_page") or not page_items:
                break
            if marker in seen or marker is None:
                meta["complete"] = False
                meta["warnings"].append("scroll marker repeated or missing")
                break
            seen.add(marker)
        else:
            meta["complete"] = False
            meta["warnings"].append("scroll exceeded the bounded page count")
    meta["written"] = count
    return items, meta


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--env", choices=["dev", "staging", "prod"], default=None,
        help="Back up the deployed mdf-connect-v2-ENV stack's search index.",
    )
    target.add_argument(
        "--index", default=None,
        help="Back up an explicit Globus Search index UUID.",
    )
    parser.add_argument(
        "-o", "--output", required=True,
        help="Output path. .gz suffix gzips; anything else is plain JSONL.",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Cap enumerated results for testing (0 = all).",
    )
    parser.add_argument(
        "--non-interactive", action="store_true",
        help="Fail instead of falling back to browser login.",
    )
    parser.add_argument(
        "--page-size", type=int, default=0,
        help="Results per request (default 100; Globus Search allows up to 1000).",
    )
    parser.add_argument(
        "--query", default="*",
        help=(
            "Scope the backup with an advanced Globus Search query, e.g. "
            "'mdf.resource_type:\"dataset\"' for dataset entries only "
            "(default '*': the whole index)."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = _parse_args(argv)

    if args.env:
        print(f"Resolving config from stack mdf-connect-v2-{args.env}...")
        resolved = resolve_env_from_stack(args.env)
        apply_env(resolved)
        index_uuid = os.environ.get("SEARCH_INDEX_UUID", "")
        if not index_uuid:
            print("ERROR: stack did not provide SEARCH_INDEX_UUID")
            return 1
    else:
        index_uuid = args.index

    print(f"Backing up index {index_uuid} (read-only)...")
    client = get_legacy_search_client(interactive_ok=not args.non_interactive)

    out_path = Path(args.output)
    opener = gzip.open if out_path.suffix == ".gz" else open
    # Entries stream to disk page-by-page (multi-million-entry indexes must
    # not accumulate in memory); run metadata goes to a .meta.json sidecar.
    with opener(out_path, "wt", encoding="utf-8") as fh:
        def sink(item: Dict[str, Any]) -> None:
            fh.write(json.dumps(item, separators=(",", ":"), default=str) + "\n")

        _, metadata = enumerate_raw(
            client, index_uuid, limit=args.limit, page_size=args.page_size,
            sink=sink, query=args.query,
        )

    meta_path = Path(str(out_path) + ".meta.json")
    meta_path.write_text(
        json.dumps(
            {
                "index_uuid": index_uuid,
                "query": args.query,
                "index_total_at_backup": metadata.get("index_total_before"),
                "written": metadata.get("written"),
                "enumeration": metadata.get("method"),
                "complete": metadata.get("complete"),
                "warnings": metadata.get("warnings"),
            },
            indent=2,
        )
    )

    print(f"  Index total:   {metadata.get('index_total_before')}")
    print(f"  Written:       {metadata.get('written')} GMeta results -> {out_path}")
    print(f"  Metadata:      {meta_path}")
    if not metadata.get("complete", False):
        print("  WARNING: enumeration incomplete; backup does not cover the whole index:")
        for w in metadata.get("warnings", []):
            print(f"    - {w}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
