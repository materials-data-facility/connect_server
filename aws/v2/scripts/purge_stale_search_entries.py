#!/usr/bin/env python3
"""Find and optionally purge Search entries with no canonical v2 store record.

The deployed v2 DynamoDB table is the authority.  Search entries whose exact
``mdf.source_id`` has no versions in that table are stale.  Legacy source ids
ending in ``_vN`` or ``_vN.N`` are reported separately when their stripped
base exists, because those entries have been superseded by the canonical
version-independent detail subject.

Dry-run is the default.  Deletion requires ``--execute``; production also
requires ``--allow-prod``.  The legacy v1 production Search index is always
refused.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlsplit

# Allow imports from aws/ root and from this scripts/ directory.  v2 modules
# remain lazy imports because v2.config reads environment variables at import.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))

from ingest_converted_datasets import (  # noqa: E402
    LEGACY_SEARCH_INDEX_UUID,
    apply_env,
    resolve_env_from_stack,
)
from legacy_auth import get_legacy_search_client  # noqa: E402


PAGE_SIZE = 100
OFFSET_WINDOW = 10000
SSM_CLIENT_ID_PARAM = "/mdf/globus-client-id"
SSM_CLIENT_SECRET_PARAM = "/mdf/globus-client-secret"
_VERSION_SUFFIX = re.compile(r"^(?P<base>.+)_v\d+(?:\.\d+)*$")
VersionLookup = Tuple[Optional[List[Dict[str, Any]]], Optional[str]]


class GuardRefusal(RuntimeError):
    """A hard target-safety guard refused the requested run."""


def check_guards(
    index_uuid: str, env: str, execute: bool, allow_prod: bool
) -> None:
    """Raise GuardRefusal when the target or write intent is unsafe."""
    if index_uuid == LEGACY_SEARCH_INDEX_UUID:
        raise GuardRefusal(
            "SEARCH_INDEX_UUID resolved to {!r}, the legacy v1 production "
            "Search index. This index must never be modified or used as the "
            "purge source.".format(index_uuid)
        )
    if execute and env == "prod" and not allow_prod:
        raise GuardRefusal(
            "production deletion requires both --execute and --allow-prod"
        )


def strip_version_suffix(source_id: str) -> Optional[str]:
    """Strip a legacy ``_vN[.N...]`` suffix, or return None."""
    match = _VERSION_SUFFIX.match(source_id or "")
    return match.group("base") if match else None


def _response_data(response: Any) -> Dict[str, Any]:
    data = getattr(response, "data", response)
    if not isinstance(data, dict):
        raise TypeError("Globus Search returned a non-object response")
    return data


def _source_id_from_subject(subject: str) -> str:
    """Return the decoded final path segment of a Search subject."""
    path = urlsplit(subject or "").path.rstrip("/")
    return unquote(path.rsplit("/", 1)[-1]) if path else ""


def _title_from_content(content: Dict[str, Any]) -> str:
    dc = content.get("dc") or {}
    title = dc.get("title")
    if isinstance(title, str):
        return title
    if isinstance(title, dict):
        return str(title.get("title") or "")

    titles = dc.get("titles") or []
    if isinstance(titles, list) and titles:
        first = titles[0]
        if isinstance(first, dict):
            return str(first.get("title") or "")
        return str(first)
    return ""


def gmeta_entry_records(gmeta: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten GMeta results into one classification record per Entry.

    Search normally returns a GMetaResult per subject with an ``entries``
    array.  Accepting an already-normalized record also keeps the classifier
    convenient for unit tests and offline report processing.
    """
    records = []
    for result in gmeta:
        if (
            "source_id" in result
            and "entries" not in result
            and "content" not in result
        ):
            records.append(
                {
                    "subject": str(result.get("subject") or ""),
                    "source_id": str(result.get("source_id") or ""),
                    "title": str(result.get("title") or ""),
                }
            )
            continue

        subject = str(result.get("subject") or "")
        entries = result.get("entries")
        if not isinstance(entries, list) or not entries:
            entries = [result]

        for entry in entries:
            content = entry.get("content") if isinstance(entry, dict) else {}
            if not isinstance(content, dict):
                content = {}
            mdf = content.get("mdf") or {}
            source_id = mdf.get("source_id") if isinstance(mdf, dict) else None
            source_id = str(source_id or _source_id_from_subject(subject))
            records.append(
                {
                    "subject": subject,
                    "source_id": source_id,
                    "title": _title_from_content(content),
                }
            )
    return records


class ApiCardResolver:
    """Store-shaped adapter that resolves source_ids via the public API.

    Classifies through ``GET {base}/card/{source_id}`` — the exact user-facing
    resolution path (including legacy-id redirects) — for operators whose AWS
    credentials lack dynamodb:Query. 200 counts as present, 404 as absent;
    anything else raises so the record lands in store_errors and blocks
    deletion. Throttles politely under the API's per-container rate limit.
    """

    def __init__(self, base_url: str, pause_seconds: float = 0.35) -> None:
        self.base_url = base_url.rstrip("/")
        self.pause_seconds = pause_seconds

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        import time as _time
        import urllib.error
        import urllib.parse
        import urllib.request

        url = "{}/card/{}".format(self.base_url, urllib.parse.quote(source_id, safe=""))
        last_error: Optional[str] = None
        for attempt in range(4):
            _time.sleep(self.pause_seconds if attempt == 0 else 2.0 * attempt)
            try:
                with urllib.request.urlopen(url, timeout=30):
                    return [{"resolved_via": "api"}]
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    return []
                last_error = "HTTP {} from {}".format(exc.code, url)
                if exc.code not in (429, 500, 502, 503, 504):
                    break
            except Exception as exc:  # noqa: BLE001 — uncertainty must not read as absence
                last_error = "{}: {}".format(type(exc).__name__, exc)
        raise RuntimeError(last_error or "unknown API resolution failure")


def classify_entries(
    gmeta: Iterable[Dict[str, Any]], store: Any
) -> Dict[str, Any]:
    """Classify canned or live GMeta entries against ``store.list_versions``.

    Store lookup errors are not treated as absence: those records are held out
    of every deletion class and reported so execution can fail safely.
    """
    records = gmeta_entry_records(gmeta)
    versions_cache = {}  # type: Dict[str, VersionLookup]

    def lookup(source_id: str) -> VersionLookup:
        if source_id not in versions_cache:
            try:
                versions_cache[source_id] = (store.list_versions(source_id), None)
            except Exception as exc:  # Preserve uncertainty; never call it stale.
                versions_cache[source_id] = (None, str(exc))
        return versions_cache[source_id]

    classified = {
        "enumerated": len(records),
        "kept": [],
        "stale_superseded": [],
        "stale_orphan": [],
        "test_records_resolvable": 0,
        "store_errors": [],
    }

    for record in records:
        source_id = record["source_id"]
        exact, error = lookup(source_id)
        if error is not None:
            classified["store_errors"].append(
                dict(record, error=error, lookup_source_id=source_id)
            )
            continue

        if exact:
            classified["kept"].append(record)
            if source_id.startswith("mdf-"):
                classified["test_records_resolvable"] += 1
            continue

        base = strip_version_suffix(source_id)
        if base:
            base_versions, base_error = lookup(base)
            if base_error is not None:
                classified["store_errors"].append(
                    dict(record, error=base_error, lookup_source_id=base)
                )
                continue
            if base_versions:
                classified["stale_superseded"].append(
                    dict(record, canonical_source_id=base)
                )
                continue

        classified["stale_orphan"].append(record)

    return classified


def _search_page(
    client: Any, index_uuid: str, offset: int, limit: int
) -> Dict[str, Any]:
    return _response_data(
        client.post_search(
            index_uuid,
            {"q": "*", "advanced": False},
            offset=offset,
            limit=limit,
        )
    )


def _scroll_page(
    client: Any, index_uuid: str, marker: Optional[str]
) -> Dict[str, Any]:
    data = {"q": "*", "advanced": False, "limit": PAGE_SIZE}
    if marker is None:
        return _response_data(client.scroll(index_uuid, data))
    return _response_data(client.scroll(index_uuid, data, marker=marker))


def enumerate_index(
    client: Any, index_uuid: str, limit: int = 0
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Enumerate the target result window with offset paging or scroll.

    POST offset paging is used through the Search 10k window.  For a larger
    uncapped window, enumeration restarts with marker-based scroll so no page
    is skipped or duplicated.  Marker and page-count checks make a malformed
    or changing response stop with an incomplete report rather than loop.
    """
    first_limit = min(PAGE_SIZE, limit) if limit > 0 else PAGE_SIZE
    first = _search_page(client, index_uuid, 0, first_limit)
    index_total = int(first.get("total") or 0)
    target = min(index_total, limit) if limit > 0 else index_total
    metadata = {
        "method": "offset" if target <= OFFSET_WINDOW else "scroll",
        "page_size": PAGE_SIZE,
        "index_total_before": index_total,
        "limited": bool(limit > 0 and limit < index_total),
        "requested_limit": limit,
        "complete": True,
        "warnings": [],
        "gmeta_results_enumerated": 0,
    }
    if target == 0:
        return [], metadata

    gmeta_results = []  # type: List[Dict[str, Any]]
    if target <= OFFSET_WINDOW:
        page = first
        offset = 0
        while len(gmeta_results) < target:
            page_items = page.get("gmeta") or []
            if not isinstance(page_items, list):
                raise TypeError("Globus Search response gmeta is not a list")
            remaining = target - len(gmeta_results)
            gmeta_results.extend(page_items[:remaining])
            if len(gmeta_results) >= target:
                break
            if not page_items:
                metadata["complete"] = False
                metadata["warnings"].append(
                    "offset pagination returned an empty page before the target"
                )
                break
            offset += len(page_items)
            if offset >= OFFSET_WINDOW:
                metadata["complete"] = False
                metadata["warnings"].append(
                    "offset pagination reached the 10,000-result window"
                )
                break
            page = _search_page(
                client, index_uuid, offset, min(PAGE_SIZE, target - offset)
            )
    else:
        marker = None  # type: Optional[str]
        seen_markers = set()
        # Ten pages of tolerance allow modest index growth during a read while
        # still placing a finite bound on a stream of ever-changing markers.
        max_pages = ((target + PAGE_SIZE - 1) // PAGE_SIZE) + 10
        for _page_number in range(max_pages):
            page = _scroll_page(client, index_uuid, marker)
            page_items = page.get("gmeta") or []
            if not isinstance(page_items, list):
                raise TypeError("Globus Search scroll response gmeta is not a list")
            remaining = target - len(gmeta_results)
            gmeta_results.extend(page_items[:remaining])
            if len(gmeta_results) >= target or not page.get("has_next_page"):
                break
            next_marker = page.get("marker")
            if not page_items:
                metadata["complete"] = False
                metadata["warnings"].append(
                    "scroll returned an empty page while claiming another page"
                )
                break
            if not next_marker:
                metadata["complete"] = False
                metadata["warnings"].append(
                    "scroll response omitted the next-page marker"
                )
                break
            if next_marker in seen_markers:
                metadata["complete"] = False
                metadata["warnings"].append(
                    "scroll repeated a marker; stopped to avoid an infinite loop"
                )
                break
            seen_markers.add(next_marker)
            marker = str(next_marker)
        else:
            metadata["complete"] = False
            metadata["warnings"].append(
                "scroll exceeded its bounded page count; index may be changing"
            )

        if len(gmeta_results) < target and metadata["complete"]:
            metadata["complete"] = False
            metadata["warnings"].append(
                "scroll ended before the index-reported target was reached"
            )

    metadata["gmeta_results_enumerated"] = len(gmeta_results)
    records = gmeta_entry_records(gmeta_results)
    if limit > 0:
        records = records[:limit]
    return records, metadata


def query_total(client: Any, index_uuid: str) -> int:
    """Return the current Search total with a minimal POST query."""
    return int(_search_page(client, index_uuid, 0, 1).get("total") or 0)


def _safe_stale_subjects(classified: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """Return unique deletable stale subjects and mixed/invalid blockers."""
    kept_subjects = {item["subject"] for item in classified["kept"]}
    error_subjects = {item["subject"] for item in classified["store_errors"]}
    stale_subjects = {
        item["subject"]
        for key in ("stale_superseded", "stale_orphan")
        for item in classified[key]
    }
    blocked = sorted(
        subject
        for subject in stale_subjects
        if not subject or subject in kept_subjects or subject in error_subjects
    )
    return sorted(stale_subjects - set(blocked)), blocked


def delete_stale_subjects(
    client: Any, index_uuid: str, subjects: Iterable[str]
) -> Dict[str, Any]:
    """Submit idempotent per-subject deletes and report API acceptance."""
    # Defense in depth: check_guards() already refused the legacy index at
    # startup, but this function is the only place deletes are issued, so it
    # re-asserts independently — no caller mistake can aim it at legacy.
    if index_uuid == LEGACY_SEARCH_INDEX_UUID:
        raise GuardRefusal(
            "delete_stale_subjects called with the LEGACY v1 production index"
        )
    subject_list = list(subjects)
    accepted = []
    errors = []
    for position, subject in enumerate(subject_list, 1):
        try:
            response = client.delete_subject(index_uuid, subject)
            data = _response_data(response)
            accepted.append(
                {"subject": subject, "task_id": data.get("task_id")}
            )
        except Exception as exc:
            errors.append({"subject": subject, "error": str(exc)})
        if position % 50 == 0 or position == len(subject_list):
            print(
                "  Delete progress: {}/{} submitted ({} accepted, {} errors)".format(
                    position, len(subject_list), len(accepted), len(errors)
                )
            )
    return {"accepted": accepted, "errors": errors}


def _print_samples(label: str, records: List[Dict[str, Any]], show: int) -> None:
    print(
        "\n{} samples (showing {} of {}):".format(
            label, min(show, len(records)), len(records)
        )
    )
    for record in records[:show]:
        title = record.get("title") or "(untitled)"
        print("  - {} | {}".format(record.get("subject") or "(no subject)", title))


def _write_report(path: str, report: Dict[str, Any]) -> None:
    absolute = os.path.abspath(path)
    parent = os.path.dirname(absolute)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent)
    with open(absolute, "w") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print("\nJSON report: {}".format(absolute))


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env", choices=["dev", "staging", "prod"], required=True,
        help="Resolve the deployed mdf-connect-v2-ENV stack configuration.",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Delete classified stale subjects (default is read-only dry-run).",
    )
    parser.add_argument(
        "--allow-prod", action="store_true",
        help="Second required flag for --execute against prod.",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Cap entries enumerated for testing (0 = all).",
    )
    parser.add_argument(
        "--report-file", default=None,
        help="Write the complete audit report as JSON at PATH.",
    )
    parser.add_argument(
        "--show", type=int, default=20,
        help="Stale samples to print per class (default: 20).",
    )
    parser.add_argument(
        "--resolve-via-api", metavar="BASE_URL", default=None,
        help=(
            "Classify entries via GET BASE_URL/card/{source_id} (the public "
            "user-facing resolution path) instead of direct DynamoDB — for "
            "operators without dynamodb:Query."
        ),
    )
    args = parser.parse_args(argv)
    if args.limit < 0:
        parser.error("--limit must be zero or greater")
    if args.show < 0:
        parser.error("--show must be zero or greater")
    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)

    print("Resolving config from stack mdf-connect-v2-{}...".format(args.env))
    try:
        resolved = resolve_env_from_stack(args.env)
        apply_env(resolved)
    except SystemExit as exc:
        print("ERROR: could not resolve deployed configuration: {}".format(exc))
        return 1
    except Exception as exc:
        print("ERROR: could not resolve deployed configuration: {}".format(exc))
        return 1

    index_uuid = os.environ.get("SEARCH_INDEX_UUID", "")
    table_name = os.environ.get("DYNAMO_SUBMISSIONS_TABLE", "")
    region = os.environ.get("AWS_REGION", "us-east-1")
    print("  SEARCH_INDEX_UUID:        {}".format(index_uuid or "(not set)"))
    print("  DYNAMO_SUBMISSIONS_TABLE: {}".format(table_name or "(not set)"))
    print("  AWS_REGION:               {}".format(region))
    print(
        "  Mode:                     {}".format(
            "EXECUTE" if args.execute else "DRY RUN"
        )
    )

    try:
        check_guards(index_uuid, args.env, args.execute, args.allow_prod)
    except GuardRefusal as exc:
        print("\nFATAL GUARD REFUSAL: {}".format(exc))
        return 2

    if not index_uuid or not table_name:
        print(
            "\nERROR: deployed configuration must provide SEARCH_INDEX_UUID "
            "and DYNAMO_SUBMISSIONS_TABLE"
        )
        return 1

    missing_auth = [
        name
        for name in ("GLOBUS_CLIENT_ID", "GLOBUS_CLIENT_SECRET")
        if not os.environ.get(name)
    ]
    if missing_auth:
        print(
            "\nERROR: missing {}. Export confidential-client credentials "
            "before running; the operator SSM parameters are {} and {} "
            "(use --with-decryption for the secret).".format(
                ", ".join(missing_auth),
                SSM_CLIENT_ID_PARAM,
                SSM_CLIENT_SECRET_PARAM,
            )
        )
        return 1

    try:
        client = get_legacy_search_client(interactive_ok=False)
        if args.resolve_via_api:
            store: Any = ApiCardResolver(args.resolve_via_api)
            print("  Resolution:               public API ({})".format(args.resolve_via_api))
        else:
            from v2.store import get_store

            store = get_store()
    except Exception as exc:
        print("\nERROR: could not initialize Search/store clients: {}".format(exc))
        return 1

    print(
        "\nEnumerating Search (credentials need ingest/admin delete rights on this index)..."
    )
    try:
        entries, pagination = enumerate_index(
            client, index_uuid, limit=args.limit
        )
        classified = classify_entries(entries, store)
    except Exception as exc:
        print("ERROR: enumeration/classification failed: {}".format(exc))
        return 1

    stale_superseded = classified["stale_superseded"]
    stale_orphan = classified["stale_orphan"]
    safe_subjects, blocked_subjects = _safe_stale_subjects(classified)
    stale_total = len(stale_superseded) + len(stale_orphan)

    counts = {
        "enumerated": classified["enumerated"],
        "kept": len(classified["kept"]),
        "stale_superseded": len(stale_superseded),
        "stale_orphan": len(stale_orphan),
        "stale_total": stale_total,
        "test_records_resolvable": classified["test_records_resolvable"],
        "stale_subjects_safe": len(safe_subjects),
        "stale_subjects_blocked": len(blocked_subjects),
        "store_errors": len(classified["store_errors"]),
        "delete_accepted": 0,
        "delete_errors": 0,
    }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": args.env,
        "mode": "execute" if args.execute else "dry-run",
        "index_uuid": index_uuid,
        "dynamo_submissions_table": table_name,
        "aws_region": region,
        "pagination": pagination,
        "counts": counts,
        "records": {
            "stale_superseded": stale_superseded,
            "stale_orphan": stale_orphan,
            "store_errors": classified["store_errors"],
            "blocked_subjects": blocked_subjects,
        },
        "deletion": {"accepted": [], "errors": []},
    }

    print("\nClassification:")
    print(
        "  Index total before:         {}".format(
            pagination["index_total_before"]
        )
    )
    print("  Enumerated:                 {}".format(counts["enumerated"]))
    print("  Kept:                       {}".format(counts["kept"]))
    print("  Stale, superseded by base:  {}".format(counts["stale_superseded"]))
    print("  Stale, orphan:              {}".format(counts["stale_orphan"]))
    print(
        "  Test records (resolvable):  {}".format(
            counts["test_records_resolvable"]
        )
    )
    print("  Store lookup errors:        {}".format(counts["store_errors"]))
    print("  Pagination:                 {}".format(pagination["method"]))
    if pagination["limited"]:
        print("  Enumeration capped:         {}".format(args.limit))
    for warning in pagination["warnings"]:
        print("  WARNING: {}".format(warning))

    _print_samples("Stale superseded-by-base", stale_superseded, args.show)
    _print_samples("Stale orphan", stale_orphan, args.show)

    has_read_errors = (
        bool(classified["store_errors"]) or not pagination["complete"]
    )
    has_safety_blockers = bool(blocked_subjects)
    if args.execute and has_read_errors:
        print(
            "\nERROR: refusing deletion because enumeration/store lookups were incomplete."
        )
    elif args.execute:
        if blocked_subjects:
            print(
                "\nWARNING: {} stale subject(s) blocked because they were empty "
                "or also contained a kept/error record.".format(len(blocked_subjects))
            )
        print(
            "\nSubmitting {} stale subject delete(s)...".format(
                len(safe_subjects)
            )
        )
        deletion = delete_stale_subjects(client, index_uuid, safe_subjects)
        report["deletion"] = deletion
        counts["delete_accepted"] = len(deletion["accepted"])
        counts["delete_errors"] = len(deletion["errors"])
        try:
            pagination["index_total_after"] = query_total(client, index_uuid)
        except Exception as exc:
            pagination["index_total_after"] = None
            pagination["warnings"].append(
                "final total query failed: {}".format(exc)
            )
            counts["delete_errors"] += 1
        print("  Deletes accepted:           {}".format(counts["delete_accepted"]))
        print("  Delete errors:              {}".format(counts["delete_errors"]))
        print(
            "  Index total after (immediate): {}".format(
                pagination.get("index_total_after")
            )
        )
    else:
        pagination["index_total_after"] = None
        print("\nDRY RUN: no Search entries were deleted; the store was not modified.")

    if args.report_file:
        try:
            _write_report(args.report_file, report)
        except Exception as exc:
            print("ERROR: could not write JSON report: {}".format(exc))
            return 1

    if has_read_errors or has_safety_blockers or counts["delete_errors"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
