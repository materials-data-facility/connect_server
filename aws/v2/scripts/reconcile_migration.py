#!/usr/bin/env python3
"""Reconcile the v1→v2 migration: detect drift between what *should* be in v2
and what actually landed in the v2 store (and, optionally, the search index).

For the ongoing-bridge sync this is the safety check after each delta run:
it answers "is every dataset we converted actually present, published, and
discoverable in v2 — and is its metadata good enough to render?"

Source of truth for "expected" is converted_datasets.json (output of
convert_production_datasets.py). The "actual" side is read from the deployed
v2 store via the same env-resolution the ingest script uses.

Usage:
    cd cs/aws
    # Reconcile against the deployed prod store
    PYTHONPATH=. python v2/scripts/reconcile_migration.py --env prod

    # Local SQLite, also probe the search index, machine-readable output
    PYTHONPATH=. STORE_BACKEND=sqlite USE_MOCK_SEARCH=true \
        python v2/scripts/reconcile_migration.py --check-search --json

Exit code is 1 for missing records, stale content, Search-pending latest
datasets, or (with --check-search) missing Search subjects. Pre-hash records
are reported as ``never_hashed`` warnings but do not fail reconciliation.
"""

import argparse
import json
import os
import sys

# Allow imports from aws/ root and from this scripts/ dir.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))

# Reuse the ingest script's stack/env resolution and record builder so the two
# stay consistent (same table, same versioned ids).
from ingest_converted_datasets import (  # noqa: E402
    apply_env,
    build_submission_record,
    compute_sync_content_hash,
    resolve_env_from_stack,
)


def _expected_version(converted):
    return str(converted.get("metadata", {}).get("version", "1.0"))


def _has_title(meta):
    t = (meta.get("title") or "").strip()
    return bool(t) and t.lower() != "untitled"


def _has_authors(meta):
    authors = meta.get("authors") or []
    return any((a or {}).get("name") for a in authors if isinstance(a, dict))


def reconcile(records, *, check_search=False, show=10):
    """Compare expected (converted) records against the v2 store/search."""
    from v2.store import get_store

    store = get_store()
    search = None
    search_subject = None
    if check_search:
        try:
            from v2.search_client import get_search_client

            search = get_search_client()
            read_client = search._get_read_client()  # noqa: SLF001 — exact-subject probe
            index_id = getattr(search, "index_id", None)
        except Exception as exc:  # pragma: no cover - depends on Globus creds
            print(f"  (search check unavailable: {exc}; skipping --check-search)")
            search = None

    report = {
        "expected": len(records),
        "in_store": 0,
        "missing_from_store": [],
        "never_hashed": 0,
        "never_hashed_records": [],
        "stale_content": 0,
        "stale_content_records": [],
        "search_pending": 0,
        "search_pending_records": [],
        "not_published": [],
        "no_legacy_id": 0,            # migrated records whose old id won't redirect
        "missing_title": [],
        "missing_authors": [],
        "missing_doi": 0,
        "in_search": 0,
        "missing_from_search": [],
        "search_checked": bool(search),
    }

    checked_search_pending = set()
    for converted in records:
        source_id = converted.get("source_id")
        version = _expected_version(converted)
        meta = converted.get("metadata", {}) or {}

        # Quality checks on the expected metadata (independent of the store).
        if not _has_title(meta):
            report["missing_title"].append(source_id)
        if not _has_authors(meta):
            report["missing_authors"].append(source_id)
        if not converted.get("doi"):
            report["missing_doi"] += 1

        # Store presence.
        try:
            stored = store.get_submission(source_id, version)
        except Exception as exc:
            report["missing_from_store"].append(f"{source_id}-{version} (store error: {exc})")
            continue

        if not stored:
            report["missing_from_store"].append(f"{source_id}-{version}")
            continue

        report["in_store"] += 1
        expected_hash = compute_sync_content_hash(converted)
        stored_hash = stored.get("sync_content_hash")
        if not stored_hash:
            report["never_hashed"] += 1
            report["never_hashed_records"].append(
                f"{source_id}-{version}"
            )
        elif stored_hash != expected_hash:
            report["stale_content"] += 1
            report["stale_content_records"].append(
                f"{source_id}-{version} (sync_content_hash differs)"
            )

        # Search owns one version-less subject per dataset, so pending state is
        # determined once from the store-derived newest published version.
        if source_id not in checked_search_pending:
            checked_search_pending.add(source_id)
            try:
                published = [
                    item for item in store.list_versions(source_id)
                    if item.get("status") == "published"
                ]
                latest = max(
                    published,
                    key=lambda item: [
                        (
                            (0, int(part), "")
                            if part.isdigit()
                            else (1, 0, part)
                        )
                        for part in str(item.get("version") or "0").split(".")
                    ],
                ) if published else None
            except Exception:
                latest = None
            if (
                latest
                and latest.get("sync_content_hash")
                and latest.get("search_synced_hash")
                != latest.get("sync_content_hash")
            ):
                report["search_pending"] += 1
                report["search_pending_records"].append(
                    "{}-{}".format(source_id, latest.get("version"))
                )

        if stored.get("status") != "published":
            report["not_published"].append(f"{source_id}-{version} ({stored.get('status')})")

        # Will the old (v1) id still resolve via the legacy redirect?
        legacy = converted.get("legacy_source_id")
        if legacy and legacy != source_id and not stored.get("legacy_source_id"):
            report["no_legacy_id"] += 1

        # Optional exact search presence probe.
        if search is not None:
            try:
                subject = search.build_gmeta_entry(build_submission_record(converted))["subject"]
                read_client.get_subject(index_id, subject)
                report["in_search"] += 1
            except Exception:
                report["missing_from_search"].append(f"{source_id}-{version}")

    return report


def _print_report(report, show):
    print(f"\n{'='*56}")
    print("Migration reconciliation")
    print(f"{'='*56}")
    print(f"  Expected (converted):   {report['expected']}")
    print(f"  In v2 store:            {report['in_store']}")
    print(f"  Missing from store:     {len(report['missing_from_store'])}")
    for sid in report["missing_from_store"][:show]:
        print(f"    - {sid}")
    if len(report["missing_from_store"]) > show:
        print(f"    ... and {len(report['missing_from_store']) - show} more")

    print(f"  Never hashed (warning): {report['never_hashed']}")
    for sid in report["never_hashed_records"][:show]:
        print(f"    - {sid}")
    if len(report["never_hashed_records"]) > show:
        print(
            f"    ... and {len(report['never_hashed_records']) - show} more"
        )

    print(f"  Stale content:          {report['stale_content']}")
    for sid in report["stale_content_records"][:show]:
        print(f"    - {sid}")
    if len(report["stale_content_records"]) > show:
        print(
            f"    ... and {len(report['stale_content_records']) - show} more"
        )

    print(f"  Search pending:         {report['search_pending']}")
    for sid in report["search_pending_records"][:show]:
        print(f"    - {sid}")
    if len(report["search_pending_records"]) > show:
        print(
            f"    ... and {len(report['search_pending_records']) - show} more"
        )

    if report["not_published"]:
        print(f"  In store but NOT published: {len(report['not_published'])}")
        for sid in report["not_published"][:show]:
            print(f"    - {sid}")

    print(f"\n  --- metadata quality (expected set) ---")
    print(f"  Missing/Untitled title: {len(report['missing_title'])}")
    print(f"  Missing authors:        {len(report['missing_authors'])}")
    print(f"  No DOI:                 {report['missing_doi']}")
    print(f"  Old id won't redirect:  {report['no_legacy_id']} "
          f"(stored record lacks top-level legacy_source_id)")

    if report["search_checked"]:
        print(f"\n  --- search index ---")
        print(f"  In search:              {report['in_search']}")
        print(f"  Missing from search:    {len(report['missing_from_search'])}")
        for sid in report["missing_from_search"][:show]:
            print(f"    - {sid}")
    else:
        print(f"\n  (search not checked — pass --check-search to probe the index)")


def main():
    parser = argparse.ArgumentParser(description="Reconcile v1→v2 migration drift.")
    parser.add_argument("--env", choices=["dev", "staging", "prod"], default=None,
                        help="Auto-resolve store/search config from the deployed stack.")
    parser.add_argument("-i", "--input",
                        default=os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "converted_datasets.json"),
                        help="Path to converted_datasets.json (the expected set).")
    parser.add_argument("--check-search", action="store_true",
                        help="Also probe the search index for each record (exact subject).")
    parser.add_argument("--limit", type=int, default=0, help="Only check first N records (0 = all).")
    parser.add_argument("--show", type=int, default=10, help="How many missing ids to list.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    args = parser.parse_args()

    if args.env:
        print(f"Resolving config from stack mdf-connect-v2-{args.env}...")
        apply_env(resolve_env_from_stack(args.env))

    input_path = os.path.abspath(args.input)
    with open(input_path) as f:
        data = json.load(f)
    records = data.get("records", [])
    if args.limit > 0:
        records = records[:args.limit]
    print(f"Loaded {len(records)} expected records from {input_path}")

    report = reconcile(records, check_search=args.check_search, show=args.show)

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_report(report, args.show)

    drift = (
        bool(report["missing_from_store"])
        or report["stale_content"] > 0
        or report["search_pending"] > 0
        or (
        report["search_checked"] and bool(report["missing_from_search"])
        )
    )
    sys.exit(1 if drift else 0)


if __name__ == "__main__":
    main()
