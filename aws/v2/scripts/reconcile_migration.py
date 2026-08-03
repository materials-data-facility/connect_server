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

Exit code is 1 for hard drift: missing records, stale content, Search-pending
latest datasets, or (with --check-search) missing/extra Search subjects.
Incomplete/erroring Search audits are warnings by default; --strict-search
makes those inconclusive signals fail. Pre-hash records are warnings only.
"""

import argparse
import contextlib
import io
import json
import os
import random
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
from convert_production_datasets import convert_entry  # noqa: E402
from extract_mdf_production_datasets import fetch_all_datasets  # noqa: E402
from legacy_auth import get_legacy_search_client  # noqa: E402
from purge_stale_search_entries import (  # noqa: E402
    classify_entries,
    enumerate_index,
)


def _reverse_search_report(store, search=None, search_scan_limit=0):
    """Return Search subjects that have no corresponding store record."""
    if search is None:
        from v2.search_client import get_search_client

        search = get_search_client()
    if not hasattr(search, "_get_read_client"):
        return {
            "search_checked": False,
            "search_scan_complete": None,
            "search_scan_warnings": [
                "Search client does not support a read scan; audit skipped"
            ],
        }
    read_client = search._get_read_client()  # noqa: SLF001 — audit raw index
    index_id = getattr(search, "index_id", None)
    if not index_id:
        raise RuntimeError("Search client has no index_id")
    index_records = []
    scan = None
    first_warnings = []
    first_error = None
    for attempt in range(2):
        try:
            index_records, scan = enumerate_index(
                read_client, index_id, limit=search_scan_limit
            )
        except Exception as exc:
            if attempt == 0:
                first_error = str(exc)
                continue
            raise
        if scan.get("complete"):
            break
        if attempt == 0:
            first_warnings = list(scan.get("warnings") or [])
            continue
        scan.setdefault("warnings", []).insert(
            0, "Search scan remained incomplete after one retry"
        )
    if scan is None:  # pragma: no cover - second exception is raised above
        raise RuntimeError(first_error or "Search scan failed")
    if not first_error and not scan.get("complete") and first_warnings:
        scan.setdefault("warnings", []).extend(first_warnings)
    classified = classify_entries(index_records, store)
    extras = classified["stale_superseded"] + classified["stale_orphan"]
    extra_ids = sorted(
        {
            str(record.get("source_id") or record.get("subject") or "")
            for record in extras
            if record.get("source_id") or record.get("subject")
        }
    )
    scan_complete = bool(scan.get("complete")) and not bool(
        scan.get("limited")
    )
    scan_warnings = list(scan.get("warnings") or [])
    if scan.get("limited"):
        scan_warnings.append(
            "Search scan was limited to {}; full-index parity is inconclusive"
            .format(search_scan_limit)
        )
    report = {
        "search_checked": True,
        "extra_in_index_count": len(extra_ids),
        "extra_in_index": extra_ids[:50],
        "search_index_total": scan.get("index_total_before", 0),
        "search_scan_complete": scan_complete,
        "search_scan_warnings": scan_warnings,
        "search_store_errors": classified.get("store_errors") or [],
    }
    if extra_ids:
        report["extra_in_index_remedy"] = (
            "run scripts/purge_stale_search_entries.py --execute; entries: {}"
            .format(", ".join(extra_ids[:10]))
        )
    return report


def _expected_version(converted):
    return str(converted.get("metadata", {}).get("version", "1.0"))


def _has_title(meta):
    t = (meta.get("title") or "").strip()
    return bool(t) and t.lower() != "untitled"


def _has_authors(meta):
    authors = meta.get("authors") or []
    return any((a or {}).get("name") for a in authors if isinstance(a, dict))


def reconcile(
    records, *, check_search=False, show=10, search_scan_limit=0
):
    """Compare expected (converted) records against the v2 store/search."""
    from v2.store import get_store

    store = get_store()
    search = None
    search_subject = None
    search_setup_warning = None
    if check_search:
        try:
            from v2.search_client import get_search_client

            search = get_search_client()
            if not hasattr(search, "_get_read_client"):
                search_setup_warning = (
                    "Search client does not support reads; audit skipped"
                )
                search = None
                raise AttributeError(search_setup_warning)
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
        "extra_in_index_count": 0,
        "extra_in_index": [],
        "search_index_total": 0,
        "search_scan_complete": None,
        "search_scan_warnings": (
            [search_setup_warning] if search_setup_warning else []
        ),
        "search_store_errors": [],
        "search_check_error": None,
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

    if search is not None:
        try:
            report.update(
                _reverse_search_report(
                    store, search, search_scan_limit=search_scan_limit
                )
            )
        except Exception as exc:
            report["search_check_error"] = str(exc)

    return report


def _version_key(record):
    """Return a stable mixed numeric/text version sort key."""
    return [
        (0, int(part), "") if part.isdigit() else (1, 0, part)
        for part in str(record.get("version") or "0").split(".")
    ]


def _normalize_doi(value):
    if not value:
        return None
    normalized = str(value).strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
            break
    return normalized or None


def _legacy_expected(entry):
    """Reuse conversion logic to obtain parity fields from legacy content."""
    converted = convert_entry(entry)
    metadata = converted.get("metadata") or {}
    return {
        "source_name": converted.get("source_name"),
        "title": (metadata.get("title") or "").strip(),
        "doi": _normalize_doi(converted.get("doi")),
        "author_count": len(metadata.get("authors") or []),
    }


def reconcile_legacy(
    legacy_client, *, sample=25, store=None, rng=None, check_search=False,
    search=None, search_scan_limit=0,
):
    """Compare the live legacy index with records marked as v1 migrations."""
    from v2.store import get_store

    store = store or get_store()
    rng = rng or random
    gmeta, legacy_total = fetch_all_datasets(legacy_client)
    if legacy_total is None:
        legacy_total = len(gmeta)

    all_store_records = store.list_all(limit=1000000)
    migrated = [
        record
        for record in all_store_records
        if record.get("legacy_source_id")
        or record.get("user_id") == "v1-migration"
    ]
    chains = {}
    for record in migrated:
        source_id = record.get("source_id")
        if source_id:
            chains.setdefault(source_id, []).append(record)

    legacy_expected = []
    conversion_errors = []
    for entry in gmeta:
        try:
            legacy_expected.append(_legacy_expected(entry))
        except Exception as exc:
            conversion_errors.append(
                {
                    "subject": entry.get("subject"),
                    "error": str(exc),
                }
            )

    missing_names = []
    seen_missing = set()
    matched = []
    for expected in legacy_expected:
        source_name = expected.get("source_name")
        chain = chains.get(source_name)
        if not chain:
            if source_name not in seen_missing:
                missing_names.append(source_name)
                seen_missing.add(source_name)
            continue
        stored = max(chain, key=_version_key)
        matched.append((expected, stored))

    chosen = (
        rng.sample(matched, min(sample, len(matched)))
        if sample > 0
        else []
    )
    mismatch_records = []
    for expected, stored in chosen:
        actual = stored.get("dataset_mdata") or {}
        actual_doi = _normalize_doi(
            stored.get("doi")
            or stored.get("dataset_doi")
            or actual.get("doi")
        )
        fields = {}
        actual_title = (actual.get("title") or "").strip()
        if expected["title"] != actual_title:
            fields["title"] = {
                "legacy": expected["title"],
                "v2": actual_title,
            }
        if expected["doi"] != actual_doi:
            fields["doi"] = {
                "legacy": expected["doi"],
                "v2": actual_doi,
            }
        actual_author_count = len(actual.get("authors") or [])
        if expected["author_count"] != actual_author_count:
            fields["author_count"] = {
                "legacy": expected["author_count"],
                "v2": actual_author_count,
            }
        if fields:
            mismatch_records.append(
                {"source_name": expected["source_name"], "fields": fields}
            )

    report = {
        "legacy_total": int(legacy_total),
        "migrated_total": len(migrated),
        "missing_from_v2": missing_names[:50],
        "missing_from_v2_count": len(missing_names),
        "sampled": len(chosen),
        "field_mismatches": len(mismatch_records),
        "field_mismatch_records": mismatch_records,
        "legacy_conversion_errors": conversion_errors,
        "search_checked": bool(check_search),
        "extra_in_index_count": 0,
        "extra_in_index": [],
        "search_index_total": 0,
        "search_scan_complete": None,
        "search_scan_warnings": [],
        "search_store_errors": [],
        "search_check_error": None,
    }
    if check_search:
        try:
            report.update(
                _reverse_search_report(
                    store, search, search_scan_limit=search_scan_limit
                )
            )
        except Exception as exc:
            report["search_check_error"] = str(exc)
    return report


def _print_legacy_report(report, show):
    print(f"\n{'='*56}")
    print("Live legacy-to-v2 parity")
    print(f"{'='*56}")
    print(f"  Legacy datasets:        {report['legacy_total']}")
    print(f"  Migrated store records: {report['migrated_total']}")
    print(f"  Missing from v2:        {report['missing_from_v2_count']}")
    for source_name in report["missing_from_v2"][:show]:
        print(f"    - {source_name}")
    print(f"  Field sample size:      {report['sampled']}")
    print(f"  Field mismatches:       {report['field_mismatches']}")
    for mismatch in report["field_mismatch_records"][:show]:
        print(
            "    - {}: {}".format(
                mismatch["source_name"],
                ", ".join(sorted(mismatch["fields"])),
            )
        )
    if report.get("search_checked"):
        print(f"  Extra in Search index:   {report['extra_in_index_count']}")
        for source_id in report["extra_in_index"][:show]:
            print(f"    - {source_id}")
        if report.get("search_check_error"):
            print(f"  Search check error:      {report['search_check_error']}")
        if report.get("extra_in_index_remedy"):
            print(f"  Remedy:                  {report['extra_in_index_remedy']}")


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
        print(f"  Extra in Search index:   {report['extra_in_index_count']}")
        for source_id in report["extra_in_index"][:show]:
            print(f"    - {source_id}")
        if report.get("search_check_error"):
            print(f"  Search check error:      {report['search_check_error']}")
        if report.get("extra_in_index_remedy"):
            print(f"  Remedy:                  {report['extra_in_index_remedy']}")
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
                        help="Probe expected subjects and scan for extra index entries.")
    parser.add_argument(
        "--strict-search",
        action="store_true",
        help="Fail when Search reconciliation is incomplete or errors.",
    )
    parser.add_argument(
        "--search-scan-limit",
        type=int,
        default=0,
        help="Cap reverse Search scan entries (0 = full index).",
    )
    parser.add_argument(
        "--legacy-check",
        action="store_true",
        help="Compare the live legacy Search index with migrated v2 records.",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=25,
        help="Random matched datasets to field-compare in --legacy-check mode.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Only check first N records (0 = all).")
    parser.add_argument("--show", type=int, default=10, help="How many missing ids to list.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    args = parser.parse_args()

    if args.env and not args.json:
        print(f"Resolving config from stack mdf-connect-v2-{args.env}...")
    if args.env:
        apply_env(resolve_env_from_stack(args.env))

    if args.legacy_check:
        legacy_client = get_legacy_search_client()
        if args.json:
            # fetch_all_datasets intentionally emits operator progress. Keep
            # the legacy JSON mode machine-readable without changing the
            # established converted-vs-store output behavior.
            with contextlib.redirect_stdout(io.StringIO()):
                try:
                    report = reconcile_legacy(
                        legacy_client,
                        sample=args.sample,
                        check_search=args.check_search,
                        search_scan_limit=args.search_scan_limit,
                    )
                except Exception as exc:
                    report = _legacy_check_error_report(exc)
        else:
            try:
                report = reconcile_legacy(
                    legacy_client,
                    sample=args.sample,
                    check_search=args.check_search,
                    search_scan_limit=args.search_scan_limit,
                )
            except Exception as exc:
                report = _legacy_check_error_report(exc)
    else:
        input_path = os.path.abspath(args.input)
        with open(input_path) as f:
            data = json.load(f)
        records = data.get("records", [])
        if args.limit > 0:
            records = records[:args.limit]
        print(f"Loaded {len(records)} expected records from {input_path}")
        report = reconcile(
            records,
            check_search=args.check_search,
            show=args.show,
            search_scan_limit=args.search_scan_limit,
        )

    inconclusive = _inconclusive_reasons(report)
    report["inconclusive"] = bool(inconclusive)
    report["inconclusive_reasons"] = inconclusive

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    elif args.legacy_check:
        _print_legacy_report(report, args.show)
    else:
        _print_report(report, args.show)

    if args.legacy_check:
        drift = _legacy_report_has_drift(
            report, strict_search=args.strict_search
        )
    else:
        drift = _report_has_drift(report, strict_search=args.strict_search)
    if inconclusive:
        print(
            "WARNING: reconciliation inconclusive: {}".format(
                "; ".join(inconclusive)
            ),
            file=sys.stderr,
        )
    if report.get("extra_in_index_remedy"):
        print(report["extra_in_index_remedy"], file=sys.stderr)
    sys.exit(1 if drift else 0)


def _legacy_check_error_report(exc):
    """Return a JSON-safe inconclusive report for a failed live check."""
    return {
        "legacy_total": 0,
        "migrated_total": 0,
        "missing_from_v2": [],
        "missing_from_v2_count": 0,
        "sampled": 0,
        "field_mismatches": 0,
        "field_mismatch_records": [],
        "legacy_conversion_errors": [],
        "legacy_check_error": str(exc),
        "search_checked": False,
        "extra_in_index_count": 0,
        "extra_in_index": [],
        "search_scan_complete": None,
        "search_scan_warnings": [],
        "search_store_errors": [],
        "search_check_error": None,
    }


def _inconclusive_reasons(report):
    """Return operational/check failures that are not evidence of drift."""
    reasons = []
    if report.get("legacy_check_error"):
        reasons.append("legacy check error: {}".format(
            report["legacy_check_error"]
        ))
    if report.get("search_check_error"):
        reasons.append("Search check error: {}".format(
            report["search_check_error"]
        ))
    if report.get("search_scan_complete") is False:
        reasons.append("Search scan incomplete")
    if report.get("search_store_errors"):
        reasons.append(
            "{} store lookup error(s) during Search scan".format(
                len(report["search_store_errors"])
            )
        )
    reasons.extend(str(item) for item in report.get("search_scan_warnings") or [])
    return list(dict.fromkeys(reasons))


def _search_report_has_drift(report):
    """Return only proven reverse-index drift, never audit uncertainty."""
    return bool(report.get("extra_in_index_count", 0))


def _legacy_report_has_drift(report, strict_search=False):
    hard_drift = bool(
        report["missing_from_v2_count"] > 0
        or report["field_mismatches"] > 0
        or report["legacy_conversion_errors"]
        or (report.get("search_checked") and _search_report_has_drift(report))
    )
    return hard_drift or bool(strict_search and _inconclusive_reasons(report))


def _report_has_drift(report, strict_search=False):
    hard_drift = bool(
        report["missing_from_store"]
        or report["stale_content"] > 0
        or report["search_pending"] > 0
        or (
            report["search_checked"]
            and (
                report["missing_from_search"]
                or _search_report_has_drift(report)
            )
        )
    )
    return hard_drift or bool(strict_search and _inconclusive_reasons(report))


if __name__ == "__main__":
    main()
