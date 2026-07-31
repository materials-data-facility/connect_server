#!/usr/bin/env python3
"""Extract all dataset entries from the MDF production Globus Search index.

Reads all resource_type=dataset entries from the MDF production index
and saves the raw GMetaEntry data to a JSON file. This is READ-ONLY —
nothing is written back to the production index.

The production index is: 1a57bbe5-5272-477f-9d31-343b8258b7a5

Usage:
    # Interactive Globus login (opens browser)
    python cs/aws/v2/scripts/extract_mdf_production_datasets.py

    # Save to a specific file
    python cs/aws/v2/scripts/extract_mdf_production_datasets.py -o datasets.json

    # Limit results (for testing)
    python cs/aws/v2/scripts/extract_mdf_production_datasets.py --limit 50

    # Just print count, don't save
    python cs/aws/v2/scripts/extract_mdf_production_datasets.py --count-only

    # Legacy behavior: advance --since-file immediately after extraction
    python cs/aws/v2/scripts/extract_mdf_production_datasets.py \
        --since-file watermark.txt --advance-watermark

By default, extraction only embeds ``candidate_watermark`` in the output.
Pass that candidate to ingest_converted_datasets.py; a complete successful
ingest advances the watermark. ``--advance-watermark`` is an explicit escape
hatch that restores the old write-immediately behavior.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

from legacy_auth import get_legacy_search_client  # noqa: E402

MDF_PRODUCTION_INDEX = "1a57bbe5-5272-477f-9d31-343b8258b7a5"

# Globus Search max limit per request
PAGE_SIZE = 100


def _parse_dt(value):
    """Parse an ISO-8601-ish timestamp into a tz-aware datetime, or None."""
    if not value:
        return None
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _entry_contents(entry):
    """Yield content dicts from a gmeta entry, handling both response shapes.

    Modern Globus Search: entry['entries'][0]['content'].
    Older/flattened:      entry['content'] (a dict or a list of dicts).
    """
    ents = entry.get("entries")
    if isinstance(ents, list):
        for e in ents:
            c = e.get("content") if isinstance(e, dict) else None
            if isinstance(c, dict):
                yield c
    c = entry.get("content")
    if isinstance(c, list):
        for item in c:
            if isinstance(item, dict):
                yield item
    elif isinstance(c, dict):
        yield c


def _entry_ingest_date(entry):
    """Return the mdf.ingest_date string for a gmeta entry, or None."""
    for content in _entry_contents(entry):
        mdf = content.get("mdf", {}) if isinstance(content, dict) else {}
        ingest = mdf.get("ingest_date")
        if ingest:
            return ingest
    return None


def _candidate_watermark_fields(max_dt, limit):
    """Return safe candidate-watermark fields for an extract payload.

    A limited extract is necessarily partial, so its newest observed timestamp
    must never become a resumable watermark.
    """
    if limit is not None:
        return {
            "candidate_watermark": None,
            "candidate_watermark_suppressed": "limit",
        }
    return {
        "candidate_watermark": max_dt.isoformat() if max_dt else None,
        "candidate_watermark_suppressed": None,
    }


def authenticate(interactive_ok=True):
    """Return the shared legacy Search client (compatibility wrapper)."""
    return get_legacy_search_client(interactive_ok=interactive_ok)


def _is_forbidden(exc):
    """Return True when a Globus/API exception represents HTTP 403."""
    return (
        getattr(exc, "http_status", None) == 403
        or getattr(exc, "status_code", None) == 403
        or (
            isinstance(getattr(exc, "response", None), dict)
            and exc.response.get("status_code") == 403
        )
    )


def fetch_all_datasets(search_client, limit=None):
    """Fetch all resource_type=dataset entries from the production index.

    Uses offset-based pagination to walk through all results.
    Returns a list of raw gmeta entries (each with subject, content, etc).
    """
    query = 'mdf.resource_type:"dataset"'
    offset = 0
    all_gmeta = []
    total = None

    while True:
        fetch_limit = PAGE_SIZE
        if limit is not None:
            remaining = limit - len(all_gmeta)
            if remaining <= 0:
                break
            fetch_limit = min(PAGE_SIZE, remaining)

        print(f"  Fetching offset={offset}, limit={fetch_limit} ...", end=" ", flush=True)

        try:
            result = search_client.search(
                MDF_PRODUCTION_INDEX,
                query,
                limit=fetch_limit,
                offset=offset,
                advanced=True,
            )
        except Exception as exc:
            if _is_forbidden(exc):
                print(
                    "\nERROR: Legacy Search returned 403. Grant this Globus "
                    "confidential client read permission on legacy index {}."
                    .format(MDF_PRODUCTION_INDEX),
                    file=sys.stderr,
                )
            raise
        data = result.data if hasattr(result, "data") else result

        if total is None:
            total = data.get("total", 0)
            print(f"(total in index: {total})")
        else:
            print()

        gmeta_list = data.get("gmeta", [])
        if not gmeta_list:
            break

        all_gmeta.extend(gmeta_list)
        print(f"  ... got {len(gmeta_list)} entries (cumulative: {len(all_gmeta)})")

        offset += len(gmeta_list)

        # Stop if we've fetched everything
        if offset >= total:
            break

        # Be polite to the API
        time.sleep(0.2)

    return all_gmeta, total


def summarize(gmeta_list):
    """Print a summary of the fetched datasets."""
    print(f"\nTotal GMetaEntries fetched: {len(gmeta_list)}")

    titles = []
    source_ids = []
    orgs = set()
    for entry in gmeta_list:
        for content in entry.get("content", []):
            dc = content.get("dc", {})
            mdf = content.get("mdf", {})
            title = dc.get("title") or dc.get("titles", [{}])[0].get("title", "?") if dc else "?"
            titles.append(title)
            sid = mdf.get("source_id", "?")
            source_ids.append(sid)
            for org in mdf.get("organizations", []):
                orgs.add(org)

    print(f"Unique organizations: {sorted(orgs)}")
    print(f"\nFirst 10 datasets:")
    for i, (title, sid) in enumerate(zip(titles[:10], source_ids[:10])):
        print(f"  {i+1}. [{sid}] {title}")
    if len(titles) > 10:
        print(f"  ... and {len(titles) - 10} more")


def main():
    parser = argparse.ArgumentParser(
        description="Extract dataset entries from the MDF production Globus Search index."
    )
    parser.add_argument(
        "-o", "--output",
        default="mdf_production_datasets.json",
        help="Output JSON file (default: mdf_production_datasets.json)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of entries to fetch (default: all)",
    )
    parser.add_argument(
        "--count-only",
        action="store_true",
        help="Just count entries, don't save to file",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="Only keep datasets with mdf.ingest_date >= this ISO timestamp "
             "(delta sync). Entries without a parseable ingest_date are skipped.",
    )
    parser.add_argument(
        "--since-file",
        default=None,
        help="Watermark file for ongoing-bridge sync. Read as --since if it "
             "exists (and --since not given). It is not updated unless "
             "--advance-watermark is explicitly passed.",
    )
    parser.add_argument(
        "--advance-watermark",
        action="store_true",
        help="Immediately write the candidate watermark to --since-file after "
             "a successful extract (legacy behavior; default: do not advance).",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Require GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET; never prompt.",
    )
    args = parser.parse_args()

    if args.advance_watermark and not args.since_file:
        parser.error("--advance-watermark requires --since-file")
    if args.advance_watermark and args.since:
        parser.error("--advance-watermark cannot be combined with --since")

    print(f"MDF Production Index: {MDF_PRODUCTION_INDEX}")
    print(f"Query: resource_type=dataset\n")

    # Resolve the delta watermark: explicit --since wins; else read --since-file.
    since_raw = args.since
    if not since_raw and args.since_file and os.path.exists(args.since_file):
        with open(args.since_file) as f:
            since_raw = f.read().strip() or None
    since_dt = _parse_dt(since_raw) if since_raw else None
    if since_raw and not since_dt:
        print(f"ERROR: could not parse --since value: {since_raw!r}", file=sys.stderr)
        sys.exit(1)
    if since_dt:
        print(f"Delta sync: keeping datasets with ingest_date >= {since_dt.isoformat()}\n")

    print("Authenticating with Globus...")
    try:
        search_client = authenticate(interactive_ok=not args.non_interactive)
    except RuntimeError as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        sys.exit(1)
    print("Authenticated.\n")

    print("Fetching datasets...")
    fetched, total = fetch_all_datasets(search_client, limit=args.limit)

    # Newest ingest_date across everything fetched — becomes the next watermark
    # even if some entries are filtered out below.
    max_dt = None
    for entry in fetched:
        dt = _parse_dt(_entry_ingest_date(entry))
        if dt and (max_dt is None or dt > max_dt):
            max_dt = dt

    # Apply the --since delta filter. The index isn't sorted by date, so we scan
    # all and keep the delta client-side (correct and simple at MDF's scale; a
    # server-side range query would be the optimization for a very large index).
    skipped_undated = 0
    if since_dt:
        kept = []
        for entry in fetched:
            dt = _parse_dt(_entry_ingest_date(entry))
            if dt is None:
                skipped_undated += 1
                continue
            if dt >= since_dt:
                kept.append(entry)
        gmeta_list = kept
        print(f"Delta filter: {len(gmeta_list)} of {len(fetched)} fetched match "
              f"(ingest_date >= since); {skipped_undated} skipped (no ingest_date)")
    else:
        gmeta_list = fetched

    summarize(gmeta_list)

    if args.count_only:
        print("\n--count-only: skipping file save.")
        return

    # Save raw gmeta entries
    watermark_fields = _candidate_watermark_fields(max_dt, args.limit)
    output = {
        "source_index": MDF_PRODUCTION_INDEX,
        "query": 'mdf.resource_type:"dataset"',
        "total_in_index": total,
        "fetched_count": len(fetched),
        "matched_count": len(gmeta_list),
        "since": since_dt.isoformat() if since_dt else None,
        "max_ingest_date": max_dt.isoformat() if max_dt else None,
        "skipped_undated": skipped_undated,
        "gmeta": gmeta_list,
    }
    output.update(watermark_fields)

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nSaved {len(gmeta_list)} entries to {args.output}")

    # By default the ingest phase owns advancement: extraction alone does not
    # prove that the converted records landed in the store/search index.
    candidate_watermark = watermark_fields["candidate_watermark"]
    if args.since_file and candidate_watermark and args.advance_watermark:
        with open(args.since_file, "w") as f:
            f.write(candidate_watermark)
        print(f"Watermark updated: {args.since_file} -> {candidate_watermark}")
    elif watermark_fields["candidate_watermark_suppressed"]:
        print(
            "Watermark NOT advanced: candidate suppressed because --limit "
            "produced a partial extract."
        )
    elif candidate_watermark:
        if args.since_file:
            print(
                f"Watermark NOT advanced: {args.since_file} remains unchanged. "
                "A fully successful ingest will advance it to "
                f"{candidate_watermark}."
            )
        else:
            print(
                "Watermark NOT advanced. The candidate was embedded in the "
                "extract JSON and should be committed by a fully successful ingest."
            )
    elif args.since_file:
        print(
            f"Watermark NOT advanced: {args.since_file} remains unchanged "
            "because no parseable ingest_date was found."
        )


if __name__ == "__main__":
    main()
