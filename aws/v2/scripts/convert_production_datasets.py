#!/usr/bin/env python3
"""Convert extracted MDF production datasets from v1 format to v2 flat format.

Reads the raw gmeta dump (from extract_mdf_production_datasets.py) and runs
each entry through migrate_v1_payload() to produce the v2 DatasetMetadata
format. Outputs a JSON file with one record per dataset.

Skips: services, mrr, jarvis, oqmd blocks (not needed).
Organizations: collapsed to first entry when plural.
DOI: preserved from dc.identifier.
Version linking: builds previous_version chains from source_name groups.
Download URL: constructed from endpoint_path when available.

Usage:
    python cs/aws/v2/scripts/convert_production_datasets.py
    python cs/aws/v2/scripts/convert_production_datasets.py -i datasets.json -o converted.json
"""

import argparse
import json
import re
import sys
import os
from collections import defaultdict

# Allow importing v2.metadata from the scripts/ directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from v2.metadata import migrate_v1_payload


def parse_version_number(version_raw) -> tuple:
    """Normalize version to a comparable tuple.

    Handles: 1, "1", "1.0", "1.1", "1.2", etc.
    Returns (major, minor) tuple for sorting.
    """
    s = str(version_raw)
    m = re.match(r"(\d+)(?:\.(\d+))?", s)
    if m:
        return (int(m.group(1)), int(m.group(2) or 0))
    return (0, 0)


def make_version_key(record: dict) -> str:
    """Build a unique version key for a record.

    Uses source_id + version to uniquely identify a specific version,
    since some datasets share the same source_id across versions.
    """
    sid = record["source_id"] or ""
    v = record["version"]
    if v is not None:
        return f"{sid}_v{v}"
    return sid


def build_version_chains(records: list):
    """Build previous_version and root_version for all records.

    Three cases:
    1. Multiple versions present in the index (same source_name, different
       versions) — chain them together, root is the earliest.
    2. Single entry but version > 1 — prior versions were superseded and
       aren't in the index. Use source_name as the root identifier to
       represent the original dataset lineage.
    3. Single entry, version 1 — standalone. root_version = its own source_id.

    Returns (prev_map, root_map, stats) where maps are keyed by
    make_version_key(record).
    """
    by_source_name = defaultdict(list)
    for r in records:
        sn = r["source_name"]
        if sn:
            by_source_name[sn].append(r)

    prev_map = {}
    root_map = {}
    latest_set = set()  # version_keys of the latest version per group
    multi_present = 0
    implicit_versioned = 0

    for source_name, group in by_source_name.items():
        group.sort(key=lambda r: parse_version_number(r["version"]))

        if len(group) > 1:
            # Case 1: multiple versions present in index
            multi_present += 1

            unique_sids = set(r["source_id"] for r in group)
            needs_version_suffix = len(unique_sids) == 1

            root_r = group[0]
            root_ref = make_version_key(root_r) if needs_version_suffix else root_r["source_id"]

            for r in group:
                root_map[make_version_key(r)] = root_ref

            for i in range(1, len(group)):
                cur_key = make_version_key(group[i])
                prev_r = group[i - 1]
                prev_ref = make_version_key(prev_r) if needs_version_suffix else prev_r["source_id"]
                prev_map[cur_key] = prev_ref

            # Last in sorted order is the latest
            latest_set.add(make_version_key(group[-1]))
        else:
            # Single entry — it's the latest (and only) version
            r = group[0]
            major, _ = parse_version_number(r["version"])
            key = make_version_key(r)
            latest_set.add(key)

            if major > 1:
                # Case 2: version > 1 but prior versions not in index.
                implicit_versioned += 1
                root_map[key] = source_name
            else:
                # Case 3: standalone v1 — root is itself
                root_map[key] = r["source_id"]

    stats = {
        "multi_present": multi_present,
        "implicit_versioned": implicit_versioned,
        "prev_links": len(prev_map),
        "root_set": len(root_map),
        "latest_count": len(latest_set),
    }
    return prev_map, root_map, latest_set, stats


def build_download_url(endpoint_path: str) -> str | None:
    """Build a direct HTTPS download URL from a Globus endpoint path.

    Converts: globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/path/
    To: https://data.materialsdatafacility.org/path/
    """
    if not endpoint_path:
        return None
    # Strip the globus://UUID/ prefix
    m = re.match(r"globus://[a-f0-9-]+/(.+)", endpoint_path)
    if m:
        return f"https://data.materialsdatafacility.org/{m.group(1)}"
    return None


def convert_entry(gmeta_entry: dict) -> dict:
    """Convert a single gmeta entry to v2 format.

    Returns a record with:
      - source_id, source_name, version, ingest_date (from mdf block)
      - doi (from dc.identifier if present)
      - endpoint_path (from data block)
      - metadata: the v2 flat metadata dict
    """
    content = gmeta_entry.get("entries", [{}])[0].get("content", {})
    mdf = content.get("mdf", {})
    dc = content.get("dc", {})
    data = content.get("data", {})

    # Run the v1 -> v2 migration
    v2_metadata = migrate_v1_payload(content)

    # Extract DOI from dc.identifier if present
    doi = None
    dc_id = dc.get("identifier")
    if isinstance(dc_id, dict):
        doi = dc_id.get("identifier")
    elif isinstance(dc_id, str):
        doi = dc_id

    endpoint_path = data.get("endpoint_path")

    # Set download_url from endpoint_path
    download_url = build_download_url(endpoint_path)
    if download_url:
        v2_metadata["download_url"] = download_url

    # Build the output record
    record = {
        "source_id": mdf.get("source_id"),
        "source_name": mdf.get("source_name"),
        "version": mdf.get("version"),
        "ingest_date": mdf.get("ingest_date"),
        "doi": doi,
        "endpoint_path": endpoint_path,
        "metadata": v2_metadata,
    }

    return record


def main():
    parser = argparse.ArgumentParser(
        description="Convert MDF production datasets from v1 to v2 format."
    )
    parser.add_argument(
        "-i", "--input",
        default="datasets.json",
        help="Input JSON from extract script (default: datasets.json)",
    )
    parser.add_argument(
        "-o", "--output",
        default="converted_datasets.json",
        help="Output JSON file (default: converted_datasets.json)",
    )
    args = parser.parse_args()

    with open(args.input) as f:
        data = json.load(f)

    gmeta_list = data.get("gmeta", [])
    print(f"Input: {len(gmeta_list)} entries from {args.input}")

    converted = []
    errors = []
    for i, entry in enumerate(gmeta_list):
        try:
            record = convert_entry(entry)
            converted.append(record)
        except Exception as exc:
            subject = entry.get("subject", f"entry_{i}")
            errors.append({"subject": subject, "error": str(exc)})
            print(f"  ERROR [{subject}]: {exc}", file=sys.stderr)

    print(f"Converted: {len(converted)}")
    if errors:
        print(f"Errors: {len(errors)}")

    # Build version chains across records sharing the same source_name
    prev_map, root_map, latest_set, stats = build_version_chains(converted)
    for record in converted:
        key = make_version_key(record)
        # Version string (normalize to "major.minor")
        major, minor = parse_version_number(record["version"])
        record["metadata"]["version"] = f"{major}.{minor}"
        # Previous version link
        prev = prev_map.get(key)
        if prev:
            record["metadata"]["previous_version"] = prev
        # Root version
        root = root_map.get(key)
        if root:
            record["metadata"]["root_version"] = root
        # Latest flag
        record["metadata"]["latest"] = key in latest_set
    print(f"Versioning: {stats['multi_present']} with multiple versions in index, "
          f"{stats['implicit_versioned']} with prior versions superseded, "
          f"{stats['prev_links']} previous_version links, "
          f"{stats['latest_count']} marked latest")

    # Summary stats
    with_doi = sum(1 for r in converted if r["doi"])
    with_ml = sum(1 for r in converted if r["metadata"].get("ml"))
    with_org = sum(1 for r in converted if r["metadata"].get("organization"))
    with_keywords = sum(1 for r in converted if r["metadata"].get("keywords"))
    with_license = sum(1 for r in converted if r["metadata"].get("license"))
    with_related = sum(1 for r in converted if r["metadata"].get("related_works"))
    with_extensions = sum(1 for r in converted if r["metadata"].get("extensions"))
    with_download = sum(1 for r in converted if r["metadata"].get("download_url"))
    with_prev_ver = sum(1 for r in converted if r["metadata"].get("previous_version"))
    with_root_ver = sum(1 for r in converted if r["metadata"].get("root_version"))
    with_version = sum(1 for r in converted if r["metadata"].get("version"))
    is_latest = sum(1 for r in converted if r["metadata"].get("latest"))

    print(f"\n--- Field coverage ---")
    print(f"  DOI:              {with_doi}/{len(converted)}")
    print(f"  ML metadata:      {with_ml}/{len(converted)}")
    print(f"  Organization:     {with_org}/{len(converted)}")
    print(f"  Keywords:         {with_keywords}/{len(converted)}")
    print(f"  License:          {with_license}/{len(converted)}")
    print(f"  Related works:    {with_related}/{len(converted)}")
    print(f"  Extensions:       {with_extensions}/{len(converted)}")
    print(f"  Download URL:     {with_download}/{len(converted)}")
    print(f"  Version:          {with_version}/{len(converted)}")
    print(f"  Previous version: {with_prev_ver}/{len(converted)}")
    print(f"  Root version:     {with_root_ver}/{len(converted)}")
    print(f"  Latest:           {is_latest}/{len(converted)}")

    output = {
        "source": args.input,
        "count": len(converted),
        "errors": errors,
        "records": converted,
    }

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
