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
"""

import argparse
import json
import sys
import time

MDF_PRODUCTION_INDEX = "1a57bbe5-5272-477f-9d31-343b8258b7a5"
NATIVE_APP_CLIENT_ID = "074cebcc-19ad-4332-bbf2-78402291b659"
SEARCH_SCOPE = "urn:globus:auth:scope:search.api.globus.org:all"

# Globus Search max limit per request
PAGE_SIZE = 100


def authenticate():
    """Authenticate via interactive Globus OAuth login. Returns a SearchClient."""
    import globus_sdk

    client = globus_sdk.NativeAppAuthClient(NATIVE_APP_CLIENT_ID)
    client.oauth2_start_flow(requested_scopes=SEARCH_SCOPE)

    authorize_url = client.oauth2_get_authorize_url()
    print(f"Go to this URL and login:\n\n  {authorize_url}\n")
    auth_code = input("Paste the authorization code here: ").strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    search_token_data = token_response.by_resource_server.get("search.api.globus.org")
    if not search_token_data:
        print("ERROR: No search token in response. Check app scopes.")
        sys.exit(1)

    access_token = (
        search_token_data.get("access_token")
        if isinstance(search_token_data, dict)
        else getattr(search_token_data, "access_token", None)
    )
    if not access_token:
        print("ERROR: access_token is None")
        sys.exit(1)

    authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
    return globus_sdk.SearchClient(authorizer=authorizer)


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

        result = search_client.search(
            MDF_PRODUCTION_INDEX,
            query,
            limit=fetch_limit,
            offset=offset,
            advanced=True,
        )
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
    args = parser.parse_args()

    print(f"MDF Production Index: {MDF_PRODUCTION_INDEX}")
    print(f"Query: resource_type=dataset\n")

    print("Authenticating with Globus...")
    search_client = authenticate()
    print("Authenticated.\n")

    print("Fetching datasets...")
    gmeta_list, total = fetch_all_datasets(search_client, limit=args.limit)

    summarize(gmeta_list)

    if args.count_only:
        print("\n--count-only: skipping file save.")
        return

    # Save raw gmeta entries
    output = {
        "source_index": MDF_PRODUCTION_INDEX,
        "query": 'mdf.resource_type:"dataset"',
        "total_in_index": total,
        "fetched_count": len(gmeta_list),
        "gmeta": gmeta_list,
    }

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nSaved {len(gmeta_list)} entries to {args.output}")


if __name__ == "__main__":
    main()
