#!/usr/bin/env python3
"""Test whether the confidential app can obtain a Globus Search token.

This verifies that:
1. GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET are set (reads from SSM if not)
2. The app can obtain an access token for search.api.globus.org
3. The token can be used to query the search index

Usage:
    python cs/aws/v2/scripts/test_search_token.py
"""

import json
import os
import subprocess
import sys


def get_from_ssm(name: str, region: str = "us-east-1") -> str:
    result = subprocess.run(
        ["aws", "ssm", "get-parameter", "--name", name,
         "--with-decryption", "--region", region,
         "--query", "Parameter.Value", "--output", "text"],
        capture_output=True, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else ""


def main():
    client_id = os.environ.get("GLOBUS_CLIENT_ID") or get_from_ssm("/mdf/globus-client-id")
    client_secret = os.environ.get("GLOBUS_CLIENT_SECRET") or get_from_ssm("/mdf/globus-client-secret")

    if not client_id or not client_secret:
        print("ERROR: Could not resolve GLOBUS_CLIENT_ID / GLOBUS_CLIENT_SECRET")
        sys.exit(1)

    print(f"Client ID: {client_id}")
    print(f"Secret:    {'*' * len(client_secret)}")

    import globus_sdk

    cc = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)

    print("\nRequesting token for search.api.globus.org...")
    try:
        token_response = cc.oauth2_client_credentials_tokens(
            requested_scopes="urn:globus:auth:scope:search.api.globus.org:all"
        )
    except Exception as exc:
        print(f"ERROR: Token request failed: {exc}")
        print("\nThis likely means the Globus app registration does not have the")
        print("'urn:globus:auth:scope:search.api.globus.org:all' scope configured.")
        print("Go to https://app.globus.org/settings/developers and add it.")
        sys.exit(1)

    print(f"by_resource_server keys: {list(token_response.by_resource_server.keys())}")

    search_token_data = token_response.by_resource_server.get("search.api.globus.org")
    if not search_token_data:
        print("ERROR: No search.api.globus.org entry in token response")
        print("The app likely doesn't have the search scope configured.")
        sys.exit(1)

    access_token = (
        search_token_data.get("access_token")
        if isinstance(search_token_data, dict)
        else getattr(search_token_data, "access_token", None)
    )
    if not access_token:
        print("ERROR: access_token is None")
        sys.exit(1)

    print(f"Access token: {access_token[:20]}...")
    print("Token obtained successfully!\n")

    # Try a simple query
    index_id = "ab19b80b-0887-4337-b9f8-b8cc7feb1fdc"
    print(f"Testing search query on index {index_id}...")
    authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
    sc = globus_sdk.SearchClient(authorizer=authorizer)

    try:
        result = sc.search(index_id, "test", limit=5)
        data = result.data if hasattr(result, "data") else result
        print(f"Search returned {data.get('total', 0)} total results")
        for gmeta in data.get("gmeta", []):
            for c in gmeta.get("content", []):
                print(f"  - {c.get('dc', {}).get('title', '?')}")
        print("\nSearch query works!")
    except Exception as exc:
        print(f"Search query failed: {exc}")
        print("The token works but the search query failed — check index permissions.")
        sys.exit(1)


if __name__ == "__main__":
    main()
