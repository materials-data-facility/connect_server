#!/usr/bin/env python3
"""Grant writer role on a Globus Search index to the MDF confidential app.

Run once to allow the backend's confidential app to ingest entries into
the search index.  Authenticates interactively as **you** (the index owner),
then creates a writer role for the app identity.

Usage:
    # Auto-resolve client ID from AWS SSM (/mdf/globus-client-id):
    python grant_search_index_role.py

    # Or pass client ID explicitly:
    python grant_search_index_role.py --client-id 86e4853e-...

    # Override index UUID (default: test index):
    python grant_search_index_role.py --index ab19b80b-...
"""

import argparse
import subprocess
import sys

import globus_sdk

# MDF v2 test search index
DEFAULT_INDEX_UUID = "ab19b80b-0887-4337-b9f8-b8cc7feb1fdc"

# Native app client for interactive login (same one used by mdf_agent)
NATIVE_APP_CLIENT_ID = "074cebcc-19ad-4332-bbf2-78402291b659"


def resolve_client_id_from_ssm() -> str | None:
    """Try to read the confidential app client ID from AWS SSM."""
    try:
        result = subprocess.run(
            [
                "aws", "ssm", "get-parameter",
                "--name", "/mdf/globus-client-id",
                "--region", "us-east-1",
                "--query", "Parameter.Value",
                "--output", "text",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def interactive_login() -> globus_sdk.SearchClient:
    """Login interactively and return a SearchClient authorized as the user."""
    client = globus_sdk.NativeAppAuthClient(NATIVE_APP_CLIENT_ID)
    client.oauth2_start_flow(
        requested_scopes=["urn:globus:auth:scope:search.api.globus.org:all"],
        refresh_tokens=False,
    )

    authorize_url = client.oauth2_get_authorize_url()
    print(f"\nOpen this URL in your browser:\n  {authorize_url}\n")
    auth_code = input("Paste the authorization code here: ").strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    search_token = token_response.by_resource_server["search.api.globus.org"]
    access_token = search_token["access_token"]

    authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
    return globus_sdk.SearchClient(authorizer=authorizer)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--index", default=DEFAULT_INDEX_UUID, help="Globus Search index UUID")
    parser.add_argument("--client-id", default=None, help="Confidential app client ID (auto-resolved from SSM if omitted)")
    parser.add_argument("--role", default="writer", choices=["writer", "admin"], help="Role to grant (default: writer)")
    parser.add_argument("--list-only", action="store_true", help="Just list current roles, don't create")
    args = parser.parse_args()

    # Resolve the confidential app's client ID
    app_client_id = args.client_id
    if not app_client_id:
        print("Resolving confidential app client ID from SSM...")
        app_client_id = resolve_client_id_from_ssm()
        if app_client_id:
            print(f"  Found: {app_client_id}")
        else:
            print("  Could not resolve from SSM. Pass --client-id explicitly.")
            sys.exit(1)

    # Login interactively as the index owner
    print("\nAuthenticating as index owner (interactive login)...")
    search_client = interactive_login()

    # List current roles
    print(f"\nCurrent roles on index {args.index}:")
    try:
        roles = search_client.get_role_list(args.index)
        role_list = roles.get("role_list", []) if hasattr(roles, "get") else roles.data.get("role_list", [])
        if role_list:
            for r in role_list:
                print(f"  {r.get('role', '?'):10s}  {r.get('principal', '?')}")
        else:
            print("  (no roles set)")
    except Exception as exc:
        print(f"  Error listing roles: {exc}")
        role_list = []

    if args.list_only:
        return

    # Check if role already exists
    app_principal = f"{app_client_id}@clients.auth.globus.org"
    existing = [r for r in role_list if r.get("principal") == app_principal and r.get("role") == args.role]
    if existing:
        print(f"\nRole '{args.role}' already granted to {app_principal}. Nothing to do.")
        return

    # Create the role
    print(f"\nGranting '{args.role}' role to {app_principal} on index {args.index}...")
    try:
        result = search_client.create_role(
            args.index,
            data={
                "principal": app_principal,
                "principal_type": "identity",
                "role": args.role,
            },
        )
        print(f"  Success: {result.data if hasattr(result, 'data') else result}")
    except globus_sdk.GlobusAPIError as exc:
        print(f"  API error: {exc.message} (code={exc.code}, status={exc.http_status})")
        sys.exit(1)

    # Verify
    print("\nVerifying roles...")
    roles = search_client.get_role_list(args.index)
    role_list = roles.get("role_list", []) if hasattr(roles, "get") else roles.data.get("role_list", [])
    for r in role_list:
        marker = " <-- NEW" if r.get("principal") == app_principal else ""
        print(f"  {r.get('role', '?'):10s}  {r.get('principal', '?')}{marker}")

    print("\nDone.")


if __name__ == "__main__":
    main()
