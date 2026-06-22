#!/usr/bin/env python3
"""Test Globus HTTPS upload to MDF endpoint.

Uses Globus native app auth to get a token for the NCSA HTTPS endpoint,
then uploads a test file.

Run interactively:
    python test_globus_upload.py
"""

import json
import os
import sys
from datetime import datetime

import httpx

try:
    from globus_sdk import NativeAppAuthClient
except ImportError:
    print("Please install globus-sdk: pip install globus-sdk")
    sys.exit(1)


# MDF's registered native app client ID
MDF_CLIENT_ID = "984464e2-90ab-433d-8145-ac0215d26c8e"

# NCSA endpoint UUID (from Foundry code)
NCSA_ENDPOINT_UUID = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"

# Scope for HTTPS access to NCSA endpoint
NCSA_HTTPS_SCOPE = f"https://auth.globus.org/scopes/{NCSA_ENDPOINT_UUID}/https"

# Token storage location
TOKEN_FILE = os.path.expanduser("~/.mdf/v2_https_tokens.json")


def load_tokens():
    """Load cached tokens if they exist."""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE) as f:
            return json.load(f)
    return None


def save_tokens(tokens):
    """Save tokens to cache file."""
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f, indent=2)
    print(f"Tokens saved to {TOKEN_FILE}")


def get_tokens():
    """Get or refresh Globus tokens for NCSA HTTPS endpoint."""

    # Try cached tokens first
    cached = load_tokens()
    if cached and cached.get("access_token"):
        print(f"Using cached tokens from {TOKEN_FILE}")
        return cached["access_token"]

    # Need to authenticate
    print("\nStarting Globus authentication...")
    print(f"Scope: {NCSA_HTTPS_SCOPE}\n")

    auth_client = NativeAppAuthClient(MDF_CLIENT_ID)
    auth_client.oauth2_start_flow(
        requested_scopes=[NCSA_HTTPS_SCOPE],
        refresh_tokens=True,
    )

    authorize_url = auth_client.oauth2_get_authorize_url()
    print(f"Please visit this URL:\n{authorize_url}\n")

    auth_code = input("Enter the authorization code: ").strip()

    # Exchange code for tokens
    token_response = auth_client.oauth2_exchange_code_for_tokens(auth_code)

    # Get the HTTPS token for our endpoint
    https_tokens = token_response.by_resource_server.get(NCSA_ENDPOINT_UUID)
    if not https_tokens:
        print(f"Available resource servers: {list(token_response.by_resource_server.keys())}")
        raise RuntimeError(f"No token received for {NCSA_ENDPOINT_UUID}")

    # Cache tokens
    save_tokens(dict(https_tokens))

    return https_tokens["access_token"]


def upload_test_file(access_token: str):
    """Upload a test file to MDF endpoint via HTTPS."""

    # Test file content
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    content = f"Hello from MDF v2 backend test - {timestamp}\n"

    # Upload URL - note: endpoint uses the short hostname format
    base_url = "https://data.materialsdatafacility.org"
    upload_path = "/tmp/testing/mdf_v2_test.txt"
    url = f"{base_url}{upload_path}"

    print(f"\nUploading to: {url}")
    print(f"Content: {content.strip()}")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "text/plain",
    }

    # Use httpx with redirect following
    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        response = client.put(url, content=content.encode(), headers=headers)

        print(f"\nResponse status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")

        if response.status_code in (200, 201, 204):
            print("\n✓ Upload successful!")

            # Try to read it back
            print("\nReading file back...")
            get_response = client.get(url, headers=headers)
            print(f"GET status: {get_response.status_code}")
            if get_response.status_code == 200:
                print(f"Content: {get_response.text}")
        else:
            print(f"\n✗ Upload failed: {response.text}")
            return False

    return True


def main():
    print("=" * 60)
    print("MDF v2 Globus HTTPS Upload Test")
    print("=" * 60)

    # Get authentication token
    access_token = get_tokens()
    print(f"\nGot access token: {access_token[:20]}...")

    # Upload test file
    success = upload_test_file(access_token)

    if success:
        print("\n" + "=" * 60)
        print("Test completed successfully!")
        print("=" * 60)
    else:
        print("\n" + "=" * 60)
        print("Test failed - see errors above")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
