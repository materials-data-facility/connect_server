#!/usr/bin/env python3
"""Authentication helpers for read-only access to the legacy MDF index."""

import os

NATIVE_APP_CLIENT_ID = "074cebcc-19ad-4332-bbf2-78402291b659"
SEARCH_SCOPE = "urn:globus:auth:scope:search.api.globus.org:all"


def _interactive_search_client(globus_sdk):
    """Run the historical native-app authorization-code flow."""
    client = globus_sdk.NativeAppAuthClient(NATIVE_APP_CLIENT_ID)
    client.oauth2_start_flow(requested_scopes=SEARCH_SCOPE)

    authorize_url = client.oauth2_get_authorize_url()
    print("Go to this URL and login:\n\n  {}\n".format(authorize_url))
    auth_code = input("Paste the authorization code here: ").strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    search_token_data = token_response.by_resource_server.get(
        "search.api.globus.org"
    )
    if not search_token_data:
        raise RuntimeError(
            "No Globus Search token was returned; check the native app scopes."
        )

    access_token = (
        search_token_data.get("access_token")
        if isinstance(search_token_data, dict)
        else getattr(search_token_data, "access_token", None)
    )
    if not access_token:
        raise RuntimeError("The Globus Search access token was empty.")

    authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
    return globus_sdk.SearchClient(authorizer=authorizer)


def get_legacy_search_client(interactive_ok: bool = True):
    """Return an authenticated Globus Search client for legacy read access.

    Confidential client credentials take precedence whenever both environment
    variables are present. Interactive native-app authentication is retained
    for operators running the extraction script by hand.
    """
    import globus_sdk

    client_id = os.environ.get("GLOBUS_CLIENT_ID")
    client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")
    if client_id and client_secret:
        auth_client = globus_sdk.ConfidentialAppAuthClient(
            client_id, client_secret
        )
        authorizer = globus_sdk.ClientCredentialsAuthorizer(
            auth_client, SEARCH_SCOPE
        )
        return globus_sdk.SearchClient(authorizer=authorizer)

    if interactive_ok:
        return _interactive_search_client(globus_sdk)

    missing = [
        name
        for name, value in (
            ("GLOBUS_CLIENT_ID", client_id),
            ("GLOBUS_CLIENT_SECRET", client_secret),
        )
        if not value
    ]
    raise RuntimeError(
        "Non-interactive legacy Search authentication requires "
        "GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET; missing: {}.".format(
            ", ".join(missing)
        )
    )
