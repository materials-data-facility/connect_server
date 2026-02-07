import os
from typing import Any, Dict, Optional

from fastapi import Depends, Header, HTTPException, Request

from v2.app.models import AuthContext


AUTH_MODE = os.environ.get("AUTH_MODE", "dev")


async def get_auth(
    request: Request,
    x_user_id: Optional[str] = Header(None),
    x_user_email: Optional[str] = Header(None),
    x_user_name: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None),
) -> AuthContext:
    if AUTH_MODE == "dev":
        user_id = x_user_id or os.environ.get("LOCAL_USER_ID", "local-user")
        user_email = x_user_email or os.environ.get("LOCAL_USER_EMAIL", "local@example.com")
        name = x_user_name or os.environ.get("LOCAL_USER_NAME", "Local User")
        return AuthContext(
            user_id=user_id,
            name=name,
            user_email=user_email,
            identities=[],
            group_info={},
            dependent_token={},
        )

    # Production mode: Globus token introspection
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    token = authorization.replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    try:
        import globus_sdk

        # Validate the token by calling the Globus userinfo endpoint.
        # This works with any valid Globus access token regardless of
        # which resource server it was issued for (unlike introspect,
        # which only reports active=True for the introspecting app's
        # own resource server).
        try:
            ac = globus_sdk.AuthClient(
                authorizer=globus_sdk.AccessTokenAuthorizer(token)
            )
            userinfo = ac.userinfo()
        except globus_sdk.AuthAPIError:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        user_id = userinfo.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Optionally fetch group info via confidential app dependent tokens
        client_id = os.environ.get("GLOBUS_CLIENT_ID")
        client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")
        group_info = {}
        dependent_token = {}
        if client_id and client_secret:
            try:
                conf_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
                dependent_token = conf_client.oauth2_get_dependent_tokens(token).by_resource_server
                groups_token = dependent_token.get("groups.api.globus.org", {}).get("access_token")
                if groups_token:
                    groups_client = globus_sdk.GroupsClient(
                        authorizer=globus_sdk.AccessTokenAuthorizer(groups_token)
                    )
                    groups = groups_client.get_my_groups()
                    group_info = {
                        group["id"]: {"name": group["name"], "description": group["description"]}
                        for group in groups
                    }
            except Exception:
                pass

        return AuthContext(
            user_id=user_id,
            name=userinfo.get("name"),
            user_email=userinfo.get("email"),
            identities=userinfo.get("identity_set", []),
            group_info=group_info,
            dependent_token=dependent_token,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Authentication failed: {e}")


async def get_optional_auth(
    request: Request,
    x_user_id: Optional[str] = Header(None),
    x_user_email: Optional[str] = Header(None),
    x_user_name: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None),
) -> Optional[AuthContext]:
    if not authorization and not x_user_id and AUTH_MODE != "dev":
        return None
    try:
        return await get_auth(request, x_user_id, x_user_email, x_user_name, authorization)
    except HTTPException:
        return None


def is_curator(auth: AuthContext) -> bool:
    curator_user_ids = set(os.environ.get("CURATOR_USER_IDS", "").split(",")) - {""}
    curator_group_ids = set(os.environ.get("CURATOR_GROUP_IDS", "").split(",")) - {""}

    if auth.user_id in curator_user_ids:
        return True

    user_groups = set((auth.group_info or {}).keys())
    if user_groups & curator_group_ids:
        return True

    if os.environ.get("ALLOW_ALL_CURATORS", "").lower() in ("true", "1", "yes"):
        return True

    return False


def _is_curator(auth: AuthContext) -> bool:
    """Backward-compatible alias."""
    return is_curator(auth)


def ensure_submission_owner_or_curator(auth: AuthContext, submission: Dict[str, Any]) -> None:
    """Only the submitter (or a curator) may mutate a submission."""
    owner_id = submission.get("user_id")
    if owner_id and owner_id == auth.user_id:
        return
    if is_curator(auth):
        return
    raise HTTPException(status_code=403, detail="You do not have permission for this submission")


def ensure_stream_owner_or_curator(auth: AuthContext, stream: Dict[str, Any]) -> None:
    """Only the stream owner (or a curator) may mutate/view a stream."""
    owner_id = stream.get("user_id")
    if owner_id and owner_id == auth.user_id:
        return
    if is_curator(auth):
        return
    raise HTTPException(status_code=403, detail="You do not have permission for this stream")


async def require_curator(
    auth: AuthContext = Depends(get_auth),
) -> AuthContext:
    if not is_curator(auth):
        raise HTTPException(status_code=403, detail="You do not have curator permissions")
    return auth
