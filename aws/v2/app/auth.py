import logging
import os
from typing import Any, Dict, Optional

from fastapi import Depends, Header, HTTPException, Request

from v2.app.models import AuthContext

logger = logging.getLogger(__name__)


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _allow_local_dev_auth() -> bool:
    return _env_truthy("LOCAL_DEV_AUTH") or _env_truthy("AWS_SAM_LOCAL")


def get_auth_mode() -> str:
    mode = os.environ.get("AUTH_MODE", "production")
    normalized = (mode or "production").strip().lower() or "production"
    if normalized == "dev" and not _allow_local_dev_auth():
        logger.error("AUTH_MODE=dev requested without local runtime guard; forcing production auth")
        return "production"
    return normalized


async def get_auth(
    request: Request,
    x_user_id: Optional[str] = Header(None),
    x_user_email: Optional[str] = Header(None),
    x_user_name: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None),
    x_mdf_token: Optional[str] = Header(None),
    x_groups_token: Optional[str] = Header(None),
) -> AuthContext:
    if get_auth_mode() == "dev":
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

        # Fetch group memberships.
        # Strategy 1: Use the direct groups token from X-Groups-Token header
        #             (CLI requests Groups scope during login and sends it here).
        # Strategy 2: Dependent token exchange via X-MDF-Token / Bearer token
        #             (works if the Globus app has Groups as a dependent scope).
        client_id = os.environ.get("GLOBUS_CLIENT_ID")
        client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")
        group_info = {}
        dependent_token = {}
        groups_token = x_groups_token  # Direct token from CLI

        # Fallback: dependent token exchange
        if not groups_token and client_id and client_secret:
            exchange_token = x_mdf_token or token
            try:
                conf_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
                dependent_token = conf_client.oauth2_get_dependent_tokens(exchange_token).by_resource_server
                groups_token = dependent_token.get("groups.api.globus.org", {}).get("access_token")
            except Exception:
                logger.warning("Failed dependent token exchange for groups", exc_info=True)

        if groups_token:
            try:
                groups_client = globus_sdk.GroupsClient(
                    authorizer=globus_sdk.AccessTokenAuthorizer(groups_token)
                )
                groups = groups_client.get_my_groups()
                group_info = {
                    group["id"]: {"name": group["name"], "description": group["description"]}
                    for group in groups
                }
            except Exception:
                logger.warning("Failed to fetch group memberships", exc_info=True)

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
    x_mdf_token: Optional[str] = Header(None),
    x_groups_token: Optional[str] = Header(None),
) -> Optional[AuthContext]:
    if not authorization and not x_user_id and get_auth_mode() != "dev":
        return None
    try:
        return await get_auth(request, x_user_id, x_user_email, x_user_name, authorization, x_mdf_token, x_groups_token)
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


def is_submission_owner_or_curator(
    auth: Optional[AuthContext], submission: Dict[str, Any]
) -> bool:
    """Non-raising form of :func:`ensure_submission_owner_or_curator`."""
    if not auth or not submission:
        return False
    owner_id = submission.get("user_id")
    if owner_id and owner_id == auth.user_id:
        return True
    return is_curator(auth)


def can_view_dataset(auth: Optional[AuthContext], record: Optional[Dict[str, Any]]) -> bool:
    """True when the caller may read a dataset's content.

    A dataset is viewable when it is published AND (it is public, OR the caller
    is its owner or a curator). "published" alone is not enough: a restricted
    dataset is published into Globus Search with a ``visible_to`` limited to its
    acl identities, so serving it to anonymous callers through cards, citations,
    detail pages or previews is an ACL bypass around that gate.
    """
    if not record or record.get("status") != "published":
        return False
    if is_submission_owner_or_curator(auth, record):
        return True
    # Imported lazily: v2.search pulls the store/stream-store modules and is
    # itself imported by the search router, so a module-level import here would
    # close an import cycle through the app package.
    from v2.search import dataset_is_public

    return dataset_is_public(record)


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


def is_submitter(auth: AuthContext) -> bool:
    """Check whether the user is allowed to submit datasets.

    In dev-auth mode (or when REQUIRED_GROUP_MEMBERSHIP is empty) everyone
    is allowed. In production the user must belong to the submitter group.
    """
    if get_auth_mode() == "dev":
        return True

    required = os.environ.get("REQUIRED_GROUP_MEMBERSHIP", "").strip()
    if not required:
        return True

    user_groups = set((auth.group_info or {}).keys())
    return required in user_groups


async def require_submitter(
    auth: AuthContext = Depends(get_auth),
) -> AuthContext:
    if not is_submitter(auth):
        raise HTTPException(
            status_code=403,
            detail="You must be a member of the MDF submitters group to submit datasets",
        )
    return auth
