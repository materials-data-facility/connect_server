import hashlib
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

from fastapi import Depends, Header, HTTPException, Request

from v2.app.models import AuthContext

logger = logging.getLogger(__name__)

# Validated-token cache. Every authenticated request otherwise costs two
# Globus Auth round-trips (userinfo + dependent-token exchange), and a single
# signed-in detail-page load fires ~5 authenticated requests — enough
# sustained traffic to hit Globus Auth rate limits and hold Lambda
# concurrency slots for seconds per request. Entries are keyed by token hash
# (never the raw token) and expire after AUTH_CACHE_TTL_SECONDS; a revoked
# token therefore stays usable for at most the TTL, which matches common
# introspection-cache practice.
AUTH_CACHE_TTL_SECONDS = int(os.environ.get("AUTH_CACHE_TTL_SECONDS", "300"))
_AUTH_CACHE_MAX_ENTRIES = 256
_auth_cache: Dict[str, Tuple[float, AuthContext]] = {}

# The dependent-token grant fails deterministically when the Globus client
# isn't configured for it (UNAUTHORIZED_CLIENT). Without this flag every
# request re-attempts the doomed exchange — one guaranteed-400 Globus Auth
# call per request. Set once per container, cleared only on cold start.
_dependent_grant_unsupported = False


def _token_cache_key(token: str, groups_token: Optional[str] = None) -> str:
    """Hash of everything the cached AuthContext was derived from.

    The Groups token belongs in the key, not just the bearer token: group
    memberships come from it, so two requests with the same bearer token but
    different (or absent) Groups tokens legitimately produce different contexts.
    Keying on the bearer alone let the first request of a session decide, for the
    whole TTL, whether the caller appeared to have any groups — so a user who
    loaded the app before their client started sending X-Groups-Token stayed
    group-less (no submit, no curation) for up to five minutes afterwards, and a
    later group-less request could likewise serve a cached context that still had
    groups.
    """
    material = token if not groups_token else f"{token}\x00{groups_token}"
    return hashlib.sha256(material.encode()).hexdigest()


def _auth_cache_get(token: str, groups_token: Optional[str] = None) -> Optional[AuthContext]:
    key = _token_cache_key(token, groups_token)
    entry = _auth_cache.get(key)
    if not entry:
        return None
    expires_at, ctx = entry
    if time.monotonic() >= expires_at:
        _auth_cache.pop(key, None)
        return None
    return ctx


#: Bearer tokens Globus has rejected, keyed by token hash. Without this, every
#: request carrying a garbage bearer (optional-auth routes answer anonymously on
#: failure) costs a Globus Auth userinfo call, so a token spray could push MDF
#: into Globus rate limits and break sign-in for real users (SEC-M1). Only
#: definitive rejections are cached, never transport errors.
AUTH_NEGATIVE_CACHE_TTL_SECONDS = float(os.environ.get("AUTH_NEGATIVE_CACHE_TTL_SECONDS", "60"))
_AUTH_NEGATIVE_CACHE_MAX_ENTRIES = 4096
_auth_negative_cache: Dict[str, float] = {}
#: Globus Auth answers that mean "this token is bad", as opposed to throttling
#: (429) or timeouts (408), which say nothing about the token.
_DEFINITIVE_REJECTION_STATUSES = frozenset({400, 401, 403})


def _auth_rejected_recently(token: str) -> bool:
    key = _token_cache_key(token, None)
    expires_at = _auth_negative_cache.get(key)
    if expires_at is None:
        return False
    if time.monotonic() >= expires_at:
        _auth_negative_cache.pop(key, None)
        return False
    return True


def _auth_remember_rejection(token: str) -> None:
    if AUTH_NEGATIVE_CACHE_TTL_SECONDS <= 0:
        return
    now = time.monotonic()
    if len(_auth_negative_cache) >= _AUTH_NEGATIVE_CACHE_MAX_ENTRIES:
        for key in [k for k, exp in _auth_negative_cache.items() if exp <= now]:
            _auth_negative_cache.pop(key, None)
        if len(_auth_negative_cache) >= _AUTH_NEGATIVE_CACHE_MAX_ENTRIES:
            # Evict the soonest-to-expire quarter rather than clearing, so a
            # spray of fresh garbage tokens cannot flush the whole cache.
            for key in sorted(_auth_negative_cache, key=_auth_negative_cache.get)[
                : _AUTH_NEGATIVE_CACHE_MAX_ENTRIES // 4
            ]:
                _auth_negative_cache.pop(key, None)
    _auth_negative_cache[_token_cache_key(token, None)] = now + AUTH_NEGATIVE_CACHE_TTL_SECONDS


def _auth_cache_put(token: str, ctx: AuthContext, groups_token: Optional[str] = None) -> None:
    if AUTH_CACHE_TTL_SECONDS <= 0:
        return
    if len(_auth_cache) >= _AUTH_CACHE_MAX_ENTRIES:
        # Drop the soonest-to-expire entries rather than clearing everything.
        for key in sorted(_auth_cache, key=lambda k: _auth_cache[k][0])[
            : _AUTH_CACHE_MAX_ENTRIES // 4
        ]:
            _auth_cache.pop(key, None)
    _auth_cache[_token_cache_key(token, groups_token)] = (
        time.monotonic() + AUTH_CACHE_TTL_SECONDS,
        ctx,
    )


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


def _bearer_identity_ids(user_id: str, userinfo: Dict[str, Any]) -> set:
    """Every identity the bearer token vouches for: ``sub`` plus its linked identity set."""
    ids = {user_id}
    for identity in userinfo.get("identity_set") or []:
        sub = identity.get("sub") if isinstance(identity, dict) else None
        if sub:
            ids.add(sub)
    return ids


def _memberships_for_identities(groups: list, identity_ids: set) -> Dict[str, Dict[str, Any]]:
    """Keep only groups where an ACTIVE membership belongs to one of ``identity_ids``.

    ``GET /v2/groups/my_groups`` returns, per group, ``my_memberships`` — the
    memberships held by the identities behind the *Groups* token. A group counts
    for the bearer only if one of those memberships is the bearer's own identity
    (or a linked one). A group with no ``my_memberships`` at all cannot be bound
    and is dropped rather than trusted.
    """
    bound: Dict[str, Dict[str, Any]] = {}
    for group in groups:
        memberships = group.get("my_memberships") or []
        matched = any(
            m.get("identity_id") in identity_ids
            and (m.get("status") or "active").lower() == "active"
            for m in memberships
            if isinstance(m, dict)
        )
        if matched:
            bound[group["id"]] = {
                "name": group.get("name"),
                "description": group.get("description"),
            }
    return bound


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

    cached = _auth_cache_get(token, x_groups_token)
    if cached is not None:
        return cached
    if _auth_rejected_recently(token):
        raise HTTPException(status_code=401, detail="Invalid or expired token")

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
        except globus_sdk.AuthAPIError as exc:
            status = int(getattr(exc, "http_status", 0) or 0)
            if status >= 500 or status in (408, 429):
                # Globus is down or throttling us: not the caller's fault, so
                # never cache it and don't tell them their token is bad.
                logger.warning("Globus Auth unavailable (HTTP %s) during token check", status)
                raise HTTPException(status_code=503, detail="Authentication service unavailable")
            if status in _DEFINITIVE_REJECTION_STATUSES:
                _auth_remember_rejection(token)
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        user_id = userinfo.get("sub")
        if not user_id:
            _auth_remember_rejection(token)
            raise HTTPException(status_code=401, detail="Invalid or expired token")

        # Fetch group memberships.
        #
        # Strategy 1 (the supported path): the caller sends its own Globus Groups
        #   token in X-Groups-Token. Both clients request
        #   groups.api.globus.org:view_my_groups_and_memberships at login — the CLI
        #   in mdf/auth/globus.py, the web app in mdf-next2 app/providers.tsx — and
        #   forward the token on every request.
        #
        # Strategy 2 (fallback, currently inoperable): exchange the caller's token
        #   for a dependent Groups token. This cannot succeed with the deployed
        #   credentials: GLOBUS_CLIENT_ID is 86e4853e-9bdd-4ea5-9130-e4a0b0638400,
        #   a different Globus app from 4d5f8e8b-a61d-40d8-bb58-5a3f5d1d200a, the
        #   resource server that issues MDF Connect tokens. A dependent-token grant
        #   may only be performed by the app owning the scope the token was issued
        #   for, so Globus answers UNAUTHORIZED_CLIENT ("Client not configured to
        #   use the urn:globus:auth:grant_type:dependent_token grant") — registering
        #   a dependent scope on 86e4853e would not change that. Making it work
        #   would mean redeploying with the 4d5f8e8b client's own credentials.
        #   Kept because it costs one guarded attempt per container and would start
        #   working the moment those credentials are deployed.
        client_id = os.environ.get("GLOBUS_CLIENT_ID")
        client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")
        group_info = {}
        dependent_token = {}
        groups_token = x_groups_token  # Direct token from CLI

        # Fallback: dependent token exchange. Skipped for the container's
        # lifetime once Globus reports the client isn't configured for the
        # grant — that failure is deterministic, and retrying it per request
        # adds a guaranteed-400 Globus Auth call to every authed request.
        global _dependent_grant_unsupported
        if (
            not groups_token
            and client_id
            and client_secret
            and not _dependent_grant_unsupported
        ):
            exchange_token = x_mdf_token or token
            try:
                conf_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
                dependent_token = conf_client.oauth2_get_dependent_tokens(exchange_token).by_resource_server
                groups_token = dependent_token.get("groups.api.globus.org", {}).get("access_token")
            except globus_sdk.AuthAPIError as exc:
                if getattr(exc, "code", "") == "UNAUTHORIZED_CLIENT":
                    _dependent_grant_unsupported = True
                    logger.warning(
                        "Globus client %s cannot perform the dependent-token grant "
                        "for this token (it is not the token's resource server); "
                        "disabling groups exchange for this container. Callers must "
                        "send their own Groups token in X-Groups-Token.",
                        client_id,
                    )
                else:
                    logger.warning("Failed dependent token exchange for groups", exc_info=True)
            except Exception:
                logger.warning("Failed dependent token exchange for groups", exc_info=True)

        if groups_token:
            try:
                groups_client = globus_sdk.GroupsClient(
                    authorizer=globus_sdk.AccessTokenAuthorizer(groups_token)
                )
                groups = list(groups_client.get_my_groups())
            except Exception:
                groups = []
                logger.warning("Failed to fetch group memberships", exc_info=True)
            # Bind the Groups token to the bearer identity. The header is
            # caller-supplied, so without this check any authenticated caller
            # could pair their own bearer token with a curator's leaked Groups
            # token and inherit that curator's memberships.
            bearer_identity_ids = _bearer_identity_ids(user_id, userinfo)
            group_info = _memberships_for_identities(groups, bearer_identity_ids)
            if groups and not group_info:
                logger.warning(
                    "X-Groups-Token identity does not match bearer identity set for user %s",
                    user_id,
                )
                raise HTTPException(
                    status_code=401,
                    detail=(
                        "X-Groups-Token belongs to a different Globus identity than the "
                        "bearer token. Re-authenticate so both tokens come from the same login."
                    ),
                )

        ctx = AuthContext(
            user_id=user_id,
            name=userinfo.get("name"),
            user_email=userinfo.get("email"),
            identities=userinfo.get("identity_set", []),
            group_info=group_info,
            dependent_token=dependent_token,
        )
        _auth_cache_put(token, ctx, x_groups_token)
        return ctx
    except HTTPException:
        raise
    except Exception:
        logger.warning("Authentication failed", exc_info=True)
        raise HTTPException(status_code=401, detail="Authentication failed")


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


def _caller_identity_ids(auth: Optional[AuthContext]) -> set:
    """The caller's primary Globus identity plus every linked identity."""
    if not auth:
        return set()
    ids = {auth.user_id} if auth.user_id else set()
    for identity in auth.identities or []:
        sub = identity.get("sub") if isinstance(identity, dict) else identity
        if isinstance(sub, str) and sub:
            ids.add(sub)
    return ids


def _caller_in_acl(auth: Optional[AuthContext], record: Dict[str, Any]) -> bool:
    """True when one of the caller's identities or groups is in the record's acl.

    Mirrors the principals ``search_client.resolve_visible_to`` writes into the
    index, so a collaborator who can find a restricted dataset in search can
    also open it.
    """
    if not auth:
        return False
    from v2.submission_utils import normalize_acl_principal, resolve_record_acl

    acl = resolve_record_acl(record) or []
    principals = {normalize_acl_principal(entry) for entry in acl if entry}
    principals.discard(None)
    mine = {f"urn:globus:auth:identity:{i}" for i in _caller_identity_ids(auth)}
    mine |= {f"urn:globus:groups:id:{g}" for g in (auth.group_info or {})}
    return bool(principals & mine)


def is_submission_owner_or_curator(
    auth: Optional[AuthContext], submission: Dict[str, Any]
) -> bool:
    """Non-raising form of :func:`ensure_submission_owner_or_curator`."""
    if not auth or not submission:
        return False
    owner_id = submission.get("user_id")
    # A user may log in with any identity linked to the one that submitted.
    if owner_id and owner_id in _caller_identity_ids(auth):
        return True
    return is_curator(auth)


#: Unpublished statuses a dataset's owner (and curators) may read.
OWNER_VISIBLE_UNPUBLISHED_STATUSES = frozenset({"pending_curation", "approved", "rejected", "withdrawn"})


def can_view_dataset(auth: Optional[AuthContext], record: Optional[Dict[str, Any]]) -> bool:
    """True when the caller may read a dataset's content.

    Unpublished versions (pending_curation / approved / rejected) are viewable
    by their owner and curators only. A published dataset is viewable when it
    is public, OR the caller is its owner or a curator. "published" alone is
    not enough: a restricted
    dataset is published into Globus Search with a ``visible_to`` limited to its
    acl identities, so serving it to anonymous callers through cards, citations,
    detail pages or previews is an ACL bypass around that gate.
    """
    if not record:
        return False
    status = record.get("status")
    if status in OWNER_VISIBLE_UNPUBLISHED_STATUSES:
        # In-flight and rejected versions exist only for their submitter and
        # the curators: /my-datasets links owners to them, and OwnerTools
        # (withdraw / resubmit) lives on that page. Everyone else gets the
        # same 404 as for a nonexistent dataset.
        return is_submission_owner_or_curator(auth, record)
    if status != "published":
        return False
    if is_submission_owner_or_curator(auth, record):
        return True
    # Imported lazily: v2.search pulls the store/stream-store modules and is
    # itself imported by the search router, so a module-level import here would
    # close an import cycle through the app package.
    from v2.search import dataset_is_public

    return dataset_is_public(record) or _caller_in_acl(auth, record)


def ensure_stream_owner_or_curator(auth: AuthContext, stream: Dict[str, Any]) -> None:
    """Only the stream owner (or a curator) may mutate/view a stream."""
    owner_id = stream.get("user_id")
    if owner_id and owner_id == auth.user_id:
        return
    if is_curator(auth):
        return
    raise HTTPException(status_code=403, detail="You do not have permission for this stream")


def _groups_unresolved_hint(auth: AuthContext) -> str:
    """Extra 403 detail when the caller's groups could not be resolved at all.

    An empty ``group_info`` is indistinguishable from "member of nothing" to the
    permission checks, but the two need very different fixes: a real non-member
    must be added to the group, whereas a caller whose Groups token never arrived
    just needs to re-authenticate so the client requests the Groups scope. Saying
    so here is what keeps the previous cycle's "you must be a member" red herring
    from being rediscovered.
    """
    if auth.group_info:
        return ""
    return (
        " No Globus group memberships could be resolved for this request — the"
        " caller sent no X-Groups-Token. Re-authenticate (`mdf login`, or log out"
        " and back in on the web app) so the Groups scope is requested."
    )


async def require_curator(
    auth: AuthContext = Depends(get_auth),
) -> AuthContext:
    if not is_curator(auth):
        raise HTTPException(
            status_code=403,
            detail="You do not have curator permissions." + _groups_unresolved_hint(auth),
        )
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
            detail=(
                "You must be a member of the MDF submitters group to submit datasets."
                + _groups_unresolved_hint(auth)
            ),
        )
    return auth
