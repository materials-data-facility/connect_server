"""Tests for how the caller's Globus Groups token becomes group memberships.

This is the whole basis of ``is_submitter`` and curator-by-group, and it has
exactly one working source: the ``X-Groups-Token`` header that the CLI
(``mdf/auth/globus.py``) and the web app (``mdf-next2`` ``app/providers.tsx``)
request at login and forward on every request.

The other source in ``get_auth`` — a dependent-token exchange off the caller's
own token — cannot work with the deployed credentials. ``GLOBUS_CLIENT_ID`` is
``86e4853e-9bdd-4ea5-9130-e4a0b0638400``, a *different* Globus app from
``4d5f8e8b-a61d-40d8-bb58-5a3f5d1d200a``, the resource server that issues MDF
Connect tokens; only the app owning a token's scope may exchange it, so Globus
answers ``UNAUTHORIZED_CLIENT`` and registering a dependent scope on
``86e4853e`` would not change that.

That asymmetry cost a cycle of debugging once already: a caller with no Groups
token looks identical to a caller who belongs to nothing, so ``POST /submit``
403s every legitimate submitter with "you must be a member of the MDF submitters
group". These tests pin both the working path and the diagnostic that tells the
two cases apart.
"""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import auth as auth_module
from v2.app.models import AuthContext

CURATOR_GROUP = "3ce2c53e-3752-11e8-891c-0e00fd09bf20"
SUBMITTER_GROUP = "cc192dca-3751-11e8-90c1-0a7c735d220a"


class _FakeAuthAPIError(Exception):
    def __init__(self, code: str = "UNAUTHORIZED_CLIENT"):
        super().__init__(code)
        self.code = code


def _fake_globus_sdk(groups_by_token: dict, exchange_calls: list, groups_calls: list):
    """A globus_sdk stand-in where only a real Groups token yields memberships."""
    mod = types.ModuleType("globus_sdk")
    mod.AuthAPIError = _FakeAuthAPIError

    class AccessTokenAuthorizer:
        def __init__(self, token):
            self.token = token

    class AuthClient:
        def __init__(self, authorizer=None):
            self._authorizer = authorizer

        def userinfo(self):
            return {
                "sub": "user-1",
                "name": "Test User",
                "email": "test@example.com",
                "identity_set": [{"sub": "user-1"}, {"sub": "user-1-linked"}],
            }

    class ConfidentialAppAuthClient:
        def __init__(self, client_id, client_secret):
            pass

        def oauth2_get_dependent_tokens(self, token):
            # Mirrors production: the deployed client is not the resource server
            # for this token, so Globus refuses the grant outright.
            exchange_calls.append(token)
            raise _FakeAuthAPIError("UNAUTHORIZED_CLIENT")

    class GroupsClient:
        def __init__(self, authorizer=None):
            self._token = authorizer.token

        def get_my_groups(self):
            groups_calls.append(self._token)
            return groups_by_token.get(self._token, [])

    mod.AccessTokenAuthorizer = AccessTokenAuthorizer
    mod.AuthClient = AuthClient
    mod.ConfidentialAppAuthClient = ConfidentialAppAuthClient
    mod.GroupsClient = GroupsClient
    return mod


@pytest.fixture()
def prod_auth(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AUTH_MODE", "production")
    monkeypatch.delenv("LOCAL_DEV_AUTH", raising=False)
    monkeypatch.setenv("GLOBUS_CLIENT_ID", "86e4853e-9bdd-4ea5-9130-e4a0b0638400")
    monkeypatch.setenv("GLOBUS_CLIENT_SECRET", "test-secret")
    # The cache is keyed by bearer token, so it would otherwise let one test's
    # groups leak into the next case that reuses the same token.
    auth_module._auth_cache.clear()
    monkeypatch.setattr(auth_module, "_dependent_grant_unsupported", False)
    exchange_calls: list = []
    groups_calls: list = []
    # ``my_memberships`` is what binds a Groups token to an identity: the
    # fixture's bearer is ``user-1`` (see the fake ``userinfo``), so these
    # groups count for it. ``other-tok`` belongs to a different identity.
    groups_by_token = {
        "groups-tok": [
            {"id": SUBMITTER_GROUP, "name": "MDF Connect Publishers", "description": "",
             "my_memberships": [{"identity_id": "user-1", "status": "active"}]},
            {"id": CURATOR_GROUP, "name": "MDF Open Curator Group", "description": "",
             "my_memberships": [{"identity_id": "user-1", "status": "active"}]},
        ],
        "other-tok": [
            {"id": CURATOR_GROUP, "name": "MDF Open Curator Group", "description": "",
             "my_memberships": [{"identity_id": "someone-else", "status": "active"}]},
        ],
        "mixed-tok": [
            {"id": CURATOR_GROUP, "name": "MDF Open Curator Group", "description": "",
             "my_memberships": [{"identity_id": "someone-else", "status": "active"}]},
            {"id": SUBMITTER_GROUP, "name": "MDF Connect Publishers", "description": "",
             "my_memberships": [{"identity_id": "user-1", "status": "active"}]},
        ],
        "linked-tok": [
            {"id": CURATOR_GROUP, "name": "MDF Open Curator Group", "description": "",
             "my_memberships": [{"identity_id": "user-1-linked", "status": "active"}]},
        ],
        "unbound-tok": [
            {"id": CURATOR_GROUP, "name": "MDF Open Curator Group", "description": ""},
        ],
    }
    monkeypatch.setitem(
        sys.modules,
        "globus_sdk",
        _fake_globus_sdk(groups_by_token, exchange_calls, groups_calls),
    )
    yield exchange_calls, groups_calls
    auth_module._auth_cache.clear()


def _get_auth(token: str, groups_token: str | None = None, mdf_token: str | None = None):
    return asyncio.run(
        auth_module.get_auth(
            request=None,
            x_user_id=None,
            x_user_email=None,
            x_user_name=None,
            authorization=f"Bearer {token}",
            x_mdf_token=mdf_token,
            x_groups_token=groups_token,
        )
    )


class TestGroupsFromCallerToken:
    def test_x_groups_token_resolves_memberships(self, prod_auth):
        _, groups_calls = prod_auth
        ctx = _get_auth("bearer-a", groups_token="groups-tok")

        assert set(ctx.group_info) == {SUBMITTER_GROUP, CURATOR_GROUP}
        assert groups_calls == ["groups-tok"]

    def test_x_groups_token_skips_the_doomed_dependent_exchange(self, prod_auth):
        exchange_calls, _ = prod_auth
        _get_auth("bearer-a", groups_token="groups-tok")

        # No point spending a guaranteed-400 Globus call when the caller already
        # handed us the token.
        assert exchange_calls == []

    def test_without_the_header_no_groups_resolve(self, prod_auth):
        exchange_calls, groups_calls = prod_auth
        ctx = _get_auth("bearer-a")

        # This is the frontend's old behavior: the exchange is attempted, fails,
        # and the caller looks like a member of nothing.
        assert ctx.group_info == {}
        assert exchange_calls == ["bearer-a"]
        assert groups_calls == []

    def test_dependent_exchange_cannot_rescue_a_connect_token_either(self, prod_auth):
        exchange_calls, _ = prod_auth
        ctx = _get_auth("bearer-a", mdf_token="connect-tok")

        # X-MDF-Token routes the *connect* token into the exchange, which is the
        # arrangement that would work if the deployed client owned that scope.
        # It does not, so this stays empty.
        assert ctx.group_info == {}
        assert exchange_calls == ["connect-tok"]


class TestCacheKeyIncludesGroupsToken:
    """Group state must not leak between requests sharing a bearer token.

    Keying the AuthContext cache on the bearer token alone meant the first
    request of a session fixed the caller's apparent group membership for the
    whole TTL — so a user whose client only started sending X-Groups-Token after
    that first load stayed group-less for five more minutes.
    """

    def test_adding_the_groups_token_is_not_served_from_the_groupless_entry(self, prod_auth):
        _, groups_calls = prod_auth
        first = _get_auth("bearer-a")
        assert first.group_info == {}

        second = _get_auth("bearer-a", groups_token="groups-tok")
        assert set(second.group_info) == {SUBMITTER_GROUP, CURATOR_GROUP}
        assert groups_calls == ["groups-tok"]

    def test_dropping_the_groups_token_does_not_reuse_the_grouped_entry(self, prod_auth):
        _get_auth("bearer-a", groups_token="groups-tok")
        without = _get_auth("bearer-a")

        assert without.group_info == {}

    def test_same_pair_still_hits_the_cache(self, prod_auth):
        _, groups_calls = prod_auth
        _get_auth("bearer-a", groups_token="groups-tok")
        _get_auth("bearer-a", groups_token="groups-tok")

        # The point of the cache: no second Globus round-trip.
        assert groups_calls == ["groups-tok"]

    def test_cache_never_stores_the_raw_groups_token(self, prod_auth):
        _get_auth("bearer-a", groups_token="groups-tok")

        assert all("groups-tok" not in key for key in auth_module._auth_cache)


class TestPermissionsFollowGroups:
    @pytest.fixture(autouse=True)
    def _curator_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("CURATOR_GROUP_IDS", CURATOR_GROUP)
        monkeypatch.setenv("CURATOR_USER_IDS", "")
        monkeypatch.delenv("ALLOW_ALL_CURATORS", raising=False)
        monkeypatch.setenv("REQUIRED_GROUP_MEMBERSHIP", SUBMITTER_GROUP)

    def test_curator_and_submitter_granted_with_the_groups_token(self, prod_auth):
        ctx = _get_auth("bearer-a", groups_token="groups-tok")

        assert auth_module.is_curator(ctx) is True
        assert auth_module.is_submitter(ctx) is True

    def test_both_denied_without_it(self, prod_auth):
        ctx = _get_auth("bearer-a")

        assert auth_module.is_curator(ctx) is False
        assert auth_module.is_submitter(ctx) is False


class TestCuratorUserIdsBreakGlass:
    """CURATOR_USER_IDS must work with NO groups at all.

    That is the entire point: curator rights otherwise depend on a Globus Groups
    lookup, and the dependent-token fallback is inoperable, so a Groups outage
    would drop every curator simultaneously. This path is only ever exercised
    during that outage — precisely when nobody is in a position to debug it.
    """

    BLAISZIK_GLOBUSID = "c8741264-d274-11e5-bee7-f30dff9f1ea8"
    BLAISZIK_UCHICAGO = "c8745ef4-d274-11e5-bee8-3b6845397ac9"

    @pytest.fixture(autouse=True)
    def _break_glass_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("AUTH_MODE", "production")
        monkeypatch.delenv("LOCAL_DEV_AUTH", raising=False)
        monkeypatch.delenv("ALLOW_ALL_CURATORS", raising=False)
        monkeypatch.setenv("CURATOR_GROUP_IDS", CURATOR_GROUP)
        monkeypatch.setenv(
            "CURATOR_USER_IDS", f"{self.BLAISZIK_GLOBUSID},{self.BLAISZIK_UCHICAGO}"
        )

    @pytest.mark.parametrize(
        "sub",
        [
            pytest.param(BLAISZIK_GLOBUSID, id="globusid-identity"),
            pytest.param(BLAISZIK_UCHICAGO, id="uchicago-identity"),
        ],
    )
    def test_either_linked_identity_is_a_curator_without_groups(self, sub):
        # is_curator() matches userinfo['sub'] — the EFFECTIVE identity of the
        # presented token. Linked identities produce different subs, so listing
        # only one would work from the CLI and fail from the browser (or vice
        # versa) with no obvious cause.
        ctx = AuthContext(user_id=sub, group_info={})

        assert auth_module.is_curator(ctx) is True

    def test_a_stray_backslash_would_break_the_match(self, monkeypatch):
        # SAM passes '\,' through literally, so escaping the comma in
        # samconfig.toml deploys an id with a trailing backslash that can never
        # match. Pinning it here makes that misconfiguration a test failure
        # rather than a silent lockout discovered mid-outage.
        monkeypatch.setenv(
            "CURATOR_USER_IDS", f"{self.BLAISZIK_GLOBUSID}\\,{self.BLAISZIK_UCHICAGO}"
        )
        ctx = AuthContext(user_id=self.BLAISZIK_GLOBUSID, group_info={})

        assert auth_module.is_curator(ctx) is False

    def test_an_unlisted_user_with_no_groups_is_still_denied(self):
        ctx = AuthContext(user_id="somebody-else", group_info={})

        assert auth_module.is_curator(ctx) is False


class TestUnresolvedGroupsDiagnostic:
    """The 403 must distinguish "not a member" from "no Groups token arrived"."""

    @pytest.fixture(autouse=True)
    def _gated_env(self, monkeypatch: pytest.MonkeyPatch):
        # Both gates must actually be armed, or is_submitter/is_curator pass and
        # there is no 403 to inspect.
        monkeypatch.setenv("AUTH_MODE", "production")
        monkeypatch.delenv("LOCAL_DEV_AUTH", raising=False)
        monkeypatch.setenv("REQUIRED_GROUP_MEMBERSHIP", SUBMITTER_GROUP)
        monkeypatch.setenv("CURATOR_GROUP_IDS", CURATOR_GROUP)
        monkeypatch.setenv("CURATOR_USER_IDS", "")
        monkeypatch.delenv("ALLOW_ALL_CURATORS", raising=False)

    def _ctx(self, group_info):
        return AuthContext(user_id="user-1", group_info=group_info)

    def test_curator_403_names_the_missing_header(self):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(auth_module.require_curator(self._ctx({})))

        assert exc.value.status_code == 403
        assert "X-Groups-Token" in exc.value.detail
        assert "Re-authenticate" in exc.value.detail

    def test_submitter_403_names_the_missing_header(self):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(auth_module.require_submitter(self._ctx({})))

        assert exc.value.status_code == 403
        assert "X-Groups-Token" in exc.value.detail

    def test_genuine_non_member_gets_no_misleading_token_advice(self):
        # Groups resolved fine — this user simply isn't a curator.
        ctx = self._ctx({"some-other-group": {"name": "Other", "description": ""}})

        with pytest.raises(HTTPException) as exc:
            asyncio.run(auth_module.require_curator(ctx))

        assert "X-Groups-Token" not in exc.value.detail

    def test_submitter_gate_is_off_when_no_group_is_required(self, monkeypatch):
        monkeypatch.setenv("REQUIRED_GROUP_MEMBERSHIP", "")
        # An unconfigured deployment must not become unusable; require_submitter
        # passes and no diagnostic is needed.
        assert asyncio.run(auth_module.require_submitter(self._ctx({}))) is not None


class TestGroupsTokenIsBoundToTheBearer:
    """A Groups token only counts for the identity that owns it.

    Without this, any authenticated caller could pair their own bearer token
    with a curator's leaked Groups token and inherit curator rights. The
    binding uses ``my_memberships[].identity_id`` from the Groups API against
    the bearer's ``sub`` + ``identity_set``.
    """

    def test_another_identitys_groups_token_is_rejected(self, prod_auth):
        with pytest.raises(HTTPException) as exc:
            _get_auth("bearer-1", groups_token="other-tok")
        assert exc.value.status_code == 401
        assert "different Globus identity" in exc.value.detail

    def test_only_the_bearers_own_memberships_survive_a_mixed_token(self, prod_auth):
        ctx = _get_auth("bearer-1", groups_token="mixed-tok")
        assert SUBMITTER_GROUP in ctx.group_info
        assert CURATOR_GROUP not in ctx.group_info
        assert auth_module.is_submitter(ctx)
        assert not auth_module.is_curator(ctx)

    def test_a_linked_identity_counts(self, prod_auth):
        ctx = _get_auth("bearer-1", groups_token="linked-tok")
        assert CURATOR_GROUP in ctx.group_info

    def test_a_group_without_membership_records_is_not_trusted(self, prod_auth):
        with pytest.raises(HTTPException) as exc:
            _get_auth("bearer-1", groups_token="unbound-tok")
        assert exc.value.status_code == 401

    def test_rejected_pairing_is_not_cached(self, prod_auth):
        with pytest.raises(HTTPException):
            _get_auth("bearer-1", groups_token="other-tok")
        assert auth_module._auth_cache_get("bearer-1", "other-tok") is None
