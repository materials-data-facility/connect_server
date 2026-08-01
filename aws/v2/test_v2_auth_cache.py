"""Tests for the production-auth token cache and the dependent-grant
negative cache.

Every authenticated request used to cost two live Globus Auth calls
(userinfo + a dependent-token exchange that fails deterministically when the
client isn't configured for the grant). A signed-in detail-page load fires
~5 authenticated requests, which was enough sustained traffic to hit Globus
rate limits and hold Lambda concurrency slots long enough to throttle.
"""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import auth as auth_module


class _FakeAuthAPIError(Exception):
    def __init__(self, code: str = "UNAUTHORIZED_CLIENT"):
        super().__init__(code)
        self.code = code


def _fake_globus_sdk(userinfo_calls: list, exchange_calls: list, exchange_code: str = "UNAUTHORIZED_CLIENT"):
    mod = types.ModuleType("globus_sdk")
    mod.AuthAPIError = _FakeAuthAPIError

    class AccessTokenAuthorizer:
        def __init__(self, token):
            self.token = token

    class AuthClient:
        def __init__(self, authorizer=None):
            self._authorizer = authorizer

        def userinfo(self):
            userinfo_calls.append(self._authorizer.token)
            return {
                "sub": "user-" + self._authorizer.token,
                "name": "Test User",
                "email": "test@example.com",
                "identity_set": [],
            }

    class ConfidentialAppAuthClient:
        def __init__(self, client_id, client_secret):
            pass

        def oauth2_get_dependent_tokens(self, token):
            exchange_calls.append(token)
            raise _FakeAuthAPIError(exchange_code)

    mod.AccessTokenAuthorizer = AccessTokenAuthorizer
    mod.AuthClient = AuthClient
    mod.ConfidentialAppAuthClient = ConfidentialAppAuthClient
    return mod


@pytest.fixture()
def prod_auth(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AUTH_MODE", "production")
    monkeypatch.delenv("LOCAL_DEV_AUTH", raising=False)
    monkeypatch.setenv("GLOBUS_CLIENT_ID", "test-client")
    monkeypatch.setenv("GLOBUS_CLIENT_SECRET", "test-secret")
    auth_module._auth_cache.clear()
    monkeypatch.setattr(auth_module, "_dependent_grant_unsupported", False)
    userinfo_calls: list = []
    exchange_calls: list = []
    monkeypatch.setitem(
        sys.modules, "globus_sdk", _fake_globus_sdk(userinfo_calls, exchange_calls)
    )
    yield userinfo_calls, exchange_calls
    auth_module._auth_cache.clear()


def _get_auth(token: str):
    return asyncio.run(
        auth_module.get_auth(
            request=None,
            x_user_id=None,
            x_user_email=None,
            x_user_name=None,
            authorization=f"Bearer {token}",
            x_mdf_token=None,
            x_groups_token=None,
        )
    )


class TestAuthTokenCache:
    def test_repeat_token_served_from_cache(self, prod_auth):
        userinfo_calls, _ = prod_auth
        ctx1 = _get_auth("tok-a")
        ctx2 = _get_auth("tok-a")
        assert ctx1.user_id == ctx2.user_id == "user-tok-a"
        assert len(userinfo_calls) == 1

    def test_distinct_tokens_validated_separately(self, prod_auth):
        userinfo_calls, _ = prod_auth
        _get_auth("tok-a")
        _get_auth("tok-b")
        assert len(userinfo_calls) == 2

    def test_zero_ttl_disables_cache(self, prod_auth, monkeypatch):
        userinfo_calls, _ = prod_auth
        monkeypatch.setattr(auth_module, "AUTH_CACHE_TTL_SECONDS", 0)
        _get_auth("tok-a")
        _get_auth("tok-a")
        assert len(userinfo_calls) == 2

    def test_expired_entry_revalidates(self, prod_auth, monkeypatch):
        userinfo_calls, _ = prod_auth
        _get_auth("tok-a")
        # Force the single entry to be expired.
        key = next(iter(auth_module._auth_cache))
        expires_at, ctx = auth_module._auth_cache[key]
        auth_module._auth_cache[key] = (expires_at - auth_module.AUTH_CACHE_TTL_SECONDS - 1e9, ctx)
        _get_auth("tok-a")
        assert len(userinfo_calls) == 2

    def test_cache_never_stores_raw_token(self, prod_auth):
        _get_auth("tok-secret-value")
        assert all("tok-secret-value" not in key for key in auth_module._auth_cache)


class TestDependentGrantNegativeCache:
    def test_unauthorized_client_disables_exchange_for_container(self, prod_auth):
        _, exchange_calls = prod_auth
        # Distinct tokens so the AuthContext cache can't mask the behavior.
        _get_auth("tok-a")
        _get_auth("tok-b")
        _get_auth("tok-c")
        assert len(exchange_calls) == 1
        assert auth_module._dependent_grant_unsupported is True

    def test_other_exchange_errors_keep_retrying(self, prod_auth, monkeypatch):
        userinfo_calls, exchange_calls = prod_auth
        monkeypatch.setitem(
            sys.modules,
            "globus_sdk",
            _fake_globus_sdk(userinfo_calls, exchange_calls, exchange_code="RATE_LIMITED"),
        )
        _get_auth("tok-a")
        _get_auth("tok-b")
        assert len(exchange_calls) == 2
        assert auth_module._dependent_grant_unsupported is False
