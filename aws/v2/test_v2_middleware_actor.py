"""The rate limiter must not key on caller-supplied identity in production.

``X-User-Id`` is only an identity under dev auth. In production it is an
arbitrary header, so keying the per-actor bucket on it would let any client
mint a fresh bucket per request and bypass the limiter entirely — including on
the paid semantic-search path.
"""

from __future__ import annotations

import pytest
from starlette.requests import Request

from v2.app import middleware


def _request(headers: dict, client_host: str = "203.0.113.9") -> Request:
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/search",
        "headers": raw_headers,
        "client": (client_host, 12345),
        "query_string": b"",
    }
    return Request(scope)


@pytest.fixture(autouse=True)
def _prod(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AUTH_MODE", "production")
    monkeypatch.delenv("LOCAL_DEV_AUTH", raising=False)
    monkeypatch.delenv("AWS_SAM_LOCAL", raising=False)


def test_spoofed_user_id_does_not_create_a_bucket_in_production():
    a = middleware._actor_key(_request({"x-user-id": "attacker-1"}))
    b = middleware._actor_key(_request({"x-user-id": "attacker-2"}))
    assert a == b == "ip:203.0.113.9"


def test_bearer_token_still_keys_authenticated_callers():
    key = middleware._actor_key(_request({"authorization": "Bearer abc", "x-user-id": "spoof"}))
    assert key.startswith("auth:")


def test_dev_mode_still_honors_the_header(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    assert middleware._actor_key(_request({"x-user-id": "dev-user"})) == "user:dev-user"


def test_rotating_bogus_bearer_tokens_hits_the_ip_ceiling(monkeypatch: pytest.MonkeyPatch):
    """Each new bearer hashes to a fresh actor bucket; the IP bucket still fills."""
    monkeypatch.setenv("RATE_LIMIT_IP_PER_MIN", "3")
    middleware.reset_middleware_state()
    window = middleware._rate_limit_window_seconds()
    for i in range(3):
        allowed, _ = middleware._check_rate_limit(f"ip:203.0.113.9:search", 3, window)
        assert allowed
        actor = middleware._actor_key(_request({"authorization": f"Bearer bogus-{i}"}))
        assert actor.startswith("auth:")
    allowed, retry_after = middleware._check_rate_limit("ip:203.0.113.9:search", 3, window)
    assert not allowed and retry_after >= 0
    middleware.reset_middleware_state()
