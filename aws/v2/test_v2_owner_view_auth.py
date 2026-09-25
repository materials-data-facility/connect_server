"""Owner/curator visibility of unpublished versions (GAP-3) and the negative
auth cache (SEC-M1)."""

import asyncio

import globus_sdk
import pytest
from fastapi import HTTPException

from v2.app import auth as auth_mod
from v2.app.auth import can_view_dataset
from v2.app.models import AuthContext

OWNER = AuthContext(user_id="owner-1")
OTHER = AuthContext(user_id="someone-else")


@pytest.fixture(autouse=True)
def _curators(monkeypatch):
    monkeypatch.setenv("CURATOR_USER_IDS", "curator-1")
    monkeypatch.delenv("ALLOW_ALL_CURATORS", raising=False)


@pytest.mark.parametrize("status", ["pending_curation", "approved", "rejected"])
def test_unpublished_versions_visible_to_owner_and_curator_only(status):
    record = {"source_id": "ds", "version": "1.1", "status": status, "user_id": "owner-1"}
    assert can_view_dataset(OWNER, record) is True
    assert can_view_dataset(AuthContext(user_id="curator-1"), record) is True
    assert can_view_dataset(OTHER, record) is False
    assert can_view_dataset(None, record) is False


@pytest.mark.parametrize("status", ["withdrawn", "deleted", "draft", None])
def test_other_statuses_stay_hidden_even_from_the_owner(status):
    record = {"source_id": "ds", "version": "1.0", "status": status, "user_id": "owner-1"}
    assert can_view_dataset(OWNER, record) is False


def test_published_rules_unchanged():
    public = {"status": "published", "user_id": "owner-1"}
    restricted = {"status": "published", "user_id": "owner-1",
                  "acl": ["urn:globus:auth:identity:x"]}
    assert can_view_dataset(None, public) is True
    assert can_view_dataset(OTHER, restricted) is False
    assert can_view_dataset(OWNER, restricted) is True


# -- negative auth cache ----------------------------------------------------


class _FakeAuthAPIError(Exception):
    def __init__(self, http_status):
        super().__init__(f"HTTP {http_status}")
        self.http_status = http_status


def _run_get_auth(token):
    return asyncio.run(auth_mod.get_auth(
        request=None, x_user_id=None, x_user_email=None, x_user_name=None,
        authorization=f"Bearer {token}", x_mdf_token=None, x_groups_token=None,
    ))


@pytest.fixture
def fake_globus(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "production")
    monkeypatch.setattr(auth_mod, "get_auth_mode", lambda: "production", raising=False)
    auth_mod._auth_negative_cache.clear()
    auth_mod._auth_cache.clear()
    calls = {"n": 0, "status": 401}

    class FakeAuthClient:
        def __init__(self, authorizer=None):
            pass

        def userinfo(self):
            calls["n"] += 1
            raise _FakeAuthAPIError(calls["status"])

    monkeypatch.setattr(globus_sdk, "AuthAPIError", _FakeAuthAPIError)
    monkeypatch.setattr(globus_sdk, "AuthClient", FakeAuthClient)
    yield calls
    auth_mod._auth_negative_cache.clear()


def test_rejected_token_is_not_revalidated_within_ttl(fake_globus):
    for _ in range(3):
        with pytest.raises(HTTPException) as exc:
            _run_get_auth("garbage-token")
        assert exc.value.status_code == 401
    assert fake_globus["n"] == 1


def test_globus_5xx_is_not_cached(fake_globus):
    fake_globus["status"] = 503
    for _ in range(2):
        with pytest.raises(HTTPException):
            _run_get_auth("maybe-valid-token")
    assert fake_globus["n"] == 2


def test_unexpected_errors_do_not_echo_exception_text(monkeypatch, fake_globus):
    class ExplodingAuthClient:
        def __init__(self, authorizer=None):
            raise RuntimeError("internal detail: secret-ish")

    monkeypatch.setattr(globus_sdk, "AuthClient", ExplodingAuthClient)
    with pytest.raises(HTTPException) as exc:
        _run_get_auth("another-token")
    assert exc.value.detail == "Authentication failed"


def test_throttling_and_timeouts_are_not_cached_and_not_401(fake_globus):
    for status in (429, 408, 502):
        fake_globus["status"] = status
        with pytest.raises(HTTPException) as exc:
            _run_get_auth(f"token-{status}")
        assert exc.value.status_code == 503
    assert auth_mod._auth_negative_cache == {}


def test_negative_cache_saturation_evicts_instead_of_flushing(monkeypatch):
    auth_mod._auth_negative_cache.clear()
    monkeypatch.setattr(auth_mod, "_AUTH_NEGATIVE_CACHE_MAX_ENTRIES", 8)
    for i in range(8):
        auth_mod._auth_remember_rejection(f"t{i}")
    auth_mod._auth_remember_rejection("t8")
    assert 0 < len(auth_mod._auth_negative_cache) <= 8
    assert auth_mod._auth_rejected_recently("t8")
    assert auth_mod._auth_rejected_recently("t7")
    auth_mod._auth_negative_cache.clear()


# -- ACL collaborators and linked identities ---------------------------------

COLLAB = "11111111-2222-3333-4444-555555555555"
GROUP = "99999999-8888-7777-6666-555555555555"


def test_acl_identity_and_group_members_can_view_restricted_published():
    record = {"status": "published", "user_id": "owner-1",
              "acl": [COLLAB, f"urn:globus:groups:id:{GROUP}"]}
    assert can_view_dataset(AuthContext(user_id=COLLAB), record) is True
    assert can_view_dataset(AuthContext(user_id="x", group_info={GROUP: {}}), record) is True
    assert can_view_dataset(AuthContext(user_id="x", identities=[{"sub": COLLAB}]), record) is True
    assert can_view_dataset(OTHER, record) is False
    assert can_view_dataset(None, record) is False


def test_acl_does_not_open_unpublished_versions_to_collaborators():
    record = {"status": "pending_curation", "user_id": "owner-1", "acl": [COLLAB]}
    assert can_view_dataset(AuthContext(user_id=COLLAB), record) is False


def test_linked_identity_counts_as_owner():
    record = {"status": "pending_curation", "user_id": "owner-1"}
    linked = AuthContext(user_id="other-login", identities=[{"sub": "owner-1"}])
    assert can_view_dataset(linked, record) is True


# -- endpoint behavior --------------------------------------------------------

from fastapi.testclient import TestClient  # noqa: E402

from v2.app import app  # noqa: E402


@pytest.fixture()
def api(tmp_path, monkeypatch):
    from v2.storage import reset_storage_backend
    from v2.app.middleware import reset_middleware_state

    db = tmp_path / "store.db"
    for key, value in {
        "STORE_BACKEND": "sqlite", "SQLITE_PATH": str(db), "ASYNC_SQLITE_PATH": str(db),
        "ASYNC_DISPATCH_MODE": "inline", "STORAGE_BACKEND": "local",
        "FILE_STORE_PATH": str(tmp_path / "files"), "AUTH_MODE": "dev",
        "LOCAL_DEV_AUTH": "true", "ALLOW_ALL_CURATORS": "false",
        "CURATOR_USER_IDS": "curator-1", "USE_MOCK_DATACITE": "true",
        "USE_MOCK_SEARCH": "true",
    }.items():
        monkeypatch.setenv(key, value)
    reset_storage_backend()
    reset_middleware_state()
    yield TestClient(app)
    reset_storage_backend()
    reset_middleware_state()


def _put(source_id, version, status, **extra):
    import json as _json

    from v2.store import get_store

    record = {
        "source_id": source_id, "version": version, "status": status,
        "user_id": "owner-1", "acl": ["public"],
        "dataset_mdata": _json.dumps({"title": f"T {version}", "authors": [{"name": "A B"}],
                                      "publication_year": 2026}),
        "dataset_profile": _json.dumps({"files": [], "version": version}),
        "created_at": f"2026-01-0{version[-1]}T00:00:00Z",
        "updated_at": f"2026-01-0{version[-1]}T00:00:00Z",
        **extra,
    }
    get_store().put_submission(record)


ANON_OTHER = {"X-User-Id": "someone-else"}
OWNER_H = {"X-User-Id": "owner-1"}


def test_preview_serves_published_version_while_update_is_pending(api):
    _put("ds", "1.0", "published")
    _put("ds", "1.1", "pending_curation")
    resp = api.get("/preview/ds", headers=ANON_OTHER)
    assert resp.status_code == 200
    assert resp.json()["profile"]["version"] == "1.0"


def test_stats_counts_only_published_versions(api):
    _put("ds", "1.0", "published", view_count=3)
    _put("ds", "1.1", "pending_curation", view_count=5)
    body = api.get("/stats/ds", headers=OWNER_H).json()
    assert body["version_count"] == 1
    assert body["view_count"] == 3
    _put("never", "1.0", "pending_curation")
    assert api.get("/stats/never", headers=OWNER_H).status_code == 404


def test_legacy_id_with_explicit_version_resolves_on_canonical(api):
    _put("canon", "1.0", "published", legacy_source_id="canon_v1.0")
    _put("canon", "2.0", "published")
    body = api.get("/card/canon_v1.0", params={"version": "2.0"}, headers=ANON_OTHER).json()
    assert body["card"]["version"] == "2.0"
    body = api.get("/card/canon_v1.0", headers=ANON_OTHER).json()
    assert body["card"]["version"] == "1.0"


def test_citation_accepts_version_latest(api):
    _put("ds", "1.0", "published")
    assert api.get("/citation/ds", params={"version": "latest"}).status_code == 200
