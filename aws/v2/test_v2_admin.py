"""Tests for the admin sync visibility + trigger endpoints.

Covers:
- GET /admin/stats -> "sync" block (report + lock present, absent, stale lock,
  SSM unreachable, malformed JSON)
- GET /admin/sync  -> the same block on a cheap standalone route
- POST /admin/sync -> auth gating (401/403), 501 unconfigured, 409 while a run
  holds the lock, 202 on a successful workflow_dispatch, 502 on GitHub errors

Nothing here touches the network or AWS: the SSM accessor and the GitHub
dispatch helper are both monkeypatched at the module level.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.app.routers import admin as admin_router
from v2.storage import reset_storage_backend


CURATOR_HEADERS = {"X-User-Id": "curator-user"}


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    monkeypatch.setenv("ENVIRONMENT", "staging")
    # Start every test from "trigger not configured"; tests opt in explicitly.
    monkeypatch.delenv("GITHUB_SYNC_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_SYNC_REPO", raising=False)
    monkeypatch.delenv("GITHUB_SYNC_WORKFLOW", raising=False)
    monkeypatch.delenv("GITHUB_SYNC_REF", raising=False)
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _now_minus(**kwargs) -> str:
    return _iso(datetime.now(timezone.utc) - timedelta(**kwargs))


REPORT = {
    "status": "success",
    "started_at": "2026-07-30T01:00:00Z",
    "finished_at": "2026-07-30T01:12:00Z",
    "counts": {
        "created": 12,
        "updated": 3,
        "unchanged": 400,
        "conflicts": 0,
        "search_pending": 2,
        "store_errors": 0,
        "search_errors": 0,
    },
    "watermark": "2026-07-30T01:11:59Z",
    "run_source": "github-actions",
}


def _fake_ssm(monkeypatch: pytest.MonkeyPatch, params: dict, seen: list | None = None):
    """Monkeypatch the SSM accessor with an in-memory parameter table.

    `params` maps parameter name -> raw string value; anything absent behaves
    like ParameterNotFound (returns None).
    """

    def fake_get(name: str):
        if seen is not None:
            seen.append(name)
        return params.get(name)

    monkeypatch.setattr(admin_router, "_get_ssm_param", fake_get)


def _fake_ssm_unavailable(monkeypatch: pytest.MonkeyPatch):
    def boom(name: str):
        raise admin_router.SsmUnavailable("NoCredentialsError: unable to locate credentials")

    monkeypatch.setattr(admin_router, "_get_ssm_param", boom)


def _fake_dispatch(monkeypatch: pytest.MonkeyPatch, result, calls: list | None = None):
    def fake(repo, workflow, ref, token, inputs):
        if calls is not None:
            calls.append(
                {"repo": repo, "workflow": workflow, "ref": ref, "token": token, "inputs": inputs}
            )
        return result

    monkeypatch.setattr(admin_router, "_dispatch_github_workflow", fake)


def _configure_trigger(monkeypatch: pytest.MonkeyPatch, **overrides):
    monkeypatch.setenv("GITHUB_SYNC_TOKEN", overrides.get("token", "ghp_test"))
    monkeypatch.setenv("GITHUB_SYNC_REPO", overrides.get("repo", "materials-data-facility/connect_server"))
    if "workflow" in overrides:
        monkeypatch.setenv("GITHUB_SYNC_WORKFLOW", overrides["workflow"])
    if "ref" in overrides:
        monkeypatch.setenv("GITHUB_SYNC_REF", overrides["ref"])


# ---------------------------------------------------------------------------
# GET /admin/stats — sync block
# ---------------------------------------------------------------------------

class TestStatsSyncBlock:
    def test_report_and_lock_present(self, env, monkeypatch):
        seen: list[str] = []
        _fake_ssm(
            monkeypatch,
            {
                "/mdf/staging/sync-last-report": json.dumps(REPORT),
                "/mdf/staging/sync-lock": json.dumps(
                    {"acquired_at": _now_minus(minutes=5), "run_source": "github-actions"}
                ),
            },
            seen=seen,
        )
        client = TestClient(app)
        resp = client.get("/admin/stats", headers=CURATOR_HEADERS)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        sync = body["sync"]

        # Parameter names are environment-scoped.
        assert seen == ["/mdf/staging/sync-last-report", "/mdf/staging/sync-lock"]
        assert sync["available"] is True
        assert sync["environment"] == "staging"
        assert sync["last_report"] == REPORT
        assert sync["last_report"]["counts"]["created"] == 12
        assert sync["running"] is True
        assert sync["stale_lock"] is False
        assert sync["lock"]["run_source"] == "github-actions"
        assert "acquired_at" in sync["lock"]
        # The pre-existing stats payload is untouched.
        assert body["success"] is True
        assert "by_status" in body

    def test_report_only_no_lock(self, env, monkeypatch):
        _fake_ssm(monkeypatch, {"/mdf/staging/sync-last-report": json.dumps(REPORT)})
        client = TestClient(app)
        sync = client.get("/admin/stats", headers=CURATOR_HEADERS).json()["sync"]
        assert sync["available"] is True
        assert sync["last_report"]["status"] == "success"
        assert sync["running"] is False
        assert sync["lock"] is None
        assert sync["stale_lock"] is False

    def test_both_params_absent(self, env, monkeypatch):
        _fake_ssm(monkeypatch, {})
        client = TestClient(app)
        resp = client.get("/admin/stats", headers=CURATOR_HEADERS)
        assert resp.status_code == 200
        sync = resp.json()["sync"]
        assert sync["available"] is False
        assert sync["last_report"] is None
        assert sync["lock"] is None
        assert sync["running"] is False
        assert sync["stale_lock"] is False

    def test_stale_lock(self, env, monkeypatch):
        _fake_ssm(
            monkeypatch,
            {
                "/mdf/staging/sync-last-report": json.dumps(REPORT),
                "/mdf/staging/sync-lock": json.dumps(
                    {"acquired_at": _now_minus(hours=5), "run_source": "manual"}
                ),
            },
        )
        client = TestClient(app)
        sync = client.get("/admin/stats", headers=CURATOR_HEADERS).json()["sync"]
        assert sync["available"] is True
        assert sync["stale_lock"] is True
        assert sync["running"] is False
        assert sync["lock"]["run_source"] == "manual"

    def test_lock_just_inside_stale_window_is_live(self, env, monkeypatch):
        _fake_ssm(
            monkeypatch,
            {
                "/mdf/staging/sync-lock": json.dumps(
                    {"acquired_at": _now_minus(hours=1, minutes=55), "run_source": "github-actions"}
                )
            },
        )
        client = TestClient(app)
        sync = client.get("/admin/stats", headers=CURATOR_HEADERS).json()["sync"]
        assert sync["running"] is True
        assert sync["stale_lock"] is False

    def test_lock_without_timestamp_counts_as_running(self, env, monkeypatch):
        _fake_ssm(monkeypatch, {"/mdf/staging/sync-lock": json.dumps({"run_source": "unknown"})})
        client = TestClient(app)
        sync = client.get("/admin/stats", headers=CURATOR_HEADERS).json()["sync"]
        assert sync["running"] is True
        assert sync["stale_lock"] is False

    def test_ssm_unavailable_degrades_gracefully(self, env, monkeypatch):
        _fake_ssm_unavailable(monkeypatch)
        client = TestClient(app)
        resp = client.get("/admin/stats", headers=CURATOR_HEADERS)
        assert resp.status_code == 200, resp.text
        sync = resp.json()["sync"]
        assert sync["available"] is False
        assert sync["last_report"] is None
        assert sync["lock"] is None
        assert sync["running"] is False
        assert "NoCredentialsError" in sync["error"]

    def test_malformed_report_json_does_not_500(self, env, monkeypatch):
        _fake_ssm(
            monkeypatch,
            {
                "/mdf/staging/sync-last-report": "not json at all",
                "/mdf/staging/sync-lock": "[]",
            },
        )
        client = TestClient(app)
        resp = client.get("/admin/stats", headers=CURATOR_HEADERS)
        assert resp.status_code == 200
        sync = resp.json()["sync"]
        assert sync["last_report"] is None
        assert sync["lock"] is None
        assert sync["available"] is False

    def test_environment_defaults_to_dev(self, env, monkeypatch):
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        seen: list[str] = []
        _fake_ssm(monkeypatch, {}, seen=seen)
        client = TestClient(app)
        sync = client.get("/admin/stats", headers=CURATOR_HEADERS).json()["sync"]
        assert sync["environment"] == "dev"
        assert seen == ["/mdf/dev/sync-last-report", "/mdf/dev/sync-lock"]


class TestSyncStatusRoute:
    def test_get_sync_returns_same_block(self, env, monkeypatch):
        _fake_ssm(monkeypatch, {"/mdf/staging/sync-last-report": json.dumps(REPORT)})
        client = TestClient(app)
        resp = client.get("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["sync"]["last_report"] == REPORT

    def test_get_sync_requires_curator(self, env, monkeypatch):
        monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
        _fake_ssm(monkeypatch, {})
        client = TestClient(app)
        assert client.get("/admin/sync", headers={"X-User-Id": "nobody"}).status_code == 403


# ---------------------------------------------------------------------------
# POST /admin/sync — auth gating
# ---------------------------------------------------------------------------

class TestTriggerAuth:
    def test_non_curator_gets_403(self, env, monkeypatch):
        monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
        monkeypatch.setenv("CURATOR_USER_IDS", "")
        _configure_trigger(monkeypatch)
        calls: list[dict] = []
        _fake_ssm(monkeypatch, {})
        _fake_dispatch(monkeypatch, (204, ""), calls)

        client = TestClient(app)
        resp = client.post("/admin/sync", headers={"X-User-Id": "random-user"})
        assert resp.status_code == 403, resp.text
        assert "curator" in resp.json()["detail"].lower()
        assert calls == []

    def test_unauthenticated_gets_401(self, env, monkeypatch):
        # Production auth mode: no Authorization header -> 401 before any work.
        monkeypatch.setenv("AUTH_MODE", "production")
        monkeypatch.delenv("LOCAL_DEV_AUTH", raising=False)
        monkeypatch.delenv("AWS_SAM_LOCAL", raising=False)
        _configure_trigger(monkeypatch)
        calls: list[dict] = []
        _fake_ssm(monkeypatch, {})
        _fake_dispatch(monkeypatch, (204, ""), calls)

        client = TestClient(app)
        resp = client.post("/admin/sync")
        assert resp.status_code == 401, resp.text
        assert calls == []

    def test_curator_user_id_allowlist_is_honored(self, env, monkeypatch):
        monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
        monkeypatch.setenv("CURATOR_USER_IDS", "curator-user")
        _configure_trigger(monkeypatch)
        _fake_ssm(monkeypatch, {})
        _fake_dispatch(monkeypatch, (204, ""))

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 202, resp.text


# ---------------------------------------------------------------------------
# POST /admin/sync — behavior
# ---------------------------------------------------------------------------

class TestTriggerSync:
    def test_501_when_unconfigured(self, env, monkeypatch):
        _fake_ssm(monkeypatch, {})
        calls: list[dict] = []
        _fake_dispatch(monkeypatch, (204, ""), calls)

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 501, resp.text
        body = resp.json()
        assert body["detail"] == "sync trigger not configured"
        assert "GITHUB_SYNC_TOKEN" in body["hint"]
        assert calls == []

    def test_501_when_repo_missing(self, env, monkeypatch):
        monkeypatch.setenv("GITHUB_SYNC_TOKEN", "ghp_test")
        _fake_ssm(monkeypatch, {})
        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 501
        assert resp.json()["detail"] == "sync trigger not configured"

    def test_409_when_locked(self, env, monkeypatch):
        _configure_trigger(monkeypatch)
        acquired = _now_minus(minutes=3)
        _fake_ssm(
            monkeypatch,
            {
                "/mdf/staging/sync-lock": json.dumps(
                    {"acquired_at": acquired, "run_source": "github-actions"}
                )
            },
        )
        calls: list[dict] = []
        _fake_dispatch(monkeypatch, (204, ""), calls)

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 409, resp.text
        body = resp.json()
        assert body["detail"] == "sync already running"
        assert body["lock"]["acquired_at"] == acquired
        assert body["lock"]["run_source"] == "github-actions"
        assert body["stale_lock"] is False
        assert calls == [], "must not dispatch while a run holds the lock"

    def test_stale_lock_does_not_block(self, env, monkeypatch):
        _configure_trigger(monkeypatch)
        _fake_ssm(
            monkeypatch,
            {
                "/mdf/staging/sync-lock": json.dumps(
                    {"acquired_at": _now_minus(hours=3), "run_source": "github-actions"}
                )
            },
        )
        calls: list[dict] = []
        _fake_dispatch(monkeypatch, (204, ""), calls)

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 202, resp.text
        assert len(calls) == 1

    def test_202_on_success(self, env, monkeypatch):
        _configure_trigger(monkeypatch)
        _fake_ssm(monkeypatch, {"/mdf/staging/sync-last-report": json.dumps(REPORT)})
        calls: list[dict] = []
        _fake_dispatch(monkeypatch, (204, ""), calls)

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 202, resp.text
        body = resp.json()
        assert body["triggered"] is True
        assert body["workflow"] == "sync-v1-to-v2.yml"
        assert body["environment"] == "staging"
        assert body["repo"] == "materials-data-facility/connect_server"
        assert body["ref"] == "main"

        assert len(calls) == 1
        call = calls[0]
        assert call["repo"] == "materials-data-facility/connect_server"
        assert call["workflow"] == "sync-v1-to-v2.yml"
        assert call["ref"] == "main"
        assert call["token"] == "ghp_test"
        assert call["inputs"] == {"environment": "staging"}

    def test_workflow_and_ref_overrides(self, env, monkeypatch):
        _configure_trigger(monkeypatch, workflow="other-sync.yml", ref="release-x")
        _fake_ssm(monkeypatch, {})
        calls: list[dict] = []
        _fake_dispatch(monkeypatch, (204, ""), calls)

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 202, resp.text
        assert calls[0]["workflow"] == "other-sync.yml"
        assert calls[0]["ref"] == "release-x"
        assert resp.json()["workflow"] == "other-sync.yml"

    def test_502_on_github_error(self, env, monkeypatch):
        _configure_trigger(monkeypatch)
        _fake_ssm(monkeypatch, {})
        _fake_dispatch(monkeypatch, (404, '{"message": "Not Found"}'))

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 502, resp.text
        body = resp.json()
        assert body["detail"] == "failed to trigger sync workflow"
        assert body["upstream_status"] == 404
        assert "Not Found" in body["upstream_body"]

    def test_502_on_network_failure(self, env, monkeypatch):
        _configure_trigger(monkeypatch)
        _fake_ssm(monkeypatch, {})
        _fake_dispatch(monkeypatch, (None, "ConnectTimeout: timed out"))

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 502
        body = resp.json()
        assert body["upstream_status"] is None
        assert "ConnectTimeout" in body["upstream_body"]

    def test_ssm_unavailable_still_allows_trigger(self, env, monkeypatch):
        """No lock visibility must not become a hard block on triggering."""
        _configure_trigger(monkeypatch)
        _fake_ssm_unavailable(monkeypatch)
        calls: list[dict] = []
        _fake_dispatch(monkeypatch, (204, ""), calls)

        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 202, resp.text
        assert len(calls) == 1

    def test_placeholder_token_treated_as_unconfigured(self, env, monkeypatch):
        """The SAM default for unset secrets is a placeholder, not a real token."""
        monkeypatch.setenv("GITHUB_SYNC_TOKEN", "not-configured")
        monkeypatch.setenv("GITHUB_SYNC_REPO", "materials-data-facility/connect_server")
        _fake_ssm(monkeypatch, {})
        client = TestClient(app)
        resp = client.post("/admin/sync", headers=CURATOR_HEADERS)
        assert resp.status_code == 501


# ---------------------------------------------------------------------------
# Unit-level checks on the SSM accessor itself
# ---------------------------------------------------------------------------

class TestSsmAccessor:
    def test_parameter_not_found_returns_none(self, env, monkeypatch):
        class ParameterNotFound(Exception):
            pass

        class FakeClient:
            def get_parameter(self, Name):  # noqa: N803 - boto3 kwarg casing
                raise ParameterNotFound(Name)

        fake_boto3 = type("boto3", (), {"client": staticmethod(lambda *a, **k: FakeClient())})
        monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
        assert admin_router._get_ssm_param("/mdf/staging/sync-lock") is None

    def test_other_client_error_raises_ssm_unavailable(self, env, monkeypatch):
        class AccessDeniedException(Exception):
            pass

        class FakeClient:
            def get_parameter(self, Name):  # noqa: N803
                raise AccessDeniedException("denied")

        fake_boto3 = type("boto3", (), {"client": staticmethod(lambda *a, **k: FakeClient())})
        monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
        with pytest.raises(admin_router.SsmUnavailable):
            admin_router._get_ssm_param("/mdf/staging/sync-lock")

    def test_returns_value_and_uses_region(self, env, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "us-west-2")
        seen: dict = {}

        class FakeClient:
            def get_parameter(self, Name):  # noqa: N803
                seen["name"] = Name
                return {"Parameter": {"Value": "{}"}}

        def fake_client(service, region_name=None, **kwargs):
            seen["service"] = service
            seen["region"] = region_name
            return FakeClient()

        fake_boto3 = type("boto3", (), {"client": staticmethod(fake_client)})
        monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

        assert admin_router._get_ssm_param("/mdf/staging/sync-lock") == "{}"
        assert seen == {
            "service": "ssm",
            "region": "us-west-2",
            "name": "/mdf/staging/sync-lock",
        }
