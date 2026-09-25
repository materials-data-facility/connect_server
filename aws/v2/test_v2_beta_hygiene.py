"""Bounded regressions for MDF v2 beta privacy and view tracking."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.storage import reset_storage_backend
from v2.store import get_store


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


def _seed_record(
    source_id: str,
    version: str = "1.0",
    *,
    acl: Optional[List[str]] = None,
    extra: Optional[Dict] = None,
) -> None:
    metadata = {
        "title": "Beta hygiene dataset",
        "description": "A record used by a focused route regression test.",
        "authors": [{"name": "Test Author"}],
        "acl": acl or ["public"],
    }
    get_store().upsert_submission({
        "source_id": source_id,
        "version": version,
        "status": "published",
        "user_id": "record-owner",
        "dataset_mdata": metadata,
        "dataset_profile": {"files": []},
        **(extra or {}),
    })


@pytest.mark.parametrize(
    "path",
    [
        "/card/beta-hygiene?track=false",
        "/detail/beta-hygiene?track=false",
        "/preview/beta-hygiene?track=false",
    ],
)
def test_track_false_does_not_increment_view_count(env, path):
    _seed_record("beta-hygiene")

    response = TestClient(app).get(path)

    assert response.status_code == 200, response.text
    assert get_store().get_submission("beta-hygiene", "1.0").get("view_count", 0) == 0


def test_anonymous_status_omits_embedding_fields(env, monkeypatch):
    embedding_fields = {
        "title_description_embedding": [0.1, 0.2],
        "embedding_model": "test-embedding-model",
        "embedding_generated_at": "2026-09-25T00:00:00Z",
    }
    _seed_record("embedding-beta", extra=embedding_fields)
    monkeypatch.setenv("AUTH_MODE", "production")

    response = TestClient(app).get("/status", params={"source_id": "embedding-beta"})

    assert response.status_code == 200, response.text
    public_record = response.json()["submission"]
    assert not (embedding_fields.keys() & public_record.keys())


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"from": "1.0"},
        {"to": "1.0"},
        {"from": "", "to": "1.0"},
        {"from": "1.0", "to": ""},
    ],
)
def test_version_diff_requires_both_nonempty_versions(env, params):
    response = TestClient(app).get("/versions/diff-beta/diff", params=params)

    assert response.status_code == 400
    assert response.json()["detail"] == "Both 'from' and 'to' versions are required"


def test_version_diff_hides_existing_inaccessible_versions_like_missing_ones(
    env, monkeypatch,
):
    _seed_record("private-beta", acl=["urn:globus:auth:identity:private"])
    _seed_record("mixed-beta", "1.0")
    _seed_record("mixed-beta", "2.0", acl=["urn:globus:auth:identity:private"])
    _seed_record("public-v1-beta", "1.0")
    monkeypatch.setenv("AUTH_MODE", "production")
    client = TestClient(app)

    hidden_from = client.get(
        "/versions/private-beta/diff", params={"from": "1.0", "to": "1.0"}
    )
    missing_from = client.get(
        "/versions/no-private-beta/diff", params={"from": "1.0", "to": "1.0"}
    )
    hidden_to = client.get(
        "/versions/mixed-beta/diff", params={"from": "1.0", "to": "2.0"}
    )
    missing_to = client.get(
        "/versions/public-v1-beta/diff", params={"from": "1.0", "to": "2.0"}
    )

    assert hidden_from.status_code == missing_from.status_code == 404
    assert hidden_from.json()["detail"] == missing_from.json()["detail"]
    assert hidden_to.status_code == missing_to.status_code == 404
    assert hidden_to.json()["detail"] == missing_to.json()["detail"]


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("get", "/detail/..evil", None),
        ("get", "/status?source_id=..evil", None),
        (
            "post",
            "/status/update",
            {"source_id": "..evil", "version": "1.0", "status": "rejected"},
        ),
    ],
)
def test_malformed_source_ids_are_not_found_on_uncovered_routes(env, method, path, body):
    request = getattr(TestClient(app), method)
    response = (
        request(path, headers={"X-User-Id": "curator-user"}, json=body)
        if body is not None
        else request(path)
    )

    assert response.status_code == 404
