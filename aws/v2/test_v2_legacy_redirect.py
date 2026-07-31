"""Tests for legacy (v1) source_id resolution on the public dataset endpoints.

Datasets migrated from the legacy MDF index keep a top-level ``legacy_source_id``
(the original versioned v1 id) while their canonical v2 ``source_id`` is the
version-independent name. Old links/DOIs that reference a v1 id must keep
resolving via /card, /citation, and /detail after the source_name→source_id
promotion. Also covers the store's get_by_legacy_source_id (SQLite).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.storage import reset_storage_backend
from v2.store import get_store

HEADERS = {"X-User-Id": "test-user"}

VALID_SUBMISSION = {
    "title": "Perovskite Stability Dataset",
    "authors": [{"name": "Jane Researcher"}],
    "description": "Migrated dataset for legacy-id resolution.",
    "data_sources": ["https://example.com/data.csv"],
}

LEGACY_ID = "perovskite_stability_v2"


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(tmp_path / "files"))
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


def _publish_with_legacy_id(client: TestClient) -> str:
    """Submit + publish a dataset, then stamp a legacy_source_id on it.

    Returns the canonical (v2) source_id.
    """
    resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
    assert resp.status_code == 200, resp.text
    source_id = resp.json()["source_id"]

    pub = client.post(
        "/status/update",
        headers=HEADERS,
        json={"source_id": source_id, "version": "1.0", "status": "published"},
    )
    assert pub.status_code == 200, pub.text

    # Stamp the pre-migration id, as the ingest script does for migrated records.
    store = get_store()
    record = store.get_submission(source_id, "1.0")
    assert record is not None
    record["legacy_source_id"] = LEGACY_ID
    store.upsert_submission(record)

    return source_id


def test_store_resolves_by_legacy_source_id(env):
    client = TestClient(app)
    source_id = _publish_with_legacy_id(client)

    store = get_store()
    found = store.get_by_legacy_source_id(LEGACY_ID)
    assert found is not None
    assert found["source_id"] == source_id
    # Unknown legacy id resolves to nothing.
    assert store.get_by_legacy_source_id("does-not-exist") is None
    assert store.get_by_legacy_source_id("") is None


def test_card_resolves_canonical_and_legacy(env):
    client = TestClient(app)
    source_id = _publish_with_legacy_id(client)

    # Canonical id: resolves, no redirect hint.
    direct = client.get(f"/card/{source_id}")
    assert direct.status_code == 200, direct.text
    assert "redirected_from" not in direct.json()

    # Legacy id: resolves to the same record, with redirect hints.
    legacy = client.get(f"/card/{LEGACY_ID}")
    assert legacy.status_code == 200, legacy.text
    body = legacy.json()
    assert body["canonical_source_id"] == source_id
    assert body["redirected_from"] == LEGACY_ID


def test_detail_and_citation_resolve_legacy(env):
    client = TestClient(app)
    source_id = _publish_with_legacy_id(client)

    detail = client.get(f"/detail/{LEGACY_ID}")
    assert detail.status_code == 200, detail.text
    assert detail.json()["source_id"] == source_id
    assert detail.json()["redirected_from"] == LEGACY_ID

    citation = client.get(f"/citation/{LEGACY_ID}")
    assert citation.status_code == 200, citation.text
    assert citation.json()["source_id"] == source_id
    assert citation.json()["redirected_from"] == LEGACY_ID


def test_unknown_id_still_404(env):
    client = TestClient(app)
    _publish_with_legacy_id(client)

    assert client.get("/card/totally-unknown-id").status_code == 404
    assert client.get("/detail/totally-unknown-id").status_code == 404
