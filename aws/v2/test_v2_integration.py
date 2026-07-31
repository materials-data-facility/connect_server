from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.storage import reset_storage_backend
from v2.stream_store import SqliteStreamStore
from v2.store import SqliteSubmissionStore


@pytest.fixture()
def local_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("MAX_REQUEST_BYTES", str(1024 * 1024))
    monkeypatch.setenv("MAX_SUBMIT_METADATA_BYTES", str(256 * 1024))
    monkeypatch.setenv("RATE_LIMIT_DEFAULT_PER_MIN", "200")
    monkeypatch.setenv("RATE_LIMIT_SUBMIT_PER_MIN", "100")
    monkeypatch.setenv("RATE_LIMIT_WINDOW_SECONDS", "60")
    reset_storage_backend()
    reset_middleware_state()
    yield db_path
    reset_storage_backend()
    reset_middleware_state()


def test_happy_path_submit_to_curation_to_card(local_env: Path):
    client = TestClient(app)
    headers = {"X-User-Id": "owner-user"}

    submit = client.post(
        "/submit",
        headers=headers,
        json={
            "title": "Integration Dataset",
            "authors": [{"name": "Jane Scientist"}],
            "description": "integration test dataset",
            "data_sources": ["https://example.com/data.csv"],
        },
    )
    assert submit.status_code == 200
    source_id = submit.json()["source_id"]

    pending = client.post(
        "/status/update",
        headers=headers,
        json={"source_id": source_id, "version": "1.0", "status": "pending_curation"},
    )
    assert pending.status_code == 200

    queue = client.get("/curation/pending", headers=headers)
    assert queue.status_code == 200
    queued = {(x["source_id"], x["version"]) for x in queue.json().get("submissions", [])}
    assert (source_id, "1.0") in queued

    approve = client.post(
        f"/curation/{source_id}/approve",
        headers=headers,
        json={"notes": "looks good", "mint_doi": True},
    )
    assert approve.status_code == 200
    approve_body = approve.json()
    assert approve_body["status"] == "published"
    assert approve_body["doi"]["success"] is True

    card = client.get(f"/card/{source_id}")
    assert card.status_code == 200
    card_body = card.json()["card"]
    assert card_body["source_id"] == source_id
    assert card_body["status"] == "published"
    assert card_body.get("doi")


def test_throttling_and_request_size_limits(local_env: Path, monkeypatch: pytest.MonkeyPatch):
    client = TestClient(app)
    headers = {"X-User-Id": "limited-user"}

    monkeypatch.setenv("RATE_LIMIT_SUBMIT_PER_MIN", "2")
    monkeypatch.setenv("RATE_LIMIT_WINDOW_SECONDS", "60")
    reset_middleware_state()
    payload = {
        "title": "RL dataset",
        "authors": [{"name": "A"}],
        "data_sources": ["https://example.com/a.csv"],
    }
    assert client.post("/submit", headers=headers, json=payload).status_code == 200
    assert client.post("/submit", headers=headers, json=payload).status_code == 200
    limited = client.post("/submit", headers=headers, json=payload)
    assert limited.status_code == 429
    assert limited.json()["error"] == "Rate limit exceeded"

    monkeypatch.setenv("MAX_REQUEST_BYTES", "300")
    reset_middleware_state()
    oversized_payload = {
        "title": "X" * 500,
        "authors": [{"name": "A"}],
        "data_sources": ["https://example.com/a.csv"],
    }
    too_large = client.post("/submit", headers=headers, json=oversized_payload)
    assert too_large.status_code == 413


def test_submissions_pagination(local_env: Path):
    client = TestClient(app)
    headers = {"X-User-Id": "pager-user"}
    store = SqliteSubmissionStore(path=str(local_env))
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    for i in range(55):
        ts = (base_time + timedelta(minutes=i)).isoformat().replace("+00:00", "Z")
        source_id = f"src-{i:03d}"
        store.put_submission(
            {
                "source_id": source_id,
                "version": "1.0",
                "versioned_source_id": f"{source_id}-1.0",
                "user_id": "pager-user",
                "user_email": "pager@example.com",
                "organization": "org",
                "status": "submitted",
                "dataset_mdata": json.dumps(
                    {"title": f"Dataset {i}", "authors": [{"name": "A"}], "data_sources": ["https://example.com/a.csv"]}
                ),
                "test": 0,
                "created_at": ts,
                "updated_at": ts,
            }
        )

    p1 = client.get("/submissions", headers=headers, params={"limit": 20})
    assert p1.status_code == 200
    b1 = p1.json()
    assert len(b1["submissions"]) == 20
    assert b1["next_key"]

    p2 = client.get("/submissions", headers=headers, params={"limit": 20, "start_key": b1["next_key"]})
    assert p2.status_code == 200
    b2 = p2.json()
    assert len(b2["submissions"]) == 20
    assert b2["next_key"]

    p3 = client.get("/submissions", headers=headers, params={"limit": 20, "start_key": b2["next_key"]})
    assert p3.status_code == 200
    b3 = p3.json()
    assert len(b3["submissions"]) == 15
    assert b3["next_key"] is None


@pytest.mark.skip(reason="streams router disabled pending B-1 decision")
def test_path_validation_edge_cases(local_env: Path):
    client = TestClient(app)
    headers = {"X-User-Id": "owner-user"}

    created = client.post("/stream/create", headers=headers, json={"title": "Edge Stream"})
    assert created.status_code == 200
    stream_id = created.json()["stream_id"]

    content_b64 = base64.b64encode(b"col1,col2\n1,2\n").decode("ascii")
    uploaded = client.post(
        f"/stream/{stream_id}/upload",
        headers=headers,
        json={"filename": "edge.csv", "content_base64": content_b64, "content_type": "text/csv"},
    )
    assert uploaded.status_code == 200
    path = uploaded.json()["files"][0]["path"]

    valid_download = client.post(
        f"/stream/{stream_id}/download-url",
        headers=headers,
        json={"path": path},
    )
    assert valid_download.status_code == 200

    invalid_download = client.post(
        f"/stream/{stream_id}/download-url",
        headers=headers,
        json={"path": "streams/other-stream/2026-02-06/edge.csv"},
    )
    assert invalid_download.status_code == 400


def test_curation_reject_transition_rules(local_env: Path):
    """Submissions land as pending_curation; reject works once, then fails on double-reject."""
    client = TestClient(app)
    headers = {"X-User-Id": "curator-user"}

    submit = client.post(
        "/submit",
        headers=headers,
        json={
            "title": "Reject Transition Dataset",
            "authors": [{"name": "Reviewer"}],
            "data_sources": ["https://example.com/data.csv"],
        },
    )
    assert submit.status_code == 200
    source_id = submit.json()["source_id"]

    # Submission is already pending_curation — reject should succeed immediately
    reject = client.post(
        f"/curation/{source_id}/reject",
        headers=headers,
        json={"reason": "missing metadata"},
    )
    assert reject.status_code == 200
    assert reject.json()["status"] == "rejected"

    # Rejecting again should fail — it's no longer pending_curation
    reject_again = client.post(
        f"/curation/{source_id}/reject",
        headers=headers,
        json={"reason": "double reject"},
    )
    assert reject_again.status_code == 400


def test_search_limit_is_capped(local_env: Path):
    client = TestClient(app)
    store = SqliteSubmissionStore(path=str(local_env))
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    for i in range(60):
        ts = (base_time + timedelta(minutes=i)).isoformat().replace("+00:00", "Z")
        source_id = f"search-cap-{i:03d}"
        store.put_submission(
            {
                "source_id": source_id,
                "version": "1.0",
                "versioned_source_id": f"{source_id}-1.0",
                "user_id": "search-user",
                "user_email": "search@example.com",
                "organization": "org",
                "status": "published",
                "dataset_mdata": json.dumps(
                    {
                        "title": f"Search Cap Dataset {i}",
                        "authors": [{"name": "Cap Tester"}],
                        "description": "Used to test search limit clamping",
                        "data_sources": ["https://example.com/a.csv"],
                    }
                ),
                "test": 0,
                "created_at": ts,
                "updated_at": ts,
            }
        )

    resp = client.get("/search", params={"q": "Search Cap Dataset", "type": "datasets", "limit": 500})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["results"]) == 50


def test_search_fallback_excludes_unpublished_datasets(
    local_env: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    client = TestClient(app)
    store = SqliteSubmissionStore(path=str(local_env))

    def _search_unavailable():
        raise RuntimeError("search temporarily unavailable")

    monkeypatch.setattr("v2.search_client.get_search_client", _search_unavailable)

    for source_id, status in (
        ("search-published", "published"),
        ("search-pending", "pending_curation"),
    ):
        store.put_submission(
            {
                "source_id": source_id,
                "version": "1.0",
                "versioned_source_id": f"{source_id}-1.0",
                "user_id": "search-user",
                "user_email": "search@example.com",
                "organization": "org",
                "status": status,
                "dataset_mdata": json.dumps(
                    {
                        "title": "Fallback Visibility Dataset",
                        "authors": [{"name": "Visibility Tester"}],
                        "description": "Used to verify fallback search filtering",
                        "data_sources": ["https://example.com/a.csv"],
                    }
                ),
                "test": 0,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        )

    resp = client.get(
        "/search",
        params={"q": "Fallback Visibility Dataset", "type": "datasets"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert [item["source_id"] for item in body["results"]] == ["search-published"]


def test_search_does_not_expose_private_streams_without_auth(
    local_env: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("AUTH_MODE", "production")
    client = TestClient(app)
    stream_store = SqliteStreamStore(path=str(local_env))
    stream_store.create_stream(
        {
            "stream_id": "stream-private-1",
            "title": "Secret Beamline Stream",
            "status": "open",
            "file_count": 3,
            "total_bytes": 1024,
            "last_append_at": None,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "user_id": "owner-user",
            "organization": "org",
            "metadata": {"operator": "Beamline A"},
        }
    )

    resp = client.get("/search", params={"q": "Secret Beamline", "type": "streams"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["results"] == []
