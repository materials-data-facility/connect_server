from __future__ import annotations

import base64
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.async_jobs import run_sqlite_worker_once
from v2.storage import reset_storage_backend


@pytest.fixture()
def async_sqlite_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("ASYNC_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "sqlite")
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


def test_async_profile_job_with_sqlite_worker(async_sqlite_env):
    client = TestClient(app)
    headers = {"X-User-Id": "owner-user"}

    stream = client.post("/stream/create", headers=headers, json={"title": "Async Profile Stream"})
    assert stream.status_code == 200
    stream_id = stream.json()["stream_id"]

    content_b64 = base64.b64encode(b"a,b\n1,2\n3,4\n").decode("ascii")
    upload = client.post(
        f"/stream/{stream_id}/upload",
        headers=headers,
        json={"filename": "sample.csv", "content_base64": content_b64, "content_type": "text/csv"},
    )
    assert upload.status_code == 200

    snap = client.post(
        f"/stream/{stream_id}/snapshot",
        headers=headers,
        json={"title": "Snapshot"},
    )
    assert snap.status_code == 200
    body = snap.json()
    source_id = body["source_id"]
    assert body["profile_job"]["queued"] is True
    assert body["profile_job"]["mode"] == "sqlite"

    before = client.get(f"/status/{source_id}")
    assert before.status_code == 200
    assert before.json()["submission"]["status"] == "pending_curation"
    assert before.json()["submission"].get("dataset_profile") in (None, "")

    worker_result = run_sqlite_worker_once(limit=10)
    assert worker_result["processed"] >= 1
    assert worker_result["failed"] == 0

    after = client.get(f"/status/{source_id}")
    assert after.status_code == 200
    profile = after.json()["submission"].get("dataset_profile")
    assert isinstance(profile, dict)
    assert profile.get("total_files", 0) >= 1


def test_async_submission_doi_job_with_sqlite_worker(async_sqlite_env):
    client = TestClient(app)
    headers = {"X-User-Id": "curator-user"}

    submit = client.post(
        "/submit",
        headers=headers,
        json={
            "title": "Async DOI Dataset",
            "authors": [{"name": "Curator"}],
            "data_sources": ["https://example.com/data.csv"],
        },
    )
    assert submit.status_code == 200
    source_id = submit.json()["source_id"]

    approve = client.post(
        f"/curation/{source_id}/approve",
        headers=headers,
        json={"mint_doi": True},
    )
    assert approve.status_code == 200
    approve_body = approve.json()
    assert approve_body["status"] == "approved"
    assert approve_body["publish_job"]["queued"] is True
    assert approve_body["publish_job"]["mode"] == "sqlite"

    before = client.get(f"/status/{source_id}")
    assert before.status_code == 200
    assert before.json()["submission"]["status"] == "approved"
    assert not before.json()["submission"].get("doi")

    worker_result = run_sqlite_worker_once(limit=10)
    assert worker_result["processed"] >= 1
    assert worker_result["failed"] == 0

    after = client.get(f"/status/{source_id}")
    assert after.status_code == 200
    assert after.json()["submission"]["status"] == "published"
    assert after.json()["submission"].get("doi")
