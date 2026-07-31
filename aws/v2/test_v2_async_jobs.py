from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.async_jobs import JOB_PUBLISH_SUBMISSION, handle_sqs_event, run_sqlite_worker_once
from v2.storage import get_storage_backend, reset_storage_backend


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


@pytest.fixture()
def mock_search():
    """A clean mock search index for the duration of a test."""
    from v2.search_client import get_search_client, reset_search_client

    reset_search_client()
    client = get_search_client()
    yield client
    reset_search_client()


@pytest.fixture()
def shared_datacite(monkeypatch: pytest.MonkeyPatch):
    """One MockDataCiteClient shared across all get_datacite_client() calls."""
    from v2.datacite import MockDataCiteClient

    mock = MockDataCiteClient(prefix="10.99999")
    monkeypatch.setattr("v2.datacite.get_datacite_client", lambda *a, **k: mock)
    return mock


def _sqs_event(message_id: str, source_id: str, version: str, mint_doi: bool = True) -> dict:
    return {
        "Records": [
            {
                "messageId": message_id,
                "body": json.dumps({
                    "job_type": JOB_PUBLISH_SUBMISSION,
                    "payload": {
                        "source_id": source_id,
                        "version": version,
                        "mint_doi": mint_doi,
                    },
                }),
            }
        ]
    }


def test_publish_search_failure_reports_sqs_batch_item_failure(
    async_sqlite_env, mock_search, shared_datacite,
):
    """A publish that cannot index fails the SQS batch item so it is redelivered.

    Regression test for B-7: the job used to log a warning and mark the
    submission published anyway, so the message was deleted from the queue and
    the dataset silently never reached the search index.
    """
    client = TestClient(app)
    headers = {"X-User-Id": "curator-user"}

    submit = client.post(
        "/submit",
        headers=headers,
        json={
            "title": "Batch Item Failure Dataset",
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
    assert approve.json()["status"] == "approved"

    # First delivery: search ingest fails
    mock_search.fail_next_ingests = 1
    failed = handle_sqs_event(_sqs_event("msg-1", source_id, "1.0"))
    assert failed["batchItemFailures"] == [{"itemIdentifier": "msg-1"}]

    sub = client.get(f"/status/{source_id}").json()["submission"]
    assert sub["status"] == "approved"
    assert mock_search.get_entry(source_id) is None

    # Redelivery of the same message: succeeds, and does not mint a second DOI
    retried = handle_sqs_event(_sqs_event("msg-1", source_id, "1.0"))
    assert retried["batchItemFailures"] == []

    sub = client.get(f"/status/{source_id}").json()["submission"]
    assert sub["status"] == "published"
    assert sub.get("doi")
    assert mock_search.get_entry(source_id) is not None
    assert len(shared_datacite._dois) == 1


def test_legacy_mint_submission_doi_job_also_indexes(
    async_sqlite_env, mock_search, shared_datacite,
):
    """The legacy mint_submission_doi job must not publish without indexing.

    It is unreachable from the v2 API, but a queued message of that type used to
    mark the record published on DOI success alone.
    """
    from v2.async_jobs import JOB_MINT_SUBMISSION_DOI, process_job

    client = TestClient(app)
    headers = {"X-User-Id": "curator-user"}

    submit = client.post(
        "/submit",
        headers=headers,
        json={
            "title": "Legacy DOI Job Dataset",
            "authors": [{"name": "Curator"}],
            "data_sources": ["https://example.com/data.csv"],
        },
    )
    source_id = submit.json()["source_id"]

    # Approve queues (but does not run) the real publish job in sqlite mode
    approve = client.post(
        f"/curation/{source_id}/approve",
        headers=headers,
        json={"mint_doi": True},
    )
    assert approve.status_code == 200
    assert approve.json()["status"] == "approved"

    result = process_job(JOB_MINT_SUBMISSION_DOI, {"source_id": source_id, "version": "1.0"})
    assert result["success"] is True

    sub = client.get(f"/status/{source_id}").json()["submission"]
    assert sub["status"] == "published"
    assert sub.get("doi")
    assert mock_search.get_entry(source_id) is not None
    assert len(shared_datacite._dois) == 1


def test_async_profile_job_with_sqlite_worker(async_sqlite_env):
    client = TestClient(app)
    headers = {"X-User-Id": "owner-user"}
    stream_id = "async-profile-stream"

    storage = get_storage_backend()
    storage.store_file(
        stream_id=stream_id,
        filename="sample.csv",
        content=b"a,b\n1,2\n3,4\n",
        content_type="text/csv",
    )
    submit = client.post(
        "/submit",
        headers=headers,
        json={
            "title": "Async Profile Dataset",
            "authors": [{"name": "Owner"}],
            "data_sources": [f"stream://{stream_id}"],
        },
    )
    assert submit.status_code == 200
    body = submit.json()
    source_id = body["source_id"]
    assert body["profile_jobs"][0]["queued"] is True
    assert body["profile_jobs"][0]["mode"] == "sqlite"

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
