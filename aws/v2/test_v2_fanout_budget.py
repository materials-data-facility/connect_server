"""Budgeted fan-out and projected candidate listing regressions."""

import json

import pytest

from v2 import async_jobs
from v2.store import SqliteSubmissionStore


@pytest.fixture()
def store(tmp_path, monkeypatch):
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "submissions.db"))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "sqlite")
    monkeypatch.setenv("ASYNC_SQLITE_PATH", str(tmp_path / "jobs.db"))
    result = SqliteSubmissionStore()
    for name in "abcdefghijk":
        result.put_submission({
            "source_id": name, "version": "1", "status": "published",
            "dataset_mdata": {"latest": True},
            "title_description_embedding": [0.1] * 1536,
            "embedding_model": "old", "embedding_generated_at": "2025-01-01",
            "metadata_updated_at": "2026-01-01",
            "link_health": {"status": "ok", "checked_at": "2025-01-01"},
            "link_health_checked_at": "2025-01-01",
        })
    return result


def test_projected_listing_has_skip_fields_without_vector(store):
    rows = store.list_fanout_candidates()
    assert [row["source_id"] for row in rows] == list("abcdefghijk")
    assert all("title_description_embedding" not in row for row in rows)
    assert {"version", "status", "dataset_mdata", "embedding_model",
            "embedding_generated_at", "metadata_updated_at", "link_health",
            "link_health_checked_at"} <= rows[0].keys()


@pytest.mark.parametrize("job_type,record_job", [
    (async_jobs.JOB_DISPATCH_EMBEDDING_REBUILD, async_jobs.JOB_GENERATE_EMBEDDING),
    (async_jobs.JOB_LINK_HEALTH_SWEEP, async_jobs.JOB_LINK_HEALTH),
])
def test_budget_continuation_resumes_once(store, monkeypatch, job_type, record_job):
    sent = []
    continuations = []
    snapshots = []
    monkeypatch.setattr(async_jobs, "enqueue_embedding_job",
                        lambda source_id, version: sent.append((source_id, version)))
    monkeypatch.setattr(async_jobs, "enqueue_link_health_job",
                        lambda source_id, version: sent.append((source_id, version)))
    monkeypatch.setattr(async_jobs, "enqueue_snapshot_build_job",
                        lambda: snapshots.append(True) or {"queued": True})
    dispatcher = async_jobs.SqliteJobDispatcher()
    monkeypatch.setattr(dispatcher, "dispatch",
                        lambda kind, payload: continuations.append((kind, payload)) or {})
    monkeypatch.setattr(async_jobs, "get_job_dispatcher", lambda: dispatcher)
    ticks = iter([100] * 11 + [0])
    monkeypatch.setattr(async_jobs, "_fanout_clock", lambda: lambda: next(ticks))
    first = async_jobs.process_job(job_type, {"force": True})
    assert first["continued"] is True
    assert first["enqueued"] == 10
    assert first.get("snapshot_job", {}).get("enqueued") is not True
    assert continuations == [(job_type, {"force": True, "start_after": ["j", "1"],
                                          "enqueued_so_far": 10})]
    monkeypatch.setattr(async_jobs, "_fanout_clock", lambda: lambda: 100)
    second = async_jobs.process_job(job_type, continuations[0][1])
    assert second["continued"] is False
    assert sent == [(name, "1") for name in "abcdefghijk"]
    assert snapshots == ([True] if record_job == async_jobs.JOB_GENERATE_EMBEDDING else [])


def test_sqs_batch_splits_and_retries_failed_entries_once(monkeypatch):
    import boto3

    class Client:
        def __init__(self):
            self.calls = []

        def send_message_batch(self, **kwargs):
            entries = kwargs["Entries"]
            self.calls.append(entries)
            if len(self.calls) == 1:
                return {"Failed": [{"Id": "2", "Code": "Throttled"}]}
            if len(self.calls) == 2:
                return {"Failed": [{"Id": "2", "Message": "still throttled"}]}
            return {"Failed": []}

    client = Client()
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    dispatcher = async_jobs.SQSJobDispatcher("queue")
    jobs = [{"job_type": async_jobs.JOB_LINK_HEALTH,
             "payload": {"source_id": str(i), "version": "1"}} for i in range(12)]
    result = dispatcher.dispatch_batch(jobs)
    assert [len(call) for call in client.calls] == [10, 1, 2]
    assert result["enqueued"] == 11
    assert result["enqueue_failures"] == [
        {"source_id": "2", "version": "1", "error": "still throttled"}]
    assert json.loads(client.calls[0][0]["MessageBody"])["job_type"] == async_jobs.JOB_LINK_HEALTH
