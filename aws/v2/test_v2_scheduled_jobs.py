from v2 import async_worker


def test_scheduled_event_without_job_runs_transfer_cleanup(monkeypatch):
    calls = []
    monkeypatch.setattr("v2.async_jobs.process_job", lambda job, payload: calls.append((job, payload)) or {"ok": True})
    async_worker.lambda_handler({"source": "aws.events", "detail-type": "Scheduled Event"}, None)
    assert calls == [("cleanup_transfers", {})] or calls[0][0].endswith("cleanup_transfers")


def test_scheduled_event_with_job_input_routes_to_link_health_sweep(monkeypatch):
    calls = []
    monkeypatch.setattr("v2.async_jobs.process_job", lambda job, payload: calls.append((job, payload)) or {"ok": True})
    async_worker.lambda_handler({"source": "aws.events", "job": "link_health_sweep", "payload": {"limit": 5}}, None)
    assert calls == [("link_health_sweep", {"limit": 5})]


def test_unknown_scheduled_job_is_ignored(monkeypatch):
    calls = []
    monkeypatch.setattr("v2.async_jobs.process_job", lambda job, payload: calls.append(job))
    out = async_worker.lambda_handler({"source": "aws.events", "job": "drop_tables"}, None)
    assert calls == [] and out == {"ignored": "drop_tables"}
