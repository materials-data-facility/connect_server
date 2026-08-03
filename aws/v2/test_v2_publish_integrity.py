"""Publish-pipeline integrity: search-task confirmation, DOI gating, publish lock.

Covers the wave-6 fixes:

- B-17  Globus Search ingest returns task *acceptance*; publish now waits for the
        task to reach SUCCESS and treats FAILED/unconfirmed as a publish failure.
- B-19  DataCite mint failure is a publish precondition, not a warning (except
        when DataCite is deliberately mocked).
- B-16  Publishes of the same dataset are serialized by a per-dataset lock, so
        two versions cannot interleave and leave the search index stale.
- B-14c The search subject base is derived from PORTAL_URL instead of being
        hardcoded (canonicalized, because subjects are identity keys).
"""

from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.async_jobs import (
    JOB_PUBLISH_SUBMISSION,
    JobExecutionError,
    handle_sqs_event,
    process_job,
)
from v2.storage import reset_storage_backend

HEADERS = {"X-User-Id": "test-user"}

VALID_SUBMISSION = {
    "title": "Test Dataset",
    "authors": [{"name": "Test User"}],
    "data_sources": ["https://example.com/data.csv"],
}


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """SQLite store + inline job dispatch (a publish runs in the request)."""
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(tmp_path / "files"))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    # Don't burn wall-clock waiting for a contended lock in tests.
    monkeypatch.setenv("PUBLISH_LOCK_WAIT_SECONDS", "0")
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


@pytest.fixture()
def mock_search():
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


def _submit(client: TestClient) -> str:
    resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
    assert resp.status_code == 200
    return resp.json()["source_id"]


def _approve(client: TestClient, source_id: str, mint_doi: bool = True):
    return client.post(
        f"/curation/{source_id}/approve", headers=HEADERS, json={"mint_doi": mint_doi},
    )


def _submission(client: TestClient, source_id: str) -> dict:
    return client.get(f"/status/{source_id}", headers=HEADERS).json()["submission"]


def _history_actions(submission: dict) -> list:
    return [h.get("action") for h in (submission.get("curation_history") or [])]


def _approved_record(source_id: str, version: str) -> dict:
    """A store-ready submission sitting in the pre-publish state."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "source_id": source_id,
        "version": version,
        "versioned_source_id": f"{source_id}-{version}",
        "user_id": "test-user",
        "user_email": "test@example.org",
        "organization": "MDF",
        "status": "approved",
        "dataset_mdata": json.dumps({
            "title": f"Race Dataset v{version}",
            "authors": [{"name": "Test User"}],
            "data_sources": ["https://example.com/data.csv"],
            "version": version,
        }),
        "created_at": now,
        "updated_at": now,
    }


# =========================================================================
# B-17 — a publish waits for the ingest task, not just its acceptance
# =========================================================================


class ApiError(Exception):
    """Stand-in for globus_sdk.GlobusAPIError (which carries http_status)."""

    def __init__(self, http_status, message="api error"):
        super().__init__(f"{http_status}: {message}")
        self.http_status = http_status


class FakeSearchSDK:
    """Stand-in for globus_sdk.SearchClient with scripted task states."""

    class _Resp:
        def __init__(self, data):
            self.data = data

    def __init__(self, task_states, task_id="task-1", ingest_error=None):
        self.task_states = list(task_states)
        self.task_id = task_id
        self.ingest_error = ingest_error
        self.ingest_calls = 0
        self.get_task_calls = 0

    def ingest(self, index_id, ingest_doc):
        self.ingest_calls += 1
        if self.ingest_error:
            raise self.ingest_error
        return self._Resp({"task_id": self.task_id, "acknowledged": True})

    def get_task(self, task_id):
        self.get_task_calls += 1
        state = self.task_states.pop(0) if self.task_states else "PROGRESS"
        if isinstance(state, Exception):
            raise state
        return self._Resp({"task_id": task_id, "state": state, "message": f"mock {state}"})


def _real_client(sdk) -> "object":
    from v2.search_client import GlobusSearchClient

    client = GlobusSearchClient(index_id="idx-1")
    client._client = sdk
    return client


SDK_SUBMISSION = {
    "source_id": "task-ds",
    "version": "1.0",
    "dataset_mdata": '{"title":"T","authors":[{"name":"A"}],"data_sources":[]}',
}


class TestIngestTaskConfirmation:
    """ingest() must report the task outcome, not the API's acceptance."""

    @pytest.fixture(autouse=True)
    def fast_calls(self, monkeypatch):
        """Per-call HTTP ceiling small enough that tiny budgets still poll."""
        monkeypatch.setenv("GLOBUS_HTTP_TIMEOUT_SECONDS", "0.01")

    def test_ingest_waits_for_task_success(self):
        sdk = FakeSearchSDK(["PENDING", "PROGRESS", "SUCCESS"])
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=5)

        assert result["success"] is True
        assert result["confirmed"] is True
        assert result["task_state"] == "SUCCESS"
        assert sdk.get_task_calls == 3

    def test_ingest_reports_failed_task(self):
        sdk = FakeSearchSDK(["PROGRESS", "FAILED"])
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=5)

        assert result["success"] is False
        assert result["confirmed"] is False
        assert result["task_state"] == "FAILED"
        assert "FAILED" in result["error"]

    def test_ingest_reports_timeout_when_task_never_settles(self):
        sdk = FakeSearchSDK(["PENDING"] * 50)
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=0.05)

        assert result["success"] is False
        assert result["timed_out"] is True
        assert "not confirmed" in result["error"]

    def test_transient_poll_error_does_not_fail_the_ingest(self):
        sdk = FakeSearchSDK([RuntimeError("503 from search"), "SUCCESS"])
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=5)

        assert result["success"] is True
        assert result["task_state"] == "SUCCESS"

    def test_wait_disabled_returns_unconfirmed_acceptance(self):
        """Accept-only mode stays available for callers not gating a publish."""
        sdk = FakeSearchSDK(["SUCCESS"])
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=0)

        assert result["success"] is True
        assert result["confirmed"] is False
        assert sdk.get_task_calls == 0

    def test_missing_task_id_is_a_failure_when_confirmation_was_asked_for(self):
        """No task id means completion can never be established — not a success."""
        sdk = FakeSearchSDK(["SUCCESS"], task_id=None)
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=5)

        assert result["success"] is False
        assert result["confirmed"] is False
        assert "no task_id" in result["error"]
        assert sdk.get_task_calls == 0

    def test_permanent_4xx_fails_immediately(self):
        """A 4xx will not heal: fail fast instead of burning the whole budget."""
        sdk = FakeSearchSDK([ApiError(404, "no such task")] * 10)
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=30)

        assert result["success"] is False
        assert result.get("timed_out") is not True
        assert sdk.get_task_calls == 1

    def test_rate_limit_and_5xx_are_retried(self):
        sdk = FakeSearchSDK([ApiError(429, "slow down"), ApiError(503, "unavailable"), "SUCCESS"])
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=5)

        assert result["success"] is True
        assert sdk.get_task_calls == 3

    def test_submission_call_counts_against_the_budget(self, monkeypatch):
        """A slow ingest submission eats the budget; polling cannot extend it."""
        from v2 import search_client as sc

        sdk = FakeSearchSDK(["PENDING"] * 50)
        client = _real_client(sdk)

        real_ingest = sdk.ingest

        def slow_ingest(index_id, doc):
            # Burn the whole budget inside the submission call
            import time as _t
            _t.sleep(0.12)
            return real_ingest(index_id, doc)

        sdk.ingest = slow_ingest
        monkeypatch.setenv("GLOBUS_HTTP_TIMEOUT_SECONDS", "0.01")
        result = client.ingest(SDK_SUBMISSION, wait_seconds=0.1)

        assert result["success"] is False
        assert result["timed_out"] is True
        # One poll is always allowed, but the budget was already spent
        assert sdk.get_task_calls <= 1

    def test_transport_is_bounded(self, monkeypatch):
        """The SDK's 60s/5-retry defaults would outlive the whole Lambda."""
        import globus_sdk

        from v2.search_client import _build_search_client

        monkeypatch.setenv("GLOBUS_HTTP_TIMEOUT_SECONDS", "3.5")
        monkeypatch.setenv("GLOBUS_HTTP_MAX_RETRIES", "0")
        client = _build_search_client(globus_sdk)

        timeout = getattr(client.transport, "http_timeout", None)
        assert timeout == 3.5
        retry_config = getattr(client, "retry_config", None) or getattr(
            client.transport, "retry_config", None,
        )
        assert getattr(retry_config, "max_retries", 0) == 0

    def test_permanent_error_classification(self):
        from v2.search_client import _is_permanent_api_error

        assert _is_permanent_api_error(ApiError(400)) is True
        assert _is_permanent_api_error(ApiError(401)) is True
        assert _is_permanent_api_error(ApiError(404)) is True
        assert _is_permanent_api_error(ApiError(429)) is False
        assert _is_permanent_api_error(ApiError(408)) is False
        assert _is_permanent_api_error(ApiError(500)) is False
        assert _is_permanent_api_error(RuntimeError("connection reset")) is False

    def test_wait_budget_comes_from_env(self, monkeypatch: pytest.MonkeyPatch):
        from v2.search_client import _default_ingest_wait_seconds

        monkeypatch.delenv("SEARCH_INGEST_WAIT_SECONDS", raising=False)
        assert _default_ingest_wait_seconds() == 20.0
        monkeypatch.setenv("SEARCH_INGEST_WAIT_SECONDS", "3.5")
        assert _default_ingest_wait_seconds() == 3.5
        monkeypatch.setenv("SEARCH_INGEST_WAIT_SECONDS", "not-a-number")
        assert _default_ingest_wait_seconds() == 20.0

    def test_ingest_api_failure_still_reported(self):
        sdk = FakeSearchSDK([], ingest_error=RuntimeError("index unavailable"))
        result = _real_client(sdk).ingest(SDK_SUBMISSION, wait_seconds=5)

        assert result["success"] is False
        assert "index unavailable" in result["error"]


class TestPublishGatedOnTaskCompletion:
    """An accepted-but-not-completed ingest must not publish the record."""

    def test_failed_ingest_task_does_not_publish(self, env, mock_search, shared_datacite):
        mock_search.fail_next_ingest_tasks = 1

        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 502

        sub = _submission(client, source_id)
        assert sub["status"] == "approved"
        assert not sub.get("published_at")
        assert mock_search.get_entry(source_id) is None
        assert "publish_failed" in _history_actions(sub)

    def test_unconfirmed_ingest_task_does_not_publish(self, env, mock_search, shared_datacite):
        mock_search.timeout_next_ingest_tasks = 1

        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 502

        sub = _submission(client, source_id)
        assert sub["status"] == "approved"
        assert mock_search.get_entry(source_id) is None

    def test_retry_after_task_failure_publishes(self, env, mock_search, shared_datacite):
        mock_search.fail_next_ingest_tasks = 1

        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 502

        result = process_job(
            JOB_PUBLISH_SUBMISSION,
            {"source_id": source_id, "version": "1.0", "mint_doi": True},
        )
        assert result["success"] is True
        assert result["search_ingest"]["confirmed"] is True

        sub = _submission(client, source_id)
        assert sub["status"] == "published"
        assert mock_search.get_entry(source_id) is not None
        # The failed task minted no second DOI
        assert len(shared_datacite._dois) == 1

    def test_accepted_but_unconfirmed_ingest_does_not_publish(
        self, env, mock_search, shared_datacite,
    ):
        """success=True + confirmed=False is acceptance, not indexing."""
        mock_search.unconfirmed_next_ingests = 1

        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 502

        sub = _submission(client, source_id)
        assert sub["status"] == "approved"
        assert not sub.get("published_at")
        assert "publish_failed" in _history_actions(sub)

    def test_publish_never_runs_in_accept_only_mode(self, env, monkeypatch):
        """Whatever the config says, the publish path keeps a positive budget."""
        from v2.async_jobs import MIN_PUBLISH_SEARCH_WAIT_SECONDS, _publish_search_wait_seconds
        from v2.async_jobs import record_time_budget

        monkeypatch.setenv("PUBLISH_SEARCH_WAIT_SECONDS", "0")
        assert _publish_search_wait_seconds() == MIN_PUBLISH_SEARCH_WAIT_SECONDS

        monkeypatch.setenv("PUBLISH_SEARCH_WAIT_SECONDS", "30")
        assert _publish_search_wait_seconds() == 30.0

        # Narrowed by the batch loop's remaining-time budget...
        with record_time_budget(25.0):
            assert _publish_search_wait_seconds() == 10.0
        # ...but never below the floor
        with record_time_budget(1.0):
            assert _publish_search_wait_seconds() == MIN_PUBLISH_SEARCH_WAIT_SECONDS

    def test_publish_passes_a_positive_budget_to_the_client(
        self, env, mock_search, shared_datacite,
    ):
        seen = {}
        real_ingest = mock_search.ingest

        def recording_ingest(submission, **kwargs):
            seen.update(kwargs)
            return real_ingest(submission, **kwargs)

        mock_search.ingest = recording_ingest
        try:
            client = TestClient(app)
            source_id = _submit(client)
            assert _approve(client, source_id).status_code == 200
        finally:
            mock_search.ingest = real_ingest

        assert seen["wait_seconds"] >= 5.0

    def test_successful_publish_confirms_the_task(self, env, mock_search, shared_datacite):
        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 200

        result = process_job(
            JOB_PUBLISH_SUBMISSION,
            {"source_id": source_id, "version": "1.0", "mint_doi": True},
        )
        ingest = result["search_ingest"]
        assert ingest["confirmed"] is True
        assert mock_search.get_task(ingest["task_id"])["state"] == "SUCCESS"


# =========================================================================
# B-19 — a DOI is a publish precondition
# =========================================================================


class FailingDataCiteClient:
    """Mints nothing; every call reports failure the way DataCite outages do."""

    def __init__(self):
        self.mint_calls = 0
        self._dois = {}

    def _generate_suffix(self, source_id: str) -> str:
        return source_id.lower()

    def mint_doi(self, *args, **kwargs):
        self.mint_calls += 1
        return {"success": False, "error": "DataCite 503: service unavailable"}

    def update_metadata(self, *args, **kwargs):
        return {"success": False, "error": "DataCite 503: service unavailable"}

    def close(self):
        pass


class TestDoiIsAPublishPrecondition:

    def test_mint_failure_blocks_publish(self, env, mock_search, monkeypatch):
        """No DOI, no publish: status holds and the job raises for retry."""
        monkeypatch.setenv("USE_MOCK_DATACITE", "false")
        failing = FailingDataCiteClient()
        monkeypatch.setattr("v2.datacite.get_datacite_client", lambda *a, **k: failing)

        client = TestClient(app)
        source_id = _submit(client)
        approve = _approve(client, source_id)
        assert approve.status_code == 502

        sub = _submission(client, source_id)
        assert sub["status"] == "approved"
        assert not sub.get("published_at")
        assert not sub.get("doi")
        # The search index is never touched when the DOI step fails
        assert mock_search.get_entry(source_id) is None
        assert mock_search.ingest_calls == 0
        assert "publish_failed" in _history_actions(sub)

    def test_mint_failure_raises_job_execution_error(self, env, mock_search, monkeypatch):
        monkeypatch.setenv("USE_MOCK_DATACITE", "false")
        monkeypatch.setattr(
            "v2.datacite.get_datacite_client", lambda *a, **k: FailingDataCiteClient(),
        )

        client = TestClient(app)
        source_id = _submit(client)
        _approve(client, source_id)

        with pytest.raises(JobExecutionError) as excinfo:
            process_job(
                JOB_PUBLISH_SUBMISSION,
                {"source_id": source_id, "version": "1.0", "mint_doi": True},
            )
        assert "DOI step failed" in str(excinfo.value)
        assert excinfo.value.result["success"] is False

    def test_retry_after_datacite_recovers_publishes(self, env, mock_search, monkeypatch):
        """The gate is a hold, not a dead end: the retry publishes with a DOI."""
        from v2.datacite import MockDataCiteClient

        monkeypatch.setenv("USE_MOCK_DATACITE", "false")
        failing = FailingDataCiteClient()
        monkeypatch.setattr("v2.datacite.get_datacite_client", lambda *a, **k: failing)

        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 502
        assert failing.mint_calls == 1

        working = MockDataCiteClient(prefix="10.99999")
        monkeypatch.setattr("v2.datacite.get_datacite_client", lambda *a, **k: working)

        result = process_job(
            JOB_PUBLISH_SUBMISSION,
            {"source_id": source_id, "version": "1.0", "mint_doi": True},
        )
        assert result["success"] is True

        sub = _submission(client, source_id)
        assert sub["status"] == "published"
        assert sub["doi"]
        assert len(working._dois) == 1

    def test_mint_failure_is_tolerated_when_datacite_is_mocked(
        self, env, mock_search, monkeypatch,
    ):
        """USE_MOCK_DATACITE=true: DOIs are fake, so a DOI failure must not block."""
        monkeypatch.setenv("USE_MOCK_DATACITE", "true")
        monkeypatch.setattr(
            "v2.datacite.get_datacite_client", lambda *a, **k: FailingDataCiteClient(),
        )

        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 200

        sub = _submission(client, source_id)
        assert sub["status"] == "published"
        assert not sub.get("doi")
        assert mock_search.get_entry(source_id) is not None

    def test_publish_without_doi_when_minting_declined(self, env, mock_search, shared_datacite):
        """mint_doi=false with no prior DOI is a legitimate no-DOI publish."""
        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id, mint_doi=False).status_code == 200

        # Re-running the job reports a successful DOI step that minted nothing —
        # the gate must key on the step's success, not on a DOI being present.
        result = process_job(
            JOB_PUBLISH_SUBMISSION,
            {"source_id": source_id, "version": "1.0", "mint_doi": False},
        )
        assert result["success"] is True
        assert result["doi"]["success"] is True
        assert result["doi"]["doi"] is None

        sub = _submission(client, source_id)
        assert sub["status"] == "published"
        assert not sub.get("doi")
        assert shared_datacite._dois == {}
        assert mock_search.get_entry(source_id) is not None


# =========================================================================
# B-16 — per-dataset publish lock
# =========================================================================


class TestPublishLockStore:
    """The lock primitive itself, on the sqlite backend."""

    @pytest.fixture()
    def store(self, tmp_path, monkeypatch):
        from v2.store import SqliteSubmissionStore

        return SqliteSubmissionStore(path=str(tmp_path / "lock.db"))

    def test_second_acquire_is_refused(self, store):
        assert store.acquire_publish_lock("ds-1", "owner-a") is True
        assert store.acquire_publish_lock("ds-1", "owner-b") is False

    def test_different_datasets_do_not_contend(self, store):
        assert store.acquire_publish_lock("ds-1", "owner-a") is True
        assert store.acquire_publish_lock("ds-2", "owner-b") is True

    def test_release_frees_the_lock(self, store):
        assert store.acquire_publish_lock("ds-1", "owner-a") is True
        assert store.release_publish_lock("ds-1", "owner-a") is True
        assert store.acquire_publish_lock("ds-1", "owner-b") is True

    def test_release_by_a_non_owner_is_a_no_op(self, store):
        store.acquire_publish_lock("ds-1", "owner-a")
        assert store.release_publish_lock("ds-1", "someone-else") is False
        assert store.acquire_publish_lock("ds-1", "owner-b") is False

    def test_expired_lock_is_taken_over(self, store):
        """A worker killed mid-publish must not deadlock the dataset."""
        assert store.acquire_publish_lock("ds-1", "crashed-worker", ttl_seconds=0) is True
        assert store.acquire_publish_lock("ds-1", "next-worker") is True

    def test_old_owner_release_after_takeover_is_refused(self, store):
        store.acquire_publish_lock("ds-1", "crashed-worker", ttl_seconds=0)
        assert store.acquire_publish_lock("ds-1", "next-worker") is True
        assert store.release_publish_lock("ds-1", "crashed-worker") is False
        assert store.acquire_publish_lock("ds-1", "third-worker") is False

    def test_reserved_version_is_not_a_submission(self, store):
        """The lock sentinel is unreadable and unwritable as a version."""
        from v2.store import PUBLISH_LOCK_VERSION

        assert store.get_submission("ds-1", PUBLISH_LOCK_VERSION) is None
        with pytest.raises(ValueError):
            store.upsert_submission({"source_id": "ds-1", "version": PUBLISH_LOCK_VERSION})
        with pytest.raises(ValueError):
            store.put_submission({"source_id": "ds-1", "version": PUBLISH_LOCK_VERSION})
        with pytest.raises(ValueError):
            store.update_status("ds-1", PUBLISH_LOCK_VERSION, "published")
        with pytest.raises(ValueError):
            store.update_profile("ds-1", PUBLISH_LOCK_VERSION, "{}")
        with pytest.raises(ValueError):
            store.increment_counter("ds-1", PUBLISH_LOCK_VERSION, "view_count")

    def test_context_manager_releases_on_error(self, store):
        from v2.store import PublishLockUnavailable, publish_lock

        with pytest.raises(ValueError):
            with publish_lock(store, "ds-1", owner="owner-a", wait_seconds=0):
                raise ValueError("boom")

        # Released despite the exception
        with publish_lock(store, "ds-1", owner="owner-b", wait_seconds=0):
            pass

        store.acquire_publish_lock("ds-1", "holder")
        with pytest.raises(PublishLockUnavailable):
            with publish_lock(store, "ds-1", owner="owner-c", wait_seconds=0):
                pass

    def test_lock_items_are_hidden_from_version_reads(self):
        """The DynamoDB lock shares the submissions table and must stay invisible."""
        from v2.store import PUBLISH_LOCK_VERSION, _without_lock_items

        items = [
            {"source_id": "ds-1", "version": "1.0"},
            {"source_id": "ds-1", "version": PUBLISH_LOCK_VERSION, "lock_owner": "w"},
            {"source_id": "ds-1", "version": "2.0"},
        ]
        assert [i["version"] for i in _without_lock_items(items)] == ["1.0", "2.0"]


ACQUIRE_CONDITION = "attribute_not_exists(source_id) OR lock_expires_at < :now"
RELEASE_CONDITION = "lock_owner = :owner"


def _evaluate_condition(expression, item, values):
    """Evaluate the DynamoDB condition expressions the lock actually uses.

    Written as an evaluator rather than a rubber stamp so the test fails if the
    production expression stops meaning what the lock needs it to mean.
    """
    if expression == ACQUIRE_CONDITION:
        if item is None:  # attribute_not_exists(source_id)
            return True
        return item.get("lock_expires_at", 0) < values[":now"]  # expired
    if expression == RELEASE_CONDITION:
        return item is not None and item.get("lock_owner") == values[":owner"]
    raise AssertionError(f"unhandled ConditionExpression: {expression!r}")


class FakeDynamoTable:
    """Minimal DynamoDB table that evaluates the lock's conditional writes."""

    def __init__(self):
        self.items = {}
        self.conditions = []

    def put_item(self, Item, ConditionExpression=None, ExpressionAttributeValues=None):
        key = (Item["source_id"], Item["version"])
        existing = self.items.get(key)
        if ConditionExpression is not None:
            self.conditions.append((ConditionExpression, ExpressionAttributeValues))
            if not _evaluate_condition(
                ConditionExpression, existing, ExpressionAttributeValues or {},
            ):
                raise _conditional_check_failure()
        self.items[key] = dict(Item)

    def delete_item(self, Key, ConditionExpression=None, ExpressionAttributeValues=None):
        key = (Key["source_id"], Key["version"])
        existing = self.items.get(key)
        if ConditionExpression is not None:
            self.conditions.append((ConditionExpression, ExpressionAttributeValues))
            if not _evaluate_condition(
                ConditionExpression, existing, ExpressionAttributeValues or {},
            ):
                raise _conditional_check_failure()
        self.items.pop(key, None)


def _conditional_check_failure() -> Exception:
    exc = Exception("conditional check failed")
    exc.response = {"Error": {"Code": "ConditionalCheckFailedException"}}
    return exc


class TestPublishLockDynamoBackend:
    """The DynamoDB lock uses a conditional write, the only atomic primitive here."""

    @pytest.fixture()
    def store(self):
        from v2.store import DynamoSubmissionStore

        store = DynamoSubmissionStore.__new__(DynamoSubmissionStore)
        store.table = FakeDynamoTable()
        return store

    def test_conditional_write_serializes_acquire(self, store):
        assert store.acquire_publish_lock("ds-1", "owner-a") is True
        assert store.acquire_publish_lock("ds-1", "owner-b") is False
        assert store.release_publish_lock("ds-1", "owner-a") is True
        assert store.acquire_publish_lock("ds-1", "owner-b") is True

    def test_expired_lock_is_taken_over(self, store):
        assert store.acquire_publish_lock("ds-1", "crashed", ttl_seconds=-1) is True
        assert store.acquire_publish_lock("ds-1", "next") is True

    def test_old_owner_release_after_takeover_does_not_free_the_new_lock(self, store):
        """The crashed worker coming back must not unlock its successor."""
        store.acquire_publish_lock("ds-1", "crashed", ttl_seconds=-1)
        assert store.acquire_publish_lock("ds-1", "next") is True

        assert store.release_publish_lock("ds-1", "crashed") is False
        # Still held by "next"
        assert store.acquire_publish_lock("ds-1", "third") is False
        assert store.release_publish_lock("ds-1", "next") is True

    def test_exact_condition_expressions(self, store):
        """Pin the expressions: the lock is only atomic because of them."""
        store.acquire_publish_lock("ds-1", "owner-a")
        store.release_publish_lock("ds-1", "owner-a")

        acquire_expr, acquire_values = store.table.conditions[0]
        release_expr, release_values = store.table.conditions[1]

        assert acquire_expr == ACQUIRE_CONDITION
        assert set(acquire_values) == {":now"}
        assert release_expr == RELEASE_CONDITION
        assert release_values == {":owner": "owner-a"}

    def test_release_by_non_owner_is_refused(self, store):
        store.acquire_publish_lock("ds-1", "owner-a")
        assert store.release_publish_lock("ds-1", "intruder") is False

    def test_reserved_version_is_not_a_submission(self, store):
        from v2.store import PUBLISH_LOCK_VERSION

        store.acquire_publish_lock("ds-1", "owner-a")
        # The lock item exists in the table...
        assert ("ds-1", PUBLISH_LOCK_VERSION) in store.table.items
        # ...but is not reachable as a submission, and cannot be overwritten
        assert store.get_submission("ds-1", PUBLISH_LOCK_VERSION) is None
        with pytest.raises(ValueError):
            store.upsert_submission({"source_id": "ds-1", "version": PUBLISH_LOCK_VERSION})
        with pytest.raises(ValueError):
            store.update_status("ds-1", PUBLISH_LOCK_VERSION, "published")
        assert store.table.items[("ds-1", PUBLISH_LOCK_VERSION)]["lock_owner"] == "owner-a"

    def test_lock_item_carries_no_gsi_keys(self, store):
        """A lock item must not surface in the status/user/org/legacy indexes."""
        store.acquire_publish_lock("ds-1", "owner-a")
        item = store.table.items[("ds-1", "__publish_lock__")]
        for gsi_key in ("status", "user_id", "organization", "legacy_source_id"):
            assert gsi_key not in item
        assert item["record_type"] == "publish_lock"

    def test_unexpected_errors_propagate(self, store):
        def explode(**kwargs):
            raise RuntimeError("throttled")

        store.table.put_item = explode
        with pytest.raises(RuntimeError):
            store.acquire_publish_lock("ds-1", "owner-a")


class TestPublishJobSerialization:
    """The publish critical section is exclusive per dataset."""

    def test_concurrent_publish_of_same_dataset_is_deferred(
        self, env, mock_search, shared_datacite,
    ):
        """A publish that starts while another holds the lock is retried, not raced.

        The second job is launched from inside the first one's search ingest —
        i.e. exactly when the first is between reading the version set and
        writing the index — which is the window B-16 describes.
        """
        from v2.store import get_store

        client = TestClient(app)
        source_id = _submit(client)

        inner: dict = {}
        real_ingest = mock_search.ingest

        def reentrant_ingest(submission, **kwargs):
            if "error" not in inner and "ok" not in inner:
                try:
                    process_job(
                        JOB_PUBLISH_SUBMISSION,
                        {"source_id": source_id, "version": "1.0", "mint_doi": True},
                    )
                    inner["ok"] = True
                except JobExecutionError as exc:
                    inner["error"] = exc
            return real_ingest(submission, **kwargs)

        mock_search.ingest = reentrant_ingest
        try:
            assert _approve(client, source_id).status_code == 200
        finally:
            mock_search.ingest = real_ingest

        assert "error" in inner, "the concurrent publish was not blocked"
        assert inner["error"].result["lock_contended"] is True

        # The outer publish still completed normally and holds no stale lock
        sub = _submission(client, source_id)
        assert sub["status"] == "published"
        assert mock_search.get_entry(source_id) is not None
        assert get_store().acquire_publish_lock(source_id, "after-the-fact") is True

    def test_lock_is_released_when_publish_fails(self, env, mock_search, shared_datacite):
        from v2.store import get_store

        mock_search.fail_next_ingests = 1

        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 502

        # Not deadlocked: the retry can take the lock and publish
        assert get_store().release_publish_lock(source_id, "nobody") is False
        result = process_job(
            JOB_PUBLISH_SUBMISSION,
            {"source_id": source_id, "version": "1.0", "mint_doi": True},
        )
        assert result["success"] is True

    def test_two_threads_publishing_two_versions_leave_the_newest_indexed(
        self, env, mock_search, shared_datacite, monkeypatch,
    ):
        """The real race: two workers, two versions, one search entry.

        Both jobs run concurrently on separate store connections and are
        released from a barrier together. Whichever order they interleave in,
        the surviving index entry must describe v2.0 — the loser re-reads the
        version set inside the lock and defers to the newer publish.
        """
        from v2.store import get_store

        # Let the loser wait for the lock rather than bounce to the queue.
        monkeypatch.setenv("PUBLISH_LOCK_WAIT_SECONDS", "20")

        source_id = "race-dataset"
        store = get_store()
        for version in ("1.0", "2.0"):
            store.put_submission(_approved_record(source_id, version))

        # Pin the losing interleaving: the OLD version's ingest is the slow one,
        # so without serialization v1.0 lands in the index last and the entry
        # ends up describing the superseded version. With the lock, whichever
        # job runs second re-reads the version set and does the right thing.
        real_ingest = mock_search.ingest

        def slow_ingest(submission, **kwargs):
            time.sleep(0.4 if submission.get("version") == "1.0" else 0.05)
            return real_ingest(submission, **kwargs)

        mock_search.ingest = slow_ingest

        barrier = threading.Barrier(2)
        results = {}
        errors = {}

        def publish(version):
            barrier.wait(timeout=10)
            try:
                results[version] = process_job(
                    JOB_PUBLISH_SUBMISSION,
                    {"source_id": source_id, "version": version, "mint_doi": True},
                )
            except Exception as exc:  # pragma: no cover - surfaced by the assert below
                errors[version] = exc

        threads = [threading.Thread(target=publish, args=(v,)) for v in ("1.0", "2.0")]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
            assert not t.is_alive()
        mock_search.ingest = real_ingest

        assert not errors, f"publish raised: {errors}"
        assert set(results) == {"1.0", "2.0"}
        assert all(r["success"] for r in results.values())

        # One entry for the dataset, describing the newest version
        assert len(mock_search._entries) == 1
        entry = mock_search.get_entry(source_id)
        assert entry["content"]["mdf"]["version"] == "2.0"

        # Both records published, and no lock left behind
        for version in ("1.0", "2.0"):
            assert store.get_submission(source_id, version)["status"] == "published"
        assert get_store().acquire_publish_lock(source_id, "after-the-fact") is True

    def test_lock_does_not_leak_into_version_listing(self, env, mock_search, shared_datacite):
        from v2.store import get_store

        client = TestClient(app)
        source_id = _submit(client)
        assert _approve(client, source_id).status_code == 200

        store = get_store()
        store.acquire_publish_lock(source_id, "held")
        versions = store.list_versions(source_id)
        assert [v["version"] for v in versions] == ["1.0"]


# =========================================================================
# Batch time budget — an invocation must return its batchItemFailures
# =========================================================================


class FakeLambdaContext:
    def __init__(self, remaining_seconds):
        self.remaining_seconds = remaining_seconds

    def get_remaining_time_in_millis(self):
        return int(self.remaining_seconds * 1000)


def _sqs_records(count: int) -> dict:
    return {
        "Records": [
            {
                "messageId": f"msg-{i}",
                "body": json.dumps({
                    "job_type": JOB_PUBLISH_SUBMISSION,
                    "payload": {"source_id": f"ds-{i}", "version": "1.0", "mint_doi": True},
                }),
            }
            for i in range(count)
        ]
    }


class TestBatchTimeBudget:
    """Records the invocation cannot finish are deferred, not silently dropped."""

    @pytest.fixture()
    def processed(self, monkeypatch):
        seen = []
        monkeypatch.setattr(
            "v2.async_jobs.process_job",
            lambda job_type, payload: seen.append(payload["source_id"]) or {"success": True},
        )
        return seen

    def test_all_records_run_when_time_allows(self, env, processed):
        result = handle_sqs_event(_sqs_records(3), FakeLambdaContext(600))

        assert processed == ["ds-0", "ds-1", "ds-2"]
        assert result["batchItemFailures"] == []

    def test_remaining_records_are_deferred_when_time_runs_out(self, env, processed):
        """Deferred records must be reported so SQS redelivers only them."""
        result = handle_sqs_event(_sqs_records(4), FakeLambdaContext(10))

        # The first is always attempted; the rest never start
        assert processed == ["ds-0"]
        assert [f["itemIdentifier"] for f in result["batchItemFailures"]] == [
            "msg-1", "msg-2", "msg-3",
        ]

    def test_failures_and_deferrals_are_both_reported(self, env, monkeypatch):
        calls = []

        def flaky(job_type, payload):
            calls.append(payload["source_id"])
            raise RuntimeError("boom")

        monkeypatch.setattr("v2.async_jobs.process_job", flaky)
        result = handle_sqs_event(_sqs_records(3), FakeLambdaContext(5))

        assert calls == ["ds-0"]
        ids = [f["itemIdentifier"] for f in result["batchItemFailures"]]
        assert ids == ["msg-0", "msg-1", "msg-2"]

    def test_without_a_context_the_configured_timeout_is_used(self, env, processed, monkeypatch):
        """async_worker does not pass context yet; the env fallback still bounds us."""
        monkeypatch.setenv("ASYNC_WORKER_TIMEOUT_SECONDS", "1")
        result = handle_sqs_event(_sqs_records(3))

        assert processed == ["ds-0"]
        assert len(result["batchItemFailures"]) == 2

    def test_no_context_and_no_config_processes_everything(self, env, processed, monkeypatch):
        monkeypatch.delenv("ASYNC_WORKER_TIMEOUT_SECONDS", raising=False)
        result = handle_sqs_event(_sqs_records(3))

        assert processed == ["ds-0", "ds-1", "ds-2"]
        assert result["batchItemFailures"] == []

    def test_record_budget_narrows_the_search_wait(self, env, monkeypatch):
        """The per-record budget reaches the publish job that consumes it."""
        from v2.async_jobs import _publish_search_wait_seconds

        monkeypatch.setenv("PUBLISH_SEARCH_WAIT_SECONDS", "30")
        seen = []
        monkeypatch.setattr(
            "v2.async_jobs.process_job",
            lambda job_type, payload: seen.append(_publish_search_wait_seconds()),
        )
        handle_sqs_event(_sqs_records(1), FakeLambdaContext(28))

        # 28s left - 5s margin - 15s of non-search overhead = 8s for the ingest
        assert seen == [8.0]


# =========================================================================
# B-14c — search subject base comes from PORTAL_URL
# =========================================================================


class TestSearchSubjectBase:
    """Subjects are identity keys: configurable, but stable by default."""

    def _subject(self, source_id="ds-1"):
        from v2.search_client import MockGlobusSearchClient

        entry = MockGlobusSearchClient().build_gmeta_entry({
            "source_id": source_id,
            "version": "1.0",
            "dataset_mdata": '{"title":"T","authors":[{"name":"A"}],"data_sources":[]}',
        })
        return entry["subject"]

    def test_default_matches_the_existing_index(self, monkeypatch):
        monkeypatch.delenv("PORTAL_URL", raising=False)
        monkeypatch.delenv("SEARCH_SUBJECT_BASE", raising=False)
        assert self._subject() == "https://materialsdatafacility.org/detail/ds-1"

    def test_portal_url_drives_the_subject(self, monkeypatch):
        monkeypatch.delenv("SEARCH_SUBJECT_BASE", raising=False)
        monkeypatch.setenv("PORTAL_URL", "https://staging.materialsdatafacility.org/")
        assert self._subject() == "https://staging.materialsdatafacility.org/detail/ds-1"

    def test_www_portal_url_keeps_existing_subjects(self, monkeypatch):
        """PORTAL_URL is deployed as www.…; subjects in the index are not."""
        monkeypatch.delenv("SEARCH_SUBJECT_BASE", raising=False)
        monkeypatch.setenv("PORTAL_URL", "https://www.materialsdatafacility.org")
        assert self._subject() == "https://materialsdatafacility.org/detail/ds-1"

    def test_portal_url_already_ending_in_detail_is_not_doubled(self, monkeypatch):
        monkeypatch.delenv("SEARCH_SUBJECT_BASE", raising=False)
        monkeypatch.setenv("PORTAL_URL", "https://materialsdatafacility.org/detail")
        assert self._subject() == "https://materialsdatafacility.org/detail/ds-1"

    def test_explicit_override_wins(self, monkeypatch):
        monkeypatch.setenv("PORTAL_URL", "https://www.materialsdatafacility.org")
        monkeypatch.setenv("SEARCH_SUBJECT_BASE", "https://example.test/d/")
        assert self._subject() == "https://example.test/d/ds-1"

    def test_ingest_and_delete_agree_on_the_subject(self, monkeypatch):
        from v2.search_client import MockGlobusSearchClient

        monkeypatch.delenv("SEARCH_SUBJECT_BASE", raising=False)
        monkeypatch.setenv("PORTAL_URL", "https://portal.example.test")

        client = MockGlobusSearchClient()
        submission = {
            "source_id": "round-trip",
            "version": "1.0",
            "dataset_mdata": '{"title":"T","authors":[{"name":"A"}],"data_sources":[]}',
        }
        client.ingest(submission)
        assert client.get_entry("round-trip") is not None
        client.delete_entry("round-trip")
        assert client.get_entry("round-trip") is None
