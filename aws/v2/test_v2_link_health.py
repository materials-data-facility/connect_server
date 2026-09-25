"""Tests for link health / data availability (extensions proposal P1).

Covers:
- URI -> anonymous HTTPS rewriting (https passthrough, NCSA globus://, other
  collections and schemes -> unverifiable)
- probe_url: 200, HEAD-refused-then-GET-ok, 404, timeout
- aggregate_status: the full ok/degraded/broken/unverifiable matrix
- check_record end to end against a mocked httpx transport, and the 5-URL cap
- store.update_link_health round-trips through the sqlite backend and survives
  a whole-record upsert
- JOB_LINK_HEALTH writes the block; JOB_LINK_HEALTH_SWEEP enqueues one job per
  published record, honours the staleness short-circuit and `force`
- POST /admin/link-health/run enqueues the sweep; GET /admin/link-health/summary
  counts by status and lists broken datasets
- the dataset card exposes status + checked_at only (never the per-URL checks)
- GET /card/{id}?format=agent builds the agent card under the same visibility
  rules

Nothing here touches the network: every probe goes through httpx.MockTransport.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2 import link_health
from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.storage import reset_storage_backend

HEADERS = {"X-User-Id": "test-user"}
CURATOR_HEADERS = {"X-User-Id": "curator-user"}
NCSA = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("ASYNC_SQLITE_PATH", str(db_path))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
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


@pytest.fixture()
def strict_curators(monkeypatch: pytest.MonkeyPatch):
    """Turn off the blanket dev-mode curator grant so authz is actually tested."""
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
    monkeypatch.setenv("CURATOR_USER_IDS", "curator-user")
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _record(
    source_id: str = "ds-1",
    version: str = "1.0",
    data_sources=None,
    download_url=None,
    status: str = "published",
    **extra,
) -> dict:
    """A minimal published record, in the v2.1 top-level shape."""
    mdata = {
        "title": "Fe-Al alloy DFT",
        "authors": [{"name": "Ada Curie", "orcid": "0000-0002-1825-0097"}],
        "description": "Density functional theory calculations of Fe-Al alloys.",
        "keywords": ["alloys", "dft"],
        "publication_year": 2026,
        "data_sources": list(data_sources or []),
        "license": {"name": "CC-BY-4.0", "identifier": "CC-BY-4.0"},
    }
    if download_url:
        mdata["download_url"] = download_url
    record = {
        "source_id": source_id,
        "version": version,
        "versioned_source_id": f"{source_id}-{version}",
        "user_id": "test-user",
        "user_email": "test@example.com",
        "organization": "MDF Open",
        "status": status,
        "acl": ["public"],
        "dataset_mdata": mdata,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "doi": "10.18126/abcd-1234",
        "total_bytes": 4096,
        "file_count": 3,
    }
    record.update(extra)
    return record


def _client_for(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), timeout=1.0)


def _status_handler(mapping: dict, default: int = 200):
    """MockTransport handler driven by a {url_suffix: status_or_exc} map."""

    def handler(request: httpx.Request) -> httpx.Response:
        for suffix, outcome in mapping.items():
            if str(request.url).endswith(suffix):
                if isinstance(outcome, dict):
                    outcome = outcome.get(request.method, 200)
                if isinstance(outcome, Exception):
                    raise outcome
                return httpx.Response(int(outcome))
        return httpx.Response(default)

    return handler


# ---------------------------------------------------------------------------
# URI -> anonymous HTTPS
# ---------------------------------------------------------------------------

class TestPublicHttpsUrl:
    def test_https_passes_through(self):
        url = "https://data.materialsdatafacility.org/mdf_open/x/data.csv"
        assert link_health.public_https_url(url) == url

    def test_ncsa_globus_uri_becomes_the_public_https_form(self):
        assert link_health.public_https_url(f"globus://{NCSA}/mdf_open/ds/data.csv") == (
            "https://data.materialsdatafacility.org/mdf_open/ds/data.csv"
        )

    def test_globus_uri_on_another_collection_has_no_public_form(self):
        other = "11111111-2222-3333-4444-555555555555"
        assert link_health.public_https_url(f"globus://{other}/some/path") is None

    def test_non_https_schemes_and_junk_are_unverifiable(self):
        for uri in (
            "http://insecure.example.com/data.csv",
            "ftp://old.example.com/data",
            f"globus://{NCSA}",  # no path
            "/local/path",
            "",
            None,
        ):
            assert link_health.public_https_url(uri) is None


class TestCollectCandidates:
    def test_download_url_is_probed_first(self):
        record = _record(
            download_url="https://example.com/archive.zip",
            data_sources=["https://example.com/data.csv"],
        )
        candidates = link_health.collect_candidates(record)
        assert [uri for uri, _ in candidates] == [
            "https://example.com/archive.zip",
            "https://example.com/data.csv",
        ]

    def test_duplicates_are_probed_once(self):
        record = _record(
            download_url="https://example.com/a.zip",
            data_sources=["https://example.com/a.zip", "https://example.com/b.csv"],
        )
        assert len(link_health.collect_candidates(record)) == 2

    def test_never_more_than_five_urls_per_record(self):
        record = _record(
            data_sources=[f"https://example.com/f{i}.csv" for i in range(40)],
        )
        assert len(link_health.collect_candidates(record)) == link_health.MAX_URLS_PER_RECORD == 5


# ---------------------------------------------------------------------------
# Probing one URL
# ---------------------------------------------------------------------------

class TestProbeUrl:
    def test_200_head_is_ok(self):
        with _client_for(_status_handler({}, default=200)) as client:
            check = link_health.probe_url("https://example.com/a.csv", client=client)
        assert check["ok"] is True
        assert check["http_status"] == 200
        assert check["state"] == link_health.CHECK_OK
        assert check["method"] == "HEAD"
        assert isinstance(check["ms"], int)

    def test_head_refused_but_get_ok_is_ok(self):
        """Plenty of hosts answer HEAD with 405/403 and GET fine."""
        handler = _status_handler({"a.csv": {"HEAD": 405, "GET": 206}})
        with _client_for(handler) as client:
            check = link_health.probe_url("https://example.com/a.csv", client=client)
        assert check["ok"] is True
        assert check["http_status"] == 206
        assert check["method"] == "GET"

    def test_404_on_both_methods_is_broken(self):
        with _client_for(_status_handler({}, default=404)) as client:
            check = link_health.probe_url("https://example.com/gone.csv", client=client)
        assert check["ok"] is False
        assert check["http_status"] == 404
        assert check["state"] == link_health.CHECK_BROKEN

    def test_timeout_is_unverifiable_never_broken(self):
        """A false 'broken' is worse than silence — transport noise is not proof."""
        boom = httpx.ConnectTimeout("timed out")
        with _client_for(_status_handler({"slow.csv": boom})) as client:
            check = link_health.probe_url("https://example.com/slow.csv", client=client)
        assert check["state"] == link_health.CHECK_UNVERIFIABLE
        assert check["ok"] is False
        assert check["http_status"] is None
        assert "ConnectTimeout" in check["error"]

    def test_no_float_ever_reaches_the_store(self):
        """DynamoDB rejects floats, so latencies must be ints by construction."""
        with _client_for(_status_handler({}, default=200)) as client:
            check = link_health.probe_url("https://example.com/a.csv", client=client)
        assert not isinstance(check["ms"], float)


class TestAggregateStatus:
    def _c(self, state):
        return {"state": state}

    def test_no_checks_is_unverifiable(self):
        assert link_health.aggregate_status([]) == "unverifiable"

    def test_all_ok(self):
        assert link_health.aggregate_status([self._c("ok"), self._c("ok")]) == "ok"

    def test_all_broken(self):
        assert link_health.aggregate_status([self._c("broken")]) == "broken"

    def test_mixed_ok_and_broken_is_degraded(self):
        assert link_health.aggregate_status([self._c("ok"), self._c("broken")]) == "degraded"

    def test_unverifiable_alone_is_unverifiable(self):
        assert link_health.aggregate_status([self._c("unverifiable")]) == "unverifiable"

    def test_unverifiable_does_not_downgrade_a_resolving_dataset(self):
        """A consent-gated Globus collection is honest, not bad."""
        assert link_health.aggregate_status(
            [self._c("ok"), self._c("unverifiable")]
        ) == "ok"

    def test_unverifiable_does_not_rescue_a_broken_one(self):
        assert link_health.aggregate_status(
            [self._c("broken"), self._c("unverifiable")]
        ) == "broken"


# ---------------------------------------------------------------------------
# check_record
# ---------------------------------------------------------------------------

class TestCheckRecord:
    def test_all_sources_resolve(self):
        record = _record(data_sources=["https://example.com/a.csv", "https://example.com/b.csv"])
        with _client_for(_status_handler({}, default=200)) as client:
            result = link_health.check_record(record, client=client)
        assert result["status"] == "ok"
        assert len(result["checks"]) == 2
        assert result["checked_at"].endswith("Z")

    def test_one_dead_source_degrades_the_dataset(self):
        record = _record(
            data_sources=["https://example.com/a.csv", "https://example.com/gone.csv"],
        )
        handler = _status_handler({"gone.csv": 404})
        with _client_for(handler) as client:
            result = link_health.check_record(record, client=client)
        assert result["status"] == "degraded"
        broken = [c for c in result["checks"] if c["state"] == "broken"]
        assert [c["url"] for c in broken] == ["https://example.com/gone.csv"]

    def test_globus_uri_on_a_foreign_collection_is_reported_unverifiable(self):
        record = _record(data_sources=["globus://11111111-2222-3333-4444-555555555555/p/"])
        result = link_health.check_record(record, client=_client_for(_status_handler({})))
        assert result["status"] == "unverifiable"
        assert result["checks"][0]["state"] == "unverifiable"
        assert result["checks"][0]["http_status"] is None

    def test_ncsa_globus_uri_is_probed_over_https_and_reports_both_forms(self):
        uri = f"globus://{NCSA}/mdf_open/ds/data.csv"
        record = _record(data_sources=[uri])
        with _client_for(_status_handler({}, default=200)) as client:
            result = link_health.check_record(record, client=client)
        assert result["status"] == "ok"
        check = result["checks"][0]
        assert check["url"].startswith("https://data.materialsdatafacility.org/")
        assert check["source_uri"] == uri

    def test_a_record_with_no_sources_is_unverifiable_not_ok(self):
        result = link_health.check_record(_record(data_sources=[]))
        assert result["status"] == "unverifiable"
        assert result["checks"] == []

    def test_timeouts_never_mark_a_dataset_broken(self):
        record = _record(data_sources=["https://example.com/slow.csv"])
        handler = _status_handler({"slow.csv": httpx.ReadTimeout("nope")})
        with _client_for(handler) as client:
            result = link_health.check_record(record, client=client)
        assert result["status"] == "unverifiable"


class TestPublicProjection:
    def test_only_status_and_checked_at_are_public(self):
        record = _record(link_health={
            "status": "degraded",
            "checked_at": "2026-09-01T00:00:00Z",
            "checks": [{"url": "https://internal/x", "state": "broken", "error": "boom"}],
        })
        assert link_health.public_link_health(record) == {
            "status": "degraded",
            "checked_at": "2026-09-01T00:00:00Z",
        }

    def test_missing_or_malformed_blocks_read_as_none(self):
        assert link_health.public_link_health(_record()) is None
        assert link_health.public_link_health(_record(link_health="not json")) is None
        assert link_health.public_link_health(_record(link_health={"status": "great"})) is None

    def test_a_json_string_block_is_tolerated(self):
        raw = json.dumps({"status": "ok", "checked_at": "2026-09-01T00:00:00Z", "checks": []})
        assert link_health.parse_link_health(_record(link_health=raw))["status"] == "ok"


# ---------------------------------------------------------------------------
# Store persistence (both backends go through the same helper)
# ---------------------------------------------------------------------------

class TestStorePersistence:
    def test_update_link_health_round_trips_as_a_dict(self, env):
        from v2.store import get_store

        store = get_store()
        store.put_submission(_record())
        payload = {
            "status": "ok",
            "checked_at": "2026-09-02T00:00:00Z",
            "checks": [{"url": "https://x/a.csv", "http_status": 200, "ok": True, "ms": 12}],
        }
        store.update_link_health("ds-1", "1.0", payload)

        got = store.get_submission("ds-1", "1.0")
        assert isinstance(got["link_health"], dict)
        assert got["link_health"]["status"] == "ok"
        assert got["link_health"]["checks"][0]["ms"] == 12
        # Flat column so the sweep's staleness filter is not a JSON parse.
        assert got["link_health_checked_at"] == "2026-09-02T00:00:00Z"

    def test_float_latencies_are_coerced_to_int_for_dynamo(self, env):
        from v2.store import get_store

        store = get_store()
        store.put_submission(_record())
        store.update_link_health("ds-1", "1.0", {
            "status": "ok",
            "checked_at": "2026-09-02T00:00:00Z",
            "checks": [{"url": "https://x", "ms": 12.7}],
        })
        ms = store.get_submission("ds-1", "1.0")["link_health"]["checks"][0]["ms"]
        assert ms == 12 and isinstance(ms, int)

    def test_a_whole_record_upsert_does_not_drop_link_health(self, env):
        """The sqlite column list is fixed — link_health must be in it."""
        from v2.store import get_store

        store = get_store()
        store.put_submission(_record())
        store.update_link_health("ds-1", "1.0", {
            "status": "broken", "checked_at": "2026-09-02T00:00:00Z", "checks": [],
        })

        record = store.get_submission("ds-1", "1.0")
        record["updated_at"] = "2026-09-03T00:00:00Z"
        store.upsert_submission(record)

        assert store.get_submission("ds-1", "1.0")["link_health"]["status"] == "broken"

    def test_records_without_link_health_do_not_grow_a_null_key(self, env):
        from v2.store import get_store

        store = get_store()
        store.put_submission(_record())
        assert "link_health" not in store.get_submission("ds-1", "1.0")


# ---------------------------------------------------------------------------
# Async jobs
# ---------------------------------------------------------------------------

class TestLinkHealthJob:
    def test_job_probes_and_persists(self, env, monkeypatch):
        from v2 import async_jobs
        from v2.store import get_store

        store = get_store()
        store.put_submission(_record(data_sources=["https://example.com/a.csv"]))

        monkeypatch.setattr(
            link_health,
            "check_record",
            lambda record, **kw: {
                "status": "ok", "checked_at": "2026-09-02T00:00:00Z", "checks": [],
            },
        )
        result = async_jobs.process_job(
            async_jobs.JOB_LINK_HEALTH, {"source_id": "ds-1", "version": "1.0"},
        )
        assert result["success"] is True
        assert result["status"] == "ok"
        assert store.get_submission("ds-1", "1.0")["link_health"]["status"] == "ok"

    def test_unpublished_records_are_skipped_not_probed(self, env, monkeypatch):
        from v2 import async_jobs
        from v2.store import get_store

        get_store().put_submission(_record(status="pending_curation"))
        monkeypatch.setattr(
            link_health, "check_record", lambda *a, **k: pytest.fail("must not probe"),
        )
        result = async_jobs.process_job(
            async_jobs.JOB_LINK_HEALTH, {"source_id": "ds-1", "version": "1.0"},
        )
        assert result["success"] is False
        assert result["skipped"] == "not_published"

    def test_missing_record_reports_instead_of_raising(self, env):
        from v2 import async_jobs

        result = async_jobs.process_job(
            async_jobs.JOB_LINK_HEALTH, {"source_id": "nope", "version": "1.0"},
        )
        assert result["success"] is False
        assert "not found" in result["error"]


class TestLinkHealthSweep:
    def _seed(self, count: int = 3, **kw):
        from v2.store import get_store

        store = get_store()
        for i in range(count):
            store.put_submission(_record(source_id=f"ds-{i}", **kw))
        return store

    def test_sweep_enqueues_one_job_per_published_record(self, env, monkeypatch):
        from v2 import async_jobs

        self._seed(3)
        enqueued = []
        monkeypatch.setattr(
            async_jobs,
            "enqueue_link_health_job",
            lambda source_id, version: enqueued.append((source_id, version)),
        )
        result = async_jobs.process_job(async_jobs.JOB_LINK_HEALTH_SWEEP, {})
        assert result["success"] is True
        assert result["enqueued"] == 3
        assert sorted(enqueued) == [("ds-0", "1.0"), ("ds-1", "1.0"), ("ds-2", "1.0")]

    def test_unpublished_and_superseded_records_are_not_swept(self, env, monkeypatch):
        from v2 import async_jobs
        from v2.store import get_store

        store = get_store()
        store.put_submission(_record(source_id="live"))
        store.put_submission(_record(source_id="draft", status="pending_curation"))
        old = _record(source_id="old")
        old["dataset_mdata"]["latest"] = False
        store.put_submission(old)

        enqueued = []
        monkeypatch.setattr(
            async_jobs,
            "enqueue_link_health_job",
            lambda source_id, version: enqueued.append(source_id),
        )
        async_jobs.process_job(async_jobs.JOB_LINK_HEALTH_SWEEP, {})
        assert enqueued == ["live"]

    def test_recently_checked_records_are_skipped(self, env, monkeypatch):
        from v2 import async_jobs

        store = self._seed(2)
        store.update_link_health("ds-0", "1.0", {
            "status": "ok",
            "checked_at": _iso(datetime.now(timezone.utc) - timedelta(minutes=5)),
            "checks": [],
        })

        enqueued = []
        monkeypatch.setattr(
            async_jobs,
            "enqueue_link_health_job",
            lambda source_id, version: enqueued.append(source_id),
        )
        result = async_jobs.process_job(async_jobs.JOB_LINK_HEALTH_SWEEP, {})
        assert result["skipped_current"] == 1
        assert enqueued == ["ds-1"]

    def test_a_stale_check_is_refreshed(self, env, monkeypatch):
        from v2 import async_jobs

        store = self._seed(1)
        store.update_link_health("ds-0", "1.0", {
            "status": "ok",
            "checked_at": _iso(datetime.now(timezone.utc) - timedelta(days=30)),
            "checks": [],
        })

        enqueued = []
        monkeypatch.setattr(
            async_jobs,
            "enqueue_link_health_job",
            lambda source_id, version: enqueued.append(source_id),
        )
        result = async_jobs.process_job(async_jobs.JOB_LINK_HEALTH_SWEEP, {})
        assert result["skipped_current"] == 0
        assert enqueued == ["ds-0"]

    def test_force_reprobes_current_records(self, env, monkeypatch):
        from v2 import async_jobs

        store = self._seed(1)
        store.update_link_health("ds-0", "1.0", {
            "status": "ok", "checked_at": _iso(datetime.now(timezone.utc)), "checks": [],
        })

        enqueued = []
        monkeypatch.setattr(
            async_jobs,
            "enqueue_link_health_job",
            lambda source_id, version: enqueued.append(source_id),
        )
        result = async_jobs.process_job(
            async_jobs.JOB_LINK_HEALTH_SWEEP, {"force": True},
        )
        assert result["enqueued"] == 1 and enqueued == ["ds-0"]

    def test_limit_caps_the_fan_out(self, env, monkeypatch):
        from v2 import async_jobs

        self._seed(5)
        monkeypatch.setattr(async_jobs, "enqueue_link_health_job", lambda *a: None)
        result = async_jobs.process_job(
            async_jobs.JOB_LINK_HEALTH_SWEEP, {"limit": 2},
        )
        assert result["enqueued"] == 2

    def test_one_failing_enqueue_does_not_abort_the_sweep(self, env, monkeypatch):
        from v2 import async_jobs

        self._seed(3)

        def flaky(source_id, version):
            if source_id == "ds-1":
                raise RuntimeError("SQS down")

        monkeypatch.setattr(async_jobs, "enqueue_link_health_job", flaky)
        result = async_jobs.process_job(async_jobs.JOB_LINK_HEALTH_SWEEP, {})
        assert result["enqueued"] == 2
        assert [f["source_id"] for f in result["enqueue_failures"]] == ["ds-1"]


# ---------------------------------------------------------------------------
# Admin endpoints
# ---------------------------------------------------------------------------

class TestAdminLinkHealth:
    def test_both_routes_require_a_curator(self, env, strict_curators):
        client = TestClient(app)
        assert client.post("/admin/link-health/run", headers=HEADERS).status_code == 403
        assert client.get("/admin/link-health/summary", headers=HEADERS).status_code == 403

    def test_run_enqueues_the_sweep(self, env, monkeypatch):
        from v2.app.routers import admin as admin_router  # noqa: F401
        from v2 import async_jobs

        calls = []
        monkeypatch.setattr(
            async_jobs,
            "enqueue_link_health_sweep_job",
            lambda force=False, limit=None: calls.append((force, limit))
            or {"mode": "inline", "queued": False, "job_type": "link_health_sweep"},
        )
        client = TestClient(app)
        resp = client.post(
            "/admin/link-health/run", headers=CURATOR_HEADERS, json={"force": True, "limit": 10},
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["success"] is True
        assert body["sweep_job"]["job_type"] == "link_health_sweep"
        assert calls == [(True, 10)]

    def test_run_with_no_body_defaults_to_a_non_forced_sweep(self, env, monkeypatch):
        from v2 import async_jobs

        calls = []
        monkeypatch.setattr(
            async_jobs,
            "enqueue_link_health_sweep_job",
            lambda force=False, limit=None: calls.append((force, limit)) or {"queued": True},
        )
        client = TestClient(app)
        assert client.post("/admin/link-health/run", headers=CURATOR_HEADERS).status_code == 200
        assert calls == [(False, None)]

    def test_summary_counts_by_status_and_lists_broken(self, env):
        from v2.store import get_store

        store = get_store()
        for source_id, status in (
            ("ok-1", "ok"),
            ("ok-2", "ok"),
            ("deg-1", "degraded"),
            ("broke-1", "broken"),
            ("unv-1", "unverifiable"),
        ):
            store.put_submission(_record(source_id=source_id))
            store.update_link_health(source_id, "1.0", {
                "status": status,
                "checked_at": f"2026-09-0{1 + len(source_id) % 5}T00:00:00Z",
                "checks": [
                    {"url": f"https://example.com/{source_id}", "state": "broken",
                     "http_status": 404, "ok": False, "ms": 3},
                ] if status in ("broken", "degraded") else [],
            })
        store.put_submission(_record(source_id="never-checked"))

        client = TestClient(app)
        body = client.get("/admin/link-health/summary", headers=CURATOR_HEADERS).json()
        assert body["success"] is True
        assert body["published_total"] == 6
        assert body["checked"] == 5
        assert body["unchecked"] == 1
        assert body["by_status"] == {
            "ok": 2, "degraded": 1, "broken": 1, "unverifiable": 1,
        }
        # degraded counts as work to do, so it shows up alongside broken.
        assert body["broken_total"] == 2
        assert {item["source_id"] for item in body["broken_sample"]} == {"deg-1", "broke-1"}
        assert body["broken_sample"][0]["failed_checks"][0]["http_status"] == 404

    def test_summary_caps_the_broken_sample_at_fifty(self, env):
        from v2.store import get_store

        store = get_store()
        for i in range(60):
            source_id = f"broke-{i:03d}"
            store.put_submission(_record(source_id=source_id))
            store.update_link_health(source_id, "1.0", {
                "status": "broken", "checked_at": f"2026-09-01T00:00:{i:02d}Z", "checks": [],
            })

        client = TestClient(app)
        body = client.get("/admin/link-health/summary", headers=CURATOR_HEADERS).json()
        assert body["broken_total"] == 60
        assert len(body["broken_sample"]) == 50
        # Most recently checked first.
        assert body["broken_sample"][0]["source_id"] == "broke-059"

    def test_summary_is_empty_but_successful_with_no_data(self, env):
        client = TestClient(app)
        body = client.get("/admin/link-health/summary", headers=CURATOR_HEADERS).json()
        assert body["published_total"] == 0
        assert body["broken_sample"] == []
        assert body["by_status"] == {
            "ok": 0, "degraded": 0, "broken": 0, "unverifiable": 0,
        }


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------

class TestCardExposure:
    def test_dataset_card_shows_status_and_date_only(self):
        from v2.dataset_card import build_dataset_card

        card = build_dataset_card(_record(link_health={
            "status": "degraded",
            "checked_at": "2026-09-01T00:00:00Z",
            "checks": [{"url": "https://internal/secret", "state": "broken"}],
        }))
        assert card["link_health"] == {
            "status": "degraded", "checked_at": "2026-09-01T00:00:00Z",
        }
        assert "checks" not in card["link_health"]

    def test_dataset_card_omits_the_key_when_never_checked(self):
        from v2.dataset_card import build_dataset_card

        assert "link_health" not in build_dataset_card(_record())

    def test_card_endpoint_serves_link_health(self, env):
        from v2.store import get_store

        store = get_store()
        store.put_submission(_record())
        store.update_link_health("ds-1", "1.0", {
            "status": "ok", "checked_at": "2026-09-01T00:00:00Z", "checks": [],
        })

        client = TestClient(app)
        card = client.get("/card/ds-1").json()["card"]
        assert card["link_health"]["status"] == "ok"


class TestAgentCard:
    def test_builder_shape(self):
        from v2.dataset_card import build_agent_card

        record = _record(
            data_sources=[f"globus://{NCSA}/mdf_open/ds/data.csv"],
            download_url="https://example.com/ds.zip",
            link_health={"status": "ok", "checked_at": "2026-09-01T00:00:00Z", "checks": []},
        )
        card = build_agent_card(record)

        assert card["source_id"] == "ds-1"
        assert card["version"] == "1.0"
        assert card["title"] == "Fe-Al alloy DFT"
        assert card["doi"] == "10.18126/abcd-1234"
        assert card["license"]["identifier"] == "CC-BY-4.0"
        assert card["organization"] == "MDF Open"
        assert card["authors"] == [
            {"name": "Ada Curie", "orcid": "0000-0002-1825-0097"},
        ]
        assert card["keywords"] == ["alloys", "dft"]
        assert card["size_bytes"] == 4096
        assert card["file_count"] == 3
        assert card["download_url"] == "https://example.com/ds.zip"
        assert card["data_sources"] == [f"globus://{NCSA}/mdf_open/ds/data.csv"]
        assert card["link_health"] == {"status": "ok", "checked_at": "2026-09-01T00:00:00Z"}
        assert "Curie" in card["citation_apa"]
        assert card["urls"]["landing"].endswith("/detail/ds-1?version=1.0")
        assert card["urls"]["citation"] == "/citation/ds-1"
        assert card["urls"]["files"] == "/preview/ds-1/files"
        # No ml block and no profile on this record.
        assert "ml" not in card and "columns" not in card

    def test_description_is_capped(self):
        from v2.dataset_card import AGENT_DESCRIPTION_MAX_CHARS, build_agent_card

        record = _record()
        record["dataset_mdata"]["description"] = "word " * 500
        card = build_agent_card(record)
        assert len(card["description"]) <= AGENT_DESCRIPTION_MAX_CHARS
        assert card["description"].endswith("…")

    def test_short_description_is_untouched(self):
        from v2.dataset_card import build_agent_card

        card = build_agent_card(_record())
        assert card["description"] == (
            "Density functional theory calculations of Fe-Al alloys."
        )

    def test_loading_recipe_uses_mdf_clone_by_default(self):
        from v2.dataset_card import build_agent_card

        recipe = build_agent_card(_record())["loading_recipe"]
        assert "MDFAgent" in recipe["python"]
        assert 'agent.clone("ds-1", version="1.0")' in recipe["python"]
        assert "mdf clone ds-1" in recipe["shell"]

    def test_loading_recipe_uses_foundry_for_ml_ready_records(self):
        from v2.dataset_card import build_agent_card

        record = _record()
        record["dataset_mdata"]["ml"] = {
            "data_format": "csv",
            "task_type": ["regression"],
            "short_name": "fe_al",
            "splits": [{"type": "train", "path": "train.csv", "n_items": 100}],
            "keys": [{"name": "band_gap", "role": "target"}],
        }
        card = build_agent_card(record)
        assert "from foundry import Foundry" in card["loading_recipe"]["python"]
        assert 'f.get_dataset("10.18126/abcd-1234", version="1.0")' in (
            card["loading_recipe"]["python"]
        )
        assert card["ml"]["short_name"] == "fe_al"
        assert card["ml"]["splits"] == [
            {"type": "train", "path": "train.csv", "n_items": 100},
        ]
        assert card["ml"]["keys"] == [{"name": "band_gap", "role": "target"}]

    def test_columns_come_from_the_dataset_profile(self):
        from v2.dataset_card import build_agent_card

        record = _record(dataset_profile={
            "total_files": 2,
            "total_bytes": 900,
            "files": [
                {"filename": "readme.txt", "columns": []},
                {"filename": "data.csv", "n_rows": 10, "columns": [
                    {"name": "formula", "dtype": "str"},
                    {"name": "band_gap", "dtype": "float64"},
                ]},
            ],
        })
        card = build_agent_card(record)
        assert card["columns"] == [
            {"name": "formula", "dtype": "str"},
            {"name": "band_gap", "dtype": "float64"},
        ]

    def test_size_and_count_fall_back_to_the_profile(self):
        from v2.dataset_card import build_agent_card

        record = _record()
        record.pop("total_bytes")
        record.pop("file_count")
        record["dataset_profile"] = {"total_files": 7, "total_bytes": 12345, "files": []}
        card = build_agent_card(record)
        assert card["file_count"] == 7
        assert card["size_bytes"] == 12345

    def test_endpoint_returns_the_agent_card(self, env):
        from v2.store import get_store

        get_store().put_submission(_record())
        client = TestClient(app)
        resp = client.get("/card/ds-1", params={"format": "agent"})
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["success"] is True
        assert body["format"] == "agent"
        assert body["card"]["loading_recipe"]["python"]
        # The agent projection, not the UI one.
        assert "stats" not in body["card"]
        assert "links" not in body["card"]

    def test_default_format_is_unchanged(self, env):
        from v2.store import get_store

        get_store().put_submission(_record())
        client = TestClient(app)
        body = client.get("/card/ds-1").json()
        assert "stats" in body["card"] and "links" in body["card"]
        assert "format" not in body

    def test_unknown_format_is_rejected(self, env):
        from v2.store import get_store

        get_store().put_submission(_record())
        client = TestClient(app)
        assert client.get("/card/ds-1", params={"format": "yaml"}).status_code == 400

    def test_agent_format_honours_dataset_visibility(self, env, strict_curators):
        """Same gate as the normal card: a restricted dataset 404s for others."""
        from v2.store import get_store

        get_store().put_submission(_record(acl=["urn:globus:auth:identity:someone-else"]))
        client = TestClient(app)
        assert client.get(
            "/card/ds-1", params={"format": "agent"}, headers={"X-User-Id": "stranger"},
        ).status_code == 404
        # ...and the owner still sees it.
        resp = client.get("/card/ds-1", params={"format": "agent"}, headers=HEADERS)
        assert resp.status_code == 200, resp.json()

    def test_agent_format_404s_for_unpublished_datasets(self, env, monkeypatch):
        from v2.store import get_store

        # Owners and curators may read their own pending record (GAP-3); any
        # other caller must still get a 404.
        monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
        get_store().put_submission(_record(status="pending_curation"))
        client = TestClient(app)
        resp = client.get(
            "/card/ds-1", params={"format": "agent"}, headers={"X-User-Id": "not-the-owner"}
        )
        assert resp.status_code == 404
