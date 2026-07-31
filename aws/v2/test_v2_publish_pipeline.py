"""Tests for the v2 publication pipeline.

Covers:
- Data source format validation
- Status transitions (pending_curation on submit)
- Publish pipeline (approve → DOI + search ingest + published)
- Search client (mock)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.async_jobs import run_sqlite_worker_once
from v2.storage import reset_storage_backend


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Standard test environment with SQLite store and inline dispatch."""
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
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


@pytest.fixture()
def sqlite_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Test environment with SQLite async dispatch (queued jobs)."""
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
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


HEADERS = {"X-User-Id": "test-user"}

VALID_SUBMISSION = {
    "title": "Test Dataset",
    "authors": [{"name": "Test User"}],
    "data_sources": ["https://example.com/data.csv"],
}


# =========================================================================
# Status transitions
# =========================================================================


class TestStatusTransitions:
    """Submissions land as pending_curation and follow the v2 lifecycle."""

    def test_submit_lands_as_pending_curation(self, env):
        client = TestClient(app)
        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        status = client.get(f"/status/{source_id}")
        assert status.json()["submission"]["status"] == "pending_curation"

    def test_allowed_status_updates(self, env):
        client = TestClient(app)
        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        source_id = resp.json()["source_id"]

        for target in ["approved", "published", "rejected"]:
            r = client.post(
                "/status/update",
                headers=HEADERS,
                json={"source_id": source_id, "version": "1.0", "status": target},
            )
            assert r.status_code == 200, f"Failed to set status to {target}"

    def test_disallowed_status_update(self, env):
        client = TestClient(app)
        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        source_id = resp.json()["source_id"]

        r = client.post(
            "/status/update",
            headers=HEADERS,
            json={"source_id": source_id, "version": "1.0", "status": "processing"},
        )
        assert r.status_code == 400


# =========================================================================
# Data source validation
# =========================================================================


class TestDataSourceValidation:
    """Server-side format validation of data_sources."""

    def test_valid_globus_uri(self, env):
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "data_sources": ["globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/tmp/data.csv"],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200

    def test_valid_https_url(self, env):
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "data_sources": ["https://example.com/data.csv"],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200

    def test_valid_stream_uri(self, env):
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "data_sources": ["stream://my-stream-id"],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200

    def test_globus_uri_invalid_uuid(self, env):
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "data_sources": ["globus://not-a-uuid/path/data"],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 400
        assert "invalid collection UUID" in resp.json()["detail"]

    def test_globus_uri_missing_path(self, env):
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "data_sources": ["globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 400
        assert "missing path" in resp.json()["detail"]

    def test_stream_uri_empty_id(self, env):
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "data_sources": ["stream://"],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 400
        assert "empty ID" in resp.json()["detail"]

    def test_mixed_valid_sources(self, env):
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "data_sources": [
                "globus://82f1b5c6-6e9b-11e5-ba47-22000b92c6ec/path/data.csv",
                "https://zenodo.org/record/12345/files/data.zip",
                "stream://my-stream",
            ],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200


# =========================================================================
# Publish pipeline (inline dispatch)
# =========================================================================


class TestPublishPipelineInline:
    """Full publish pipeline with inline (synchronous) dispatch."""

    def test_approve_triggers_publish_and_doi(self, env):
        """Approve with mint_doi=true → published status + mock DOI."""
        client = TestClient(app)

        # Submit
        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        # Approve (inline dispatch → runs synchronously)
        approve = client.post(
            f"/curation/{source_id}/approve",
            headers=HEADERS,
            json={"mint_doi": True},
        )
        assert approve.status_code == 200
        body = approve.json()
        assert body["success"] is True
        # Inline dispatch: publish job ran immediately
        assert body.get("publish_job", {}).get("mode") == "inline"
        assert body["status"] == "published"

        # Verify final state
        status = client.get(f"/status/{source_id}")
        sub = status.json()["submission"]
        assert sub["status"] == "published"
        assert sub.get("doi") is not None
        assert sub.get("published_at") is not None

    def test_approve_without_doi(self, env):
        """Approve with mint_doi=false → published but no DOI."""
        client = TestClient(app)

        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        source_id = resp.json()["source_id"]

        approve = client.post(
            f"/curation/{source_id}/approve",
            headers=HEADERS,
            json={"mint_doi": False},
        )
        assert approve.status_code == 200
        assert approve.json()["status"] == "published"

        status = client.get(f"/status/{source_id}")
        sub = status.json()["submission"]
        assert sub["status"] == "published"
        assert sub.get("doi") is None  # No DOI minted

    def test_reject_does_not_publish(self, env):
        """Rejection does not trigger publish pipeline."""
        client = TestClient(app)

        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        source_id = resp.json()["source_id"]

        reject = client.post(
            f"/curation/{source_id}/reject",
            headers=HEADERS,
            json={"reason": "Incomplete metadata"},
        )
        assert reject.status_code == 200
        assert reject.json()["status"] == "rejected"

        status = client.get(f"/status/{source_id}")
        assert status.json()["submission"]["status"] == "rejected"

    def test_cannot_approve_non_pending(self, env):
        """Cannot approve a submission that is not pending_curation."""
        client = TestClient(app)

        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        source_id = resp.json()["source_id"]

        # Approve it first
        client.post(
            f"/curation/{source_id}/approve",
            headers=HEADERS,
            json={"mint_doi": False},
        )

        # Try to approve again
        second = client.post(
            f"/curation/{source_id}/approve",
            headers=HEADERS,
            json={"mint_doi": True},
        )
        assert second.status_code == 400


# =========================================================================
# Publish pipeline (SQLite async dispatch)
# =========================================================================


class TestPublishPipelineAsync:
    """Publish pipeline with SQLite async dispatch (queued then processed)."""

    def test_approve_queues_publish_job(self, sqlite_env):
        client = TestClient(app)

        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        approve = client.post(
            f"/curation/{source_id}/approve",
            headers=HEADERS,
            json={"mint_doi": True},
        )
        assert approve.status_code == 200
        body = approve.json()
        assert body["publish_job"]["queued"] is True
        assert body["publish_job"]["mode"] == "sqlite"
        assert body["status"] == "approved"  # Not yet published

        # Status should be approved (job not yet processed)
        status = client.get(f"/status/{source_id}")
        assert status.json()["submission"]["status"] == "approved"

        # Process the async job
        result = run_sqlite_worker_once(limit=10)
        assert result["processed"] >= 1
        assert result["failed"] == 0

        # Now it should be published
        status = client.get(f"/status/{source_id}")
        sub = status.json()["submission"]
        assert sub["status"] == "published"
        assert sub.get("doi") is not None


# =========================================================================
# Mock search client
# =========================================================================


class TestMockSearchClient:
    """Tests for the MockGlobusSearchClient."""

    def test_ingest_and_search(self):
        from v2.search_client import MockGlobusSearchClient

        client = MockGlobusSearchClient()

        submission = {
            "source_id": "test-dataset-1",
            "version": "1.0",
            "organization": "MDF",
            "dataset_mdata": '{"title":"Test Dataset","authors":[{"name":"Test"}],"data_sources":["https://example.com"]}',
        }

        # Ingest
        result = client.ingest(submission)
        assert result["success"] is True
        assert result.get("mock") is True

        # Search — should find it
        search = client.search("Test Dataset")
        assert search["total"] == 1
        assert search["results"][0]["source_id"] == "test-dataset-1"

        # Search — no match
        search = client.search("nonexistent")
        assert search["total"] == 0

    def test_delete_entry(self):
        from v2.search_client import MockGlobusSearchClient

        client = MockGlobusSearchClient()

        submission = {
            "source_id": "to-delete",
            "version": "1.0",
            "dataset_mdata": '{"title":"Delete Me","authors":[{"name":"X"}],"data_sources":[]}',
        }
        client.ingest(submission)
        assert client.search("Delete Me")["total"] == 1

        client.delete_entry("to-delete")
        assert client.search("Delete Me")["total"] == 0

    def test_search_pagination(self):
        from v2.search_client import MockGlobusSearchClient

        client = MockGlobusSearchClient()

        for i in range(5):
            client.ingest({
                "source_id": f"ds-{i}",
                "version": "1.0",
                "dataset_mdata": f'{{"title":"Dataset {i}","authors":[{{"name":"A"}}],"data_sources":[]}}',
            })

        result = client.search("Dataset", limit=2)
        assert result["total"] == 5
        assert len(result["results"]) == 2

        result2 = client.search("Dataset", limit=2, offset=2)
        assert len(result2["results"]) == 2

    def test_search_datasets_uses_mock(self, env):
        """Unpublished submissions should not leak through fallback search."""
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "title": "Search Fallback Unpublished Isolation Title",
        }

        # Submit a dataset so fallback has a matching unpublished record available.
        client.post("/submit", headers=HEADERS, json=submission)

        # Search via API
        search = client.get(
            "/search",
            params={"q": "Search Fallback Unpublished Isolation Title", "type": "datasets"},
        )
        assert search.status_code == 200
        body = search.json()
        assert body["total"] == 0
        assert body["results"] == []


# =========================================================================
# Domains and external import fields
# =========================================================================


class TestDomainsAndExternalImport:
    """Round-trip tests for domains and external import provenance fields."""

    def test_domains_round_trip(self, env):
        """Submit with domains, verify they appear in /status."""
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "domains": ["materials", "chemistry"],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        status = client.get(f"/status/{source_id}")
        mdata = status.json()["submission"]["dataset_mdata"]
        assert mdata["domains"] == ["materials", "chemistry"]

    def test_external_import_round_trip(self, env):
        """Submit with external import fields, verify they appear in /status."""
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "external_doi": "10.1234/ext-dataset",
            "external_url": "https://zenodo.org/record/12345",
            "external_source": "Zenodo",
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        status = client.get(f"/status/{source_id}")
        mdata = status.json()["submission"]["dataset_mdata"]
        assert mdata["external"]["doi"] == "10.1234/ext-dataset"
        assert mdata["external"]["url"] == "https://zenodo.org/record/12345"
        assert mdata["external"]["source"] == "Zenodo"

    def test_combined_domains_and_external_import(self, env):
        """Submit with both domains and external fields, verify all appear."""
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "domains": ["physics"],
            "external_doi": "10.5678/phys",
            "external_url": "https://arxiv.org/abs/2301.00001",
            "external_source": "arXiv",
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        status = client.get(f"/status/{source_id}")
        mdata = status.json()["submission"]["dataset_mdata"]
        assert mdata["domains"] == ["physics"]
        assert mdata["external"]["doi"] == "10.5678/phys"
        assert mdata["external"]["url"] == "https://arxiv.org/abs/2301.00001"
        assert mdata["external"]["source"] == "arXiv"

    def test_domains_empty_by_default(self, env):
        """Submit without domains, verify dataset_mdata has domains: []."""
        client = TestClient(app)
        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        status = client.get(f"/status/{source_id}")
        mdata = status.json()["submission"]["dataset_mdata"]
        assert mdata["domains"] == []

    def test_external_fields_absent_by_default(self, env):
        """Submit without external fields, verify external provenance is absent."""
        client = TestClient(app)
        resp = client.post("/submit", headers=HEADERS, json=VALID_SUBMISSION)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        status = client.get(f"/status/{source_id}")
        mdata = status.json()["submission"]["dataset_mdata"]
        assert mdata.get("external") is None

    def test_domains_in_search_index(self, env):
        """Approve with domains, verify GMetaEntry mdf block contains them."""
        import v2.search_client as sc

        # Reset mock singleton so we get a fresh client
        sc._mock_client = None

        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "domains": ["materials", "chemistry"],
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        # Approve (inline dispatch triggers search ingest)
        approve = client.post(
            f"/curation/{source_id}/approve",
            headers=HEADERS,
            json={"mint_doi": False},
        )
        assert approve.status_code == 200

        # Inspect the mock search client's stored entries
        mock_client = sc.get_search_client()
        assert len(mock_client._entries) >= 1
        entry = list(mock_client._entries.values())[0]
        mdf_block = entry["content"]["mdf"]
        assert mdf_block["domains"] == ["materials", "chemistry"]

    def test_external_import_still_mints_own_doi(self, env):
        """Submit with external_doi, approve with mint_doi=True, verify MDF mints its own DOI."""
        client = TestClient(app)
        submission = {
            **VALID_SUBMISSION,
            "external_doi": "10.9999/someone-elses-doi",
        }
        resp = client.post("/submit", headers=HEADERS, json=submission)
        assert resp.status_code == 200
        source_id = resp.json()["source_id"]

        approve = client.post(
            f"/curation/{source_id}/approve",
            headers=HEADERS,
            json={"mint_doi": True},
        )
        assert approve.status_code == 200
        assert approve.json()["status"] == "published"

        status = client.get(f"/status/{source_id}")
        sub = status.json()["submission"]
        # MDF minted its own DOI, distinct from the external one
        assert sub.get("doi") is not None
        assert sub["doi"] != "10.9999/someone-elses-doi"
        # External DOI is preserved in metadata
        mdata = sub["dataset_mdata"]
        assert mdata["external"]["doi"] == "10.9999/someone-elses-doi"
