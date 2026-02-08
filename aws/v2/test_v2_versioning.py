"""Tests for dataset versioning: DOI inheritance, version-specific DOIs, search metadata.

Covers:
- v1.0 publish → dataset DOI minted, stored as both doi and dataset_doi
- v1.1 publish (update, mint_doi=False) → inherits dataset_doi, updates DataCite metadata
- v1.2 publish (update, mint_doi=True) → version-specific DOI with -v suffix + IsVersionOf
- Search index includes dataset_doi and version_count
- dataset_doi propagated on submit with update=True
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.storage import reset_storage_backend


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


HEADERS = {"X-User-Id": "test-user"}

BASE_SUBMISSION = {
    "title": "Test Versioning Dataset",
    "authors": [{"name": "Test Author"}],
    "data_sources": ["https://example.com/data.csv"],
}


def _submit(client, extra=None):
    payload = {**BASE_SUBMISSION, **(extra or {})}
    resp = client.post("/submit", headers=HEADERS, json=payload)
    assert resp.status_code == 200, resp.json()
    return resp.json()


def _approve(client, source_id, mint_doi=True, version=None):
    body = {"mint_doi": mint_doi}
    if version:
        body["version"] = version
    resp = client.post(
        f"/curation/{source_id}/approve",
        headers=HEADERS,
        json=body,
    )
    assert resp.status_code == 200, resp.json()
    return resp.json()


def _status(client, source_id, version=None):
    url = f"/status/{source_id}"
    params = {"version": version} if version else {}
    resp = client.get(url, params=params)
    assert resp.status_code == 200
    return resp.json().get("submission", {})


class TestVersioningDOILifecycle:
    """Full versioning lifecycle: v1.0 → v1.1 (inherit) → v1.2 (version DOI)."""

    def test_v10_gets_dataset_doi(self, env):
        """v1.0 with mint_doi=True gets both doi and dataset_doi."""
        client = TestClient(app)

        result = _submit(client)
        source_id = result["source_id"]

        _approve(client, source_id, mint_doi=True)

        sub = _status(client, source_id, version="1.0")
        assert sub["status"] == "published"
        assert sub["doi"] is not None
        assert sub["dataset_doi"] is not None
        assert sub["doi"] == sub["dataset_doi"]

    def test_v11_inherits_dataset_doi(self, env):
        """v1.1 (update, mint_doi=False) inherits dataset_doi, no version doi."""
        client = TestClient(app)

        # Publish v1.0
        r1 = _submit(client)
        source_id = r1["source_id"]
        _approve(client, source_id, mint_doi=True)
        v10 = _status(client, source_id, version="1.0")
        dataset_doi = v10["dataset_doi"]

        # Submit v1.1
        r2 = _submit(client, extra={
            "title": "Updated Dataset v1.1",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        assert r2["version"] == "1.1"

        _approve(client, source_id, mint_doi=False, version="1.1")

        v11 = _status(client, source_id, version="1.1")
        assert v11["status"] == "published"
        # v1.1 should NOT have its own version doi (doi field stays None or unset)
        # but dataset_doi should be inherited from v1.0
        assert v11["dataset_doi"] == dataset_doi

    def test_v12_gets_version_specific_doi(self, env):
        """v1.2 (update, mint_doi=True) gets version-specific DOI different from dataset DOI."""
        client = TestClient(app)

        # Publish v1.0
        r1 = _submit(client)
        source_id = r1["source_id"]
        _approve(client, source_id, mint_doi=True)
        v10 = _status(client, source_id, version="1.0")
        dataset_doi = v10["dataset_doi"]

        # Submit and publish v1.1 (inherit)
        _submit(client, extra={
            "title": "Updated Dataset v1.1",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        _approve(client, source_id, mint_doi=False, version="1.1")

        # Submit and publish v1.2 (new version DOI)
        r3 = _submit(client, extra={
            "title": "Updated Dataset v1.2",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        assert r3["version"] == "1.2"
        _approve(client, source_id, mint_doi=True, version="1.2")

        v12 = _status(client, source_id, version="1.2")
        assert v12["status"] == "published"
        assert v12["doi"] is not None
        assert v12["dataset_doi"] == dataset_doi
        # Version DOI should be different from dataset DOI (has -v1.2 suffix)
        assert v12["doi"] != dataset_doi
        assert "-v1.2" in v12["doi"]

    def test_full_lifecycle_three_versions(self, env):
        """Full lifecycle: three versions with different DOI strategies."""
        client = TestClient(app)

        # v1.0: mint dataset DOI
        r1 = _submit(client)
        source_id = r1["source_id"]
        _approve(client, source_id, mint_doi=True)
        v10 = _status(client, source_id, version="1.0")

        # v1.1: inherit DOI
        _submit(client, extra={
            "title": "Updated v1.1",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        _approve(client, source_id, mint_doi=False, version="1.1")
        v11 = _status(client, source_id, version="1.1")

        # v1.2: version-specific DOI
        _submit(client, extra={
            "title": "Updated v1.2",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        _approve(client, source_id, mint_doi=True, version="1.2")
        v12 = _status(client, source_id, version="1.2")

        # Verify DOI structure
        dataset_doi = v10["dataset_doi"]
        assert v10["doi"] == dataset_doi  # v1.0: doi == dataset_doi
        assert v11["dataset_doi"] == dataset_doi  # v1.1: inherits dataset_doi
        assert v12["dataset_doi"] == dataset_doi  # v1.2: same dataset_doi
        assert v12["doi"] != dataset_doi  # v1.2: has own version DOI
        assert "-v1.2" in v12["doi"]


class TestVersioningDatasetDOIPropagation:
    """dataset_doi is propagated at submission time for update=True."""

    def test_dataset_doi_set_on_submit_update(self, env):
        """When submitting with update=True, dataset_doi is inherited from published versions."""
        client = TestClient(app)

        # Publish v1.0
        r1 = _submit(client)
        source_id = r1["source_id"]
        _approve(client, source_id, mint_doi=True)
        v10 = _status(client, source_id, version="1.0")
        dataset_doi = v10["dataset_doi"]

        # Submit v1.1 — check that dataset_doi is set before approval
        _submit(client, extra={
            "title": "Updated v1.1",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        v11_pending = _status(client, source_id, version="1.1")
        assert v11_pending["status"] == "pending_curation"
        assert v11_pending.get("dataset_doi") == dataset_doi


class TestVersioningSearchIndex:
    """Search index includes dataset_doi and version_count."""

    def test_search_entry_includes_dataset_doi(self, env):
        """build_gmeta_entry includes mdf.dataset_doi when set on submission."""
        from v2.search_client import MockGlobusSearchClient

        search_client = MockGlobusSearchClient()

        submission = {
            "source_id": "test-ds-1",
            "version": "1.1",
            "organization": "MDF",
            "dataset_doi": "10.99999/test-ds-1",
            "dataset_mdata": json.dumps({
                "title": "Test Dataset",
                "authors": [{"name": "Test"}],
                "data_sources": ["https://example.com"],
            }),
        }

        entry = search_client.build_gmeta_entry(submission, version_count=2)
        content = entry["content"]

        assert content["mdf"]["dataset_doi"] == "10.99999/test-ds-1"
        assert content["mdf"]["version_count"] == 2
        # dc.doi falls back to dataset_doi when no version-specific doi
        assert content["dc"]["doi"] == "10.99999/test-ds-1"

    def test_search_entry_prefers_version_doi(self, env):
        """dc.doi uses version-specific DOI when available."""
        from v2.search_client import MockGlobusSearchClient

        search_client = MockGlobusSearchClient()

        submission = {
            "source_id": "test-ds-1",
            "version": "1.2",
            "organization": "MDF",
            "doi": "10.99999/test-ds-1-v1.2",
            "dataset_doi": "10.99999/test-ds-1",
            "dataset_mdata": json.dumps({
                "title": "Test Dataset v1.2",
                "authors": [{"name": "Test"}],
                "data_sources": ["https://example.com"],
            }),
        }

        entry = search_client.build_gmeta_entry(submission, version_count=3)
        content = entry["content"]

        assert content["dc"]["doi"] == "10.99999/test-ds-1-v1.2"
        assert content["mdf"]["dataset_doi"] == "10.99999/test-ds-1"
        assert content["mdf"]["version_count"] == 3

    def test_search_entry_without_doi(self, env):
        """Entry without any DOI should not have dc.doi."""
        from v2.search_client import MockGlobusSearchClient

        search_client = MockGlobusSearchClient()

        submission = {
            "source_id": "no-doi-ds",
            "version": "1.0",
            "dataset_mdata": json.dumps({
                "title": "No DOI",
                "authors": [{"name": "Test"}],
                "data_sources": [],
            }),
        }

        entry = search_client.build_gmeta_entry(submission)
        assert "doi" not in entry["content"]["dc"]
        assert "dataset_doi" not in entry["content"]["mdf"]


class TestVersioningMockDataCite:
    """Tests for MockDataCiteClient version-aware methods."""

    def test_mock_mint_with_suffix_and_relations(self):
        from v2.datacite import MockDataCiteClient

        client = MockDataCiteClient(prefix="10.99999")

        # Mint dataset DOI
        r1 = client.mint_doi(
            source_id="test-ds",
            metadata={"titles": [{"title": "Test"}], "creators": [{"name": "X"}]},
        )
        assert r1["success"]
        assert r1["doi"] == "10.99999/test-ds"

        # Mint version DOI with custom suffix and relations
        r2 = client.mint_doi(
            source_id="test-ds",
            metadata={"titles": [{"title": "Test v1.2"}], "creators": [{"name": "X"}]},
            doi_suffix="test-ds-v1.2",
            related_identifiers=[{
                "relatedIdentifier": "10.99999/test-ds",
                "relatedIdentifierType": "DOI",
                "relationType": "IsVersionOf",
            }],
        )
        assert r2["success"]
        assert r2["doi"] == "10.99999/test-ds-v1.2"

        # Check stored data
        stored = client.get_doi("10.99999/test-ds-v1.2")
        assert stored["related_identifiers"][0]["relationType"] == "IsVersionOf"

    def test_mock_update_metadata(self):
        from v2.datacite import MockDataCiteClient

        client = MockDataCiteClient(prefix="10.99999")

        # Mint initial
        client.mint_doi(
            source_id="test-ds",
            metadata={"titles": [{"title": "Original"}], "creators": [{"name": "A"}]},
        )

        # Update metadata
        result = client.update_metadata(
            doi="10.99999/test-ds",
            metadata={"titles": [{"title": "Updated"}], "creators": [{"name": "B"}]},
            related_identifiers=[{
                "relatedIdentifier": "10.99999/test-ds-v1.2",
                "relatedIdentifierType": "DOI",
                "relationType": "HasVersion",
            }],
        )
        assert result["success"]
        assert result["updated"]

        stored = client.get_doi("10.99999/test-ds")
        assert stored["metadata"]["titles"][0]["title"] == "Updated"
        assert stored["related_identifiers"][0]["relationType"] == "HasVersion"


class TestVersioningCurationLogic:
    """Unit tests for version-aware _mint_doi_for_submission."""

    def test_first_version_mints_dataset_doi(self, env):
        from v2.curation import _mint_doi_for_submission

        submission = {
            "source_id": "first-ds",
            "version": "1.0",
            "dataset_mdata": json.dumps({
                "title": "First Dataset",
                "authors": [{"name": "Author"}],
                "data_sources": [],
            }),
        }

        result = _mint_doi_for_submission(submission, all_versions=[], mint_doi=True)
        assert result["success"]
        assert result["doi"] is not None
        assert result["dataset_doi"] == result["doi"]

    def test_subsequent_version_mint_false_updates_metadata(self, env):
        from v2.curation import _mint_doi_for_submission

        prior_versions = [
            {"version": "1.0", "status": "published", "doi": "10.99999/test-ds", "dataset_doi": "10.99999/test-ds"},
        ]

        submission = {
            "source_id": "test-ds",
            "version": "1.1",
            "dataset_mdata": json.dumps({
                "title": "Updated Dataset",
                "authors": [{"name": "New Author"}],
                "data_sources": [],
            }),
        }

        result = _mint_doi_for_submission(submission, all_versions=prior_versions, mint_doi=False)
        assert result["success"]
        assert result["dataset_doi"] == "10.99999/test-ds"
        assert result.get("doi") is None
        assert result.get("metadata_updated") is True

    def test_subsequent_version_mint_true_creates_version_doi(self, env):
        from v2.curation import _mint_doi_for_submission

        prior_versions = [
            {"version": "1.0", "status": "published", "doi": "10.99999/test-ds", "dataset_doi": "10.99999/test-ds"},
            {"version": "1.1", "status": "published", "dataset_doi": "10.99999/test-ds"},
        ]

        submission = {
            "source_id": "test-ds",
            "version": "1.2",
            "dataset_mdata": json.dumps({
                "title": "Version 1.2",
                "authors": [{"name": "Author v1.2"}],
                "data_sources": [],
            }),
        }

        result = _mint_doi_for_submission(submission, all_versions=prior_versions, mint_doi=True)
        assert result["success"]
        assert result["doi"] is not None
        assert "-v1.2" in result["doi"]
        assert result["dataset_doi"] == "10.99999/test-ds"
        assert result["doi"] != result["dataset_doi"]
