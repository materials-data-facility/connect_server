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
    """Full versioning lifecycle: v1.0 → v2.0 (inherit) → v3.0 (version DOI).

    Note: updates with new data_sources produce major version bumps.
    """

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

    def test_major_update_inherits_dataset_doi(self, env):
        """v2.0 (update with new data, mint_doi=False) inherits dataset_doi."""
        client = TestClient(app)

        # Publish v1.0
        r1 = _submit(client)
        source_id = r1["source_id"]
        _approve(client, source_id, mint_doi=True)
        v10 = _status(client, source_id, version="1.0")
        dataset_doi = v10["dataset_doi"]

        # Submit v2.0 (update with new data_sources → major bump)
        r2 = _submit(client, extra={
            "title": "Updated Dataset v2.0",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        assert r2["version"] == "2.0"

        _approve(client, source_id, mint_doi=False, version="2.0")

        v20 = _status(client, source_id, version="2.0")
        assert v20["status"] == "published"
        assert v20["dataset_doi"] == dataset_doi

    def test_major_update_gets_version_specific_doi(self, env):
        """v3.0 (update with data, mint_doi=True) gets version-specific DOI."""
        client = TestClient(app)

        # Publish v1.0
        r1 = _submit(client)
        source_id = r1["source_id"]
        _approve(client, source_id, mint_doi=True)
        v10 = _status(client, source_id, version="1.0")
        dataset_doi = v10["dataset_doi"]

        # Submit and publish v2.0
        _submit(client, extra={
            "title": "Updated Dataset v2.0",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        _approve(client, source_id, mint_doi=False, version="2.0")

        # Submit and publish v3.0 (new version DOI)
        r3 = _submit(client, extra={
            "title": "Updated Dataset v3.0",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        assert r3["version"] == "3.0"
        _approve(client, source_id, mint_doi=True, version="3.0")

        v30 = _status(client, source_id, version="3.0")
        assert v30["status"] == "published"
        assert v30["doi"] is not None
        assert v30["dataset_doi"] == dataset_doi
        assert v30["doi"] != dataset_doi
        assert "-v3.0" in v30["doi"]

    def test_full_lifecycle_three_versions(self, env):
        """Full lifecycle: three major versions with different DOI strategies."""
        client = TestClient(app)

        # v1.0: mint dataset DOI
        r1 = _submit(client)
        source_id = r1["source_id"]
        _approve(client, source_id, mint_doi=True)
        v10 = _status(client, source_id, version="1.0")

        # v2.0: inherit DOI (update with data → major)
        _submit(client, extra={
            "title": "Updated v2.0",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        _approve(client, source_id, mint_doi=False, version="2.0")
        v20 = _status(client, source_id, version="2.0")

        # v3.0: version-specific DOI (update with data → major)
        _submit(client, extra={
            "title": "Updated v3.0",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        _approve(client, source_id, mint_doi=True, version="3.0")
        v30 = _status(client, source_id, version="3.0")

        # Verify DOI structure
        dataset_doi = v10["dataset_doi"]
        assert v10["doi"] == dataset_doi
        assert v20["dataset_doi"] == dataset_doi
        assert v30["dataset_doi"] == dataset_doi
        assert v30["doi"] != dataset_doi
        assert "-v3.0" in v30["doi"]


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

        # Submit v2.0 (has data_sources → major bump) — check dataset_doi set before approval
        _submit(client, extra={
            "title": "Updated v2.0",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        v20_pending = _status(client, source_id, version="2.0")
        assert v20_pending["status"] == "pending_curation"
        assert v20_pending.get("dataset_doi") == dataset_doi


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


class TestVersioningSearchEntryOwnership:
    """The single search entry per dataset always describes the LATEST version.

    Regression tests for B-2: publishing a new version used to re-ingest the
    prior versions into the same subject, overwriting the entry just written for
    the new version with stale metadata carrying latest=false.
    """

    def _fresh_search_client(self):
        from v2.search_client import get_search_client, reset_search_client

        reset_search_client()
        return get_search_client()

    def test_new_version_publish_owns_search_entry(self, env):
        """After v2.0 publishes, the dataset's entry is v2.0 with latest=true."""
        search = self._fresh_search_client()
        client = TestClient(app)

        r1 = _submit(client)
        source_id = r1["source_id"]
        _approve(client, source_id, mint_doi=True)

        # v1.0 owns the entry while it is the only published version
        entry = search.get_entry(source_id)
        assert entry is not None
        assert entry["content"]["mdf"]["version"] == "1.0"
        assert entry["content"]["mdf"]["latest"] is True

        # Publish v2.0
        r2 = _submit(client, extra={
            "title": "Updated Dataset v2.0",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        assert r2["version"] == "2.0"
        _approve(client, source_id, mint_doi=False, version="2.0")

        # One entry per dataset, and it is the new version
        assert len(search._entries) == 1
        entry = search.get_entry(source_id)
        assert entry["content"]["mdf"]["version"] == "2.0"
        assert entry["content"]["mdf"]["latest"] is True
        assert entry["content"]["dc"]["title"] == "Updated Dataset v2.0"

        # The prior version is marked not-latest in the store
        v10 = _status(client, source_id, version="1.0")
        mdata10 = v10.get("dataset_mdata")
        if isinstance(mdata10, str):
            mdata10 = json.loads(mdata10)
        assert mdata10["latest"] is False

    def test_out_of_order_publish_does_not_clobber_entry(self, env):
        """Publishing an older version later must not overwrite the entry."""
        search = self._fresh_search_client()
        client = TestClient(app)

        r1 = _submit(client)
        source_id = r1["source_id"]

        r2 = _submit(client, extra={
            "title": "Updated Dataset v2.0",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        assert r2["version"] == "2.0"

        # Approve the newer version first, then the older one
        _approve(client, source_id, mint_doi=False, version="2.0")
        _approve(client, source_id, mint_doi=False, version="1.0")

        # v1.0 is published, but the index still describes v2.0
        assert _status(client, source_id, version="1.0")["status"] == "published"
        assert _status(client, source_id, version="2.0")["status"] == "published"

        assert len(search._entries) == 1
        entry = search.get_entry(source_id)
        assert entry["content"]["mdf"]["version"] == "2.0"
        assert entry["content"]["mdf"]["latest"] is True
        assert entry["content"]["dc"]["title"] == "Updated Dataset v2.0"


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

    def _mint_with_shared_mock(self, monkeypatch, mint_doi):
        """Run a subsequent-version mint against one inspectable mock client.

        The dataset DOI starts registered against a version-pinned landing URL
        (how every v1-migrated DOI arrives); the publish must repoint it at the
        unversioned detail page so the DOI resolves to the latest version.
        """
        import v2.datacite as datacite
        from v2.curation import _mint_doi_for_submission

        mock = datacite.MockDataCiteClient()
        mock._dois["10.99999/test-ds"] = {
            "doi": "10.99999/test-ds",
            "url": "https://materialsdatafacility.org/detail/test-ds-1.0",
        }
        monkeypatch.setattr(datacite, "get_datacite_client", lambda **kw: mock)

        prior_versions = [
            {"version": "1.0", "status": "published", "doi": "10.99999/test-ds", "dataset_doi": "10.99999/test-ds"},
        ]
        submission = {
            "source_id": "test-ds",
            "version": "1.1",
            "dataset_mdata": json.dumps({
                "title": "Updated Dataset",
                "authors": [{"name": "Author"}],
                "data_sources": [],
            }),
        }
        result = _mint_doi_for_submission(
            submission, all_versions=prior_versions, mint_doi=mint_doi
        )
        assert result["success"]
        return mock

    def test_metadata_edit_repoints_dataset_doi_to_latest(self, env, monkeypatch):
        """mint_doi=False publish repoints the concept DOI's landing URL."""
        mock = self._mint_with_shared_mock(monkeypatch, mint_doi=False)
        assert (
            mock._dois["10.99999/test-ds"]["url"]
            == "https://materialsdatafacility.org/detail/test-ds"
        )

    def test_version_mint_repoints_dataset_doi_to_latest(self, env, monkeypatch):
        """mint_doi=True publish also repoints the concept DOI's landing URL."""
        mock = self._mint_with_shared_mock(monkeypatch, mint_doi=True)
        assert (
            mock._dois["10.99999/test-ds"]["url"]
            == "https://materialsdatafacility.org/detail/test-ds"
        )


class TestCardDatasetDOIFallback:
    """The card serves the concept DOI when a version has no per-version DOI."""

    def test_card_doi_falls_back_to_dataset_doi(self, env):
        from v2.dataset_card import build_dataset_card as build_card

        record = {
            "source_id": "test-ds",
            "version": "1.1",
            "status": "published",
            "dataset_doi": "10.99999/test-ds",
            "dataset_mdata": json.dumps({
                "title": "Edited Dataset",
                "authors": [{"name": "Author"}],
                "data_sources": [],
            }),
        }
        card = build_card(record)
        assert card["doi"] == "10.99999/test-ds"
        assert card["dataset_doi"] == "10.99999/test-ds"
        assert card["links"]["doi"] == "https://doi.org/10.99999/test-ds"

    def test_card_prefers_version_doi(self, env):
        from v2.dataset_card import build_dataset_card as build_card

        record = {
            "source_id": "test-ds",
            "version": "1.2",
            "status": "published",
            "doi": "10.99999/test-ds-v1.2",
            "dataset_doi": "10.99999/test-ds",
            "dataset_mdata": json.dumps({
                "title": "Dataset",
                "authors": [{"name": "Author"}],
                "data_sources": [],
            }),
        }
        card = build_card(record)
        assert card["doi"] == "10.99999/test-ds-v1.2"
        assert card["dataset_doi"] == "10.99999/test-ds"

    def test_card_serves_full_author_objects(self, env):
        """Cards keep affiliations/ORCID/name parts — the edit form loads
        authors from the card and replaces the array wholesale on save."""
        from v2.dataset_card import build_dataset_card as build_card

        record = {
            "source_id": "test-ds",
            "version": "1.0",
            "status": "published",
            "dataset_mdata": json.dumps({
                "title": "Dataset",
                "authors": [
                    {
                        "name": "Jane Scientist",
                        "given_name": "Jane",
                        "family_name": "Scientist",
                        "orcid": "0000-0001-2345-6789",
                        "affiliations": ["University of Chicago"],
                    },
                    {"name": "Plain Name"},
                ],
                "data_sources": [],
            }),
        }
        card = build_card(record)
        assert card["authors"][0] == {
            "name": "Jane Scientist",
            "given_name": "Jane",
            "family_name": "Scientist",
            "orcid": "0000-0001-2345-6789",
            "affiliations": ["University of Chicago"],
        }
        # Empty optional fields are dropped, name always present
        assert card["authors"][1] == {"name": "Plain Name"}


class TestNewestVisibleResolution:
    """An in-flight version must never 404 the whole dataset.

    While a new version is mid-publish (status "approved" for as long as the
    SQS publish job takes) or pending curation, the absolute-newest row is
    not anonymously viewable. GET /card without a version must fall back to
    the newest version the caller may see instead of returning 404.
    """

    def _publish_v10_with_pending_update(self, client):
        result = _submit(client)
        source_id = result["source_id"]
        _approve(client, source_id, mint_doi=True)
        # A data update goes through curation: the new latest version sits in
        # pending_curation until approved — exactly like the mid-publish
        # window, the newest row is not anonymously viewable.
        r2 = _submit(client, extra={
            "title": "In-flight update",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
        })
        return source_id, r2["version"]

    def test_anonymous_gets_newest_published_version(self, env):
        client = TestClient(app)
        source_id, pending_version = self._publish_v10_with_pending_update(client)

        # No auth headers: anonymous view during the in-flight window.
        resp = client.get(f"/card/{source_id}")
        assert resp.status_code == 200, resp.json()
        card = resp.json()["card"]
        assert card["version"] == "1.0"
        assert card["status"] == "published"
        assert card["version"] != pending_version

    def test_owner_also_gets_newest_published_version(self, env):
        """Cards serve only published content, owner or not (can_view_dataset
        is deliberately status-gated). The guarantee here is that the owner's
        refresh during the publish window gets the previous version with a
        200 — never a 404. The owner sees their in-flight content via the
        card returned by the save response, not via a card refetch."""
        client = TestClient(app)
        source_id, pending_version = self._publish_v10_with_pending_update(client)

        resp = client.get(f"/card/{source_id}", headers=HEADERS)
        assert resp.status_code == 200, resp.json()
        card = resp.json()["card"]
        assert card["version"] == "1.0"
        assert card["status"] == "published"

    def test_explicit_version_requests_are_not_widened(self, env):
        """Asking for the pending version explicitly still respects visibility."""
        client = TestClient(app)
        source_id, pending_version = self._publish_v10_with_pending_update(client)

        resp = client.get(f"/card/{source_id}", params={"version": pending_version})
        assert resp.status_code == 404


class TestMajorMinorVersioning:
    """Major/minor version detection: new data → major bump, metadata-only → minor bump."""

    def test_new_data_causes_major_bump(self, env):
        """Update with data_sources → version goes from 1.0 to 2.0."""
        client = TestClient(app)

        r1 = _submit(client)
        source_id = r1["source_id"]
        assert r1["version"] == "1.0"

        r2 = _submit(client, extra={
            "title": "New data update",
            "update": True,
            "extensions": {"mdf_source_id": source_id},
            "data_sources": ["https://example.com/new-data.csv"],
        })
        assert r2["version"] == "2.0"

    def test_metadata_only_causes_minor_bump(self, env):
        """Update without data_sources → version goes from 1.0 to 1.1."""
        client = TestClient(app)

        r1 = _submit(client)
        source_id = r1["source_id"]
        assert r1["version"] == "1.0"

        r2 = _submit(client, extra={
            "title": "Metadata-only update",
            "update": True,
            "data_sources": [],
            "extensions": {"mdf_source_id": source_id},
        })
        assert r2["version"] == "1.1"

    def test_metadata_only_inherits_data_sources(self, env):
        """Metadata-only update inherits data_sources from prior version."""
        client = TestClient(app)

        r1 = _submit(client, extra={
            "data_sources": ["https://example.com/original-data.csv"],
        })
        source_id = r1["source_id"]

        r2 = _submit(client, extra={
            "title": "Metadata-only update",
            "update": True,
            "data_sources": [],
            "extensions": {"mdf_source_id": source_id},
        })
        assert r2["version"] == "1.1"

        # Check that v1.1 inherited data_sources from v1.0
        sub = _status(client, source_id, version="1.1")
        mdata = sub.get("dataset_mdata")
        if isinstance(mdata, str):
            mdata = json.loads(mdata)
        assert mdata["data_sources"] == ["https://example.com/original-data.csv"]

    def test_version_chain_major_minor_mixed(self, env):
        """Chain: 1.0 → 2.0 → 2.1 → 3.0 with correct version numbers."""
        client = TestClient(app)

        # v1.0: initial submission with data
        r1 = _submit(client, extra={
            "data_sources": ["https://example.com/v1-data.csv"],
        })
        source_id = r1["source_id"]
        assert r1["version"] == "1.0"

        # v2.0: update with new data → major bump
        r2 = _submit(client, extra={
            "title": "Major update",
            "update": True,
            "data_sources": ["https://example.com/v2-data.csv"],
            "extensions": {"mdf_source_id": source_id},
        })
        assert r2["version"] == "2.0"

        # v2.1: metadata-only update → minor bump
        r3 = _submit(client, extra={
            "title": "Minor metadata tweak",
            "update": True,
            "data_sources": [],
            "extensions": {"mdf_source_id": source_id},
        })
        assert r3["version"] == "2.1"

        # v3.0: another data update → major bump
        r4 = _submit(client, extra={
            "title": "Another major update",
            "update": True,
            "data_sources": ["https://example.com/v3-data.csv"],
            "extensions": {"mdf_source_id": source_id},
        })
        assert r4["version"] == "3.0"

    def test_version_chain_metadata_inherits_correct_sources(self, env):
        """Minor update after major update inherits the major version's data_sources."""
        client = TestClient(app)

        # v1.0
        r1 = _submit(client, extra={
            "data_sources": ["https://example.com/v1.csv"],
        })
        source_id = r1["source_id"]

        # v2.0 with new data
        _submit(client, extra={
            "title": "Major update",
            "update": True,
            "data_sources": ["https://example.com/v2.csv"],
            "extensions": {"mdf_source_id": source_id},
        })

        # v2.1 metadata-only — should inherit v2.0's data_sources
        r3 = _submit(client, extra={
            "title": "Minor tweak after v2",
            "update": True,
            "data_sources": [],
            "extensions": {"mdf_source_id": source_id},
        })
        assert r3["version"] == "2.1"

        sub = _status(client, source_id, version="2.1")
        mdata = sub.get("dataset_mdata")
        if isinstance(mdata, str):
            mdata = json.loads(mdata)
        assert mdata["data_sources"] == ["https://example.com/v2.csv"]

    def test_previous_and_root_version_across_bumps(self, env):
        """previous_version and root_version are correct across major/minor bumps."""
        client = TestClient(app)

        r1 = _submit(client, extra={
            "data_sources": ["https://example.com/data.csv"],
        })
        source_id = r1["source_id"]

        # v2.0
        _submit(client, extra={
            "update": True,
            "data_sources": ["https://example.com/new-data.csv"],
            "extensions": {"mdf_source_id": source_id},
        })

        # v2.1
        _submit(client, extra={
            "title": "Minor tweak",
            "update": True,
            "data_sources": [],
            "extensions": {"mdf_source_id": source_id},
        })

        # Check v2.0 metadata
        v20 = _status(client, source_id, version="2.0")
        mdata20 = v20.get("dataset_mdata")
        if isinstance(mdata20, str):
            mdata20 = json.loads(mdata20)
        assert mdata20["previous_version"] == f"{source_id}-1.0"
        assert mdata20["root_version"] == f"{source_id}-1.0"

        # Check v2.1 metadata
        v21 = _status(client, source_id, version="2.1")
        mdata21 = v21.get("dataset_mdata")
        if isinstance(mdata21, str):
            mdata21 = json.loads(mdata21)
        assert mdata21["previous_version"] == f"{source_id}-2.0"
        assert mdata21["root_version"] == f"{source_id}-1.0"
