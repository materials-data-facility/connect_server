"""Regression tests for the beta citation, search, and legacy-ID fixes."""

from xml.etree import ElementTree as ET

import pytest
from fastapi.testclient import TestClient

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.citation import (
    generate_apa,
    generate_bibtex,
    generate_datacite_xml,
    generate_ris,
)
from v2.storage import reset_storage_backend
from v2.store import get_store


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(tmp_path / "files"))
    reset_storage_backend()
    reset_middleware_state()
    yield TestClient(app)
    reset_storage_backend()
    reset_middleware_state()


def _record(source_id="beta-data", **extra):
    return {
        "source_id": source_id,
        "version": "1.0",
        "status": "published",
        "latest": True,
        "acl": ["public"],
        "dataset_mdata": {
            "title": "Beta Dataset",
            "authors": [{"name": "Jane Researcher"}],
            "publication_year": 2026,
            "publisher": "MDF",
        },
        **extra,
    }


def test_citations_use_dataset_doi_and_omit_missing_published_doi():
    record = _record(dataset_doi="10.18126/beta", version="2.1")
    outputs = (
        generate_bibtex(record), generate_ris(record),
        generate_apa(record), generate_datacite_xml(record),
    )
    assert all("10.18126/beta" in value for value in outputs)
    assert "version = {2.1}" in outputs[0]
    assert "Version 2.1" in outputs[2]
    assert ET.fromstring(outputs[3]).find(
        "{http://datacite.org/schema/kernel-4}version"
    ).text == "2.1"

    no_doi = _record()
    outputs = (
        generate_bibtex(no_doi), generate_ris(no_doi),
        generate_apa(no_doi), generate_datacite_xml(no_doi),
    )
    assert all("10.xxxxx/pending" not in value for value in outputs)
    assert "  doi = " not in outputs[0]
    assert "DO  - " not in outputs[1]
    assert "doi.org" not in outputs[2]
    assert ET.fromstring(outputs[3]).find(
        "{http://datacite.org/schema/kernel-4}identifier"
    ) is None

    metadata_doi = _record(dataset_mdata={**no_doi["dataset_mdata"], "doi": "10.18126/meta"})
    assert all("10.18126/meta" in generator(metadata_doi) for generator in (
        generate_bibtex, generate_ris, generate_apa, generate_datacite_xml,
    ))


def test_search_offset_cap_returns_400(env):
    response = env.get("/search?limit=20&offset=9981")
    assert response.status_code == 400
    assert "10000" in response.json()["detail"]


def test_search_failure_does_not_drop_filters_or_scan_on_4xx(env, monkeypatch):
    from v2 import search

    class FailedSearch:
        def __init__(self, error):
            self.error = error

        def faceted_search(self, *args, **kwargs):
            return {"success": False, "error": self.error}

    monkeypatch.setattr("v2.search_client.get_search_client", lambda: FailedSearch("HTTP 503"))
    response = env.get("/search?q=Beta&year=2026")
    assert response.status_code == 503
    assert "Filtered search" in response.json()["detail"]

    monkeypatch.setattr("v2.search_client.get_search_client", lambda: FailedSearch("HTTP 400"))
    monkeypatch.setattr(search, "get_store", lambda: pytest.fail("store scan on 4xx"))
    response = env.get("/search?q=Beta")
    assert response.status_code == 502


def test_search_error_without_status_still_falls_back(env, monkeypatch):
    """A transport-style error string (no HTTP status) is an outage, not a
    rejected query: answer from the store instead of 502."""
    from v2 import search

    class FailedSearch:
        def faceted_search(self, *args, **kwargs):
            return {"success": False, "error": "Connection reset by peer"}

    class EmptyStore:
        def list_all(self, limit):
            return []

    monkeypatch.setattr("v2.search_client.get_search_client", FailedSearch)
    monkeypatch.setattr(search, "get_store", EmptyStore)
    response = env.get("/search?q=Beta")
    assert response.status_code == 200


def test_search_fallback_scan_limit_is_ceiling(env, monkeypatch):
    from v2 import search

    class UnavailableSearch:
        def faceted_search(self, *args, **kwargs):
            raise ConnectionError("offline")

    class LimitedStore:
        def list_all(self, limit):
            assert limit == search.SEARCH_MAX_DATASET_SCAN
            return []

    monkeypatch.setattr("v2.search_client.get_search_client", UnavailableSearch)
    monkeypatch.setattr(search, "get_store", LimitedStore)
    response = env.get("/search?q=Beta&offset=9000")
    assert response.status_code == 200


def test_legacy_versions_stats_and_related_resolve_and_unknown_404(env):
    store = get_store()
    store.upsert_submission(_record(legacy_source_id="beta-data_v1.0"))

    for endpoint in ("versions", "stats", "datasets"):
        suffix = "/related" if endpoint == "datasets" else ""
        direct = env.get(f"/{endpoint}/beta-data{suffix}")
        legacy = env.get(f"/{endpoint}/beta-data_v1.0{suffix}")
        assert direct.status_code == 200, direct.text
        assert legacy.status_code == 200, legacy.text
        assert legacy.json()["source_id"] == "beta-data"
        assert legacy.json()["canonical_source_id"] == "beta-data"
        assert legacy.json()["redirected_from"] == "beta-data_v1.0"
        assert "redirected_from" not in direct.json()

        missing = env.get(f"/{endpoint}/does-not-exist{suffix}")
        assert missing.status_code == 404
        assert "detail" in missing.json()
