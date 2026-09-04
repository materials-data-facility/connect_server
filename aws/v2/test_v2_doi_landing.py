"""Persistent landing URL contracts for concept and version DataCite DOIs."""

import json

from v2.curation import _mint_doi_for_submission
from v2.datacite import DataCiteClient, MockDataCiteClient
from v2.doi_utils import landing_url


def _submission(source_id: str, version: str, title: str = "Dataset"):
    return {
        "source_id": source_id,
        "version": version,
        "dataset_mdata": json.dumps({
            "title": title,
            "authors": [{"name": "Test Author"}],
            "data_sources": [],
        }),
    }


def test_landing_url_uses_portal_config_for_concept_and_version(monkeypatch):
    monkeypatch.setenv("PORTAL_URL", "https://portal.example.test/")

    assert landing_url("example") == "https://portal.example.test/detail/example"
    assert landing_url("example", "1.1") == (
        "https://portal.example.test/detail/example?version=1.1"
    )


def test_datacite_payload_has_version_alternate_identifier():
    client = DataCiteClient(username="user", password="password")
    try:
        payload = client._build_payload(
            "10.99999/example-v1.1",
            landing_url("example", "1.1"),
            {"title": "Example", "authors": ["Test Author"]},
            source_id="example",
            source_version="1.1",
        )
    finally:
        client.close()

    attrs = payload["data"]["attributes"]
    assert attrs["url"] == "https://materialsdatafacility.org/detail/example?version=1.1"
    assert {
        "alternateIdentifier": "example@1.1",
        "alternateIdentifierType": "mdf-source-version",
    } in attrs["alternateIdentifiers"]


def test_publish_uses_distinct_concept_and_version_urls(monkeypatch):
    mock = MockDataCiteClient(prefix="10.99999")
    monkeypatch.setattr("v2.datacite.get_datacite_client", lambda: mock)

    first = _mint_doi_for_submission(_submission("example", "1.0"), [])
    prior = [{
        "source_id": "example",
        "version": "1.0",
        "status": "published",
        "doi": first["doi"],
        "dataset_doi": first["dataset_doi"],
    }]
    second = _mint_doi_for_submission(
        _submission("example", "1.1", "Dataset v1.1"), prior
    )

    assert mock._dois[first["doi"]]["url"] == (
        "https://materialsdatafacility.org/detail/example"
    )
    assert mock._dois[second["doi"]]["url"] == (
        "https://materialsdatafacility.org/detail/example?version=1.1"
    )
    assert {
        "alternateIdentifier": "example@1.1",
        "alternateIdentifierType": "mdf-source-version",
    } in mock._dois[second["doi"]]["alternate_identifiers"]


def test_later_publish_does_not_rewrite_prior_version_url(monkeypatch):
    mock = MockDataCiteClient(prefix="10.99999")
    monkeypatch.setattr("v2.datacite.get_datacite_client", lambda: mock)
    concept_doi = "10.99999/example"
    mock._dois[concept_doi] = {
        "doi": concept_doi,
        "url": landing_url("example"),
    }
    prior = [{
        "version": "1.0",
        "status": "published",
        "doi": concept_doi,
        "dataset_doi": concept_doi,
    }]

    v11 = _mint_doi_for_submission(_submission("example", "1.1"), prior)
    v11_url = mock._dois[v11["doi"]]["url"]
    prior.append({
        "version": "1.1",
        "status": "published",
        "doi": v11["doi"],
        "dataset_doi": concept_doi,
    })
    _mint_doi_for_submission(_submission("example", "1.2"), prior)

    assert mock._dois[v11["doi"]]["url"] == v11_url
    assert v11_url.endswith("/detail/example?version=1.1")


def test_source_ownership_accepts_version_query_landing():
    existing = {
        "data": {"attributes": {
            "url": "https://materialsdatafacility.org/detail/example?version=1.1"
        }}
    }
    assert DataCiteClient._doi_belongs_to_source(existing, "example")
