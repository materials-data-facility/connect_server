"""Source-ID, DOI-collision, and restricted-submission regression tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.datacite import DOICollisionError, DataCiteClient
from v2.storage import reset_storage_backend
from v2.submission_utils import validate_source_id


OWNER_HEADERS = {"X-User-Id": "owner-user"}
CURATOR_HEADERS = {"X-User-Id": "curator-user"}
BASE_SUBMISSION = {
    "title": "Source ID test dataset",
    "authors": [{"name": "Test Author"}],
    "data_sources": ["https://example.com/data.csv"],
}


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
    monkeypatch.setenv("CURATOR_USER_IDS", "curator-user")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


@pytest.mark.parametrize("source_id", ["../x", "a/b", "A B", "-abc", ".abc", "ab", "abc..def"])
def test_validate_source_id_rejects_unsafe_values(source_id):
    with pytest.raises(ValueError, match=r"\^\[a-z0-9\]"):
        validate_source_id(source_id)


@pytest.mark.parametrize(
    "source_id", ["mdf-abc123", "levine_abo2179_database_v2.1"]
)
def test_validate_source_id_accepts_generated_and_legacy_values(source_id):
    assert validate_source_id(source_id) == source_id


def test_submit_rejects_invalid_client_source_id_with_grammar(env):
    response = TestClient(app).post(
        "/submit",
        headers=OWNER_HEADERS,
        json={
            **BASE_SUBMISSION,
            "extensions": {"mdf_source_id": "../other"},
        },
    )

    assert response.status_code == 400
    assert "^[a-z0-9][a-z0-9._-]{2,63}$" in response.json()["detail"]


# A leading "-" is rejected by the LENIENT path validator. It has to be:
# "Bad" is a perfectly addressable id now (32 live staging ids carry uppercase,
# non-ASCII or >64-character names that predate the strict grammar), so path
# parameters are only screened for genuinely unsafe shapes.
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("get", "/status/-bad", None),
        ("get", "/versions/-bad", None),
        ("get", "/versions/-bad/diff?from=1.0&to=2.0", None),
        ("get", "/stats/-bad", None),
        ("post", "/submissions/-bad/metadata", {"title": "Changed"}),
        ("post", "/submissions/-bad/withdraw", {}),
        ("post", "/submissions/-bad/resubmit", {}),
        ("post", "/submissions/-bad/delete", {"reason": "invalid id"}),
    ],
)
def test_invalid_path_source_id_is_not_found(env, method, path, body):
    headers = CURATOR_HEADERS if path.endswith("/delete") else OWNER_HEADERS
    request = getattr(TestClient(app), method)
    response = request(path, headers=headers, json=body) if body is not None else request(path, headers=headers)

    assert response.status_code == 404
    assert "grammar" not in response.text.lower()


def test_doi_suffix_preserves_underscore():
    client = DataCiteClient.__new__(DataCiteClient)
    assert client._generate_suffix("Dataset_A-B") == "dataset_a-b"


def test_existing_doi_for_another_dataset_raises_collision():
    client = DataCiteClient.__new__(DataCiteClient)
    client.prefix = "10.99999"
    client.get_doi = lambda doi: {
        "data": {
            "attributes": {
                "url": "https://materialsdatafacility.org/detail/dataset-a",
                "alternateIdentifiers": [
                    {
                        "alternateIdentifier": "dataset-a",
                        "alternateIdentifierType": "MDF source ID",
                    }
                ],
            }
        }
    }
    client._update_doi = lambda *args, **kwargs: pytest.fail("collision must not update DOI")

    with pytest.raises(DOICollisionError, match="different dataset"):
        client.mint_doi("dataset_a", BASE_SUBMISSION)


class _CollidingDataCiteClient(DataCiteClient):
    def __init__(self):
        self.prefix = "10.99999"

    def get_doi(self, doi):
        return {
            "data": {
                "attributes": {
                    "url": "https://materialsdatafacility.org/detail/someone-else",
                    "alternateIdentifiers": [],
                }
            }
        }

    def _update_doi(self, doi, payload):
        pytest.fail("collision must not update DOI")

    def close(self):
        pass


def test_doi_collision_does_not_publish(env, monkeypatch):
    monkeypatch.setenv("USE_MOCK_DATACITE", "false")
    monkeypatch.setattr(
        "v2.datacite.get_datacite_client", lambda *args, **kwargs: _CollidingDataCiteClient()
    )
    client = TestClient(app)
    submitted = client.post("/submit", headers=OWNER_HEADERS, json=BASE_SUBMISSION)
    source_id = submitted.json()["source_id"]

    approved = client.post(
        f"/curation/{source_id}/approve",
        headers=CURATOR_HEADERS,
        json={"mint_doi": True},
    )

    assert approved.status_code == 502
    status = client.get(f"/status/{source_id}", headers=OWNER_HEADERS)
    assert status.json()["submission"]["status"] == "approved"


def _publish_restricted_versions(client: TestClient) -> str:
    submitted = client.post(
        "/submit",
        headers=OWNER_HEADERS,
        json={**BASE_SUBMISSION, "acl": ["urn:globus:auth:identity:allowed-user"]},
    )
    source_id = submitted.json()["source_id"]
    approved = client.post(
        f"/curation/{source_id}/approve",
        headers=CURATOR_HEADERS,
        json={"mint_doi": False},
    )
    assert approved.status_code == 200
    updated = client.post(
        "/submit",
        headers=OWNER_HEADERS,
        json={
            **BASE_SUBMISSION,
            "title": "Restricted version two",
            "update": True,
            "update_metadata_only": True,
            "extensions": {"mdf_source_id": source_id},
            "acl": ["urn:globus:auth:identity:allowed-user"],
        },
    )
    assert updated.status_code == 200
    second_version = updated.json()["version"]
    approved = client.post(
        f"/curation/{source_id}/approve",
        headers=CURATOR_HEADERS,
        json={"mint_doi": False, "version": second_version},
    )
    assert approved.status_code == 200
    return source_id


def test_restricted_published_submission_routes_are_owner_only(env):
    client = TestClient(app)
    source_id = _publish_restricted_versions(client)
    owner_versions = client.get(f"/versions/{source_id}", headers=OWNER_HEADERS).json()["versions"]
    v_from, v_to = owner_versions[0]["version"], owner_versions[-1]["version"]
    assert v_from != v_to
    for path in (f"/versions/{source_id}/diff?from={v_from}&to={v_to}", f"/stats/{source_id}"):
        assert client.get(path).status_code == 404
        assert client.get(path, headers=OWNER_HEADERS).status_code == 200

    # A hidden record must be indistinguishable from a nonexistent one (no
    # existence oracle). /status keeps its historical 200 + {"success": false}
    # shape; /versions answers 404 like /card and /stats.
    ghost = "mdf-000000000000000000000000000000ff"
    anon = client.get(f"/status/{source_id}").json()
    assert anon["success"] is False
    assert anon == client.get(f"/status/{ghost}").json()
    assert client.get(f"/status/{source_id}", headers=OWNER_HEADERS).json()["success"] is True

    hidden = client.get(f"/versions/{source_id}")
    missing = client.get(f"/versions/{ghost}")
    assert hidden.status_code == missing.status_code == 404
    assert hidden.json() == missing.json()
    assert client.get(f"/versions/{source_id}", headers=OWNER_HEADERS).json()["success"] is True


def test_other_routers_reject_path_hostile_source_ids_as_not_found(env):
    """cards/preview/search/curation share the lenient path guard via a router dependency."""
    client = TestClient(app)
    for path in ("/card/-bad", "/preview/..evil", "/citation/a%20b", "/datasets/-bad/related"):
        resp = client.get(path)
        assert resp.status_code == 404, path
        assert "grammar" not in resp.text.lower()

    # Legacy-shaped ids (uppercase, non-ASCII) are still addressable through the guard:
    # they fall through to the store lookup instead of being rejected up front.
    for path in ("/card/Dataset_Li_conductivity", "/card/kononov_identifying_native_αalumina"):
        resp = client.get(path)
        assert resp.status_code in (200, 404), path
        assert resp.json().get("detail") in (None, "Dataset not found")
