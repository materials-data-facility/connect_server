"""Focused beta checks for curation pinning and search visibility."""

import json

import pytest
from fastapi.testclient import TestClient

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.search import build_author_index, search_all
from v2.search_client import _format_facet_results
from v2.store import get_store
from v2.submission_utils import dataset_mdata_dict


@pytest.fixture()
def beta_env(tmp_path, monkeypatch):
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "beta.db"))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    reset_middleware_state()
    store = get_store()
    store.put_submission({
        "source_id": "beta-dataset", "version": "1.0", "user_id": "owner",
        "status": "pending_curation", "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "metadata_updated_at": "2026-01-02T00:00:00Z",
        "dataset_mdata": json.dumps({"title": "Beta dataset", "authors": [{"name": "Owner"}]}),
    })
    yield store
    reset_middleware_state()


def _approve(body):
    return TestClient(app).post(
        "/curation/beta-dataset/approve", headers={"X-User-Id": "owner"}, json=body,
    )


def test_approve_stale_stamp_changes_nothing(beta_env):
    before = beta_env.get_submission("beta-dataset", "1.0")
    response = _approve({"expected_updated_at": "2026-01-01T00:00:00Z"})
    assert response.status_code == 409
    assert "changed" in response.json()["detail"]
    assert beta_env.get_submission("beta-dataset", "1.0") == before


@pytest.mark.parametrize("updates", [{"status": "published"}, {"latest": False},
                                     {"title": "x" * (64 * 1024)}])
def test_approve_rejects_reserved_or_oversize_updates(beta_env, updates):
    response = _approve({"metadata_updates": updates})
    assert response.status_code == 400
    assert beta_env.get_submission("beta-dataset", "1.0")["status"] == "pending_curation"


def test_approve_rejects_non_object_updates(beta_env):
    assert _approve({"metadata_updates": ["bad"]}).status_code == 400


def test_self_approval_is_recorded_and_queue_is_explicit(beta_env, monkeypatch):
    monkeypatch.setattr(
        "v2.app.routers.curation.dispatch_publish_job",
        lambda *a, **k: {"queued": True, "job_id": 42},
    )
    response = _approve({"expected_updated_at": "2026-01-02T00:00:00Z"})
    assert response.status_code == 200, response.json()
    assert response.json()["publish_queued"] is True
    assert response.json()["job_id"] == 42
    record = beta_env.get_submission("beta-dataset", "1.0")
    assert record["curation_history"][-1]["self_approved"] is True


def test_dataset_mdata_reader_accepts_dict_string_and_garbage():
    assert dataset_mdata_dict({"dataset_mdata": {"latest": False}}) == {"latest": False}
    assert dataset_mdata_dict({"dataset_mdata": '{"latest": false}'}) == {"latest": False}
    assert dataset_mdata_dict({"dataset_mdata": "{"}) == {}
    assert dataset_mdata_dict({"dataset_mdata": None}) == {}


def test_author_index_excludes_superseded_json_metadata(beta_env, monkeypatch):
    import v2.search as search

    store = beta_env
    base = store.get_submission("beta-dataset", "1.0")
    base.update(status="published", acl=json.dumps(["public"]))
    base["dataset_mdata"] = json.dumps({"title": "Old", "authors": [{"name": "Owner"}], "latest": False})
    store.upsert_submission(base)
    monkeypatch.setattr(search, "get_store", lambda: store)
    assert build_author_index()["counts"]["datasets"] == 0


def test_facet_cleanup_year_order_and_search_paging(monkeypatch):
    facets = _format_facet_results([
        {"name": "Year", "buckets": [
            {"value": "2020", "count": 9}, {"value": " ", "count": 8},
            {"value": "2024", "count": 1}, {"value": "1999", "count": 3},
        ]},
        {"name": "Organization", "buckets": [
            {"value": "Low", "count": 1}, {"value": " ", "count": 10},
            {"value": "High", "count": 5},
        ]},
    ])
    assert [b["value"] for b in facets["Year"]] == ["2024", "2020", "1999"]
    assert [b["value"] for b in facets["Organization"]] == ["High", "Low"]
    import v2.search as search
    monkeypatch.setattr(search, "search_datasets", lambda *a, **k: {
        "results": [], "total": 0, "facets": facets,
    })
    response = search_all("beta", include_streams=False, limit=7, offset=14)
    assert response["limit"] == 7
    assert response["offset"] == 14
    assert response["facets"] == facets
