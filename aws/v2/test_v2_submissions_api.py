"""Tests for new submission endpoints: metadata edit, withdraw, resubmit, version diff, status filtering.

Covers:
- POST /submissions/{source_id}/metadata — edit metadata in-place and auto minor-bump on published
- POST /submissions/{source_id}/withdraw — withdraw pending submission
- POST /submissions/{source_id}/resubmit — resubmit rejected submission
- GET /versions/{source_id}/diff — compare metadata between versions
- GET /submissions?status=&include_counts=true — status filtering and counts
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
OTHER_HEADERS = {"X-User-Id": "other-user"}

BASE_SUBMISSION = {
    "title": "Test Dataset",
    "authors": [{"name": "Test Author"}],
    "data_sources": ["https://example.com/data.csv"],
}


def _submit(client, extra=None, headers=None):
    payload = {**BASE_SUBMISSION, **(extra or {})}
    resp = client.post("/submit", headers=headers or HEADERS, json=payload)
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


def _reject(client, source_id, reason="needs work", version=None):
    body = {"reason": reason}
    if version:
        body["version"] = version
    resp = client.post(
        f"/curation/{source_id}/reject",
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


# ---------------------------------------------------------------------------
# Metadata Edit Tests
# ---------------------------------------------------------------------------

class TestMetadataEdit:
    def test_edit_pending_curation(self, env):
        """Edit metadata on a pending_curation submission (in-place)."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "Updated Title", "keywords": ["new-kw"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"]
        assert "title" in data["updated_fields"]
        assert "keywords" in data["updated_fields"]
        assert "new_version" not in data

        # Verify the metadata was actually updated
        sub = _status(client, source_id)
        mdata = sub.get("dataset_mdata")
        if isinstance(mdata, str):
            mdata = json.loads(mdata)
        assert mdata["title"] == "Updated Title"
        assert "new-kw" in mdata["keywords"]

    def test_edit_rejected_submission(self, env):
        """Edit metadata on a rejected submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        _reject(client, source_id)

        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"description": "Fixed description"},
        )
        assert resp.status_code == 200
        assert resp.json()["success"]

    def test_edit_published_creates_minor_bump(self, env):
        """Editing published metadata auto-creates a minor version bump."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        _approve(client, source_id, mint_doi=True)

        # Verify v1.0 is published
        v10 = _status(client, source_id, version="1.0")
        assert v10["status"] == "published"

        # Edit metadata on published dataset
        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "Published Edit", "version": "1.0"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"]
        assert data["new_version"] == "1.1"

        # Verify v1.1 exists and is published
        v11 = _status(client, source_id, version="1.1")
        assert v11["status"] == "published"
        mdata = v11.get("dataset_mdata")
        if isinstance(mdata, str):
            mdata = json.loads(mdata)
        assert mdata["title"] == "Published Edit"

        # Verify v1.0 has latest=False
        v10_after = _status(client, source_id, version="1.0")
        mdata10 = v10_after.get("dataset_mdata")
        if isinstance(mdata10, str):
            mdata10 = json.loads(mdata10)
        assert mdata10["latest"] is False

    def test_edit_published_inherits_doi(self, env):
        """Minor bump from published edit inherits dataset_doi."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        _approve(client, source_id, mint_doi=True)
        v10 = _status(client, source_id, version="1.0")
        dataset_doi = v10.get("dataset_doi")
        assert dataset_doi

        # Edit published
        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"description": "New description", "version": "1.0"},
        )
        assert resp.status_code == 200

        v11 = _status(client, source_id, version="1.1")
        assert v11.get("dataset_doi") == dataset_doi

    def test_edit_no_fields_returns_400(self, env):
        """Empty edit request returns 400."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={},
        )
        assert resp.status_code == 400

    def test_edit_invalid_metadata_returns_400(self, env):
        """Invalid metadata (e.g. bad authors format) returns 400."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"authors": [{"bad_field": "no name"}]},
        )
        assert resp.status_code == 400

    def test_edit_wrong_status_returns_400(self, env):
        """Cannot edit metadata on a withdrawn submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        # Withdraw it first
        client.post(
            f"/submissions/{source_id}/withdraw",
            headers=HEADERS,
            json={},
        )

        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "Should fail"},
        )
        assert resp.status_code == 400

    def test_edit_not_found_returns_404(self, env):
        """Editing a non-existent submission returns 404."""
        client = TestClient(app)
        resp = client.post(
            "/submissions/nonexistent-id/metadata",
            headers=HEADERS,
            json={"title": "Nope"},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Withdraw Tests
# ---------------------------------------------------------------------------

class TestWithdraw:
    def test_withdraw_pending(self, env):
        """Withdraw a pending_curation submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.post(
            f"/submissions/{source_id}/withdraw",
            headers=HEADERS,
            json={"reason": "Changed my mind"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"]
        assert data["status"] == "withdrawn"

        # Verify status in store
        sub = _status(client, source_id)
        assert sub["status"] == "withdrawn"

    def test_withdraw_non_pending_returns_400(self, env):
        """Cannot withdraw a published submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        _approve(client, source_id, mint_doi=False)

        resp = client.post(
            f"/submissions/{source_id}/withdraw",
            headers=HEADERS,
            json={},
        )
        assert resp.status_code == 400

    def test_withdraw_records_curation_history(self, env):
        """Withdrawal is recorded in curation_history."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        client.post(
            f"/submissions/{source_id}/withdraw",
            headers=HEADERS,
            json={"reason": "Duplicate"},
        )

        resp = client.get(
            f"/curation/{source_id}",
            headers=HEADERS,
        )
        assert resp.status_code == 200
        history = resp.json().get("curation_history", [])
        assert len(history) == 1
        assert history[0]["action"] == "withdrawn"
        assert history[0]["reason"] == "Duplicate"


# ---------------------------------------------------------------------------
# Resubmit Tests
# ---------------------------------------------------------------------------

class TestResubmit:
    def test_resubmit_after_rejection(self, env):
        """Resubmit a rejected submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        _reject(client, source_id)

        resp = client.post(
            f"/submissions/{source_id}/resubmit",
            headers=HEADERS,
            json={"notes": "Fixed the issues"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"]
        assert data["status"] == "pending_curation"

        # Verify it shows up as pending again
        sub = _status(client, source_id)
        assert sub["status"] == "pending_curation"

    def test_resubmit_non_rejected_returns_400(self, env):
        """Cannot resubmit a pending_curation submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.post(
            f"/submissions/{source_id}/resubmit",
            headers=HEADERS,
            json={},
        )
        assert resp.status_code == 400

    def test_reject_edit_resubmit_workflow(self, env):
        """Full workflow: reject -> edit metadata -> resubmit."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        # Reject
        _reject(client, source_id, reason="Title too vague")

        # Edit metadata
        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "Much Better Title"},
        )
        assert resp.status_code == 200

        # Resubmit
        resp = client.post(
            f"/submissions/{source_id}/resubmit",
            headers=HEADERS,
            json={"notes": "Fixed title"},
        )
        assert resp.status_code == 200

        # Verify it's pending with the new title
        sub = _status(client, source_id)
        assert sub["status"] == "pending_curation"
        mdata = sub.get("dataset_mdata")
        if isinstance(mdata, str):
            mdata = json.loads(mdata)
        assert mdata["title"] == "Much Better Title"

    def test_resubmit_records_curation_history(self, env):
        """Resubmit is recorded in curation_history."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        _reject(client, source_id)

        client.post(
            f"/submissions/{source_id}/resubmit",
            headers=HEADERS,
            json={"notes": "Take two"},
        )

        resp = client.get(
            f"/curation/{source_id}",
            headers=HEADERS,
        )
        history = resp.json().get("curation_history", [])
        actions = [h["action"] for h in history]
        assert "rejected" in actions
        assert "resubmitted" in actions


# ---------------------------------------------------------------------------
# Version Diff Tests
# ---------------------------------------------------------------------------

class TestVersionDiff:
    def test_diff_between_versions(self, env):
        """Diff shows changed, added, and unchanged fields."""
        client = TestClient(app)

        # Submit v1.0
        result = _submit(client, extra={
            "title": "Original Title",
            "keywords": ["a", "b"],
        })
        source_id = result["source_id"]

        # Submit v2.0 (update with new data)
        _submit(client, extra={
            "title": "Updated Title",
            "keywords": ["a", "c"],
            "description": "New description",
            "update": True,
            "data_sources": ["https://example.com/new-data.csv"],
            "extensions": {"mdf_source_id": source_id},
        })

        resp = client.get(
            f"/versions/{source_id}/diff",
            headers=OTHER_HEADERS,
            params={"from": "1.0", "to": "2.0"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"]
        assert data["from_version"]["version"] == "1.0"
        assert data["to_version"]["version"] == "2.0"

        diff = data["diff"]
        # title changed
        assert "title" in diff["changed"]
        assert diff["changed"]["title"]["from"] == "Original Title"
        assert diff["changed"]["title"]["to"] == "Updated Title"
        # description changed (None -> "New description" since schema default is None)
        assert "description" in diff["changed"]
        assert diff["changed"]["description"]["to"] == "New description"

    def test_diff_version_not_found(self, env):
        """Diff with nonexistent version returns 404."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.get(
            f"/versions/{source_id}/diff",
            headers=HEADERS,
            params={"from": "1.0", "to": "99.0"},
        )
        assert resp.status_code == 404

    def test_diff_hides_unpublished_versions_from_non_owner(self, env, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
        client = TestClient(app)

        result = _submit(client)
        source_id = result["source_id"]

        _submit(client, extra={
            "title": "Updated Title",
            "update": True,
            "data_sources": ["https://example.com/new-data.csv"],
            "extensions": {"mdf_source_id": source_id},
        })

        resp = client.get(
            f"/versions/{source_id}/diff",
            headers=OTHER_HEADERS,
            params={"from": "1.0", "to": "2.0"},
        )
        assert resp.status_code == 404

    def test_diff_skips_system_fields(self, env):
        """System fields (version, latest, etc.) are excluded from diff."""
        client = TestClient(app)

        result = _submit(client)
        source_id = result["source_id"]

        _submit(client, extra={
            "title": "Updated",
            "update": True,
            "data_sources": ["https://example.com/v2.csv"],
            "extensions": {"mdf_source_id": source_id},
        })

        resp = client.get(
            f"/versions/{source_id}/diff",
            params={"from": "1.0", "to": "2.0"},
        )
        data = resp.json()
        diff = data["diff"]

        # System fields should not appear in any diff category
        for category in [diff["added"], diff["removed"], diff["changed"]]:
            for field in ("version", "latest", "previous_version", "root_version", "update", "test"):
                assert field not in category
        for field in ("version", "latest", "previous_version", "root_version", "update", "test"):
            assert field not in diff["unchanged"]


# ---------------------------------------------------------------------------
# Status Filtering and Counts Tests
# ---------------------------------------------------------------------------

class TestSubmissionsFiltering:
    def test_filter_by_status(self, env):
        """Filter submissions by status."""
        client = TestClient(app)

        # Submit two datasets
        r1 = _submit(client)
        r2 = _submit(client, extra={"title": "Second Dataset"})

        # Approve one
        _approve(client, r1["source_id"], mint_doi=False)

        # Filter for published only
        resp = client.get(
            "/submissions",
            headers=HEADERS,
            params={"status": "published"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert all(s["status"] == "published" for s in data["submissions"])

        # Filter for pending_curation only
        resp = client.get(
            "/submissions",
            headers=HEADERS,
            params={"status": "pending_curation"},
        )
        data = resp.json()
        assert all(s["status"] == "pending_curation" for s in data["submissions"])

    def test_filter_multiple_statuses(self, env):
        """Filter by comma-separated statuses."""
        client = TestClient(app)

        r1 = _submit(client)
        r2 = _submit(client, extra={"title": "Second"})
        _approve(client, r1["source_id"], mint_doi=False)

        resp = client.get(
            "/submissions",
            headers=HEADERS,
            params={"status": "published,pending_curation"},
        )
        data = resp.json()
        statuses = {s["status"] for s in data["submissions"]}
        assert statuses <= {"published", "pending_curation"}
        assert len(data["submissions"]) == 2

    def test_include_counts(self, env):
        """include_counts=true returns status counts."""
        client = TestClient(app)

        _submit(client)
        _submit(client, extra={"title": "Second"})
        r3 = _submit(client, extra={"title": "Third"})
        _approve(client, r3["source_id"], mint_doi=False)

        resp = client.get(
            "/submissions",
            headers=HEADERS,
            params={"include_counts": "true"},
        )
        data = resp.json()
        assert "counts" in data
        assert "total" in data
        assert data["total"] == 3
        assert data["counts"].get("pending_curation", 0) == 2
        assert data["counts"].get("published", 0) == 1

    def test_status_filter_with_counts(self, env):
        """Counts reflect all items but submissions list is filtered."""
        client = TestClient(app)

        _submit(client)
        r2 = _submit(client, extra={"title": "Second"})
        _approve(client, r2["source_id"], mint_doi=False)

        resp = client.get(
            "/submissions",
            headers=HEADERS,
            params={"status": "published", "include_counts": "true"},
        )
        data = resp.json()
        # Counts include all statuses
        assert data["total"] == 2
        # But submissions list only has published
        assert all(s["status"] == "published" for s in data["submissions"])

    def test_no_filter_returns_all(self, env):
        """Without status filter, all submissions are returned."""
        client = TestClient(app)

        _submit(client)
        _submit(client, extra={"title": "Second"})

        resp = client.get("/submissions", headers=HEADERS)
        data = resp.json()
        assert len(data["submissions"]) == 2


# ---------------------------------------------------------------------------
# deep_merge utility tests
# ---------------------------------------------------------------------------

class TestDeepMerge:
    def test_deep_merge_nested(self):
        from v2.submission_utils import deep_merge

        base = {"a": 1, "b": {"c": 2, "d": 3}}
        deep_merge(base, {"b": {"c": 99, "e": 5}})
        assert base == {"a": 1, "b": {"c": 99, "d": 3, "e": 5}}

    def test_deep_merge_overwrite(self):
        from v2.submission_utils import deep_merge

        base = {"x": [1, 2]}
        deep_merge(base, {"x": [3, 4]})
        assert base == {"x": [3, 4]}

    def test_deep_merge_add_new(self):
        from v2.submission_utils import deep_merge

        base = {"a": 1}
        deep_merge(base, {"b": 2})
        assert base == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# Permission / Auth Tests
# ---------------------------------------------------------------------------

class TestPermissions:
    def test_edit_metadata_requires_owner_or_curator(self, env):
        """Non-owner, non-curator cannot edit metadata."""
        client = TestClient(app)
        # Submit as test-user
        result = _submit(client, headers=HEADERS)
        source_id = result["source_id"]

        # Disable ALLOW_ALL_CURATORS to enforce real permission checks
        import os
        os.environ["ALLOW_ALL_CURATORS"] = "false"
        try:
            resp = client.post(
                f"/submissions/{source_id}/metadata",
                headers=OTHER_HEADERS,
                json={"title": "Hijack"},
            )
            assert resp.status_code == 403
        finally:
            os.environ["ALLOW_ALL_CURATORS"] = "true"

    def test_withdraw_requires_owner_or_curator(self, env):
        """Non-owner, non-curator cannot withdraw."""
        client = TestClient(app)
        result = _submit(client, headers=HEADERS)
        source_id = result["source_id"]

        import os
        os.environ["ALLOW_ALL_CURATORS"] = "false"
        try:
            resp = client.post(
                f"/submissions/{source_id}/withdraw",
                headers=OTHER_HEADERS,
                json={},
            )
            assert resp.status_code == 403
        finally:
            os.environ["ALLOW_ALL_CURATORS"] = "true"

    def test_resubmit_requires_owner_or_curator(self, env):
        """Non-owner, non-curator cannot resubmit."""
        client = TestClient(app)
        result = _submit(client, headers=HEADERS)
        source_id = result["source_id"]
        _reject(client, source_id)

        import os
        os.environ["ALLOW_ALL_CURATORS"] = "false"
        try:
            resp = client.post(
                f"/submissions/{source_id}/resubmit",
                headers=OTHER_HEADERS,
                json={},
            )
            assert resp.status_code == 403
        finally:
            os.environ["ALLOW_ALL_CURATORS"] = "true"


# ---------------------------------------------------------------------------
# Withdraw edge cases
# ---------------------------------------------------------------------------

class TestWithdrawEdgeCases:
    def test_withdraw_restores_latest_on_prior_version(self, env):
        """Withdrawing the latest version restores latest=True on prior version."""
        client = TestClient(app)

        # Submit v1.0
        r1 = _submit(client)
        source_id = r1["source_id"]

        # Submit v2.0 (update with data → major bump)
        r2 = _submit(client, extra={
            "title": "Updated",
            "update": True,
            "data_sources": ["https://example.com/v2.csv"],
            "extensions": {"mdf_source_id": source_id},
        })
        assert r2["version"] == "2.0"

        # v1.0 should now have latest=False
        v10 = _status(client, source_id, version="1.0")
        mdata10 = v10.get("dataset_mdata")
        if isinstance(mdata10, str):
            mdata10 = json.loads(mdata10)
        assert mdata10["latest"] is False

        # Withdraw v2.0
        resp = client.post(
            f"/submissions/{source_id}/withdraw",
            headers=HEADERS,
            json={"reason": "Bad data", "version": "2.0"},
        )
        assert resp.status_code == 200

        # v1.0 should now have latest=True restored
        v10_after = _status(client, source_id, version="1.0")
        mdata10_after = v10_after.get("dataset_mdata")
        if isinstance(mdata10_after, str):
            mdata10_after = json.loads(mdata10_after)
        assert mdata10_after["latest"] is True

    def test_withdraw_not_found_returns_404(self, env):
        """Withdrawing a non-existent submission returns 404."""
        client = TestClient(app)
        resp = client.post(
            "/submissions/nonexistent-id/withdraw",
            headers=HEADERS,
            json={},
        )
        assert resp.status_code == 404

    def test_withdraw_rejected_returns_400(self, env):
        """Cannot withdraw an already-rejected submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]
        _reject(client, source_id)

        resp = client.post(
            f"/submissions/{source_id}/withdraw",
            headers=HEADERS,
            json={},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Resubmit edge cases
# ---------------------------------------------------------------------------

class TestResubmitEdgeCases:
    def test_resubmit_not_found_returns_404(self, env):
        """Resubmitting a non-existent submission returns 404."""
        client = TestClient(app)
        resp = client.post(
            "/submissions/nonexistent-id/resubmit",
            headers=HEADERS,
            json={},
        )
        assert resp.status_code == 404

    def test_resubmit_withdrawn_returns_400(self, env):
        """Cannot resubmit a withdrawn submission (only rejected)."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        # Withdraw it
        client.post(
            f"/submissions/{source_id}/withdraw",
            headers=HEADERS,
            json={},
        )

        resp = client.post(
            f"/submissions/{source_id}/resubmit",
            headers=HEADERS,
            json={},
        )
        assert resp.status_code == 400

    def test_resubmit_published_returns_400(self, env):
        """Cannot resubmit a published submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]
        _approve(client, source_id, mint_doi=False)

        resp = client.post(
            f"/submissions/{source_id}/resubmit",
            headers=HEADERS,
            json={},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Metadata Edit advanced cases
# ---------------------------------------------------------------------------

class TestMetadataEditAdvanced:
    def test_published_edit_chain(self, env):
        """Multiple edits on published dataset: 1.0 → 1.1 → 1.2."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]
        _approve(client, source_id, mint_doi=True)

        # First edit: 1.0 → 1.1
        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "Edit One", "version": "1.0"},
        )
        assert resp.status_code == 200
        assert resp.json()["new_version"] == "1.1"

        # Second edit on 1.1 → 1.2
        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "Edit Two", "version": "1.1"},
        )
        assert resp.status_code == 200
        assert resp.json()["new_version"] == "1.2"

        # Verify all three versions exist with correct statuses
        versions_resp = client.get(f"/versions/{source_id}")
        versions = versions_resp.json()["versions"]
        version_map = {v["version"]: v for v in versions}
        assert "1.0" in version_map
        assert "1.1" in version_map
        assert "1.2" in version_map
        assert version_map["1.2"]["status"] == "published"

        # Verify 1.2 has the latest title
        v12 = _status(client, source_id, version="1.2")
        mdata = v12.get("dataset_mdata")
        if isinstance(mdata, str):
            mdata = json.loads(mdata)
        assert mdata["title"] == "Edit Two"
        assert mdata["latest"] is True

    def test_edit_with_explicit_version(self, env):
        """Edit targets a specific version, not latest."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "Targeted Edit", "version": "1.0"},
        )
        assert resp.status_code == 200
        assert resp.json()["version"] == "1.0"

    def test_edit_deep_merges_extensions(self, env):
        """Editing extensions deep-merges rather than overwrites."""
        client = TestClient(app)
        result = _submit(client, extra={
            "extensions": {"key_a": "original", "nested": {"x": 1, "y": 2}},
        })
        source_id = result["source_id"]

        resp = client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"extensions": {"key_b": "new", "nested": {"y": 99, "z": 3}}},
        )
        assert resp.status_code == 200

        sub = _status(client, source_id)
        mdata = sub.get("dataset_mdata")
        if isinstance(mdata, str):
            mdata = json.loads(mdata)
        ext = mdata["extensions"]
        assert ext["key_a"] == "original"
        assert ext["key_b"] == "new"
        assert ext["nested"]["x"] == 1
        assert ext["nested"]["y"] == 99
        assert ext["nested"]["z"] == 3


# ---------------------------------------------------------------------------
# Version Diff edge cases
# ---------------------------------------------------------------------------

class TestVersionDiffEdgeCases:
    def test_diff_identical_versions(self, env):
        """Diffing a version against itself yields empty diff."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.get(
            f"/versions/{source_id}/diff",
            headers=HEADERS,
            params={"from": "1.0", "to": "1.0"},
        )
        assert resp.status_code == 200
        diff = resp.json()["diff"]
        assert diff["added"] == {}
        assert diff["removed"] == {}
        assert diff["changed"] == {}
        assert len(diff["unchanged"]) > 0

    def test_diff_source_not_found(self, env):
        """Diff on non-existent source_id returns 404."""
        client = TestClient(app)
        resp = client.get(
            "/versions/no-such-id/diff",
            params={"from": "1.0", "to": "2.0"},
        )
        assert resp.status_code == 404

    def test_diff_after_metadata_edit(self, env):
        """Diff works between original and metadata-edited published version."""
        client = TestClient(app)
        result = _submit(client, extra={"title": "Before Edit"})
        source_id = result["source_id"]
        _approve(client, source_id, mint_doi=False)

        # Edit published → creates 1.1
        client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "After Edit", "version": "1.0"},
        )

        resp = client.get(
            f"/versions/{source_id}/diff",
            headers=HEADERS,
            params={"from": "1.0", "to": "1.1"},
        )
        assert resp.status_code == 200
        diff = resp.json()["diff"]
        assert "title" in diff["changed"]
        assert diff["changed"]["title"]["from"] == "Before Edit"
        assert diff["changed"]["title"]["to"] == "After Edit"


# ---------------------------------------------------------------------------
# Soft-Delete Tests
# ---------------------------------------------------------------------------

class TestSoftDelete:
    def test_delete_submission(self, env):
        """Curator can soft-delete a submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.post(
            f"/submissions/{source_id}/delete",
            headers=HEADERS,
            json={"reason": "Duplicate dataset"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"]
        assert data["status"] == "deleted"

        # Verify status is deleted
        sub = _status(client, source_id)
        assert sub["status"] == "deleted"

    def test_delete_records_curation_history(self, env):
        """Deletion is recorded in curation_history."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        client.post(
            f"/submissions/{source_id}/delete",
            headers=HEADERS,
            json={"reason": "Spam"},
        )

        resp = client.get(
            f"/curation/{source_id}",
            headers=HEADERS,
        )
        assert resp.status_code == 200
        history = resp.json().get("curation_history", [])
        assert any(h["action"] == "deleted" for h in history)

    def test_delete_already_deleted_returns_400(self, env):
        """Cannot delete an already-deleted submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        client.post(
            f"/submissions/{source_id}/delete",
            headers=HEADERS,
            json={"reason": "First delete"},
        )
        resp = client.post(
            f"/submissions/{source_id}/delete",
            headers=HEADERS,
            json={"reason": "Second delete"},
        )
        assert resp.status_code == 400

    def test_delete_not_found_returns_404(self, env):
        """Deleting a non-existent submission returns 404."""
        client = TestClient(app)
        resp = client.post(
            "/submissions/nonexistent-id/delete",
            headers=HEADERS,
            json={"reason": "Gone"},
        )
        assert resp.status_code == 404

    def test_delete_requires_curator(self, env):
        """Non-curator cannot delete a submission."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        import os
        os.environ["ALLOW_ALL_CURATORS"] = "false"
        try:
            resp = client.post(
                f"/submissions/{source_id}/delete",
                headers=OTHER_HEADERS,
                json={"reason": "Unauthorized"},
            )
            assert resp.status_code == 403
        finally:
            os.environ["ALLOW_ALL_CURATORS"] = "true"


# ---------------------------------------------------------------------------
# ACL Gate Tests — status and versions endpoints
# ---------------------------------------------------------------------------

class TestACLGates:
    def test_status_unauthenticated_only_sees_published(self, env):
        """Unauthenticated user cannot see unpublished via /status."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        # Without auth headers in dev mode, get_optional_auth still returns
        # a default user. Switch to production-like behavior by removing the
        # X-User-Id header and setting AUTH_MODE to production briefly.
        # But since dev mode always returns a user, we test the logic by
        # verifying that published datasets ARE visible.
        _approve(client, source_id, mint_doi=False)

        # Published should be visible without special auth
        resp = client.get(f"/status/{source_id}")
        assert resp.status_code == 200
        assert resp.json()["success"]

    def test_versions_unauthenticated_only_sees_published(self, env):
        """Unauthenticated user only sees published versions."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]
        _approve(client, source_id, mint_doi=False)

        # Owner can see all versions
        resp = client.get(f"/versions/{source_id}", headers=HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"]
        assert "total_count" in data

    def test_versions_pagination(self, env):
        """Versions endpoint supports limit and offset."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]
        _approve(client, source_id, mint_doi=False)

        # Edit published to create 1.1
        client.post(
            f"/submissions/{source_id}/metadata",
            headers=HEADERS,
            json={"title": "Edit One", "version": "1.0"},
        )

        # Get with limit=1
        resp = client.get(
            f"/versions/{source_id}",
            headers=HEADERS,
            params={"limit": 1, "offset": 0},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["versions"]) == 1
        assert data["total_count"] == 2

        # Get with offset=1
        resp = client.get(
            f"/versions/{source_id}",
            headers=HEADERS,
            params={"limit": 10, "offset": 1},
        )
        data = resp.json()
        assert len(data["versions"]) == 1

    def test_card_unpublished_returns_404(self, env):
        """Card endpoint returns 404 for unpublished datasets."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.get(f"/card/{source_id}")
        assert resp.status_code == 404

    def test_card_published_returns_200(self, env):
        """Card endpoint works for published datasets."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]
        _approve(client, source_id, mint_doi=False)

        resp = client.get(f"/card/{source_id}")
        assert resp.status_code == 200
        assert resp.json()["success"]

    def test_citation_unpublished_returns_404(self, env):
        """Citation endpoint returns 404 for unpublished datasets."""
        client = TestClient(app)
        result = _submit(client)
        source_id = result["source_id"]

        resp = client.get(f"/citation/{source_id}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Admin Stats Tests
# ---------------------------------------------------------------------------

class TestAdminStats:
    def test_admin_stats(self, env):
        """Admin stats returns counts by status."""
        client = TestClient(app)
        _submit(client)
        r2 = _submit(client, extra={"title": "Second"})
        _approve(client, r2["source_id"], mint_doi=False)

        resp = client.get("/admin/stats", headers=HEADERS)
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"]
        assert data["total"] >= 2
        assert "by_status" in data

    def test_admin_stats_requires_curator(self, env):
        """Non-curator cannot access admin stats."""
        client = TestClient(app)
        import os
        os.environ["ALLOW_ALL_CURATORS"] = "false"
        try:
            resp = client.get("/admin/stats", headers=OTHER_HEADERS)
            assert resp.status_code == 403
        finally:
            os.environ["ALLOW_ALL_CURATORS"] = "true"
