from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.async_jobs import SqliteJobDispatcher, enqueue_transfer_job
from v2.clone import StreamCloner
from v2.storage import reset_storage_backend
from v2.storage.globus_https import GlobusHTTPSStorage
from v2.storage.local import LocalStorage
from v2.stream_store import SqliteStreamStore
from v2.store import SqliteSubmissionStore, SubmissionStore


class _DummyStore(SubmissionStore):
    def get_submission(self, source_id, version):
        return {"source_id": source_id, "version": version}

    def list_versions(self, source_id):
        return [{"version": "1.9"}, {"version": "1.10"}]

    def put_submission(self, record):
        raise NotImplementedError

    def upsert_submission(self, record):
        raise NotImplementedError

    def update_status(self, source_id, version, status):
        raise NotImplementedError

    def list_by_user(self, user_id, limit=50, start_key=None):
        raise NotImplementedError

    def list_by_org(self, organization, limit=50, start_key=None):
        raise NotImplementedError

    def list_by_status(self, statuses, limit=100):
        raise NotImplementedError

    def update_profile(self, source_id, version, profile_json):
        raise NotImplementedError

    def list_all(self, limit=1000):
        raise NotImplementedError


def test_submission_store_get_uses_semver():
    store = _DummyStore()
    latest = store.get("source")
    assert latest["version"] == "1.10"


def test_sqlite_upsert_preserves_dataset_profile():
    fd, db_path = tempfile.mkstemp(prefix="mdf_v2_", suffix=".db")
    os.close(fd)
    try:
        store = SqliteSubmissionStore(path=db_path)
        record = {
            "source_id": "source-1",
            "version": "1.0",
            "versioned_source_id": "source-1-1.0",
            "user_id": "user-1",
            "user_email": "user@example.com",
            "organization": "org",
            "status": "submitted",
            "dataset_mdata": {"title": "t", "authors": [{"name": "a"}], "data_sources": ["x"]},
            "test": 0,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }
        store.put_submission(record)
        store.update_profile("source-1", "1.0", '{"total_files": 1}')
        current = store.get_submission("source-1", "1.0")
        current["status"] = "approved"
        store.upsert_submission(current)
        updated = store.get_submission("source-1", "1.0")
        assert updated["dataset_profile"]["total_files"] == 1
    finally:
        os.remove(db_path)


def test_local_storage_rejects_path_traversal(tmp_path: Path):
    storage = LocalStorage(str(tmp_path))
    with pytest.raises(ValueError):
        storage.store_file("stream-1", "../../escape.txt", b"bad")

    secret = tmp_path.parent / "secret.txt"
    secret.write_text("top-secret")
    assert storage.get_file("../secret.txt") is None
    assert storage.get_download_url("../secret.txt") is None


def test_globus_upload_url_does_not_expose_server_auth_header():
    storage = GlobusHTTPSStorage(access_token="server-secret")
    try:
        upload = storage.get_upload_url("stream-1", "file.csv", content_type="text/csv")
        assert "Authorization" not in upload.get("headers", {})
        assert upload.get("auth_type") == "bearer"
    finally:
        storage.close()


def test_stream_file_access_requires_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
    reset_storage_backend()

    client = TestClient(app)
    stream_id = "stream-secure-owner"
    stream_store = SqliteStreamStore(path=str(db_path))
    stream_store.create_stream(
        {
            "stream_id": stream_id,
            "title": "Owner stream",
            "status": "open",
            "file_count": 0,
            "total_bytes": 0,
            "last_append_at": None,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "user_id": "owner-user",
            "organization": None,
            "metadata": None,
        }
    )

    denied = client.get(f"/stream/{stream_id}/files", headers={"X-User-Id": "other-user"})
    assert denied.status_code == 403


def test_status_update_requires_curator(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
    reset_storage_backend()

    client = TestClient(app)
    submit = client.post(
        "/submit",
        headers={"X-User-Id": "submitter"},
        json={"title": "Dataset", "authors": [{"name": "A"}], "data_sources": ["https://example.com/a.csv"]},
    )
    assert submit.status_code == 200
    source_id = submit.json()["source_id"]

    denied = client.post(
        "/status/update",
        headers={"X-User-Id": "submitter"},
        json={"source_id": source_id, "version": "1.0", "status": "processing"},
    )
    assert denied.status_code == 403

    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    allowed = client.post(
        "/status/update",
        headers={"X-User-Id": "submitter"},
        json={"source_id": source_id, "version": "1.0", "status": "approved"},
    )
    assert allowed.status_code == 200


def test_clone_rejects_untrusted_host_for_token_use(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GLOBUS_HTTPS_SERVER", "data.materialsdatafacility.org")
    cloner = StreamCloner(dest_dir=".", token="dummy-token", verbose=False)
    try:
        with pytest.raises(ValueError):
            cloner._validate_globus_url("https://evil.example.org/path/file.csv")
        cloner._validate_globus_url("https://data.materialsdatafacility.org/path/file.csv")
    finally:
        cloner.close()


def test_upload_confirm_requires_existing_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
    reset_storage_backend()

    client = TestClient(app)
    stream_id = "stream-upload-confirm"
    stream_store = SqliteStreamStore(path=str(db_path))
    stream_store.create_stream(
        {
            "stream_id": stream_id,
            "title": "Owner stream",
            "status": "open",
            "file_count": 0,
            "total_bytes": 0,
            "last_append_at": None,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "user_id": "owner-user",
            "organization": None,
            "metadata": None,
        }
    )

    missing = client.post(
        f"/stream/{stream_id}/upload-confirm",
        headers={"X-User-Id": "owner-user"},
        json={"path": f"streams/{stream_id}/2026-02-06/not-there.csv", "size_bytes": 10},
    )
    assert missing.status_code == 400


def test_curation_without_version_uses_latest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    reset_storage_backend()

    store = SqliteSubmissionStore(path=str(db_path))
    common = {
        "source_id": "src-1",
        "user_id": "submitter",
        "user_email": "submitter@example.com",
        "organization": "org",
        "status": "pending_curation",
        "dataset_mdata": {"title": "Dataset", "authors": [{"name": "Author"}], "data_sources": ["https://example.com/a.csv"]},
        "test": 0,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    v1 = dict(common)
    v1.update({"version": "1.0", "versioned_source_id": "src-1-1.0"})
    v2 = dict(common)
    v2.update({"version": "1.10", "versioned_source_id": "src-1-1.10"})
    store.put_submission(v1)
    store.put_submission(v2)

    client = TestClient(app)
    resp = client.get("/curation/src-1", headers={"X-User-Id": "curator"})
    assert resp.status_code == 200
    assert resp.json()["submission"]["version"] == "1.10"


def test_submit_requires_submitter_group(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """In production auth mode the submitter group is enforced; in dev mode it's bypassed."""
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
    monkeypatch.setenv("REQUIRED_GROUP_MEMBERSHIP", "cc192dca-3751-11e8-90c1-0a7c735d220a")
    reset_storage_backend()

    client = TestClient(app)
    payload = {"title": "Dataset", "authors": [{"name": "A"}], "data_sources": ["https://example.com/a.csv"]}

    # In dev mode, group check is bypassed — submit should succeed
    resp = client.post("/submit", headers={"X-User-Id": "anybody"}, json=payload)
    assert resp.status_code == 200

    # Switch to production auth mode — without group membership, should be denied.
    # We can't do full Globus auth in tests, so we test the is_submitter function directly.
    from v2.app.auth import is_submitter
    from v2.app.models import AuthContext

    monkeypatch.setenv("AUTH_MODE", "production")

    no_groups = AuthContext(user_id="outsider", group_info={})
    assert is_submitter(no_groups) is False

    has_group = AuthContext(
        user_id="member",
        group_info={"cc192dca-3751-11e8-90c1-0a7c735d220a": {"name": "MDF"}},
    )
    assert is_submitter(has_group) is True

    # Empty REQUIRED_GROUP_MEMBERSHIP means everyone is allowed
    monkeypatch.setenv("REQUIRED_GROUP_MEMBERSHIP", "")
    assert is_submitter(no_groups) is True


def test_dev_auth_requires_explicit_local_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "store.db"
    file_store = tmp_path / "files"
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(db_path))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(file_store))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.delenv("LOCAL_DEV_AUTH", raising=False)
    monkeypatch.delenv("AWS_SAM_LOCAL", raising=False)
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
    reset_storage_backend()
    reset_middleware_state()

    client = TestClient(app)
    resp = client.get("/auth/check", headers={"X-User-Id": "spoofed-user"})
    assert resp.status_code == 401


def test_transfer_job_never_persists_user_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "jobs.db"
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "sqlite")
    monkeypatch.setenv("ASYNC_SQLITE_PATH", str(db_path))

    captured = {}

    def _fake_process_job(job_type, payload):
        captured["job_type"] = job_type
        captured["payload"] = dict(payload)
        return {"success": True}

    monkeypatch.setattr("v2.async_jobs.process_job", _fake_process_job)

    result = enqueue_transfer_job(
        source_id="src-1",
        version="1.0",
        data_sources=["globus://12345678-1234-1234-1234-123456789abc/path/data.csv"],
        user_transfer_token="super-secret-transfer-token",
        user_identity_id="user-1",
    )

    assert result["queued"] is False
    assert result["mode"] == "inline"
    assert captured["job_type"] == "transfer_data"
    assert captured["payload"]["user_transfer_token"] == "super-secret-transfer-token"

    dispatcher = SqliteJobDispatcher(db_path=str(db_path))
    conn = sqlite3.connect(dispatcher.path)
    try:
        count = conn.execute("SELECT COUNT(*) FROM async_jobs").fetchone()[0]
    finally:
        conn.close()
    assert count == 0
