from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.routers import files as files_router
from v2.app.middleware import reset_middleware_state
from v2.async_jobs import SqliteJobDispatcher, enqueue_transfer_job
from v2.clone import StreamCloner
from v2.storage import reset_storage_backend
from v2.storage.globus_https import GlobusHTTPSStorage
from v2.storage.local import LocalStorage
from v2.stream_store import SqliteStreamStore
from v2.store import SqliteSubmissionStore, SubmissionStore


def _files_client() -> TestClient:
    # The files router is unmounted from the main app while streams are
    # disabled (B-1); mount it on an isolated app so its authz invariants
    # stay covered until the feature returns.
    files_app = FastAPI()
    files_app.include_router(files_router.router)
    return TestClient(files_app)


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

    client = _files_client()
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

    client = _files_client()
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


# ---------------------------------------------------------------------------
# Baseline security response headers
# ---------------------------------------------------------------------------

def test_responses_carry_baseline_security_headers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    reset_storage_backend()
    reset_middleware_state()

    resp = TestClient(app).get("/")
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["Referrer-Policy"] == "no-referrer"
    assert "max-age=31536000" in resp.headers["Strict-Transport-Security"]


# ---------------------------------------------------------------------------
# OpenAI-backed endpoints are not anonymous
# ---------------------------------------------------------------------------

def test_openai_backed_endpoints_require_auth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """/embed and /search/semantic spend the server's OpenAI key per call.

    Anonymous access is an unmetered bill; anonymous keyword /search is not
    affected and must stay open.
    """
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    monkeypatch.setenv("AUTH_MODE", "production")
    monkeypatch.delenv("LOCAL_DEV_AUTH", raising=False)
    monkeypatch.delenv("AWS_SAM_LOCAL", raising=False)
    reset_storage_backend()
    reset_middleware_state()

    client = TestClient(app)
    assert client.post("/embed", json={"text": "hello"}).status_code == 401
    assert client.get("/search/semantic", params={"q": "hello"}).status_code == 401
    assert client.get("/search", params={"q": "hello"}).status_code == 200


# ---------------------------------------------------------------------------
# DataCite client selection
# ---------------------------------------------------------------------------

def test_datacite_client_fails_loud_when_mock_disabled_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
):
    """Silently mocking in a real environment mints fake DOIs and reports success."""
    from v2.datacite import MockDataCiteClient, get_datacite_client

    monkeypatch.delenv("DATACITE_USERNAME", raising=False)
    monkeypatch.delenv("DATACITE_PASSWORD", raising=False)

    monkeypatch.setenv("USE_MOCK_DATACITE", "false")
    with pytest.raises(RuntimeError, match="DataCite credentials required"):
        get_datacite_client()

    # Explicit mock and auto/unset both keep local dev and tests working.
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    assert isinstance(get_datacite_client(), MockDataCiteClient)
    monkeypatch.setenv("USE_MOCK_DATACITE", "")
    assert isinstance(get_datacite_client(), MockDataCiteClient)


# ---------------------------------------------------------------------------
# Search document shape (v1 index parity)
# ---------------------------------------------------------------------------

def test_gmeta_entry_writes_resource_type_and_stable_source_name():
    """v1 tooling filters on mdf.resource_type and groups on mdf.source_name.

    The old rsplit("-", 1) derivation turned every "mdf-<uuid>" id into
    source_name="mdf", and resource_type was never written at all — so a v1
    parity/migration query against the v2 index returned zero rows.
    """
    import json as _json

    from v2.search_client import GlobusSearchClient

    client = GlobusSearchClient.__new__(GlobusSearchClient)

    migrated = {
        "source_id": "mdf-abc123def456",
        "version": "1.0",
        "organization": "MDF Open",
        "dataset_mdata": _json.dumps({
            "title": "T", "authors": [{"name": "A"}], "acl": ["public"],
            "extensions": {"mdf_source_name": "pub_42_smith"},
        }),
    }
    mdf = client.build_gmeta_entry(migrated)["content"]["mdf"]
    assert mdf["resource_type"] == "dataset"
    assert mdf["source_name"] == "pub_42_smith"

    native = {
        "source_id": "mdf-deadbeefcafe",
        "version": "1.0",
        "dataset_mdata": _json.dumps({"title": "T2", "authors": [{"name": "A"}], "acl": ["public"]}),
    }
    mdf_native = client.build_gmeta_entry(native)["content"]["mdf"]
    assert mdf_native["resource_type"] == "dataset"
    assert mdf_native["source_name"] == "mdf-deadbeefcafe"


# ---------------------------------------------------------------------------
# Public-visibility rule shared by search, cards, citations and previews
# ---------------------------------------------------------------------------

def test_dataset_is_public_matches_the_globus_visible_to_rule():
    import json as _json

    from v2.search import dataset_is_public

    assert dataset_is_public({"dataset_mdata": _json.dumps({"acl": ["public"]})}) is True
    # No acl means public, matching build_gmeta_entry's `meta.acl or ["public"]`.
    assert dataset_is_public({"dataset_mdata": _json.dumps({})}) is True
    assert dataset_is_public(
        {"dataset_mdata": _json.dumps({"acl": ["urn:globus:auth:identity:x"]})}
    ) is False
    # Fails closed on unparseable metadata.
    assert dataset_is_public({"dataset_mdata": object()}) is False


# ---------------------------------------------------------------------------
# dataset_mdata is always stored as a JSON string
# ---------------------------------------------------------------------------

def test_curation_approve_stores_dataset_mdata_as_json_string(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """DynamoDB's put_item stores whatever it is given.

    The sqlite backend normalizes a dict to a JSON string, so only the record
    handed to the store reveals the divergence: on DynamoDB a raw dict becomes a
    Map on this path and a String on every other write path.
    """
    from v2.app.deps import get_submission_store
    from v2.store import get_store

    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(tmp_path / "files"))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "true")
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    reset_storage_backend()
    reset_middleware_state()

    written = []

    class _RecordingStore:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def upsert_submission(self, record):
            written.append(dict(record))
            return self._inner.upsert_submission(record)

    app.dependency_overrides[get_submission_store] = lambda: _RecordingStore(get_store())
    try:
        client = TestClient(app)
        submit = client.post(
            "/submit",
            headers={"X-User-Id": "submitter"},
            json={"title": "Dataset", "authors": [{"name": "A"}],
                  "data_sources": ["https://example.com/a.csv"]},
        )
        assert submit.status_code == 200
        source_id = submit.json()["source_id"]

        approve = client.post(
            f"/curation/{source_id}/approve",
            headers={"X-User-Id": "curator"},
            json={"mint_doi": False, "metadata_updates": {"description": "Curator note"}},
        )
        assert approve.status_code == 200, approve.json()
    finally:
        app.dependency_overrides.pop(get_submission_store, None)

    assert written, "approve should have written the submission"
    for record in written:
        assert isinstance(record["dataset_mdata"], str), record["dataset_mdata"]
    assert "Curator note" in written[0]["dataset_mdata"]
