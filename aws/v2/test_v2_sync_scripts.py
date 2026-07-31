from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

import extract_mdf_production_datasets as extract  # noqa: E402
import ingest_converted_datasets as ingest  # noqa: E402
import legacy_auth  # noqa: E402
import reconcile_migration as reconcile_script  # noqa: E402
import sync_prod_to_v2 as sync_script  # noqa: E402
from v2.store import SqliteSubmissionStore  # noqa: E402


class RecordingSearch:
    def __init__(self):
        self.submissions = []

    def batch_ingest(self, submissions, batch_size=100):
        self.submissions.extend(copy.deepcopy(submissions))
        return {
            "success": True,
            "total": len(submissions),
            "ingested": len(submissions),
            "errors": [],
            "task_ids": ["accepted"],
        }


@pytest.fixture
def sync_runtime(tmp_path, monkeypatch):
    store = SqliteSubmissionStore(str(tmp_path / "sync.db"))
    search = RecordingSearch()
    monkeypatch.setattr("v2.store.get_store", lambda: store)
    monkeypatch.setattr("v2.search_client.get_search_client", lambda: search)
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    return store, search


def converted_record(
    source_id="dataset",
    version="1.0",
    title="Original",
    *,
    latest=True,
):
    return {
        "source_id": source_id,
        "legacy_source_id": "{}_v{}".format(
            source_id, version.split(".")[0]
        ),
        "source_name": source_id,
        "version": version,
        "ingest_date": "2026-01-01T00:00:00Z",
        "doi": None,
        "endpoint_path": "globus://endpoint/path/",
        "metadata": {
            "title": title,
            "authors": [{"name": "Ada Lovelace"}],
            "version": version,
            "latest": latest,
            "root_version": source_id,
        },
    }


def test_create_unchanged_update_conflict_matrix(sync_runtime):
    store, search = sync_runtime
    original = converted_record()

    created = ingest.ingest_records([original])
    assert (created["created"], created["updated"], created["unchanged"]) == (
        1,
        0,
        0,
    )
    stored = store.get_submission("dataset", "1.0")
    assert stored["search_synced_hash"] == stored["sync_content_hash"]
    assert stored["last_synced_at"]

    unchanged = ingest.ingest_records([copy.deepcopy(original)])
    assert unchanged["unchanged"] == 1
    assert unchanged["search_required"] == 0

    stored["title_description_embedding"] = [0.1, 0.2]
    stored["embedding_model"] = "old-model"
    stored["embedding_generated_at"] = "2026-01-01T01:00:00Z"
    stored["view_count"] = 7
    store.upsert_submission(stored)

    changed = converted_record(title="From v1 delta")
    updated = ingest.ingest_records([changed])
    assert updated["updated"] == 1
    stored = store.get_submission("dataset", "1.0")
    assert stored["dataset_mdata"]["title"] == "From v1 delta"
    assert stored["view_count"] == 7
    assert stored["title_description_embedding"] is None
    assert stored["embedding_model"] is None
    assert stored["embedding_generated_at"] is None
    assert not stored.get("metadata_updated_at")

    stored["user_id"] = "v2-user"
    store.upsert_submission(stored)
    conflicted = ingest.ingest_records(
        [converted_record(title="Must not overwrite")]
    )
    assert conflicted["conflicts"] == 1
    assert conflicted["created"] == conflicted["updated"] == 0
    assert (
        store.get_submission("dataset", "1.0")["dataset_mdata"]["title"]
        == "From v1 delta"
    )
    assert len(search.submissions) == 2


def test_v2_touch_on_any_version_freezes_entire_dataset(sync_runtime):
    store, search = sync_runtime
    version_one = converted_record(version="1.0", title="One", latest=True)
    version_two = converted_record(version="2.0", title="Two", latest=False)

    initial = ingest.ingest_records([version_one, version_two])
    assert initial["created"] == 2
    # Batch flags lie, but the store-derived numeric latest owns Search.
    assert search.submissions[-1]["version"] == "2.0"

    touched = store.get_submission("dataset", "1.0")
    touched["user_id"] = "human-editor"
    store.upsert_submission(touched)

    incoming_one = converted_record(
        version="1.0", title="Changed one", latest=False
    )
    incoming_two = converted_record(
        version="2.0", title="Changed two", latest=True
    )
    result = ingest.ingest_records([incoming_one, incoming_two])

    assert result["conflicts"] == 1
    assert result["conflict_records"][0]["source_id"] == "dataset"
    assert result["created"] == result["updated"] == 0
    assert store.get_submission("dataset", "1.0")["dataset_mdata"]["title"] == "One"
    assert store.get_submission("dataset", "2.0")["dataset_mdata"]["title"] == "Two"


def test_search_hash_retries_unchanged_skip_search_record(sync_runtime):
    store, search = sync_runtime
    record = converted_record()

    skipped = ingest.ingest_records([record], skip_search=True)
    assert skipped["created"] == 1
    assert skipped["search_pending"] == 1
    stored = store.get_submission("dataset", "1.0")
    assert stored["sync_content_hash"]
    assert stored.get("search_synced_hash") is None

    retried = ingest.ingest_records([copy.deepcopy(record)])
    assert retried["unchanged"] == 1
    assert retried["search_required"] == 1
    assert retried["search_ingested"] == 1
    assert retried["search_pending"] == 0
    stored = store.get_submission("dataset", "1.0")
    assert stored["search_synced_hash"] == stored["sync_content_hash"]
    assert search.submissions[-1]["source_id"] == "dataset"


def test_watermark_gates_and_conflict_exit_code():
    args = SimpleNamespace(
        dry_run=False,
        limit=0,
        skip_store=False,
        skip_search=True,
    )
    stats = {
        "conflicts": 0,
        "search_required": 1,
        "search_client_is_real": None,
        "search_pending": 1,
    }
    reasons = ingest._watermark_blocked_reasons(
        args, "2026-01-01T00:00:00+00:00", stats, False
    )
    assert "--skip-search was used" in reasons
    assert any("Search-pending" in reason for reason in reasons)

    args.skip_search = False
    stats.update(
        conflicts=1,
        search_client_is_real=False,
        search_pending=0,
    )
    reasons = ingest._watermark_blocked_reasons(
        args, "2026-01-01T00:00:00+00:00", stats, False
    )
    assert any("unresolved conflict" in reason for reason in reasons)
    assert any("not real Globus Search" in reason for reason in reasons)
    assert ingest._result_exit_code(
        dry_run=False, has_errors=False, conflicts=1
    ) == 3
    assert ingest._result_exit_code(
        dry_run=True, has_errors=True, conflicts=1
    ) == 0


def test_limited_extract_suppresses_candidate_watermark():
    from datetime import datetime, timezone

    max_dt = datetime(2026, 1, 2, tzinfo=timezone.utc)
    limited = extract._candidate_watermark_fields(max_dt, limit=10)
    assert limited == {
        "candidate_watermark": None,
        "candidate_watermark_suppressed": "limit",
    }
    complete = extract._candidate_watermark_fields(max_dt, limit=None)
    assert complete["candidate_watermark"] == "2026-01-02T00:00:00+00:00"
    assert complete["candidate_watermark_suppressed"] is None


def test_sqlite_upsert_round_trips_sync_fields(tmp_path):
    store = SqliteSubmissionStore(str(tmp_path / "roundtrip.db"))
    record = {
        "source_id": "roundtrip",
        "version": "1.0",
        "status": "published",
        "dataset_mdata": {"title": "Round trip", "authors": [{"name": "A"}]},
        "sync_content_hash": "content",
        "search_synced_hash": "search",
        "last_synced_at": "2026-01-01T00:00:00Z",
    }
    store.upsert_submission(record)
    loaded = store.get_submission("roundtrip", "1.0")
    loaded["status"] = "approved"
    store.upsert_submission(loaded)
    final = store.get_submission("roundtrip", "1.0")
    assert final["sync_content_hash"] == "content"
    assert final["search_synced_hash"] == "search"
    assert final["last_synced_at"] == "2026-01-01T00:00:00Z"


def test_sync_hash_excludes_batch_relative_latest_and_root():
    first = converted_record(latest=True)
    second = copy.deepcopy(first)
    second["metadata"]["latest"] = False
    second["metadata"]["root_version"] = "batch-specific-root"
    assert ingest.compute_sync_content_hash(first) == ingest.compute_sync_content_hash(
        second
    )


def test_reconcile_separates_never_hashed_stale_and_search_pending(
    tmp_path, monkeypatch
):
    store = SqliteSubmissionStore(str(tmp_path / "reconcile.db"))
    monkeypatch.setattr("v2.store.get_store", lambda: store)
    records = [
        converted_record("never"),
        converted_record("stale"),
        converted_record("pending"),
    ]

    never = ingest.build_submission_record(records[0])
    never.pop("sync_content_hash")
    store.upsert_submission(never)

    stale = ingest.build_submission_record(records[1])
    stale["sync_content_hash"] = "old-content"
    stale["search_synced_hash"] = "old-content"
    store.upsert_submission(stale)

    pending = ingest.build_submission_record(records[2])
    store.upsert_submission(pending)

    report = reconcile_script.reconcile(records)
    assert report["never_hashed"] == 1
    assert report["stale_content"] == 1
    assert report["search_pending"] == 1


class FakeClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeSSM:
    def __init__(self):
        self.parameters = {}
        self.deletes = []

    def put_parameter(self, *, Name, Value, Type, Overwrite):
        assert Type == "String"
        if Name in self.parameters and not Overwrite:
            raise FakeClientError("ParameterAlreadyExists")
        self.parameters[Name] = Value
        return {"Version": 1}

    def get_parameter(self, *, Name):
        if Name not in self.parameters:
            raise FakeClientError("ParameterNotFound")
        return {"Parameter": {"Value": self.parameters[Name]}}

    def delete_parameter(self, *, Name):
        self.deletes.append(Name)
        self.parameters.pop(Name, None)


def test_ssm_lock_acquire_held_stale_and_release():
    ssm = FakeSSM()
    now = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
    lock_name = "/mdf/staging/sync-lock"

    sync_script.acquire_ssm_lock(
        ssm, "staging", "github-actions", now=now
    )
    lock = json.loads(ssm.parameters[lock_name])
    assert lock == {
        "acquired_at": "2026-07-30T12:00:00+00:00",
        "run_source": "github-actions",
    }

    with pytest.raises(sync_script.LockHeldError, match="github-actions"):
        sync_script.acquire_ssm_lock(
            ssm, "staging", "manual", now=now + timedelta(minutes=30)
        )

    sync_script.acquire_ssm_lock(
        ssm, "staging", "manual", now=now + timedelta(hours=3)
    )
    replacement = json.loads(ssm.parameters[lock_name])
    assert replacement["run_source"] == "manual"
    assert ssm.deletes == [lock_name]

    sync_script.release_ssm_lock(ssm, "staging")
    assert lock_name not in ssm.parameters
    assert ssm.deletes == [lock_name, lock_name]


def test_ssm_report_writer_uses_pinned_contract():
    ssm = FakeSSM()
    report = {
        "status": "success",
        "started_at": "2026-07-30T12:00:00+00:00",
        "finished_at": "2026-07-30T12:01:00+00:00",
        "counts": {
            "created": 1,
            "updated": 2,
            "unchanged": 3,
            "conflicts": 0,
            "search_pending": 0,
            "store_errors": 0,
            "search_errors": 0,
        },
        "watermark": "2026-07-30T11:59:00+00:00",
        "run_source": "github-actions",
    }

    sync_script.write_ssm_report(ssm, "prod", report)

    saved = json.loads(ssm.parameters["/mdf/prod/sync-last-report"])
    assert saved == report


def test_legacy_auth_prefers_confidential_credentials(monkeypatch):
    calls = []
    marker = object()

    class FakeConfidentialClient:
        def __init__(self, client_id, client_secret):
            calls.append(("confidential", client_id, client_secret))

    class FakeClientCredentialsAuthorizer:
        def __init__(self, client, scope):
            calls.append(("authorizer", client, scope))

    fake_sdk = SimpleNamespace(
        ConfidentialAppAuthClient=FakeConfidentialClient,
        ClientCredentialsAuthorizer=FakeClientCredentialsAuthorizer,
        SearchClient=lambda authorizer: (
            calls.append(("search", authorizer)) or marker
        ),
        NativeAppAuthClient=lambda *_: pytest.fail(
            "interactive auth must not be selected"
        ),
    )
    monkeypatch.setitem(sys.modules, "globus_sdk", fake_sdk)
    monkeypatch.setenv("GLOBUS_CLIENT_ID", "client-id")
    monkeypatch.setenv("GLOBUS_CLIENT_SECRET", "client-secret")

    assert legacy_auth.get_legacy_search_client(interactive_ok=True) is marker
    assert calls[0] == ("confidential", "client-id", "client-secret")
    assert calls[1][0] == "authorizer"
    assert calls[1][2] == legacy_auth.SEARCH_SCOPE
    assert calls[2][0] == "search"


def _legacy_gmeta(source_name, title, doi, authors):
    return {
        "subject": source_name,
        "entries": [
            {
                "content": {
                    "mdf": {
                        "resource_type": "dataset",
                        "source_id": "{}_v1".format(source_name),
                        "source_name": source_name,
                        "version": 1,
                        "ingest_date": "2026-07-01T00:00:00Z",
                    },
                    "dc": {
                        "titles": [{"title": title}],
                        "creators": [
                            {"creatorName": name} for name in authors
                        ],
                        "identifier": {
                            "identifier": doi,
                            "identifierType": "DOI",
                        },
                    },
                }
            }
        ],
    }


def test_reconcile_legacy_check_presence_and_field_sample():
    legacy_entries = [
        _legacy_gmeta("alpha", "Alpha", "10.1234/alpha", ["A"]),
        _legacy_gmeta("beta", "Beta legacy", "10.1234/beta", ["B", "C"]),
        _legacy_gmeta("missing", "Missing", "10.1234/missing", ["D"]),
    ]

    class LegacyClient:
        def search(self, index_id, query, **kwargs):
            assert index_id == extract.MDF_PRODUCTION_INDEX
            assert query == 'mdf.resource_type:"dataset"'
            return {"total": 3, "gmeta": legacy_entries}

    class Store:
        def list_all(self, limit):
            assert limit >= 3
            return [
                {
                    "source_id": "alpha",
                    "version": "1.0",
                    "legacy_source_id": "alpha_v1",
                    "doi": "https://doi.org/10.1234/alpha",
                    "dataset_mdata": {
                        "title": "Alpha",
                        "authors": [{"name": "A"}],
                    },
                },
                {
                    "source_id": "beta",
                    "version": "1.0",
                    "user_id": "v1-migration",
                    "doi": "10.1234/beta",
                    "dataset_mdata": {
                        "title": "Beta changed",
                        "authors": [{"name": "B"}],
                    },
                },
                {
                    "source_id": "native-v2",
                    "version": "1.0",
                    "user_id": "someone",
                    "dataset_mdata": {"title": "Not migrated"},
                },
            ]

    report = reconcile_script.reconcile_legacy(
        LegacyClient(), sample=25, store=Store()
    )

    assert report["legacy_total"] == 3
    assert report["migrated_total"] == 2
    assert report["missing_from_v2"] == ["missing"]
    assert report["missing_from_v2_count"] == 1
    assert report["sampled"] == 2
    assert report["field_mismatches"] == 1
    mismatch = report["field_mismatch_records"][0]
    assert mismatch["source_name"] == "beta"
    assert set(mismatch["fields"]) == {"title", "author_count"}


def test_sync_no_ssm_dry_run_end_to_end(tmp_path):
    extract_file = tmp_path / "tiny-extract.json"
    extract_file.write_text(
        json.dumps(
            {
                "source_index": extract.MDF_PRODUCTION_INDEX,
                "query": 'mdf.resource_type:"dataset"',
                "total_in_index": 1,
                "fetched_count": 1,
                "matched_count": 1,
                "candidate_watermark": "2026-07-01T00:00:00+00:00",
                "candidate_watermark_suppressed": None,
                "gmeta": [
                    _legacy_gmeta(
                        "dry-run-dataset",
                        "Dry run dataset",
                        "10.1234/dry-run",
                        ["Ada Lovelace"],
                    )
                ],
            }
        )
    )
    state_dir = tmp_path / "state"
    report_file = tmp_path / "report.json"
    environment = dict(os.environ)
    environment.update(
        {
            "STORE_BACKEND": "sqlite",
            "SQLITE_PATH": str(tmp_path / "dry-run.db"),
            "AUTH_MODE": "dev",
            "USE_MOCK_DATACITE": "true",
            "USE_MOCK_SEARCH": "true",
            "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            str(Path(sync_script.__file__).resolve()),
            "--env",
            "dev",
            "--no-ssm",
            "--local-config",
            "--dry-run",
            "--state-dir",
            str(state_dir),
            "--from-extract-file",
            str(extract_file),
            "--report-file",
            str(report_file),
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr + result.stdout
    print(result.stdout.rstrip())
    report = json.loads(report_file.read_text())
    assert report["status"] == "success"
    assert report["counts"]["created"] == 1
    assert not (state_dir / "sync-watermark").exists()
    assert not (state_dir / "sync-lock").exists()
    assert json.loads((state_dir / "sync-last-report").read_text()) == report
