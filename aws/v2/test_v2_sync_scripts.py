from __future__ import annotations

import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

import extract_mdf_production_datasets as extract  # noqa: E402
import ingest_converted_datasets as ingest  # noqa: E402
import reconcile_migration as reconcile_script  # noqa: E402
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
