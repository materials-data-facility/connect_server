"""Narrow tests for the sparse curation queue and DynamoDB index template."""

from pathlib import Path

import pytest

from v2.store import SqliteSubmissionStore


def _record(source_id, status, updated_at):
    return {
        "source_id": source_id,
        "version": "1.0",
        "user_id": "user-1",
        "organization": "org-1",
        "status": status,
        "title": f"Title for {source_id}",
        "dataset_mdata": {"title": f"Metadata for {source_id}"},
        "created_at": updated_at,
        "updated_at": updated_at,
    }


def test_sqlite_curation_queue_is_set_and_cleared_on_transitions(tmp_path):
    store = SqliteSubmissionStore(str(tmp_path / "store.db"))
    store.put_submission(_record("dataset-a", "pending_curation", "2026-01-01T00:00:00Z"))

    pending = store.get_submission("dataset-a", "1.0")
    assert pending["curation_queue"] == "pending_curation"

    store.update_status("dataset-a", "1.0", "approved")
    assert store.get_submission("dataset-a", "1.0")["curation_queue"] == "approved"

    store.update_status("dataset-a", "1.0", "published")
    published = store.get_submission("dataset-a", "1.0")
    assert published["status"] == "published"
    assert published["curation_queue"] is None

    published["status"] = "rejected"
    published["curation_queue"] = "published"
    store.upsert_submission(published)
    assert store.get_submission("dataset-a", "1.0")["curation_queue"] == "rejected"

    for terminal_status in ("withdrawn", "deleted"):
        store.update_status("dataset-a", "1.0", terminal_status)
        assert store.get_submission("dataset-a", "1.0")["curation_queue"] is None


def test_sqlite_list_by_status_matches_sparse_queue_and_returns_full_rows(tmp_path):
    store = SqliteSubmissionStore(str(tmp_path / "store.db"))
    records = [
        _record("pending", "pending_curation", "2026-01-01T00:00:00Z"),
        _record("approved", "approved", "2026-01-02T00:00:00Z"),
        _record("rejected", "rejected", "2026-01-03T00:00:00Z"),
        _record("published", "published", "2026-01-04T00:00:00Z"),
    ]
    for record in records:
        store.put_submission(record)

    queued = store.list_by_status(
        ["pending_curation", "approved", "rejected"], limit=10
    )
    assert [item["source_id"] for item in queued] == [
        "rejected",
        "approved",
        "pending",
    ]
    assert queued[0]["dataset_mdata"]["title"] == "Metadata for rejected"
    assert store.list_by_status([], limit=10) == []

    with pytest.raises(NotImplementedError, match="scan-free only"):
        store.list_by_status(["published"])


def test_template_uses_sparse_projected_indexes_and_table_protection():
    template = (Path(__file__).parents[1] / "template.yaml").read_text()

    assert "IndexName: status-submissions" not in template
    assert "IndexName: curation-queue-index" in template
    assert "- AttributeName: curation_queue" in template
    # Pre-existing GSIs cannot change projection in place, so they stay ALL;
    # only the new curation index is slimmed (KEYS_ONLY + store-side hydration).
    assert template.count("ProjectionType: INCLUDE") == 0
    assert template.count("ProjectionType: KEYS_ONLY") == 1
    assert template.count("DeletionProtectionEnabled: true") == 2
    assert "GSI_CURATION_INDEX" not in template  # store default is the contract


def test_template_scopes_email_and_enables_gateway_access_logs():
    template = (Path(__file__).parents[1] / "template.yaml").read_text()

    ses_identity = (
        'Resource: !Sub "arn:aws:ses:${AWS::Region}:'
        '${AWS::AccountId}:identity/*"'
    )
    assert template.count(ses_identity) == 2
    assert template.count("- ses:SendRawEmail") == 2
    assert "AccessLogSettings:" in template
    assert "RetentionInDays: !Ref LogRetentionDays" in template
    for field in (
        "requestId",
        "ip",
        "requestTime",
        "httpMethod",
        "routeKey",
        "status",
        "responseLength",
        "integrationLatency",
    ):
        assert f'"{field}"' in template
