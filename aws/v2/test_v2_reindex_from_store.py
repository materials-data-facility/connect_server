"""reindex_search_from_store: selection rule, guards, and the ingest loop."""

import pytest

from v2.scripts import reindex_search_from_store as reindex


def _rec(source_id, version, status="published", **extra):
    return {"source_id": source_id, "version": version, "status": status, **extra}


def test_selects_newest_published_version_per_dataset():
    records = [
        _rec("a", "1.0"),
        _rec("a", "1.10"),  # numeric-aware: 1.10 > 1.9
        _rec("a", "1.9"),
        _rec("a", "2.0", status="pending_curation"),  # never indexed
        _rec("b", "1.0", status="rejected"),
        _rec("c", "1.0", acl=["urn:globus:auth:identity:x"]),  # restricted: still indexed
    ]
    selected, stats = reindex.select_index_records(records)

    assert [(r["source_id"], r["version"], n) for r, n in selected] == [
        ("a", "1.10", 4),
        ("c", "1.0", 1),
    ]
    assert stats["datasets"] == 3
    assert stats["datasets_without_published_version"] == 1
    assert stats["entries_to_ingest"] == 2


def test_rows_without_keys_are_ignored():
    selected, stats = reindex.select_index_records([{"status": "published"}, _rec("a", "1.0")])
    assert [r["source_id"] for r, _ in selected] == ["a"]
    assert stats["datasets"] == 1


def test_refuses_legacy_index(monkeypatch):
    monkeypatch.setenv("SEARCH_INDEX_UUID", reindex.LEGACY_SEARCH_INDEX_UUID)
    assert reindex.main(["--env", "staging", "--no-stack", "--execute"]) == 2


def test_prod_execute_requires_allow_prod(monkeypatch):
    monkeypatch.setenv("SEARCH_INDEX_UUID", "some-prod-index")
    assert reindex.main(["--env", "prod", "--no-stack", "--execute"]) == 2


def test_dry_run_reports_without_ingesting(monkeypatch, capsys):
    monkeypatch.setenv("SEARCH_INDEX_UUID", "staging-index")

    class Store:
        def list_all(self, limit):
            return [_rec("a", "1.0"), _rec("a", "1.1"), _rec("b", "1.0", status="rejected")]

    monkeypatch.setattr("v2.store.get_store", lambda: Store())
    monkeypatch.setattr(
        "v2.search_client.get_search_client",
        lambda: pytest.fail("dry run must not touch Search"),
    )
    assert reindex.main(["--env", "staging", "--no-stack"]) == 0
    out = capsys.readouterr().out
    assert "entries_to_ingest: 1" in out
    assert "DRY RUN" in out


def test_ingest_batches_waits_for_tasks_and_passes_version_count():
    built = []

    class Resp:
        def __init__(self, task_id):
            self.data = {"task_id": task_id}

    class Client:
        def __init__(self):
            self.docs = []

        def ingest(self, index_id, doc):
            self.docs.append(doc)
            return Resp(f"task-{len(self.docs)}")

    class Search:
        index_id = "idx"

        def __init__(self):
            self.client = Client()

        def _get_client(self):
            return self.client

        def build_gmeta_entry(self, record, version_count=None):
            if record["source_id"] == "bad":
                raise ValueError("unbuildable")
            built.append((record["source_id"], version_count))
            return {"subject": record["source_id"]}

        def _wait_for_task(self, task_id, deadline):
            return {"state": "FAILED" if task_id == "task-2" else "SUCCESS"}

    search = Search()
    selected = [(_rec("a", "1.1"), 2), (_rec("bad", "1.0"), 1), (_rec("c", "1.0"), 1)]
    result = reindex._ingest(search, selected, batch_size=2, wait_seconds=1)

    assert built == [("a", 2), ("c", 1)]
    assert len(search.client.docs) == 2
    assert result["accepted"] == 2
    assert [e["source_id"] for e in result["errors"]] == ["bad"]
    assert result["unconfirmed_tasks"] == [{"task_id": "task-2", "state": "FAILED"}]
