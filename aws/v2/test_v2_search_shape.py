"""Pins the GMeta entry shape and the restricted-dataset boundaries around it.

Three things are load-bearing here and each has burned us:

1. ``content`` is FLAT. Facets and filters are evaluated *inside* Globus Search
   against literal field names, so the layout the builder writes and the names
   ``DEFAULT_FACETS``/``FILTER_FIELD_MAP`` bind to must agree exactly. Nothing
   in the response would look broken if they drifted — every filter would just
   silently select nothing, which is precisely the bug this replaces.
2. ``subject`` is the index's identity key. Change its spelling and every
   existing entry is orphaned: duplicated on the next publish, un-deletable by
   ``delete_entry``.
3. The public API surface (result keys, facet display names) is consumed by the
   frontend and the CLI, so it must survive an index-layout change untouched.

Plus the leak boundary: a published-but-restricted dataset must not reach the
Globus ``visible_to``, the GMeta ``content``, the author index behind
``/related``, or the browser-readable embedding snapshot.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app.routers.search import FILTER_FIELD_MAP
from v2.search_client import (
    DEFAULT_FACETS,
    DENY_ALL_PRINCIPAL,
    MockGlobusSearchClient,
    _detail_base,
    _flat_content,
    _result_from_content,
    reset_search_client,
)

RESTRICTED_IDENTITY = "11111111-2222-3333-4444-555555555555"

# The exact result keys staging serves today (GET /search?q=band&limit=1).
# root_version/download_url are conditional and checked separately.
PUBLIC_RESULT_KEYS = {
    "type", "source_id", "version", "title", "authors", "keywords",
    "description", "publication_year", "organization", "domains", "doi",
    "license", "size_bytes", "file_count", "status", "score", "latest",
}

# Facet display names the frontend and CLI key their filter UI off.
PUBLIC_FACET_NAMES = ["Year", "Organization", "Authors", "Keywords", "Domains"]


def _submission(source_id="ds-flat", *, acl=None, status="published", **overrides):
    mdata = {
        "title": "Flat Content Dataset",
        "authors": [{"name": "Blaiszik, Ben"}, {"name": "Ward, Logan"}],
        "description": "A dataset about band gaps.",
        "keywords": ["DFT", "band gap"],
        "domains": ["materials science"],
        "publication_year": 2024,
        "license": {"identifier": "CC-BY-4.0", "name": "CC BY 4.0"},
        "data_sources": ["https://example.com/data.csv"],
        "download_url": "https://data.example.com/ds-flat/",
        "version": "1.0",
    }
    if acl is not None:
        mdata["acl"] = acl
    mdata.update(overrides.pop("mdata", {}))
    record = {
        "source_id": source_id,
        "version": "1.0",
        "organization": "MDF Open",
        "status": status,
        "created_at": "2026-02-01T00:00:00Z",
        "published_at": "2026-02-01T00:00:00Z",
        "doi": "10.99999/ds-flat",
        "total_bytes": 4096,
        "file_count": 7,
        # v2.1 record shape: the version-chain pointer is a bare version string
        # in a top-level record attribute, not a composite id in the blob.
        "root_version": "1.0",
        "dataset_mdata": json.dumps(mdata),
    }
    record.update(overrides)
    return record


def _entry(**kwargs):
    return MockGlobusSearchClient().build_gmeta_entry(_submission(**kwargs))


# --- Flat content layout -----------------------------------------------------


class TestFlatContent:
    def test_content_has_no_nested_v1_blocks(self):
        content = _entry()["content"]
        for key in ("dc", "mdf", "data"):
            assert key not in content, key
        # Nothing nested at all — a stray dict would mean a field the facets
        # and filters cannot reach.
        assert not [k for k, v in content.items() if isinstance(v, dict)]

    def test_content_carries_the_v2_field_names(self):
        content = _entry()["content"]
        assert content["source_id"] == "ds-flat"
        assert content["source_name"] == "ds-flat"
        assert content["title"] == "Flat Content Dataset"
        assert content["description"] == "A dataset about band gaps."
        assert content["keywords"] == ["DFT", "band gap"]
        assert content["domains"] == ["materials science"]
        assert content["organization"] == "MDF Open"
        assert content["publication_year"] == 2024
        assert content["license"] == "CC-BY-4.0"
        assert content["doi"] == "10.99999/ds-flat"
        assert content["version"] == "1.0"
        assert content["latest"] is True
        assert content["resource_type"] == "dataset"
        assert content["status"] == "published"
        assert content["ingest_date"] == "2026-02-01T00:00:00Z"
        assert content["download_url"] == "https://data.example.com/ds-flat/"
        assert content["size_bytes"] == 4096
        assert content["file_count"] == 7
        assert content["root_version"] == "1.0"

    def test_authors_stay_family_given_strings(self):
        """Facet values are whole "Family, Given" names, not {"name": ...} dicts."""
        authors = _entry()["content"]["authors"]
        assert authors == ["Blaiszik, Ben", "Ward, Logan"]
        assert all(isinstance(a, str) for a in authors)

    def test_legacy_source_id_is_indexed_from_either_home(self):
        top_level = _entry(legacy_source_id="ds_flat_v1")["content"]
        assert top_level["legacy_source_id"] == "ds_flat_v1"

        in_extensions = _entry(
            mdata={"extensions": {"legacy_source_id": "ds_flat_v2"}},
        )["content"]
        assert in_extensions["legacy_source_id"] == "ds_flat_v2"

    def test_subject_is_unchanged(self):
        """Byte-identical or every entry already in the index is orphaned."""
        entry = _entry()
        assert entry["subject"] == f"{_detail_base()}/ds-flat"
        assert entry["subject"].endswith("/detail/ds-flat")


class TestFacetAndFilterFieldsAgree:
    def test_every_facet_field_exists_in_content(self):
        content = _entry()["content"]
        for facet in DEFAULT_FACETS:
            assert facet["field_name"] in content, facet

    def test_every_router_filter_field_is_a_facet_field(self):
        facet_fields = {facet["field_name"] for facet in DEFAULT_FACETS}
        assert set(FILTER_FIELD_MAP.values()) == facet_fields

    def test_facet_display_names_are_unchanged(self):
        assert [facet["name"] for facet in DEFAULT_FACETS] == PUBLIC_FACET_NAMES

    def test_public_filter_param_names_are_unchanged(self):
        assert set(FILTER_FIELD_MAP) == {
            "year", "organization", "author", "keyword", "domain",
        }


# --- Public API surface is unchanged by the layout change --------------------


class TestPublicResponseShape:
    def test_result_keys_match_the_deployed_contract(self):
        result = _result_from_content(_entry()["content"])
        assert PUBLIC_RESULT_KEYS <= set(result)
        assert set(result) - PUBLIC_RESULT_KEYS <= {"root_version", "download_url"}

    def test_facet_names_in_a_mock_response_are_unchanged(self):
        client = MockGlobusSearchClient()
        client.ingest(_submission())
        assert list(client.faceted_search("*")["facets"]) == PUBLIC_FACET_NAMES

    def test_flat_filter_selects_the_entry(self):
        client = MockGlobusSearchClient()
        client.ingest(_submission())
        for field, value in (
            ("authors", "Blaiszik, Ben"),
            ("keywords", "DFT"),
            ("domains", "materials science"),
            ("organization", "MDF Open"),
            ("publication_year", "2024"),
        ):
            got = client.faceted_search("*", filters={field: [value]})
            assert got["total"] == 1, (field, value)

    def test_year_facet_buckets_are_strings(self):
        """Round-trip guard: a bucket value is fed straight back as a filter."""
        client = MockGlobusSearchClient()
        client.ingest(_submission())
        buckets = client.faceted_search("*")["facets"]["Year"]
        assert buckets == [{"value": "2024", "count": 1}]


# --- Tolerant reader for pre-flatten entries ---------------------------------


NESTED_ENTRY_CONTENT = {
    "mdf": {
        "source_id": "old-ds",
        "source_name": "old-ds",
        "resource_type": "dataset",
        "version": "1.0",
        "organization": "MDF Open",
        "acl": ["public"],
        "ingest_date": "2020-01-01T00:00:00Z",
        "domains": ["chemistry"],
        "latest": True,
        "root_version": "old-ds",
        "download_url": "https://data.example.com/old-ds/",
        "dataset_doi": "10.11111/old-ds",
    },
    "dc": {
        "title": "Old Nested Dataset",
        "creators": [{"name": "Hersam, Mark C."}],
        "publisher": "Materials Data Facility",
        "year": 2020,
        "description": "Indexed before the flatten.",
        "subjects": ["perovskite"],
        "license": "MIT",
    },
    "data": {
        "location": "globus://old", "size_bytes": 12, "file_count": 3,
    },
}


class TestTolerantReaderForOldEntries:
    """Until the re-ingest lands, the index holds both layouts at once."""

    def test_nested_content_normalizes_to_flat_names(self):
        flat = _flat_content(NESTED_ENTRY_CONTENT)
        assert flat["source_id"] == "old-ds"
        assert flat["title"] == "Old Nested Dataset"
        assert flat["authors"] == ["Hersam, Mark C."]
        assert flat["keywords"] == ["perovskite"]
        assert flat["publication_year"] == 2020
        assert flat["organization"] == "MDF Open"
        assert flat["domains"] == ["chemistry"]
        assert flat["license"] == "MIT"
        assert flat["doi"] == "10.11111/old-ds"
        assert flat["ingest_date"] == "2020-01-01T00:00:00Z"
        assert flat["size_bytes"] == 12
        assert flat["file_count"] == 3
        for key in ("dc", "mdf", "data"):
            assert key not in flat

    def test_an_old_entry_still_renders_a_full_result(self):
        result = _result_from_content(NESTED_ENTRY_CONTENT, score=0.5)
        assert PUBLIC_RESULT_KEYS <= set(result)
        assert result["title"] == "Old Nested Dataset"
        assert result["authors"] == ["Hersam, Mark C."]
        assert result["doi"] == "10.11111/old-ds"
        assert result["download_url"] == "https://data.example.com/old-ds/"
        assert result["status"] == "published"

    def test_old_and_new_entries_render_the_same_keys(self):
        old = _result_from_content(NESTED_ENTRY_CONTENT)
        new = _result_from_content(_entry()["content"])
        assert set(old) == set(new)

    def test_flat_content_is_idempotent(self):
        once = _flat_content(NESTED_ENTRY_CONTENT)
        assert _flat_content(once) == once

    def test_legacy_dotted_filter_names_still_select(self):
        client = MockGlobusSearchClient()
        client.ingest(_submission())
        got = client.faceted_search("*", filters={"dc.creators.name": ["Blaiszik, Ben"]})
        assert got["total"] == 1

    def test_acl_never_survives_the_reader(self):
        assert "acl" not in _flat_content(NESTED_ENTRY_CONTENT)
        assert "acl" not in _flat_content({"source_id": "x", "acl": ["public"]})


# --- Restricted datasets: visible_to, content, and the derived indexes -------


class TestVisibleToAndAcl:
    def test_public_dataset_is_visible_to_public(self):
        entry = _entry()
        assert entry["visible_to"] == ["public"]

    def test_explicit_public_acl_is_still_public(self):
        assert _entry(acl=["public"])["visible_to"] == ["public"]

    def test_restricted_dataset_gets_identity_urns(self):
        entry = _entry(acl=[RESTRICTED_IDENTITY])
        assert entry["visible_to"] == [
            f"urn:globus:auth:identity:{RESTRICTED_IDENTITY}"
        ]

    def test_acl_is_not_written_into_content(self):
        """Publishing the acl handed anonymous searchers the reader identities."""
        for acl in (None, ["public"], [RESTRICTED_IDENTITY]):
            content = _entry(acl=acl)["content"]
            assert "acl" not in content
            assert RESTRICTED_IDENTITY not in json.dumps(content)

    def test_unparseable_metadata_fails_closed(self):
        """The old ``meta.acl or ["public"]`` published these to the world."""
        record = _submission()
        record["dataset_mdata"] = "{not json"
        entry = MockGlobusSearchClient().build_gmeta_entry(record)
        assert entry["visible_to"] == [DENY_ALL_PRINCIPAL]
        assert "public" not in entry["visible_to"]


@pytest.fixture()
def store_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("EMBEDDING_SNAPSHOT_DIR", str(tmp_path / "snap"))
    monkeypatch.delenv("EMBEDDING_SNAPSHOT_BUCKET", raising=False)
    reset_search_client()

    import v2.search as search_mod
    from v2.embedding_snapshot import invalidate_cached_snapshot
    from v2.store import get_store

    store = get_store()

    public = _submission("ds-public")
    restricted = _submission("ds-restricted", acl=[RESTRICTED_IDENTITY])
    for record in (public, restricted):
        record["title_description_embedding"] = json.dumps([0.1, 0.2, 0.3])
        record["embedding_model"] = "test-model"
        store.put_submission(record)

    search_mod.invalidate_author_index()
    invalidate_cached_snapshot()
    yield store
    search_mod.invalidate_author_index()
    invalidate_cached_snapshot()
    reset_search_client()


class TestRestrictedDatasetsAreNotLeaked:
    def test_author_index_excludes_restricted_datasets(self, store_env):
        from v2.search import build_author_index

        index = build_author_index()
        assert "ds-public" in index["source_to_authors"]
        assert "ds-restricted" not in index["source_to_authors"]
        assert "ds-restricted" not in json.dumps(index["author_to_rows"])

    def test_related_by_author_cannot_surface_a_restricted_dataset(self, store_env):
        from v2.search import find_related_by_author

        # The restricted dataset shares both authors with the public one, so it
        # would be the top co-author hit if it were in the index at all.
        related = find_related_by_author("ds-public")
        assert related["authors"], "public anchor must be indexed, or this proves nothing"
        assert [r["source_id"] for r in related["results"]] == []

        # And it is not even an anchor: asking about it reveals nothing.
        assert find_related_by_author("ds-restricted")["results"] == []

    def test_embedding_snapshot_excludes_restricted_datasets(self, store_env):
        from v2.embedding_snapshot import build_snapshot, load_snapshot

        summary = build_snapshot()
        assert summary["count"] == 1
        assert summary["skipped_not_public"] == 1

        snapshot = load_snapshot()
        assert [row["source_id"] for row in snapshot.rows] == ["ds-public"]

    def test_similar_datasets_cannot_surface_a_restricted_dataset(self, store_env):
        from v2.embedding_snapshot import build_snapshot
        from v2.search import find_similar_by_embedding

        build_snapshot()
        similar = find_similar_by_embedding("ds-public")
        assert [r["source_id"] for r in similar["results"]] == []
        assert "ds-restricted" not in json.dumps(similar)

    def test_local_search_fallback_excludes_restricted_datasets(self, store_env):
        from v2.search import search_datasets

        found = search_datasets("Flat Content Dataset", limit=20)
        assert [r["source_id"] for r in found["results"]] == ["ds-public"]
