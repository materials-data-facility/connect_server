"""Tests for the search API: filter parsing, result ordering, and browse mode.

The filter-parsing and mock-index expectations here are pinned to measured
behaviour of the production Globus Search index (935 datasets):

  * ``dc.creators.name`` is keyword-mapped, not tokenized. Its facet buckets are
    whole names ("Blaiszik, Ben", count 20), and a ``match_any`` filter on that
    whole value returns those 20 datasets while the bare token "Blaiszik"
    returns 0.
  * 476 of 500 author facet values contain a comma (authors are indexed
    "Family, Given"); dc.year, mdf.organization, dc.subjects and mdf.domains
    have zero comma-bearing values.
  * ``mdf.ingest_date`` is sortable and orders newest-first as expected.

That is why multi-select is expressed with repeated query params and why author
values are never split on commas.
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
from v2.app.routers.search import (
    DEFAULT_BROWSE_SORT,
    SORT_MOST_VIEWED,
    SORT_NEWEST,
    SORT_RELEVANCE,
    _parse_filters,
    _resolve_sort,
)
from v2.search import SORT_STRATEGIES, resolve_sort
from v2.storage import reset_storage_backend


# --- Filter query-param parsing ---------------------------------------------


class TestParseFilters:
    def test_author_with_comma_is_kept_whole(self):
        """The bug: "Blaiszik, Ben" must not become ["Blaiszik", "Ben"]."""
        filters = _parse_filters(None, None, ["Blaiszik, Ben"], None, None)
        assert filters == {"dc.creators.name": ["Blaiszik, Ben"]}

    def test_author_with_initials_is_kept_whole(self):
        filters = _parse_filters(None, None, ["Hersam, Mark C."], None, None)
        assert filters == {"dc.creators.name": ["Hersam, Mark C."]}

    def test_repeated_author_params_are_multi_select(self):
        filters = _parse_filters(
            None, None, ["Blaiszik, Ben", "Ward, Logan"], None, None,
        )
        assert filters == {"dc.creators.name": ["Blaiszik, Ben", "Ward, Logan"]}

    def test_comma_joined_keyword_still_splits(self):
        """Backward compatibility: no dc.subjects value contains a comma."""
        filters = _parse_filters(None, None, None, ["perovskite,DFT"], None)
        assert filters == {"dc.subjects": ["perovskite", "DFT"]}

    def test_repeated_keyword_params_also_work(self):
        filters = _parse_filters(None, None, None, ["metals and alloys", "DFT"], None)
        assert filters == {"dc.subjects": ["metals and alloys", "DFT"]}

    def test_comma_joined_year_and_organization_split(self):
        filters = _parse_filters(["2024,2025"], ["MDF Open"], None, None, None)
        assert filters == {
            "dc.year": ["2024", "2025"],
            "mdf.organization": ["MDF Open"],
        }

    def test_values_are_trimmed_and_deduped(self):
        filters = _parse_filters(None, None, None, ["a , b, a"], None)
        assert filters == {"dc.subjects": ["a", "b"]}

    def test_blank_and_missing_params_yield_no_filters(self):
        assert _parse_filters(None, None, None, None, None) is None
        assert _parse_filters([""], None, ["  "], None, None) is None

    def test_all_fields_map_to_index_field_names(self):
        filters = _parse_filters(
            ["2024"], ["MDF Open"], ["Ward, Logan"], ["DFT"], ["batteries"],
        )
        assert filters == {
            "dc.year": ["2024"],
            "mdf.organization": ["MDF Open"],
            "dc.creators.name": ["Ward, Logan"],
            "dc.subjects": ["DFT"],
            "mdf.domains": ["batteries"],
        }


# --- Sort resolution ---------------------------------------------------------


class TestResolveSort:
    def test_query_defaults_to_relevance(self):
        assert _resolve_sort(None, has_query=True) == SORT_RELEVANCE

    def test_empty_query_defaults_to_browse_sort(self):
        assert _resolve_sort(None, has_query=False) == DEFAULT_BROWSE_SORT
        assert DEFAULT_BROWSE_SORT == SORT_NEWEST

    def test_relevance_without_a_query_becomes_browse_sort(self):
        """Relevance cannot rank when there are no query terms to rank against."""
        assert _resolve_sort(SORT_RELEVANCE, has_query=False) == SORT_NEWEST

    def test_explicit_sorts_are_honoured(self):
        assert _resolve_sort(SORT_NEWEST, has_query=True) == SORT_NEWEST
        assert _resolve_sort(SORT_MOST_VIEWED, has_query=True) == SORT_MOST_VIEWED
        assert _resolve_sort("NEWEST", has_query=True) == SORT_NEWEST

    def test_unknown_sort_falls_back_instead_of_erroring(self):
        assert _resolve_sort("bogus", has_query=True) == SORT_RELEVANCE
        assert _resolve_sort("bogus", has_query=False) == SORT_NEWEST

    def test_relevance_leaves_engine_ordering_alone(self):
        assert resolve_sort(SORT_RELEVANCE) is None

    def test_newest_sorts_on_ingest_date(self):
        assert resolve_sort(SORT_NEWEST) == [
            {"field_name": "mdf.ingest_date", "order": "desc"}
        ]

    def test_most_viewed_is_a_declared_strategy(self):
        """Seam check: most_viewed is selectable and falls back to recency
        until view counts are mirrored into the search index."""
        assert SORT_MOST_VIEWED in SORT_STRATEGIES
        assert resolve_sort(SORT_MOST_VIEWED) == resolve_sort(SORT_NEWEST)


# --- Mock index fidelity -----------------------------------------------------


def _submission(source_id: str, *, authors, created_at, title=None, keywords=None):
    return {
        "source_id": source_id,
        "version": "1.0",
        "organization": "MDF Open",
        "status": "published",
        "created_at": created_at,
        "published_at": created_at,
        "dataset_mdata": json.dumps({
            "title": title or f"Dataset {source_id}",
            "authors": [{"name": name} for name in authors],
            "keywords": keywords or ["DFT"],
            "data_sources": ["https://example.com/data.csv"],
        }),
    }


class TestMockIndexMatchesRealIndex:
    """The mock must not be more forgiving than the real index."""

    def _seeded_client(self):
        from v2.search_client import MockGlobusSearchClient

        client = MockGlobusSearchClient()
        client.ingest(_submission(
            "ds-a", authors=["Blaiszik, Ben", "Ward, Logan"],
            created_at="2026-01-03T00:00:00Z",
        ))
        client.ingest(_submission(
            "ds-b", authors=["Hersam, Mark C."],
            created_at="2026-01-02T00:00:00Z",
        ))
        client.ingest(_submission(
            "ds-c", authors=["Blaiszik, Ben"],
            created_at="2026-01-01T00:00:00Z",
        ))
        return client

    def test_author_facet_lists_complete_names(self):
        client = self._seeded_client()
        result = client.faceted_search("*")
        authors = {b["value"]: b["count"] for b in result["facets"]["Authors"]}
        assert authors == {
            "Blaiszik, Ben": 2,
            "Ward, Logan": 1,
            "Hersam, Mark C.": 1,
        }
        # No bucket is a bare word token split out of a full name.
        assert "Blaiszik" not in authors
        assert "Ben" not in authors

    def test_filtering_on_a_whole_author_name_matches(self):
        client = self._seeded_client()
        result = client.faceted_search(
            "*", filters={"dc.creators.name": ["Blaiszik, Ben"]},
        )
        assert result["total"] == 2
        assert {r["source_id"] for r in result["results"]} == {"ds-a", "ds-c"}

    def test_filtering_on_a_name_fragment_matches_nothing(self):
        """Mirrors the real index, where match_any "Blaiszik" returns 0."""
        client = self._seeded_client()
        for fragment in ("Blaiszik", "Ben"):
            result = client.faceted_search(
                "*", filters={"dc.creators.name": [fragment]},
            )
            assert result["total"] == 0, fragment

    def test_multiple_authors_filter_as_or(self):
        client = self._seeded_client()
        result = client.faceted_search(
            "*", filters={"dc.creators.name": ["Ward, Logan", "Hersam, Mark C."]},
        )
        assert {r["source_id"] for r in result["results"]} == {"ds-a", "ds-b"}

    def test_sort_orders_by_ingest_date_desc(self):
        client = self._seeded_client()
        result = client.faceted_search("*", sort=resolve_sort(SORT_NEWEST))
        assert [r["source_id"] for r in result["results"]] == ["ds-a", "ds-b", "ds-c"]

    def test_no_sort_leaves_order_untouched(self):
        client = self._seeded_client()
        result = client.faceted_search("*")
        assert len(result["results"]) == 3

    def test_faceted_and_plain_search_agree_on_result_shape(self):
        client = self._seeded_client()
        faceted = client.faceted_search("*")["results"][0]
        plain = client.search("Dataset ds-a")["results"][0]
        assert set(faceted) == set(plain)


# --- Endpoint behaviour ------------------------------------------------------


@pytest.fixture()
def search_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from v2.search_client import get_search_client, reset_search_client

    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("FILE_STORE_PATH", str(tmp_path / "files"))
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("RATE_LIMIT_DEFAULT_PER_MIN", "500")
    monkeypatch.setenv("RATE_LIMIT_WINDOW_SECONDS", "60")
    reset_storage_backend()
    reset_middleware_state()
    reset_search_client()

    client = get_search_client()
    client.ingest(_submission(
        "ds-new", authors=["Blaiszik, Ben"], created_at="2026-06-01T00:00:00Z",
        title="Newest Dataset",
    ))
    client.ingest(_submission(
        "ds-mid", authors=["Ward, Logan"], created_at="2026-03-01T00:00:00Z",
        title="Middle Dataset",
    ))
    client.ingest(_submission(
        "ds-old", authors=["Blaiszik, Ben", "Ward, Logan"],
        created_at="2026-01-01T00:00:00Z", title="Oldest Dataset",
    ))

    yield TestClient(app)

    reset_search_client()
    reset_storage_backend()
    reset_middleware_state()


class TestSearchEndpoint:
    def test_browse_without_a_query_returns_newest_first(self, search_env: TestClient):
        response = search_env.get("/search")
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 3
        assert [r["source_id"] for r in body["results"]] == [
            "ds-new", "ds-mid", "ds-old",
        ]

    def test_browse_response_shape_matches_a_keyword_search(self, search_env: TestClient):
        browse = search_env.get("/search").json()
        searched = search_env.get("/search", params={"q": "Newest"}).json()
        assert set(browse) == set(searched)
        assert set(browse["results"][0]) == set(searched["results"][0])
        assert browse["query"] == ""
        assert browse["offset"] == 0

    def test_browse_still_returns_facets(self, search_env: TestClient):
        body = search_env.get("/search").json()
        authors = {b["value"] for b in body["facets"]["Authors"]}
        assert authors == {"Blaiszik, Ben", "Ward, Logan"}

    def test_browse_ignores_relevance_and_uses_recency(self, search_env: TestClient):
        body = search_env.get("/search", params={"sort": "relevance"}).json()
        assert [r["source_id"] for r in body["results"]] == [
            "ds-new", "ds-mid", "ds-old",
        ]

    def test_browse_respects_limit_and_offset(self, search_env: TestClient):
        body = search_env.get("/search", params={"limit": 1, "offset": 1}).json()
        assert body["total"] == 3
        assert [r["source_id"] for r in body["results"]] == ["ds-mid"]
        assert body["offset"] == 1

    def test_author_filter_with_a_comma_in_the_name(self, search_env: TestClient):
        """End-to-end regression: one param whose value contains a comma."""
        response = search_env.get("/search", params={"author": "Blaiszik, Ben"})
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 2
        assert {r["source_id"] for r in body["results"]} == {"ds-new", "ds-old"}

    def test_repeated_author_params_select_multiple_authors(self, search_env: TestClient):
        response = search_env.get(
            "/search?author=Blaiszik%2C+Ben&author=Ward%2C+Logan",
        )
        body = response.json()
        assert {r["source_id"] for r in body["results"]} == {
            "ds-new", "ds-mid", "ds-old",
        }

    def test_author_filter_narrows_a_keyword_search(self, search_env: TestClient):
        body = search_env.get(
            "/search", params={"q": "Dataset", "author": "Ward, Logan"},
        ).json()
        assert {r["source_id"] for r in body["results"]} == {"ds-mid", "ds-old"}

    def test_facet_values_round_trip_as_filters(self, search_env: TestClient):
        """Every author bucket the UI can show must select something."""
        facets = search_env.get("/search").json()["facets"]["Authors"]
        assert facets
        for bucket in facets:
            body = search_env.get(
                "/search", params={"author": bucket["value"]},
            ).json()
            assert body["total"] == bucket["count"], bucket["value"]

    def test_most_viewed_sort_is_accepted(self, search_env: TestClient):
        response = search_env.get("/search", params={"sort": "most_viewed"})
        assert response.status_code == 200
        assert len(response.json()["results"]) == 3

    def test_unknown_sort_does_not_error(self, search_env: TestClient):
        response = search_env.get("/search", params={"q": "Dataset", "sort": "bogus"})
        assert response.status_code == 200
