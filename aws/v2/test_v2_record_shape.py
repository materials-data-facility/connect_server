"""v2.1 record shape: version pointers, identity, ACL, paging, backfill.

Covers the schema-review findings N3 (version-chain pointers are bare version
strings), N4 (system identity out of the user-editable ``extensions`` blob),
N5 (``acl`` is a top-level record attribute), the ``GET /submissions``
``next_key`` paging bug, and the ``scripts/backfill_record_shape.py`` dry run.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from v2.app import app
from v2.app.middleware import reset_middleware_state
from v2.dataset_card import build_dataset_card
from v2.search import dataset_is_public
from v2.search_client import DENY_ALL_PRINCIPAL, resolve_visible_to
from v2.storage import reset_storage_backend
from v2.store import (
    CURATION_QUEUE_STATUSES,
    SqliteSubmissionStore,
    parse_pagination_key,
    serialize_pagination_key,
)
from v2.submission_utils import (
    DEFAULT_ACL,
    normalize_acl_principal,
    normalize_record_shape,
    normalize_version_pointer,
    record_previous_version,
    record_root_version,
    record_source_name,
    resolve_record_acl,
    validate_source_id,
    validate_source_id_lenient,
)


OWNER_HEADERS = {"X-User-Id": "owner-user"}
OTHER_HEADERS = {"X-User-Id": "other-user"}
CURATOR_HEADERS = {"X-User-Id": "curator-user"}

IDENTITY_A = "8f5e1c9a-1111-2222-3333-444455556666"
IDENTITY_B = "0a1b2c3d-9999-8888-7777-666655554444"

BASE_SUBMISSION = {
    "title": "Record shape dataset",
    "authors": [{"name": "Blaiszik, Ben"}],
    "data_sources": ["https://example.com/data.csv"],
}


def _load_backfill():
    """Import the backfill script by path (scripts/ is not a package)."""
    path = Path(__file__).resolve().parents[0] / "scripts" / "backfill_record_shape.py"
    spec = importlib.util.spec_from_file_location("backfill_record_shape", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backfill_script = _load_backfill()


@pytest.fixture()
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("STORE_BACKEND", "sqlite")
    monkeypatch.setenv("SQLITE_PATH", str(tmp_path / "store.db"))
    monkeypatch.setenv("ASYNC_DISPATCH_MODE", "inline")
    monkeypatch.setenv("AUTH_MODE", "dev")
    monkeypatch.setenv("LOCAL_DEV_AUTH", "true")
    monkeypatch.setenv("ALLOW_ALL_CURATORS", "false")
    monkeypatch.setenv("CURATOR_USER_IDS", "curator-user")
    monkeypatch.setenv("USE_MOCK_DATACITE", "true")
    monkeypatch.setenv("USE_MOCK_SEARCH", "true")
    reset_storage_backend()
    reset_middleware_state()
    yield
    reset_storage_backend()
    reset_middleware_state()


def _submit(client: TestClient, headers=OWNER_HEADERS, **extra) -> Dict[str, Any]:
    payload = {**BASE_SUBMISSION, **extra}
    response = client.post("/submit", headers=headers, json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def _record(client: TestClient, source_id: str, version: Optional[str] = None) -> Dict[str, Any]:
    url = f"/status/{source_id}"
    if version:
        url += f"?version={version}"
    response = client.get(url, headers=CURATOR_HEADERS)
    assert response.status_code == 200, response.text
    return response.json()["submission"]


def _publish(client: TestClient, source_id: str) -> None:
    response = client.post(
        f"/curation/{source_id}/approve",
        headers=CURATOR_HEADERS,
        json={"mint_doi": False},
    )
    assert response.status_code == 200, response.text


# --- N3: version pointers are bare version strings ---------------------------


class TestVersionPointers:
    @pytest.mark.parametrize(
        ("value", "source_id", "expected"),
        [
            ("1.0", "foo", "1.0"),
            ("2.13", "foo", "2.13"),
            ("foo-1.0", "foo", "1.0"),
            # An id-only pointer (what the v1 migrator wrote for single-entry
            # groups) carries no version at all.
            ("foo", "foo", None),
            ("narayananbadri_g4mp2gdb9_database", "narayananbadri_g4mp2gdb9_database", None),
            ("narayananbadri_g4mp2gdb9_database-1.0", "narayananbadri_g4mp2gdb9_database", "1.0"),
            # A composite naming a DIFFERENT dataset is not this chain's link.
            ("bar-1.0", "foo", None),
            # Legacy ids that merely end in something version-like.
            ("levine_abo2179_database_v2.1", "levine_abo2179_database_v2.1", None),
            (None, "foo", None),
            ("", "foo", None),
        ],
    )
    def test_normalize_version_pointer(self, value, source_id, expected):
        assert normalize_version_pointer(value, source_id) == expected

    def test_submit_writes_bare_pointers_at_top_level(self, env):
        client = TestClient(app)
        source_id = _submit(client)["source_id"]
        _submit(
            client,
            update=True,
            source_id=source_id,
            data_sources=["https://example.com/v2.csv"],
        )

        v2 = _record(client, source_id, version="2.0")
        assert v2["root_version"] == "1.0"
        assert v2["previous_version"] == "1.0"
        # And nowhere near the user-editable blob.
        assert "root_version" not in v2["dataset_mdata"]
        assert "previous_version" not in v2["dataset_mdata"]

    def test_edit_of_published_version_keeps_pointers_bare(self, env):
        client = TestClient(app)
        source_id = _submit(client)["source_id"]
        _publish(client, source_id)

        response = client.post(
            f"/submissions/{source_id}/metadata",
            headers=OWNER_HEADERS,
            json={"title": "Edited title"},
        )
        assert response.status_code == 200, response.text

        v11 = _record(client, source_id, version="1.1")
        assert v11["root_version"] == "1.0"
        assert v11["previous_version"] == "1.0"
        assert "root_version" not in v11["dataset_mdata"]

    def test_versions_route_and_card_expose_bare_pointers(self, env):
        client = TestClient(app)
        source_id = _submit(client)["source_id"]
        _submit(
            client,
            update=True,
            source_id=source_id,
            data_sources=["https://example.com/v2.csv"],
        )

        versions = client.get(f"/versions/{source_id}", headers=OWNER_HEADERS).json()
        by_version = {v["version"]: v for v in versions["versions"]}
        assert by_version["2.0"]["previous_version"] == "1.0"
        assert by_version["2.0"]["root_version"] == "1.0"
        assert by_version["1.0"]["previous_version"] is None

        card = build_dataset_card(_record(client, source_id, version="2.0"))
        assert card["previous_version"] == "1.0"
        assert card["root_version"] == "1.0"

    def test_legacy_composite_in_the_blob_still_reads(self):
        """Un-backfilled rows keep working through the tolerant reader."""
        record = {
            "source_id": "legacy_ds",
            "version": "2.0",
            "dataset_mdata": json.dumps(
                {"root_version": "legacy_ds-1.0", "previous_version": "legacy_ds-1.0"}
            ),
        }
        assert record_root_version(record) == "1.0"
        assert record_previous_version(record) == "1.0"


# --- N4: identity out of the user-editable blob ------------------------------


class TestIdentityOutOfExtensions:
    def test_top_level_source_id_is_accepted(self, env):
        client = TestClient(app)
        result = _submit(client, source_id="my-dataset-id")
        assert result["source_id"] == "my-dataset-id"

    def test_top_level_source_id_must_satisfy_the_strict_grammar(self, env):
        client = TestClient(app)
        response = client.post(
            "/submit",
            headers=OWNER_HEADERS,
            json={**BASE_SUBMISSION, "source_id": "../escape"},
        )
        assert response.status_code == 400
        assert "^[a-z0-9][a-z0-9._-]{2,63}$" in response.json()["detail"]

    def test_deprecated_alias_is_copied_to_top_level_and_stripped(self, env):
        client = TestClient(app)
        result = _submit(
            client,
            extensions={
                "mdf_source_id": "aliased-dataset",
                "mdf_source_name": "aliased_family",
                "instrument": "APS 12-ID",
            },
        )
        assert result["source_id"] == "aliased-dataset"

        record = _record(client, "aliased-dataset")
        # Copied to the top level ...
        assert record["source_name"] == "aliased_family"
        # ... and gone from what got stored, while user keys survive.
        extensions = record["dataset_mdata"]["extensions"]
        assert extensions == {"instrument": "APS 12-ID"}

    def test_other_reserved_extension_keys_are_rejected_on_submit(self, env):
        client = TestClient(app)
        response = client.post(
            "/submit",
            headers=OWNER_HEADERS,
            json={**BASE_SUBMISSION, "extensions": {"mdf_organization": "Elsewhere"}},
        )
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert "mdf_organization" in detail
        assert "reserved" in detail

    def test_edit_may_not_write_any_reserved_extension_key(self, env):
        """The alias is grandfathered on submit only — an edit must not repoint identity."""
        client = TestClient(app)
        source_id = _submit(client)["source_id"]

        for key in ("mdf_source_name", "mdf_source_id", "mdf_anything"):
            response = client.post(
                f"/submissions/{source_id}/metadata",
                headers=OWNER_HEADERS,
                json={"extensions": {key: "hijacked"}},
            )
            assert response.status_code == 400, key
            assert key in response.json()["detail"]

    def test_source_name_defaults_to_source_id(self, env):
        client = TestClient(app)
        source_id = _submit(client)["source_id"]
        assert _record(client, source_id)["source_name"] == source_id

    def test_source_name_reader_prefers_top_level(self):
        record = {
            "source_id": "ds",
            "source_name": "canonical",
            "dataset_mdata": json.dumps({"extensions": {"mdf_source_name": "stale"}}),
        }
        assert record_source_name(record) == "canonical"
        assert record_source_name({"source_id": "ds", "dataset_mdata": "{}"}) == "ds"


# --- N4/amendment: source_id grammar, strict vs lenient ----------------------


class TestSourceIdGrammar:
    #: Real ids from the staging corpus that predate the strict grammar.
    LEGACY_IDS = [
        "Dataset_Li_conductivity",
        "kononov_identifying_native_αalumina",
        "cs",
    ]

    @pytest.mark.parametrize("source_id", LEGACY_IDS)
    def test_legacy_ids_are_addressable_but_not_mintable(self, source_id):
        assert validate_source_id_lenient(source_id) == source_id
        with pytest.raises(ValueError):
            validate_source_id(source_id)

    def test_a_139_character_id_is_addressable(self):
        long_id = "a" * 139
        assert validate_source_id_lenient(long_id) == long_id

    @pytest.mark.parametrize(
        "source_id",
        ["../x", "a/b", "a\\b", "A B", "-abc", ".abc", "", "a\tb", "a" * 161],
    )
    def test_lenient_still_rejects_unsafe_values(self, source_id):
        with pytest.raises(ValueError):
            validate_source_id_lenient(source_id)

    def test_a_legacy_id_reaches_the_store_instead_of_being_404d_by_grammar(self, env):
        """A pre-grammar id must be routable: update, then read it back."""
        client = TestClient(app)
        store = SqliteSubmissionStore()
        store.put_submission(
            {
                "source_id": "Dataset_Li_conductivity",
                "version": "1.0",
                "user_id": "owner-user",
                "status": "published",
                "dataset_mdata": json.dumps(
                    {"title": "Legacy", "authors": [{"name": "A"}], "version": "1.0"}
                ),
                "created_at": "2020-01-01T00:00:00Z",
                "updated_at": "2020-01-01T00:00:00Z",
            }
        )
        response = client.get(
            "/status/Dataset_Li_conductivity", headers=OWNER_HEADERS
        )
        assert response.status_code == 200
        assert response.json()["submission"]["source_id"] == "Dataset_Li_conductivity"

    def test_update_submit_accepts_a_legacy_source_id(self, env):
        client = TestClient(app)
        store = SqliteSubmissionStore()
        store.put_submission(
            {
                "source_id": "Dataset_Li_conductivity",
                "version": "1.0",
                "user_id": "owner-user",
                "status": "published",
                "dataset_mdata": json.dumps(
                    {"title": "Legacy", "authors": [{"name": "A"}], "version": "1.0"}
                ),
                "created_at": "2020-01-01T00:00:00Z",
                "updated_at": "2020-01-01T00:00:00Z",
            }
        )
        result = _submit(
            client,
            update=True,
            source_id="Dataset_Li_conductivity",
            data_sources=["https://example.com/v2.csv"],
        )
        assert result["source_id"] == "Dataset_Li_conductivity"
        assert result["version"] == "2.0"


# --- N5: acl is a top-level record attribute ---------------------------------


class TestAclTopLevel:
    def test_submit_stores_acl_top_level_and_strips_the_blob_copy(self, env):
        client = TestClient(app)
        source_id = _submit(client, acl=[IDENTITY_A])["source_id"]

        record = _record(client, source_id)
        assert record["acl"] == [f"urn:globus:auth:identity:{IDENTITY_A}"]
        assert "acl" not in record["dataset_mdata"]

    def test_public_is_the_default(self, env):
        client = TestClient(app)
        source_id = _submit(client)["source_id"]
        record = _record(client, source_id)
        assert record["acl"] == DEFAULT_ACL
        assert dataset_is_public(record)

    def test_restricted_published_dataset_is_still_invisible(self, env):
        client = TestClient(app)
        source_id = _submit(client, acl=[IDENTITY_A])["source_id"]
        _publish(client, source_id)

        # Hidden == nonexistent, unchanged from wave 1.
        body = client.get(f"/status/{source_id}", headers=OTHER_HEADERS).json()
        assert body["success"] is False
        # /versions answers hidden datasets exactly like missing ones: 404.
        resp = client.get(f"/versions/{source_id}", headers=OTHER_HEADERS)
        assert resp.status_code == 404
        assert "detail" in resp.json()

    def test_outsider_never_receives_the_top_level_acl(self, env):
        client = TestClient(app)
        source_id = _submit(client)["source_id"]
        _publish(client, source_id)

        sanitized = client.get(f"/status/{source_id}", headers=OTHER_HEADERS).json()
        assert sanitized["success"] is True
        assert "acl" not in sanitized["submission"]

    def test_owner_can_edit_the_acl_through_the_metadata_route(self, env):
        client = TestClient(app)
        source_id = _submit(client)["source_id"]

        response = client.post(
            f"/submissions/{source_id}/metadata",
            headers=OWNER_HEADERS,
            json={"acl": [IDENTITY_A, IDENTITY_B]},
        )
        assert response.status_code == 200, response.text
        assert response.json()["updated_fields"] == ["acl"]

        record = _record(client, source_id)
        assert record["acl"] == [
            f"urn:globus:auth:identity:{IDENTITY_A}",
            f"urn:globus:auth:identity:{IDENTITY_B}",
        ]
        assert not dataset_is_public(record)

        # And back to public.
        client.post(
            f"/submissions/{source_id}/metadata",
            headers=OWNER_HEADERS,
            json={"acl": ["public"]},
        )
        assert dataset_is_public(_record(client, source_id))

    def test_an_empty_acl_edit_is_rejected_rather_than_failing_open(self, env):
        client = TestClient(app)
        source_id = _submit(client, acl=[IDENTITY_A])["source_id"]
        response = client.post(
            f"/submissions/{source_id}/metadata",
            headers=OWNER_HEADERS,
            json={"acl": []},
        )
        assert response.status_code == 400

    def test_editing_a_restricted_published_dataset_keeps_it_restricted(self, env):
        client = TestClient(app)
        source_id = _submit(client, acl=[IDENTITY_A])["source_id"]
        _publish(client, source_id)

        response = client.post(
            f"/submissions/{source_id}/metadata",
            headers=OWNER_HEADERS,
            json={"title": "Edited"},
        )
        assert response.status_code == 200, response.text
        new_version = _record(client, source_id, version="1.1")
        assert new_version["acl"] == [f"urn:globus:auth:identity:{IDENTITY_A}"]
        assert not dataset_is_public(new_version)

    def test_update_submit_inherits_visibility_when_acl_is_omitted(self, env):
        client = TestClient(app)
        source_id = _submit(client, acl=[IDENTITY_A])["source_id"]
        _submit(
            client,
            update=True,
            source_id=source_id,
            data_sources=["https://example.com/v2.csv"],
        )
        v2 = _record(client, source_id, version="2.0")
        assert v2["acl"] == [f"urn:globus:auth:identity:{IDENTITY_A}"]

    @pytest.mark.parametrize(
        ("record", "expected"),
        [
            ({"acl": ["public"], "dataset_mdata": "{}"}, ["public"]),
            # Top level wins over a stale blob copy.
            ({"acl": ["x"], "dataset_mdata": json.dumps({"acl": ["public"]})}, ["x"]),
            # Blob fallback for un-backfilled rows.
            ({"dataset_mdata": json.dumps({"acl": ["y"]})}, ["y"]),
            # Absent everywhere -> the legacy default.
            ({"dataset_mdata": json.dumps({"title": "t"})}, ["public"]),
            # Empty reads as public, matching the pre-v2.1 default.
            ({"dataset_mdata": json.dumps({"acl": []})}, ["public"]),
            # Unreadable -> None, so callers fail closed.
            ({"dataset_mdata": "{not json"}, None),
        ],
    )
    def test_resolve_record_acl(self, record, expected):
        assert resolve_record_acl(record) == expected

    def test_unreadable_metadata_is_not_public(self):
        assert dataset_is_public({"source_id": "x", "dataset_mdata": "{not json"}) is False


# --- Amendment: visible_to principal spelling --------------------------------


class TestVisibleToPrincipals:
    @pytest.mark.parametrize(
        ("entry", "expected"),
        [
            (IDENTITY_A, f"urn:globus:auth:identity:{IDENTITY_A}"),
            (f"urn:globus:auth:identity:{IDENTITY_A}", f"urn:globus:auth:identity:{IDENTITY_A}"),
            (f"urn:globus:groups:id:{IDENTITY_B}", f"urn:globus:groups:id:{IDENTITY_B}"),
            ("public", "public"),
            ("not-a-principal", None),
        ],
    )
    def test_normalize_acl_principal(self, entry, expected):
        assert normalize_acl_principal(entry) == expected

    @pytest.mark.parametrize(
        "entry",
        [
            IDENTITY_A,
            f"urn:globus:auth:identity:{IDENTITY_A}",
        ],
    )
    def test_bare_and_qualified_identities_index_identically(self, entry):
        record = {"source_id": "ds", "acl": [entry], "dataset_mdata": "{}"}
        assert resolve_visible_to(record) == [
            f"urn:globus:auth:identity:{IDENTITY_A}"
        ]

    def test_group_urn_is_passed_through_verbatim(self):
        group = f"urn:globus:groups:id:{IDENTITY_B}"
        record = {"source_id": "ds", "acl": [group], "dataset_mdata": "{}"}
        assert resolve_visible_to(record) == [group]

    def test_double_prefixing_no_longer_happens(self):
        record = {
            "source_id": "ds",
            "acl": [f"urn:globus:auth:identity:{IDENTITY_A}"],
            "dataset_mdata": "{}",
        }
        assert "urn:globus:auth:identity:urn:globus:" not in resolve_visible_to(record)[0]

    def test_an_unusable_entry_denies_rather_than_going_public(self):
        record = {"source_id": "ds", "acl": ["garbage"], "dataset_mdata": "{}"}
        assert resolve_visible_to(record) == [DENY_ALL_PRINCIPAL]

    def test_public_record_is_visible_to_public(self):
        record = {"source_id": "ds", "acl": ["public"], "dataset_mdata": "{}"}
        assert resolve_visible_to(record) == ["public"]


# --- GET /submissions paging -------------------------------------------------


class TestSubmissionsPaging:
    def test_cursor_round_trips_and_is_url_safe(self):
        for key in ({"offset": 4}, {"source_id": "a-b", "version": "1.0"}):
            token = serialize_pagination_key(key)
            assert token is not None
            # Opaque and safe to drop straight into a query string.
            assert all(char.isalnum() or char in "-_" for char in token), token
            assert parse_pagination_key(token) == key

    def test_a_raw_json_cursor_is_still_accepted(self):
        assert parse_pagination_key('{"offset": 2}') == {"offset": 2}

    def test_a_garbage_cursor_restarts_instead_of_erroring(self):
        assert parse_pagination_key("!!!not-a-cursor!!!") is None
        assert serialize_pagination_key(None) is None

    def _seed(self, client: TestClient, count: int) -> List[str]:
        return [
            _submit(client, title=f"Dataset {index}")["source_id"]
            for index in range(count)
        ]

    def _walk(self, client: TestClient, query: str, pages: int) -> List[List[str]]:
        walked: List[List[str]] = []
        start_key = None
        for _ in range(pages):
            url = f"/submissions?{query}"
            if start_key:
                url += f"&start_key={start_key}"
            body = client.get(url, headers=OWNER_HEADERS).json()
            assert body["success"] is True
            walked.append([item["source_id"] for item in body["submissions"]])
            start_key = body["next_key"]
            if not start_key:
                break
        return walked

    def test_walks_three_pages_of_two(self, env):
        client = TestClient(app)
        seeded = set(self._seed(client, 6))

        pages = self._walk(client, "limit=2", pages=4)
        assert [len(page) for page in pages[:3]] == [2, 2, 2]
        seen = [source_id for page in pages for source_id in page]
        # Every row exactly once, no repeats across pages.
        assert len(seen) == len(set(seen))
        assert set(seen) == seeded

    def test_include_counts_pages_too(self, env):
        """The regression: include_counts=true always answered next_key: null."""
        client = TestClient(app)
        seeded = set(self._seed(client, 6))

        first = client.get(
            "/submissions?limit=2&include_counts=true", headers=OWNER_HEADERS
        ).json()
        assert first["total"] == 6
        assert first["counts"] == {"pending_curation": 6}
        assert len(first["submissions"]) == 2
        assert first["next_key"], "next_key must not be null when limit truncates"

        pages = self._walk(client, "limit=2&include_counts=true", pages=4)
        seen = [source_id for page in pages for source_id in page]
        assert len(seen) == len(set(seen))
        assert set(seen) == seeded

    def test_last_page_reports_no_next_key(self, env):
        client = TestClient(app)
        self._seed(client, 4)
        pages = self._walk(client, "limit=2", pages=5)
        assert len(pages) == 2

    def test_counts_are_stable_across_pages(self, env):
        client = TestClient(app)
        self._seed(client, 5)
        start_key = None
        for _ in range(3):
            url = "/submissions?limit=2&include_counts=true"
            if start_key:
                url += f"&start_key={start_key}"
            body = client.get(url, headers=OWNER_HEADERS).json()
            assert body["total"] == 5
            start_key = body["next_key"]
            if not start_key:
                break

    def test_sqlite_store_paging_directly(self, env):
        store = SqliteSubmissionStore()
        for index in range(5):
            store.put_submission(
                {
                    "source_id": f"ds-{index}",
                    "version": "1.0",
                    "user_id": "owner-user",
                    "status": "published",
                    "dataset_mdata": json.dumps({"title": f"D{index}"}),
                    "created_at": f"2026-01-0{index + 1}T00:00:00Z",
                    "updated_at": f"2026-01-0{index + 1}T00:00:00Z",
                }
            )

        seen: List[str] = []
        start_key = None
        for _ in range(4):
            rows, start_key = store.list_by_user(
                "owner-user", limit=2, start_key=start_key
            )
            seen.extend(row["source_id"] for row in rows)
            if not start_key:
                break
        assert sorted(seen) == [f"ds-{index}" for index in range(5)]
        assert start_key is None


# --- normalize_record_shape --------------------------------------------------


class TestNormalizeRecordShape:
    def test_promotes_and_strips(self):
        record = {
            "source_id": "ds",
            "version": "1.1",
            "dataset_mdata": json.dumps(
                {
                    "title": "T",
                    "acl": [IDENTITY_A],
                    "root_version": "ds-1.0",
                    "previous_version": "ds-1.0",
                    "extensions": {"mdf_source_name": "family", "instrument": "APS"},
                }
            ),
        }
        normalized = normalize_record_shape(record)
        assert normalized["acl"] == [f"urn:globus:auth:identity:{IDENTITY_A}"]
        assert normalized["root_version"] == "1.0"
        assert normalized["previous_version"] == "1.0"
        assert normalized["source_name"] == "family"

        blob = json.loads(normalized["dataset_mdata"])
        assert "acl" not in blob
        assert "root_version" not in blob
        assert blob["extensions"] == {"instrument": "APS"}

    def test_is_idempotent(self):
        record = {
            "source_id": "ds",
            "version": "1.0",
            "dataset_mdata": json.dumps({"title": "T", "acl": ["public"]}),
        }
        once = normalize_record_shape(record)
        assert normalize_record_shape(once) == once

    def test_unreadable_metadata_is_left_alone_and_stays_non_public(self):
        record = {"source_id": "ds", "version": "1.0", "dataset_mdata": "{not json"}
        normalized = normalize_record_shape(record)
        assert "acl" not in normalized
        assert normalized["dataset_mdata"] == "{not json"
        assert dataset_is_public(normalized) is False

    def test_legacy_source_id_equal_to_source_id_is_not_persisted(self):
        record = {
            "source_id": "ds",
            "version": "1.0",
            "legacy_source_id": "ds",
            "dataset_mdata": json.dumps({"title": "T"}),
        }
        assert "legacy_source_id" not in normalize_record_shape(record)

    def test_dict_blobs_stay_dicts_and_string_blobs_stay_strings(self):
        as_dict = normalize_record_shape(
            {"source_id": "ds", "version": "1.0", "dataset_mdata": {"title": "T"}}
        )
        assert isinstance(as_dict["dataset_mdata"], dict)
        as_str = normalize_record_shape(
            {"source_id": "ds", "version": "1.0", "dataset_mdata": '{"title": "T"}'}
        )
        assert isinstance(as_str["dataset_mdata"], str)


# --- backfill_record_shape.py ------------------------------------------------


class FakeTable:
    """Minimal DynamoDB Table stand-in over an in-memory row list."""

    name = "fake-submissions"

    def __init__(self, rows: List[Dict[str, Any]]):
        self.rows = [dict(row) for row in rows]
        self.writes: List[Dict[str, Any]] = []
        self.reject_conditional = False

    def scan(self, **kwargs):
        return {"Items": [dict(row) for row in self.rows]}

    def put_item(self, Item, ConditionExpression=None, ExpressionAttributeValues=None):
        if self.reject_conditional:
            error = Exception("conditional check failed")
            error.response = {"Error": {"Code": "ConditionalCheckFailedException"}}
            raise error
        self.writes.append(dict(Item))
        for index, row in enumerate(self.rows):
            if (row.get("source_id"), row.get("version")) == (
                Item.get("source_id"),
                Item.get("version"),
            ):
                self.rows[index] = dict(Item)
                return
        self.rows.append(dict(Item))


def _legacy_rows() -> List[Dict[str, Any]]:
    return [
        {
            "source_id": "narayananbadri_g4mp2gdb9_database",
            "version": "1.1",
            "status": "published",
            "updated_at": "2024-01-01T00:00:00Z",
            "dataset_mdata": json.dumps(
                {
                    "title": "G4MP2 GDB9",
                    "acl": ["public"],
                    # The exact inconsistency the schema review found live: a
                    # bare id in root_version and a composite in previous_version.
                    "root_version": "narayananbadri_g4mp2gdb9_database",
                    "previous_version": "narayananbadri_g4mp2gdb9_database-1.0",
                    "extensions": {"mdf_source_name": "g4mp2_family"},
                }
            ),
        },
        {
            "source_id": "restricted_ds",
            "version": "1.0",
            "status": "pending_curation",
            "updated_at": "2024-02-01T00:00:00Z",
            "dataset_mdata": json.dumps({"title": "R", "acl": [IDENTITY_A]}),
        },
        {
            "source_id": "already_shaped",
            "version": "1.0",
            "status": "published",
            "updated_at": "2024-03-01T00:00:00Z",
            "acl": ["public"],
            "source_name": "already_shaped",
            "dataset_mdata": json.dumps({"title": "A"}),
        },
        # The publish lock shares the table and is not a submission.
        {
            "source_id": "narayananbadri_g4mp2gdb9_database",
            "version": "__publish_lock__",
            "owner": "worker-1",
        },
    ]


class TestBackfillScript:
    def test_curation_queue_statuses_match_the_store(self):
        assert backfill_script.CURATION_QUEUE_STATUSES == CURATION_QUEUE_STATUSES

    def test_dry_run_writes_nothing_and_reports_counts(self):
        table = FakeTable(_legacy_rows())
        report = backfill_script.backfill(
            table, execute=False, limit=None, verbose=True
        )
        assert table.writes == []
        assert report["mode"] == "dry-run"
        # The lock row is skipped, so three submissions are scanned.
        assert report["counts"]["scanned"] == 3
        assert report["counts"]["changed"] == 2
        assert report["counts"]["unchanged"] == 1
        assert report["counts"]["errors"] == 0
        # And the report is JSON-serializable, since it goes to stdout.
        json.dumps(report, default=str)

    def test_execute_normalizes_pointers_identity_and_acl(self):
        table = FakeTable(_legacy_rows())
        report = backfill_script.backfill(
            table, execute=True, limit=None, verbose=False
        )
        assert report["counts"]["changed"] == 2
        assert report["counts"]["conflicts"] == 0

        rewritten = {
            (row["source_id"], row["version"]): row
            for row in table.rows
            if row.get("version") != "__publish_lock__"
        }
        gdb9 = rewritten[("narayananbadri_g4mp2gdb9_database", "1.1")]
        # The composite is normalized; the bare id carried no version at all
        # and is therefore dropped rather than invented.
        assert gdb9["previous_version"] == "1.0"
        assert "root_version" not in gdb9
        assert gdb9["source_name"] == "g4mp2_family"
        assert gdb9["acl"] == ["public"]
        assert "acl" not in json.loads(gdb9["dataset_mdata"])
        assert "mdf_source_name" not in json.loads(gdb9["dataset_mdata"]).get(
            "extensions", {}
        )

        restricted = rewritten[("restricted_ds", "1.0")]
        assert restricted["acl"] == [f"urn:globus:auth:identity:{IDENTITY_A}"]

    def test_execute_stamps_and_clears_curation_queue(self):
        table = FakeTable(_legacy_rows())
        backfill_script.backfill(table, execute=True, limit=None, verbose=False)
        rewritten = {
            (row["source_id"], row["version"]): row
            for row in table.rows
            if row.get("version") != "__publish_lock__"
        }
        assert rewritten[("restricted_ds", "1.0")]["curation_queue"] == "pending_curation"
        # Published rows must stay OUT of the sparse index.
        assert "curation_queue" not in rewritten[
            ("narayananbadri_g4mp2gdb9_database", "1.1")
        ]

    def test_second_run_is_a_no_op(self):
        table = FakeTable(_legacy_rows())
        backfill_script.backfill(table, execute=True, limit=None, verbose=False)
        second = backfill_script.backfill(
            table, execute=True, limit=None, verbose=False
        )
        assert second["counts"]["changed"] == 0
        assert second["counts"]["unchanged"] == 3

    def test_limit_stops_early(self):
        table = FakeTable(_legacy_rows())
        report = backfill_script.backfill(
            table, execute=False, limit=1, verbose=False
        )
        assert report["counts"]["scanned"] == 1

    def test_a_concurrent_edit_is_reported_not_clobbered(self):
        table = FakeTable(_legacy_rows())
        table.reject_conditional = True
        report = backfill_script.backfill(
            table, execute=True, limit=None, verbose=False
        )
        assert report["counts"]["conflicts"] == 2
        assert report["counts"]["changed"] == 0
        assert table.writes == []

    def test_it_refuses_to_write_without_execute(self, monkeypatch):
        """--dry-run is the default; only --execute writes."""
        table = FakeTable(_legacy_rows())
        monkeypatch.setenv("DYNAMO_SUBMISSIONS_TABLE", table.name)

        captured: Dict[str, Any] = {}

        class FakeResource:
            def Table(self, name):
                captured["table"] = name
                return table

        import boto3

        monkeypatch.setattr(boto3, "resource", lambda *a, **k: FakeResource())

        assert backfill_script.main(["--table", table.name]) == 0
        assert table.writes == []

        assert backfill_script.main(["--table", table.name, "--execute"]) == 0
        assert table.writes

    def test_env_or_table_is_required(self):
        with pytest.raises(SystemExit):
            backfill_script.main([])

    def test_backfilled_rows_read_the_same_through_the_public_helpers(self):
        table = FakeTable(_legacy_rows())
        before = {
            (row["source_id"], row["version"]): (
                record_previous_version(row),
                resolve_record_acl(row),
                record_source_name(row),
            )
            for row in table.rows
            if row.get("version") != "__publish_lock__"
        }
        backfill_script.backfill(table, execute=True, limit=None, verbose=False)
        after = {
            (row["source_id"], row["version"]): (
                record_previous_version(row),
                resolve_record_acl(row),
                record_source_name(row),
            )
            for row in table.rows
            if row.get("version") != "__publish_lock__"
        }
        # The backfill is a storage move, not a semantic change: every reader
        # sees exactly what it saw before, minus the normalized acl spelling.
        for key, (prev, _acl, name) in before.items():
            assert after[key][0] == prev
            assert after[key][2] == name


# --- v1 payload migration keeps identity out of extensions -------------------


class TestV1PayloadIdentity:
    def test_v1_source_id_lands_top_level_not_in_extensions(self, env):
        client = TestClient(app)
        response = client.post(
            "/submit",
            headers=OWNER_HEADERS,
            json={
                "dc": {
                    "titles": [{"title": "Migrated v1 dataset"}],
                    "creators": [{"creatorName": "Ward, Logan"}],
                },
                "mdf": {
                    "source_id": "old_v1_dataset",
                    "source_name": "old_v1_family",
                    "organization": "MDF Open",
                },
                "data_sources": ["https://example.com/data.csv"],
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["source_id"] == "old_v1_dataset"

        record = _record(client, "old_v1_dataset")
        assert record["source_name"] == "old_v1_family"
        extensions = record["dataset_mdata"].get("extensions") or {}
        assert "mdf_source_id" not in extensions
        assert "mdf_source_name" not in extensions

    def test_v1_acl_is_promoted_to_the_record(self, env):
        client = TestClient(app)
        response = client.post(
            "/submit",
            headers=OWNER_HEADERS,
            json={
                "dc": {
                    "titles": [{"title": "Restricted v1 dataset"}],
                    "creators": [{"creatorName": "Ward, Logan"}],
                },
                "mdf": {"source_id": "restricted_v1", "acl": [IDENTITY_A]},
                "data_sources": ["https://example.com/data.csv"],
            },
        )
        assert response.status_code == 200, response.text

        record = _record(client, "restricted_v1")
        assert record["acl"] == [f"urn:globus:auth:identity:{IDENTITY_A}"]
        assert "acl" not in record["dataset_mdata"]
        assert not dataset_is_public(record)


# --- DynamoDB index hydration policy -----------------------------------------


class RecordingDynamoTable:
    """Records which DynamoDB calls a store method makes."""

    name = "recording-submissions"

    def __init__(self, items: List[Dict[str, Any]]):
        self.items = items
        self.queries: List[Dict[str, Any]] = []
        self.batch_gets = 0

    def query(self, **kwargs):
        self.queries.append(kwargs)
        return {"Items": [dict(item) for item in self.items]}

    def scan(self, **kwargs):
        return {"Items": [dict(item) for item in self.items]}


class RecordingResource:
    def __init__(self, table: RecordingDynamoTable):
        self.table = table

    def batch_get_item(self, RequestItems):
        self.table.batch_gets += 1
        keys = RequestItems[self.table.name]["Keys"]
        wanted = {(key["source_id"], key["version"]) for key in keys}
        return {
            "Responses": {
                self.table.name: [
                    {**item, "hydrated": True}
                    for item in self.table.items
                    if (item.get("source_id"), item.get("version")) in wanted
                ]
            },
            "UnprocessedKeys": {},
        }


class TestDynamoIndexHydration:
    ROWS = [
        {
            "source_id": "ds-1",
            "version": "1.0",
            "status": "pending_curation",
            "curation_queue": "pending_curation",
            "user_id": "owner-user",
            "organization": "MDF Open",
            "legacy_source_id": "old-ds-1",
        }
    ]

    def _store(self, monkeypatch):
        from v2.store import DynamoSubmissionStore

        store = DynamoSubmissionStore.__new__(DynamoSubmissionStore)
        table = RecordingDynamoTable([dict(row) for row in self.ROWS])
        store.table = table
        store._resource = RecordingResource(table)

        from boto3.dynamodb.conditions import Key

        store._key = Key
        return store, table

    def test_all_projected_indexes_are_not_hydrated(self, monkeypatch):
        """user-/org-/legacy- indexes are ProjectionType: ALL — no BatchGetItem."""
        store, table = self._store(monkeypatch)

        rows, _ = store.list_by_user("owner-user", limit=10)
        assert rows and table.batch_gets == 0

        rows, _ = store.list_by_org("MDF Open", limit=10)
        assert rows and table.batch_gets == 0

        assert store.get_by_legacy_source_id("old-ds-1") is not None
        assert table.batch_gets == 0

    def test_curation_queue_index_is_hydrated(self, monkeypatch):
        """curation-queue-index is KEYS_ONLY, so it must be hydrated."""
        store, table = self._store(monkeypatch)
        rows = store.list_by_status(["pending_curation"], limit=10)
        assert table.batch_gets == 1
        assert rows[0]["hydrated"] is True

    def test_empty_curation_index_falls_back_to_a_scan(self, monkeypatch):
        """Right after deploy the sparse index is empty for the existing backlog."""
        from v2.store import DynamoSubmissionStore

        store, table = self._store(monkeypatch)
        monkeypatch.setattr(DynamoSubmissionStore, "_curation_index_warned", False)
        table.query = lambda **kwargs: {"Items": []}

        rows = store.list_by_status(["pending_curation"], limit=10)
        assert [row["source_id"] for row in rows] == ["ds-1"]
        # Answered from the scan, not from a hydration round-trip.
        assert table.batch_gets == 0

    def test_missing_curation_index_falls_back_to_a_scan(self, monkeypatch):
        """Between the two GSI-swap deploys the index may not exist yet."""
        from botocore.exceptions import ClientError

        store, table = self._store(monkeypatch)

        def missing_index(**kwargs):
            raise ClientError(
                {"Error": {"Code": "ValidationException",
                           "Message": "The table does not have the specified index"}},
                "Query",
            )

        table.query = missing_index
        rows = store.list_by_status(["pending_curation"], limit=10)
        assert [row["source_id"] for row in rows] == ["ds-1"]

    def test_other_query_errors_propagate(self, monkeypatch):
        from botocore.exceptions import ClientError

        store, table = self._store(monkeypatch)

        def throttled(**kwargs):
            raise ClientError(
                {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "x"}},
                "Query",
            )

        table.query = throttled
        with pytest.raises(ClientError):
            store.list_by_status(["pending_curation"], limit=10)

    def test_partially_indexed_statuses_return_full_records(self, monkeypatch):
        """Index hits are KEYS_ONLY and must be hydrated even when another
        status falls back to the scan."""
        from v2.store import DynamoSubmissionStore

        store, table = self._store(monkeypatch)
        monkeypatch.setattr(DynamoSubmissionStore, "_curation_index_warned", False)
        real_query = table.query

        def query(**kwargs):
            status = kwargs["KeyConditionExpression"].get_expression()["values"][1]
            if status == "pending_curation":
                return real_query(**kwargs)
            return {"Items": []}

        table.query = query
        rows = store.list_by_status(["pending_curation", "rejected"], limit=10)
        assert rows and rows[0].get("hydrated") is True

    def test_batch_get_retry_is_bounded(self, monkeypatch):
        """A permanently throttled BatchGetItem must not loop forever."""
        store, table = self._store(monkeypatch)
        attempts = {"n": 0}

        def always_unprocessed(RequestItems):
            attempts["n"] += 1
            return {
                "Responses": {},
                "UnprocessedKeys": {table.name: RequestItems[table.name]},
            }

        store._resource.batch_get_item = always_unprocessed
        monkeypatch.setattr("time.sleep", lambda *_: None)

        assert store.list_by_status(["pending_curation"], limit=10) == []
        assert attempts["n"] == store._BATCH_GET_MAX_ATTEMPTS
