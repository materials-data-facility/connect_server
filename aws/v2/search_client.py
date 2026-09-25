"""Globus Search client for MDF v2.

Provides search ingest and query capabilities via Globus Search indexes.
Falls back to MockGlobusSearchClient when credentials or indexes are not configured.

RE-INGEST REQUIRED AFTER DEPLOY
-------------------------------
``build_gmeta_entry`` now writes a FLAT ``content`` block using v2 field names
(``title``, ``authors``, ``keywords``, ``publication_year``, ``organization``,
``domains``, ``ingest_date``, ...) instead of the v1 ``mdf``/``dc``/``data``
nesting, and ``DEFAULT_FACETS``/the ``/search`` filter map bind to those flat
names. Entries already in the index still carry the nested layout, so until they
are rewritten they will not match a flat filter and will contribute nothing to a
facet bucket. ``_flat_content`` is a tolerant reader that normalizes both
layouts, which keeps *result rendering* correct in the meantime, but faceting and
filtering are done inside Globus Search and cannot be shimmed. The whole index
(935 entries on staging) must therefore be re-ingested right after the deploy —
the orchestrator runs the existing rebuild tooling, from ``aws/``:

    PYTHONPATH=. python v2/scripts/wipe_search_index.py --env staging --execute
    PYTHONPATH=. python v2/scripts/rebuild_search_from_converted.py \\
        --env staging -i converted-latest.json --execute

The rebuild script needs no change: it calls ``build_gmeta_entry`` itself, so it
emits the new layout automatically. ``subject`` (``{detail_base}/{source_id}``)
is unchanged, so re-ingest overwrites entries in place rather than duplicating
them. Verify afterwards with ``v2/scripts/reconcile_migration.py --check-search``
and by confirming ``GET /search?q=band`` still returns the five facet groups
(Year, Organization, Authors, Keywords, Domains) with non-empty buckets.
"""

import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from v2.submission_utils import (
    record_legacy_source_id,
    record_previous_version,
    record_root_version,
    record_source_name,
)

logger = logging.getLogger(__name__)

# Historical production value. Search subjects are the index's identity keys, so
# this is the fallback used when no portal URL is configured — see _detail_base().
MDF_DETAIL_BASE = "https://materialsdatafacility.org/detail"

# Globus Search ingest task states (GET /v1/task/<task_id> -> {"state": ...}).
TASK_STATE_SUCCESS = "SUCCESS"
TASK_STATE_FAILED = "FAILED"
TASK_TERMINAL_FAILURE_STATES = frozenset({TASK_STATE_FAILED})

# Default wall-clock budget for confirming an ingest task. Deliberately small:
# ingest() is called from the API request path too (withdraw/delete reconcile),
# where API Gateway caps a request at 29s. The publish worker passes its own,
# larger budget explicitly.
DEFAULT_INGEST_WAIT_SECONDS = 20.0

# Per-HTTP-call ceiling for the authenticated client. The polling loop below is
# itself the retry mechanism, so the transport must not add its own (globus_sdk
# defaults to a 60s socket timeout and up to 5 automatic retries — one hung call
# would blow through both the ingest budget and the Lambda's whole timeout).
#
# Kept comfortably below the publish path's minimum ingest budget (5s) so that
# even the smallest budget affords several polls, while capping how far a single
# hung call can overrun a deadline.
DEFAULT_HTTP_TIMEOUT_SECONDS = 3.0
DEFAULT_HTTP_MAX_RETRIES = 0

# 4xx statuses that are worth retrying; every other 4xx is a permanent error
# (bad request, unauthorized, unknown task) and must fail fast.
RETRYABLE_CLIENT_ERROR_STATUSES = frozenset({408, 425, 429})


def _http_timeout_seconds() -> float:
    return _float_env("GLOBUS_HTTP_TIMEOUT_SECONDS", DEFAULT_HTTP_TIMEOUT_SECONDS)


def _http_max_retries() -> int:
    return int(_float_env("GLOBUS_HTTP_MAX_RETRIES", DEFAULT_HTTP_MAX_RETRIES))


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning("Invalid %s=%r, using default %s", name, raw, default)
        return default


def _build_search_client(globus_sdk, **kwargs) -> Any:
    """Construct a SearchClient with a bounded HTTP transport.

    The transport API moved between globus_sdk 3.x (``transport_params``) and
    4.x (``transport=`` / ``retry_config=``) and the pinned range is ">=3.0", so
    both shapes are attempted before falling back to an unbounded client.
    """
    timeout = _http_timeout_seconds()
    retries = _http_max_retries()

    try:  # globus_sdk 4.x
        from globus_sdk.transport import RequestsTransport, RetryConfig

        return globus_sdk.SearchClient(
            transport=RequestsTransport(http_timeout=timeout),
            retry_config=RetryConfig(max_retries=retries),
            **kwargs,
        )
    except Exception:
        pass

    try:  # globus_sdk 3.x
        return globus_sdk.SearchClient(
            transport_params={"http_timeout": timeout, "max_retries": retries},
            **kwargs,
        )
    except Exception:
        logger.warning(
            "Could not configure Globus Search HTTP timeout/retries; "
            "falling back to SDK defaults (calls may block far longer than the ingest budget)",
            exc_info=True,
        )

    return globus_sdk.SearchClient(**kwargs)


def _is_permanent_api_error(exc: Exception) -> bool:
    """True for an HTTP error that will not succeed on retry (4xx, mostly)."""
    status = getattr(exc, "http_status", None)
    if status is None:
        status = getattr(exc, "status_code", None)
    try:
        status = int(status) if status is not None else None
    except (TypeError, ValueError):
        return False
    if status is None:
        return False
    return 400 <= status < 500 and status not in RETRYABLE_CLIENT_ERROR_STATUSES


def _detail_base() -> str:
    """Base URL for search subjects: ``{base}/{source_id}``.

    The subject is the Globus Search index's *identity key* for a dataset, so it
    must stay byte-stable across deploys: change it and every existing entry is
    orphaned (duplicated on the next publish, un-deletable by delete_entry).

    Resolution order:
      1. ``SEARCH_SUBJECT_BASE`` — explicit escape hatch, used verbatim.
      2. ``PORTAL_URL`` — the same portal config email_utils consumes.
      3. The historical production value.

    The host is canonicalized (leading ``www.`` stripped, a trailing ``/detail``
    not doubled) because ``PORTAL_URL`` is a *display* URL and is deployed as
    ``https://www.materialsdatafacility.org``, while every subject already in the
    index is ``https://materialsdatafacility.org/detail/...``. Canonicalizing
    keeps the two spellings pointing at one identity.
    """
    explicit = os.environ.get("SEARCH_SUBJECT_BASE")
    if explicit:
        return explicit.rstrip("/")

    portal = (os.environ.get("PORTAL_URL") or "").strip().rstrip("/")
    if not portal:
        return MDF_DETAIL_BASE

    portal = portal.replace("://www.", "://", 1)
    if portal.endswith("/detail"):
        return portal
    return f"{portal}/detail"


def _default_ingest_wait_seconds() -> float:
    """Seconds to wait for an ingest task to reach a terminal state (0 = don't)."""
    return _float_env("SEARCH_INGEST_WAIT_SECONDS", DEFAULT_INGEST_WAIT_SECONDS)

# Facets served on every /search response. ``name`` is the public, client-facing
# label (the frontend and CLI key their filter UI off these exact strings, so
# they are part of the API contract and must not change); ``field_name`` is the
# flat index field it aggregates and may change freely with the index layout.
DEFAULT_FACETS = [
    {"name": "Year",         "field_name": "publication_year", "type": "terms", "size": 20},
    {"name": "Organization", "field_name": "organization",     "type": "terms", "size": 20},
    {"name": "Authors",      "field_name": "authors",          "type": "terms", "size": 20},
    {"name": "Keywords",     "field_name": "keywords",         "type": "terms", "size": 20},
    {"name": "Domains",      "field_name": "domains",          "type": "terms", "size": 20},
]

# The flat content fields the API exposes as facets, hence the only fields it
# accepts a filter on.
_FACETED_FIELDS = frozenset(facet["field_name"] for facet in DEFAULT_FACETS)

# Index field names as they were spelled before the flatten. Any caller still
# passing a dotted v1 path (a stale filter/sort clause, a bookmarked query) is
# translated rather than silently matching nothing. Retire with _flat_content.
_LEGACY_FIELD_ALIASES = {
    "dc.year": "publication_year",
    "dc.title": "title",
    "dc.creators.name": "authors",
    "dc.subjects": "keywords",
    "dc.description": "description",
    "dc.license": "license",
    "dc.doi": "doi",
    "mdf.organization": "organization",
    "mdf.domains": "domains",
    "mdf.ingest_date": "ingest_date",
    "mdf.source_id": "source_id",
    "mdf.source_name": "source_name",
    "mdf.resource_type": "resource_type",
    "mdf.version": "version",
    "mdf.latest": "latest",
    "data.size_bytes": "size_bytes",
    "data.file_count": "file_count",
}

# Fail-closed principal for a record whose ACL cannot be determined. Globus
# Search requires a non-empty visible_to, and "readable by nobody" is the safe
# outcome for a record we cannot inspect. The nil UUID is a syntactically valid
# identity that is never issued, so it matches no caller.
DENY_ALL_PRINCIPAL = "urn:globus:auth:identity:00000000-0000-0000-0000-000000000000"


def _canonical_field(field_name: Any) -> str:
    """Map a pre-flatten dotted field name onto its flat v2 equivalent."""
    name = str(field_name or "")
    return _LEGACY_FIELD_ALIASES.get(name, name)


def _canonical_filters(
    filters: Optional[Dict[str, List]],
) -> Optional[Dict[str, List]]:
    """Rewrite a filters dict's field names to the flat index layout."""
    if not filters:
        return filters
    return {_canonical_field(field): values for field, values in filters.items()}


def _canonical_sort(
    sort: Optional[List[Dict[str, str]]],
) -> Optional[List[Dict[str, str]]]:
    """Rewrite a Globus sort clause's field names to the flat index layout."""
    if not sort:
        return sort
    return [
        {**clause, "field_name": _canonical_field(clause.get("field_name"))}
        for clause in sort
    ]


def _author_names(value: Any) -> List[str]:
    """Author display names from flat strings or v1 ``{"name": ...}`` dicts."""
    names: List[str] = []
    for item in value or []:
        name = item.get("name") if isinstance(item, dict) else item
        if name:
            names.append(str(name))
    return names


def _flat_content(content: Any) -> Dict[str, Any]:
    """Return a GMeta ``content`` block in the flat v2 field layout.

    Tolerant reader for the transition window described in the module docstring:
    entries ingested before the flatten nest their fields under
    ``mdf``/``dc``/``data``, so every reader goes through here instead of
    guessing at the layout. Also strips ``acl`` — no longer written, but present
    on every pre-flatten entry, and it must never reach a client.

    Delete this function (and ``_LEGACY_FIELD_ALIASES``) once the index has been
    fully re-ingested.
    """
    if not isinstance(content, dict):
        return {}

    mdf = content.get("mdf")
    dc = content.get("dc")
    data_block = content.get("data")

    if not any(isinstance(block, dict) for block in (mdf, dc, data_block)):
        flat = dict(content)
        flat["authors"] = _author_names(flat.get("authors"))
        flat.pop("acl", None)
        return flat

    mdf = mdf if isinstance(mdf, dict) else {}
    dc = dc if isinstance(dc, dict) else {}
    data_block = data_block if isinstance(data_block, dict) else {}

    flat = {k: v for k, v in content.items() if k not in ("mdf", "dc", "data")}
    flat.update(mdf)
    flat.update({
        "title": dc.get("title"),
        "authors": _author_names(dc.get("creators")),
        "publisher": dc.get("publisher"),
        "description": dc.get("description") or "",
        "keywords": dc.get("subjects") or [],
        "publication_year": dc.get("year"),
        "license": dc.get("license") or None,
        "location": data_block.get("location"),
        "size_bytes": data_block.get("size_bytes"),
        "file_count": data_block.get("file_count"),
    })
    doi = dc.get("doi") or mdf.get("dataset_doi")
    if doi:
        flat["doi"] = doi
    flat.pop("acl", None)
    return flat


def resolve_visible_to(submission: Dict[str, Any], meta: Any = None) -> List[str]:
    """``visible_to`` for a submission's GMeta entry.

    Derived from ``v2.search.dataset_is_public`` so the index and every
    server-side reader share one definition of "public". The inline
    ``meta.acl or ["public"]`` this replaces failed OPEN on metadata it could not
    parse — an unreadable ``dataset_mdata`` yielded an empty acl, which read as
    "public" and published a restricted dataset to the world.

    Semantics are otherwise unchanged: public -> ``["public"]``, restricted ->
    one Globus principal per acl entry. Each entry is canonicalized by
    ``submission_utils.normalize_acl_principal``: a bare UUID is prefixed with
    ``urn:globus:auth:identity:``, an already-qualified ``urn:globus:...``
    value (an identity URN a client already expanded, or a group URN
    ``urn:globus:groups:id:<uuid>``) is passed through verbatim, and anything
    else is dropped with a warning. Blind prefixing produced
    ``urn:globus:auth:identity:urn:globus:auth:identity:<uuid>``, an unknown
    principal — the dataset was then invisible to its own collaborators.

    The ACL itself comes from ``submission_utils.resolve_record_acl`` (top-level
    record attribute first, legacy ``dataset_mdata.acl`` second) rather than the
    parsed metadata blob, so a backfilled record and an un-backfilled one index
    identically. ``meta`` is accepted for the legacy call shape and used only as
    a last resort.
    """
    from v2.search import dataset_is_public
    from v2.submission_utils import normalize_acl_principal, resolve_record_acl

    if dataset_is_public(submission):
        return ["public"]

    acl = resolve_record_acl(submission)
    if acl is None and meta is not None:
        acl = getattr(meta, "acl", None)

    source_id = submission.get("source_id") if isinstance(submission, dict) else None
    principals: List[str] = []
    for entry in acl or []:
        if not entry or entry == "public":
            continue
        principal = normalize_acl_principal(entry)
        if principal is None or principal == "public":
            # Never widen to public to salvage an unreadable entry.
            logger.warning(
                "Dropping unusable acl entry %r on %s; it is neither a Globus "
                "identity UUID nor a urn:globus: principal",
                entry,
                source_id or "<unknown>",
            )
            continue
        if principal not in principals:
            principals.append(principal)
    if principals:
        return principals

    logger.error(
        "Cannot determine ACL for %s; indexing it visible to nobody",
        source_id or "<unknown>",
    )
    return [DENY_ALL_PRINCIPAL]


class GlobusSearchClient:
    """Wraps globus_sdk.SearchClient for MDF v2 search operations."""

    def __init__(self, index_id: str, test_mode: bool = False):
        self.index_id = index_id
        self.test_mode = test_mode
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client

        import globus_sdk

        client_id = os.environ.get("GLOBUS_CLIENT_ID")
        client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")

        if not client_id or not client_secret:
            raise RuntimeError("GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET required for Globus Search")

        confidential_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
        token_response = confidential_client.oauth2_client_credentials_tokens(
            requested_scopes="urn:globus:auth:scope:search.api.globus.org:all"
        )
        search_token = token_response.by_resource_server.get("search.api.globus.org", {})
        access_token = search_token.get("access_token") if isinstance(search_token, dict) else getattr(search_token, "access_token", None)
        if not access_token:
            raise RuntimeError(
                "Failed to obtain Globus Search access token. "
                "Ensure the app has the 'urn:globus:auth:scope:search.api.globus.org:all' scope configured."
            )

        authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
        self._client = _build_search_client(globus_sdk, authorizer=authorizer)
        return self._client

    def build_gmeta_entry(
        self, submission: Dict[str, Any], version_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Build a GMetaEntry from a submission record.

        ``content`` is FLAT: v2 field names at the top level, no
        ``mdf``/``dc``/``data`` nesting. See the module docstring for the
        re-ingest this layout change requires.

        ``acl`` is deliberately absent from ``content``. Access is enforced by
        ``visible_to``; publishing the list as searchable content handed every
        anonymous searcher the Globus identity ids of a restricted dataset's
        readers.

        ``subject`` is unchanged and must stay byte-identical — it is the
        index's identity key for the dataset.
        """
        from v2.metadata import parse_metadata

        source_id = submission.get("source_id", "unknown")
        version = submission.get("version", "1.0")
        meta = parse_metadata(submission)

        subject = f"{_detail_base()}/{source_id}"

        visible_to = resolve_visible_to(submission, meta)

        # Extract data location from first data_source
        data_sources = meta.data_sources or []
        location = data_sources[0] if data_sources else None

        # source_name is the v1 dataset-family grouping/facet key. It is a
        # top-level record attribute in the v2.1 shape (N4) — it decides a
        # search facet and must not be rewritable through the user-editable
        # extensions blob — with the deprecated extensions.mdf_source_name as a
        # fallback for rows the backfill has not reached. Never derive it with
        # rsplit("-", 1): for v2-native "mdf-<uuid>" ids that collapsed every
        # dataset to source_name="mdf".
        source_name = record_source_name(submission) or source_id

        content: Dict[str, Any] = {
            # --- Identity / lifecycle
            "source_id": source_id,
            "source_name": source_name,
            # v1 parity: the v1 enumeration/extraction queries filter on
            # resource_type:"dataset" (spelled mdf.resource_type before the
            # flatten). Without it, v2-native datasets are invisible to the v1
            # sync/migration tooling (zero rows on re-sync).
            "resource_type": "dataset",
            # Only published datasets are ever indexed, but carrying the status
            # explicitly means readers no longer have to hardcode it.
            "status": submission.get("status") or "published",
            "version": meta.version or version,
            "latest": meta.latest,
            "ingest_date": submission.get("created_at") or datetime.now(timezone.utc).isoformat(),
            # --- Descriptive (facets bind to publication_year / organization /
            #     authors / keywords / domains)
            "title": meta.title,
            "authors": [a.name for a in meta.authors],
            "publisher": meta.publisher,
            "description": meta.description or "",
            "keywords": meta.keywords,
            "domains": meta.domains,
            "organization": submission.get("organization", ""),
            "publication_year": meta.publication_year or datetime.now().year,
            "license": (meta.license.identifier or meta.license.name) if meta.license else "",
            # --- Data
            "location": location,
            "size_bytes": submission.get("total_bytes"),
            "file_count": submission.get("file_count"),
        }

        # doi = version-specific DOI if present, otherwise the dataset DOI.
        doi = submission.get("doi") or submission.get("dataset_doi")
        if doi:
            content["doi"] = doi

        dataset_doi = submission.get("dataset_doi")
        if dataset_doi:
            content["dataset_doi"] = dataset_doi
        if version_count is not None:
            content["version_count"] = version_count

        # Bare version strings (N3), read from the top-level record attributes
        # with a normalizing fallback to the legacy composite in the blob.
        root_version = record_root_version(submission)
        if root_version:
            content["root_version"] = root_version
        previous_version = record_previous_version(submission)
        if previous_version:
            content["previous_version"] = previous_version

        # The original v1 id, so old-id lookups can be resolved from the index
        # alone. Migrated records keep it in extensions when it duplicates the
        # canonical source_id (DynamoDB rejects empty/equal GSI keys).
        legacy_source_id = record_legacy_source_id(submission)
        if legacy_source_id:
            content["legacy_source_id"] = legacy_source_id

        if meta.download_url:
            content["download_url"] = meta.download_url

        if meta.external:
            content["external_source"] = meta.external.source
            if meta.external.doi:
                content["external_doi"] = meta.external.doi
            if meta.external.url:
                content["external_url"] = meta.external.url

        return {
            "subject": subject,
            "visible_to": visible_to,
            "content": content,
        }

    def get_task(self, task_id: str) -> Dict[str, Any]:
        """Return the raw Globus Search task document for ``task_id``."""
        client = self._get_client()
        resp = client.get_task(task_id)
        data = getattr(resp, "data", None)
        if isinstance(data, dict):
            return data
        return resp if isinstance(resp, dict) else {}

    def _wait_for_task(self, task_id: str, deadline: float) -> Dict[str, Any]:
        """Poll an ingest task until it is terminal, ``deadline`` passes, or it fails.

        Globus Search's ingest endpoint returns *acceptance*, not completion: the
        documents are not queryable — and may never become queryable — when
        ingest() returns. Publishing is gated on this confirmation.

        ``deadline`` is a ``time.monotonic()`` timestamp covering the *whole*
        ingest operation (submission included), not just this loop.

        Error classification matters as much as the states: a 4xx is permanent
        (bad task id, revoked credentials) and fails immediately, while 5xx and
        network errors are transient and keep polling. The transport is capped at
        ``GLOBUS_HTTP_TIMEOUT_SECONDS`` per call with SDK retries disabled, so a
        single hung call cannot overrun the deadline by more than that ceiling —
        without that cap globus_sdk would allow 60s per call and retry it 5 times.
        """
        per_call = _http_timeout_seconds()
        delay = 0.5
        last_state = None
        last_error = None
        polled = False

        while True:
            try:
                task = self.get_task(task_id)
                polled = True
                last_state = (task.get("state") or "").upper()
                if last_state == TASK_STATE_SUCCESS:
                    return {"success": True, "state": last_state, "task_id": task_id}
                if last_state in TASK_TERMINAL_FAILURE_STATES:
                    message = task.get("message") or task.get("fatal_error") or "ingest task failed"
                    return {
                        "success": False,
                        "state": last_state,
                        "task_id": task_id,
                        "error": f"Globus Search ingest task {task_id} {last_state}: {message}",
                    }
            except Exception as exc:
                last_error = str(exc)
                if _is_permanent_api_error(exc):
                    logger.warning(
                        "Globus Search get_task(%s) failed permanently: %s", task_id, exc,
                    )
                    return {
                        "success": False,
                        "state": last_state,
                        "task_id": task_id,
                        "error": (
                            f"Globus Search ingest task {task_id} could not be confirmed: {exc}"
                        ),
                    }
                logger.warning("Globus Search get_task(%s) failed (transient): %s", task_id, exc)

            remaining = deadline - time.monotonic()
            # Stop when the budget is gone, or when what is left cannot fit
            # another bounded call (which would overrun the deadline). Always
            # allow one attempt, so a tiny budget still asks once.
            if remaining <= 0 or (polled and remaining < per_call):
                detail = f"last state {last_state or 'unknown'}"
                if last_error:
                    detail += f", last poll error: {last_error}"
                return {
                    "success": False,
                    "timed_out": True,
                    "state": last_state,
                    "task_id": task_id,
                    "error": (
                        f"Globus Search ingest task {task_id} not confirmed "
                        f"within the ingest budget ({detail})"
                    ),
                }
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, 5.0)

    def ingest(
        self,
        submission: Dict[str, Any],
        version_count: Optional[int] = None,
        wait_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Ingest a single submission and confirm the ingest task completed.

        ``wait_seconds`` bounds the *whole* operation — submitting the ingest and
        polling its task — because the submission call can hang too.

        - ``None``  -> the ``SEARCH_INGEST_WAIT_SECONDS`` default.
        - ``0``     -> accept-only mode: return as soon as the request is
          accepted, with ``confirmed: False``. Legitimate only for callers that
          are not gating a publish on the result (the withdraw/delete reconcile
          path); the publish worker always passes a positive budget.

        Callers that need the entry to actually be in the index must require
        ``success and confirmed``: ``success: True, confirmed: False`` means the
        request was accepted and nothing more.
        """
        client = self._get_client()
        entry = self.build_gmeta_entry(submission, version_count=version_count)

        ingest_doc = {
            "ingest_type": "GMetaEntry",
            "ingest_data": entry,
        }

        if wait_seconds is None:
            wait_seconds = _default_ingest_wait_seconds()
        deadline = time.monotonic() + wait_seconds

        try:
            result = client.ingest(self.index_id, ingest_doc)
            data = getattr(result, "data", None)
            task_id = data.get("task_id") if isinstance(data, dict) else None
        except Exception as exc:
            logger.exception("Globus Search ingest failed for %s", submission.get("source_id"))
            return {"success": False, "confirmed": False, "error": str(exc)}

        if wait_seconds <= 0:
            return {"success": True, "task_id": task_id, "confirmed": False}

        if not task_id:
            # Nothing to poll: the request was accepted but its completion can
            # never be established. A caller that asked for confirmation asked
            # for a guarantee this response cannot give, so this is a failure —
            # re-ingesting is an idempotent upsert, so failing costs nothing.
            logger.warning(
                "Globus Search ingest for %s returned no task_id; completion cannot be confirmed",
                submission.get("source_id"),
            )
            return {
                "success": False,
                "task_id": None,
                "confirmed": False,
                "error": (
                    "Globus Search ingest returned no task_id; "
                    "completion could not be confirmed"
                ),
            }

        status = self._wait_for_task(task_id, deadline)
        outcome: Dict[str, Any] = {
            "success": bool(status.get("success")),
            "task_id": task_id,
            "task_state": status.get("state"),
            "confirmed": bool(status.get("success")),
        }
        if status.get("timed_out"):
            outcome["timed_out"] = True
        if not status.get("success"):
            outcome["error"] = status.get("error")
            logger.warning(
                "Globus Search ingest not confirmed for %s: %s",
                submission.get("source_id"), outcome["error"],
            )
        return outcome

    def batch_ingest(
        self, submissions: List[Dict[str, Any]], batch_size: int = 100,
    ) -> Dict[str, Any]:
        """Ingest multiple submissions using GMetaList batches.

        Batches submissions into groups of batch_size and submits each as a
        single GMetaList request (one task_id per batch). Much faster than
        individual ingest() calls for bulk loading — 10 req/s rate limit and
        10MB per request apply; batch_size=100 stays well within the 10MB cap.

        Returns a summary dict with counts and any per-batch errors.
        """
        client = self._get_client()
        total = len(submissions)
        ingested = 0
        errors = []
        task_ids = []

        for batch_start in range(0, total, batch_size):
            batch = submissions[batch_start:batch_start + batch_size]
            gmeta = []
            for sub in batch:
                try:
                    gmeta.append(self.build_gmeta_entry(sub))
                except Exception as exc:
                    errors.append({"source_id": sub.get("source_id"), "error": str(exc)})

            if not gmeta:
                continue

            ingest_doc = {
                "ingest_type": "GMetaList",
                "ingest_data": {"gmeta": gmeta},
            }

            try:
                result = client.ingest(self.index_id, ingest_doc)
                data = result.data if hasattr(result, "data") else {}
                task_id = data.get("task_id")
                if task_id:
                    task_ids.append(task_id)
                ingested += len(gmeta)
            except Exception as exc:
                logger.exception(
                    "Globus Search batch ingest failed (batch %d-%d)",
                    batch_start, batch_start + len(batch) - 1,
                )
                for sub in batch:
                    errors.append({"source_id": sub.get("source_id"), "error": str(exc)})

        return {
            "success": len(errors) == 0,
            "total": total,
            "ingested": ingested,
            "errors": errors,
            "task_ids": task_ids,
        }

    def delete_entry(self, source_id: str) -> Dict[str, Any]:
        """Delete a subject entry from the index."""
        client = self._get_client()
        subject = f"{_detail_base()}/{source_id}"

        try:
            client.delete_entry(self.index_id, subject)
            return {"success": True, "source_id": source_id}
        except Exception as exc:
            logger.exception("Globus Search delete failed for %s", source_id)
            return {"success": False, "error": str(exc)}

    def _get_read_client(self):
        """Return an unauthenticated SearchClient for public index reads.

        The MDF Search index is public, so queries do not require credentials.
        Only ingest/delete operations use the authenticated client from _get_client().
        """
        import globus_sdk
        return _build_search_client(globus_sdk)

    def search(self, query: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """Search the Globus Search index."""
        client = self._get_read_client()

        try:
            result = client.search(self.index_id, query, limit=limit, offset=offset)
            data = result.data if hasattr(result, "data") else result
            return {
                "success": True,
                "total": data.get("total", 0),
                "results": _format_globus_search_results(data),
            }
        except Exception as exc:
            logger.exception("Globus Search query failed")
            return {"success": False, "error": str(exc), "total": 0, "results": []}

    def faceted_search(
        self, query: str, limit: int = 20, offset: int = 0, filters: Optional[Dict[str, List]] = None,
        sort: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Search with facets and optional filters.

        filters: dict mapping index field_name → list of selected values
                 e.g. {"organization": ["MDF Open"], "publication_year": [2024, 2025]}
        sort:    Globus Search sort clause, e.g.
                 [{"field_name": "ingest_date", "order": "desc"}].
                 None leaves the engine's own relevance ordering in place.

        Pre-flatten dotted field names (``dc.year``, ``mdf.organization``, ...)
        are accepted and translated; see ``_LEGACY_FIELD_ALIASES``.

        Filter values must be the *whole* facet value. These fields are indexed
        as exact keywords — verified against the production index, where a
        match_any on "Blaiszik, Ben" returns 20 datasets while "Blaiszik" alone
        returns 0 — so callers must never pre-split a value on punctuation.
        """
        from globus_sdk import SearchQuery

        client = self._get_read_client()
        sq = SearchQuery(query)

        for facet in DEFAULT_FACETS:
            sq.add_facet(**facet)

        filters = _canonical_filters(filters)
        sort = _canonical_sort(sort)

        if filters:
            for field_name, values in filters.items():
                sq.add_filter(field_name, values, type="match_any")

        if sort:
            sq["sort"] = sort

        sq["limit"] = limit
        sq["offset"] = offset

        try:
            result = client.post_search(self.index_id, sq)
            data = result.data if hasattr(result, "data") else result
            return {
                "success": True,
                "total": data.get("total", 0),
                "results": _format_globus_search_results(data),
                "facets": _format_facet_results(data.get("facet_results", [])),
            }
        except Exception as exc:
            logger.exception("Globus Search faceted query failed")
            return {"success": False, "error": str(exc), "total": 0, "results": [], "facets": {}}


class MockGlobusSearchClient:
    """In-memory mock for Globus Search. Used when USE_MOCK_SEARCH=true."""

    def __init__(self, index_id: str = "mock-index", test_mode: bool = False):
        self.index_id = index_id
        self.test_mode = test_mode
        self._entries: Dict[str, Dict[str, Any]] = {}
        # Test seam: make the next N ingest() calls fail, to exercise the
        # publish pipeline's ingest-failure/retry path.
        self.fail_next_ingests = 0
        self.ingest_calls = 0
        # Test seams for the ingest *task* (B-17): the request is accepted but
        # the asynchronous task fails, or never reaches a terminal state.
        self.fail_next_ingest_tasks = 0
        self.timeout_next_ingest_tasks = 0
        # Test seam for a client that accepts the ingest but cannot confirm it
        # (no task id / accept-only transport): success without confirmation.
        self.unconfirmed_next_ingests = 0
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._task_seq = 0

    def build_gmeta_entry(
        self, submission: Dict[str, Any], version_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        # Re-use the real implementation's logic
        real = GlobusSearchClient.__new__(GlobusSearchClient)
        return real.build_gmeta_entry(submission, version_count=version_count)

    def get_entry(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Return the stored GMeta entry for a dataset, or None.

        The index holds one entry per dataset, keyed on the version-less detail
        URL subject, so this is the entry a search would return for source_id.
        """
        return self._entries.get(f"{_detail_base()}/{source_id}")

    def get_task(self, task_id: str) -> Dict[str, Any]:
        """Task document for a mock ingest, mirroring the Globus Search shape."""
        return self._tasks.get(task_id, {"task_id": task_id, "state": "FAILED", "message": "unknown task"})

    def ingest(
        self,
        submission: Dict[str, Any],
        version_count: Optional[int] = None,
        wait_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        self.ingest_calls += 1
        if self.fail_next_ingests > 0:
            self.fail_next_ingests -= 1
            return {
                "success": False,
                "mock": True,
                "error": "mock search ingest failure",
            }

        self._task_seq += 1
        task_id = f"mock-task-{self._task_seq}"

        # Accepted, entry stored, but the caller gets no confirmation — either
        # because it asked for none (wait_seconds=0) or because the client could
        # not provide one. Callers that gate on indexing must treat this as
        # "not indexed" (see the publish worker).
        if self.unconfirmed_next_ingests > 0 or (wait_seconds is not None and wait_seconds <= 0):
            if self.unconfirmed_next_ingests > 0:
                self.unconfirmed_next_ingests -= 1
            entry = self.build_gmeta_entry(submission, version_count=version_count)
            self._entries[entry["subject"]] = entry
            self._tasks[task_id] = {"task_id": task_id, "state": "PENDING"}
            return {
                "success": True,
                "mock": True,
                "subject": entry["subject"],
                "task_id": task_id,
                "confirmed": False,
            }

        # Accepted-but-not-completed cases: the entry never lands in the index.
        if self.fail_next_ingest_tasks > 0:
            self.fail_next_ingest_tasks -= 1
            self._tasks[task_id] = {
                "task_id": task_id, "state": "FAILED", "message": "mock ingest task failure",
            }
            return {
                "success": False,
                "mock": True,
                "task_id": task_id,
                "task_state": "FAILED",
                "confirmed": False,
                "error": f"Globus Search ingest task {task_id} FAILED: mock ingest task failure",
            }
        if self.timeout_next_ingest_tasks > 0:
            self.timeout_next_ingest_tasks -= 1
            self._tasks[task_id] = {"task_id": task_id, "state": "PROGRESS"}
            return {
                "success": False,
                "mock": True,
                "task_id": task_id,
                "task_state": "PROGRESS",
                "confirmed": False,
                "timed_out": True,
                "error": f"Globus Search ingest task {task_id} not confirmed within mock budget",
            }

        entry = self.build_gmeta_entry(submission, version_count=version_count)
        self._entries[entry["subject"]] = entry
        self._tasks[task_id] = {"task_id": task_id, "state": "SUCCESS"}
        return {
            "success": True,
            "mock": True,
            "subject": entry["subject"],
            "task_id": task_id,
            "task_state": "SUCCESS",
            "confirmed": True,
        }

    def batch_ingest(
        self, submissions: List[Dict[str, Any]], batch_size: int = 100,
    ) -> Dict[str, Any]:
        errors = []
        for sub in submissions:
            result = self.ingest(sub)
            if not result.get("success"):
                errors.append({"source_id": sub.get("source_id"), "error": result.get("error")})
        return {
            "success": len(errors) == 0,
            "total": len(submissions),
            "ingested": len(submissions) - len(errors),
            "errors": errors,
            "task_ids": [],
            "mock": True,
        }

    def delete_entry(self, source_id: str) -> Dict[str, Any]:
        subject = f"{_detail_base()}/{source_id}"
        self._entries.pop(subject, None)
        return {"success": True, "mock": True, "source_id": source_id}

    def search(self, query: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        # Simple text match over stored entries
        query_lower = query.lower()
        matches = [
            entry for entry in self._entries.values()
            if query_lower in self._entry_text(entry)
        ]

        paginated = matches[offset:offset + limit]
        results = [self._format_entry(entry) for entry in paginated]

        return {"success": True, "total": len(matches), "results": results, "mock": True}

    @staticmethod
    def _entry_text(entry: Dict[str, Any]) -> str:
        """Lowercased free-text blob a mock query matches against."""
        flat = _flat_content(entry.get("content"))
        return " ".join([
            flat.get("title") or "",
            flat.get("description") or "",
            " ".join(flat.get("keywords") or []),
            flat.get("source_id") or "",
        ]).lower()

    @staticmethod
    def _format_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
        """Format a stored entry the way the real client formats a Globus hit.

        Shared by search() and faceted_search() so the mock cannot drift into
        returning a shape the real backend never produces — hence the shared
        ``_result_from_content``.
        """
        return _result_from_content(entry.get("content", {}), score=1.0)

    def faceted_search(
        self, query: str, limit: int = 20, offset: int = 0, filters: Optional[Dict[str, List]] = None,
        sort: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Faceted search over in-memory entries with filter and sort support."""
        query_lower = query.lower()
        filters = _canonical_filters(filters)
        matches = []
        for entry in self._entries.values():
            if query_lower == "*" or query_lower in self._entry_text(entry):
                if filters and not self._matches_filters(entry.get("content", {}), filters):
                    continue
                matches.append(entry)

        matches = self._apply_sort(matches, sort)

        paginated = matches[offset:offset + limit]
        results = [self._format_entry(entry) for entry in paginated]

        return {
            "success": True,
            "total": len(matches),
            "results": results,
            "facets": self._compute_facets(matches),
            "mock": True,
        }

    @staticmethod
    def _apply_sort(
        entries: List[Dict[str, Any]], sort: Optional[List[Dict[str, str]]],
    ) -> List[Dict[str, Any]]:
        """Order entries by a Globus-style sort clause (dotted field paths)."""
        if not sort:
            return entries
        ordered = list(entries)
        # Applied last-key-first so the first clause wins, matching a stable
        # multi-key sort.
        for clause in reversed(sort):
            field = _canonical_field(clause.get("field_name"))
            descending = clause.get("order", "asc") == "desc"

            def key(entry: Dict[str, Any], field=field) -> str:
                value: Any = _flat_content(entry.get("content"))
                for part in field.split("."):
                    if not isinstance(value, dict):
                        return ""
                    value = value.get(part)
                return "" if value is None else str(value)

            ordered.sort(key=key, reverse=descending)
        return ordered

    def _matches_filters(self, content: Dict[str, Any], filters: Dict[str, List]) -> bool:
        """Check if a content entry matches all active filters.

        Values compare as whole strings, never tokenized. This mirrors the real
        index: these fields are keyword-mapped, so a match_any on the full
        "Blaiszik, Ben" matches and the bare token "Blaiszik" does not.
        """
        flat = _flat_content(content)
        for field_name, values in filters.items():
            field = _canonical_field(field_name)
            if field not in _FACETED_FIELDS:
                continue
            raw = flat.get(field)
            candidates = raw if isinstance(raw, list) else [raw]
            entry_values = [str(v) for v in candidates if v is not None]
            filter_values = [str(v) for v in values]
            if not any(ev in filter_values for ev in entry_values):
                return False
        return True

    def _compute_facets(self, entries: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Compute facet counts from a list of matched entries.

        Driven off DEFAULT_FACETS so the mock's bucket names and the fields they
        aggregate can never drift from what the real index is asked for.
        """
        counters: Dict[str, Counter] = {facet["name"]: Counter() for facet in DEFAULT_FACETS}
        for entry in entries:
            flat = _flat_content(entry.get("content"))
            for facet in DEFAULT_FACETS:
                raw = flat.get(facet["field_name"])
                values = raw if isinstance(raw, list) else [raw]
                for value in values:
                    if value is None or value == "":
                        continue
                    counters[facet["name"]][str(value)] += 1

        return {
            facet["name"]: _clean_facet_buckets([
                {"value": val, "count": count}
                for val, count in counters[facet["name"]].most_common(facet.get("size", 20))
            ], facet["name"])
            for facet in DEFAULT_FACETS
        }


def _result_from_content(content: Dict[str, Any], score: Any = 0) -> Dict[str, Any]:
    """Build one dataset search result from a GMeta ``content`` block.

    These output keys are the public ``/search`` result contract — the frontend
    and the CLI both index into them by name — so they must stay stable even
    though the index layout underneath them changed. The tolerant
    ``_flat_content`` read is what lets a pre-flatten and a post-flatten entry
    render identically in the same response page.
    """
    flat = _flat_content(content)
    description = flat.get("description") or ""
    result_entry = {
        "type": "dataset",
        "source_id": flat.get("source_id"),
        "version": flat.get("version"),
        "title": flat.get("title"),
        "authors": flat.get("authors") or [],
        "keywords": flat.get("keywords") or [],
        "description": description[:300] if len(description) > 300 else description,
        "publication_year": flat.get("publication_year"),
        "organization": flat.get("organization"),
        "domains": flat.get("domains") or [],
        "doi": flat.get("doi") or flat.get("dataset_doi"),
        "license": flat.get("license") or None,
        "size_bytes": flat.get("size_bytes"),
        "file_count": flat.get("file_count"),
        "status": flat.get("status") or "published",
        "score": score,
        "latest": flat.get("latest", True),
    }
    if flat.get("root_version"):
        result_entry["root_version"] = flat["root_version"]
    if flat.get("download_url"):
        result_entry["download_url"] = flat["download_url"]
    return result_entry


def _format_globus_search_results(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize Globus Search response into the MDF result format.

    Handles both response shapes:
    - POST search (post_search): gmeta[i].entries[j].content  (dict)
    - GET  search (search):      gmeta[i].content[j]          (dict in a list)
    """
    results = []
    for gmeta in data.get("gmeta", []):
        # POST search wraps entries; GET search uses content list directly
        if gmeta.get("entries") is not None:
            contents = [e.get("content", {}) for e in gmeta["entries"]]
        else:
            raw = gmeta.get("content", [])
            contents = raw if isinstance(raw, list) else [raw]

        for content in contents:
            if not isinstance(content, dict):
                continue
            results.append(_result_from_content(content, gmeta.get("score", 0)))
    return results


def _format_facet_results(facet_results: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Normalize Globus facet_results into frontend-friendly format."""
    facets = {}
    for fr in facet_results:
        name = fr.get("name", "")
        buckets = [
            {"value": b.get("value"), "count": b.get("count", 0)}
            for b in fr.get("buckets", [])
            if b.get("count", 0) > 0
        ]
        facets[name] = _clean_facet_buckets(buckets, name)
    return facets


def _clean_facet_buckets(buckets: List[Dict[str, Any]], name: str) -> List[Dict[str, Any]]:
    """Drop blank values; show years newest first and other facets by count."""
    cleaned = [b for b in buckets if str(b.get("value") or "").strip()]
    if name == "Year":
        def year_key(bucket):
            value = str(bucket["value"]).strip()
            try:
                return (1, float(value), value)
            except ValueError:
                return (0, 0, value)
        cleaned.sort(key=year_key, reverse=True)
    else:
        cleaned.sort(key=lambda b: b.get("count", 0), reverse=True)
    return cleaned


# Singleton for mock client to persist in-memory state within a Lambda invocation
_mock_client: Optional[MockGlobusSearchClient] = None


def reset_search_client() -> None:
    """Drop the cached mock client (test helper: gives each test a clean index)."""
    global _mock_client
    _mock_client = None


def get_search_client(test_mode: bool = False) -> Any:
    """Factory: returns GlobusSearchClient or MockGlobusSearchClient."""
    global _mock_client

    use_mock = os.environ.get("USE_MOCK_SEARCH", "true").lower() == "true"

    if use_mock:
        if _mock_client is None:
            _mock_client = MockGlobusSearchClient(test_mode=test_mode)
        return _mock_client

    if test_mode:
        index_id = os.environ.get("TEST_SEARCH_INDEX_UUID", "not-configured")
    else:
        index_id = os.environ.get("SEARCH_INDEX_UUID", "not-configured")

    if index_id == "not-configured":
        logger.warning("Search index UUID not configured, falling back to mock")
        if _mock_client is None:
            _mock_client = MockGlobusSearchClient(test_mode=test_mode)
        return _mock_client

    return GlobusSearchClient(index_id=index_id, test_mode=test_mode)
