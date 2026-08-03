"""Globus Search client for MDF v2.

Provides search ingest and query capabilities via Globus Search indexes.
Falls back to MockGlobusSearchClient when credentials or indexes are not configured.
"""

import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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

DEFAULT_FACETS = [
    {"name": "Year",         "field_name": "dc.year",          "type": "terms", "size": 20},
    {"name": "Organization", "field_name": "mdf.organization", "type": "terms", "size": 20},
    {"name": "Authors",      "field_name": "dc.creators.name", "type": "terms", "size": 20},
    {"name": "Keywords",     "field_name": "dc.subjects",      "type": "terms", "size": 20},
    {"name": "Domains",      "field_name": "mdf.domains",      "type": "terms", "size": 20},
]


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
        """Build a GMetaEntry from a submission record."""
        from v2.metadata import parse_metadata

        source_id = submission.get("source_id", "unknown")
        version = submission.get("version", "1.0")
        meta = parse_metadata(submission)

        subject = f"{_detail_base()}/{source_id}"

        acl = meta.acl or ["public"]
        visible_to = ["public"] if "public" in acl else [f"urn:globus:auth:identity:{a}" for a in acl]

        # Extract data location from first data_source
        data_sources = meta.data_sources or []
        location = data_sources[0] if data_sources else None

        # source_name is the v1 dataset-family grouping/facet key. Preserve the
        # original v1 name when the record carries one (migrated datasets keep it
        # in extensions.mdf_source_name); otherwise use the full, stable
        # source_id. Never derive it with rsplit("-", 1): for v2-native
        # "mdf-<uuid>" ids that collapsed every dataset to source_name="mdf".
        source_name = (meta.extensions or {}).get("mdf_source_name") or source_id

        mdf_block: Dict[str, Any] = {
            "source_id": source_id,
            "source_name": source_name,
            # v1 parity: the v1 enumeration/extraction queries filter on
            # mdf.resource_type:"dataset". Without it, v2-native datasets are
            # invisible to the v1 sync/migration tooling (zero rows on re-sync).
            "resource_type": "dataset",
            "version": version,
            "organization": submission.get("organization", ""),
            "acl": acl,
            "ingest_date": submission.get("created_at", datetime.now(timezone.utc).isoformat()),
        }

        mdf_block["domains"] = meta.domains

        if meta.external:
            mdf_block["external_source"] = meta.external.source
            if meta.external.doi:
                mdf_block["external_doi"] = meta.external.doi
            if meta.external.url:
                mdf_block["external_url"] = meta.external.url

        dataset_doi = submission.get("dataset_doi")
        if dataset_doi:
            mdf_block["dataset_doi"] = dataset_doi
        if version_count is not None:
            mdf_block["version_count"] = version_count

        # Versioning fields
        mdf_block["latest"] = meta.latest
        if meta.root_version:
            mdf_block["root_version"] = meta.root_version
        if meta.previous_version:
            mdf_block["previous_version"] = meta.previous_version
        if meta.version:
            mdf_block["version"] = meta.version

        # Download URL
        if meta.download_url:
            mdf_block["download_url"] = meta.download_url

        content = {
            "mdf": mdf_block,
            "dc": {
                "title": meta.title,
                "creators": [{"name": a.name} for a in meta.authors],
                "publisher": meta.publisher,
                "year": meta.publication_year or datetime.now().year,
                "description": meta.description or "",
                "subjects": meta.keywords,
                "license": meta.license.identifier or meta.license.name if meta.license else "",
            },
            "data": {
                "location": location,
                "size_bytes": submission.get("total_bytes"),
                "file_count": submission.get("file_count"),
            },
        }

        # dc.doi = version-specific DOI if present, otherwise dataset DOI
        doi = submission.get("doi") or submission.get("dataset_doi")
        if doi:
            content["dc"]["doi"] = doi

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

        filters: dict mapping facet field_name → list of selected values
                 e.g. {"mdf.organization": ["MDF Open"], "dc.year": [2024, 2025]}
        sort:    Globus Search sort clause, e.g.
                 [{"field_name": "mdf.ingest_date", "order": "desc"}].
                 None leaves the engine's own relevance ordering in place.

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
        matches = []
        for subject, entry in self._entries.items():
            content = entry.get("content", {})
            text = " ".join([
                content.get("dc", {}).get("title", ""),
                content.get("dc", {}).get("description", ""),
                " ".join(content.get("dc", {}).get("subjects", [])),
                content.get("mdf", {}).get("source_id", ""),
            ]).lower()
            if query_lower in text:
                matches.append(entry)

        paginated = matches[offset:offset + limit]
        results = [self._format_entry(entry) for entry in paginated]

        return {"success": True, "total": len(matches), "results": results, "mock": True}

    @staticmethod
    def _format_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
        """Format a stored entry the way the real client formats a Globus hit.

        Shared by search() and faceted_search() so the mock cannot drift into
        returning a shape the real backend never produces.
        """
        content = entry.get("content", {})
        dc = content.get("dc", {})
        mdf = content.get("mdf", {})
        data_block = content.get("data", {})
        description = dc.get("description", "") or ""
        return {
            "type": "dataset",
            "source_id": mdf.get("source_id"),
            "version": mdf.get("version"),
            "title": dc.get("title"),
            "authors": [c.get("name", "") for c in dc.get("creators", [])],
            "keywords": dc.get("subjects", []),
            "description": description[:300] if len(description) > 300 else description,
            "publication_year": dc.get("year"),
            "organization": mdf.get("organization"),
            "domains": mdf.get("domains") or [],
            "doi": dc.get("doi") or mdf.get("dataset_doi"),
            "license": dc.get("license") or None,
            "size_bytes": data_block.get("size_bytes"),
            "file_count": data_block.get("file_count"),
            "status": "published",
            "score": 1.0,
            "latest": mdf.get("latest", True),
        }

    def faceted_search(
        self, query: str, limit: int = 20, offset: int = 0, filters: Optional[Dict[str, List]] = None,
        sort: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Faceted search over in-memory entries with filter and sort support."""
        query_lower = query.lower()
        matches = []
        for subject, entry in self._entries.items():
            content = entry.get("content", {})
            text = " ".join([
                content.get("dc", {}).get("title", ""),
                content.get("dc", {}).get("description", ""),
                " ".join(content.get("dc", {}).get("subjects", [])),
                content.get("mdf", {}).get("source_id", ""),
            ]).lower()
            if query_lower == "*" or query_lower in text:
                if filters and not self._matches_filters(content, filters):
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
            field = clause.get("field_name") or ""
            descending = clause.get("order", "asc") == "desc"

            def key(entry: Dict[str, Any], field=field) -> str:
                value: Any = entry.get("content", {})
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
        field_map = {
            "dc.year": lambda c: [c.get("dc", {}).get("year")],
            "mdf.organization": lambda c: [c.get("mdf", {}).get("organization")],
            "dc.creators.name": lambda c: [cr.get("name", "") for cr in c.get("dc", {}).get("creators", [])],
            "dc.subjects": lambda c: c.get("dc", {}).get("subjects", []),
            "mdf.domains": lambda c: c.get("mdf", {}).get("domains", []),
        }
        for field_name, values in filters.items():
            extractor = field_map.get(field_name)
            if not extractor:
                continue
            entry_values = [str(v) for v in extractor(content) if v is not None]
            filter_values = [str(v) for v in values]
            if not any(ev in filter_values for ev in entry_values):
                return False
        return True

    def _compute_facets(self, entries: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Compute facet counts from a list of matched entries."""
        counters: Dict[str, Counter] = {
            "Year": Counter(),
            "Organization": Counter(),
            "Authors": Counter(),
            "Keywords": Counter(),
            "Domains": Counter(),
        }
        for entry in entries:
            content = entry.get("content", {})
            dc = content.get("dc", {})
            mdf = content.get("mdf", {})

            year = dc.get("year")
            if year is not None:
                counters["Year"][str(year)] += 1
            org = mdf.get("organization")
            if org:
                counters["Organization"][org] += 1
            for creator in dc.get("creators", []):
                name = creator.get("name")
                if name:
                    counters["Authors"][name] += 1
            for kw in dc.get("subjects", []):
                if kw:
                    counters["Keywords"][kw] += 1
            for domain in mdf.get("domains", []):
                if domain:
                    counters["Domains"][domain] += 1

        facets = {}
        for name, counter in counters.items():
            buckets = [{"value": val, "count": count} for val, count in counter.most_common(20)]
            facets[name] = buckets
        return facets


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
            mdf = content.get("mdf", {})
            dc = content.get("dc", {})
            data_block = content.get("data", {})
            description = dc.get("description", "") or ""
            result_entry = {
                "type": "dataset",
                "source_id": mdf.get("source_id"),
                "version": mdf.get("version"),
                "title": dc.get("title"),
                "authors": [c.get("name", "") for c in dc.get("creators", [])],
                "keywords": dc.get("subjects", []),
                "description": description[:300] if len(description) > 300 else description,
                "publication_year": dc.get("year"),
                "organization": mdf.get("organization"),
                "domains": mdf.get("domains") or [],
                "doi": dc.get("doi") or mdf.get("dataset_doi"),
                "license": dc.get("license") or None,
                "size_bytes": data_block.get("size_bytes"),
                "file_count": data_block.get("file_count"),
                "status": "published",
                "score": gmeta.get("score", 0),
                "latest": mdf.get("latest", True),
            }
            if mdf.get("root_version"):
                result_entry["root_version"] = mdf["root_version"]
            if mdf.get("download_url"):
                result_entry["download_url"] = mdf["download_url"]
            results.append(result_entry)
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
        facets[name] = buckets
    return facets


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
