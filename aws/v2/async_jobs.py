import contextvars
import json
import logging
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from v2.config import AWS_REGION

logger = logging.getLogger(__name__)

JOB_PROFILE_SUBMISSION = "profile_submission"
JOB_MINT_STREAM_DOI = "mint_stream_doi"
JOB_MINT_SUBMISSION_DOI = "mint_submission_doi"
JOB_PUBLISH_SUBMISSION = "publish_submission"
JOB_TRANSFER_DATA = "transfer_data"
JOB_CLEANUP_TRANSFERS = "cleanup_transfers"
JOB_GENERATE_EMBEDDING = "generate_embedding"
JOB_BUILD_EMBEDDING_SNAPSHOT = "build_embedding_snapshot"
JOB_DISPATCH_EMBEDDING_REBUILD = "dispatch_embedding_rebuild"

# Statuses a publish job accepts. "approved" is the state routes move a record
# into before dispatching (curation approve, metadata edit, status update);
# "published" is allowed so a redelivered message is a safe no-op re-run.
PUBLISHABLE_STATUSES = frozenset({"approved", "published"})


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sqlite_path() -> str:
    return os.environ.get("ASYNC_SQLITE_PATH", os.environ.get("SQLITE_PATH", "/tmp/mdf_connect_v2.db"))


class JobExecutionError(RuntimeError):
    """A job could not complete and must be retried.

    Raised out of ``process_job`` so that:
    - the SQS event source (ReportBatchItemFailures) records a batch item
      failure and redelivers the message, eventually landing it in the DLQ;
    - ``run_sqlite_worker_once`` marks the job failed;
    - ``InlineJobDispatcher`` surfaces an explicit API error instead of
      pretending the job succeeded.

    Jobs that raise this must be safe to re-run from the start.
    """

    def __init__(self, message: str, result: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.result = result or {}


class JobDispatcher:
    def dispatch(self, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class InlineJobDispatcher(JobDispatcher):
    def dispatch(self, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            result = process_job(job_type, payload)
        except Exception as exc:
            # Inline dispatch runs inside the API request, so there is no queue
            # to retry the work. Surface a targeted upstream-failure error
            # instead of letting the generic handler turn it into an opaque 500.
            logger.exception("Inline async job failed job_type=%s", job_type)
            raise _inline_job_error(job_type, exc) from exc
        return {
            "mode": "inline",
            "queued": False,
            "job_type": job_type,
            "result": result,
        }


def _inline_job_error(job_type: str, exc: Exception) -> Exception:
    """Translate an inline job failure into an HTTP-shaped error when possible.

    Inline dispatch is only used from request handlers (and local/test runs), so
    a failed dependency (DataCite, Globus Search, ...) is a 502, not a 500. If
    FastAPI is unavailable (pure worker context) the original exception is kept.
    """
    detail = f"Async job '{job_type}' failed: {exc}"
    try:
        from fastapi import HTTPException
    except Exception:  # pragma: no cover - fastapi is always present in the API
        return exc
    return HTTPException(status_code=502, detail=detail)


class SQSJobDispatcher(JobDispatcher):
    def __init__(self, queue_url: str):
        import boto3

        self.queue_url = queue_url
        self.client = boto3.client("sqs", region_name=AWS_REGION)

    def dispatch(self, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        message = {
            "job_type": job_type,
            "payload": payload,
            "created_at": _utc_now(),
        }
        resp = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(message),
        )
        return {
            "mode": "sqs",
            "queued": True,
            "job_type": job_type,
            "message_id": resp.get("MessageId"),
        }


class SqliteJobDispatcher(JobDispatcher):
    def __init__(self, db_path: Optional[str] = None):
        self.path = db_path or _sqlite_path()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            with conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS async_jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        job_type TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        result TEXT,
                        error TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_async_jobs_status ON async_jobs(status, id)"
                )
        finally:
            conn.close()

    def dispatch(self, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = _utc_now()
        conn = self._connect()
        try:
            with conn:
                cur = conn.execute(
                    """
                    INSERT INTO async_jobs (job_type, payload, status, attempts, created_at, updated_at)
                    VALUES (?, ?, 'pending', 0, ?, ?)
                    """,
                    (job_type, json.dumps(payload), now, now),
                )
                job_id = int(cur.lastrowid)
            return {
                "mode": "sqlite",
                "queued": True,
                "job_type": job_type,
                "job_id": job_id,
            }
        finally:
            conn.close()

    def claim_pending_jobs(self, limit: int = 20) -> List[Dict[str, Any]]:
        conn = self._connect()
        jobs: List[Dict[str, Any]] = []
        try:
            with conn:
                rows = conn.execute(
                    """
                    SELECT * FROM async_jobs
                    WHERE status = 'pending'
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
                for row in rows:
                    conn.execute(
                        """
                        UPDATE async_jobs
                        SET status = 'processing',
                            attempts = attempts + 1,
                            updated_at = ?
                        WHERE id = ? AND status = 'pending'
                        """,
                        (_utc_now(), row["id"]),
                    )
                    jobs.append(dict(row))
            return jobs
        finally:
            conn.close()

    def mark_completed(self, job_id: int, result: Dict[str, Any]) -> None:
        conn = self._connect()
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE async_jobs
                    SET status = 'completed',
                        result = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (json.dumps(result), _utc_now(), job_id),
                )
        finally:
            conn.close()

    def mark_failed(self, job_id: int, error: str) -> None:
        conn = self._connect()
        try:
            with conn:
                conn.execute(
                    """
                    UPDATE async_jobs
                    SET status = 'failed',
                        error = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (error, _utc_now(), job_id),
                )
        finally:
            conn.close()


def get_job_dispatcher() -> JobDispatcher:
    mode = os.environ.get("ASYNC_DISPATCH_MODE", "inline").lower()
    if mode == "sqs":
        queue_url = os.environ.get("ASYNC_QUEUE_URL")
        if not queue_url:
            raise ValueError("ASYNC_QUEUE_URL is required for ASYNC_DISPATCH_MODE=sqs")
        return SQSJobDispatcher(queue_url=queue_url)
    if mode == "sqlite":
        return SqliteJobDispatcher()
    return InlineJobDispatcher()


def enqueue_profile_job(source_id: str, version: str, stream_id: str) -> Dict[str, Any]:
    payload = {"source_id": source_id, "version": version, "stream_id": stream_id}
    return get_job_dispatcher().dispatch(JOB_PROFILE_SUBMISSION, payload)


def enqueue_stream_doi_job(stream_id: str, overrides: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"stream_id": stream_id, "overrides": overrides}
    return get_job_dispatcher().dispatch(JOB_MINT_STREAM_DOI, payload)


def enqueue_submission_doi_job(source_id: str, version: str) -> Dict[str, Any]:
    payload = {"source_id": source_id, "version": version}
    return get_job_dispatcher().dispatch(JOB_MINT_SUBMISSION_DOI, payload)


def enqueue_publish_job(source_id: str, version: str, mint_doi: bool = True) -> Dict[str, Any]:
    payload = {"source_id": source_id, "version": version, "mint_doi": mint_doi}
    return get_job_dispatcher().dispatch(JOB_PUBLISH_SUBMISSION, payload)


def dispatch_publish_job(source_id: str, version: str, mint_doi: bool = True) -> Dict[str, Any]:
    """Dispatch a publish job on behalf of an API request.

    Routes must never return success for a publish that did not happen. Inline
    dispatch already raises ``HTTPException(502)`` when the job fails; a queue
    dispatch failure (SQS/sqlite unavailable) is translated to the same error so
    both modes behave alike from the caller's point of view.
    """
    try:
        from fastapi import HTTPException
    except Exception:  # pragma: no cover - fastapi is always present in the API
        HTTPException = ()  # type: ignore[assignment]

    try:
        return enqueue_publish_job(source_id, version, mint_doi=mint_doi)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to dispatch publish job for %s v%s", source_id, version)
        raise _inline_job_error(JOB_PUBLISH_SUBMISSION, exc) from exc


def enqueue_transfer_job(
    source_id: str,
    version: str,
    data_sources: List[str],
    user_transfer_token: str,
    user_identity_id: str,
) -> Dict[str, Any]:
    payload = {
        "source_id": source_id,
        "version": version,
        "data_sources": data_sources,
        "user_transfer_token": user_transfer_token,
        "user_identity_id": user_identity_id,
    }
    # User bearer tokens must not be persisted into SQS or SQLite job payloads.
    # Transfer initiation runs inline so the token only lives in request memory.
    return InlineJobDispatcher().dispatch(JOB_TRANSFER_DATA, payload)


def enqueue_cleanup_transfers_job() -> Dict[str, Any]:
    return get_job_dispatcher().dispatch(JOB_CLEANUP_TRANSFERS, {})


def enqueue_embedding_job(source_id: str, version: str) -> Dict[str, Any]:
    payload = {"source_id": source_id, "version": version}
    return get_job_dispatcher().dispatch(JOB_GENERATE_EMBEDDING, payload)


def enqueue_snapshot_build_job() -> Dict[str, Any]:
    return get_job_dispatcher().dispatch(JOB_BUILD_EMBEDDING_SNAPSHOT, {})


def enqueue_rebuild_dispatch_job(
    force: bool = False, limit: Optional[int] = None, build_snapshot: bool = True,
) -> Dict[str, Any]:
    """Enqueue a single meta-job that will scan submissions and fan out embedding jobs.

    Keeps the admin rebuild endpoint inside the 30s API Gateway window — the
    scan + SQS fan-out happens inside the async worker instead.
    """
    payload: Dict[str, Any] = {"force": force, "build_snapshot": build_snapshot}
    if limit is not None:
        payload["limit"] = limit
    return get_job_dispatcher().dispatch(JOB_DISPATCH_EMBEDDING_REBUILD, payload)


def process_job(job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if job_type == JOB_PROFILE_SUBMISSION:
        return _process_profile_submission(payload)
    if job_type == JOB_MINT_STREAM_DOI:
        return _process_mint_stream_doi(payload)
    if job_type == JOB_MINT_SUBMISSION_DOI:
        return _process_mint_submission_doi(payload)
    if job_type == JOB_PUBLISH_SUBMISSION:
        return _process_publish_submission(payload)
    if job_type == JOB_TRANSFER_DATA:
        return _process_transfer_data(payload)
    if job_type == JOB_CLEANUP_TRANSFERS:
        return _process_cleanup_transfers(payload)
    if job_type == JOB_GENERATE_EMBEDDING:
        return _process_generate_embedding(payload)
    if job_type == JOB_BUILD_EMBEDDING_SNAPSHOT:
        return _process_build_embedding_snapshot(payload)
    if job_type == JOB_DISPATCH_EMBEDDING_REBUILD:
        return _process_dispatch_embedding_rebuild(payload)
    raise ValueError(f"Unknown job type: {job_type}")


def _process_profile_submission(payload: Dict[str, Any]) -> Dict[str, Any]:
    from v2.profiler import build_dataset_profile
    from v2.storage import get_storage_backend
    from v2.store import get_store

    source_id = payload["source_id"]
    version = payload["version"]
    stream_id = payload["stream_id"]

    storage = get_storage_backend()
    store = get_store()
    profile = build_dataset_profile(stream_id, storage)
    store.update_profile(source_id, version, profile.model_dump_json())
    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "stream_id": stream_id,
        "total_files": profile.total_files,
        "total_bytes": profile.total_bytes,
    }


def _process_mint_stream_doi(payload: Dict[str, Any]) -> Dict[str, Any]:
    from v2.doi_utils import mint_doi_for_stream
    from v2.stream_store import get_stream_store

    stream_id = payload["stream_id"]
    overrides = payload.get("overrides") or {}

    stream_store = get_stream_store()
    stream = stream_store.get_stream(stream_id)
    if not stream:
        return {"success": False, "error": f"Stream not found: {stream_id}"}

    doi_result = mint_doi_for_stream(stream, overrides)
    if doi_result.get("success"):
        stream_store.update_stream_metadata(
            stream_id,
            {
                "doi": doi_result.get("doi"),
                "published_at": _utc_now(),
            },
        )
    return doi_result


def _process_mint_submission_doi(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy job: mint a DOI for a submission and publish it.

    Unreachable from the v2 API — nothing calls ``enqueue_submission_doi_job``;
    curation approve dispatches JOB_PUBLISH_SUBMISSION instead. Kept only so
    in-flight/queued messages of this type still process correctly.

    It used to mark the submission "published" on DOI success alone, which
    bypassed the search ingest and produced published-but-unindexed datasets.
    It now delegates to the publish job so the "published implies indexed"
    invariant holds no matter which message type shows up. Do not re-wire this
    job; use ``enqueue_publish_job``.
    """
    return _process_publish_submission({
        "source_id": payload["source_id"],
        "version": payload["version"],
        "mint_doi": True,
    })


def _process_transfer_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Initiate Globus transfers for data sources on external endpoints."""
    from v2.store import get_store
    from v2.transfer import extract_transfer_sources, initiate_transfer

    source_id = payload["source_id"]
    version = payload["version"]
    data_sources = payload["data_sources"]
    user_transfer_token = payload["user_transfer_token"]
    user_identity_id = payload["user_identity_id"]

    store = get_store()
    submission = store.get_submission(source_id, version)
    if not submission:
        return {"success": False, "error": f"Submission not found: {source_id} v{version}"}

    transfer_sources = extract_transfer_sources(data_sources)
    if not transfer_sources:
        return {"success": True, "message": "No external transfers needed"}

    results = []
    for src in transfer_sources:
        try:
            result = initiate_transfer(
                source_endpoint=src["source_endpoint"],
                source_path=src["source_path"],
                source_id=source_id,
                version=version,
                user_transfer_token=user_transfer_token,
                user_identity_id=user_identity_id,
            )
            results.append(result)
        except Exception as exc:
            logger.exception("Transfer initiation failed for %s", src["uri"])
            results.append({"error": str(exc), "uri": src["uri"]})

    # Store transfer state in the submission record
    successful = [r for r in results if "task_id" in r]
    if successful:
        submission["transfer_task_ids"] = [r["task_id"] for r in successful]
        submission["transfer_acl_rule_ids"] = [r.get("acl_rule_id") for r in successful if r.get("acl_rule_id")]
        submission["transfer_status"] = "active"
        submission["transfer_destination"] = successful[0].get("destination_path", "")
        submission["transfer_initiated_at"] = successful[0].get("initiated_at", _utc_now())
        submission["updated_at"] = _utc_now()
        store.upsert_submission(submission)

    return {
        "success": len(successful) > 0,
        "source_id": source_id,
        "version": version,
        "transfers_initiated": len(successful),
        "transfers_failed": len(results) - len(successful),
        "results": results,
    }


def _process_cleanup_transfers(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Scan for submissions with active transfers and clean up completed/stale ones."""
    from v2.store import get_store
    from v2.transfer import cleanup_stale_transfers

    store = get_store()

    # Scan for all submissions with active transfers.
    # DynamoDB scan is fine here — runs 4x/day, table is small.
    active_submissions = store.scan_by_transfer_status("active")
    if not active_submissions:
        return {"success": True, "checked": 0, "cleaned": 0}

    modified = cleanup_stale_transfers(active_submissions)

    # Persist any modified submissions
    cleaned = 0
    for sub in modified:
        sub["updated_at"] = _utc_now()
        store.upsert_submission(sub)
        if sub.get("transfer_status") != "active":
            cleaned += 1

    return {
        "success": True,
        "checked": len(active_submissions),
        "cleaned": cleaned,
    }


def _version_sort_key(version: Optional[str]) -> list:
    """Numeric-aware ordering key for a version string ("10.0" > "2.0")."""
    parts = []
    for part in str(version or "0").split("."):
        parts.append((0, int(part), "") if part.isdigit() else (1, 0, part))
    return parts


def _owns_search_entry(version: str, all_versions: list) -> bool:
    """True when `version` is the newest published version of the dataset.

    The Globus Search index holds exactly ONE entry per dataset, keyed on the
    version-less subject ``{MDF_DETAIL_BASE}/{source_id}``, and that entry
    always represents the LATEST published version. So a publish of an older
    version — an out-of-order curation approval, a re-run for a superseded
    version, or a redelivered queue message — must not overwrite the subject
    with stale content.
    """
    current_key = _version_sort_key(version)
    for record in all_versions or []:
        other = record.get("version")
        if not other or other == version:
            continue
        if record.get("status") != "published":
            continue
        if _version_sort_key(other) > current_key:
            return False
    return True


def _mark_prior_versions_not_latest(
    store, source_id: str, current_version: str, all_versions: list,
) -> None:
    """Ensure prior published versions carry ``latest=false`` in the store.

    Deliberately does NOT touch Globus Search. There is one search entry per
    dataset (subject = the version-less detail URL) and it always describes the
    latest published version, so re-ingesting a prior version here would
    overwrite the entry just written for the version being published — leaving
    the index advertising stale metadata with ``latest=false`` (bug B-2).

    The submit path (`_flip_latest_on_prior`) normally does this already; this is
    a defensive backstop for records created out of band (migration, reconcile).
    """
    for v_record in all_versions or []:
        v = v_record.get("version")
        if not v or v == current_version:
            continue
        if v_record.get("status") != "published":
            continue
        mdata = v_record.get("dataset_mdata")
        if isinstance(mdata, str):
            try:
                mdata = json.loads(mdata)
            except Exception:
                continue
        if isinstance(mdata, dict) and mdata.get("latest") is not False:
            mdata["latest"] = False
            v_record["dataset_mdata"] = json.dumps(mdata)
            v_record["updated_at"] = _utc_now()
            store.upsert_submission(v_record)
            logger.info("Marked prior version %s v%s latest=false in the store", source_id, v)


def _publish_doi_step(
    store,
    submission: Dict[str, Any],
    source_id: str,
    version: str,
    all_versions: list,
    mint_doi: bool,
) -> Dict[str, Any]:
    """DOI handling for a publish job, safe under at-least-once delivery.

    Idempotency (bug B-7b): a DOI is an irreversible external side effect, so
    the job must never mint twice for the same version.

    - If the record already carries ``doi``, that DOI was minted by an earlier
      attempt (or by migration) and no new one is created. Without this guard a
      redelivered first-version job would see the ``dataset_doi`` written by the
      first attempt, conclude that a *prior* version exists, and mint a bogus
      ``-v{version}`` duplicate.
    - Only *other* versions are considered when resolving the dataset (concept)
      DOI, so a partially-completed attempt on this version cannot change which
      branch of the versioning logic runs.
    - The minted DOI is persisted immediately, before the search step, so it
      survives a later failure in this job and is visible to the retry.

    Never raises.
    """
    from v2.curation import _mint_doi_for_submission

    existing_doi = submission.get("doi")
    if existing_doi:
        logger.info(
            "Publish job for %s v%s: DOI %s already recorded, skipping mint",
            source_id, version, existing_doi,
        )
        return {
            "success": True,
            "doi": existing_doi,
            "dataset_doi": submission.get("dataset_doi") or existing_doi,
            "already_minted": True,
        }

    prior_versions = [v for v in (all_versions or []) if v.get("version") != version]

    try:
        doi_result = _mint_doi_for_submission(
            submission, all_versions=prior_versions, mint_doi=mint_doi,
        )
    except Exception:
        logger.exception("DOI handling error for %s", source_id)
        return {"success": False, "error": "DOI handling exception"}

    if not doi_result.get("success"):
        logger.warning("DOI handling failed for %s: %s", source_id, doi_result.get("error"))
        return doi_result

    changed = False
    if doi_result.get("doi") and submission.get("doi") != doi_result["doi"]:
        submission["doi"] = doi_result["doi"]
        changed = True
    if doi_result.get("dataset_doi") and submission.get("dataset_doi") != doi_result["dataset_doi"]:
        submission["dataset_doi"] = doi_result["dataset_doi"]
        changed = True
    if changed:
        submission["updated_at"] = _utc_now()
        try:
            store.upsert_submission(submission)
        except Exception:
            logger.exception("Failed to persist minted DOI for %s v%s", source_id, version)

    return doi_result


def _record_publish_failure(store, submission: Dict[str, Any], error: str) -> None:
    """Note a failed publish attempt without advancing the submission state.

    The submission keeps its prior status (typically "approved") so the retried
    job can complete the transition. ``curation_history`` is the record's
    existing audit trail and is persisted by both store backends.
    """
    now = _utc_now()
    history = submission.get("curation_history") or []
    if isinstance(history, str):
        try:
            history = json.loads(history)
        except Exception:
            history = []
    if not isinstance(history, list):
        history = []
    history.append({
        "action": "publish_failed",
        "timestamp": now,
        "error": str(error)[:500],
    })
    submission["curation_history"] = history
    # publish_error is persisted by the DynamoDB backend; the sqlite backend has
    # a fixed column set and keeps only the curation_history entry.
    submission["publish_error"] = str(error)[:500]
    submission["publish_error_at"] = now
    submission["updated_at"] = now
    try:
        store.upsert_submission(submission)
    except Exception:
        logger.exception(
            "Failed to record publish failure for %s v%s",
            submission.get("source_id"), submission.get("version"),
        )


# Ingest-confirmation budget for a publish job. Sized against the worker's
# limits: Lambda timeout 120s, SQS visibility timeout 180s, batch size up to 10.
# Ingest tasks normally settle in ~1s, so a 30s ceiling confirms the common case
# without risking the invocation, and a timeout is retried rather than lost.
DEFAULT_PUBLISH_SEARCH_WAIT_SECONDS = 30.0

# A publish must never run the search client in accept-only mode: a zero budget
# means "don't confirm", and an unconfirmed ingest is exactly the B-17 bug. The
# budget is clamped to this floor no matter what the config or the remaining
# Lambda time says.
MIN_PUBLISH_SEARCH_WAIT_SECONDS = 5.0

# Wall-clock a publish job needs on top of the search wait: DOI mint/update,
# store writes, prior-version flag normalization, the submitter email.
PUBLISH_OVERHEAD_SECONDS = 15.0

# Extra headroom kept back so an invocation returns its batchItemFailures
# instead of being killed mid-record.
BATCH_SAFETY_MARGIN_SECONDS = 5.0

# Remaining-time budget for the current record, narrowed by the batch loop from
# the Lambda context. A ContextVar (not a global) so the threaded tests and any
# future concurrent worker cannot bleed budgets into each other.
_record_time_budget: "contextvars.ContextVar[Optional[float]]" = contextvars.ContextVar(
    "publish_record_time_budget", default=None,
)


@contextmanager
def record_time_budget(seconds: Optional[float]):
    """Scope the wall-clock a single job may spend, for the duration of the block."""
    token = _record_time_budget.set(seconds)
    try:
        yield
    finally:
        _record_time_budget.reset(token)


def _configured_publish_search_wait_seconds() -> float:
    raw = os.environ.get("PUBLISH_SEARCH_WAIT_SECONDS")
    if raw is None:
        return DEFAULT_PUBLISH_SEARCH_WAIT_SECONDS
    try:
        return max(0.0, float(raw))
    except ValueError:
        logger.warning("Invalid PUBLISH_SEARCH_WAIT_SECONDS=%r, using default", raw)
        return DEFAULT_PUBLISH_SEARCH_WAIT_SECONDS


def _publish_record_worst_case_seconds() -> float:
    """Upper bound on one publish job's wall-clock, used to plan a batch."""
    return _configured_publish_search_wait_seconds() + PUBLISH_OVERHEAD_SECONDS


def _publish_search_wait_seconds() -> float:
    """Ingest-confirmation budget for this publish job.

    The configured ceiling, narrowed by whatever wall-clock the batch loop says
    is left for this record, and floored at ``MIN_PUBLISH_SEARCH_WAIT_SECONDS``
    so the publish path can never degrade into unconfirmed accept-only mode.
    """
    budget = _configured_publish_search_wait_seconds()
    remaining = _record_time_budget.get()
    if remaining is not None:
        budget = min(budget, remaining - PUBLISH_OVERHEAD_SECONDS)
    return max(MIN_PUBLISH_SEARCH_WAIT_SECONDS, budget)


def _datacite_is_mocked() -> bool:
    """True when DOIs are deliberately fake (USE_MOCK_DATACITE=true).

    In that mode a DOI is not a real, citable identifier, so a DOI failure is
    not a reason to block a publish — see ``_process_publish_submission``.
    """
    return os.environ.get("USE_MOCK_DATACITE", "").strip().lower() == "true"


def _process_publish_submission(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Publish a submission: handle DOI, ingest into search, then mark published.

    Ordering matters: status only advances to "published" once the dataset has a
    DOI *and* is actually discoverable. If either precondition fails the job
    raises ``JobExecutionError`` so the queue retries it (and eventually DLQs);
    every step is idempotent, so the retry is safe.

    The DOI + search-write + status-flip sequence runs under a per-dataset lock
    (B-16) because it is read-then-write: it reads every version to decide
    whether this version owns the dataset's single search entry. Two publish
    jobs for different versions of the same dataset could otherwise interleave
    and leave the index describing the older one. All state the decision depends
    on is re-read *inside* the lock.
    """
    from v2.store import PublishLockUnavailable, get_store, publish_lock

    source_id = payload["source_id"]
    version = payload["version"]
    mint_doi = payload.get("mint_doi", True)

    store = get_store()
    submission = store.get_submission(source_id, version)
    if not submission:
        return {"success": False, "error": f"Submission not found: {source_id} v{version}"}

    # Cheap pre-check outside the lock; re-validated inside it.
    skipped = _publish_skip_result(submission, source_id, version)
    if skipped:
        return skipped

    try:
        with publish_lock(store, source_id, owner=f"publish:{version}:{uuid.uuid4()}"):
            return _publish_under_lock(store, source_id, version, mint_doi)
    except PublishLockUnavailable as exc:
        # A concurrent publish for this dataset is in flight. Fail the job so the
        # queue redelivers it after the visibility timeout — by then the other
        # publish has finished and this one sees its writes.
        logger.info("Publish job for %s v%s deferred: %s", source_id, version, exc)
        raise JobExecutionError(
            f"Publish deferred for {source_id} v{version}: {exc}",
            result={"source_id": source_id, "version": version, "lock_contended": True},
        ) from exc


def _publish_skip_result(
    submission: Dict[str, Any], source_id: str, version: str,
) -> Optional[Dict[str, Any]]:
    """Skip payload when the record is no longer in a publishable state.

    Only records that a route deliberately moved into a pre-publish state may be
    published. A queue message can outlive the state it was created for
    (withdrawn, rejected, reverted to pending_curation), and an at-least-once
    delivery must not resurrect such a record. Skipping is reported as success
    so the message is consumed rather than retried forever against a tombstone.
    """
    status = submission.get("status")
    if status in PUBLISHABLE_STATUSES:
        return None

    logger.info(
        "Publish job for %s v%s skipped: status is %r, expected one of %s "
        "(stale queue message or the record changed state after enqueue)",
        source_id, version, status, sorted(PUBLISHABLE_STATUSES),
    )
    return {
        "success": True,
        "skipped": True,
        "source_id": source_id,
        "version": version,
        "status": status,
        "reason": f"submission status '{status}' is not publishable",
    }


def _publish_under_lock(store, source_id: str, version: str, mint_doi: bool) -> Dict[str, Any]:
    """The publish critical section, run while holding the dataset's lock."""
    from v2.search_client import get_search_client

    # Re-read under the lock: a publish that just completed for another version
    # may have changed this record's status and the version set the
    # search-ownership decision is made from.
    submission = store.get_submission(source_id, version)
    if not submission:
        return {"success": False, "error": f"Submission not found: {source_id} v{version}"}

    skipped = _publish_skip_result(submission, source_id, version)
    if skipped:
        return skipped

    status = submission.get("status")

    # Look up all versions for DOI versioning context
    all_versions = store.list_versions(source_id)
    was_published = status == "published"

    result: Dict[str, Any] = {"source_id": source_id, "version": version}

    # Step 1: DOI handling (mint new or update existing dataset DOI).
    # Handles both mint_doi=True (mint) and mint_doi=False (metadata update only).
    doi_result = _publish_doi_step(
        store, submission, source_id, version, all_versions, mint_doi,
    )
    result["doi"] = doi_result

    # A published dataset without its DOI is not publishable output: the DOI is
    # what makes it citable, and a warning-only failure left records permanently
    # DOI-less with nothing to retry (bug B-19). Minting is check-before-mint
    # idempotent, so failing the job is retry-safe. Note this gates on the DOI
    # *step* succeeding, not on a DOI existing: mint_doi=false with no prior
    # dataset DOI legitimately publishes with no DOI at all (metadata-edit
    # republish, curator approval with minting declined).
    if not doi_result.get("success"):
        error = doi_result.get("error") or "DOI handling failed"
        if _datacite_is_mocked():
            # Mock/disabled DataCite: the DOI would be fake anyway, so a failure
            # here must not block local dev or staging publishes.
            logger.warning(
                "DOI step failed for %s v%s but DataCite is mocked; publishing anyway: %s",
                source_id, version, error,
            )
            result["doi_failure_ignored"] = True
        else:
            logger.warning("DOI step failed for %s v%s: %s", source_id, version, error)
            _record_publish_failure(store, submission, error)
            result["success"] = False
            result["status"] = submission.get("status")
            result["error"] = error
            raise JobExecutionError(
                f"Publish failed for {source_id} v{version}: DOI step failed: {error}",
                result=result,
            )

    # Step 2: Ingest into Globus Search — one entry per dataset, always the
    # latest published version.
    owns_entry = _owns_search_entry(version, all_versions)
    if owns_entry:
        # Always a positive budget: a publish may never run the client in
        # accept-only mode, and the result must be *confirmed*, not merely
        # accepted (see the confirmation check below).
        search_wait = _publish_search_wait_seconds()
        assert search_wait >= MIN_PUBLISH_SEARCH_WAIT_SECONDS
        try:
            search_client = get_search_client()
            search_result = search_client.ingest(
                submission,
                version_count=len(all_versions),
                wait_seconds=search_wait,
            )
        except Exception as exc:
            logger.exception("Search ingest error for %s", source_id)
            search_result = {"success": False, "error": f"Search ingest exception: {exc}"}

        # An ingest that reports success without confirmation (no task id, an
        # accept-only client, a transport that cannot poll) has told us the
        # request was accepted and nothing more. Publishing on that is the B-17
        # bug with extra steps, so require both.
        if search_result.get("success") and not search_result.get("confirmed"):
            search_result = dict(search_result)
            search_result["success"] = False
            search_result["error"] = (
                search_result.get("error")
                or "search ingest was accepted but never confirmed as indexed"
            )
    else:
        logger.info(
            "Publish job for %s v%s: a newer published version owns the search entry, "
            "leaving the index untouched",
            source_id, version,
        )
        search_result = {
            "success": True,
            "skipped": True,
            "reason": "a newer published version owns the search entry",
        }
    result["search_ingest"] = search_result

    if not search_result.get("success"):
        error = search_result.get("error") or "search ingest failed"
        logger.warning("Search ingest failed for %s v%s: %s", source_id, version, error)
        _record_publish_failure(store, submission, error)
        result["success"] = False
        result["status"] = submission.get("status")
        result["error"] = error
        # Do NOT mark the submission published: an unindexed dataset is not
        # published. Raising fails the SQS batch item so the job is retried.
        #
        # "Failed" here includes an ingest that was *accepted* but whose task
        # did not reach SUCCESS within the budget (B-17): Globus Search ingest is
        # asynchronous, so acceptance is not indexing. An unconfirmed task is
        # treated as a failure — re-ingesting is an idempotent upsert on the
        # subject, so a retry costs nothing if the task actually did land, while
        # the alternative (assume success) is the silent-unindexed-dataset bug.
        raise JobExecutionError(
            f"Publish failed for {source_id} v{version}: search ingest failed: {error}",
            result=result,
        )

    # Step 3: normalize prior versions' latest flag in the store (never search).
    if owns_entry and len(all_versions) > 1:
        try:
            _mark_prior_versions_not_latest(store, source_id, version, all_versions)
        except Exception:
            logger.warning("Failed to update prior version latest flags for %s", source_id, exc_info=True)

    # Step 4: Update status to published
    now = _utc_now()
    submission["status"] = "published"
    if not submission.get("published_at"):
        submission["published_at"] = now
    submission["updated_at"] = now
    submission.pop("publish_error", None)
    submission.pop("publish_error_at", None)
    store.upsert_submission(submission)

    # Notify submitter their dataset is live — only on the transition into
    # published, so a redelivered job does not re-email them.
    if not was_published:
        try:
            from v2.email_utils import notify_submitter_approved
            notify_submitter_approved(submission)
        except Exception:
            logger.warning("Failed to send approval email for %s", source_id, exc_info=True)

    # Step 5: kick off embedding generation (non-blocking for publish)
    try:
        emb_result = enqueue_embedding_job(source_id, version)
        result["embedding_enqueue"] = emb_result
    except Exception:
        logger.warning("Failed to enqueue embedding job for %s", source_id, exc_info=True)

    result["success"] = True
    result["status"] = "published"
    result["published_at"] = submission["published_at"]
    return result


def _process_generate_embedding(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Embed a dataset's title+description and store the vector on the record."""
    from v2.embeddings import (
        EMBEDDING_MODEL,
        EmbeddingError,
        build_embedding_text,
        embed_text,
    )
    from v2.metadata import parse_metadata
    from v2.store import get_store

    source_id = payload["source_id"]
    version = payload["version"]

    store = get_store()
    submission = store.get_submission(source_id, version)
    if not submission:
        return {"success": False, "error": f"Submission not found: {source_id} v{version}"}

    meta = parse_metadata(submission)
    text = build_embedding_text(meta)
    if not text.strip():
        return {
            "success": False,
            "source_id": source_id,
            "version": version,
            "error": "No title/description text to embed",
        }

    try:
        vector = embed_text(text)
    except EmbeddingError as exc:
        logger.warning("Embedding failed for %s v%s: %s", source_id, version, exc)
        return {"success": False, "source_id": source_id, "version": version, "error": str(exc)}

    store.update_embedding(source_id, version, vector, EMBEDDING_MODEL)
    return {
        "success": True,
        "source_id": source_id,
        "version": version,
        "model": EMBEDDING_MODEL,
        "dims": len(vector),
    }


def _process_build_embedding_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Rebuild the S3 snapshot from whatever embeddings exist in the store.

    Runs async so the admin endpoint can return inside the 30s API Gateway
    window. Scanning all submissions + packing vectors can take a while for
    large corpora.
    """
    from v2.embedding_snapshot import build_snapshot
    from v2.search import invalidate_author_index

    result = build_snapshot()
    invalidate_author_index()
    return result


def _is_embedding_stale_record(record: Dict[str, Any]) -> bool:
    gen_at = record.get("embedding_generated_at") or ""
    mdata_at = record.get("metadata_updated_at") or ""
    if not mdata_at:
        return False
    return gen_at < mdata_at


def _process_dispatch_embedding_rebuild(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Scan all published submissions and fan out one embedding job per pending record.

    Runs inside the async worker (120s budget) so the scan + SQS sends don't
    block the API Gateway request. Queues a final snapshot-build job when
    `build_snapshot` is true.
    """
    from v2.embeddings import EMBEDDING_MODEL
    from v2.store import get_store

    force = bool(payload.get("force"))
    build_snapshot_flag = payload.get("build_snapshot", True)
    limit = payload.get("limit")

    store = get_store()
    all_subs = store.list_all(limit=100000)

    enqueued = 0
    skipped_current = 0
    failed_enqueue: List[Dict[str, Any]] = []

    for sub in all_subs:
        if sub.get("status") != "published":
            continue
        mdata = sub.get("dataset_mdata") or {}
        if isinstance(mdata, dict) and mdata.get("latest") is False:
            continue
        if not force:
            has_vec = bool(sub.get("title_description_embedding"))
            same_model = sub.get("embedding_model") == EMBEDDING_MODEL
            if has_vec and same_model and not _is_embedding_stale_record(sub):
                skipped_current += 1
                continue

        source_id = sub.get("source_id")
        version = sub.get("version")
        if not source_id or not version:
            continue
        try:
            enqueue_embedding_job(source_id, version)
            enqueued += 1
        except Exception as exc:
            failed_enqueue.append({"source_id": source_id, "error": str(exc)})
            logger.exception(
                "Dispatch: failed to enqueue embedding job for %s v%s", source_id, version
            )
        if limit and enqueued >= int(limit):
            break

    snapshot_job: Dict[str, Any] = {"enqueued": False, "skipped": True}
    if build_snapshot_flag:
        try:
            snapshot_job = enqueue_snapshot_build_job()
            snapshot_job["enqueued"] = True
        except Exception as exc:
            snapshot_job = {"enqueued": False, "error": str(exc)}
            logger.exception("Dispatch: failed to enqueue snapshot build job")

    return {
        "success": True,
        "model": EMBEDDING_MODEL,
        "enqueued": enqueued,
        "skipped_current": skipped_current,
        "enqueue_failures": failed_enqueue,
        "snapshot_job": snapshot_job,
    }


def run_sqlite_worker_once(limit: int = 20) -> Dict[str, Any]:
    dispatcher = SqliteJobDispatcher()
    jobs = dispatcher.claim_pending_jobs(limit=limit)
    processed = 0
    failed = 0
    for job in jobs:
        job_id = int(job["id"])
        try:
            payload = json.loads(job["payload"])
            result = process_job(job["job_type"], payload)
            dispatcher.mark_completed(job_id, result)
            processed += 1
        except Exception as exc:
            logger.exception("Async job failed: id=%s", job_id)
            dispatcher.mark_failed(job_id, str(exc))
            failed += 1
    return {
        "success": True,
        "processed": processed,
        "failed": failed,
        "total_claimed": len(jobs),
    }


def _remaining_time_fn(context: Any = None):
    """Return a callable giving the seconds left in this Lambda invocation.

    Prefers the real Lambda context. When the handler does not pass one (the
    current ``async_worker.lambda_handler`` calls ``handle_sqs_event(event)``),
    fall back to the configured function timeout measured from entry — the same
    number the template sets, so the estimate is only wrong if the two drift.
    """
    get_remaining = getattr(context, "get_remaining_time_in_millis", None)
    if callable(get_remaining):
        def remaining() -> float:
            try:
                return float(get_remaining()) / 1000.0
            except Exception:
                return float("inf")
        return remaining

    raw = os.environ.get("ASYNC_WORKER_TIMEOUT_SECONDS")
    if raw is None:
        return lambda: float("inf")
    try:
        budget = float(raw)
    except ValueError:
        return lambda: float("inf")

    started = time.monotonic()
    return lambda: budget - (time.monotonic() - started)


def handle_sqs_event(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """Process an SQS batch, leaving unstarted records to be redelivered.

    Records are handled serially, so a batch of 10 publishes — each of which may
    spend up to ``PUBLISH_SEARCH_WAIT_SECONDS`` confirming its search ingest —
    can outlast the Lambda timeout. A killed invocation returns *no*
    ``batchItemFailures``, so SQS redelivers the whole batch: completed records
    get reprocessed and a record whose publish lock is still held sees
    contention. So before each record we check that the remaining time covers a
    worst-case job; if it does not, the rest of the batch is reported as failed
    (untouched, never started) and redelivered cleanly.
    """
    remaining = _remaining_time_fn(context)
    reserve = _publish_record_worst_case_seconds() + BATCH_SAFETY_MARGIN_SECONDS

    failures: List[Dict[str, str]] = []
    records = event.get("Records", [])
    deferred = 0

    for index, record in enumerate(records):
        message_id = record.get("messageId", "")
        left = remaining()

        # The first record is always attempted (a batch that never starts
        # anything makes no progress); later ones only when they fit.
        if index > 0 and left < reserve:
            deferred += 1
            failures.append({"itemIdentifier": message_id})
            continue

        try:
            body = json.loads(record.get("body") or "{}")
            with record_time_budget(None if left == float("inf") else left - BATCH_SAFETY_MARGIN_SECONDS):
                process_job(body["job_type"], body["payload"])
        except Exception:
            logger.exception("Failed processing SQS async job message_id=%s", message_id)
            failures.append({"itemIdentifier": message_id})

    if deferred:
        logger.warning(
            "Deferred %d of %d SQS records: not enough invocation time left "
            "(reserve %.0fs/record); they are reported as batch item failures and redelivered",
            deferred, len(records), reserve,
        )

    return {"batchItemFailures": failures}
