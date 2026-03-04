import json
import logging
import os
import sqlite3
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sqlite_path() -> str:
    return os.environ.get("ASYNC_SQLITE_PATH", os.environ.get("SQLITE_PATH", "/tmp/mdf_connect_v2.db"))


class JobDispatcher:
    def dispatch(self, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class InlineJobDispatcher(JobDispatcher):
    def dispatch(self, job_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = process_job(job_type, payload)
        return {
            "mode": "inline",
            "queued": False,
            "job_type": job_type,
            "result": result,
        }


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
    return get_job_dispatcher().dispatch(JOB_TRANSFER_DATA, payload)


def enqueue_cleanup_transfers_job() -> Dict[str, Any]:
    return get_job_dispatcher().dispatch(JOB_CLEANUP_TRANSFERS, {})


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
    from v2.curation import _mint_doi_for_submission
    from v2.store import get_store

    source_id = payload["source_id"]
    version = payload["version"]

    store = get_store()
    submission = store.get_submission(source_id, version)
    if not submission:
        return {"success": False, "error": f"Submission not found: {source_id} v{version}"}

    all_versions = store.list_versions(source_id)
    doi_result = _mint_doi_for_submission(submission, all_versions=all_versions, mint_doi=True)
    if doi_result.get("success"):
        if doi_result.get("doi"):
            submission["doi"] = doi_result["doi"]
        if doi_result.get("dataset_doi"):
            submission["dataset_doi"] = doi_result["dataset_doi"]
        submission["status"] = "published"
        submission["published_at"] = _utc_now()
        submission["updated_at"] = _utc_now()
        store.upsert_submission(submission)
    return doi_result


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


def _update_prior_versions_search(store, search_client, source_id: str, current_version: str, all_versions: list) -> None:
    """Re-ingest prior versions into search with latest=false."""
    for v_record in all_versions:
        v = v_record.get("version")
        if v == current_version:
            continue
        if v_record.get("status") != "published":
            continue
        # Ensure the prior version's metadata has latest=false
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
        # Re-ingest into search with latest=false
        search_client.ingest(v_record, version_count=len(all_versions))
        logger.info("Updated prior version %s v%s search entry with latest=false", source_id, v)


def _process_publish_submission(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Publish a submission: mint DOI (optional), ingest into search, update status."""
    from v2.curation import _mint_doi_for_submission
    from v2.search_client import get_search_client
    from v2.store import get_store

    source_id = payload["source_id"]
    version = payload["version"]
    mint_doi = payload.get("mint_doi", True)

    store = get_store()
    submission = store.get_submission(source_id, version)
    if not submission:
        return {"success": False, "error": f"Submission not found: {source_id} v{version}"}

    # Look up all versions for DOI versioning context
    all_versions = store.list_versions(source_id)

    result: Dict[str, Any] = {"source_id": source_id, "version": version}

    # Step 1: DOI handling (mint new or update existing dataset DOI)
    # Always call _mint_doi_for_submission — it handles both mint_doi=True
    # (mint new DOI) and mint_doi=False (update dataset DOI metadata only)
    try:
        doi_result = _mint_doi_for_submission(
            submission, all_versions=all_versions, mint_doi=mint_doi,
        )
        result["doi"] = doi_result
        if doi_result.get("success"):
            if doi_result.get("doi"):
                submission["doi"] = doi_result["doi"]
            if doi_result.get("dataset_doi"):
                submission["dataset_doi"] = doi_result["dataset_doi"]
        else:
            logger.warning("DOI handling failed for %s: %s", source_id, doi_result.get("error"))
    except Exception:
        logger.exception("DOI handling error for %s", source_id)
        result["doi"] = {"success": False, "error": "DOI handling exception"}

    # Step 2: Ingest into Globus Search
    try:
        search_client = get_search_client()
        search_result = search_client.ingest(submission, version_count=len(all_versions))
        result["search_ingest"] = search_result
        if not search_result.get("success"):
            logger.warning("Search ingest failed for %s: %s", source_id, search_result.get("error"))
    except Exception:
        logger.exception("Search ingest error for %s", source_id)
        result["search_ingest"] = {"success": False, "error": "Search ingest exception"}

    # Step 2b: If this is a new version, re-ingest prior version with latest=false
    if len(all_versions) > 1:
        try:
            _update_prior_versions_search(store, search_client, source_id, version, all_versions)
        except Exception:
            logger.warning("Failed to update prior version search entries for %s", source_id, exc_info=True)

    # Step 3: Update status to published
    now = _utc_now()
    submission["status"] = "published"
    submission["published_at"] = now
    submission["updated_at"] = now
    store.upsert_submission(submission)

    # Notify submitter their dataset is live
    try:
        from v2.email_utils import notify_submitter_approved
        notify_submitter_approved(submission)
    except Exception:
        logger.warning("Failed to send approval email for %s", source_id, exc_info=True)

    result["success"] = True
    result["status"] = "published"
    result["published_at"] = now
    return result


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


def handle_sqs_event(event: Dict[str, Any]) -> Dict[str, Any]:
    failures: List[Dict[str, str]] = []
    for record in event.get("Records", []):
        message_id = record.get("messageId", "")
        try:
            body = json.loads(record.get("body") or "{}")
            process_job(body["job_type"], body["payload"])
        except Exception:
            logger.exception("Failed processing SQS async job message_id=%s", message_id)
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}
