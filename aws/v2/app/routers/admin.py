import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel


def _is_embedding_stale(record: dict) -> bool:
    """True when the embedding was generated before the last metadata edit.

    String comparison works because both stamps are ISO-8601 with Z suffix,
    and a missing `embedding_generated_at` trivially counts as stale.
    """
    gen_at = record.get("embedding_generated_at") or ""
    mdata_at = record.get("metadata_updated_at") or ""
    if not mdata_at:
        return False
    return gen_at < mdata_at

from v2.app.auth import require_curator
from v2.app.deps import get_submission_store
from v2.app.models import AuthContext
from v2.store import SubmissionStore

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# v1 -> v2 sync visibility + trigger (curator-only)
#
# The sync itself runs in GitHub Actions (.github/workflows/sync-v1-to-v2.yml),
# not in Lambda: it is a long, chatty job that would blow past both the API
# Gateway 30s cap and the async worker budget. The runner publishes its
# progress to two SSM parameters, which this module reads:
#
#   /mdf/{ENV}/sync-last-report  JSON report of the most recent finished run
#   /mdf/{ENV}/sync-lock         present only while a run is in flight
#
# Lambda only ever *reads* those parameters (see the ssm:GetParameter grant in
# aws/template.yaml); the workflow is the sole writer.
# ---------------------------------------------------------------------------

SYNC_REPORT_PARAM = "/mdf/{env}/sync-last-report"
SYNC_LOCK_PARAM = "/mdf/{env}/sync-lock"

# A lock older than this almost certainly belongs to a run that died without
# cleaning up (cancelled workflow, runner OOM), so we stop treating it as a
# live run and let curators trigger again.
SYNC_LOCK_STALE_SECONDS = 2 * 60 * 60

DEFAULT_SYNC_WORKFLOW = "sync-v1-to-v2.yml"
DEFAULT_SYNC_REF = "main"
GITHUB_DISPATCH_TIMEOUT_SECONDS = 10.0


def _sync_environment() -> str:
    return os.environ.get("ENVIRONMENT", "dev")


class SsmUnavailable(RuntimeError):
    """boto3/SSM could not be consulted at all (no boto3, no creds, denied)."""


def _get_ssm_param(name: str) -> Optional[str]:
    """Fetch a single SSM parameter value.

    Returns the raw string, or None when the parameter does not exist.
    Raises SsmUnavailable when SSM itself is unreachable (local dev without
    boto3/credentials, missing IAM grant, ...) so callers can tell "no sync has
    run yet" apart from "we cannot see the sync state".
    """
    try:
        import boto3
    except Exception as exc:  # pragma: no cover - boto3 ships in the Lambda runtime
        raise SsmUnavailable(f"boto3 unavailable: {exc}") from exc

    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    try:
        client = boto3.client("ssm", region_name=region)
        resp = client.get_parameter(Name=name)
    except Exception as exc:
        # ParameterNotFound is the expected "nothing here yet" case. botocore
        # error classes are not importable without boto3, so match on name.
        if type(exc).__name__ == "ParameterNotFound":
            return None
        raise SsmUnavailable(f"{type(exc).__name__}: {exc}") from exc

    return (resp.get("Parameter") or {}).get("Value")


def _parse_json_param(raw: Optional[str], label: str) -> Optional[dict]:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning("Sync %s SSM parameter is not valid JSON", label)
        return None
    if not isinstance(parsed, dict):
        logger.warning("Sync %s SSM parameter is not a JSON object", label)
        return None
    return parsed


def _parse_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _lock_is_stale(lock: dict) -> bool:
    """A lock is stale once it is older than SYNC_LOCK_STALE_SECONDS.

    An unparseable/missing `acquired_at` counts as *not* stale: better to make a
    curator wait than to fire a second concurrent sync.
    """
    acquired = _parse_iso(lock.get("acquired_at"))
    if acquired is None:
        return False
    age = (datetime.now(timezone.utc) - acquired).total_seconds()
    return age > SYNC_LOCK_STALE_SECONDS


def _read_sync_state() -> dict:
    """Read both sync SSM parameters and derive the sync status block.

    Never raises: on any failure the block degrades to nulls with
    ``available: false`` so /admin/stats keeps working in local dev.
    """
    env = _sync_environment()
    state: dict[str, Any] = {
        "available": False,
        "environment": env,
        "last_report": None,
        "running": False,
        "lock": None,
        "stale_lock": False,
    }

    try:
        raw_report = _get_ssm_param(SYNC_REPORT_PARAM.format(env=env))
        raw_lock = _get_ssm_param(SYNC_LOCK_PARAM.format(env=env))
    except SsmUnavailable as exc:
        logger.info("Sync state unavailable: %s", exc)
        state["error"] = str(exc)
        return state

    report = _parse_json_param(raw_report, "report")
    lock = _parse_json_param(raw_lock, "lock")

    # `available` means "we actually have sync state to show" — false both when
    # SSM is unreachable and when neither parameter has ever been written.
    state["available"] = report is not None or lock is not None
    state["last_report"] = report
    if lock is not None:
        stale = _lock_is_stale(lock)
        state["lock"] = lock
        state["stale_lock"] = stale
        state["running"] = not stale
    return state


def _dispatch_github_workflow(
    repo: str,
    workflow: str,
    ref: str,
    token: str,
    inputs: dict,
) -> tuple[Optional[int], str]:
    """POST a workflow_dispatch to the GitHub REST API.

    Returns ``(status_code, body)``; status_code is None when the request never
    got a response (DNS/timeout/TLS). Isolated in one function so tests can
    monkeypatch it instead of touching the network.
    """
    import httpx

    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
    try:
        resp = httpx.post(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={"ref": ref, "inputs": inputs},
            timeout=GITHUB_DISPATCH_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logger.warning("GitHub workflow dispatch request failed: %s", exc)
        return None, f"{type(exc).__name__}: {exc}"
    return resp.status_code, (resp.text or "")


@router.get("/admin/sync")
async def sync_status(
    auth: AuthContext = Depends(require_curator),
):
    """Just the sync block from /admin/stats, for cheap polling."""
    return {"success": True, "sync": _read_sync_state()}


@router.post("/admin/sync")
async def trigger_sync(
    auth: AuthContext = Depends(require_curator),
):
    """Trigger the v1 -> v2 sync GitHub Actions workflow.

    409 when a run is already in flight, 501 when the trigger credentials are
    not configured, 502 when GitHub rejects the dispatch, 202 on success.
    """
    env = _sync_environment()
    state = _read_sync_state()
    if state.get("running"):
        return JSONResponse(
            status_code=409,
            content={
                "detail": "sync already running",
                "lock": state.get("lock"),
                "stale_lock": state.get("stale_lock", False),
            },
        )

    token = (os.environ.get("GITHUB_SYNC_TOKEN") or "").strip()
    repo = (os.environ.get("GITHUB_SYNC_REPO") or "").strip()
    workflow = (os.environ.get("GITHUB_SYNC_WORKFLOW") or "").strip() or DEFAULT_SYNC_WORKFLOW
    ref = (os.environ.get("GITHUB_SYNC_REF") or "").strip() or DEFAULT_SYNC_REF

    if not token or token == "not-configured" or not repo:
        return JSONResponse(
            status_code=501,
            content={
                "detail": "sync trigger not configured",
                "hint": (
                    "Set GITHUB_SYNC_TOKEN (a PAT or GitHub App token with the "
                    "'actions:write' scope) and GITHUB_SYNC_REPO "
                    "(e.g. 'materials-data-facility/connect_server') on the API "
                    "Lambda. Until then, run the sync-v1-to-v2 workflow manually "
                    "from the GitHub Actions UI."
                ),
            },
        )

    status_code, body = _dispatch_github_workflow(
        repo=repo,
        workflow=workflow,
        ref=ref,
        token=token,
        inputs={"environment": env},
    )

    if status_code != 204:
        logger.warning(
            "GitHub workflow dispatch for %s/%s failed (status=%s)", repo, workflow, status_code
        )
        return JSONResponse(
            status_code=502,
            content={
                "detail": "failed to trigger sync workflow",
                "upstream_status": status_code,
                "upstream_body": body[:500],
                "workflow": workflow,
            },
        )

    return JSONResponse(
        status_code=202,
        content={
            "triggered": True,
            "workflow": workflow,
            "environment": env,
            "repo": repo,
            "ref": ref,
            "message": (
                "Sync workflow dispatched. Poll /admin/sync (or /admin/stats) "
                "for the lock and the finished report."
            ),
        },
    )


@router.get("/admin/stats")
async def admin_stats(
    auth: AuthContext = Depends(require_curator),
    store: SubmissionStore = Depends(get_submission_store),
):
    all_submissions = store.list_all(limit=10000)
    counts: dict[str, int] = {}
    total_views = 0
    total_downloads = 0
    for sub in all_submissions:
        status = sub.get("status", "unknown")
        counts[status] = counts.get(status, 0) + 1
        total_views += int(sub.get("view_count") or 0)
        total_downloads += int(sub.get("download_count") or 0)

    return {
        "success": True,
        "total": len(all_submissions),
        "by_status": counts,
        "access_totals": {
            "view_count": total_views,
            "download_count": total_downloads,
        },
        "sync": _read_sync_state(),
    }


# ---------------------------------------------------------------------------
# Embedding management (curator-only)
# ---------------------------------------------------------------------------

class RebuildEmbeddingsRequest(BaseModel):
    force: bool = False  # Re-embed even records that already have a vector
    build_snapshot: bool = True
    limit: Optional[int] = None  # Max records to embed in this run


@router.get("/admin/embeddings/status")
async def embedding_status(
    auth: AuthContext = Depends(require_curator),
    store: SubmissionStore = Depends(get_submission_store),
):
    """Coverage report: which published datasets have embeddings, which don't."""
    from v2.embedding_snapshot import read_pointer
    from v2.embeddings import EMBEDDING_MODEL

    all_submissions = store.list_all(limit=100000)

    total_published = 0
    with_embedding = 0
    stale = 0
    by_model: dict[str, int] = {}
    missing: list[dict] = []
    stale_sample: list[dict] = []

    for sub in all_submissions:
        if sub.get("status") != "published":
            continue
        total_published += 1
        mdata = sub.get("dataset_mdata") or {}
        if isinstance(mdata, dict) and mdata.get("latest") is False:
            continue
        emb = sub.get("title_description_embedding")
        model = sub.get("embedding_model") or ""
        if emb:
            with_embedding += 1
            by_model[model or "unknown"] = by_model.get(model or "unknown", 0) + 1
            if _is_embedding_stale(sub):
                stale += 1
                if len(stale_sample) < 50:
                    stale_sample.append({
                        "source_id": sub.get("source_id"),
                        "version": sub.get("version"),
                        "embedding_generated_at": sub.get("embedding_generated_at"),
                        "metadata_updated_at": sub.get("metadata_updated_at"),
                    })
        elif len(missing) < 50:
            missing.append({
                "source_id": sub.get("source_id"),
                "version": sub.get("version"),
            })

    return {
        "success": True,
        "current_model": EMBEDDING_MODEL,
        "published_total": total_published,
        "with_embedding": with_embedding,
        "without_embedding": total_published - with_embedding,
        "stale": stale,
        "by_model": by_model,
        "missing_sample": missing,
        "stale_sample": stale_sample,
        "snapshot": read_pointer(),
    }


@router.post("/admin/embeddings/rebuild")
async def rebuild_embeddings(
    body: Optional[RebuildEmbeddingsRequest] = None,
    auth: AuthContext = Depends(require_curator),
):
    """Kick off an async embedding rebuild.

    The scan + per-dataset SQS fan-out blows past API Gateway's 30s cap for
    any non-tiny corpus, so this endpoint just enqueues a single dispatcher
    job. The async worker (120s budget) scans Dynamo, fans out one
    JOB_GENERATE_EMBEDDING per pending record, and finally enqueues a
    JOB_BUILD_EMBEDDING_SNAPSHOT.

    Poll `/admin/embeddings/status` to watch the coverage catch up.
    """
    from v2.async_jobs import enqueue_rebuild_dispatch_job
    from v2.embeddings import EMBEDDING_MODEL

    params = body or RebuildEmbeddingsRequest()

    try:
        dispatch_job = enqueue_rebuild_dispatch_job(
            force=params.force,
            limit=params.limit,
            build_snapshot=params.build_snapshot,
        )
    except Exception as exc:
        logger.exception("Failed to enqueue dispatch embedding rebuild job")
        return {"success": False, "error": str(exc)}

    return {
        "success": True,
        "model": EMBEDDING_MODEL,
        "force": params.force,
        "limit": params.limit,
        "build_snapshot": params.build_snapshot,
        "dispatch_job": dispatch_job,
        "message": (
            "Rebuild dispatched to async worker. "
            "Poll /admin/embeddings/status to watch progress."
        ),
    }


@router.post("/admin/embeddings/snapshot")
async def rebuild_snapshot_only(
    auth: AuthContext = Depends(require_curator),
):
    """Enqueue just the snapshot build, no embedding work.

    Useful when the Dynamo records are already fresh but the snapshot is stale.
    Runs async to avoid the API Gateway 30s cap on large corpora.
    """
    from v2.async_jobs import enqueue_snapshot_build_job

    try:
        job = enqueue_snapshot_build_job()
    except Exception as exc:
        logger.exception("Failed to enqueue snapshot build job")
        return {"success": False, "error": str(exc)}

    return {
        "success": True,
        "snapshot_job": job,
        "message": "Snapshot build enqueued. Poll /admin/embeddings/status to see the new snapshot.",
    }
