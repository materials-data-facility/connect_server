import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
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
