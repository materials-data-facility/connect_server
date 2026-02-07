import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from v2.search import search_all

router = APIRouter()


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, parsed)


MAX_SEARCH_RESULTS = _env_int("SEARCH_MAX_RESULTS", 50)


@router.get("/search")
async def search_endpoint(
    q: Optional[str] = Query(None),
    query: Optional[str] = Query(None),
    type: Optional[str] = Query("all"),
    limit: Optional[int] = Query(20),
):
    search_query = q or query
    if not search_query:
        raise HTTPException(400, "query (q) is required")

    search_type = type or "all"
    include_datasets = search_type in ("all", "datasets", "dataset")
    include_streams = search_type in ("all", "streams", "stream")

    try:
        limit_val = int(limit) if limit else 20
    except ValueError:
        limit_val = 20
    limit_val = max(1, min(limit_val, MAX_SEARCH_RESULTS))

    results = search_all(
        query=search_query,
        include_datasets=include_datasets,
        include_streams=include_streams,
        limit=limit_val,
    )

    return results
