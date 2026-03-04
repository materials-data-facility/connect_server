import logging
import os
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

from v2.search import search_all

router = APIRouter()

# Maps query param names to Globus Search field names
FILTER_FIELD_MAP = {
    "year": "dc.year",
    "organization": "mdf.organization",
    "author": "dc.creators.name",
    "keyword": "dc.subjects",
    "domain": "mdf.domains",
}


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


def _parse_filters(
    year: Optional[str],
    organization: Optional[str],
    author: Optional[str],
    keyword: Optional[str],
    domain: Optional[str],
) -> Optional[Dict[str, List[str]]]:
    """Parse comma-separated filter query params into a filters dict."""
    raw = {
        "year": year,
        "organization": organization,
        "author": author,
        "keyword": keyword,
        "domain": domain,
    }
    filters = {}
    for param, value in raw.items():
        if value:
            values = [v.strip() for v in value.split(",") if v.strip()]
            if values:
                filters[FILTER_FIELD_MAP[param]] = values
    return filters or None


@router.get("/search")
async def search_endpoint(
    q: Optional[str] = Query(None),
    query: Optional[str] = Query(None),
    type: Optional[str] = Query("all"),
    limit: Optional[int] = Query(20),
    offset: Optional[int] = Query(0),
    year: Optional[str] = Query(None),
    organization: Optional[str] = Query(None),
    author: Optional[str] = Query(None),
    keyword: Optional[str] = Query(None),
    domain: Optional[str] = Query(None),
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

    try:
        offset_val = int(offset) if offset else 0
    except ValueError:
        offset_val = 0
    offset_val = max(0, offset_val)

    filters = _parse_filters(year, organization, author, keyword, domain)

    results = search_all(
        query=search_query,
        include_datasets=include_datasets,
        include_streams=include_streams,
        limit=limit_val,
        offset=offset_val,
        filters=filters,
    )

    return results
