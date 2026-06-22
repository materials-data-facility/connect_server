import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

from v2.app.auth import get_auth, get_optional_auth
from v2.app.models import AuthContext
from v2.search import (
    find_related_by_author,
    find_similar_by_embedding,
    search_all,
    search_semantic,
)

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
    auth: Optional[AuthContext] = Depends(get_optional_auth),
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
        auth=auth,
    )

    return results


@router.get("/search/semantic")
async def semantic_search_endpoint(
    q: str = Query(..., description="Free-text query. Will be embedded server-side."),
    limit: int = Query(20),
    auth: AuthContext = Depends(get_auth),
):
    """Top-k semantic search over the embedding snapshot.

    Requires authentication: each query is embedded server-side via the paid
    OpenAI API, so this endpoint is not exposed anonymously (anonymous keyword
    search remains available at GET /search). Falls back to
    {"available": false, ...} when no snapshot is available yet.
    """
    limit_val = max(1, min(int(limit), MAX_SEARCH_RESULTS))
    return search_semantic(q, limit=limit_val)


class EmbedRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=8000)


@router.post("/embed")
async def embed_query_endpoint(
    body: EmbedRequest,
    auth: AuthContext = Depends(get_auth),
):
    """Return an embedding vector for arbitrary text.

    Lets the frontend do client-side cosine scanning against the public snapshot
    blob without ever seeing the OpenAI key. Requires authentication: this is a
    direct proxy to the paid OpenAI embeddings API, so it must not be callable
    anonymously (which would let anyone run up unbounded OpenAI spend).
    """
    from v2.embeddings import EMBEDDING_MODEL, EmbeddingError, embed_text

    try:
        vector = embed_text(body.text)
    except EmbeddingError as exc:
        raise HTTPException(503, f"Embedding unavailable: {exc}")
    return {"model": EMBEDDING_MODEL, "dims": len(vector), "embedding": vector}


@router.get("/datasets/{source_id}/related")
async def related_datasets_endpoint(
    source_id: str,
    by: str = Query(
        "author",
        description="Relation type: 'author' (shared authors) or 'similar' (embedding cosine).",
    ),
    limit: int = Query(20),
):
    """Related datasets.

    - `by=author` — co-author lookup over the in-memory author index
    - `by=similar` — nearest neighbors over the embedding snapshot
    """
    limit_val = max(1, min(int(limit), MAX_SEARCH_RESULTS))
    if by == "author":
        return find_related_by_author(source_id, limit=limit_val)
    if by == "similar":
        return find_similar_by_embedding(source_id, limit=limit_val)
    raise HTTPException(400, f"Unsupported relation type: {by}")
