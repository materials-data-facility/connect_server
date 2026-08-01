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

# Filter params whose values may be comma-separated inside a single occurrence.
#
# Multi-select is expressed by REPEATING a param (?keyword=a&keyword=b), which is
# delimiter-safe. Comma splitting is kept only as backward compatibility for the
# params whose facet values provably never contain a comma. Measured against the
# production index (935 datasets): dc.year, mdf.organization, dc.subjects and
# mdf.domains have zero comma-bearing facet values, while 476 of 500
# dc.creators.name values contain one, because authors are indexed as
# "Family, Given" ("Blaiszik, Ben"). Comma-splitting an author therefore shredded
# one name into two non-matching terms, which is why the Authors filter selected
# nothing. Author values are always taken verbatim.
COMMA_SPLIT_PARAMS = frozenset({"year", "organization", "keyword", "domain"})


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

# Result orderings the API accepts. Ranking strategies are named here and
# resolved to a concrete backend ordering in v2.search.resolve_sort, so a new
# ranking (e.g. a real most-viewed index) is a change in one place.
SORT_RELEVANCE = "relevance"
SORT_NEWEST = "newest"
SORT_MOST_VIEWED = "most_viewed"

# What an empty query browses by. Relevance is meaningless with no query terms,
# so landing on /search without one shows the most recently published datasets.
DEFAULT_BROWSE_SORT = SORT_NEWEST


def _resolve_sort(sort: Optional[str], has_query: bool) -> str:
    """Pick the effective sort for a request.

    Unknown values fall back rather than erroring: sort is a display preference,
    not something worth failing a search over.
    """
    requested = (sort or "").strip().lower()
    if requested not in (SORT_RELEVANCE, SORT_NEWEST, SORT_MOST_VIEWED):
        requested = SORT_RELEVANCE if has_query else DEFAULT_BROWSE_SORT
    # Relevance needs query terms to rank against; without them every entry ties
    # and the order is arbitrary, so browse mode always uses the browse ordering.
    if requested == SORT_RELEVANCE and not has_query:
        return DEFAULT_BROWSE_SORT
    return requested


def _parse_filters(
    year: Optional[List[str]],
    organization: Optional[List[str]],
    author: Optional[List[str]],
    keyword: Optional[List[str]],
    domain: Optional[List[str]],
) -> Optional[Dict[str, List[str]]]:
    """Parse repeated filter query params into a filters dict.

    Multi-select uses repeated params (``?author=A&author=B``). Params in
    COMMA_SPLIT_PARAMS additionally split each occurrence on commas for
    backward compatibility; see that constant for why author does not.
    """
    raw = {
        "year": year,
        "organization": organization,
        "author": author,
        "keyword": keyword,
        "domain": domain,
    }
    filters: Dict[str, List[str]] = {}
    for param, occurrences in raw.items():
        if not occurrences:
            continue
        values: List[str] = []
        for occurrence in occurrences:
            if occurrence is None:
                continue
            parts = occurrence.split(",") if param in COMMA_SPLIT_PARAMS else [occurrence]
            for part in parts:
                part = part.strip()
                if part and part not in values:
                    values.append(part)
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
    sort: Optional[str] = Query(
        None, description="relevance | newest | most_viewed. Defaults to newest when no query."
    ),
    year: Optional[List[str]] = Query(None),
    organization: Optional[List[str]] = Query(None),
    author: Optional[List[str]] = Query(None),
    keyword: Optional[List[str]] = Query(None),
    domain: Optional[List[str]] = Query(None),
    auth: Optional[AuthContext] = Depends(get_optional_auth),
):
    """Keyword search, and browse when no query is given.

    ``q`` is optional. Landing on /search without one is a browse request: it
    returns the newest published datasets in the same response shape as a
    keyword search, so the client renders both identically.
    """
    search_query = (q or query or "").strip()
    has_query = bool(search_query)
    sort_val = _resolve_sort(sort, has_query)

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
        sort=sort_val,
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

    Requires authentication: every query is embedded server-side through the
    paid OpenAI API, so this must not be an anonymous, unmetered endpoint.
    Anonymous discovery stays available via keyword ``GET /search``.

    Falls back to {"available": false, ...} when no snapshot is available yet.
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

    Lets the frontend do client-side cosine scanning against the public
    snapshot blob without ever seeing the OpenAI key. Requires authentication:
    it is a direct proxy onto the paid OpenAI embeddings API, so leaving it
    anonymous lets anyone run up unbounded spend on the server's key.
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
