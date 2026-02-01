"""Local search endpoint for MDF v2.

Provides full-text search across datasets and streams using SQLite FTS5.
This is a local simulation of what Globus Search would provide.
"""

import json
import re
from typing import Any, Dict, List, Optional

from v2.request import parse_json_body
from v2.responses import bad_request, ok
from v2.store import get_store
from v2.stream_store import get_stream_store


def _extract_searchable_text(record: Dict[str, Any]) -> str:
    """Extract all searchable text from a submission record."""
    parts = []

    # Basic fields
    parts.append(record.get("source_id", ""))
    parts.append(record.get("organization", ""))

    # Parse dataset metadata
    mdata_str = record.get("dataset_mdata") or "{}"
    try:
        mdata = json.loads(mdata_str) if isinstance(mdata_str, str) else (mdata_str or {})
    except Exception:
        mdata = {}

    if mdata is None:
        mdata = {}

    # DataCite fields
    dc = mdata.get("dc") or {}
    for title in dc.get("titles") or []:
        if isinstance(title, dict):
            parts.append(title.get("title", ""))
        else:
            parts.append(str(title))

    for creator in dc.get("creators") or []:
        if isinstance(creator, dict):
            parts.append(creator.get("creatorName", ""))
            parts.append(creator.get("givenName", ""))
            parts.append(creator.get("familyName", ""))
        else:
            parts.append(str(creator))

    parts.append(dc.get("publisher") or "")

    for desc in dc.get("descriptions") or []:
        if isinstance(desc, dict):
            parts.append(desc.get("description", ""))
        else:
            parts.append(str(desc))

    for subj in dc.get("subjects") or []:
        if isinstance(subj, dict):
            parts.append(subj.get("subject", ""))
        else:
            parts.append(str(subj))

    # MDF block
    mdf = mdata.get("mdf") or {}
    parts.append(mdf.get("lab_id") or "")
    parts.append(mdf.get("facility") or "")
    if mdf.get("instruments"):
        parts.extend(mdf["instruments"] if isinstance(mdf["instruments"], list) else [str(mdf["instruments"])])

    # Custom block (flatten)
    custom = mdata.get("custom") or {}
    parts.append(json.dumps(custom))

    return " ".join(str(p) for p in parts if p)


def _extract_stream_text(stream: Dict[str, Any]) -> str:
    """Extract searchable text from a stream record."""
    parts = []
    parts.append(stream.get("stream_id", ""))
    parts.append(stream.get("title", ""))
    parts.append(stream.get("lab_id", ""))
    parts.append(stream.get("organization", ""))

    metadata = stream.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}
    if metadata is None:
        metadata = {}

    parts.append(metadata.get("run_id", ""))
    parts.append(metadata.get("facility", ""))
    parts.append(metadata.get("operator", ""))
    if metadata.get("instruments"):
        instr = metadata["instruments"]
        parts.extend(instr if isinstance(instr, list) else [str(instr)])

    return " ".join(str(p) for p in parts if p)


def _simple_match(text: str, query: str) -> float:
    """Simple relevance scoring - count query term matches."""
    text_lower = text.lower()
    query_terms = re.split(r'\s+', query.lower().strip())

    score = 0.0
    for term in query_terms:
        if term in text_lower:
            # Count occurrences
            score += text_lower.count(term)

    return score


def _format_dataset_result(record: Dict[str, Any], score: float) -> Dict[str, Any]:
    """Format a dataset record for search results."""
    mdata_str = record.get("dataset_mdata", "{}")
    try:
        mdata = json.loads(mdata_str) if isinstance(mdata_str, str) else mdata_str
    except Exception:
        mdata = {}

    dc = mdata.get("dc", {})
    titles = dc.get("titles", [])
    title = titles[0].get("title") if titles and isinstance(titles[0], dict) else str(titles[0]) if titles else record.get("source_id")

    creators = dc.get("creators", [])
    authors = []
    for c in creators:
        if isinstance(c, dict):
            authors.append(c.get("creatorName", c.get("familyName", "")))
        else:
            authors.append(str(c))

    return {
        "type": "dataset",
        "source_id": record.get("source_id"),
        "version": record.get("version"),
        "title": title,
        "authors": authors,
        "status": record.get("status"),
        "created_at": record.get("created_at"),
        "score": score,
    }


def _format_stream_result(stream: Dict[str, Any], score: float) -> Dict[str, Any]:
    """Format a stream record for search results."""
    return {
        "type": "stream",
        "stream_id": stream.get("stream_id"),
        "title": stream.get("title"),
        "lab_id": stream.get("lab_id"),
        "status": stream.get("status"),
        "file_count": stream.get("file_count", 0),
        "created_at": stream.get("created_at"),
        "score": score,
    }


def search_datasets(query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Search across all datasets."""
    store = get_store()
    all_submissions = store.list_all()

    results = []
    for record in all_submissions:
        text = _extract_searchable_text(record)
        score = _simple_match(text, query)
        if score > 0:
            results.append((score, record))

    # Sort by score descending
    results.sort(key=lambda x: x[0], reverse=True)

    return [_format_dataset_result(r, s) for s, r in results[:limit]]


def search_streams(query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Search across all streams."""
    stream_store = get_stream_store()
    all_streams = stream_store.list_all()

    results = []
    for stream in all_streams:
        text = _extract_stream_text(stream)
        score = _simple_match(text, query)
        if score > 0:
            results.append((score, stream))

    results.sort(key=lambda x: x[0], reverse=True)

    return [_format_stream_result(r, s) for s, r in results[:limit]]


def search_all(
    query: str,
    include_datasets: bool = True,
    include_streams: bool = True,
    limit: int = 20,
) -> Dict[str, Any]:
    """Search across datasets and streams."""
    results = []

    if include_datasets:
        results.extend(search_datasets(query, limit=limit))

    if include_streams:
        results.extend(search_streams(query, limit=limit))

    # Re-sort combined results
    results.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {
        "query": query,
        "total": len(results),
        "results": results[:limit],
    }


def lambda_handler(event, context):
    """Handle search requests.

    GET /search?q=perovskite&type=all&limit=20
    POST /search with {"query": "perovskite", "type": "all", "limit": 20}
    """
    # Handle both GET query params and POST body
    query_params = event.get("queryStringParameters") or {}

    if event.get("body"):
        payload, error = parse_json_body(event)
        if error:
            return bad_request(error)
        payload = payload or {}
    else:
        payload = {}

    # Get query from either source
    query = payload.get("query") or query_params.get("q") or query_params.get("query")
    if not query:
        return bad_request("query (q) is required")

    # Get type filter
    search_type = payload.get("type") or query_params.get("type") or "all"
    include_datasets = search_type in ("all", "datasets", "dataset")
    include_streams = search_type in ("all", "streams", "stream")

    # Get limit
    try:
        limit = int(payload.get("limit") or query_params.get("limit") or 20)
    except ValueError:
        limit = 20

    results = search_all(
        query=query,
        include_datasets=include_datasets,
        include_streams=include_streams,
        limit=limit,
    )

    return ok(results)
