"""Search for MDF v2.

Provides full-text search across datasets and streams.
Tries Globus Search first (when configured), falls back to local DynamoDB scan.
Pure helper functions imported by the search router.
"""

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

from v2.metadata import parse_metadata
from v2.store import get_store
from v2.stream_store import get_stream_store

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, parsed)


SEARCH_MAX_DATASET_SCAN = _env_int("SEARCH_MAX_DATASET_SCAN", 1000)
SEARCH_MAX_STREAM_SCAN = _env_int("SEARCH_MAX_STREAM_SCAN", 2000)


def _is_searchable_dataset(record: Dict[str, Any]) -> bool:
    """Only published datasets are eligible for public search fallback."""
    return record.get("status") == "published"


def _extract_searchable_text(record: Dict[str, Any]) -> str:
    """Extract all searchable text from a submission record."""
    parts = []

    # Basic fields
    parts.append(record.get("source_id", ""))
    parts.append(record.get("organization", ""))

    # Parse metadata using the canonical parser
    meta = parse_metadata(record)

    parts.append(meta.title)
    for author in meta.authors:
        parts.append(author.name)
        if author.given_name:
            parts.append(author.given_name)
        if author.family_name:
            parts.append(author.family_name)
    parts.append(meta.publisher)
    if meta.description:
        parts.append(meta.description)
    parts.extend(meta.keywords)
    parts.extend(meta.methods)
    if meta.facility:
        parts.append(meta.facility)
    parts.extend(meta.fields_of_science)
    parts.extend(meta.tags)
    parts.extend(meta.domains)
    if meta.external_source:
        parts.append(meta.external_source)

    # ML metadata
    if meta.ml:
        parts.append(meta.ml.data_format)
        parts.extend(meta.ml.task_type)
        parts.extend(meta.ml.domain)
        if meta.ml.short_name:
            parts.append(meta.ml.short_name)
        for key in meta.ml.keys:
            parts.append(key.name)
            if key.description:
                parts.append(key.description)

    # Extensions (flatten for full-text)
    if meta.extensions:
        parts.append(json.dumps(meta.extensions))

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
            score += text_lower.count(term)

    return score


def _format_dataset_result(record: Dict[str, Any], score: float) -> Dict[str, Any]:
    """Format a dataset record for search results."""
    meta = parse_metadata(record)

    return {
        "type": "dataset",
        "source_id": record.get("source_id"),
        "version": record.get("version"),
        "title": meta.title,
        "authors": [a.name for a in meta.authors],
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
    """Search across all datasets.

    Tries Globus Search first. Falls back to local DynamoDB scan if
    Globus Search is not configured or the query fails. The fallback
    only returns published datasets so unpublished submissions are not
    exposed by degraded search behavior.
    """
    # Try Globus Search first
    try:
        from v2.search_client import get_search_client
        client = get_search_client()
        result = client.search(query, limit=limit)
        if result.get("success") and result.get("results"):
            return result["results"][:limit]
    except Exception:
        logger.debug("Globus Search unavailable, falling back to local scan", exc_info=True)

    # Fallback: local DynamoDB scan
    store = get_store()
    all_submissions = store.list_all(limit=max(limit, SEARCH_MAX_DATASET_SCAN))

    results = []
    for record in all_submissions:
        if not _is_searchable_dataset(record):
            continue
        text = _extract_searchable_text(record)
        score = _simple_match(text, query)
        if score > 0:
            results.append((score, record))

    results.sort(key=lambda x: x[0], reverse=True)

    return [_format_dataset_result(r, s) for s, r in results[:limit]]


def search_streams(query: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Search across all streams."""
    stream_store = get_stream_store()
    all_streams = stream_store.list_all(limit=max(limit, SEARCH_MAX_STREAM_SCAN))

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

    results.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {
        "query": query,
        "total": len(results),
        "results": results[:limit],
    }
