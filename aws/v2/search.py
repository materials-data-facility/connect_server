"""Search for MDF v2.

Provides full-text search across datasets and streams.
Tries Globus Search first (when configured), falls back to local DynamoDB scan.
Pure helper functions imported by the search router.

Also hosts the semantic (vector) search path and a lightweight in-memory
author index for "related by author" lookups.
"""

import json
import logging
import math
import os
import re
import threading
from typing import Any, Dict, List, Optional, Tuple

from v2.metadata import parse_metadata
from v2.store import get_store
from v2.stream_store import get_stream_store

# v2.app.models / v2.app.auth are imported lazily inside _stream_visible_to_auth
# to avoid circular imports — the app package pulls the search router, which
# in turn imports this module.
if False:  # type-checker only
    from v2.app.models import AuthContext  # noqa: F401

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

    description = meta.description or ""
    return {
        "type": "dataset",
        "source_id": record.get("source_id"),
        "version": record.get("version"),
        "title": meta.title,
        "authors": [a.name for a in meta.authors],
        "keywords": meta.keywords,
        "description": description[:300] if len(description) > 300 else description,
        "publication_year": meta.publication_year,
        "organization": record.get("organization"),
        "domains": meta.domains,
        "doi": record.get("doi"),
        "license": meta.license.identifier or meta.license.name if meta.license else None,
        "size_bytes": record.get("total_bytes"),
        "file_count": record.get("file_count"),
        "status": record.get("status"),
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


def search_datasets(
    query: str,
    limit: int = 20,
    offset: int = 0,
    filters: Optional[Dict[str, list]] = None,
) -> Dict[str, Any]:
    """Search across all datasets, returning results and facets.

    Tries Globus Search faceted_search first. Falls back to local DynamoDB
    scan if Globus Search is not configured or the query fails. The fallback
    only returns published datasets and does not support facets.
    """
    # Try Globus Search first (faceted)
    try:
        from v2.search_client import get_search_client
        client = get_search_client()
        result = client.faceted_search(query, limit=limit, offset=offset, filters=filters)
        if result.get("success"):
            # For mock clients (no data ingested), fall through to DynamoDB
            # so dev/test can search SQLite records. For real Globus Search,
            # trust the result even when empty (e.g. offset past all results).
            if result.get("results") or not result.get("mock"):
                return {
                    "results": result.get("results", []),
                    "total": result.get("total", 0),
                    "facets": result.get("facets", {}),
                }
        else:
            logger.warning("Globus Search faceted_search failed: %s", result.get("error"))
    except Exception:
        logger.warning("Globus Search unavailable, falling back to local scan", exc_info=True)

    # Fallback: local DynamoDB scan (no faceting)
    store = get_store()
    all_submissions = store.list_all(limit=max(limit + offset, SEARCH_MAX_DATASET_SCAN))

    results = []
    for record in all_submissions:
        if not _is_searchable_dataset(record):
            continue
        text = _extract_searchable_text(record)
        score = _simple_match(text, query)
        if score > 0:
            results.append((score, record))

    results.sort(key=lambda x: x[0], reverse=True)
    total = len(results)
    page = results[offset:offset + limit]

    return {
        "results": [_format_dataset_result(r, s) for s, r in page],
        "total": total,
        "facets": {},
    }


def _stream_visible_to_auth(stream: Dict[str, Any], auth: Optional[Any]) -> bool:
    if not auth:
        return False
    from v2.app.auth import is_curator

    if is_curator(auth):
        return True
    owner_id = stream.get("user_id")
    return bool(owner_id and owner_id == auth.user_id)


def search_streams(
    query: str,
    limit: int = 20,
    auth: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Search across streams visible to the current caller."""
    stream_store = get_stream_store()
    all_streams = stream_store.list_all(limit=max(limit, SEARCH_MAX_STREAM_SCAN))

    results = []
    for stream in all_streams:
        if not _stream_visible_to_auth(stream, auth):
            continue
        text = _extract_stream_text(stream)
        score = _simple_match(text, query)
        if score > 0:
            results.append((score, stream))

    results.sort(key=lambda x: x[0], reverse=True)

    return [_format_stream_result(r, s) for s, r in results[:limit]]


# ---------------------------------------------------------------------------
# Semantic search (vectors + cosine over the S3 snapshot)
# ---------------------------------------------------------------------------

def search_semantic(query: str, limit: int = 20) -> Dict[str, Any]:
    """Embed the query and return top-k datasets by cosine similarity.

    Reads from the pre-built embedding snapshot (S3 in prod, filesystem in dev).
    Returns {"available": False, ...} when no snapshot has been built yet so
    callers can fall back cleanly.
    """
    from v2.embedding_snapshot import load_snapshot
    from v2.embeddings import EmbeddingError, embed_text

    snapshot = load_snapshot()
    if snapshot is None or snapshot.count == 0:
        return {
            "available": False,
            "reason": "No embedding snapshot found. Run `mdf admin rebuild-embeddings`.",
            "results": [],
            "total": 0,
        }

    try:
        query_vec = embed_text(query)
    except EmbeddingError as exc:
        return {
            "available": False,
            "reason": str(exc),
            "results": [],
            "total": 0,
        }

    scores = _cosine_scores(query_vec, snapshot.vectors)
    ranked = sorted(enumerate(scores), key=lambda ix: ix[1], reverse=True)

    results = []
    for idx, score in ranked[: max(1, limit)]:
        row = snapshot.rows[idx]
        description = row.get("description") or ""
        results.append({
            "type": "dataset",
            "source_id": row.get("source_id"),
            "version": row.get("version"),
            "title": row.get("title"),
            "authors": row.get("authors") or [],
            "keywords": row.get("keywords") or [],
            "description": description[:300] if len(description) > 300 else description,
            "publication_year": row.get("publication_year"),
            "organization": row.get("organization"),
            "doi": row.get("doi"),
            "score": float(score),
        })

    return {
        "available": True,
        "query": query,
        "model": snapshot.model,
        "snapshot_built_at": snapshot.built_at,
        "snapshot_count": snapshot.count,
        "total": len(results),
        "results": results,
    }


def find_similar_by_embedding(source_id: str, limit: int = 10) -> Dict[str, Any]:
    """Top-k nearest neighbors of `source_id` from the embedding snapshot.

    Used by the dataset detail page to recommend "you might also like" rows.
    Reads only the in-memory cached snapshot — no OpenAI call needed since the
    anchor's vector is already part of the snapshot.
    """
    from v2.embedding_snapshot import load_snapshot

    snapshot = load_snapshot()
    if snapshot is None or snapshot.count == 0:
        return {
            "available": False,
            "reason": "No embedding snapshot found. Run `mdf admin rebuild-embeddings`.",
            "source_id": source_id,
            "results": [],
            "total": 0,
        }

    anchor_idx: Optional[int] = None
    for idx, row in enumerate(snapshot.rows):
        if row.get("source_id") == source_id:
            anchor_idx = idx
            break

    if anchor_idx is None:
        return {
            "available": True,
            "reason": (
                f"Dataset '{source_id}' is not in the current snapshot — it may "
                "be unpublished, superseded, or missing an embedding."
            ),
            "source_id": source_id,
            "results": [],
            "total": 0,
            "snapshot_built_at": snapshot.built_at,
        }

    matrix = snapshot.vectors
    try:
        import numpy as np

        if hasattr(matrix, "shape"):
            anchor_vec = matrix[anchor_idx].tolist()
        else:
            anchor_vec = list(matrix[anchor_idx])
    except ImportError:
        anchor_vec = list(matrix[anchor_idx])

    scores = _cosine_scores(anchor_vec, matrix)

    ranked = sorted(enumerate(scores), key=lambda ix: ix[1], reverse=True)

    results: List[Dict[str, Any]] = []
    for idx, score in ranked:
        if idx == anchor_idx:
            continue
        row = snapshot.rows[idx]
        description = row.get("description") or ""
        results.append({
            "type": "dataset",
            "source_id": row.get("source_id"),
            "version": row.get("version"),
            "title": row.get("title"),
            "authors": row.get("authors") or [],
            "keywords": row.get("keywords") or [],
            "description": description[:300] if len(description) > 300 else description,
            "publication_year": row.get("publication_year"),
            "organization": row.get("organization"),
            "doi": row.get("doi"),
            "score": float(score),
        })
        if len(results) >= max(1, limit):
            break

    return {
        "available": True,
        "source_id": source_id,
        "model": snapshot.model,
        "snapshot_built_at": snapshot.built_at,
        "snapshot_count": snapshot.count,
        "total": len(results),
        "results": results,
    }


def _cosine_scores(query_vec: List[float], matrix: Any) -> List[float]:
    """Cosine similarity between the query vector and each row of `matrix`.

    `matrix` is a numpy 2-D array when numpy is available (normal case), and a
    list[list[float]] as a fallback. Shapes are trusted — the snapshot loader
    already validates dimensions.
    """
    try:
        import numpy as np

        q = np.asarray(query_vec, dtype="float32")
        q_norm = float(np.linalg.norm(q))
        if q_norm == 0.0:
            return [0.0] * (matrix.shape[0] if hasattr(matrix, "shape") else len(matrix))

        if not hasattr(matrix, "shape"):
            matrix = np.asarray(matrix, dtype="float32")

        row_norms = np.linalg.norm(matrix, axis=1)
        dots = matrix @ q
        with np.errstate(divide="ignore", invalid="ignore"):
            scores = dots / (row_norms * q_norm)
        scores = np.nan_to_num(scores, nan=0.0)
        return scores.astype("float32").tolist()
    except ImportError:
        # Pure-Python fallback (slow — ok for dev with a handful of vectors)
        q_norm = math.sqrt(sum(x * x for x in query_vec)) or 1.0
        out = []
        for row in matrix:
            dot = 0.0
            rn = 0.0
            for a, b in zip(query_vec, row):
                dot += a * b
                rn += b * b
            denom = (math.sqrt(rn) or 1.0) * q_norm
            out.append(dot / denom)
        return out


# ---------------------------------------------------------------------------
# Author linking (in-memory index rebuilt on demand)
# ---------------------------------------------------------------------------

_author_index_lock = threading.Lock()
_cached_author_index: Optional[Dict[str, Any]] = None


def normalize_author_key(author: Dict[str, Any]) -> Optional[str]:
    """Collapse a raw author dict to a comparable key.

    Preference order:
    1. ORCID (stripped of URL prefix) — `orcid:0000-0002-...`
    2. "family, given" lowercased — `name:smith, jane`
    3. raw name lowercased       — `name:jane smith`
    """
    if not author:
        return None
    orcid = (author.get("orcid") or "").strip()
    if orcid:
        orcid = orcid.replace("https://orcid.org/", "").replace("http://orcid.org/", "")
        return f"orcid:{orcid.lower()}"

    family = (author.get("family_name") or "").strip().lower()
    given = (author.get("given_name") or "").strip().lower()
    if family and given:
        return f"name:{family}, {given}"

    name = (author.get("name") or "").strip().lower()
    if name:
        return f"name:{name}"
    return None


def _author_keys_for_record(record: Dict[str, Any]) -> List[str]:
    meta = parse_metadata(record)
    keys = []
    for author in meta.authors:
        key = normalize_author_key(author.model_dump())
        if key:
            keys.append(key)
    return keys


def build_author_index(limit: int = 100000) -> Dict[str, Any]:
    """Scan published submissions and return an author_key -> [row...] index."""
    store = get_store()
    all_subs = store.list_all(limit=limit)

    author_to_rows: Dict[str, List[Dict[str, Any]]] = {}
    source_to_authors: Dict[str, List[str]] = {}

    for record in all_subs:
        if record.get("status") != "published":
            continue
        mdata = record.get("dataset_mdata") or {}
        if isinstance(mdata, dict) and mdata.get("latest") is False:
            continue

        keys = _author_keys_for_record(record)
        if not keys:
            continue

        source_id = record.get("source_id")
        if source_id:
            source_to_authors[source_id] = keys

        meta = parse_metadata(record)
        row = {
            "source_id": source_id,
            "version": record.get("version"),
            "title": meta.title,
            "authors": [a.name for a in meta.authors],
            "publication_year": meta.publication_year,
            "organization": record.get("organization"),
            "doi": record.get("doi") or record.get("dataset_doi"),
        }
        for key in keys:
            author_to_rows.setdefault(key, []).append(row)

    return {
        "author_to_rows": author_to_rows,
        "source_to_authors": source_to_authors,
        "counts": {
            "datasets": len(source_to_authors),
            "authors": len(author_to_rows),
        },
    }


def get_author_index(force_rebuild: bool = False) -> Dict[str, Any]:
    global _cached_author_index
    with _author_index_lock:
        if _cached_author_index is None or force_rebuild:
            _cached_author_index = build_author_index()
        return _cached_author_index


def invalidate_author_index() -> None:
    global _cached_author_index
    with _author_index_lock:
        _cached_author_index = None


def find_related_by_author(source_id: str, limit: int = 20) -> Dict[str, Any]:
    """Return datasets that share at least one author with `source_id`."""
    index = get_author_index()
    author_keys = index["source_to_authors"].get(source_id) or []
    if not author_keys:
        return {"source_id": source_id, "authors": [], "results": [], "total": 0}

    seen: set = {source_id}
    results: List[Dict[str, Any]] = []
    shared_by_source: Dict[str, int] = {}

    for key in author_keys:
        for row in index["author_to_rows"].get(key, []):
            sid = row.get("source_id")
            if not sid or sid == source_id:
                continue
            shared_by_source[sid] = shared_by_source.get(sid, 0) + 1
            if sid in seen:
                continue
            seen.add(sid)
            results.append(dict(row))

    for row in results:
        row["shared_authors"] = shared_by_source.get(row.get("source_id"), 0)

    results.sort(key=lambda r: (-r["shared_authors"], r.get("title") or ""))

    return {
        "source_id": source_id,
        "authors": author_keys,
        "total": len(results),
        "results": results[:limit],
    }


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def search_all(
    query: str,
    include_datasets: bool = True,
    include_streams: bool = True,
    limit: int = 20,
    offset: int = 0,
    filters: Optional[Dict[str, list]] = None,
    auth: Optional[Any] = None,
) -> Dict[str, Any]:
    """Search across datasets and streams, with faceted results."""
    results = []
    facets: Dict[str, Any] = {}
    total = 0

    if include_datasets:
        ds = search_datasets(query, limit=limit, offset=offset, filters=filters)
        results.extend(ds["results"])
        facets = ds.get("facets", {})
        total += ds.get("total", 0)

    if include_streams:
        streams = search_streams(query, limit=limit, auth=auth)
        results.extend(streams)

    results.sort(key=lambda x: x.get("score", 0), reverse=True)

    return {
        "query": query,
        "total": total,
        "offset": offset,
        "results": results[:limit],
        "facets": facets,
    }
