"""Dataset preview cards for MDF v2.

Provides compact, human-readable summaries of datasets for quick preview
without downloading the full dataset or metadata.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from v2.request import parse_authorizer
from v2.responses import bad_request, ok
from v2.store import get_store


def _parse_size(total_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if total_bytes is None:
        return "Unknown"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if total_bytes < 1024:
            return f"{total_bytes:.1f} {unit}" if unit != "B" else f"{total_bytes} {unit}"
        total_bytes /= 1024
    return f"{total_bytes:.1f} PB"


def _extract_file_types(data_sources: List[str]) -> List[str]:
    """Extract file type hints from data sources."""
    extensions = set()
    for source in (data_sources or []):
        if "." in source:
            ext = source.rsplit(".", 1)[-1].lower()
            if len(ext) <= 5 and ext.isalnum():
                extensions.add(ext)
    return sorted(extensions) if extensions else ["unknown"]


def _format_authors(creators: List[Dict]) -> List[str]:
    """Format creator list as readable author names."""
    authors = []
    for c in (creators or []):
        if isinstance(c, dict):
            name = c.get("creatorName") or f"{c.get('givenName', '')} {c.get('familyName', '')}".strip()
            if name:
                authors.append(name)
        elif isinstance(c, str):
            authors.append(c)
    return authors


def _extract_keywords(dc: Dict) -> List[str]:
    """Extract keywords from subjects."""
    keywords = []
    for subj in (dc.get("subjects") or []):
        if isinstance(subj, dict):
            keywords.append(subj.get("subject", ""))
        elif isinstance(subj, str):
            keywords.append(subj)
    return [k for k in keywords if k][:10]  # Limit to 10


def _extract_methods(mdf: Dict) -> List[str]:
    """Extract experimental/computational methods."""
    methods = []
    if mdf.get("instruments"):
        instr = mdf["instruments"]
        if isinstance(instr, list):
            methods.extend(instr)
        else:
            methods.append(str(instr))
    if mdf.get("facility"):
        methods.append(mdf["facility"])
    return methods


def build_dataset_card(record: Dict[str, Any]) -> Dict[str, Any]:
    """Build a preview card from a submission record.

    Returns a compact summary suitable for display in search results,
    dashboards, or quick previews.
    """
    # Parse stored metadata
    mdata_str = record.get("dataset_mdata") or "{}"
    try:
        mdata = json.loads(mdata_str) if isinstance(mdata_str, str) else (mdata_str or {})
    except Exception:
        mdata = {}

    if mdata is None:
        mdata = {}

    dc = mdata.get("dc") or {}
    mdf = mdata.get("mdf") or {}

    # Extract title
    titles = dc.get("titles") or []
    title = "Untitled Dataset"
    if titles:
        if isinstance(titles[0], dict):
            title = titles[0].get("title", title)
        else:
            title = str(titles[0])

    # Extract description (truncated)
    descriptions = dc.get("descriptions") or []
    description = ""
    if descriptions:
        if isinstance(descriptions[0], dict):
            description = descriptions[0].get("description", "")
        else:
            description = str(descriptions[0])
    if len(description) > 300:
        description = description[:297] + "..."

    # Build the card
    card = {
        "source_id": record.get("source_id"),
        "version": record.get("version"),
        "title": title,
        "authors": _format_authors(dc.get("creators")),
        "description": description,
        "keywords": _extract_keywords(dc),
        "publisher": dc.get("publisher"),
        "publication_year": dc.get("publicationYear"),
        "organization": record.get("organization") or mdf.get("organization"),
        "lab_id": mdf.get("lab_id"),
        "methods": _extract_methods(mdf),
        "status": record.get("status"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        # Quick stats
        "stats": {
            "file_types": _extract_file_types(record.get("data_sources")),
            "data_sources_count": len(record.get("data_sources") or []),
            "file_count": record.get("file_count", 0),
            "total_bytes": record.get("total_bytes", 0),
            "size_human": _parse_size(record.get("total_bytes", 0)),
        },
        # Links
        "links": {
            "self": f"/status/{record.get('source_id')}",
            "citation": f"/citation/{record.get('source_id')}",
        }
    }

    # Add DOI if available
    if mdf.get("doi"):
        card["doi"] = mdf["doi"]
        card["links"]["doi"] = f"https://doi.org/{mdf['doi']}"

    return card


def build_stream_card(stream: Dict[str, Any]) -> Dict[str, Any]:
    """Build a preview card for a stream."""
    metadata = stream.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}
    if metadata is None:
        metadata = {}

    return {
        "stream_id": stream.get("stream_id"),
        "title": stream.get("title"),
        "lab_id": stream.get("lab_id"),
        "organization": stream.get("organization"),
        "status": stream.get("status"),
        "created_at": stream.get("created_at"),
        "updated_at": stream.get("updated_at"),
        "stats": {
            "file_count": stream.get("file_count", 0),
            "total_size": _parse_size(stream.get("total_bytes", 0)),
            "total_bytes": stream.get("total_bytes", 0),
        },
        "metadata": {
            "instrument": metadata.get("instrument"),
            "facility": metadata.get("facility"),
            "operator": metadata.get("operator"),
            "run_id": metadata.get("run_id"),
        },
        "links": {
            "self": f"/stream/{stream.get('stream_id')}",
            "files": f"/stream/{stream.get('stream_id')}/files",
        }
    }


def lambda_handler(event, context):
    """Get a dataset preview card.

    GET /card/{source_id}?version=1.0
    """
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    source_id = path_params.get("source_id")
    version = query_params.get("version")

    if not source_id:
        return bad_request("source_id is required")

    store = get_store()
    record = store.get(source_id, version=version)

    if not record:
        return bad_request(f"Dataset not found: {source_id}")

    card = build_dataset_card(record)

    return ok({
        "success": True,
        "card": card,
    })
