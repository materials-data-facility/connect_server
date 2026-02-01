"""Stream close and publish handler for MDF v2.

Handles closing streams and optionally publishing them with DOI minting.

Close modes:
1. close_only: Just mark as closed (no DOI)
2. publish: Close + mint DOI + index to Globus Search
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from v2.request import parse_authorizer, parse_json_body
from v2.responses import bad_request, ok, server_error
from v2.stream_store import get_stream_store


def lambda_handler(event, context):
    """Close a stream and optionally publish with DOI.

    POST /stream/{stream_id}/close
    {
        "mint_doi": true,           # Mint DOI on close (default: false)
        "title": "Final Title",     # Override title for publication
        "description": "...",       # Add description
        "authors": [...],           # Override authors
        "keywords": [...],          # Add keywords
        "license": "CC-BY-4.0",     # Specify license
    }
    """
    auth = parse_authorizer(event)
    user_id = auth.get("user_id")

    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    payload = payload or {}

    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id") or payload.get("stream_id")
    if not stream_id:
        return bad_request("stream_id is required")

    store = get_stream_store()
    stream = store.get_stream(stream_id)

    if not stream:
        return bad_request("Stream not found")

    if stream.get("status") == "closed":
        return bad_request("Stream is already closed")

    # Check if DOI minting requested
    mint_doi = payload.get("mint_doi", False)

    # Close the stream first
    record = store.close_stream(stream_id)
    if not record:
        return server_error("Failed to close stream")

    result = {
        "success": True,
        "stream_id": stream_id,
        "status": "closed",
        "stream": record,
    }

    # Mint DOI if requested
    if mint_doi:
        doi_result = _mint_doi_for_stream(stream, payload)
        result["doi"] = doi_result

        # Update stream with DOI
        if doi_result.get("success"):
            store.update_stream_metadata(stream_id, {
                "doi": doi_result.get("doi"),
                "published_at": datetime.now(timezone.utc).isoformat(),
            })

        # Index to Globus Search (if configured)
        # search_result = _index_to_search(stream, doi_result.get("doi"))
        # result["search"] = search_result

    return ok(result)


def _mint_doi_for_stream(
    stream: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    """Mint a DOI for a stream."""
    from v2.datacite import get_datacite_client

    try:
        client = get_datacite_client()

        # Build metadata from stream + overrides
        metadata = {
            "title": overrides.get("title") or stream.get("title") or "Untitled Dataset",
            "description": overrides.get("description") or _extract_description(stream),
            "authors": overrides.get("authors") or _extract_authors(stream),
            "keywords": overrides.get("keywords") or _extract_keywords(stream),
            "publisher": "Materials Data Facility",
            "publication_year": datetime.now().year,
            "version": "1.0",
        }

        if overrides.get("license"):
            metadata["license"] = overrides["license"]

        # Generate source_id from stream_id
        source_id = stream["stream_id"].replace("stream-", "")

        result = client.mint_doi(
            source_id=source_id,
            metadata=metadata,
            publish=True,
        )

        client.close()
        return result

    except Exception as e:
        return {"success": False, "error": str(e)}


def _extract_description(stream: Dict[str, Any]) -> str:
    """Extract description from stream metadata."""
    metadata = stream.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}

    return metadata.get("description", "")


def _extract_authors(stream: Dict[str, Any]) -> list:
    """Extract authors from stream metadata."""
    metadata = stream.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}

    return metadata.get("authors", [])


def _extract_keywords(stream: Dict[str, Any]) -> list:
    """Extract keywords from stream metadata."""
    metadata = stream.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}

    return metadata.get("keywords", [])
