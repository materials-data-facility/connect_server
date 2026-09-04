import json
import os
from datetime import datetime, timezone
from typing import Dict, Optional


DEFAULT_PORTAL_URL = "https://materialsdatafacility.org"


def landing_url(source_id: str, version: Optional[str] = None) -> str:
    """Return the persistent portal landing URL for a concept or version DOI."""
    from v2 import config

    portal_url = (
        os.environ.get("PORTAL_URL")
        or getattr(config, "PORTAL_URL", None)
        or DEFAULT_PORTAL_URL
    ).rstrip("/")
    base = f"{portal_url}/detail/{source_id}"
    return f"{base}?version={version}" if version is not None else base


def mint_doi_for_stream(stream: Dict, overrides: Dict) -> Dict:
    from v2.datacite import get_datacite_client

    try:
        client = get_datacite_client()

        metadata_field = stream.get("metadata") or {}
        if isinstance(metadata_field, str):
            try:
                metadata_field = json.loads(metadata_field)
            except Exception:
                metadata_field = {}

        metadata = {
            "title": overrides.get("title") or stream.get("title") or "Untitled Dataset",
            "description": overrides.get("description") or metadata_field.get("description", ""),
            "authors": overrides.get("authors") or metadata_field.get("authors", []),
            "keywords": overrides.get("keywords") or metadata_field.get("keywords", []),
            "publisher": "Materials Data Facility",
            "publication_year": datetime.now(timezone.utc).year,
            "version": "1.0",
        }

        if overrides.get("license"):
            metadata["license"] = overrides["license"]

        source_id = stream["stream_id"].replace("stream-", "")

        result = client.mint_doi(
            source_id=source_id,
            metadata=metadata,
            url=landing_url(source_id),
            publish=True,
        )
        client.close()
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
