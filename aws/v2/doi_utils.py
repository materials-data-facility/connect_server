import json
from datetime import datetime, timezone
from typing import Dict


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

        result = client.mint_doi(source_id=source_id, metadata=metadata, publish=True)
        client.close()
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
