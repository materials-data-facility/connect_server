"""Globus Search client for MDF v2.

Provides search ingest and query capabilities via Globus Search indexes.
Falls back to MockGlobusSearchClient when credentials or indexes are not configured.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MDF_DETAIL_BASE = "https://materialsdatafacility.org/detail"


class GlobusSearchClient:
    """Wraps globus_sdk.SearchClient for MDF v2 search operations."""

    def __init__(self, index_id: str, test_mode: bool = False):
        self.index_id = index_id
        self.test_mode = test_mode
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client

        import globus_sdk

        client_id = os.environ.get("GLOBUS_CLIENT_ID")
        client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")

        if not client_id or not client_secret:
            raise RuntimeError("GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET required for Globus Search")

        confidential_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
        token_response = confidential_client.oauth2_client_credentials_tokens()
        search_token = token_response.by_resource_server.get("search.api.globus.org", {})
        access_token = search_token.get("access_token") if isinstance(search_token, dict) else search_token.access_token

        authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
        self._client = globus_sdk.SearchClient(authorizer=authorizer)
        return self._client

    def build_gmeta_entry(self, submission: Dict[str, Any]) -> Dict[str, Any]:
        """Build a GMetaEntry from a submission record."""
        from v2.metadata import parse_metadata

        source_id = submission.get("source_id", "unknown")
        version = submission.get("version", "1.0")
        meta = parse_metadata(submission)

        subject = f"{MDF_DETAIL_BASE}/{source_id}"

        acl = meta.acl or ["public"]
        visible_to = ["public"] if "public" in acl else [f"urn:globus:auth:identity:{a}" for a in acl]

        # Extract data location from first data_source
        data_sources = meta.data_sources or []
        location = data_sources[0] if data_sources else None

        content = {
            "mdf": {
                "source_id": source_id,
                "source_name": source_id.rsplit("-", 1)[0] if "-" in source_id else source_id,
                "version": version,
                "organization": submission.get("organization", ""),
                "acl": acl,
                "ingest_date": submission.get("created_at", datetime.now(timezone.utc).isoformat()),
            },
            "dc": {
                "title": meta.title,
                "creators": [{"name": a.name} for a in meta.authors],
                "publisher": meta.publisher,
                "year": meta.publication_year or datetime.now().year,
                "description": meta.description or "",
                "subjects": meta.keywords,
                "license": meta.license or "",
            },
            "data": {
                "location": location,
                "size_bytes": submission.get("total_bytes"),
                "file_count": submission.get("file_count"),
            },
        }

        doi = submission.get("doi")
        if doi:
            content["dc"]["doi"] = doi

        return {
            "subject": subject,
            "visible_to": visible_to,
            "content": content,
        }

    def ingest(self, submission: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest a submission into the Globus Search index."""
        client = self._get_client()
        entry = self.build_gmeta_entry(submission)

        ingest_doc = {
            "ingest_type": "GMetaEntry",
            "ingest_data": entry,
        }

        try:
            result = client.ingest(self.index_id, ingest_doc)
            return {
                "success": True,
                "task_id": getattr(result, "data", {}).get("task_id") if hasattr(result, "data") else str(result),
            }
        except Exception as exc:
            logger.exception("Globus Search ingest failed for %s", submission.get("source_id"))
            return {"success": False, "error": str(exc)}

    def delete_entry(self, source_id: str) -> Dict[str, Any]:
        """Delete a subject entry from the index."""
        client = self._get_client()
        subject = f"{MDF_DETAIL_BASE}/{source_id}"

        try:
            client.delete_entry(self.index_id, subject)
            return {"success": True, "source_id": source_id}
        except Exception as exc:
            logger.exception("Globus Search delete failed for %s", source_id)
            return {"success": False, "error": str(exc)}

    def search(self, query: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """Search the Globus Search index."""
        client = self._get_client()

        try:
            result = client.search(self.index_id, query, limit=limit, offset=offset)
            data = result.data if hasattr(result, "data") else result
            return {
                "success": True,
                "total": data.get("total", 0),
                "results": _format_globus_search_results(data),
            }
        except Exception as exc:
            logger.exception("Globus Search query failed")
            return {"success": False, "error": str(exc), "total": 0, "results": []}


class MockGlobusSearchClient:
    """In-memory mock for Globus Search. Used when USE_MOCK_SEARCH=true."""

    def __init__(self, index_id: str = "mock-index", test_mode: bool = False):
        self.index_id = index_id
        self.test_mode = test_mode
        self._entries: Dict[str, Dict[str, Any]] = {}

    def build_gmeta_entry(self, submission: Dict[str, Any]) -> Dict[str, Any]:
        # Re-use the real implementation's logic
        real = GlobusSearchClient.__new__(GlobusSearchClient)
        return real.build_gmeta_entry(submission)

    def ingest(self, submission: Dict[str, Any]) -> Dict[str, Any]:
        entry = self.build_gmeta_entry(submission)
        self._entries[entry["subject"]] = entry
        return {"success": True, "mock": True, "subject": entry["subject"]}

    def delete_entry(self, source_id: str) -> Dict[str, Any]:
        subject = f"{MDF_DETAIL_BASE}/{source_id}"
        self._entries.pop(subject, None)
        return {"success": True, "mock": True, "source_id": source_id}

    def search(self, query: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        # Simple text match over stored entries
        query_lower = query.lower()
        matches = []
        for subject, entry in self._entries.items():
            content = entry.get("content", {})
            text = " ".join([
                content.get("dc", {}).get("title", ""),
                content.get("dc", {}).get("description", ""),
                " ".join(content.get("dc", {}).get("subjects", [])),
                content.get("mdf", {}).get("source_id", ""),
            ]).lower()
            if query_lower in text:
                matches.append(entry)

        paginated = matches[offset:offset + limit]
        results = []
        for entry in paginated:
            content = entry.get("content", {})
            results.append({
                "type": "dataset",
                "source_id": content.get("mdf", {}).get("source_id"),
                "title": content.get("dc", {}).get("title"),
                "authors": [c.get("name", "") for c in content.get("dc", {}).get("creators", [])],
                "score": 1.0,
            })

        return {"success": True, "total": len(matches), "results": results, "mock": True}


def _format_globus_search_results(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize Globus Search response into the MDF result format."""
    results = []
    for gmeta in data.get("gmeta", []):
        for entry_content in gmeta.get("content", []):
            content = entry_content if isinstance(entry_content, dict) else {}
            mdf = content.get("mdf", {})
            dc = content.get("dc", {})
            results.append({
                "type": "dataset",
                "source_id": mdf.get("source_id"),
                "version": mdf.get("version"),
                "title": dc.get("title"),
                "authors": [c.get("name", "") for c in dc.get("creators", [])],
                "status": "published",
                "score": gmeta.get("score", 0),
            })
    return results


# Singleton for mock client to persist in-memory state within a Lambda invocation
_mock_client: Optional[MockGlobusSearchClient] = None


def get_search_client(test_mode: bool = False) -> Any:
    """Factory: returns GlobusSearchClient or MockGlobusSearchClient."""
    global _mock_client

    use_mock = os.environ.get("USE_MOCK_SEARCH", "true").lower() == "true"

    if use_mock:
        if _mock_client is None:
            _mock_client = MockGlobusSearchClient(test_mode=test_mode)
        return _mock_client

    if test_mode:
        index_id = os.environ.get("TEST_SEARCH_INDEX_UUID", "not-configured")
    else:
        index_id = os.environ.get("SEARCH_INDEX_UUID", "not-configured")

    if index_id == "not-configured":
        logger.warning("Search index UUID not configured, falling back to mock")
        if _mock_client is None:
            _mock_client = MockGlobusSearchClient(test_mode=test_mode)
        return _mock_client

    return GlobusSearchClient(index_id=index_id, test_mode=test_mode)
