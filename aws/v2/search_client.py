"""Globus Search client for MDF v2.

Provides search ingest and query capabilities via Globus Search indexes.
Falls back to MockGlobusSearchClient when credentials or indexes are not configured.
"""

import logging
import os
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MDF_DETAIL_BASE = "https://materialsdatafacility.org/detail"

DEFAULT_FACETS = [
    {"name": "Year",         "field_name": "dc.year",          "type": "terms", "size": 20},
    {"name": "Organization", "field_name": "mdf.organization", "type": "terms", "size": 20},
    {"name": "Authors",      "field_name": "dc.creators.name", "type": "terms", "size": 20},
    {"name": "Keywords",     "field_name": "dc.subjects",      "type": "terms", "size": 20},
    {"name": "Domains",      "field_name": "mdf.domains",      "type": "terms", "size": 20},
]


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
        token_response = confidential_client.oauth2_client_credentials_tokens(
            requested_scopes="urn:globus:auth:scope:search.api.globus.org:all"
        )
        search_token = token_response.by_resource_server.get("search.api.globus.org", {})
        access_token = search_token.get("access_token") if isinstance(search_token, dict) else getattr(search_token, "access_token", None)
        if not access_token:
            raise RuntimeError(
                "Failed to obtain Globus Search access token. "
                "Ensure the app has the 'urn:globus:auth:scope:search.api.globus.org:all' scope configured."
            )

        authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
        self._client = globus_sdk.SearchClient(authorizer=authorizer)
        return self._client

    def build_gmeta_entry(
        self, submission: Dict[str, Any], version_count: Optional[int] = None,
    ) -> Dict[str, Any]:
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

        mdf_block: Dict[str, Any] = {
            "source_id": source_id,
            "source_name": source_id.rsplit("-", 1)[0] if "-" in source_id else source_id,
            "version": version,
            "organization": submission.get("organization", ""),
            "acl": acl,
            "ingest_date": submission.get("created_at", datetime.now(timezone.utc).isoformat()),
        }

        mdf_block["domains"] = meta.domains

        if meta.external:
            mdf_block["external_source"] = meta.external.source
            if meta.external.doi:
                mdf_block["external_doi"] = meta.external.doi
            if meta.external.url:
                mdf_block["external_url"] = meta.external.url

        dataset_doi = submission.get("dataset_doi")
        if dataset_doi:
            mdf_block["dataset_doi"] = dataset_doi
        if version_count is not None:
            mdf_block["version_count"] = version_count

        # Versioning fields
        mdf_block["latest"] = meta.latest
        if meta.root_version:
            mdf_block["root_version"] = meta.root_version
        if meta.previous_version:
            mdf_block["previous_version"] = meta.previous_version
        if meta.version:
            mdf_block["version"] = meta.version

        # Download URL
        if meta.download_url:
            mdf_block["download_url"] = meta.download_url

        content = {
            "mdf": mdf_block,
            "dc": {
                "title": meta.title,
                "creators": [{"name": a.name} for a in meta.authors],
                "publisher": meta.publisher,
                "year": meta.publication_year or datetime.now().year,
                "description": meta.description or "",
                "subjects": meta.keywords,
                "license": meta.license.identifier or meta.license.name if meta.license else "",
            },
            "data": {
                "location": location,
                "size_bytes": submission.get("total_bytes"),
                "file_count": submission.get("file_count"),
            },
        }

        # dc.doi = version-specific DOI if present, otherwise dataset DOI
        doi = submission.get("doi") or submission.get("dataset_doi")
        if doi:
            content["dc"]["doi"] = doi

        return {
            "subject": subject,
            "visible_to": visible_to,
            "content": content,
        }

    def ingest(self, submission: Dict[str, Any], version_count: Optional[int] = None) -> Dict[str, Any]:
        """Ingest a single submission into the Globus Search index."""
        client = self._get_client()
        entry = self.build_gmeta_entry(submission, version_count=version_count)

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

    def batch_ingest(
        self, submissions: List[Dict[str, Any]], batch_size: int = 100,
    ) -> Dict[str, Any]:
        """Ingest multiple submissions using GMetaList batches.

        Batches submissions into groups of batch_size and submits each as a
        single GMetaList request (one task_id per batch). Much faster than
        individual ingest() calls for bulk loading — 10 req/s rate limit and
        10MB per request apply; batch_size=100 stays well within the 10MB cap.

        Returns a summary dict with counts and any per-batch errors.
        """
        client = self._get_client()
        total = len(submissions)
        ingested = 0
        errors = []
        task_ids = []

        for batch_start in range(0, total, batch_size):
            batch = submissions[batch_start:batch_start + batch_size]
            gmeta = []
            for sub in batch:
                try:
                    gmeta.append(self.build_gmeta_entry(sub))
                except Exception as exc:
                    errors.append({"source_id": sub.get("source_id"), "error": str(exc)})

            if not gmeta:
                continue

            ingest_doc = {
                "ingest_type": "GMetaList",
                "ingest_data": {"gmeta": gmeta},
            }

            try:
                result = client.ingest(self.index_id, ingest_doc)
                data = result.data if hasattr(result, "data") else {}
                task_id = data.get("task_id")
                if task_id:
                    task_ids.append(task_id)
                ingested += len(gmeta)
            except Exception as exc:
                logger.exception(
                    "Globus Search batch ingest failed (batch %d-%d)",
                    batch_start, batch_start + len(batch) - 1,
                )
                for sub in batch:
                    errors.append({"source_id": sub.get("source_id"), "error": str(exc)})

        return {
            "success": len(errors) == 0,
            "total": total,
            "ingested": ingested,
            "errors": errors,
            "task_ids": task_ids,
        }

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

    def _get_read_client(self):
        """Return an unauthenticated SearchClient for public index reads.

        The MDF Search index is public, so queries do not require credentials.
        Only ingest/delete operations use the authenticated client from _get_client().
        """
        import globus_sdk
        return globus_sdk.SearchClient()

    def search(self, query: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """Search the Globus Search index."""
        client = self._get_read_client()

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

    def faceted_search(
        self, query: str, limit: int = 20, offset: int = 0, filters: Optional[Dict[str, List]] = None,
    ) -> Dict[str, Any]:
        """Search with facets and optional filters.

        filters: dict mapping facet field_name → list of selected values
                 e.g. {"mdf.organization": ["MDF Open"], "dc.year": [2024, 2025]}
        """
        from globus_sdk import SearchQuery

        client = self._get_read_client()
        sq = SearchQuery(query)

        for facet in DEFAULT_FACETS:
            sq.add_facet(**facet)

        if filters:
            for field_name, values in filters.items():
                sq.add_filter(field_name, values, type="match_any")

        sq["limit"] = limit
        sq["offset"] = offset

        try:
            result = client.post_search(self.index_id, sq)
            data = result.data if hasattr(result, "data") else result
            return {
                "success": True,
                "total": data.get("total", 0),
                "results": _format_globus_search_results(data),
                "facets": _format_facet_results(data.get("facet_results", [])),
            }
        except Exception as exc:
            logger.exception("Globus Search faceted query failed")
            return {"success": False, "error": str(exc), "total": 0, "results": [], "facets": {}}


class MockGlobusSearchClient:
    """In-memory mock for Globus Search. Used when USE_MOCK_SEARCH=true."""

    def __init__(self, index_id: str = "mock-index", test_mode: bool = False):
        self.index_id = index_id
        self.test_mode = test_mode
        self._entries: Dict[str, Dict[str, Any]] = {}

    def build_gmeta_entry(
        self, submission: Dict[str, Any], version_count: Optional[int] = None,
    ) -> Dict[str, Any]:
        # Re-use the real implementation's logic
        real = GlobusSearchClient.__new__(GlobusSearchClient)
        return real.build_gmeta_entry(submission, version_count=version_count)

    def ingest(self, submission: Dict[str, Any], version_count: Optional[int] = None) -> Dict[str, Any]:
        entry = self.build_gmeta_entry(submission, version_count=version_count)
        self._entries[entry["subject"]] = entry
        return {"success": True, "mock": True, "subject": entry["subject"]}

    def batch_ingest(
        self, submissions: List[Dict[str, Any]], batch_size: int = 100,
    ) -> Dict[str, Any]:
        errors = []
        for sub in submissions:
            result = self.ingest(sub)
            if not result.get("success"):
                errors.append({"source_id": sub.get("source_id"), "error": result.get("error")})
        return {
            "success": len(errors) == 0,
            "total": len(submissions),
            "ingested": len(submissions) - len(errors),
            "errors": errors,
            "task_ids": [],
            "mock": True,
        }

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
            dc = content.get("dc", {})
            mdf = content.get("mdf", {})
            data_block = content.get("data", {})
            description = dc.get("description", "") or ""
            results.append({
                "type": "dataset",
                "source_id": mdf.get("source_id"),
                "title": dc.get("title"),
                "authors": [c.get("name", "") for c in dc.get("creators", [])],
                "keywords": dc.get("subjects", []),
                "description": description[:300] if len(description) > 300 else description,
                "publication_year": dc.get("year"),
                "organization": mdf.get("organization"),
                "domains": mdf.get("domains") or [],
                "doi": dc.get("doi") or mdf.get("dataset_doi"),
                "license": dc.get("license") or None,
                "size_bytes": data_block.get("size_bytes"),
                "file_count": data_block.get("file_count"),
                "score": 1.0,
            })

        return {"success": True, "total": len(matches), "results": results, "mock": True}

    def faceted_search(
        self, query: str, limit: int = 20, offset: int = 0, filters: Optional[Dict[str, List]] = None,
    ) -> Dict[str, Any]:
        """Faceted search over in-memory entries with filter support."""
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
            if query_lower == "*" or query_lower in text:
                if filters and not self._matches_filters(content, filters):
                    continue
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

        return {
            "success": True,
            "total": len(matches),
            "results": results,
            "facets": self._compute_facets(matches),
            "mock": True,
        }

    def _matches_filters(self, content: Dict[str, Any], filters: Dict[str, List]) -> bool:
        """Check if a content entry matches all active filters."""
        field_map = {
            "dc.year": lambda c: [c.get("dc", {}).get("year")],
            "mdf.organization": lambda c: [c.get("mdf", {}).get("organization")],
            "dc.creators.name": lambda c: [cr.get("name", "") for cr in c.get("dc", {}).get("creators", [])],
            "dc.subjects": lambda c: c.get("dc", {}).get("subjects", []),
            "mdf.domains": lambda c: c.get("mdf", {}).get("domains", []),
        }
        for field_name, values in filters.items():
            extractor = field_map.get(field_name)
            if not extractor:
                continue
            entry_values = [str(v) for v in extractor(content) if v is not None]
            filter_values = [str(v) for v in values]
            if not any(ev in filter_values for ev in entry_values):
                return False
        return True

    def _compute_facets(self, entries: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Compute facet counts from a list of matched entries."""
        counters: Dict[str, Counter] = {
            "Year": Counter(),
            "Organization": Counter(),
            "Authors": Counter(),
            "Keywords": Counter(),
            "Domains": Counter(),
        }
        for entry in entries:
            content = entry.get("content", {})
            dc = content.get("dc", {})
            mdf = content.get("mdf", {})

            year = dc.get("year")
            if year is not None:
                counters["Year"][str(year)] += 1
            org = mdf.get("organization")
            if org:
                counters["Organization"][org] += 1
            for creator in dc.get("creators", []):
                name = creator.get("name")
                if name:
                    counters["Authors"][name] += 1
            for kw in dc.get("subjects", []):
                if kw:
                    counters["Keywords"][kw] += 1
            for domain in mdf.get("domains", []):
                if domain:
                    counters["Domains"][domain] += 1

        facets = {}
        for name, counter in counters.items():
            buckets = [{"value": val, "count": count} for val, count in counter.most_common(20)]
            facets[name] = buckets
        return facets


def _format_globus_search_results(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize Globus Search response into the MDF result format.

    Handles both response shapes:
    - POST search (post_search): gmeta[i].entries[j].content  (dict)
    - GET  search (search):      gmeta[i].content[j]          (dict in a list)
    """
    results = []
    for gmeta in data.get("gmeta", []):
        # POST search wraps entries; GET search uses content list directly
        if gmeta.get("entries") is not None:
            contents = [e.get("content", {}) for e in gmeta["entries"]]
        else:
            raw = gmeta.get("content", [])
            contents = raw if isinstance(raw, list) else [raw]

        for content in contents:
            if not isinstance(content, dict):
                continue
            mdf = content.get("mdf", {})
            dc = content.get("dc", {})
            data_block = content.get("data", {})
            description = dc.get("description", "") or ""
            result_entry = {
                "type": "dataset",
                "source_id": mdf.get("source_id"),
                "version": mdf.get("version"),
                "title": dc.get("title"),
                "authors": [c.get("name", "") for c in dc.get("creators", [])],
                "keywords": dc.get("subjects", []),
                "description": description[:300] if len(description) > 300 else description,
                "publication_year": dc.get("year"),
                "organization": mdf.get("organization"),
                "domains": mdf.get("domains") or [],
                "doi": dc.get("doi") or mdf.get("dataset_doi"),
                "license": dc.get("license") or None,
                "size_bytes": data_block.get("size_bytes"),
                "file_count": data_block.get("file_count"),
                "status": "published",
                "score": gmeta.get("score", 0),
                "latest": mdf.get("latest", True),
            }
            if mdf.get("root_version"):
                result_entry["root_version"] = mdf["root_version"]
            if mdf.get("download_url"):
                result_entry["download_url"] = mdf["download_url"]
            results.append(result_entry)
    return results


def _format_facet_results(facet_results: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Normalize Globus facet_results into frontend-friendly format."""
    facets = {}
    for fr in facet_results:
        name = fr.get("name", "")
        buckets = [
            {"value": b.get("value"), "count": b.get("count", 0)}
            for b in fr.get("buckets", [])
            if b.get("count", 0) > 0
        ]
        facets[name] = buckets
    return facets


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
