"""DataCite DOI minting for MDF v2.

Handles DOI registration with DataCite for published datasets.

Configuration:
    DATACITE_API_URL: DataCite API endpoint (default: test API)
    DATACITE_USERNAME: Repository ID (e.g., "MDF.MDF")
    DATACITE_PASSWORD: Repository password
    DATACITE_PREFIX: DOI prefix (e.g., "10.18126")
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import httpx


# DataCite API endpoints
DATACITE_TEST_API = "https://api.test.datacite.org"
DATACITE_PROD_API = "https://api.datacite.org"


class DataCiteClient:
    """Client for DataCite DOI registration."""

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        prefix: Optional[str] = None,
        api_url: Optional[str] = None,
        test_mode: bool = True,
    ):
        """Initialize DataCite client.

        Args:
            username: DataCite repository ID
            password: DataCite password
            prefix: DOI prefix (e.g., "10.18126")
            api_url: API endpoint (defaults based on test_mode)
            test_mode: Use test API (default True for safety)
        """
        self.username = username or os.environ.get("DATACITE_USERNAME")
        self.password = password or os.environ.get("DATACITE_PASSWORD")
        self.prefix = prefix or os.environ.get("DATACITE_PREFIX", "10.23677")

        if api_url:
            self.api_url = api_url
        else:
            self.api_url = os.environ.get(
                "DATACITE_API_URL",
                DATACITE_TEST_API if test_mode else DATACITE_PROD_API
            )

        self._client = httpx.Client(
            timeout=30.0,
            auth=(self.username, self.password) if self.username and self.password else None,
        )

    def mint_doi(
        self,
        source_id: str,
        metadata: Dict[str, Any],
        url: Optional[str] = None,
        publish: bool = True,
        doi_suffix: Optional[str] = None,
        related_identifiers: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Mint a new DOI for a dataset.

        Args:
            source_id: MDF source ID (used to generate DOI suffix)
            metadata: DataCite metadata (titles, creators, etc.)
            url: Landing page URL (defaults to MDF URL)
            publish: Whether to publish immediately (vs draft)
            doi_suffix: Override DOI suffix (e.g. for version-specific DOIs)
            related_identifiers: DataCite relatedIdentifiers list

        Returns:
            Dict with doi, url, state
        """
        # Generate DOI
        suffix = doi_suffix or self._generate_suffix(source_id)
        doi = f"{self.prefix}/{suffix}"

        # Default landing page URL
        if not url:
            url = f"https://materialsdatafacility.org/detail/{source_id}"

        # Build DataCite payload
        payload = self._build_payload(doi, url, metadata, publish, related_identifiers)

        # Check if DOI already exists
        existing = self.get_doi(doi)
        if existing and existing.get("data"):
            # Update existing DOI
            return self._update_doi(doi, payload)
        else:
            # Create new DOI
            return self._create_doi(payload)

    def update_metadata(
        self,
        doi: str,
        metadata: Dict[str, Any],
        url: Optional[str] = None,
        related_identifiers: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Update metadata on an existing DOI without minting a new one.

        Used when a new version inherits the dataset DOI and we want to
        update the DataCite record to reflect the latest version's metadata.
        """
        payload = self._build_payload(
            doi, url or "", metadata, publish=True, related_identifiers=related_identifiers,
        )
        # Remove url from payload if not provided (don't overwrite)
        if not url:
            payload["data"]["attributes"].pop("url", None)
        return self._update_doi(doi, payload)

    def get_doi(self, doi: str) -> Optional[Dict[str, Any]]:
        """Get DOI metadata."""
        try:
            response = self._client.get(f"{self.api_url}/dois/{quote(doi, safe='')}")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None

    def _create_doi(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new DOI."""
        response = self._client.post(
            f"{self.api_url}/dois",
            json=payload,
            headers={"Content-Type": "application/vnd.api+json"},
        )

        if response.status_code in (200, 201):
            data = response.json()
            return {
                "success": True,
                "doi": data["data"]["id"],
                "url": data["data"]["attributes"].get("url"),
                "state": data["data"]["attributes"].get("state"),
            }
        else:
            return {
                "success": False,
                "error": response.text,
                "status_code": response.status_code,
            }

    def _update_doi(self, doi: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing DOI."""
        response = self._client.put(
            f"{self.api_url}/dois/{quote(doi, safe='')}",
            json=payload,
            headers={"Content-Type": "application/vnd.api+json"},
        )

        if response.status_code in (200, 201):
            data = response.json()
            return {
                "success": True,
                "doi": data["data"]["id"],
                "url": data["data"]["attributes"].get("url"),
                "state": data["data"]["attributes"].get("state"),
                "updated": True,
            }
        else:
            return {
                "success": False,
                "error": response.text,
                "status_code": response.status_code,
            }

    def _generate_suffix(self, source_id: str) -> str:
        """Generate DOI suffix from source ID."""
        # Clean source_id for DOI
        suffix = source_id.replace("_", "-").lower()
        # Ensure it's valid for DOI
        valid_chars = "abcdefghijklmnopqrstuvwxyz0123456789-."
        suffix = "".join(c if c in valid_chars else "-" for c in suffix)
        return suffix

    def _build_payload(
        self,
        doi: str,
        url: str,
        metadata: Dict[str, Any],
        publish: bool = True,
        related_identifiers: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Build DataCite API payload."""
        # Extract metadata fields
        titles = metadata.get("titles", [])
        if not titles:
            title = metadata.get("title", "Untitled Dataset")
            titles = [{"title": title}]

        creators = metadata.get("creators", [])
        if not creators:
            authors = metadata.get("authors", [])
            for author in authors:
                if isinstance(author, str):
                    creators.append({"name": author})
                elif isinstance(author, dict):
                    name = f"{author.get('given_name', '')} {author.get('family_name', '')}".strip()
                    if not name:
                        name = author.get("name", "Unknown")
                    creator = {"name": name}
                    if author.get("affiliation"):
                        creator["affiliation"] = [{"name": author["affiliation"]}]
                    creators.append(creator)

        if not creators:
            creators = [{"name": "Materials Data Facility"}]

        # Build attributes
        attributes = {
            "doi": doi,
            "url": url,
            "titles": titles,
            "creators": creators,
            "publisher": metadata.get("publisher", "Materials Data Facility"),
            "publicationYear": int(metadata.get("publication_year", datetime.now().year)),
            "types": {"resourceTypeGeneral": "Dataset"},
            "schemaVersion": "http://datacite.org/schema/kernel-4",
        }

        # Add optional fields
        if metadata.get("descriptions"):
            attributes["descriptions"] = metadata["descriptions"]
        elif metadata.get("description"):
            attributes["descriptions"] = [{"description": metadata["description"], "descriptionType": "Abstract"}]

        if metadata.get("subjects"):
            attributes["subjects"] = metadata["subjects"]
        elif metadata.get("keywords"):
            attributes["subjects"] = [{"subject": kw} for kw in metadata["keywords"]]

        if metadata.get("version"):
            attributes["version"] = str(metadata["version"])

        if metadata.get("rightsList"):
            attributes["rightsList"] = metadata["rightsList"]
        elif metadata.get("license"):
            attributes["rightsList"] = [{"rights": metadata["license"]}]

        if metadata.get("fundingReferences"):
            attributes["fundingReferences"] = metadata["fundingReferences"]

        if related_identifiers:
            attributes["relatedIdentifiers"] = related_identifiers

        # State: draft, registered, or findable
        if publish:
            attributes["event"] = "publish"

        return {
            "data": {
                "type": "dois",
                "attributes": attributes,
            }
        }

    def test_connection(self) -> Dict[str, Any]:
        """Test connectivity to DataCite API."""
        try:
            response = self._client.get(f"{self.api_url}/heartbeat")
            return {"success": response.status_code == 200, "status_code": response.status_code}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def close(self):
        """Close HTTP client."""
        self._client.close()


class MockDataCiteClient:
    """Mock DataCite client for testing."""

    def __init__(self, prefix: str = "10.99999"):
        self.prefix = prefix
        self._dois: Dict[str, Dict] = {}

    def _generate_suffix(self, source_id: str) -> str:
        suffix = source_id.replace("_", "-").lower()
        valid_chars = "abcdefghijklmnopqrstuvwxyz0123456789-."
        return "".join(c if c in valid_chars else "-" for c in suffix)

    def mint_doi(
        self,
        source_id: str,
        metadata: Dict[str, Any],
        url: Optional[str] = None,
        publish: bool = True,
        doi_suffix: Optional[str] = None,
        related_identifiers: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        suffix = doi_suffix or source_id.replace("_", "-").lower()
        doi = f"{self.prefix}/{suffix}"

        if not url:
            url = f"https://materialsdatafacility.org/detail/{source_id}"

        self._dois[doi] = {
            "doi": doi,
            "url": url,
            "metadata": metadata,
            "related_identifiers": related_identifiers or [],
            "state": "findable" if publish else "draft",
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        return {
            "success": True,
            "doi": doi,
            "url": url,
            "state": "findable" if publish else "draft",
            "mock": True,
        }

    def update_metadata(
        self,
        doi: str,
        metadata: Dict[str, Any],
        url: Optional[str] = None,
        related_identifiers: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        existing = self._dois.get(doi, {})
        existing["metadata"] = metadata
        if url:
            existing["url"] = url
        if related_identifiers:
            existing["related_identifiers"] = related_identifiers
        self._dois[doi] = existing
        return {
            "success": True,
            "doi": doi,
            "url": existing.get("url"),
            "state": existing.get("state", "findable"),
            "updated": True,
            "mock": True,
        }

    def get_doi(self, doi: str) -> Optional[Dict[str, Any]]:
        return self._dois.get(doi)

    def close(self):
        pass


def get_datacite_client(test_mode: bool = None) -> DataCiteClient:
    """Get configured DataCite client.

    Uses mock client if credentials not configured.
    """
    if test_mode is None:
        test_mode = os.environ.get("DATACITE_TEST_MODE", "true").lower() == "true"

    username = os.environ.get("DATACITE_USERNAME")
    password = os.environ.get("DATACITE_PASSWORD")

    # Use mock if no credentials
    use_mock = os.environ.get("USE_MOCK_DATACITE", "").lower() == "true"
    if use_mock or not (username and password):
        return MockDataCiteClient()

    return DataCiteClient(test_mode=test_mode)
