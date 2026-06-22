"""Globus HTTPS storage backend for MDF v2.

Uses Globus endpoints with HTTPS access for file storage.
This is the primary storage backend for MDF - we have 1PB of free storage.

Globus HTTPS endpoints provide:
- Direct HTTPS GET/PUT/DELETE operations
- Bearer token authentication
- High-performance data transfer
- Integration with Globus Transfer for large datasets

Configuration:
    GLOBUS_ENDPOINT_ID: The Globus endpoint UUID (default: NCSA MDF endpoint)
    GLOBUS_BASE_PATH: Base path on the endpoint (default: /mdf/streams)
    GLOBUS_HTTPS_SERVER: Override HTTPS server (default: data.materialsdatafacility.org)

Authentication (in order of priority):
    1. access_token parameter
    2. GLOBUS_ACCESS_TOKEN environment variable
    3. Cached tokens from ~/.mdf/v2_https_tokens.json
    4. Client credentials flow (GLOBUS_CLIENT_ID + GLOBUS_CLIENT_SECRET)
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, BinaryIO, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

from v2.storage.base import FileMetadata, StorageBackend

# NCSA MDF endpoint - default for MDF
NCSA_ENDPOINT_UUID = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"
NCSA_HTTPS_SERVER = "data.materialsdatafacility.org"

# Token cache location
TOKEN_CACHE_FILE = os.path.expanduser("~/.mdf/v2_https_tokens.json")


def load_cached_token() -> Optional[str]:
    """Load cached access token from disk."""
    if os.path.exists(TOKEN_CACHE_FILE):
        try:
            with open(TOKEN_CACHE_FILE) as f:
                data = json.load(f)
                return data.get("access_token")
        except Exception:
            pass
    return None


class GlobusHTTPSStorage(StorageBackend):
    """Storage backend using Globus HTTPS endpoints."""

    def __init__(
        self,
        endpoint_id: Optional[str] = None,
        base_path: Optional[str] = None,
        https_server: Optional[str] = None,
        access_token: Optional[str] = None,
    ):
        """Initialize Globus HTTPS storage.

        Args:
            endpoint_id: Globus endpoint UUID (default: NCSA MDF endpoint)
            base_path: Base path on endpoint (default: /tmp/testing for dev)
            https_server: Override HTTPS server hostname
            access_token: Globus access token (or will use cached/env token)
        """
        self.endpoint_id = endpoint_id or os.environ.get("GLOBUS_ENDPOINT_ID", NCSA_ENDPOINT_UUID)

        self.base_path = (base_path or os.environ.get("GLOBUS_BASE_PATH", "/tmp/testing")).rstrip("/")

        # Build HTTPS server URL - use MDF's custom domain by default
        self.https_server = https_server or os.environ.get(
            "GLOBUS_HTTPS_SERVER",
            NCSA_HTTPS_SERVER
        )
        self.base_url = f"https://{self.https_server}{self.base_path}"

        # Authentication
        self._access_token = access_token
        self._token_expires_at: Optional[datetime] = None

        # HTTP client with retry
        self._client = httpx.Client(
            timeout=60.0,
            follow_redirects=True,
        )

        # Metadata storage (could be DynamoDB in production)
        self._metadata_cache: Dict[str, FileMetadata] = {}

    @property
    def backend_name(self) -> str:
        return "globus"

    def _get_token(self) -> str:
        """Get a valid Globus access token."""
        # 1. Explicit token passed to constructor
        if self._access_token:
            return self._access_token

        # 2. Environment variable
        token = os.environ.get("GLOBUS_ACCESS_TOKEN")
        if token:
            return token

        # 3. Cached token from disk (from test_globus_upload.py auth flow)
        cached = load_cached_token()
        if cached:
            self._access_token = cached  # Cache in memory too
            return cached

        # 4. Client credentials flow (for server deployment)
        client_id = os.environ.get("GLOBUS_CLIENT_ID")
        client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")

        if client_id and client_secret:
            logger.info("Using client credentials flow for Globus HTTPS token")
            return self._get_client_credentials_token(client_id, client_secret)

        raise ValueError(
            "No Globus authentication configured. Run test_globus_upload.py to authenticate, "
            "or set GLOBUS_ACCESS_TOKEN or GLOBUS_CLIENT_ID + GLOBUS_CLIENT_SECRET"
        )

    def _get_client_credentials_token(self, client_id: str, client_secret: str) -> str:
        """Get token using client credentials flow for HTTPS endpoint access."""
        # Check if we have a cached valid token
        if self._access_token and self._token_expires_at:
            if datetime.now(timezone.utc) < self._token_expires_at:
                return self._access_token

        # Request token scoped to the HTTPS endpoint (data access)
        scope = f"urn:globus:auth:scope:{self.https_server}:all"
        logger.info("Requesting client credentials token for scope: %s", scope)

        response = self._client.post(
            "https://auth.globus.org/v2/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "scope": scope,
            },
            auth=(client_id, client_secret),
        )
        if response.status_code != 200:
            logger.error("Client credentials token request failed: %s %s", response.status_code, response.text)
        response.raise_for_status()

        data = response.json()
        self._access_token = data["access_token"]
        # Cache token with some buffer before expiry
        expires_in = data.get("expires_in", 3600)
        from datetime import timedelta
        self._token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 60)

        return self._access_token

    def _headers(self, content_type: str = "application/octet-stream") -> Dict[str, str]:
        """Build request headers with auth."""
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": content_type,
        }

    def _full_url(self, path: str) -> str:
        """Build full URL for a path."""
        safe_path = self._sanitize_remote_path(path)
        # Ensure path doesn't double up the base
        if safe_path.startswith(self.base_path):
            safe_path = safe_path[len(self.base_path):]
        return f"{self.base_url}/{safe_path.lstrip('/')}"

    def _sanitize_remote_path(self, path: str) -> str:
        value = (path or "").replace("\\", "/").strip()
        if not value:
            raise ValueError("path is required")
        if "://" in value:
            raise ValueError("path must not be a URL")
        if value.startswith("/"):
            value = value.lstrip("/")
        if ".." in value.split("/"):
            raise ValueError("path traversal is not allowed")
        return value

    def _build_path(self, stream_id: str, filename: str) -> str:
        """Build a flat storage path for Globus HTTPS.

        Uses flat structure to avoid directory creation issues:
        {stream_id}_{date}_{filename}

        Globus HTTPS doesn't auto-create parent directories, so we use
        a flat naming scheme instead of nested directories.
        """
        date_prefix = datetime.now(timezone.utc).strftime("%Y%m%d")
        safe_stream_id = self._sanitize_stream_id(stream_id)
        safe_filename = self._sanitize_filename(filename).replace("/", "_")
        return f"{safe_stream_id}_{date_prefix}_{safe_filename}"

    def store_file(
        self,
        stream_id: str,
        filename: str,
        content: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        user_token: Optional[str] = None,
    ) -> FileMetadata:
        """Store a file via HTTPS PUT.

        Args:
            user_token: User's Globus token for authorization. If provided,
                       the action is performed on behalf of the user.
        """
        path = self._build_path(stream_id, filename)
        url = self._full_url(path)

        # Compute checksum before upload
        checksum = self._compute_checksum(content)

        # Prefer user token (they authenticated with the data scope and
        # have write access to the endpoint); fall back to server credentials
        # for background operations (e.g., async worker) where no user token.
        if user_token:
            token = user_token
            logger.info("Using user token for Globus upload")
        else:
            try:
                token = self._get_token()
                logger.info("Using server token for Globus upload (len=%d)", len(token))
            except Exception as exc:
                raise ValueError(f"No authentication available for Globus storage: {exc}")

        # Upload file
        response = self._client.put(
            url,
            content=content,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": content_type,
            },
        )
        response.raise_for_status()

        # Build metadata
        file_meta = FileMetadata(
            filename=filename,
            path=path,
            size_bytes=len(content),
            checksum_md5=checksum,
            content_type=content_type,
            storage_backend=self.backend_name,
            download_url=url,
            custom_metadata=metadata or {},
        )

        # Cache metadata (in production, store in DynamoDB)
        self._metadata_cache[path] = file_meta

        return file_meta

    def store_file_stream(
        self,
        stream_id: str,
        filename: str,
        file_obj: BinaryIO,
        size_bytes: int,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FileMetadata:
        """Store a file from a stream via HTTPS PUT."""
        path = self._build_path(stream_id, filename)
        url = self._full_url(path)

        # For streaming uploads, we need to compute checksum separately
        # Read content, compute checksum, then upload
        content = file_obj.read()
        checksum = self._compute_checksum(content)

        response = self._client.put(
            url,
            content=content,
            headers={
                **self._headers(content_type),
                "Content-Length": str(len(content)),
            },
        )
        response.raise_for_status()

        file_meta = FileMetadata(
            filename=filename,
            path=path,
            size_bytes=len(content),
            checksum_md5=checksum,
            content_type=content_type,
            storage_backend=self.backend_name,
            download_url=url,
            custom_metadata=metadata or {},
        )

        self._metadata_cache[path] = file_meta
        return file_meta

    def get_file(self, path: str) -> Optional[bytes]:
        """Retrieve file contents via HTTPS GET."""
        try:
            url = self._full_url(path)
        except ValueError:
            return None

        try:
            response = self._client.get(url, headers=self._headers())
            response.raise_for_status()
            return response.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_download_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        """Get direct download URL.

        For Globus HTTPS, the URL is the same but requires auth.
        For unauthenticated access, you'd need to use Globus sharing.
        """
        # The download URL is just the HTTPS endpoint URL
        # Client will need to provide auth token
        try:
            return self._full_url(path)
        except ValueError:
            return None

    def get_upload_url(
        self,
        stream_id: str,
        filename: str,
        content_type: str = "application/octet-stream",
        expires_in: int = 3600,
    ) -> Optional[Dict[str, Any]]:
        """Get upload URL for direct client upload.

        Returns the URL and headers needed for direct PUT.
        """
        path = self._build_path(stream_id, filename)
        url = self._full_url(path)

        return {
            "url": url,
            "method": "PUT",
            "path": path,
            "headers": {
                "Content-Type": content_type,
            },
            "auth_type": "bearer",
            "expires_in": expires_in,
        }

    def list_files(self, stream_id: str) -> List[FileMetadata]:
        """List files in a stream.

        Uses cached metadata. In production, query DynamoDB.
        """
        # Flat structure: {stream_id}_{date}_{filename}
        try:
            safe_stream_id = self._sanitize_stream_id(stream_id)
        except ValueError:
            return []
        prefix = f"{safe_stream_id}_"
        files = [
            meta for path, meta in self._metadata_cache.items()
            if path.startswith(prefix)
        ]
        # Sort by stored_at descending
        files.sort(key=lambda x: x.stored_at, reverse=True)
        return files

    def delete_file(self, path: str) -> bool:
        """Delete a file via HTTPS DELETE."""
        try:
            safe_path = self._sanitize_remote_path(path)
            url = self._full_url(safe_path)
        except ValueError:
            return False

        try:
            response = self._client.delete(url, headers=self._headers())
            response.raise_for_status()
            self._metadata_cache.pop(safe_path, None)
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise

    def delete_stream_files(self, stream_id: str) -> int:
        """Delete all files for a stream."""
        files = self.list_files(stream_id)
        count = 0
        for f in files:
            if self.delete_file(f.path):
                count += 1
        return count

    def get_stream_size(self, stream_id: str) -> int:
        """Get total size of all files in a stream."""
        files = self.list_files(stream_id)
        return sum(f.size_bytes for f in files)

    def close(self):
        """Close the HTTP client."""
        self._client.close()
