"""Base storage interface for MDF v2."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any, BinaryIO, Dict, List, Optional


@dataclass
class FileMetadata:
    """Metadata for a stored file."""

    filename: str
    path: str  # Full path in storage (e.g., streams/{stream_id}/2026-01-31/file.csv)
    size_bytes: int
    checksum_md5: str
    content_type: str = "application/octet-stream"
    stored_at: str = ""
    storage_backend: str = ""  # globus, s3, local
    download_url: Optional[str] = None  # Direct download URL if available
    custom_metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.stored_at:
            self.stored_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "filename": self.filename,
            "path": self.path,
            "size_bytes": self.size_bytes,
            "checksum_md5": self.checksum_md5,
            "content_type": self.content_type,
            "stored_at": self.stored_at,
            "storage_backend": self.storage_backend,
            "download_url": self.download_url,
            "metadata": self.custom_metadata,
        }


class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """Return the backend name (globus, s3, local)."""
        pass

    @abstractmethod
    def store_file(
        self,
        stream_id: str,
        filename: str,
        content: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FileMetadata:
        """Store a file and return its metadata.

        Args:
            stream_id: The stream ID (used for path organization)
            filename: Name of the file
            content: File contents as bytes
            content_type: MIME type
            metadata: Optional custom metadata

        Returns:
            FileMetadata with storage details
        """
        pass

    @abstractmethod
    def store_file_stream(
        self,
        stream_id: str,
        filename: str,
        file_obj: BinaryIO,
        size_bytes: int,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FileMetadata:
        """Store a file from a file-like object (for large files).

        Args:
            stream_id: The stream ID
            filename: Name of the file
            file_obj: File-like object to read from
            size_bytes: Total size in bytes
            content_type: MIME type
            metadata: Optional custom metadata

        Returns:
            FileMetadata with storage details
        """
        pass

    @abstractmethod
    def get_file(self, path: str) -> Optional[bytes]:
        """Retrieve file contents by path.

        Args:
            path: Full path in storage

        Returns:
            File contents as bytes, or None if not found
        """
        pass

    @abstractmethod
    def get_download_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        """Get a direct download URL for a file.

        Args:
            path: Full path in storage
            expires_in: URL expiration time in seconds

        Returns:
            Direct download URL, or None if not supported
        """
        pass

    @abstractmethod
    def get_upload_url(
        self,
        stream_id: str,
        filename: str,
        content_type: str = "application/octet-stream",
        expires_in: int = 3600,
    ) -> Optional[Dict[str, Any]]:
        """Get a pre-signed upload URL for direct client upload.

        This allows clients to upload directly to storage without
        going through the API (useful for large files).

        Args:
            stream_id: The stream ID
            filename: Name of the file
            content_type: MIME type
            expires_in: URL expiration time in seconds

        Returns:
            Dict with 'url', 'method', 'headers', and 'path'
            or None if not supported
        """
        pass

    @abstractmethod
    def list_files(self, stream_id: str) -> List[FileMetadata]:
        """List all files in a stream.

        Args:
            stream_id: The stream ID

        Returns:
            List of FileMetadata objects
        """
        pass

    @abstractmethod
    def delete_file(self, path: str) -> bool:
        """Delete a file.

        Args:
            path: Full path in storage

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    def delete_stream_files(self, stream_id: str) -> int:
        """Delete all files for a stream.

        Args:
            stream_id: The stream ID

        Returns:
            Number of files deleted
        """
        pass

    @abstractmethod
    def get_stream_size(self, stream_id: str) -> int:
        """Get total size of all files in a stream.

        Args:
            stream_id: The stream ID

        Returns:
            Total size in bytes
        """
        pass

    def _build_path(self, stream_id: str, filename: str) -> str:
        """Build a storage path for a file."""
        safe_stream_id = self._sanitize_stream_id(stream_id)
        safe_filename = self._sanitize_filename(filename)
        date_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return f"streams/{safe_stream_id}/{date_prefix}/{safe_filename}"

    def _sanitize_stream_id(self, stream_id: str) -> str:
        value = (stream_id or "").strip()
        if not value:
            raise ValueError("stream_id is required")
        if not re.fullmatch(r"[A-Za-z0-9._:-]+", value):
            raise ValueError(f"Invalid stream_id: {stream_id!r}")
        return value

    def _sanitize_filename(self, filename: str) -> str:
        normalized = (filename or "").replace("\\", "/").strip("/")
        if not normalized:
            raise ValueError("filename is required")
        parts = normalized.split("/")
        if any(part in ("", ".", "..") for part in parts):
            raise ValueError(f"Invalid filename: {filename!r}")
        allowed = re.compile(r"^[A-Za-z0-9._()+=,@ -]+$")
        for part in parts:
            if not allowed.fullmatch(part):
                raise ValueError(f"Invalid filename segment: {part!r}")
        return normalized

    def _compute_checksum(self, content: bytes) -> str:
        """Compute MD5 checksum of content."""
        import hashlib
        return hashlib.md5(content).hexdigest()

    def _guess_content_type(self, filename: str) -> str:
        """Guess content type from filename."""
        import mimetypes
        content_type, _ = mimetypes.guess_type(filename)
        return content_type or "application/octet-stream"
