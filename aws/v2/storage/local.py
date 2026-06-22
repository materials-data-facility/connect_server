"""Local filesystem storage backend for MDF v2.

For development and testing only. In production, use Globus or S3.

Configuration:
    FILE_STORE_PATH: Base directory for file storage (default: /tmp/mdf_files)
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional

from v2.storage.base import FileMetadata, StorageBackend


class LocalStorage(StorageBackend):
    """Local filesystem storage backend for development."""

    def __init__(self, base_path: Optional[str] = None):
        """Initialize local storage.

        Args:
            base_path: Base directory for storage
        """
        self.base_path = Path(
            base_path or os.environ.get("FILE_STORE_PATH", "/tmp/mdf_files")
        ).resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)

    @property
    def backend_name(self) -> str:
        return "local"

    def _full_path(self, path: str) -> Path:
        """Get full filesystem path."""
        candidate = (self.base_path / path).resolve()
        try:
            candidate.relative_to(self.base_path)
        except ValueError as exc:
            raise ValueError(f"Invalid storage path: {path!r}") from exc
        return candidate

    def _meta_path(self, file_path: Path) -> Path:
        """Get metadata file path for a file."""
        return file_path.with_suffix(file_path.suffix + ".meta.json")

    def store_file(
        self,
        stream_id: str,
        filename: str,
        content: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,  # Accept user_token etc. (ignored for local storage)
    ) -> FileMetadata:
        """Store a file locally."""
        path = self._build_path(stream_id, filename)
        full_path = self._full_path(path)

        # Ensure directory exists
        full_path.parent.mkdir(parents=True, exist_ok=True)

        # Write file
        full_path.write_bytes(content)

        # Compute checksum
        checksum = self._compute_checksum(content)

        # Build metadata
        file_meta = FileMetadata(
            filename=filename,
            path=path,
            size_bytes=len(content),
            checksum_md5=checksum,
            content_type=content_type,
            storage_backend=self.backend_name,
            download_url=f"file://{full_path}",
            custom_metadata=metadata or {},
        )

        # Store metadata file
        meta_path = self._meta_path(full_path)
        meta_path.write_text(json.dumps(file_meta.to_dict(), indent=2))

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
        """Store a file from a file-like object."""
        content = file_obj.read()
        return self.store_file(stream_id, filename, content, content_type, metadata)

    def get_file(self, path: str) -> Optional[bytes]:
        """Retrieve file contents."""
        try:
            full_path = self._full_path(path)
        except ValueError:
            return None
        if full_path.exists() and full_path.is_file():
            return full_path.read_bytes()
        return None

    def get_download_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        """Get download URL (file:// URL for local)."""
        try:
            full_path = self._full_path(path)
        except ValueError:
            return None
        if full_path.exists():
            return f"file://{full_path}"
        return None

    def get_upload_url(
        self,
        stream_id: str,
        filename: str,
        content_type: str = "application/octet-stream",
        expires_in: int = 3600,
    ) -> Optional[Dict[str, Any]]:
        """Local storage doesn't support pre-signed upload URLs."""
        # Return info for direct upload through API
        path = self._build_path(stream_id, filename)
        return {
            "url": f"/stream/{stream_id}/upload",
            "method": "POST",
            "path": path,
            "note": "Local storage - upload through API",
        }

    def list_files(self, stream_id: str) -> List[FileMetadata]:
        """List all files in a stream."""
        try:
            safe_stream_id = self._sanitize_stream_id(stream_id)
            stream_path = self._full_path(f"streams/{safe_stream_id}")
        except ValueError:
            return []
        files = []

        if not stream_path.exists():
            return files

        for meta_path in stream_path.rglob("*.meta.json"):
            try:
                meta_dict = json.loads(meta_path.read_text())
                files.append(FileMetadata(
                    filename=meta_dict.get("filename", ""),
                    path=meta_dict.get("path", ""),
                    size_bytes=meta_dict.get("size_bytes", 0),
                    checksum_md5=meta_dict.get("checksum_md5", ""),
                    content_type=meta_dict.get("content_type", "application/octet-stream"),
                    stored_at=meta_dict.get("stored_at", ""),
                    storage_backend=meta_dict.get("storage_backend", "local"),
                    download_url=meta_dict.get("download_url"),
                    custom_metadata=meta_dict.get("metadata", {}),
                ))
            except Exception:
                continue

        # Sort by stored_at descending
        files.sort(key=lambda x: x.stored_at, reverse=True)
        return files

    def delete_file(self, path: str) -> bool:
        """Delete a file."""
        try:
            full_path = self._full_path(path)
        except ValueError:
            return False
        meta_path = self._meta_path(full_path)

        deleted = False
        if full_path.exists():
            full_path.unlink()
            deleted = True
        if meta_path.exists():
            meta_path.unlink()

        return deleted

    def delete_stream_files(self, stream_id: str) -> int:
        """Delete all files for a stream."""
        try:
            safe_stream_id = self._sanitize_stream_id(stream_id)
            stream_path = self._full_path(f"streams/{safe_stream_id}")
        except ValueError:
            return 0

        if not stream_path.exists():
            return 0

        # Count files (not including .meta.json)
        count = sum(1 for f in stream_path.rglob("*") if f.is_file() and not f.name.endswith(".meta.json"))

        shutil.rmtree(stream_path)
        return count

    def get_stream_size(self, stream_id: str) -> int:
        """Get total size of all files in a stream."""
        try:
            safe_stream_id = self._sanitize_stream_id(stream_id)
            stream_path = self._full_path(f"streams/{safe_stream_id}")
        except ValueError:
            return 0

        if not stream_path.exists():
            return 0

        total = 0
        for file_path in stream_path.rglob("*"):
            if file_path.is_file() and not file_path.name.endswith(".meta.json"):
                total += file_path.stat().st_size

        return total
