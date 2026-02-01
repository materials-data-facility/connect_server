"""Local file storage for MDF v2 streams.

This module provides local file storage for stream appends.
In production, files would be uploaded directly to Globus endpoints.
For local development, we store files in a configurable directory.
"""

import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, BinaryIO


class FileStore:
    """Local file storage for stream files."""

    def __init__(self, base_path: Optional[str] = None):
        self.base_path = Path(
            base_path or os.environ.get("FILE_STORE_PATH", "/tmp/mdf_files")
        )
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _stream_path(self, stream_id: str) -> Path:
        """Get the storage path for a stream."""
        path = self.base_path / "streams" / stream_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _compute_checksum(self, file_path: Path) -> str:
        """Compute MD5 checksum of a file."""
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def store_file(
        self,
        stream_id: str,
        filename: str,
        content: bytes,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Store a file for a stream.

        Args:
            stream_id: The stream ID
            filename: Name of the file
            content: File contents as bytes
            metadata: Optional metadata to store with the file

        Returns:
            Dict with file info (path, size, checksum, etc.)
        """
        stream_path = self._stream_path(stream_id)

        # Create date-based subdirectory
        date_dir = stream_path / datetime.utcnow().strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)

        # Write file
        file_path = date_dir / filename
        file_path.write_bytes(content)

        # Compute checksum
        checksum = self._compute_checksum(file_path)

        # Store metadata
        meta_path = file_path.with_suffix(file_path.suffix + ".meta.json")
        file_meta = {
            "filename": filename,
            "size_bytes": len(content),
            "checksum_md5": checksum,
            "stored_at": datetime.utcnow().isoformat() + "Z",
            "relative_path": str(file_path.relative_to(self.base_path)),
            "metadata": metadata or {},
        }
        meta_path.write_text(json.dumps(file_meta, indent=2))

        return file_meta

    def store_file_from_path(
        self,
        stream_id: str,
        source_path: Path,
        dest_filename: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Store a file from a local path.

        Args:
            stream_id: The stream ID
            source_path: Path to the source file
            dest_filename: Optional destination filename (defaults to source name)
            metadata: Optional metadata to store with the file

        Returns:
            Dict with file info
        """
        source_path = Path(source_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        filename = dest_filename or source_path.name
        content = source_path.read_bytes()

        return self.store_file(stream_id, filename, content, metadata)

    def list_files(self, stream_id: str) -> List[Dict[str, Any]]:
        """List all files in a stream.

        Returns:
            List of file metadata dicts
        """
        stream_path = self._stream_path(stream_id)
        files = []

        for meta_path in stream_path.rglob("*.meta.json"):
            try:
                meta = json.loads(meta_path.read_text())
                files.append(meta)
            except Exception:
                continue

        # Sort by stored_at
        files.sort(key=lambda x: x.get("stored_at", ""), reverse=True)
        return files

    def get_file(self, stream_id: str, relative_path: str) -> Optional[bytes]:
        """Get file contents by relative path.

        Args:
            stream_id: The stream ID
            relative_path: Relative path from store root

        Returns:
            File contents as bytes, or None if not found
        """
        file_path = self.base_path / relative_path
        if file_path.exists() and file_path.is_file():
            return file_path.read_bytes()
        return None

    def get_file_metadata(
        self, stream_id: str, relative_path: str
    ) -> Optional[Dict[str, Any]]:
        """Get file metadata by relative path.

        Args:
            stream_id: The stream ID
            relative_path: Relative path from store root

        Returns:
            File metadata dict, or None if not found
        """
        file_path = self.base_path / relative_path
        meta_path = file_path.with_suffix(file_path.suffix + ".meta.json")
        if meta_path.exists():
            return json.loads(meta_path.read_text())
        return None

    def delete_stream_files(self, stream_id: str) -> int:
        """Delete all files for a stream.

        Returns:
            Number of files deleted
        """
        stream_path = self._stream_path(stream_id)
        if not stream_path.exists():
            return 0

        count = len(list(stream_path.rglob("*")))
        shutil.rmtree(stream_path)
        return count

    def get_stream_size(self, stream_id: str) -> int:
        """Get total size of all files in a stream.

        Returns:
            Total size in bytes
        """
        stream_path = self._stream_path(stream_id)
        if not stream_path.exists():
            return 0

        total = 0
        for file_path in stream_path.rglob("*"):
            if file_path.is_file() and not file_path.name.endswith(".meta.json"):
                total += file_path.stat().st_size
        return total


def get_file_store() -> FileStore:
    """Get the configured file store instance."""
    return FileStore()
