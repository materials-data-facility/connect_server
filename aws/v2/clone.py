"""Dataset/stream cloning for MDF v2.

Allows researchers to clone datasets or streams to their local machine,
pulling files from Globus HTTPS endpoints as needed.
"""

import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import httpx

from v2.storage import get_storage_backend
from v2.storage.globus_https import load_cached_token
from v2.stream_store import get_stream_store


class StreamCloner:
    """Clone streams from MDF to local filesystem."""

    def __init__(
        self,
        dest_dir: str,
        token: Optional[str] = None,
        verbose: bool = True,
    ):
        """Initialize cloner.

        Args:
            dest_dir: Destination directory for cloned files
            token: Globus access token (or will use cached)
            verbose: Print progress messages
        """
        self.dest_dir = Path(dest_dir)
        self.token = token or load_cached_token()
        self.verbose = verbose
        self._client = httpx.Client(timeout=60.0, follow_redirects=True)

    def log(self, msg: str):
        """Print message if verbose."""
        if self.verbose:
            print(msg)

    def clone_stream(
        self,
        stream_id: str,
        include_metadata: bool = True,
        file_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Clone a stream to local directory.

        Args:
            stream_id: The stream ID to clone
            include_metadata: Save stream metadata as JSON
            file_filter: Optional glob pattern to filter files

        Returns:
            Dict with clone results
        """
        import fnmatch

        # Get stream info
        stream_store = get_stream_store()
        stream = stream_store.get_stream(stream_id)

        if not stream:
            raise ValueError(f"Stream not found: {stream_id}")

        # Create destination directory
        stream_dir = self.dest_dir / stream_id
        stream_dir.mkdir(parents=True, exist_ok=True)

        self.log(f"Cloning stream: {stream_id}")
        self.log(f"  Title: {stream.get('title', 'Untitled')}")
        self.log(f"  Destination: {stream_dir}")

        # Save metadata
        if include_metadata:
            meta_path = stream_dir / "stream_metadata.json"
            with open(meta_path, "w") as f:
                json.dump(stream, f, indent=2, default=str)
            self.log(f"  Saved metadata: {meta_path}")

        # Get file list from storage
        storage = get_storage_backend()
        files = storage.list_files(stream_id)

        if file_filter:
            files = [f for f in files if fnmatch.fnmatch(f.filename, file_filter)]

        self.log(f"  Files to download: {len(files)}")

        # Download files
        downloaded = []
        errors = []
        total_bytes = 0

        for file_meta in files:
            try:
                local_path = stream_dir / file_meta.filename
                self.log(f"  Downloading: {file_meta.filename}")

                # Get content from storage
                if storage.backend_name == "globus":
                    # Download directly via HTTPS
                    content = self._download_globus(file_meta.download_url)
                else:
                    content = storage.get_file(file_meta.path)

                if content:
                    local_path.write_bytes(content)
                    downloaded.append({
                        "filename": file_meta.filename,
                        "path": str(local_path),
                        "size_bytes": len(content),
                    })
                    total_bytes += len(content)
                else:
                    errors.append({
                        "filename": file_meta.filename,
                        "error": "Could not download file",
                    })

            except Exception as e:
                errors.append({
                    "filename": file_meta.filename,
                    "error": str(e),
                })

        self.log(f"  Downloaded: {len(downloaded)} files ({total_bytes:,} bytes)")
        if errors:
            self.log(f"  Errors: {len(errors)}")

        return {
            "success": True,
            "stream_id": stream_id,
            "destination": str(stream_dir),
            "downloaded": len(downloaded),
            "total_bytes": total_bytes,
            "files": downloaded,
            "errors": errors if errors else None,
        }

    def _download_globus(self, url: str) -> Optional[bytes]:
        """Download file from Globus HTTPS endpoint."""
        if not self.token:
            raise ValueError("No Globus token available. Run test_globus_upload.py to authenticate.")

        headers = {"Authorization": f"Bearer {self.token}"}
        response = self._client.get(url, headers=headers)
        response.raise_for_status()
        return response.content

    def clone_from_url(
        self,
        url: str,
        filename: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Clone a single file from a Globus URL.

        Args:
            url: The Globus HTTPS URL
            filename: Optional override for filename

        Returns:
            Dict with download result
        """
        if not filename:
            filename = url.rsplit("/", 1)[-1]

        self.dest_dir.mkdir(parents=True, exist_ok=True)
        local_path = self.dest_dir / filename

        self.log(f"Downloading: {url}")
        self.log(f"  To: {local_path}")

        content = self._download_globus(url)
        local_path.write_bytes(content)

        self.log(f"  Size: {len(content):,} bytes")

        return {
            "success": True,
            "url": url,
            "filename": filename,
            "path": str(local_path),
            "size_bytes": len(content),
        }

    def close(self):
        """Close HTTP client."""
        self._client.close()


def clone_stream(
    stream_id: str,
    dest_dir: str = ".",
    include_metadata: bool = True,
    file_filter: Optional[str] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Convenience function to clone a stream.

    Args:
        stream_id: The stream ID to clone
        dest_dir: Destination directory
        include_metadata: Save stream metadata as JSON
        file_filter: Optional glob pattern to filter files
        verbose: Print progress messages

    Returns:
        Dict with clone results
    """
    cloner = StreamCloner(dest_dir=dest_dir, verbose=verbose)
    try:
        return cloner.clone_stream(
            stream_id=stream_id,
            include_metadata=include_metadata,
            file_filter=file_filter,
        )
    finally:
        cloner.close()


def clone_url(
    url: str,
    dest_dir: str = ".",
    filename: Optional[str] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Convenience function to clone a file from URL.

    Args:
        url: The Globus HTTPS URL
        dest_dir: Destination directory
        filename: Optional override for filename
        verbose: Print progress messages

    Returns:
        Dict with download result
    """
    cloner = StreamCloner(dest_dir=dest_dir, verbose=verbose)
    try:
        return cloner.clone_from_url(url=url, filename=filename)
    finally:
        cloner.close()


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Clone MDF streams to local directory")
    parser.add_argument("stream_id", help="Stream ID to clone")
    parser.add_argument("-d", "--dest", default=".", help="Destination directory")
    parser.add_argument("-f", "--filter", help="File filter pattern (e.g., '*.csv')")
    parser.add_argument("--no-metadata", action="store_true", help="Skip metadata file")
    parser.add_argument("-q", "--quiet", action="store_true", help="Quiet mode")

    args = parser.parse_args()

    result = clone_stream(
        stream_id=args.stream_id,
        dest_dir=args.dest,
        include_metadata=not args.no_metadata,
        file_filter=args.filter,
        verbose=not args.quiet,
    )

    if result["errors"]:
        print(f"\nCompleted with {len(result['errors'])} errors")
        exit(1)
    else:
        print(f"\nClone complete: {result['downloaded']} files")
