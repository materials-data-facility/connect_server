"""S3 storage backend for MDF v2.

Secondary storage option using AWS S3.

Configuration:
    S3_BUCKET: Bucket name
    S3_PREFIX: Key prefix (default: streams/)
    S3_REGION: AWS region (default: us-east-1)
"""

import json
import os
from datetime import datetime
from typing import Any, BinaryIO, Dict, List, Optional

from v2.storage.base import FileMetadata, StorageBackend


class S3Storage(StorageBackend):
    """Storage backend using AWS S3."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        region: Optional[str] = None,
    ):
        """Initialize S3 storage.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix (default: streams/)
            region: AWS region
        """
        import boto3

        self.bucket = bucket or os.environ.get("S3_BUCKET")
        if not self.bucket:
            raise ValueError("S3_BUCKET is required")

        self.prefix = (prefix or os.environ.get("S3_PREFIX", "streams/")).rstrip("/")
        self.region = region or os.environ.get("S3_REGION", "us-east-1")

        self._s3 = boto3.client("s3", region_name=self.region)

    @property
    def backend_name(self) -> str:
        return "s3"

    def _full_key(self, path: str) -> str:
        """Build full S3 key from path."""
        if path.startswith(self.prefix):
            return path
        return f"{self.prefix}/{path.lstrip('/')}"

    def store_file(
        self,
        stream_id: str,
        filename: str,
        content: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,  # Accept user_token etc. (S3 uses IAM, not user tokens)
    ) -> FileMetadata:
        """Store a file in S3."""
        path = self._build_path(stream_id, filename)
        key = self._full_key(path)
        checksum = self._compute_checksum(content)

        # Store with metadata
        s3_metadata = {
            "checksum-md5": checksum,
            "original-filename": filename,
            "stream-id": stream_id,
        }
        if metadata:
            s3_metadata["custom-metadata"] = json.dumps(metadata)

        self._s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
            Metadata=s3_metadata,
        )

        return FileMetadata(
            filename=filename,
            path=path,
            size_bytes=len(content),
            checksum_md5=checksum,
            content_type=content_type,
            storage_backend=self.backend_name,
            custom_metadata=metadata or {},
        )

    def store_file_stream(
        self,
        stream_id: str,
        filename: str,
        file_obj: BinaryIO,
        size_bytes: int,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> FileMetadata:
        """Store a file from stream using multipart upload for large files."""
        path = self._build_path(stream_id, filename)
        key = self._full_key(path)

        # For large files, use multipart upload
        # For simplicity, read all and compute checksum
        content = file_obj.read()
        checksum = self._compute_checksum(content)

        s3_metadata = {
            "checksum-md5": checksum,
            "original-filename": filename,
            "stream-id": stream_id,
        }
        if metadata:
            s3_metadata["custom-metadata"] = json.dumps(metadata)

        self._s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
            Metadata=s3_metadata,
        )

        return FileMetadata(
            filename=filename,
            path=path,
            size_bytes=len(content),
            checksum_md5=checksum,
            content_type=content_type,
            storage_backend=self.backend_name,
            custom_metadata=metadata or {},
        )

    def get_file(self, path: str) -> Optional[bytes]:
        """Retrieve file from S3."""
        key = self._full_key(path)

        try:
            response = self._s3.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read()
        except self._s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            if "NoSuchKey" in str(e):
                return None
            raise

    def get_download_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        """Get pre-signed download URL."""
        key = self._full_key(path)

        return self._s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    def get_upload_url(
        self,
        stream_id: str,
        filename: str,
        content_type: str = "application/octet-stream",
        expires_in: int = 3600,
    ) -> Optional[Dict[str, Any]]:
        """Get pre-signed upload URL for direct S3 upload."""
        path = self._build_path(stream_id, filename)
        key = self._full_key(path)

        url = self._s3.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.bucket,
                "Key": key,
                "ContentType": content_type,
            },
            ExpiresIn=expires_in,
        )

        return {
            "url": url,
            "method": "PUT",
            "path": path,
            "headers": {"Content-Type": content_type},
            "expires_in": expires_in,
        }

    def list_files(self, stream_id: str) -> List[FileMetadata]:
        """List files in a stream."""
        prefix = self._full_key(f"streams/{stream_id}/")

        files = []
        paginator = self._s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Get metadata
                try:
                    head = self._s3.head_object(Bucket=self.bucket, Key=key)
                    s3_meta = head.get("Metadata", {})

                    custom_meta = {}
                    if s3_meta.get("custom-metadata"):
                        try:
                            custom_meta = json.loads(s3_meta["custom-metadata"])
                        except Exception:
                            pass

                    # Extract path relative to prefix
                    path = key[len(self.prefix):].lstrip("/") if key.startswith(self.prefix) else key

                    files.append(FileMetadata(
                        filename=s3_meta.get("original-filename", key.split("/")[-1]),
                        path=path,
                        size_bytes=obj["Size"],
                        checksum_md5=s3_meta.get("checksum-md5", ""),
                        content_type=head.get("ContentType", "application/octet-stream"),
                        stored_at=obj["LastModified"].isoformat() + "Z",
                        storage_backend=self.backend_name,
                        custom_metadata=custom_meta,
                    ))
                except Exception:
                    # If we can't get metadata, create basic entry
                    path = key[len(self.prefix):].lstrip("/") if key.startswith(self.prefix) else key
                    files.append(FileMetadata(
                        filename=key.split("/")[-1],
                        path=path,
                        size_bytes=obj["Size"],
                        checksum_md5="",
                        stored_at=obj["LastModified"].isoformat() + "Z",
                        storage_backend=self.backend_name,
                    ))

        # Sort by stored_at descending
        files.sort(key=lambda x: x.stored_at, reverse=True)
        return files

    def delete_file(self, path: str) -> bool:
        """Delete a file from S3."""
        key = self._full_key(path)

        try:
            self._s3.delete_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def delete_stream_files(self, stream_id: str) -> int:
        """Delete all files for a stream."""
        prefix = self._full_key(f"streams/{stream_id}/")

        # List and delete
        count = 0
        paginator = self._s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                self._s3.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": objects},
                )
                count += len(objects)

        return count

    def get_stream_size(self, stream_id: str) -> int:
        """Get total size of all files in a stream."""
        files = self.list_files(stream_id)
        return sum(f.size_bytes for f in files)
