"""S3 storage backend for MDF v2.

For staging and production deployments where Globus HTTPS is not needed.

Configuration:
    S3_BUCKET: S3 bucket name for file storage
    S3_PREFIX: Key prefix within the bucket (default: "streams/")
"""

import io
import os
from typing import Any, BinaryIO, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from v2.storage.base import FileMetadata, StorageBackend


class S3Storage(StorageBackend):
    """S3 storage backend for staging/production."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
    ):
        self.bucket = bucket or os.environ.get("S3_BUCKET", "")
        if not self.bucket:
            raise ValueError("S3_BUCKET environment variable is required for S3 storage backend")
        self.prefix = prefix or os.environ.get("S3_PREFIX", "streams/")
        self._s3 = boto3.client("s3")

    @property
    def backend_name(self) -> str:
        return "s3"

    def _s3_key(self, path: str) -> str:
        """Build a full S3 key from a storage path."""
        return f"{self.prefix}{path}" if not path.startswith(self.prefix) else path

    def store_file(
        self,
        stream_id: str,
        filename: str,
        content: bytes,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> FileMetadata:
        path = self._build_path(stream_id, filename)
        key = self._s3_key(path)
        checksum = self._compute_checksum(content)

        s3_metadata = {}
        if metadata:
            s3_metadata = {k: str(v) for k, v in metadata.items()}

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
        path = self._build_path(stream_id, filename)
        key = self._s3_key(path)

        s3_metadata = {}
        if metadata:
            s3_metadata = {k: str(v) for k, v in metadata.items()}

        self._s3.upload_fileobj(
            file_obj,
            self.bucket,
            key,
            ExtraArgs={
                "ContentType": content_type,
                "Metadata": s3_metadata,
            },
        )

        # Read back for checksum if possible, otherwise use empty
        checksum = ""
        try:
            file_obj.seek(0)
            content = file_obj.read()
            checksum = self._compute_checksum(content)
        except Exception:
            pass

        return FileMetadata(
            filename=filename,
            path=path,
            size_bytes=size_bytes,
            checksum_md5=checksum,
            content_type=content_type,
            storage_backend=self.backend_name,
            custom_metadata=metadata or {},
        )

    def get_file(self, path: str) -> Optional[bytes]:
        key = self._s3_key(path)
        try:
            resp = self._s3.get_object(Bucket=self.bucket, Key=key)
            return resp["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def get_download_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        key = self._s3_key(path)
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError:
            return None

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
        path = self._build_path(stream_id, filename)
        key = self._s3_key(path)

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
            "headers": {"Content-Type": content_type},
            "path": path,
            "expires_in": expires_in,
        }

    def list_files(self, stream_id: str) -> List[FileMetadata]:
        safe_stream_id = self._sanitize_stream_id(stream_id)
        prefix = self._s3_key(f"streams/{safe_stream_id}/")

        files = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Extract filename from key (last path component)
                filename = key.rsplit("/", 1)[-1]
                # Strip the prefix to get the storage path
                path = key[len(self.prefix):] if key.startswith(self.prefix) else key

                files.append(FileMetadata(
                    filename=filename,
                    path=path,
                    size_bytes=obj["Size"],
                    checksum_md5=obj.get("ETag", "").strip('"'),
                    content_type=self._guess_content_type(filename),
                    stored_at=obj["LastModified"].isoformat().replace("+00:00", "Z"),
                    storage_backend=self.backend_name,
                ))

        files.sort(key=lambda x: x.stored_at, reverse=True)
        return files

    def delete_file(self, path: str) -> bool:
        key = self._s3_key(path)
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError:
            return False

        self._s3.delete_object(Bucket=self.bucket, Key=key)
        return True

    def delete_stream_files(self, stream_id: str) -> int:
        safe_stream_id = self._sanitize_stream_id(stream_id)
        prefix = self._s3_key(f"streams/{safe_stream_id}/")

        count = 0
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            objects = page.get("Contents", [])
            if not objects:
                continue
            delete_keys = [{"Key": obj["Key"]} for obj in objects]
            self._s3.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": delete_keys},
            )
            count += len(delete_keys)

        return count

    def get_stream_size(self, stream_id: str) -> int:
        safe_stream_id = self._sanitize_stream_id(stream_id)
        prefix = self._s3_key(f"streams/{safe_stream_id}/")

        total = 0
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                total += obj["Size"]

        return total
