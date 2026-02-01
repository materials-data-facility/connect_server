"""Storage backends for MDF v2.

Supports multiple storage backends:
- Globus HTTPS endpoints (primary, 1PB free storage)
- S3 (secondary)
- Local filesystem (development only)

Configuration via environment variables:
    STORAGE_BACKEND=globus|s3|local

    # Globus settings
    GLOBUS_ENDPOINT_ID=<uuid>
    GLOBUS_BASE_PATH=/mdf/streams
    GLOBUS_CLIENT_ID=<client-id>
    GLOBUS_CLIENT_SECRET=<client-secret>

    # S3 settings
    S3_BUCKET=mdf-stream-files
    S3_PREFIX=streams/

    # Local settings
    FILE_STORE_PATH=/tmp/mdf_files
"""

from v2.storage.base import StorageBackend, FileMetadata
from v2.storage.factory import get_storage_backend, reset_storage_backend

__all__ = ["StorageBackend", "FileMetadata", "get_storage_backend", "reset_storage_backend"]
