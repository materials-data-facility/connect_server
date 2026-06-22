"""Storage backend factory for MDF v2.

Creates the appropriate storage backend based on configuration.

Configuration:
    STORAGE_BACKEND: Backend type (globus, s3, local)
                     Default: local (for development)
"""

import os
from typing import Optional

from v2.storage.base import StorageBackend


# Singleton instance
_storage_backend: Optional[StorageBackend] = None


def get_storage_backend(backend_type: Optional[str] = None) -> StorageBackend:
    """Get the configured storage backend.

    Args:
        backend_type: Override the backend type (globus, local)

    Returns:
        StorageBackend instance
    """
    global _storage_backend

    # Allow override, otherwise use environment
    backend = backend_type or os.environ.get("STORAGE_BACKEND", "local")

    # Return cached instance if same type
    if _storage_backend is not None and _storage_backend.backend_name == backend:
        return _storage_backend

    if backend == "globus":
        from v2.storage.globus_https import GlobusHTTPSStorage
        _storage_backend = GlobusHTTPSStorage()

    elif backend == "s3":
        from v2.storage.s3 import S3Storage
        _storage_backend = S3Storage()

    elif backend == "local":
        from v2.storage.local import LocalStorage
        _storage_backend = LocalStorage()

    else:
        raise ValueError(
            f"Unknown storage backend: {backend}. "
            f"Use 'globus', 's3', or 'local'"
        )

    return _storage_backend


def reset_storage_backend():
    """Reset the cached storage backend (for testing)."""
    global _storage_backend
    _storage_backend = None
