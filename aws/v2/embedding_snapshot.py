"""Read-optimized snapshot of dataset embeddings.

Two artifacts per snapshot, stored under the same content-hash prefix:

    embeddings/v1/index-{sha}.bin     Packed Float32Array (n_vectors * n_dims * 4 bytes)
    embeddings/v1/index-{sha}.json    Sidecar: [{source_id, version, title, authors, ...}]

Plus a stable pointer:

    embeddings/v1/current.json        {"bin": "...-{sha}.bin", "json": "...-{sha}.json",
                                       "count": N, "dims": D, "model": "...",
                                       "built_at": "..."}

Browsers and the semantic search Lambda both read this. Content-hashed filenames
mean both can cache forever; only `current.json` needs a short cache.

S3 is the default backend (prod/staging). For local dev we fall back to the
filesystem under `$EMBEDDING_SNAPSHOT_DIR` (default /tmp/mdf-embedding-snapshot).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import struct
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

SNAPSHOT_PREFIX = os.environ.get("EMBEDDING_SNAPSHOT_PREFIX", "embeddings/v1/")
SNAPSHOT_POINTER_KEY = SNAPSHOT_PREFIX + "current.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Backend abstraction (S3 in prod, filesystem in dev)
# ---------------------------------------------------------------------------

class SnapshotBackend:
    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        raise NotImplementedError

    def get_bytes(self, key: str) -> Optional[bytes]:
        raise NotImplementedError

    def public_url(self, key: str) -> Optional[str]:
        return None


class S3SnapshotBackend(SnapshotBackend):
    def __init__(self, bucket: str, public_base_url: Optional[str] = None):
        import boto3

        self.bucket = bucket
        self._s3 = boto3.client("s3")
        self.public_base_url = public_base_url

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        self._s3.put_object(
            Bucket=self.bucket, Key=key, Body=data, ContentType=content_type
        )

    def get_bytes(self, key: str) -> Optional[bytes]:
        try:
            resp = self._s3.get_object(Bucket=self.bucket, Key=key)
        except Exception:
            return None
        return resp["Body"].read()

    def public_url(self, key: str) -> Optional[str]:
        if self.public_base_url:
            return f"{self.public_base_url.rstrip('/')}/{key}"
        return None


class LocalSnapshotBackend(SnapshotBackend):
    def __init__(self, base_dir: str):
        self.base = Path(base_dir).resolve()
        self.base.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        target = (self.base / key).resolve()
        target.relative_to(self.base)  # guards against key traversal
        return target

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get_bytes(self, key: str) -> Optional[bytes]:
        path = self._path(key)
        if not path.exists():
            return None
        return path.read_bytes()

    def public_url(self, key: str) -> Optional[str]:
        return f"file://{self._path(key)}"


def get_snapshot_backend() -> SnapshotBackend:
    bucket = os.environ.get("EMBEDDING_SNAPSHOT_BUCKET", "").strip()
    if bucket:
        public_base = os.environ.get("EMBEDDING_SNAPSHOT_PUBLIC_URL", "").strip() or None
        return S3SnapshotBackend(bucket, public_base_url=public_base)
    local_dir = os.environ.get("EMBEDDING_SNAPSHOT_DIR", "/tmp/mdf-embedding-snapshot")
    return LocalSnapshotBackend(local_dir)


# ---------------------------------------------------------------------------
# Sidecar row shape
# ---------------------------------------------------------------------------

def _sidecar_row(record: Dict[str, Any]) -> Dict[str, Any]:
    """Shape of each row in the JSON sidecar — enough for a result card.

    Intentionally small so the whole sidecar stays a reasonable download for
    browser use (a few MB for thousands of rows).
    """
    from v2.metadata import parse_metadata

    meta = parse_metadata(record)
    description = meta.description or ""
    return {
        "source_id": record.get("source_id"),
        "version": record.get("version"),
        "title": meta.title,
        "authors": [a.name for a in meta.authors][:8],
        "keywords": meta.keywords[:12],
        "description": description[:400],
        "publication_year": meta.publication_year,
        "organization": record.get("organization"),
        "doi": record.get("doi") or record.get("dataset_doi"),
        "embedding_model": record.get("embedding_model"),
    }


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def _coerce_embedding(raw: Any) -> Optional[List[float]]:
    """Normalize the embedding field from Dynamo/SQLite into a list[float]."""
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if not isinstance(raw, list) or not raw:
        return None
    try:
        return [float(x) for x in raw]
    except (TypeError, ValueError):
        return None


def build_snapshot(limit: int = 100000) -> Dict[str, Any]:
    """Read all published records with embeddings and write a new snapshot.

    Returns a summary dict suitable for surfacing to curators. Safe to call
    repeatedly — each build gets its own content-hashed filenames, and the
    `current.json` pointer swaps atomically.
    """
    from v2.store import get_store

    store = get_store()
    all_subs = store.list_all(limit=limit)

    rows: List[Dict[str, Any]] = []
    vectors: List[List[float]] = []
    dims: Optional[int] = None
    models: Dict[str, int] = {}
    skipped_stale_dims = 0

    for record in all_subs:
        if record.get("status") != "published":
            continue
        # Skip datasets that aren't flagged as the latest version — avoid showing
        # superseded titles in semantic results.
        mdata = record.get("dataset_mdata") or {}
        if isinstance(mdata, dict) and mdata.get("latest") is False:
            continue

        vec = _coerce_embedding(record.get("title_description_embedding"))
        if not vec:
            continue
        if dims is None:
            dims = len(vec)
        elif len(vec) != dims:
            # Mixed-dim snapshot would break the cosine scan; drop outliers
            skipped_stale_dims += 1
            continue

        vectors.append(vec)
        rows.append(_sidecar_row(record))
        model_name = record.get("embedding_model") or "unknown"
        models[model_name] = models.get(model_name, 0) + 1

    if not vectors:
        logger.info("Embedding snapshot build: no vectors to write")
        return {
            "success": True,
            "count": 0,
            "dims": 0,
            "skipped_stale_dims": skipped_stale_dims,
            "built_at": _utc_now(),
        }

    bin_bytes = _pack_vectors(vectors)
    sidecar_bytes = json.dumps(rows).encode("utf-8")

    # Content hash over the packed bytes — identical corpora produce identical keys
    # so a no-op rebuild doesn't churn downstream caches.
    sha = hashlib.sha256(bin_bytes).hexdigest()[:16]
    bin_key = f"{SNAPSHOT_PREFIX}index-{sha}.bin"
    json_key = f"{SNAPSHOT_PREFIX}index-{sha}.json"

    backend = get_snapshot_backend()
    backend.put_bytes(bin_key, bin_bytes, "application/octet-stream")
    backend.put_bytes(json_key, sidecar_bytes, "application/json")

    # Pick a single "primary" model for the pointer (most common)
    primary_model = max(models.items(), key=lambda kv: kv[1])[0]

    pointer = {
        "bin": bin_key,
        "json": json_key,
        "count": len(vectors),
        "dims": dims,
        "model": primary_model,
        "models": models,
        "built_at": _utc_now(),
        "sha": sha,
    }
    backend.put_bytes(
        SNAPSHOT_POINTER_KEY, json.dumps(pointer).encode("utf-8"), "application/json"
    )

    # Bust the in-process snapshot cache so the next search picks up the build
    invalidate_cached_snapshot()

    return {
        "success": True,
        **pointer,
        "skipped_stale_dims": skipped_stale_dims,
        "public_urls": {
            "bin": backend.public_url(bin_key),
            "json": backend.public_url(json_key),
            "pointer": backend.public_url(SNAPSHOT_POINTER_KEY),
        },
    }


def _pack_vectors(vectors: List[List[float]]) -> bytes:
    """Pack a list of equal-length float lists into a contiguous Float32 buffer."""
    n = len(vectors)
    if n == 0:
        return b""
    dims = len(vectors[0])
    fmt = f"<{n * dims}f"
    flat = [v for vec in vectors for v in vec]
    return struct.pack(fmt, *flat)


# ---------------------------------------------------------------------------
# Load (with in-process cache, keyed by pointer sha)
# ---------------------------------------------------------------------------

class LoadedSnapshot:
    """In-memory view of a snapshot — vectors + sidecar rows."""

    def __init__(
        self,
        vectors: Any,  # numpy.ndarray when numpy is available, else list[list[float]]
        rows: List[Dict[str, Any]],
        pointer: Dict[str, Any],
    ):
        self.vectors = vectors
        self.rows = rows
        self.pointer = pointer
        self.count = pointer.get("count", len(rows))
        self.dims = pointer.get("dims")
        self.model = pointer.get("model")
        self.sha = pointer.get("sha")
        self.built_at = pointer.get("built_at")


_snapshot_lock = threading.Lock()
_cached_snapshot: Optional[Tuple[str, LoadedSnapshot]] = None


def invalidate_cached_snapshot() -> None:
    global _cached_snapshot
    with _snapshot_lock:
        _cached_snapshot = None


def _unpack_vectors(data: bytes, dims: int) -> Any:
    """Parse packed Float32 buffer. Prefers numpy; falls back to list[list[float]]."""
    try:
        import numpy as np

        arr = np.frombuffer(data, dtype="<f4")
        if dims <= 0 or arr.size % dims != 0:
            return arr.reshape((0, 0))
        return arr.reshape((-1, dims))
    except ImportError:
        n = (len(data) // 4) // dims if dims else 0
        flat = struct.unpack(f"<{n * dims}f", data)
        return [list(flat[i * dims:(i + 1) * dims]) for i in range(n)]


def load_snapshot() -> Optional[LoadedSnapshot]:
    """Load the current snapshot, caching by its content sha.

    Returns None if no snapshot has been built yet.
    """
    global _cached_snapshot

    backend = get_snapshot_backend()
    pointer_bytes = backend.get_bytes(SNAPSHOT_POINTER_KEY)
    if not pointer_bytes:
        return None

    try:
        pointer = json.loads(pointer_bytes.decode("utf-8"))
    except Exception:
        logger.warning("Invalid embedding snapshot pointer", exc_info=True)
        return None

    sha = pointer.get("sha") or "unknown"
    with _snapshot_lock:
        if _cached_snapshot and _cached_snapshot[0] == sha:
            return _cached_snapshot[1]

    bin_key = pointer.get("bin")
    json_key = pointer.get("json")
    dims = int(pointer.get("dims") or 0)
    if not bin_key or not json_key or dims <= 0:
        return None

    bin_data = backend.get_bytes(bin_key)
    sidecar_data = backend.get_bytes(json_key)
    if bin_data is None or sidecar_data is None:
        logger.warning("Snapshot pointer references missing artifacts: %s", pointer)
        return None

    try:
        rows = json.loads(sidecar_data.decode("utf-8"))
    except Exception:
        logger.warning("Invalid sidecar JSON in snapshot", exc_info=True)
        return None

    vectors = _unpack_vectors(bin_data, dims)
    snapshot = LoadedSnapshot(vectors=vectors, rows=rows, pointer=pointer)

    with _snapshot_lock:
        _cached_snapshot = (sha, snapshot)
    return snapshot


def read_pointer() -> Optional[Dict[str, Any]]:
    """Read the current snapshot pointer without loading the full blob."""
    backend = get_snapshot_backend()
    raw = backend.get_bytes(SNAPSHOT_POINTER_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None
