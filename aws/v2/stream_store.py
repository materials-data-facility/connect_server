import json
import os
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


class StreamStore:
    def create_stream(self, record: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def get_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def append_stream(self, stream_id: str, file_count: int, total_bytes: int, last_file: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def close_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """List all streams (for search)."""
        raise NotImplementedError


class SqliteStreamStore(StreamStore):
    def __init__(self, path: Optional[str] = None):
        db_path = path or os.environ.get("SQLITE_PATH", "/tmp/mdf_connect_v2.db")
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS streams (
                    stream_id TEXT PRIMARY KEY,
                    lab_id TEXT,
                    title TEXT,
                    status TEXT,
                    file_count INTEGER,
                    total_bytes INTEGER,
                    last_append_at TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    user_id TEXT,
                    organization TEXT,
                    last_file TEXT,
                    metadata TEXT
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_streams_user ON streams(user_id, updated_at)"
            )

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        if data.get("metadata"):
            try:
                data["metadata"] = json.loads(data["metadata"])
            except Exception:
                pass
        if data.get("last_file"):
            try:
                data["last_file"] = json.loads(data["last_file"])
            except Exception:
                pass
        return data

    def create_stream(self, record: Dict[str, Any]) -> Dict[str, Any]:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO streams (
                    stream_id, lab_id, title, status, file_count, total_bytes,
                    last_append_at, created_at, updated_at, user_id, organization,
                    last_file, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.get("stream_id"),
                    record.get("lab_id"),
                    record.get("title"),
                    record.get("status"),
                    record.get("file_count"),
                    record.get("total_bytes"),
                    record.get("last_append_at"),
                    record.get("created_at"),
                    record.get("updated_at"),
                    record.get("user_id"),
                    record.get("organization"),
                    json.dumps(record.get("last_file")) if record.get("last_file") else None,
                    json.dumps(record.get("metadata")) if record.get("metadata") else None,
                ),
            )
        return record

    def get_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM streams WHERE stream_id = ?",
            (stream_id,),
        )
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    def append_stream(self, stream_id: str, file_count: int, total_bytes: int, last_file: Optional[Dict[str, Any]] = None):
        now = datetime.utcnow().isoformat("T") + "Z"
        with self.conn:
            self.conn.execute(
                """
                UPDATE streams
                SET file_count = file_count + ?, total_bytes = total_bytes + ?,
                    last_append_at = ?, updated_at = ?, last_file = ?
                WHERE stream_id = ?
                """,
                (
                    file_count,
                    total_bytes,
                    now,
                    now,
                    json.dumps(last_file) if last_file else None,
                    stream_id,
                ),
            )
        return self.get_stream(stream_id)

    def close_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        now = datetime.utcnow().isoformat("T") + "Z"
        with self.conn:
            self.conn.execute(
                "UPDATE streams SET status = ?, updated_at = ? WHERE stream_id = ?",
                ("closed", now, stream_id),
            )
        return self.get_stream(stream_id)

    def update_stream_metadata(self, stream_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update stream metadata fields (like DOI, published_at)."""
        now = datetime.utcnow().isoformat("T") + "Z"
        stream = self.get_stream(stream_id)
        if not stream:
            return None

        # Merge updates into existing metadata
        existing_metadata = stream.get("metadata") or {}
        if isinstance(existing_metadata, str):
            try:
                existing_metadata = json.loads(existing_metadata)
            except Exception:
                existing_metadata = {}

        existing_metadata.update(updates)

        with self.conn:
            self.conn.execute(
                "UPDATE streams SET metadata = ?, updated_at = ? WHERE stream_id = ?",
                (json.dumps(existing_metadata), now, stream_id),
            )
        return self.get_stream(stream_id)

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM streams ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        return [self._row_to_dict(row) for row in cur.fetchall()]


class TinyDBStreamStore(StreamStore):
    def __init__(self, path: Optional[str] = None):
        try:
            from tinydb import TinyDB
        except Exception as exc:
            raise RuntimeError("TinyDB is not installed. pip install tinydb") from exc

        db_path = path or os.environ.get("TINYDB_PATH", "/tmp/mdf_connect_v2.json")
        self.db = TinyDB(db_path)
        self.table = self.db.table("streams")

    def create_stream(self, record: Dict[str, Any]) -> Dict[str, Any]:
        from tinydb import Query

        query = Query()
        existing = self.table.get(query.stream_id == record.get("stream_id"))
        if existing:
            raise ValueError("Stream already exists")
        self.table.insert(record)
        return record

    def get_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        from tinydb import Query

        query = Query()
        return self.table.get(query.stream_id == stream_id)

    def append_stream(self, stream_id: str, file_count: int, total_bytes: int, last_file: Optional[Dict[str, Any]] = None):
        from tinydb import Query

        now = datetime.utcnow().isoformat("T") + "Z"
        query = Query()
        stream = self.table.get(query.stream_id == stream_id)
        if not stream:
            return None
        updated = {
            "file_count": int(stream.get("file_count", 0)) + file_count,
            "total_bytes": int(stream.get("total_bytes", 0)) + total_bytes,
            "last_append_at": now,
            "updated_at": now,
            "last_file": last_file,
        }
        self.table.update(updated, query.stream_id == stream_id)
        return self.table.get(query.stream_id == stream_id)

    def close_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        from tinydb import Query

        now = datetime.utcnow().isoformat("T") + "Z"
        query = Query()
        self.table.update({"status": "closed", "updated_at": now}, query.stream_id == stream_id)
        return self.table.get(query.stream_id == stream_id)

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        rows = self.table.all()
        rows.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        return rows[:limit]


def get_stream_store() -> StreamStore:
    backend = os.environ.get("STORE_BACKEND", "dynamo").lower()
    if backend == "tinydb":
        return TinyDBStreamStore()
    return SqliteStreamStore()
