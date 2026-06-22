import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from v2.config import AWS_REGION, DYNAMO_ENDPOINT_URL, DYNAMO_STREAMS_TABLE


class StreamStore:
    def create_stream(self, record: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    def get_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def append_stream(self, stream_id: str, file_count: int, total_bytes: int, last_file: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def close_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def update_stream_metadata(self, stream_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """List all streams (for search)."""
        raise NotImplementedError


class DynamoStreamStore(StreamStore):
    def __init__(self):
        import boto3
        from boto3.dynamodb.conditions import Key

        resource_kwargs = {"region_name": AWS_REGION}
        if DYNAMO_ENDPOINT_URL:
            resource_kwargs["endpoint_url"] = DYNAMO_ENDPOINT_URL
        self._resource = boto3.resource("dynamodb", **resource_kwargs)
        self.table = self._resource.Table(DYNAMO_STREAMS_TABLE)
        self._key = Key

    def create_stream(self, record: Dict[str, Any]) -> Dict[str, Any]:
        # Serialize metadata if dict
        item = dict(record)
        if isinstance(item.get("metadata"), dict):
            item["metadata"] = json.dumps(item["metadata"])
        if isinstance(item.get("last_file"), dict):
            item["last_file"] = json.dumps(item["last_file"])
        self.table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(stream_id)",
        )
        return record

    def _deserialize(self, item: Dict[str, Any]) -> Dict[str, Any]:
        if item.get("metadata") and isinstance(item["metadata"], str):
            try:
                item["metadata"] = json.loads(item["metadata"])
            except Exception:
                pass
        if item.get("last_file") and isinstance(item["last_file"], str):
            try:
                item["last_file"] = json.loads(item["last_file"])
            except Exception:
                pass
        return item

    def get_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        resp = self.table.get_item(Key={"stream_id": stream_id})
        item = resp.get("Item")
        return self._deserialize(item) if item else None

    def append_stream(self, stream_id: str, file_count: int, total_bytes: int, last_file: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        update_expr = (
            "SET file_count = file_count + :fc, total_bytes = total_bytes + :tb, "
            "last_append_at = :now, updated_at = :now"
        )
        expr_values = {
            ":fc": file_count,
            ":tb": total_bytes,
            ":now": now,
        }
        if last_file:
            update_expr += ", last_file = :lf"
            expr_values[":lf"] = json.dumps(last_file) if isinstance(last_file, dict) else last_file

        self.table.update_item(
            Key={"stream_id": stream_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
        )
        return self.get_stream(stream_id)

    def close_stream(self, stream_id: str) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.table.update_item(
            Key={"stream_id": stream_id},
            UpdateExpression="SET #status = :status, updated_at = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": "closed", ":now": now},
        )
        return self.get_stream(stream_id)

    def update_stream_metadata(self, stream_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        stream = self.get_stream(stream_id)
        if not stream:
            return None

        existing_metadata = stream.get("metadata") or {}
        if isinstance(existing_metadata, str):
            try:
                existing_metadata = json.loads(existing_metadata)
            except Exception:
                existing_metadata = {}
        existing_metadata.update(updates)

        self.table.update_item(
            Key={"stream_id": stream_id},
            UpdateExpression="SET metadata = :meta, updated_at = :now",
            ExpressionAttributeValues={
                ":meta": json.dumps(existing_metadata),
                ":now": now,
            },
        )
        return self.get_stream(stream_id)

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        last_key = None
        while len(items) < limit:
            page_limit = min(limit - len(items), 1000)
            kwargs: Dict[str, Any] = {"Limit": page_limit}
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        return [self._deserialize(item) for item in items[:limit]]


class SqliteStreamStore(StreamStore):
    def __init__(self, path: Optional[str] = None):
        db_path = path or os.environ.get("SQLITE_PATH", "/tmp/mdf_connect_v2.db")
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
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
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
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
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with self.conn:
            self.conn.execute(
                "UPDATE streams SET status = ?, updated_at = ? WHERE stream_id = ?",
                ("closed", now, stream_id),
            )
        return self.get_stream(stream_id)

    def update_stream_metadata(self, stream_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update stream metadata fields (like DOI, published_at)."""
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
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


def get_stream_store() -> StreamStore:
    backend = os.environ.get("STORE_BACKEND", "dynamo").lower()
    if backend == "sqlite":
        return SqliteStreamStore()
    return DynamoStreamStore()
