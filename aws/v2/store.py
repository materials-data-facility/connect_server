import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from v2.config import AWS_REGION, DYNAMO_ENDPOINT_URL, DYNAMO_SUBMISSIONS_TABLE


class SubmissionStore:
    def get(self, source_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get a submission, optionally by version. If no version, gets latest."""
        if version:
            return self.get_submission(source_id, version)
        # Get latest version
        versions = self.list_versions(source_id)
        if not versions:
            return None
        # Sort by version descending and return latest
        versions.sort(key=lambda x: x.get("version", "0"), reverse=True)
        return versions[0]

    def get_submission(self, source_id: str, version: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def put_submission(self, record: Dict[str, Any]) -> None:
        raise NotImplementedError

    def update_status(self, source_id: str, version: str, status: str) -> None:
        raise NotImplementedError

    def list_by_user(
        self, user_id: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        raise NotImplementedError

    def list_by_org(
        self, organization: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        raise NotImplementedError

    def list_by_status(self, statuses: List[str], limit: int = 100) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """List all submissions (for search)."""
        raise NotImplementedError


class DynamoSubmissionStore(SubmissionStore):
    def __init__(self):
        import boto3
        from boto3.dynamodb.conditions import Key

        resource_kwargs = {"region_name": AWS_REGION}
        if DYNAMO_ENDPOINT_URL:
            resource_kwargs["endpoint_url"] = DYNAMO_ENDPOINT_URL
        self._resource = boto3.resource("dynamodb", **resource_kwargs)
        self.table = self._resource.Table(DYNAMO_SUBMISSIONS_TABLE)
        self._key = Key

    def get_submission(self, source_id: str, version: str) -> Optional[Dict[str, Any]]:
        resp = self.table.get_item(Key={"source_id": source_id, "version": version})
        return resp.get("Item")

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        resp = self.table.query(KeyConditionExpression=self._key("source_id").eq(source_id))
        return resp.get("Items", [])

    def put_submission(self, record: Dict[str, Any]) -> None:
        self.table.put_item(
            Item=record,
            ConditionExpression="attribute_not_exists(source_id) AND attribute_not_exists(version)",
        )

    def update_status(self, source_id: str, version: str, status: str) -> None:
        now = datetime.utcnow().isoformat("T") + "Z"
        self.table.update_item(
            Key={"source_id": source_id, "version": version},
            UpdateExpression="SET #status = :status, updated_at = :updated_at",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={":status": status, ":updated_at": now},
        )

    def list_by_user(self, user_id: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None):
        kwargs = {
            "IndexName": os.environ.get("GSI_USER_INDEX", "user-submissions"),
            "KeyConditionExpression": self._key("user_id").eq(user_id),
            "Limit": limit,
            "ScanIndexForward": False,
        }
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = self.table.query(**kwargs)
        return resp.get("Items", []), resp.get("LastEvaluatedKey")

    def list_by_org(self, organization: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None):
        kwargs = {
            "IndexName": os.environ.get("GSI_ORG_INDEX", "org-submissions"),
            "KeyConditionExpression": self._key("organization").eq(organization),
            "Limit": limit,
            "ScanIndexForward": False,
        }
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = self.table.query(**kwargs)
        return resp.get("Items", []), resp.get("LastEvaluatedKey")

    def list_by_status(self, statuses: List[str], limit: int = 100) -> List[Dict[str, Any]]:
        # Not implemented for Dynamo in local mode.
        return []

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        # Scan is expensive but acceptable for search
        resp = self.table.scan(Limit=limit)
        return resp.get("Items", [])


class SqliteSubmissionStore(SubmissionStore):
    def __init__(self, path: Optional[str] = None):
        db_path = path or os.environ.get("SQLITE_PATH", "/tmp/mdf_connect_v2.db")
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS submissions (
                    source_id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    versioned_source_id TEXT,
                    user_id TEXT,
                    user_email TEXT,
                    organization TEXT,
                    status TEXT,
                    dataset_mdata TEXT,
                    test INTEGER,
                    created_at TEXT,
                    updated_at TEXT,
                    action_id TEXT,
                    doi TEXT,
                    published_at TEXT,
                    approved_at TEXT,
                    approved_by TEXT,
                    rejected_at TEXT,
                    rejected_by TEXT,
                    rejection_reason TEXT,
                    curation_history TEXT,
                    PRIMARY KEY (source_id, version)
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_submissions_user ON submissions(user_id, updated_at)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_submissions_org ON submissions(organization, source_id)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_submissions_status ON submissions(status)"
            )

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        data = dict(row)
        # Deserialize JSON fields
        if data.get("dataset_mdata"):
            try:
                data["dataset_mdata"] = json.loads(data["dataset_mdata"])
            except Exception:
                pass
        if data.get("curation_history"):
            try:
                data["curation_history"] = json.loads(data["curation_history"])
            except Exception:
                pass
        return data

    def get_submission(self, source_id: str, version: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM submissions WHERE source_id = ? AND version = ?",
            (source_id, version),
        )
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM submissions WHERE source_id = ?",
            (source_id,),
        )
        return [self._row_to_dict(row) for row in cur.fetchall()]

    def put_submission(self, record: Dict[str, Any]) -> None:
        # Serialize JSON fields
        dataset_mdata = record.get("dataset_mdata")
        if isinstance(dataset_mdata, dict):
            dataset_mdata = json.dumps(dataset_mdata)

        curation_history = record.get("curation_history")
        if isinstance(curation_history, list):
            curation_history = json.dumps(curation_history)

        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO submissions (
                    source_id, version, versioned_source_id, user_id, user_email,
                    organization, status, dataset_mdata, test, created_at, updated_at, action_id,
                    doi, published_at, approved_at, approved_by, rejected_at, rejected_by,
                    rejection_reason, curation_history
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.get("source_id"),
                    record.get("version"),
                    record.get("versioned_source_id"),
                    record.get("user_id"),
                    record.get("user_email"),
                    record.get("organization"),
                    record.get("status"),
                    dataset_mdata,
                    int(record.get("test") or 0),
                    record.get("created_at"),
                    record.get("updated_at"),
                    record.get("action_id"),
                    record.get("doi"),
                    record.get("published_at"),
                    record.get("approved_at"),
                    record.get("approved_by"),
                    record.get("rejected_at"),
                    record.get("rejected_by"),
                    record.get("rejection_reason"),
                    curation_history,
                ),
            )

    def update_status(self, source_id: str, version: str, status: str) -> None:
        now = datetime.utcnow().isoformat("T") + "Z"
        with self.conn:
            self.conn.execute(
                "UPDATE submissions SET status = ?, updated_at = ? WHERE source_id = ? AND version = ?",
                (status, now, source_id, version),
            )

    def list_by_user(self, user_id: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None):
        offset = int(start_key.get("offset")) if start_key and "offset" in start_key else 0
        cur = self.conn.execute(
            """
            SELECT * FROM submissions
            WHERE user_id = ?
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?
            """,
            (user_id, limit, offset),
        )
        rows = [self._row_to_dict(row) for row in cur.fetchall()]
        next_key = {"offset": offset + limit} if len(rows) == limit else None
        return rows, next_key

    def list_by_org(self, organization: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None):
        offset = int(start_key.get("offset")) if start_key and "offset" in start_key else 0
        cur = self.conn.execute(
            """
            SELECT * FROM submissions
            WHERE organization = ?
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?
            """,
            (organization, limit, offset),
        )
        rows = [self._row_to_dict(row) for row in cur.fetchall()]
        next_key = {"offset": offset + limit} if len(rows) == limit else None
        return rows, next_key

    def list_by_status(self, statuses: List[str], limit: int = 100) -> List[Dict[str, Any]]:
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        query = (
            f"SELECT * FROM submissions WHERE status IN ({placeholders}) "
            "ORDER BY updated_at DESC LIMIT ?"
        )
        cur = self.conn.execute(query, (*statuses, limit))
        return [self._row_to_dict(row) for row in cur.fetchall()]

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM submissions ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        return [self._row_to_dict(row) for row in cur.fetchall()]


class TinyDBSubmissionStore(SubmissionStore):
    def __init__(self, path: Optional[str] = None):
        try:
            from tinydb import TinyDB
        except Exception as exc:
            raise RuntimeError("TinyDB is not installed. pip install tinydb") from exc

        db_path = path or os.environ.get("TINYDB_PATH", "/tmp/mdf_connect_v2.json")
        self.db = TinyDB(db_path)
        self.table = self.db.table("submissions")

    def get_submission(self, source_id: str, version: str) -> Optional[Dict[str, Any]]:
        from tinydb import Query

        query = Query()
        return self.table.get((query.source_id == source_id) & (query.version == version))

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        from tinydb import Query

        query = Query()
        return self.table.search(query.source_id == source_id)

    def put_submission(self, record: Dict[str, Any]) -> None:
        from tinydb import Query

        query = Query()
        existing = self.table.get(
            (query.source_id == record.get("source_id"))
            & (query.version == record.get("version"))
        )
        if existing:
            raise ValueError("Record already exists for source_id/version")
        self.table.insert(record)

    def update_status(self, source_id: str, version: str, status: str) -> None:
        from tinydb import Query

        now = datetime.utcnow().isoformat("T") + "Z"
        query = Query()
        self.table.update(
            {"status": status, "updated_at": now},
            (query.source_id == source_id) & (query.version == version),
        )

    def list_by_user(self, user_id: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None):
        from tinydb import Query

        query = Query()
        rows = self.table.search(query.user_id == user_id)
        rows.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        offset = int(start_key.get("offset")) if start_key and "offset" in start_key else 0
        page = rows[offset : offset + limit]
        next_key = {"offset": offset + limit} if len(page) == limit else None
        return page, next_key

    def list_by_org(self, organization: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None):
        from tinydb import Query

        query = Query()
        rows = self.table.search(query.organization == organization)
        rows.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        offset = int(start_key.get("offset")) if start_key and "offset" in start_key else 0
        page = rows[offset : offset + limit]
        next_key = {"offset": offset + limit} if len(page) == limit else None
        return page, next_key

    def list_by_status(self, statuses: List[str], limit: int = 100) -> List[Dict[str, Any]]:
        if not statuses:
            return []
        from tinydb import Query

        query = Query()
        rows = self.table.search(query.status.one_of(statuses))
        rows.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        return rows[:limit]

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        rows = self.table.all()
        rows.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        return rows[:limit]


def get_store() -> SubmissionStore:
    backend = os.environ.get("STORE_BACKEND", "dynamo").lower()
    if backend == "sqlite":
        return SqliteSubmissionStore()
    if backend == "tinydb":
        return TinyDBSubmissionStore()
    return DynamoSubmissionStore()


def parse_pagination_key(key_str: Optional[str]) -> Optional[Dict[str, Any]]:
    if not key_str:
        return None
    try:
        return json.loads(key_str)
    except Exception:
        return None


def serialize_pagination_key(key: Optional[Dict[str, Any]]) -> Optional[str]:
    if not key:
        return None
    return json.dumps(key)
