import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from v2.config import AWS_REGION, DYNAMO_ENDPOINT_URL, DYNAMO_SUBMISSIONS_TABLE
from v2.submission_utils import latest_version

# ---------------------------------------------------------------------------
# Per-dataset publish lock (B-16)
#
# Two publish jobs for different versions of the same dataset can run at the
# same time (SQS is not ordered and the worker's reserved concurrency is > 1).
# The publish critical section is read-then-write — read every version, decide
# whether this version owns the single search entry, write the entry, flip the
# status — so interleaved jobs can leave the index describing an older version.
#
# The queue-level fix is a FIFO queue with MessageGroupId=source_id, but the
# queue is owned by the CloudFormation template. This lock gives the same
# serialization from inside the worker: mutual exclusion per source_id, with an
# expiry so a crashed worker cannot deadlock a dataset's publishes.
# ---------------------------------------------------------------------------

# Sentinel sort key for the lock item in the submissions table. It shares the
# table (adding one is a template change) and is filtered out of every read that
# enumerates versions, so it is invisible to the rest of the system. Chosen to
# be un-collidable with a real version string.
PUBLISH_LOCK_VERSION = "__publish_lock__"

# TTL of a held lock. Must exceed the worker Lambda timeout (120s) so a running
# worker never has its lock stolen, and must not exceed the queue visibility
# timeout (180s) by much, so a redelivered message after a worker crash finds
# the lock already expired rather than burning a delivery attempt.
DEFAULT_PUBLISH_LOCK_TTL_SECONDS = 180.0

# How long a publish job waits in-process for a contended lock before giving up
# and letting the queue retry it. Publishes take seconds, so a short wait
# resolves nearly all contention without spending an SQS delivery attempt
# (maxReceiveCount is 3).
DEFAULT_PUBLISH_LOCK_WAIT_SECONDS = 15.0


class PublishLockUnavailable(RuntimeError):
    """Another worker holds the publish lock for this dataset."""


def _without_lock_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Drop publish-lock sentinel items from a table read.

    The lock shares the submissions table, so every read that enumerates rows
    for a dataset must skip it — otherwise it would masquerade as a version.
    """
    return [i for i in items if i.get("version") != PUBLISH_LOCK_VERSION]


def is_reserved_version(version: Any) -> bool:
    """True for the internal sentinel that is never a real submission version."""
    return version == PUBLISH_LOCK_VERSION


def _reject_reserved_version(version: Any) -> None:
    """Guard writes against the lock sentinel.

    ``version`` reaches the store from URL paths and request bodies, so a caller
    could address the lock item directly (``/submissions/{id}/__publish_lock__``)
    and overwrite or delete a held lock. Reads of it return None; writes raise.
    """
    if is_reserved_version(version):
        raise ValueError(f"{PUBLISH_LOCK_VERSION!r} is a reserved internal version")


def _is_conditional_check_failure(exc: Exception) -> bool:
    """True for a DynamoDB ConditionalCheckFailedException (lock contention)."""
    if exc.__class__.__name__ == "ConditionalCheckFailedException":
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = (response.get("Error") or {}).get("Code")
        return code == "ConditionalCheckFailedException"
    return False


def _publish_lock_ttl_seconds() -> float:
    return _float_env("PUBLISH_LOCK_TTL_SECONDS", DEFAULT_PUBLISH_LOCK_TTL_SECONDS)


def _publish_lock_wait_seconds() -> float:
    return _float_env("PUBLISH_LOCK_WAIT_SECONDS", DEFAULT_PUBLISH_LOCK_WAIT_SECONDS)


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        return default


@contextmanager
def publish_lock(
    store: "SubmissionStore",
    source_id: str,
    owner: Optional[str] = None,
    ttl_seconds: Optional[float] = None,
    wait_seconds: Optional[float] = None,
):
    """Hold the publish lock for ``source_id`` for the duration of the block.

    Raises :class:`PublishLockUnavailable` if the lock cannot be taken within
    ``wait_seconds``; the caller is expected to turn that into a retryable job
    failure (the whole publish is idempotent, so retrying is safe).

    The lock is always released in ``finally`` — and self-expires anyway, so a
    worker killed mid-publish delays the next publish of that dataset by at most
    the TTL instead of blocking it forever.
    """
    owner = owner or f"publish-{uuid.uuid4()}"
    ttl = _publish_lock_ttl_seconds() if ttl_seconds is None else ttl_seconds
    wait = _publish_lock_wait_seconds() if wait_seconds is None else wait_seconds

    deadline = time.monotonic() + wait
    while True:
        if store.acquire_publish_lock(source_id, owner, ttl_seconds=ttl):
            break
        if time.monotonic() >= deadline:
            raise PublishLockUnavailable(
                f"another publish job holds the lock for {source_id} "
                f"(waited {wait:g}s)"
            )
        time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))

    try:
        yield owner
    finally:
        try:
            store.release_publish_lock(source_id, owner)
        except Exception:  # pragma: no cover - release is best effort; TTL covers it
            pass


class SubmissionStore:
    def get(self, source_id: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get a submission, optionally by version. If no version, gets latest."""
        if version:
            return self.get_submission(source_id, version)
        # Get latest version using semantic version sorting
        versions = self.list_versions(source_id)
        if not versions:
            return None
        latest = latest_version(versions)
        if not latest:
            return None
        for item in versions:
            if item.get("version") == latest:
                return item
        return None

    def get_submission(self, source_id: str, version: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def get_by_legacy_source_id(self, legacy_source_id: str) -> Optional[Dict[str, Any]]:
        """Resolve a record by its pre-migration (v1) source_id.

        Datasets migrated from the legacy MDF index carry a top-level
        ``legacy_source_id`` (the original versioned v1 id, e.g. ``foo_v2``)
        while their canonical v2 ``source_id`` is the version-independent name.
        This lets old links/DOIs that reference a v1 id keep resolving after the
        source_name→source_id promotion. Returns the single matching record (a
        v1 id maps to exactly one record) or None.
        """
        raise NotImplementedError

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def put_submission(self, record: Dict[str, Any]) -> None:
        raise NotImplementedError

    def upsert_submission(self, record: Dict[str, Any]) -> None:
        """Put a submission without condition check (for updates like curation)."""
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

    def update_profile(self, source_id: str, version: str, profile_json: str) -> None:
        raise NotImplementedError

    def scan_by_transfer_status(self, transfer_status: str) -> List[Dict[str, Any]]:
        """Return submissions with the given transfer_status (e.g. 'active')."""
        raise NotImplementedError

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """List all submissions (for search)."""
        raise NotImplementedError

    def update_embedding(
        self,
        source_id: str,
        version: str,
        embedding: List[float],
        model: str,
    ) -> None:
        """Persist a dataset embedding with the model that produced it."""
        raise NotImplementedError

    _ALLOWED_COUNTERS = {"view_count", "download_count"}

    def increment_counter(self, source_id: str, version: str, counter: str) -> None:
        """Atomically increment a counter (view_count, download_count)."""
        if counter not in self._ALLOWED_COUNTERS:
            raise ValueError(f"Invalid counter name: {counter}")
        raise NotImplementedError

    def acquire_publish_lock(
        self, source_id: str, owner: str, ttl_seconds: float = DEFAULT_PUBLISH_LOCK_TTL_SECONDS,
    ) -> bool:
        """Take the per-dataset publish lock. True if acquired, False if held.

        Must be atomic against concurrent workers (conditional write), and must
        take over a lock whose expiry has passed.
        """
        raise NotImplementedError

    def release_publish_lock(self, source_id: str, owner: str) -> bool:
        """Release the lock if ``owner`` still holds it. True if released."""
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
        # ConsistentRead on the base-table reads: a metadata edit reads its own
        # write immediately (new version row -> card refetch), and the default
        # eventually-consistent read can miss it for seconds, serving the OLD
        # latest right after a successful save. Write volume is tiny (O(1000)
        # edits/year), so the doubled read cost is irrelevant. GSI queries
        # (user/org/status/legacy) cannot be consistent and stay as they are.
        #
        # The publish lock lives in this table under a reserved version; it is
        # not a submission, so a direct read of it returns nothing.
        if is_reserved_version(version):
            return None
        resp = self.table.get_item(
            Key={"source_id": source_id, "version": version},
            ConsistentRead=True,
        )
        return resp.get("Item")

    def get_by_legacy_source_id(self, legacy_source_id: str) -> Optional[Dict[str, Any]]:
        if not legacy_source_id:
            return None
        from v2.config import GSI_LEGACY_INDEX

        try:
            resp = self.table.query(
                IndexName=os.environ.get("GSI_LEGACY_INDEX", GSI_LEGACY_INDEX),
                KeyConditionExpression=self._key("legacy_source_id").eq(legacy_source_id),
                Limit=1,
            )
            items = resp.get("Items", [])
        except Exception:
            # GSI may not exist yet (table created before the index was added)
            # — fall back to a filtered scan on the top-level attribute.
            from boto3.dynamodb.conditions import Attr

            resp = self.table.scan(
                FilterExpression=Attr("legacy_source_id").eq(legacy_source_id),
                Limit=1000,
            )
            items = resp.get("Items", [])
        return items[0] if items else None

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        # ConsistentRead: this resolves "latest" for the card — see get_submission.
        resp = self.table.query(
            KeyConditionExpression=self._key("source_id").eq(source_id),
            ConsistentRead=True,
        )
        return _without_lock_items(resp.get("Items", []))

    def put_submission(self, record: Dict[str, Any]) -> None:
        _reject_reserved_version(record.get("version"))
        self.table.put_item(
            Item=record,
            ConditionExpression="attribute_not_exists(source_id) AND attribute_not_exists(version)",
        )

    def upsert_submission(self, record: Dict[str, Any]) -> None:
        _reject_reserved_version(record.get("version"))
        self.table.put_item(Item=record)

    def update_status(self, source_id: str, version: str, status: str) -> None:
        _reject_reserved_version(version)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
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
        if not statuses:
            return []
        # Query the status-submissions GSI for each requested status,
        # then merge results.  Falls back to scan if the GSI doesn't exist
        # (e.g. table created before the GSI was added).
        index_name = os.environ.get("GSI_STATUS_INDEX", "status-submissions")
        items: List[Dict[str, Any]] = []
        try:
            for status_val in statuses:
                if len(items) >= limit:
                    break
                last_key = None
                while len(items) < limit:
                    kwargs: Dict[str, Any] = {
                        "IndexName": index_name,
                        "KeyConditionExpression": self._key("status").eq(status_val),
                        "ScanIndexForward": False,
                        "Limit": limit - len(items),
                    }
                    if last_key:
                        kwargs["ExclusiveStartKey"] = last_key
                    resp = self.table.query(**kwargs)
                    items.extend(resp.get("Items", []))
                    last_key = resp.get("LastEvaluatedKey")
                    if not last_key:
                        break
        except Exception:
            # GSI may not exist yet — fall back to scan
            items = self._list_by_status_scan(statuses, limit)
        return items[:limit]

    def _list_by_status_scan(self, statuses: List[str], limit: int) -> List[Dict[str, Any]]:
        """Fallback: full table scan filtered by status (used before GSI exists)."""
        from boto3.dynamodb.conditions import Attr
        filter_expr = Attr("status").eq(statuses[0])
        for s in statuses[1:]:
            filter_expr = filter_expr | Attr("status").eq(s)
        items: List[Dict[str, Any]] = []
        last_key = None
        while len(items) < limit:
            kwargs: Dict[str, Any] = {"FilterExpression": filter_expr}
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.table.scan(**kwargs)
            for item in resp.get("Items", []):
                items.append(item)
                if len(items) >= limit:
                    break
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        return items[:limit]

    def scan_by_transfer_status(self, transfer_status: str) -> List[Dict[str, Any]]:
        from boto3.dynamodb.conditions import Attr

        items: List[Dict[str, Any]] = []
        last_key = None
        while True:
            kwargs: Dict[str, Any] = {
                "FilterExpression": Attr("transfer_status").eq(transfer_status),
            }
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        return items

    def update_profile(self, source_id: str, version: str, profile_json: str) -> None:
        _reject_reserved_version(version)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.table.update_item(
            Key={"source_id": source_id, "version": version},
            UpdateExpression="SET dataset_profile = :profile, updated_at = :updated_at",
            ExpressionAttributeValues={":profile": profile_json, ":updated_at": now},
        )

    def update_embedding(
        self,
        source_id: str,
        version: str,
        embedding: List[float],
        model: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        # Dynamo stores floats as Decimal; serialize to JSON so snapshot builder
        # doesn't have to deal with Decimal and float size stays predictable.
        self.table.update_item(
            Key={"source_id": source_id, "version": version},
            UpdateExpression=(
                "SET title_description_embedding = :emb, "
                "embedding_model = :model, "
                "embedding_generated_at = :gen_at, "
                "updated_at = :updated_at"
            ),
            ExpressionAttributeValues={
                ":emb": json.dumps(embedding),
                ":model": model,
                ":gen_at": now,
                ":updated_at": now,
            },
        )

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        # Scan is expensive but acceptable for search
        items: List[Dict[str, Any]] = []
        last_key = None
        while len(items) < limit:
            page_limit = min(limit - len(items), 1000)
            kwargs: Dict[str, Any] = {"Limit": page_limit}
            if last_key:
                kwargs["ExclusiveStartKey"] = last_key
            resp = self.table.scan(**kwargs)
            items.extend(_without_lock_items(resp.get("Items", [])))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
        return items[:limit]

    def increment_counter(self, source_id: str, version: str, counter: str) -> None:
        _reject_reserved_version(version)
        self.table.update_item(
            Key={"source_id": source_id, "version": version},
            UpdateExpression="ADD #counter :one",
            ExpressionAttributeNames={"#counter": counter},
            ExpressionAttributeValues={":one": 1},
        )

    # -- publish lock (B-16) ------------------------------------------------
    #
    # The lock is an item in the submissions table keyed
    # (source_id, PUBLISH_LOCK_VERSION). A conditional PutItem is the atomic
    # primitive: it succeeds only when no lock item exists or the existing one
    # has expired, so exactly one concurrent worker wins. The lock item carries
    # none of the GSI key attributes (user_id / organization / status /
    # legacy_source_id), so it stays out of every sparse index, and the two
    # reads that could see it (list_versions, list_all) filter it out.

    def acquire_publish_lock(
        self, source_id: str, owner: str, ttl_seconds: float = DEFAULT_PUBLISH_LOCK_TTL_SECONDS,
    ) -> bool:
        now = int(time.time())
        expires_at = now + int(ttl_seconds)
        try:
            self.table.put_item(
                Item={
                    "source_id": source_id,
                    "version": PUBLISH_LOCK_VERSION,
                    "record_type": "publish_lock",
                    "lock_owner": owner,
                    "lock_acquired_at": now,
                    "lock_expires_at": expires_at,
                    # DynamoDB TTL attribute (if enabled on the table): sweeps
                    # abandoned lock items long after they stop being honored.
                    "expires_at": expires_at + 86400,
                },
                ConditionExpression=(
                    "attribute_not_exists(source_id) OR lock_expires_at < :now"
                ),
                ExpressionAttributeValues={":now": now},
            )
            return True
        except Exception as exc:
            if _is_conditional_check_failure(exc):
                return False
            raise

    def release_publish_lock(self, source_id: str, owner: str) -> bool:
        try:
            self.table.delete_item(
                Key={"source_id": source_id, "version": PUBLISH_LOCK_VERSION},
                ConditionExpression="lock_owner = :owner",
                ExpressionAttributeValues={":owner": owner},
            )
            return True
        except Exception as exc:
            if _is_conditional_check_failure(exc):
                # Lock already expired and was taken over — not ours to delete.
                return False
            raise


class SqliteSubmissionStore(SubmissionStore):
    def __init__(self, path: Optional[str] = None):
        db_path = path or os.environ.get("SQLITE_PATH", "/tmp/mdf_connect_v2.db")
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
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
                    dataset_doi TEXT,
                    legacy_source_id TEXT,
                    metadata_updated_at TEXT,
                    published_at TEXT,
                    approved_at TEXT,
                    approved_by TEXT,
                    rejected_at TEXT,
                    rejected_by TEXT,
                    rejection_reason TEXT,
                    curation_history TEXT,
                    dataset_profile TEXT,
                    view_count INTEGER DEFAULT 0,
                    download_count INTEGER DEFAULT 0,
                    title_description_embedding TEXT,
                    embedding_model TEXT,
                    embedding_generated_at TEXT,
                    sync_content_hash TEXT,
                    search_synced_hash TEXT,
                    last_synced_at TEXT,
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
            # Publish lock (B-16). A PRIMARY KEY on source_id makes the INSERT
            # itself the mutual-exclusion primitive, mirroring the DynamoDB
            # conditional write. Local dev is single-process, but a real lock
            # here keeps the two backends behaviourally identical (and testable).
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS publish_locks (
                    source_id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                )
                """
            )
            # Migrations: add columns if missing
            cur = self.conn.execute("PRAGMA table_info(submissions)")
            col_names = {row["name"] for row in cur.fetchall()}
            if "dataset_profile" not in col_names:
                self.conn.execute("ALTER TABLE submissions ADD COLUMN dataset_profile TEXT")
            if "legacy_source_id" not in col_names:
                self.conn.execute("ALTER TABLE submissions ADD COLUMN legacy_source_id TEXT")
            # Index created after the column is guaranteed to exist (above).
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_submissions_legacy ON submissions(legacy_source_id)"
            )
            if "dataset_doi" not in col_names:
                self.conn.execute("ALTER TABLE submissions ADD COLUMN dataset_doi TEXT")
            if "view_count" not in col_names:
                self.conn.execute("ALTER TABLE submissions ADD COLUMN view_count INTEGER DEFAULT 0")
            if "download_count" not in col_names:
                self.conn.execute("ALTER TABLE submissions ADD COLUMN download_count INTEGER DEFAULT 0")
            if "title_description_embedding" not in col_names:
                self.conn.execute(
                    "ALTER TABLE submissions ADD COLUMN title_description_embedding TEXT"
                )
            if "embedding_model" not in col_names:
                self.conn.execute("ALTER TABLE submissions ADD COLUMN embedding_model TEXT")
            if "embedding_generated_at" not in col_names:
                self.conn.execute(
                    "ALTER TABLE submissions ADD COLUMN embedding_generated_at TEXT"
                )
            if "metadata_updated_at" not in col_names:
                self.conn.execute(
                    "ALTER TABLE submissions ADD COLUMN metadata_updated_at TEXT"
                )
            if "sync_content_hash" not in col_names:
                self.conn.execute(
                    "ALTER TABLE submissions ADD COLUMN sync_content_hash TEXT"
                )
            if "search_synced_hash" not in col_names:
                self.conn.execute(
                    "ALTER TABLE submissions ADD COLUMN search_synced_hash TEXT"
                )
            if "last_synced_at" not in col_names:
                self.conn.execute(
                    "ALTER TABLE submissions ADD COLUMN last_synced_at TEXT"
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
        if data.get("dataset_profile"):
            try:
                data["dataset_profile"] = json.loads(data["dataset_profile"])
            except Exception:
                pass
        if data.get("title_description_embedding"):
            try:
                data["title_description_embedding"] = json.loads(
                    data["title_description_embedding"]
                )
            except Exception:
                pass
        return data

    def get_submission(self, source_id: str, version: str) -> Optional[Dict[str, Any]]:
        # The reserved publish-lock version is never a submission (see the
        # DynamoDB backend, where the lock shares the submissions table).
        if is_reserved_version(version):
            return None
        cur = self.conn.execute(
            "SELECT * FROM submissions WHERE source_id = ? AND version = ?",
            (source_id, version),
        )
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    def get_by_legacy_source_id(self, legacy_source_id: str) -> Optional[Dict[str, Any]]:
        if not legacy_source_id:
            return None
        cur = self.conn.execute(
            "SELECT * FROM submissions WHERE legacy_source_id = ? LIMIT 1",
            (legacy_source_id,),
        )
        row = cur.fetchone()
        return self._row_to_dict(row) if row else None

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM submissions WHERE source_id = ?",
            (source_id,),
        )
        return [self._row_to_dict(row) for row in cur.fetchall()]

    def _write_submission(self, record: Dict[str, Any]) -> None:
        dataset_mdata = record.get("dataset_mdata")
        if isinstance(dataset_mdata, dict):
            dataset_mdata = json.dumps(dataset_mdata)

        curation_history = record.get("curation_history")
        if isinstance(curation_history, list):
            curation_history = json.dumps(curation_history)

        dataset_profile = record.get("dataset_profile")
        if isinstance(dataset_profile, dict):
            dataset_profile = json.dumps(dataset_profile)

        embedding = record.get("title_description_embedding")
        if isinstance(embedding, list):
            embedding = json.dumps(embedding)

        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO submissions (
                    source_id, version, versioned_source_id, user_id, user_email,
                    organization, status, dataset_mdata, test, created_at, updated_at, action_id,
                    doi, dataset_doi, legacy_source_id, published_at, approved_at, approved_by, rejected_at, rejected_by,
                    rejection_reason, curation_history, dataset_profile, metadata_updated_at,
                    title_description_embedding, embedding_model, embedding_generated_at,
                    view_count, download_count, sync_content_hash, search_synced_hash,
                    last_synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    record.get("dataset_doi"),
                    record.get("legacy_source_id"),
                    record.get("published_at"),
                    record.get("approved_at"),
                    record.get("approved_by"),
                    record.get("rejected_at"),
                    record.get("rejected_by"),
                    record.get("rejection_reason"),
                    curation_history,
                    dataset_profile,
                    record.get("metadata_updated_at"),
                    embedding,
                    record.get("embedding_model"),
                    record.get("embedding_generated_at"),
                    int(record.get("view_count") or 0),
                    int(record.get("download_count") or 0),
                    record.get("sync_content_hash"),
                    record.get("search_synced_hash"),
                    record.get("last_synced_at"),
                ),
            )

    def put_submission(self, record: Dict[str, Any]) -> None:
        _reject_reserved_version(record.get("version"))
        self._write_submission(record)

    def upsert_submission(self, record: Dict[str, Any]) -> None:
        _reject_reserved_version(record.get("version"))
        self._write_submission(record)

    def update_status(self, source_id: str, version: str, status: str) -> None:
        _reject_reserved_version(version)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
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

    def scan_by_transfer_status(self, transfer_status: str) -> List[Dict[str, Any]]:
        # Transfer fields aren't persisted in the SQLite schema (dev-only store).
        # Globus transfers only run in production with DynamoDB.
        return []

    def update_profile(self, source_id: str, version: str, profile_json: str) -> None:
        _reject_reserved_version(version)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with self.conn:
            self.conn.execute(
                "UPDATE submissions SET dataset_profile = ?, updated_at = ? WHERE source_id = ? AND version = ?",
                (profile_json, now, source_id, version),
            )

    def update_embedding(
        self,
        source_id: str,
        version: str,
        embedding: List[float],
        model: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with self.conn:
            self.conn.execute(
                "UPDATE submissions SET title_description_embedding = ?, "
                "embedding_model = ?, embedding_generated_at = ?, updated_at = ? "
                "WHERE source_id = ? AND version = ?",
                (json.dumps(embedding), model, now, now, source_id, version),
            )

    def list_all(self, limit: int = 1000) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT * FROM submissions ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        )
        return [self._row_to_dict(row) for row in cur.fetchall()]

    def increment_counter(self, source_id: str, version: str, counter: str) -> None:
        _reject_reserved_version(version)
        with self.conn:
            self.conn.execute(
                f"UPDATE submissions SET {counter} = COALESCE({counter}, 0) + 1 "
                "WHERE source_id = ? AND version = ?",
                (source_id, version),
            )

    # -- publish lock (B-16) ------------------------------------------------

    def acquire_publish_lock(
        self, source_id: str, owner: str, ttl_seconds: float = DEFAULT_PUBLISH_LOCK_TTL_SECONDS,
    ) -> bool:
        now = time.time()
        try:
            with self.conn:
                # Clear an expired lock first, then let the PRIMARY KEY decide
                # the winner. Both statements run in one transaction.
                self.conn.execute(
                    "DELETE FROM publish_locks WHERE source_id = ? AND expires_at <= ?",
                    (source_id, now),
                )
                self.conn.execute(
                    "INSERT INTO publish_locks (source_id, owner, acquired_at, expires_at) "
                    "VALUES (?, ?, ?, ?)",
                    (source_id, owner, now, now + ttl_seconds),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def release_publish_lock(self, source_id: str, owner: str) -> bool:
        with self.conn:
            cur = self.conn.execute(
                "DELETE FROM publish_locks WHERE source_id = ? AND owner = ?",
                (source_id, owner),
            )
        return cur.rowcount > 0


def get_store() -> SubmissionStore:
    backend = os.environ.get("STORE_BACKEND", "dynamo").lower()
    if backend == "sqlite":
        return SqliteSubmissionStore()
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
