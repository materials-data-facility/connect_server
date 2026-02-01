import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key

from v2.config import (
    AWS_REGION,
    DYNAMO_ENDPOINT_URL,
    DYNAMO_SUBMISSIONS_TABLE,
    GSI_ORG_INDEX,
    GSI_USER_INDEX,
)


class DynamoSubmissions:
    def __init__(self):
        resource_kwargs = {"region_name": AWS_REGION}
        if DYNAMO_ENDPOINT_URL:
            resource_kwargs["endpoint_url"] = DYNAMO_ENDPOINT_URL
        self._resource = boto3.resource("dynamodb", **resource_kwargs)
        self.table = self._resource.Table(DYNAMO_SUBMISSIONS_TABLE)

    def get_submission(self, source_id: str, version: str) -> Optional[Dict[str, Any]]:
        resp = self.table.get_item(Key={"source_id": source_id, "version": version})
        return resp.get("Item")

    def list_versions(self, source_id: str) -> List[Dict[str, Any]]:
        resp = self.table.query(KeyConditionExpression=Key("source_id").eq(source_id))
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
            "IndexName": GSI_USER_INDEX,
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "Limit": limit,
            "ScanIndexForward": False,
        }
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = self.table.query(**kwargs)
        return resp.get("Items", []), resp.get("LastEvaluatedKey")

    def list_by_org(self, organization: str, limit: int = 50, start_key: Optional[Dict[str, Any]] = None):
        kwargs = {
            "IndexName": GSI_ORG_INDEX,
            "KeyConditionExpression": Key("organization").eq(organization),
            "Limit": limit,
            "ScanIndexForward": False,
        }
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        resp = self.table.query(**kwargs)
        return resp.get("Items", []), resp.get("LastEvaluatedKey")


def parse_pagination_key(key_str: Optional[str]) -> Optional[Dict[str, Any]]:
    if not key_str:
        return None
    try:
        return json.loads(key_str)
    except Exception:
        return None
