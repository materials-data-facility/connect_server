import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from v2.config import DEFAULT_ORGANIZATION
from v2.submission_utils import generate_source_id, increment_version, latest_version
from v2.store import get_store
from v2.request import parse_authorizer, parse_json_body
from v2.responses import bad_request, ok, server_error

logger = logging.getLogger(__name__)


def _ensure_dc_defaults(metadata: Dict[str, Any]) -> None:
    if not metadata.get("dc") or not isinstance(metadata.get("dc"), dict):
        metadata["dc"] = {}
    if not metadata["dc"].get("resourceType"):
        metadata["dc"]["resourceType"] = {
            "resourceTypeGeneral": "Dataset",
            "resourceType": "Dataset",
        }


def _normalize_tags(metadata: Dict[str, Any]) -> None:
    tags = metadata.pop("tags", None)
    if not tags:
        return
    if not isinstance(tags, list):
        tags = [tags]
    metadata.setdefault("dc", {}).setdefault("subjects", [])
    for tag in tags:
        metadata["dc"]["subjects"].append({"subject": tag})


def _source_id_from_metadata(metadata: Dict[str, Any]) -> Optional[str]:
    mdf = metadata.get("mdf", {})
    return mdf.get("source_id") or mdf.get("source_name")


def lambda_handler(event, context):
    auth = parse_authorizer(event)
    user_id = auth.get("user_id")
    user_email = auth.get("user_email")
    metadata, error = parse_json_body(event)
    if error:
        return bad_request(error)

    if not metadata:
        return bad_request("POST data empty or not JSON")

    try:
        json.dumps(metadata, allow_nan=False)
    except Exception:
        return bad_request("Submission may not contain NaN or Infinity")

    _ensure_dc_defaults(metadata)
    _normalize_tags(metadata)

    if not metadata.get("data_sources") and not metadata.get("update_metadata_only"):
        return bad_request("You must populate dc and data_sources before submission")

    organization = metadata.get("mdf", {}).get("organization", DEFAULT_ORGANIZATION)
    if isinstance(organization, list):
        organization = organization[0]

    is_test = bool(metadata.get("test", False))
    update = bool(metadata.get("update", False))

    store = get_store()

    source_id = _source_id_from_metadata(metadata)
    existing_versions = []
    if source_id:
        existing_versions = store.list_versions(source_id)

    if update and not source_id:
        return bad_request("Missing source_id for update submission")

    if not update and not source_id:
        source_id = generate_source_id()

    if not update and source_id and existing_versions:
        source_id = "{}-{}".format(source_id, uuid.uuid4().hex[:8])
        existing_versions = []

    latest_ver = latest_version(existing_versions)
    version = increment_version(latest_ver) if update else "1.0"

    if update and not latest_ver:
        return bad_request("Update requested but no prior submission found")

    mdf_block = metadata.setdefault("mdf", {})
    mdf_block["source_id"] = source_id
    mdf_block["version"] = version
    mdf_block["versioned_source_id"] = "{}-{}".format(source_id, version)
    mdf_block["source_name"] = mdf_block.get("source_name", source_id)
    mdf_block["resource_type"] = "dataset"
    mdf_block["ingest_date"] = datetime.utcnow().isoformat("T") + "Z"

    now = datetime.utcnow().isoformat("T") + "Z"
    record = {
        "source_id": source_id,
        "version": version,
        "versioned_source_id": mdf_block["versioned_source_id"],
        "user_id": user_id,
        "user_email": user_email,
        "organization": organization,
        "status": "submitted",
        "dataset_mdata": json.dumps(metadata),
        "test": is_test,
        "created_at": now,
        "updated_at": now,
    }

    action_id = None
    use_mock_flow = os.environ.get("USE_MOCK_FLOW", "").lower() in {"1", "true", "yes"}
    if use_mock_flow:
        try:
            from v2.mock_flow import run_flow

            flow_result = run_flow({
                "source_id": source_id,
                "version": version,
                "user_id": user_id,
                "user_email": user_email,
                "organization": organization,
                "metadata": metadata,
            })
            action_id = flow_result.get("action_id")
            record["action_id"] = action_id
        except Exception as exc:
            logger.warning("Mock flow failed: %s", exc)

    try:
        store.put_submission(record)
    except Exception as exc:
        logger.exception("Failed to store submission")
        return server_error(str(exc))

    response_body = {
        "success": True,
        "source_id": source_id,
        "version": version,
        "versioned_source_id": mdf_block["versioned_source_id"],
        "organization": organization,
        "action_id": action_id,
    }

    return ok(response_body)
