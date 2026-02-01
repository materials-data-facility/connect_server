import json
from datetime import datetime

from v2.request import parse_json_body
from v2.responses import bad_request, ok
from v2.store import get_store
from v2.stream_store import get_stream_store
from v2.submission_utils import generate_source_id, increment_version, latest_version


def _build_dc(metadata, title):
    creators = []
    operator = metadata.get("operator") if isinstance(metadata, dict) else None
    if operator:
        creators.append({"creatorName": operator})
    else:
        creators.append({"creatorName": "MDF Stream"})

    return {
        "titles": [{"title": title}],
        "creators": creators,
        "publisher": "Materials Data Facility",
        "publicationYear": str(datetime.utcnow().year),
        "resourceType": {"resourceTypeGeneral": "Dataset", "resourceType": "Dataset"},
    }


def lambda_handler(event, context):
    payload, error = parse_json_body(event)
    if error:
        return bad_request(error)
    payload = payload or {}

    path_params = event.get("pathParameters") or {}
    stream_id = path_params.get("stream_id") or payload.get("stream_id")
    if not stream_id:
        return bad_request("stream_id is required")

    stream_store = get_stream_store()
    stream = stream_store.get_stream(stream_id)
    if not stream:
        return bad_request("Stream not found")

    metadata = stream.get("metadata") or {}
    title = payload.get("title") or stream.get("title") or f"Stream {stream_id}"

    source_id = payload.get("source_id") or stream_id
    update = bool(payload.get("update", False))

    store = get_store()
    existing_versions = store.list_versions(source_id)

    if update and not existing_versions:
        return bad_request("Update requested but no prior submission found")

    if not update and existing_versions:
        source_id = generate_source_id(prefix=source_id)
        existing_versions = []

    latest = latest_version(existing_versions)
    version = increment_version(latest) if update else "1.0"

    mdf_block = {
        "source_id": source_id,
        "version": version,
        "versioned_source_id": f"{source_id}-{version}",
        "resource_type": "dataset",
        "stream_id": stream_id,
        "lab_id": stream.get("lab_id"),
        "run_id": metadata.get("run_id"),
        "instruments": metadata.get("instruments"),
        "facility": metadata.get("facility"),
    }

    dataset = {
        "dc": _build_dc(metadata, title),
        "data_sources": payload.get("data_sources") or [f"stream://{stream_id}"],
        "test": payload.get("test", False),
        "update": update,
        "mdf": mdf_block,
        "custom": {
            "stream": {
                "file_count": stream.get("file_count"),
                "total_bytes": stream.get("total_bytes"),
                "last_append_at": stream.get("last_append_at"),
            }
        },
    }

    now = datetime.utcnow().isoformat("T") + "Z"
    record = {
        "source_id": source_id,
        "version": version,
        "versioned_source_id": mdf_block["versioned_source_id"],
        "user_id": stream.get("user_id"),
        "user_email": None,
        "organization": stream.get("organization"),
        "status": "submitted",
        "dataset_mdata": json.dumps(dataset),
        "test": dataset.get("test", False),
        "created_at": now,
        "updated_at": now,
    }

    store.put_submission(record)

    return ok({
        "success": True,
        "source_id": source_id,
        "version": version,
        "versioned_source_id": mdf_block["versioned_source_id"],
        "stream_id": stream_id,
    })
