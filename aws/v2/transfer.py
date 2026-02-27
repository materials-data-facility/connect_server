"""Globus Transfer orchestration for MDF Connect v2.

Handles data movement from user Globus endpoints to MDF storage:
- Creates destination directories on MDF NCSA endpoint
- Manages ACL rules (grant/revoke) for user access to destination
- Submits and monitors transfer tasks

Auth pattern:
- Server credentials (GLOBUS_CLIENT_ID/SECRET): mkdir, ACL create/delete
  (requires endpoint manager role on NCSA MDF endpoint)
- User's transfer token: submitting the actual transfer task
  (runs as user so no source endpoint permissions needed)
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# NCSA MDF collection (Globus Connect Server endpoint)
NCSA_MDF_COLLECTION_UUID = "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec"

# Base path for open datasets on the MDF endpoint
MDF_BASE_PATH = "/mdf_open"

# Globus Transfer auto-cancels tasks after this deadline.
TRANSFER_DEADLINE_HOURS = 24


def _get_server_transfer_client():
    """Get a TransferClient authenticated with server (confidential app) credentials.

    Used for operations requiring endpoint manager privileges:
    mkdir, ACL create/delete, task status checks.
    """
    import globus_sdk

    client_id = os.environ.get("GLOBUS_CLIENT_ID")
    client_secret = os.environ.get("GLOBUS_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET are required for transfer operations"
        )

    confidential_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
    token_response = confidential_client.oauth2_client_credentials_tokens(
        requested_scopes="urn:globus:auth:scope:transfer.api.globus.org:all",
    )
    transfer_token = token_response.by_resource_server["transfer.api.globus.org"]["access_token"]
    return globus_sdk.TransferClient(
        authorizer=globus_sdk.AccessTokenAuthorizer(transfer_token)
    )


def _get_user_transfer_client(user_transfer_token: str):
    """Get a TransferClient authenticated with the user's transfer token.

    Used for submitting transfer tasks (runs as user).
    """
    import globus_sdk

    return globus_sdk.TransferClient(
        authorizer=globus_sdk.AccessTokenAuthorizer(user_transfer_token)
    )


def parse_globus_uri(uri: str) -> tuple[str, str]:
    """Parse a globus://collection_uuid/path URI into (collection_uuid, path)."""
    if not uri.startswith("globus://"):
        raise ValueError(f"Not a globus:// URI: {uri}")
    rest = uri[len("globus://"):]
    slash_idx = rest.find("/")
    if slash_idx <= 0:
        raise ValueError(f"globus:// URI missing path: {uri}")
    return rest[:slash_idx], rest[slash_idx:]


def initiate_transfer(
    source_endpoint: str,
    source_path: str,
    source_id: str,
    version: str,
    user_transfer_token: str,
    user_identity_id: str,
) -> Dict[str, Any]:
    """Initiate a Globus transfer from user endpoint to MDF storage.

    Steps:
    1. Create destination directory on NCSA endpoint (server creds)
    2. Create ACL rule granting user rw access to destination (server creds)
    3. Submit transfer task (user's token, runs as user)

    Args:
        source_endpoint: Source Globus collection UUID
        source_path: Path on source collection
        source_id: MDF dataset source_id
        version: Dataset version string
        user_transfer_token: User's Globus Transfer access token
        user_identity_id: User's Globus identity UUID (for ACL)

    Returns:
        Dict with task_id, acl_rule_id, destination_path
    """
    import globus_sdk

    destination_path = f"{MDF_BASE_PATH}/{source_id}/{version}/"

    server_tc = _get_server_transfer_client()

    # Step 1: Create destination directory
    try:
        server_tc.operation_mkdir(NCSA_MDF_COLLECTION_UUID, destination_path)
        logger.info("Created destination directory: %s", destination_path)
    except globus_sdk.GlobusAPIError as exc:
        # 502 "Exists" is fine — directory already created
        if exc.http_status == 502 and "Exists" in str(exc):
            logger.info("Destination directory already exists: %s", destination_path)
        else:
            raise

    # Step 2: Create ACL rule granting user rw access
    acl_rule_id = None
    try:
        acl_result = server_tc.add_endpoint_acl_rule(
            NCSA_MDF_COLLECTION_UUID,
            dict(
                DATA_TYPE="access",
                principal_type="identity",
                principal=user_identity_id,
                path=destination_path,
                permissions="rw",
            ),
        )
        acl_rule_id = acl_result.get("access_id")
        logger.info("Created ACL rule %s for user %s on %s", acl_rule_id, user_identity_id, destination_path)
    except globus_sdk.GlobusAPIError as exc:
        # If ACL creation fails, log but continue — user may already have access
        logger.warning("ACL creation failed (continuing): %s", exc)

    # Step 3: Submit transfer using user's token
    user_tc = _get_user_transfer_client(user_transfer_token)

    deadline = datetime.now(timezone.utc) + timedelta(hours=TRANSFER_DEADLINE_HOURS)
    transfer_data = globus_sdk.TransferData(
        source_endpoint=source_endpoint,
        destination_endpoint=NCSA_MDF_COLLECTION_UUID,
        label=f"MDF Connect: {source_id} v{version}",
        sync_level="checksum",
        deadline=deadline.isoformat(),
    )

    # If source_path ends with /, transfer entire directory recursively
    if source_path.endswith("/"):
        transfer_data.add_item(source_path, destination_path, recursive=True)
    else:
        # Single file — extract filename for destination
        filename = source_path.rsplit("/", 1)[-1]
        transfer_data.add_item(source_path, f"{destination_path}{filename}")

    task_result = user_tc.submit_transfer(transfer_data)
    task_id = task_result["task_id"]
    initiated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    logger.info("Submitted transfer task %s: %s -> %s:%s", task_id, source_endpoint, NCSA_MDF_COLLECTION_UUID, destination_path)

    return {
        "task_id": task_id,
        "acl_rule_id": acl_rule_id,
        "destination_path": destination_path,
        "source_endpoint": source_endpoint,
        "source_path": source_path,
        "initiated_at": initiated_at,
    }


def check_transfer_status(task_id: str) -> Dict[str, Any]:
    """Check the status of a Globus transfer task.

    Uses server credentials to check status (any task is visible
    to the endpoint manager).

    Returns:
        Dict with status, bytes_transferred, files_transferred, etc.
    """
    server_tc = _get_server_transfer_client()
    task = server_tc.get_task(task_id)

    return {
        "task_id": task_id,
        "status": task["status"],
        "bytes_transferred": task.get("bytes_transferred", 0),
        "files_transferred": task.get("files_transferred", 0),
        "files_skipped": task.get("files_skipped", 0),
        "is_ok": task.get("is_ok"),
        "nice_status": task.get("nice_status"),
        "label": task.get("label"),
    }


def cleanup_transfer_acl(acl_rule_id: str) -> bool:
    """Remove an ACL rule from the MDF NCSA endpoint.

    Called after transfer completes (success or failure) to revoke
    the temporary write access.

    Returns:
        True if successfully removed, False otherwise.
    """
    if not acl_rule_id:
        return True

    try:
        server_tc = _get_server_transfer_client()
        server_tc.delete_endpoint_acl_rule(NCSA_MDF_COLLECTION_UUID, acl_rule_id)
        logger.info("Removed ACL rule %s", acl_rule_id)
        return True
    except Exception:
        logger.exception("Failed to remove ACL rule %s", acl_rule_id)
        return False


def cleanup_stale_transfers(
    submissions: List[Dict[str, Any]],
    max_age_hours: float = 26,
) -> List[Dict[str, Any]]:
    """Bulk-check submissions with active transfers, clean up completed/stale ones.

    For each submission whose transfer is still marked active:
    - Check Globus task status
    - If done/failed/timed-out or older than max_age_hours: clean up ACLs, update record
    - If still running within deadline: update progress fields

    Returns the list of submissions that were modified (caller should persist them).
    """
    now = datetime.now(timezone.utc)
    modified: List[Dict[str, Any]] = []

    for submission in submissions:
        task_ids = submission.get("transfer_task_ids", [])
        if not task_ids:
            continue

        # Check age
        initiated_str = submission.get("transfer_initiated_at")
        is_stale = False
        if initiated_str:
            try:
                initiated = datetime.fromisoformat(initiated_str.replace("Z", "+00:00"))
                is_stale = (now - initiated).total_seconds() > max_age_hours * 3600
            except (ValueError, TypeError):
                pass

        all_succeeded = True
        any_failed = False
        total_bytes = 0
        total_files = 0

        for task_id in task_ids:
            try:
                status = check_transfer_status(task_id)
                total_bytes += status.get("bytes_transferred", 0)
                total_files += status.get("files_transferred", 0)

                if status["status"] == "SUCCEEDED":
                    continue
                elif status["status"] in ("FAILED", "INACTIVE"):
                    any_failed = True
                    all_succeeded = False
                else:
                    all_succeeded = False
            except Exception:
                logger.exception("Failed to check transfer task %s", task_id)
                all_succeeded = False

        submission["transfer_bytes_transferred"] = total_bytes
        submission["transfer_files_transferred"] = total_files

        if all_succeeded or any_failed or is_stale:
            # Terminal state — clean up ACLs
            if all_succeeded:
                submission["transfer_status"] = "succeeded"
            elif any_failed or is_stale:
                submission["transfer_status"] = "failed" if any_failed else "stale"
            for acl_id in submission.get("transfer_acl_rule_ids", []):
                cleanup_transfer_acl(acl_id)
            logger.info(
                "Transfer cleanup for %s v%s: %s",
                submission.get("source_id"),
                submission.get("version"),
                submission["transfer_status"],
            )
            modified.append(submission)
        else:
            # Still active — update progress only
            modified.append(submission)

    return modified


def extract_transfer_sources(data_sources: list[str]) -> list[dict]:
    """Identify data sources that need Globus transfer.

    Returns entries for globus:// URIs that point to endpoints OTHER
    than the MDF NCSA endpoint (data already on MDF doesn't need transfer).
    """
    transfer_needed = []
    for uri in data_sources:
        if not uri.startswith("globus://"):
            continue
        try:
            collection_uuid, path = parse_globus_uri(uri)
        except ValueError:
            continue
        # Skip sources already on the MDF endpoint
        if collection_uuid.lower() == NCSA_MDF_COLLECTION_UUID.lower():
            continue
        transfer_needed.append({
            "uri": uri,
            "source_endpoint": collection_uuid,
            "source_path": path,
        })
    return transfer_needed
