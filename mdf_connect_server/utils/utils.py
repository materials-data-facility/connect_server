from copy import deepcopy
from datetime import date
import json
import logging
import os
import re
import subprocess
import time
import urllib

import boto3
from citrination_client import CitrinationClient
import globus_sdk
import jsonschema
import mdf_toolbox
import requests

from mdf_connect_server import CONFIG


logger = logging.getLogger(__name__)


# SQS setup
SQS_CLIENT = boto3.resource('sqs',
                            aws_access_key_id=CONFIG["AWS_KEY"],
                            aws_secret_access_key=CONFIG["AWS_SECRET"],
                            region_name="us-east-1")
SQS_QUEUE_NAME = CONFIG["SQS_QUEUE"]
assert SQS_QUEUE_NAME.endswith(".fifo")
SQS_ATTRIBUTES = {
    "FifoQueue": 'true',
    "ContentBasedDeduplication": 'true',
    "ReceiveMessageWaitTimeSeconds": '20'
}
SQS_GROUP = CONFIG["SQS_GROUP_ID"]

# DynamoDB setup
DMO_CLIENT = boto3.resource('dynamodb',
                            aws_access_key_id=CONFIG["AWS_KEY"],
                            aws_secret_access_key=CONFIG["AWS_SECRET"],
                            region_name="us-east-1")
DMO_TABLE = CONFIG["DYNAMO_TABLE"]
DMO_SCHEMA = {
    "TableName": DMO_TABLE,
    "AttributeDefinitions": [{
        "AttributeName": "source_id",
        "AttributeType": "S"
    }],
    "KeySchema": [{
        "AttributeName": "source_id",
        "KeyType": "HASH"
    }],
    "ProvisionedThroughput": {
        "ReadCapacityUnits": 20,
        "WriteCapacityUnits": 20
    }
}
STATUS_STEPS = (
    ("convert_start", "Conversion initialization"),
    ("convert_download", "Conversion data download"),
    ("converting", "Data conversion"),
    ("convert_ingest", "Ingestion preparation"),
    ("ingest_start", "Ingestion initialization"),
    ("ingest_download", "Ingestion data download"),
    ("ingest_integration", "Integration data download"),
    ("ingest_search", "Globus Search ingestion"),
    ("ingest_publish", "Globus Publish publication"),
    ("ingest_citrine", "Citrine upload"),
    ("ingest_mrr", "Materials Resource Registration"),
    ("ingest_cleanup", "Post-processing cleanup")
)
# This is the start of ingest steps in STATUS_STEPS
# In other words, the ingest steps are STATUS_STEPS[INGEST_MARK:]
# and the convert steps are STATUS_STEPS[:INGEST_MARK]
INGEST_MARK = 4

# Status codes indicating some form of not-failure
SUCCESS_CODES = [
    "S",
    "M",
    "L",
    "R",
    "U",
    "N"
]

# Global save locations for whitelists
CONVERT_GROUP = {
    # Globus Groups UUID
    "group_id": CONFIG["CONVERT_GROUP_ID"],
    # Group member IDs
    "whitelist": [],
    # UNIX timestamp of last update
    "updated": 0,
    # Refresh frequency (in seconds)
    #   X days * 24 hours/day * 60 minutes/hour * 60 seconds/minute
    "frequency": 1 * 60 * 60  # 1 hour
}
INGEST_GROUP = {
    "group_id": CONFIG["INGEST_GROUP_ID"],
    "whitelist": [],
    "updated": 0,
    "frequency": 1 * 24 * 60 * 60  # 1 day
}
ADMIN_GROUP = {
    "group_id": CONFIG["ADMIN_GROUP_ID"],
    "whitelist": [],
    "updated": 0,
    "frequency": 1 * 24 * 60 * 60  # 1 day
}


def authenticate_token(token, auth_level):
    """Auth a token
    Levels:
        convert
        ingest
        admin
    """
    if not token:
        return {
            "success": False,
            "error": "Not Authenticated",
            "error_code": 401
        }
    try:
        token = token.replace("Bearer ", "")
        auth_client = globus_sdk.ConfidentialAppAuthClient(CONFIG["API_CLIENT_ID"],
                                                           CONFIG["API_CLIENT_SECRET"])
        auth_res = auth_client.oauth2_token_introspect(token, include="identities_set")
    except Exception as e:
        logger.error("Error authenticating token: {}".format(repr(e)))
        return {
            "success": False,
            "error": "Authentication could not be completed",
            "error_code": 500
        }
    if not auth_res:
        return {
            "success": False,
            "error": "Token could not be validated",
            "error_code": 401
        }
    # Check that token is active
    if not auth_res["active"]:
        return {
            "success": False,
            "error": "Token expired",
            "error_code": 403
        }
    # Check correct scope and audience
    if (CONFIG["API_SCOPE"] not in auth_res["scope"]
            or CONFIG["API_SCOPE_ID"] not in auth_res["aud"]):
        return {
            "success": False,
            "error": "Not authorized to MDF Connect scope",
            "error_code": 401
        }
    # Finally, verify user is in appropriate group
    try:
        whitelist = fetch_whitelist(auth_level)
        if len(whitelist) == 0:
            raise ValueError("Whitelist empty")
    except Exception as e:
        logger.warning("Whitelist generation failed:", e)
        return {
            "success": False,
            "error": "Unable to fetch Group memberships.",
            "error_code": 500
        }
    if not any([uid in whitelist for uid in auth_res["identities_set"]]):
        logger.info("User not in whitelist:", auth_res["username"])
        return {
            "success": False,
            "error": "You cannot access this service or collection",
            "error_code": 403
        }

    return {
        "success": True,
        "token_info": auth_res,
        "user_id": auth_res["sub"],
        "username": auth_res["username"],
        "name": auth_res["name"] or "Not given",
        "email": auth_res["email"] or "Not given",
        "identities_set": auth_res["identities_set"]
    }


def fetch_whitelist(auth_level):
    # auth_level values:
    # admin, convert, ingest, [Group ID]
    whitelist = []
    groups_auth = {
        "app_name": "MDF Open Connect",
        "client_id": CONFIG["API_CLIENT_ID"],
        "client_secret": CONFIG["API_CLIENT_SECRET"],
        "services": ["groups"]
    }
    # Always add admin list
    # Check for staleness
    global ADMIN_GROUP
    if int(time.time()) - ADMIN_GROUP["updated"] > ADMIN_GROUP["frequency"]:
        # If NexusClient has not been created yet, create it
        if type(groups_auth) is dict:
            groups_auth = mdf_toolbox.confidential_login(groups_auth)["groups"]
        # Get all the members
        member_list = groups_auth.get_group_memberships(ADMIN_GROUP["group_id"])["members"]
        # Whitelist is all IDs in the group that are active
        ADMIN_GROUP["whitelist"] = [member["identity_id"]
                                    for member in member_list
                                    if member["status"] == "active"]
        # Update timestamp
        ADMIN_GROUP["updated"] = int(time.time())
    whitelist.extend(ADMIN_GROUP["whitelist"])
    # Add either convert or ingest whitelists
    if auth_level == "convert":
        global CONVERT_GROUP
        if int(time.time()) - CONVERT_GROUP["updated"] > CONVERT_GROUP["frequency"]:
            # If NexusClient has not been created yet, create it
            if type(groups_auth) is dict:
                groups_auth = mdf_toolbox.confidential_login(groups_auth)["groups"]
            # Get all the members
            member_list = groups_auth.get_group_memberships(CONVERT_GROUP["group_id"])["members"]
            # Whitelist is all IDs in the group that are active
            CONVERT_GROUP["whitelist"] = [member["identity_id"]
                                          for member in member_list
                                          if member["status"] == "active"]
            # Update timestamp
            CONVERT_GROUP["updated"] = int(time.time())
        whitelist.extend(CONVERT_GROUP["whitelist"])
    elif auth_level == "ingest":
        global INGEST_GROUP
        if int(time.time()) - INGEST_GROUP["updated"] > INGEST_GROUP["frequency"]:
            # If NexusClient has not been created yet, create it
            if type(groups_auth) is dict:
                groups_auth = mdf_toolbox.confidential_login(groups_auth)["groups"]
            # Get all the members
            member_list = groups_auth.get_group_memberships(INGEST_GROUP["group_id"])["members"]
            # Whitelist is all IDs in the group that are active
            INGEST_GROUP["whitelist"] = [member["identity_id"]
                                         for member in member_list
                                         if member["status"] == "active"]
            # Update timestamp
            INGEST_GROUP["updated"] = int(time.time())
        whitelist.extend(INGEST_GROUP["whitelist"])
    elif auth_level == "admin":
        # Already handled admins
        pass
    else:
        # Assume auth_level is Group ID
        # If NexusClient has not been created yet, create it
        if type(groups_auth) is dict:
            groups_auth = mdf_toolbox.confidential_login(groups_auth)["groups"]
        # Get all the members
        try:
            member_list = groups_auth.get_group_memberships(auth_level)["members"]
        except Exception:
            pass
        else:
            whitelist.extend([member["identity_id"]
                              for member in member_list
                              if member["status"] == "active"])
    return whitelist


def make_source_id(title, test=False):
    """Make a source name out of a title."""
    delete_words = [
        "and",
        "or",
        "the",
        "a",
        "an",
        "of"
    ]
    title = title.strip().lower()
    # Remove unimportant words
    for dw in delete_words:
        # Replace words that are by themselves
        # e.g. do not replace "and" in "random", do replace in "materials and design"
        title = title.replace(" "+dw+" ", " ")
        # Same for underscore separation
        title = title.replace("_"+dw+"_", "_")
        # Replace words at the start and end of the title
        if title.startswith(dw+" "):
            title = title[len(dw+" "):]
        if title.endswith(" "+dw):
            title = title[:-len(" "+dw)]
    # Replace spaces with underscores, remove leading/trailing underscores
    title = title.replace(" ", "_").strip("_")
    # Clear double underscores
    while title.find("__") != -1:
        title = title.replace("__", "_")
    # Filter out special characters
    if not title.isalnum():
        source_id = ""
        for char in title:
            # If is alnum, or non-duplicate underscore, add to source_id
            if char.isalnum() or (char == "_" and not source_id.endswith("_")):
                source_id += char
    else:
        source_id = title
    # Add test flag if necessary
    if test:
        # If test flag already applied, don't re-apply
        if source_id.startswith("test"):
            # Just add back initial underscore
            source_id = "_" + source_id
        # Otherwise, apply test flag
        else:
            source_id = "_test_" + source_id

    # Determine version number to add
    # Remove any existing version number
    source_id = re.sub("_v[0-9]+$", "", source_id)
    # Save source_name
    source_name = source_id
    version = 1
    user_ids = set()
    while True:
        # Try new source name
        new_source_id = source_id + "_v{}".format(version)
        status_res = read_status(new_source_id)
        # If name already exists, increment version and try again
        if status_res["success"]:
            version += 1
            user_ids.add(status_res["status"]["user_id"])
        # Otherwise, correct name found
        else:
            source_id = new_source_id
            break

    return {
        "source_id": source_id,
        "source_name": source_name,
        "version": version,
        "user_id_list": user_ids
    }


def clean_start():
    """Reset the Connect environment to a clean state, as best as possible.
    """
    logger.debug("Cleaning Connect state")
    # Auth to get Transfer client
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": CONFIG["API_CLIENT_ID"],
        "client_secret": CONFIG["API_CLIENT_SECRET"],
        "services": ["transfer"]
    }
    transfer_client = mdf_toolbox.confidential_login(creds)["transfer"]
    logger.debug("Cancelling active Transfer tasks")
    # List all Transfers active on endpoint
    all_tasks = transfer_client.endpoint_manager_task_list(num_results=None,
                                                           filter_status="ACTIVE,INACTIVE",
                                                           filter_endpoint=CONFIG["LOCAL_EP"])
    all_ids = [task["task_id"] for task in all_tasks]
    # Terminate active Transfers
    cancel_res = transfer_client.endpoint_manager_cancel_tasks(all_ids,
                                                               CONFIG["TRANSFER_CANCEL_MSG"])
    # Wait for all Transfers to be terminated
    if not cancel_res["done"]:
        while not transfer_client.endpoint_manager_cancel_status(cancel_res["id"])["done"]:
            logger.debug("Waiting for all active Transfers to cancel")
            time.sleep(CONFIG["CANCEL_WAIT_TIME"])
    logger.debug("Active Transfer tasks cancelled")

    # Delete data, feedstock, service_data
    logger.debug("Deleting old Connect files")
    if os.path.exists(CONFIG["LOCAL_PATH"]):
        lpath_res = local_admin_delete(CONFIG["LOCAL_PATH"])
        if not lpath_res["success"]:
            logger.error("Error deleting {}: {}".format(CONFIG["LOCAL_PATH"],
                                                        lpath_res["error"]))
        else:
            os.mkdir(CONFIG["LOCAL_PATH"])
            logger.debug("Cleaned {}".format(CONFIG["LOCAL_PATH"]))
    if os.path.exists(CONFIG["FEEDSTOCK_PATH"]):
        fpath_res = local_admin_delete(CONFIG["FEEDSTOCK_PATH"])
        if not fpath_res["success"]:
            logger.error("Error deleting {}: {}".format(CONFIG["LOCAL_PATH"],
                                                        fpath_res["error"]))
        else:
            os.mkdir(CONFIG["FEEDSTOCK_PATH"])
            logger.debug("Cleaned {}".format(CONFIG["FEEDSTOCK_PATH"]))
    if os.path.exists(CONFIG["SERVICE_DATA"]):
        spath_res = local_admin_delete(CONFIG["SERVICE_DATA"])
        if not spath_res["success"]:
            logger.error("Error deleting {}: {}".format(CONFIG["LOCAL_PATH"],
                                                        spath_res["error"]))
        else:
            os.mkdir(CONFIG["SERVICE_DATA"])
            logger.debug("Cleaned {}".format(CONFIG["SERVICE_DATA"]))

    logger.info("Connect startup state clean complete")
    return


def download_data(transfer_client, data_loc, local_ep, local_path):
    """Download data from a remote host to the configured machine.

    Arguments:
    transfer_client (TransferClient): An authenticated TransferClient with access to the data.
                                      Technically unnecessary for non-Globus data locations.
    data_loc (list of str): The location(s) of the data.
    local_ep (str): The local machine's endpoint ID.
    local_path (str): The path to the local storage location.

    Returns:
    dict: success (bool): True on success, False on failure.
    """
    filename = None
    # If the local_path is a file and not a directory, use the directory
    if local_path[-1] != "/":
        # Save the filename for later
        filename = os.path.basename(local_path)
        local_path = os.path.dirname(local_path) + "/"

    os.makedirs(local_path, exist_ok=True)
    if not isinstance(data_loc, list):
        data_loc = [data_loc]

    # Download data locally
    for location in data_loc:
        loc_info = urllib.parse.urlparse(location)

        # Special case pre-processing
        # Globus Web App link into globus:// form
        if (location.startswith("https://www.globus.org/app/transfer")
                or location.startswith("https://app.globus.org/file-manager")):
            data_info = urllib.parse.unquote(loc_info.query)
            # EP ID is in origin or dest
            ep_start = data_info.find("origin_id=")
            if ep_start < 0:
                ep_start = data_info.find("destination_id=")
                if ep_start < 0:
                    raise ValueError("Invalid Globus Transfer UI link")
                else:
                    ep_start += len("destination_id=")
            else:
                ep_start += len("origin_id=")
            ep_end = data_info.find("&", ep_start)
            if ep_end < 0:
                ep_end = len(data_info)
            ep_id = data_info[ep_start:ep_end]

            # Same for path
            path_start = data_info.find("origin_path=")
            if path_start < 0:
                path_start = data_info.find("destination_path=")
                if path_start < 0:
                    raise ValueError("Invalid Globus Transfer UI link")
                else:
                    path_start += len("destination_path=")
            else:
                path_start += len("origin_path=")
            path_end = data_info.find("&", path_start)
            if path_end < 0:
                path_end = len(data_info)
            path = data_info[path_start:path_end]

            # Make new location
            location = "globus://{}{}".format(ep_id, path)
            loc_info = urllib.parse.urlparse(location)
        # Google Drive protocol into globus:// form
        elif loc_info.scheme in ["gdrive", "google", "googledrive"]:
            # Correct form is "google:///path/file.dat"
            # (three slashes - two for scheme end, one for path start)
            # But if a user uses two slashes, the netloc will incorrectly be the top dir
            # (netloc="path", path="/file.dat")
            # Otherwise netloc is nothing
            if loc_info.netloc:
                gpath = "/" + loc_info.netloc + loc_info.path
            else:
                gpath = loc_info.path
            # Don't use os.path.join because gpath starts with /
            # GDRIVE_ROOT does not end in / to make compatible
            location = "globus://{}{}{}".format(CONFIG["GDRIVE_EP"],
                                                CONFIG["GDRIVE_ROOT"], gpath)
            loc_info = urllib.parse.urlparse(location)

        # Globus Transfer
        if loc_info.scheme == "globus":
            # Check that data not already in place
            if (loc_info.netloc != local_ep
                    and loc_info.path != (local_path + (filename if filename else ""))):
                # Transfer locally
                transfer = mdf_toolbox.custom_transfer(
                                transfer_client, loc_info.netloc, local_ep,
                                [(loc_info.path, local_path)],
                                interval=CONFIG["TRANSFER_PING_INTERVAL"],
                                inactivity_time=CONFIG["TRANSFER_DEADLINE"], notify=False)
                for event in transfer:
                    if not event["success"]:
                        logger.info("Transfer is_error: {} - {}"
                                    .format(event.get("code", "No code found"),
                                            event.get("description", "No description found")))
                        yield {
                            "success": False,
                            "error": "{} - {}".format(event.get("code", "No code found"),
                                                      event.get("description",
                                                                "No description found"))
                        }
                if not event["success"]:
                    raise ValueError(event)
        # HTTP(S)
        elif loc_info.scheme.startswith("http"):
            # Get default filename and extension
            http_filename = os.path.basename(loc_info.path)
            if not http_filename:
                http_filename = "archive"
            ext = os.path.splitext(http_filename)[1]
            if not ext:
                ext = ".archive"

            # Fetch file
            res = requests.get(location)
            # Get filename from header if present
            con_disp = res.headers.get("Content-Disposition", "")
            filename_start = con_disp.find("filename=")
            if filename_start >= 0:
                filename_end = con_disp.find(";", filename_start)
                if filename_end < 0:
                    filename_end = None
                http_filename = con_disp[filename_start+len("filename="):filename_end]
                http_filename = http_filename.strip("\"'; ")

            # Create path for file
            archive_path = os.path.join(local_path, filename or http_filename)
            # Make filename unique if filename is duplicate
            collisions = 0
            while os.path.exists(archive_path):
                # Save and remove extension
                archive_path, ext = os.path.splitext(archive_path)
                old_add = "({})".format(collisions)
                collisions += 1
                new_add = "({})".format(collisions)
                # If added number already, remove before adding new number
                if archive_path.endswith(old_add):
                    archive_path = archive_path[:-len(old_add)]
                # Add "($num_collisions)" to end of filename to make filename unique
                archive_path = archive_path + new_add + ext

            # Download and save file
            with open(archive_path, 'wb') as out:
                out.write(res.content)
            logger.debug("Downloaded HTTP file: {}".format(archive_path))
        # Not supported
        else:
            # Nothing to do
            raise IOError("Invalid data location: '{}' is not a recognized protocol "
                          "(from {}).".format(loc_info.scheme, str(location)))

    # Extract all archives, delete extracted archives
    extract_res = mdf_toolbox.uncompress_tree(local_path, delete_archives=True)
    if not extract_res["success"]:
        raise IOError("Unable to extract archives in dataset")

    yield {
        "success": True,
        "num_extracted": extract_res["num_extracted"],
        "total_files": sum([len(files) for _, _, files in os.walk(local_path)])
    }


def backup_data(transfer_client, local_ep, local_path, backup_ep, backup_path):
    """Back up data to a remote endpoint.

    Arguments:
    transfer_client (TransferClient): An authenticated TransferClient with access to the data.
    local_ep (str): The local machine's endpoint ID.
    local_path (str): The path to the local storage location.
    backup_ep (str): The backup machine's endpoint ID.
    backup_path (str): The path to the backup storage location.

    Returns:
    dict: success (bool): True on success, False on failure.
    """
    filename = None
    # If the local_path is a file and not a directory, use the directory
    if local_path[-1] != "/":
        # Save the filename for later
        filename = os.path.basename(local_path)
        local_path = os.path.dirname(local_path) + "/"

    transfer = mdf_toolbox.custom_transfer(
                    transfer_client, local_ep, backup_ep,
                    [(local_path + (filename if filename else ""), backup_path)],
                    interval=CONFIG["TRANSFER_PING_INTERVAL"],
                    inactivity_time=CONFIG["TRANSFER_DEADLINE"], notify=False)
    for event in transfer:
        if not event["success"]:
            logger.debug(event)
    if not event["success"]:
        raise ValueError("{}: {}".format(event.get("code", "No code found"),
                                         event.get("description", "No description found")))

    return {
        "success": event["success"]
    }


def globus_publish_data(publish_client, transfer_client, metadata, collection,
                        data_ep=None, data_path=None, data_loc=None):
    if not data_loc:
        if not data_ep or not data_path:
            raise ValueError("Invalid call to globus_publish_data()")
        data_loc = []
    if data_ep and data_path:
        data_loc.append("globus://{}{}".format(data_ep, data_path))
    # Format collection
    collection_id = publish_collection_lookup(publish_client, collection)
    # Submit metadata
    pub_md = get_publish_metadata(metadata)
    md_result = publish_client.push_metadata(collection_id, pub_md)
    pub_endpoint = md_result['globus.shared_endpoint.name']
    pub_path = os.path.join(md_result['globus.shared_endpoint.path'], "data") + "/"
    submission_id = md_result["id"]
    # Transfer data
    for loc in data_loc:
        loc = loc.replace("globus://", "")
        ep, path = loc.split("/", 1)
        path = "/" + path + ("/" if not path.endswith("/") else "")
        transfer = mdf_toolbox.custom_transfer(
                        transfer_client, ep, pub_endpoint, [(path, pub_path)],
                        inactivity_time=CONFIG["TRANSFER_DEADLINE"], notify=False)
        for event in transfer:
            pass
        if not event["success"]:
            raise ValueError("{}: {}".format(event.get("code", "No code found"),
                                             event.get("description", "No description found")))
    # Complete submission
    fin_res = publish_client.complete_submission(submission_id)

    return fin_res.data


def publish_collection_lookup(publish_client, collection):
    valid_cols = publish_client.list_collections().data
    try:
        collection_id = int(collection)
    except ValueError:
        collection_id = 0
        for coll in valid_cols:
            if collection.replace(" ", "").lower() == coll["name"].replace(" ", "").lower():
                if collection_id:
                    raise ValueError("Collection name '{}' has multiple matches"
                                     .format(collection))
                collection_id = coll["id"]
    if not any([col["id"] == collection_id for col in valid_cols]):
        raise ValueError("Collection not found")

    return collection_id


def get_publish_metadata(metadata):
    dc_metadata = metadata.get("dc", {})
    # TODO: Find full Publish schema for translation
    # Required fields
    pub_metadata = {
        "dc.title": ", ".join([title.get("title", "")
                               for title in dc_metadata.get("titles", [])]),
        "dc.date.issued": str(date.today().year),
        "dc.publisher": "Materials Data Facility",
        "dc.contributor.author": [author.get("creatorName", "")
                                  for author in dc_metadata.get("creators", [])],
        "accept_license": True
    }
    return pub_metadata


def citrine_upload(citrine_data, api_key, mdf_dataset, previous_id=None,
                   public=CONFIG["DEFAULT_CITRINATION_PUBLIC"]):
    cit_client = CitrinationClient(api_key).data
    source_id = mdf_dataset.get("mdf", {}).get("source_id", "NO_ID")
    try:
        cit_title = mdf_dataset["dc"]["titles"][0]["title"]
    except (KeyError, IndexError, TypeError):
        cit_title = "Untitled"
    try:
        cit_desc = " ".join([desc["description"]
                             for desc in mdf_dataset["dc"]["descriptions"]])
        if not cit_desc:
            raise KeyError
    except (KeyError, IndexError, TypeError):
        cit_desc = None

    # Create new version if dataset previously created
    if previous_id:
        try:
            rev_res = cit_client.create_dataset_version(previous_id)
            assert rev_res.number > 1
        except Exception:
            previous_id = "INVALID"
        else:
            cit_ds_id = previous_id
            cit_client.update_dataset(cit_ds_id,
                                      name=cit_title,
                                      description=cit_desc,
                                      public=False)
    # Create new dataset if not created
    if not previous_id or previous_id == "INVALID":
        try:
            cit_ds_id = cit_client.create_dataset(name=cit_title,
                                                  description=cit_desc,
                                                  public=False).id
            assert cit_ds_id > 0
        except Exception as e:
            logger.warning("{}: Citrine dataset creation failed: {}".format(source_id, repr(e)))
            if previous_id == "INVALID":
                return {
                    "success": False,
                    "error": "Unable to create revision or new dataset in Citrine"
                }
            else:
                return {
                    "success": False,
                    "error": "Unable to create Citrine dataset, possibly due to duplicate entry"
                }

    success = 0
    failed = 0
    for path, _, files in os.walk(os.path.abspath(citrine_data)):
        for pif in files:
            up_res = cit_client.upload(cit_ds_id, os.path.join(path, pif))
            if up_res.successful():
                success += 1
            else:
                logger.warning("{}: Citrine upload failure: {}".format(source_id, str(up_res)))
                failed += 1

    cit_client.update_dataset(cit_ds_id, public=public)

    return {
        "success": bool(success),
        "cit_ds_id": cit_ds_id,
        "success_count": success,
        "failure_count": failed
        }


def cancel_submission(source_id, wait=True):
    """Cancel an in-progress submission.
    Will not cancel completed or already-cancelled submissions.

    Arguments:
    source_id (str): The source_id of the submission.
    wait (bool): If True, will wait on submission completion before returning success.
                 If False, will not wait.
                 Default True.
    Returns:
    success (bool): True if the submission was successfully cancelled, False otherwise.
    stopped (bool): True if the submission is no longer operating. False otherwise.
                    The difference between success and stopped is that a submission
                    that completed previously was not successfully cancelled, but was stopped.
                    Can be False when success is True if wait is True.
    error (str): The error message. Only exists if success is False.
    """
    logger.debug("Attempting to cancel {}".format(source_id))
    # Check if submission can be cancelled
    stat_res = read_status(source_id)
    if not stat_res["success"]:
        stat_res["stopped"] = False
        return stat_res
    current_status = stat_res["status"]
    if current_status["cancelled"]:
        return {
            "success": False,
            "error": "Submission already cancelled",
            "stopped": True
        }
    elif not current_status["active"]:
        return {
            "success": False,
            "error": "Submission already completed",
            "stopped": True
        }
    # Check if PID still alive, if dead, is effectively cancelled
    else:
        try:
            # If this does not throw exception, process is alive
            os.kill(current_status["pid"], 0)  # Signal 0 is noop
        except ProcessLookupError:
            # No process found
            complete_submission(source_id)
            return {
                "success": False,
                "error": "Submission not processing",
                "stopped": True
            }
        except Exception:
            # Other exception unexpected
            raise

    # Change submission to cancelled
    update_res = modify_status_entry(source_id, {"cancelled": True})
    if not update_res["success"]:
        return {
            "success": False,
            "error": update_res["error"],
            "stopped": False
        }

    # Wait for completion if requested
    if wait:
        try:
            while read_status(source_id)["status"]["active"]:
                os.kill(current_status["pid"], 0)  # Triggers ProcessLookupError on failure
                logger.info("Waiting for submission {} (PID {}) to cancel".format(
                                                                            source_id,
                                                                            current_status["pid"]))
                time.sleep(CONFIG["CANCEL_WAIT_TIME"])
        except ProcessLookupError:
            # Process is dead
            complete_submission(source_id)

    # Change status code to reflect cancellation
    old_status_code = read_status(source_id)["status"]["code"]
    new_status_code = old_status_code.replace("z", "X").replace("W", "X") \
                                     .replace("T", "X").replace("P", "W")
    update_res = modify_status_entry(source_id, {"code": new_status_code})
    if not update_res["success"]:
        return {
            "success": False,
            "error": update_res["error"],
            "stopped": wait
        }
    logger.debug("Submission {} cancelled: {}".format(source_id, new_status_code))

    return {
        "success": True,
        "stopped": wait
    }


def complete_submission(source_id, cleanup=CONFIG["DEFAULT_CLEANUP"]):
    """Complete a submission.

    Arguments:
    source_id (str): The source_id of the submission.
    cleanup (bool): If True, will delete the local submission data.
                    If False, will not.
                    Default True.

    Returns:
    success (bool): True on success, False otherwise.
    error (str): The error message. Only exists if success is False.
    """
    # Check that status active is True
    if not read_status(source_id).get("status", {}).get("active", False):
        return {
            "success": False,
            "error": "Submission not in progress"
        }
    logger.debug("{}: Starting cleanup".format(source_id))
    # Remove dirs containing processed data, if requested
    if cleanup:
        cleanup_paths = [
            os.path.join(CONFIG["LOCAL_PATH"], source_id) + "/",
            os.path.join(CONFIG["SERVICE_DATA"], source_id) + "/"
        ]
        for cleanup in cleanup_paths:
            if os.path.exists(cleanup):
                try:
                    clean_res = local_admin_delete(cleanup)
                    if not clean_res["success"]:
                        raise IOError(clean_res["error"])
                    elif not clean_res["deleted"]:
                        logger.warning("{}: Cleanup path '{}' did not exist".format(source_id,
                                                                                    cleanup))
                except Exception as e:
                    logger.error("{}: Could not remove path '{}': {}".format(source_id,
                                                                             cleanup, e))
                    return {
                        "success": False,
                        "error": "Unable to clear processed data"
                    }
            else:
                logger.debug("{}: Cleanup path does not exist: {}".format(source_id, cleanup))
        logger.debug("{}: File cleanup finished".format(source_id))
    # Update status to inactive
    update_res = modify_status_entry(source_id, {"active": False})
    if not update_res["success"]:
        return update_res

    logger.debug("{}: Cleanup finished".format(source_id))
    return {
        "success": True
    }


def local_admin_delete(path):
    """Use sudo permissions to delete a path.
    This is a separate function to isolate destructive behavior and assert sanity-checks.

    Arguments:
    path (str): The path to delete. This must be a string.
                For safety, this must be a path inside a user's home directory.
                Ex. "/home/[user]/[some path]"

    Returns:
    dict:
        success (bool): True iff the path does not exist now.
        deleted (bool): True iff the path was deleted by this function.
                        This is more strict than success; the path must have existed
                        before this function was called and then been removed.
        error (str): The error message, if not successful.
    """
    if not isinstance(path, str):
        return {
            "success": False,
            "deleted": False,
            "error": "Path must be a string"
        }
    path = os.path.abspath(os.path.expanduser(path))
    if not re.match("^/home/.+/.+", path):
        return {
            "success": False,
            "deleted": False,
            "error": "Path must be inside a user's home directory"
        }
    if not os.path.exists(path):
        return {
            "success": True,
            "deleted": False
        }

    try:
        if os.path.isdir(path):
            proc_res = subprocess.run(["sudo", "rm", "-rf", path])
        elif os.path.isfile(path):
            proc_res = subprocess.run(["sudo", "rm", path])
        else:
            return {
                "success": False,
                "deleted": False,
                "error": "Path must lead to a directory or file"
            }
    except Exception as e:
        return {
            "success": False,
            "deleted": False,
            "error": "Exception while deleting path: {}".format(e)
        }
    if proc_res.returncode == 0:
        return {
            "success": True,
            "deleted": True
        }
    else:
        return {
            "success": False,
            "deleted": False,
            "error": "Process exited with return code {}".format(proc_res.returncode)
        }


def validate_status(status, code_mode=None):
    """Validate a submission status.

    Arguments:
    status (dict): The status to validate.
    code_mode (str): The mode to check the status code, or None to skip code check.
                        "convert": No steps have started or finished.
                        "ingest": All convert steps have finished (except the ingest handoff)
                                  and no ingest steps have started or finished.

    Returns:
    dict:
        success: True if the status is valid, False if not.
        error: If the status is not valid, the reason why. Only present when success is False.
        details: Optional further details about an error.
    """
    # Load status schema
    with open(os.path.join(CONFIG["SCHEMA_PATH"], "internal_status.json")) as schema_file:
        schema = json.load(schema_file)
    resolver = jsonschema.RefResolver(base_uri="file://{}/".format(CONFIG["SCHEMA_PATH"]),
                                      referrer=schema)
    # Validate against status schema
    try:
        jsonschema.validate(status, schema, resolver=resolver)
    except jsonschema.ValidationError as e:
        return {
            "success": False,
            "error": "Invalid status: {}".format(str(e).split("\n")[0]),
            "details": str(e)
        }

    code = status["code"]
    try:
        assert len(code) == len(STATUS_STEPS)
        if code_mode == "convert":
            # Nothing started or finished
            assert code == "z" * len(STATUS_STEPS)
        elif code_mode == "ingest":
            # convert finished until handoff
            assert all([c in SUCCESS_CODES for c in code[:INGEST_MARK-1]])
            # convert handoff to ingest in progress
            assert code[INGEST_MARK-1] == "P"
            # ingest not started
            assert code[INGEST_MARK:] == "z" * (len(STATUS_STEPS) - INGEST_MARK)
    except AssertionError:
        return {
            "success": False,
            "error": "Invalid status code '{}' for mode {}".format(code, code_mode)
        }
    else:
        return {
            "success": True
        }


def read_status(source_id):
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]

    status_res = table.get_item(Key={"source_id": source_id}, ConsistentRead=True).get("Item")
    if not status_res:
        return {
            "success": False,
            "error": "ID {} not found in status database".format(source_id)
            }
    return {
        "success": True,
        "status": status_res
        }


def create_status(status):
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]

    # Add defaults
    status["messages"] = ["No message available"] * len(STATUS_STEPS)
    status["code"] = "z" * len(STATUS_STEPS)
    status["active"] = True
    status["cancelled"] = False
    status["pid"] = os.getpid()
    status["extensions"] = []
    status["converted"] = False

    status_valid = validate_status(status, "convert")
    if not status_valid["success"]:
        return status_valid

    # Check that status does not already exist
    if read_status(status["source_id"])["success"]:
        return {
            "success": False,
            "error": "ID {} already exists in database".format(status["source_id"])
            }
    try:
        table.put_item(Item=status)
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
            }
    else:
        logger.info("Status for {}: Created".format(status["source_id"]))
        return {
            "success": True,
            "status": status
            }


def update_status(source_id, step, code, text=None, link=None, except_on_fail=False):
    """Update the status of a given submission.

    Arguments:
    source_id (str): The source_id of the submission.
    step (str or int): The step of the process to update.
    code (char): The applicable status code character.
    text (str): The message or error text. Only used if required for the code. Default None.
    link (str): The link to add. Only used if required for the code. Default None.
    except_on_fail (bool): If True, will raise an Exception if the status cannot be updated.
                           If False, will return a dict as normal, with success=False.

    Returns:
    dict: success (bool): Success state
          error (str): The error. Only exists if success is False.
          status (str): The updated status. Only exists if success is True.
    """
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        if except_on_fail:
            raise ValueError(tbl_res["error"])
        return tbl_res
    table = tbl_res["table"]
    # Get old status
    old_status = read_status(source_id)
    if not old_status["success"]:
        if except_on_fail:
            raise ValueError(old_status["error"])
        return old_status
    status = old_status["status"]
    # Update code
    try:
        step_index = int(step) - 1
    except ValueError:
        step_index = None
        # Why yes, this would be easier if STATUS_STEPS was a dict
        # But we need slicing for translate_status
        # Or else we're duplicating the info and making maintenance hard
        # And I'd rather one dumb hack than several painful, error-prone changes
        for i, s in enumerate(STATUS_STEPS):
            if step == s[0]:
                step_index = i
                break
    code_list = list(status["code"])
    code_list[step_index] = code
    # If needed, update messages or errors and cancel tasks
    if code == 'M':
        status["messages"][step_index] = (text or "No message available")
    elif code == 'L':
        status["messages"][step_index] = [
            text or "No message available",
            link or "No link available"
        ]
    elif code == 'F':
        status["messages"][step_index] = (text or "An error occurred and we're trying to fix it")
        # Cancel subsequent tasks
        code_list = code_list[:step_index+1] + ["X"]*len(code_list[step_index+1:])
    elif code == 'H':
        status["messages"][step_index] = [text or "An error occurred and we're trying to fix it",
                                          link or "Help may be available soon."]
        # Cancel subsequent tasks
        code_list = code_list[:step_index+1] + ["X"]*len(code_list[step_index+1:])
    elif code == 'R':
        status["messages"][step_index] = (text or "An error occurred but we're recovering")
    elif code == 'U':
        status["messages"][step_index] = (text or "Processing will continue")
    elif code == 'T':
        status["messages"][step_index] = (text or "Retrying")
    status["code"] = "".join(code_list)

    status_valid = validate_status(status)
    if not status_valid["success"]:
        if except_on_fail:
            raise ValueError(status_valid["error"])
        return status_valid

    try:
        # put_item will overwrite
        table.put_item(Item=status)
    except Exception as e:
        if except_on_fail:
            raise
        else:
            return {
                "success": False,
                "error": repr(e)
            }
    else:
        logger.info("[{}]{}: {}: {}, {}, {}".format(status["pid"], source_id, step, code,
                                                    text, link))
        return {
            "success": True,
            "status": status
            }


def modify_status_entry(source_id, modifications, except_on_fail=False):
    """Change the status entry of a given submission.
    This is a generalized (and more powerful) version of update_status.
    This function should be used carefully, as most fields in the status DB should never change.

    Arguments:
    source_id (str): The source_id of the submission.
    modifications (dict): The keys and values to update.
    except_on_fail (bool): If True, will raise an Exception if the status cannot be updated.
                           If False, will return a dict as normal, with success=False.

    Returns:
    dict: success (bool): Success state
          error (str): The error. Only exists if success is False.
          status (str): The updated status. Only exists if success is True.
    """
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        if except_on_fail:
            raise ValueError(tbl_res["error"])
        return tbl_res
    table = tbl_res["table"]
    # Get old status
    old_status = read_status(source_id)
    if not old_status["success"]:
        if except_on_fail:
            raise ValueError(old_status["error"])
        return old_status
    status = old_status["status"]

    # Overwrite old status
    status = mdf_toolbox.dict_merge(deepcopy(modifications), status)

    status_valid = validate_status(status)
    if not status_valid["success"]:
        if except_on_fail:
            raise ValueError(status_valid["error"])
        return status_valid

    try:
        # put_item will overwrite
        table.put_item(Item=status)
    except Exception as e:
        if except_on_fail:
            raise
        return {
            "success": False,
            "error": repr(e)
            }
    else:
        logger.info("[{}]{}: Modified: '{}'".format(status["pid"], source_id, modifications))
        return {
            "success": True,
            "status": status
            }


def translate_status(status):
    # {
    # source_id: str,
    # submission_code: "C" or "I"
    # code: str, based on char position
    # messages: list of str, in order generated
    # errors: list of str, in order of failures
    # title: str,
    # submitter: str,
    # submission_time: str
    # }
    full_code = list(status["code"])
    messages = status["messages"]
    sub_type = status["submission_code"]
    # Submission type determines steps
    if sub_type == 'C':
        steps = [st[1] for st in STATUS_STEPS]
        subm = "convert"
    elif sub_type == 'I':
        steps = [st[1] for st in STATUS_STEPS[INGEST_MARK:]]
        subm = "ingest"
    else:
        steps = []
        subm = "unknown submission type '{}'".format(sub_type)

    usr_msg = ("Status of {}{} submission {} ({})\n"
               "Submitted by {} at {}\n\n").format("TEST " if status["test"] else "",
                                                   subm,
                                                   status["source_id"],
                                                   status["title"],
                                                   status["submitter"],
                                                   status["submission_time"])
    web_msg = []

    for code, step, index in zip(full_code, steps, range(len(steps))):
        if code == 'S':
            msg = "{} was successful.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "success",
                "text": msg
            })
        elif code == 'M':
            msg = "{} was successful: {}.".format(step, messages[index])
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "success",
                "text": msg
            })
        elif code == 'L':
            tup_msg = messages[index]
            msg = "{} was successful: {}.".format(step, tup_msg[0])
            usr_msg += msg + " Link: {}\n".format(tup_msg[1])
            web_msg.append({
                "signal": "success",
                "text": msg,
                "link": tup_msg[1]
            })
        elif code == 'F':
            msg = "{} failed: {}.".format(step, messages[index])
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "failure",
                "text": msg
            })
        elif code == 'R':
            msg = "{} failed (processing will continue): {}.".format(step, messages[index])
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "failure",
                "text": msg
            })
        elif code == 'U':
            msg = "{} was unsuccessful: {}.".format(step, messages[index])
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "warning",
                "text": msg
            })
        elif code == 'H':
            tup_msg = messages[index]
            msg = "{} failed: {}.".format(step, tup_msg[0])
            usr_msg += msg + " Link: {}\n".format(tup_msg[1])
            web_msg.append({
                "signal": "failure",
                "text": msg,
                "link": tup_msg[1]
            })
        elif code == 'N':
            msg = "{} was not requested or required.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "idle",
                "text": msg
            })
        elif code == 'P':
            msg = "{} is in progress.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "started",
                "text": msg
            })
        elif code == 'T':
            msg = "{} is retrying due to an error: {}".format(step, messages[index])
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "started",
                "text": msg
            })
        elif code == 'X':
            msg = "{} was cancelled.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "idle",
                "text": msg
            })
        elif code == 'z':
            msg = "{} has not started yet.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "idle",
                "text": msg
            })
        else:
            msg = "{} is unknown. Code: '{}', message: '{}'".format(step, code, messages[index])
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "warning",
                "text": msg
            })

    return {
        "source_id": status["source_id"],
        "status_message": usr_msg,
        "status_list": web_msg,
        "status_code": status["code"],
        "title": status["title"],
        "submitter": status["submitter"],
        "submission_time": status["submission_time"],
        "test": status["test"],
        "active": status["active"]
        }


def initialize_dmo_table(client=DMO_CLIENT, table_name=DMO_TABLE, schema=DMO_SCHEMA):
    tbl_res = get_dmo_table(client, table_name)
    # Table should not be active already
    if tbl_res["success"]:
        return {
            "success": False,
            "error": "Table already created"
            }
    # If misc/other exception, cannot create table
    elif tbl_res["error"] != "Table does not exist or is not active":
        return tbl_res

    try:
        new_table = client.create_table(**schema)
        new_table.wait_until_exists()
    except client.meta.client.exceptions.ResourceInUseException:
        return {
            "success": False,
            "error": "Table concurrently created"
            }
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
            }

    tbl_res2 = get_dmo_table(client, table_name)
    if not tbl_res2["success"]:
        return {
            "success": False,
            "error": "Unable to create table: {}".format(tbl_res2["error"])
            }
    else:
        return {
            "success": True,
            "table": tbl_res2["table"]
            }


def get_dmo_table(client=DMO_CLIENT, table_name=DMO_TABLE):
    try:
        table = client.Table(table_name)
        dmo_status = table.table_status
        if dmo_status != "ACTIVE":
            raise ValueError("Table not active")
    except (ValueError, client.meta.client.exceptions.ResourceNotFoundException):
        return {
            "success": False,
            "error": "Table does not exist or is not active"
            }
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
            }
    else:
        return {
            "success": True,
            "table": table
            }


def submit_to_queue(entry):
    """Submit entry to SQS queue.

    Arguments:
    entry (dict): The entry to submit.

    Returns:
    dict: The result.
        success (bool): True when the entry was successfully submitted, False otherwise.
        error (str): Present when success is False. The reason for failure.
    """
    queue_res = get_sqs_queue(SQS_CLIENT, SQS_QUEUE_NAME)
    if not queue_res["success"]:
        return queue_res
    queue = queue_res["queue"]
    try:
        # Send message and check that return value has MD5OfMessageBody
        if not queue.send_message(MessageBody=json.dumps(entry),
                                  MessageGroupId=SQS_GROUP).get("MD5OfMessageBody"):
            return {
                "success": False,
                "error": "Message unable to be sent"
            }
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
        }
    else:
        return {
            "success": True
        }


def retrieve_from_queue(wait_time=0, max_entries=10):
    """Retrieve entries from SQS queue.

    Arguments:
    wait_time (int): The number of seconds to wait on a message. Default 0.
    max_entries (int): The maximum number of entries to return. Default 10, the AWS limit.

    Returns:
    dict: The result.
        success (bool): True when successful, False, otherwise.
        error (str): When success is False, the error message.
        entries (list): When successful, the list of entries.
        delete_info (list): When successful, the value to pass to delete_from_queue
    """
    queue_res = get_sqs_queue(SQS_CLIENT, SQS_QUEUE_NAME)
    if not queue_res["success"]:
        return queue_res
    queue = queue_res["queue"]
    try:
        messages = queue.receive_messages(MaxNumberOfMessages=max_entries,
                                          WaitTimeSeconds=wait_time)
        entries = []
        delete_info = []
        for msg in messages:
            entries.append(json.loads(msg.body))
            delete_info.append({
                "Id": msg.message_id,
                "ReceiptHandle": msg.receipt_handle
            })
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
        }
    else:
        if len(entries) != len(delete_info):
            return {
                "success": False,
                "error": "Unable to match message body to ID and receipt handle."
            }
        else:
            return {
                "success": True,
                "entries": entries,
                "delete_info": delete_info
            }


def delete_from_queue(delete_info):
    if not delete_info:
        return {
            "success": False,
            "error": "No entries submitted for deletion"
        }
    queue_res = get_sqs_queue(SQS_CLIENT, SQS_QUEUE_NAME)
    if not queue_res["success"]:
        return queue_res
    queue = queue_res["queue"]
    try:
        del_res = queue.delete_messages(Entries=delete_info)
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
        }
    else:
        if len(del_res.get("Failed", [])):
            return {
                "success": False,
                "error": del_res
            }
        else:
            return {
                "success": True
            }


def get_sqs_queue(client=SQS_CLIENT, queue_name=SQS_QUEUE_NAME):
    try:
        queue = client.get_queue_by_name(QueueName=queue_name)
    except (ValueError, client.meta.client.exceptions.QueueDoesNotExist):
        return {
            "success": False,
            "error": "Queue does not exist"
        }
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
        }
    else:
        return {
            "success": True,
            "queue": queue
        }


def initialize_sqs_queue(client=SQS_CLIENT, queue_name=SQS_QUEUE_NAME,
                         attributes=SQS_ATTRIBUTES):
    q_res = get_sqs_queue(client=client, queue_name=queue_name)
    # Queue must not already exist
    if q_res["success"]:
        return {
            "success": False,
            "error": "Queue already exists"
        }
    # Other error indicates other problem
    elif q_res["error"] != "Queue does not exist":
        return q_res

    try:
        client.create_queue(QueueName=queue_name, Attributes=attributes)
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
        }
    q_res = get_sqs_queue(client=client, queue_name=queue_name)
    if not q_res["success"]:
        return q_res
    else:
        return {
            "success": True,
            "queue": q_res["queue"]
        }
