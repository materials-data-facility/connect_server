from datetime import date
import logging
import os
import re
import shutil
import time
import urllib

import boto3
from citrination_client import CitrinationClient
import globus_sdk
import mdf_toolbox
import requests

from mdf_connect import app


logger = logging.getLogger(__name__)

# DynamoDB setup
DMO_CLIENT = boto3.resource('dynamodb',
                            aws_access_key_id=app.config["DYNAMO_KEY"],
                            aws_secret_access_key=app.config["DYNAMO_SECRET"],
                            region_name="us-east-1")
DMO_TABLE = app.config["DYNAMO_TABLE"]
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

# Global save locations for whitelists
CONVERT_GROUP = {
    # Globus Groups UUID
    "group_id": app.config["CONVERT_GROUP_ID"],
    # Group member IDs
    "whitelist": [],
    # UNIX timestamp of last update
    "updated": 0,
    # Refresh frequency (in seconds)
    #   X days * 24 hours/day * 60 minutes/hour * 60 seconds/minute
    "frequency": 1 * 60 * 60  # 1 hour
}
INGEST_GROUP = {
    "group_id": app.config["INGEST_GROUP_ID"],
    "whitelist": [],
    "updated": 0,
    "frequency": 1 * 24 * 60 * 60  # 1 day
}
ADMIN_GROUP = {
    "group_id": app.config["ADMIN_GROUP_ID"],
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
        auth_client = globus_sdk.ConfidentialAppAuthClient(app.config["API_CLIENT_ID"],
                                                           app.config["API_CLIENT_SECRET"])
        auth_res = auth_client.oauth2_token_introspect(token, include="identities_set")
    except Exception as e:
        return {
            "success": False,
            # TODO: Check that the exception doesn't leak info
            "error": "Unacceptable auth: " + repr(e),
            "error_code": 400
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
    if (app.config["API_SCOPE"] not in auth_res["scope"]
            or app.config["API_SCOPE_ID"] not in auth_res["aud"]):
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
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
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
        except Exception as e:
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


def download_and_backup(mdf_transfer_client, data_loc,
                        local_ep, local_path, backup_ep=None, backup_path=None):
    """Download data from a remote host to the configured machine.

    Arguments:
    mdf_transfer_client (TransferClient): An authenticated TransferClient.
    data_loc (list of str): The location(s) of the data.
    local_ep (str): The local machine's endpoint ID.
    local_path (str): The path to the local storage location.
    backup_ep (str): The backup machine's endpoint ID. Default None for no backup.
    backup_path (str): The path to the backup storage location Default None for no backup.

    Returns:
    dict: success (bool): True on success, False on failure.
    """
    os.makedirs(local_path, exist_ok=True)
    if not isinstance(data_loc, list):
        raise TypeError("Data locations must be in a list")
    # Download data locally
    for location in data_loc:
        loc_info = urllib.parse.urlparse(location)

        # Special case pre-processing
        # Globus Web App link into globus:// form
        if location.startswith("https://www.globus.org/app/transfer"):
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
            # Don't use os.path.join because path starts with /
            # GDRIVE_ROOT does not end in / to make compatible
            location = "globus://{}{}{}".format(app.config["GDRIVE_EP"],
                                                app.config["GDRIVE_ROOT"], loc_info.path)
            loc_info = urllib.parse.urlparse(location)

        # Globus Transfer
        if loc_info.scheme == "globus":
            # Check that data not already in place
            if loc_info.netloc != local_ep and loc_info.path != local_path:
                # If there is a dir mismatch (one has trailing slash, other does not)
                # has_slash XOR has_slash
                if (loc_info.path[-1] == "/") != (local_path[-1] == "/"):
                    # If Transferring file to dir, add file to dir
                    # Otherwise error - cannot transfer dir into file
                    f_name = os.path.basename(loc_info.path)
                    if not f_name:
                        raise ValueError("Cannot back up a directory into a file")
                    transfer_path = os.path.join(local_path, f_name)
                else:
                    transfer_path = local_path
                # Transfer locally
                transfer = mdf_toolbox.custom_transfer(
                                mdf_transfer_client, loc_info.netloc, local_ep,
                                [(loc_info.path, transfer_path)],
                                interval=app.config["TRANSFER_PING_INTERVAL"],
                                inactivity_time=app.config["TRANSFER_DEADLINE"])
                for event in transfer:
                    if not event["success"]:
                        logger.info("Transfer is_error: {} - {}".format(event["code"],
                                                                        event["description"]))
                        yield {
                            "success": False,
                            "error": "{} - {}".format(event["code"], event["description"])
                        }
                if not event["success"]:
                    raise ValueError(event)
        # HTTP(S)
        elif loc_info.scheme.startswith("http"):
            # Get extension (mostly for debugging)
            ext = os.path.splitext(loc_info.path)[1]
            if not ext:
                ext = ".archive"

            archive_path = os.path.join(local_path, "archive"+ext)

            # Fetch file
            res = requests.get(location)
            with open(archive_path, 'wb') as out:
                out.write(res.content)
        # Not supported
        else:
            # Nothing to do
            raise IOError("Invalid data location: '{}' is not a recognized protocol "
                          "(from {}).".format(loc_info.scheme, str(location)))

    # Extract all archives, delete extracted archives
    extract_res = mdf_toolbox.uncompress_tree(local_path, delete_archives=True)
    if not extract_res["success"]:
        raise IOError("Unable to extract archives in dataset")

    # Back up data
    if backup_ep and backup_path:
        transfer = mdf_toolbox.custom_transfer(
                        mdf_transfer_client, local_ep, backup_ep, [(local_path, backup_path)],
                        interval=app.config["TRANSFER_PING_INTERVAL"],
                        inactivity_time=app.config["TRANSFER_DEADLINE"])
        for event in transfer:
            if not event["success"]:
                logger.debug(event)
        if not event["success"]:
            raise ValueError(event["code"]+": "+event["description"])
    yield {
        "success": True,
        "num_extracted": extract_res["num_extracted"]
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
                        inactivity_time=app.config["TRANSFER_DEADLINE"])
        for event in transfer:
            pass
        if not event["success"]:
            raise ValueError(event["code"]+": "+event["description"])
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
                   public=app.config["DEFAULT_CITRINATION_PUBLIC"]):
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
    success (bool): True on success, False otherwise.
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
        return stat_res
    current_status = stat_res["status"]
    if current_status["cancelled"]:
        return {
            "success": False,
            "error": "Submission already cancelled",
            "stopped": True
        }
    elif current_status["completed"]:
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
        while not read_status(source_id)["status"]["completed"]:
            logger.info("Waiting for submission {} to cancel".format(source_id))
            time.sleep(app.config["CANCEL_WAIT_TIME"])

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


def complete_submission(source_id, cleanup=True):
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
    # Check that status completed is False
    if read_status(source_id).get("status", {}).get("completed", True):
        return {
            "success": False,
            "error": "Submission not in progress"
        }
    logger.debug("{}: Starting cleanup".format(source_id))
    # Remove dirs containing processed data, if requested
    if cleanup:
        cleanup_paths = [
            os.path.join(app.config["LOCAL_PATH"], source_id) + "/",
            os.path.join(app.config["SERVICE_DATA"], source_id) + "/"
        ]
        for cleanup in cleanup_paths:
            if os.path.exists(cleanup):
                try:
                    shutil.rmtree(cleanup)
                except Exception as e:
                    logger.warning("{}: Could not remove path '{}': {}".format(source_id,
                                                                               cleanup, repr(e)))
                    return {
                        "success": False,
                        "error": "Unable to clear processed data"
                    }
            else:
                logger.debug("{}: Cleanup path does not exist: {}".format(source_id, cleanup))
    # Update status to "completed"
    update_res = modify_status_entry(source_id, {"completed": True})
    if not update_res["success"]:
        return update_res

    logger.debug("{}: Cleanup finished".format(source_id))
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
    else:
        return {
            "success": True,
            "status": status_res
            }


def create_status(status):
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]
    # TODO: Validate status better (JSONSchema?)
    if not status.get("source_id"):
        return {
            "success": False,
            "error": "source_id missing"
            }
    elif not status.get("submission_code"):
        return {
            "success": False,
            "error": "submission_code missing"
            }
    elif not status.get("title"):
        return {
            "success": False,
            "error": "title missing"
            }
    elif not status.get("submitter"):
        return {
            "success": False,
            "error": "submitter missing"
            }
    elif not status.get("submission_time"):
        return {
            "success": False,
            "error": "submission_time missing"
            }

    # Create defaults
    status["messages"] = ["No message available"] * len(STATUS_STEPS)
    status["code"] = "z" * len(STATUS_STEPS)
    status["completed"] = False
    status["cancelled"] = False
    status["pid"] = os.getpid()

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
        return tbl_res
    table = tbl_res["table"]
    # TODO: Validate status
    # Get old status
    old_status = read_status(source_id)
    if not old_status["success"]:
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
        logger.info("{}: {}: {}, {}, {}".format(source_id, step, code, text, link))
        return {
            "success": True,
            "status": status
            }


def modify_status_entry(source_id, modifications):
    """Change the status entry of a given submission.
    This is a generalized (and more powerful) version of update_status.
    This function should be used carefully, as most fields in the status DB should never change.

    Arguments:
    source_id (str): The source_id of the submission.
    modifications (dict): The keys and values to update.

    Returns:
    dict: success (bool): Success state
          error (str): The error. Only exists if success is False.
          status (str): The updated status. Only exists if success is True.
    """
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]
    # TODO: Validate status
    # Get old status
    old_status = read_status(source_id)
    if not old_status["success"]:
        return old_status
    status = old_status["status"]

    # Overwrite old status
    status = mdf_toolbox.dict_merge(modifications, status)

    try:
        # put_item will overwrite
        table.put_item(Item=status)
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
            }
    else:
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
        "test": status["test"]
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
