from copy import deepcopy
import json
import logging
import os
import random
import re
import string
import subprocess
import time
import urllib

import boto3
from boto3.dynamodb.conditions import Attr
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
DMO_TABLES = {
    "status": CONFIG["DYNAMO_STATUS_TABLE"],
    "curation": CONFIG["DYNAMO_CURATION_TABLE"]
}
DMO_SCHEMA = {
    # "TableName": DMO_TABLE,
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
    ("sub_start", "Submission initialization"),
    ("data_download", "Connect data download"),
    ("data_transfer", "Data transfer to primary destination"),
    ("extracting", "Metadata extraction"),
    ("curation", "Dataset curation"),
    ("ingest_search", "MDF Search ingestion"),
    ("ingest_backup", "Data transfer to secondary destinations"),
    ("ingest_publish", "MDF Publish publication"),
    ("ingest_citrine", "Citrine upload"),
    ("ingest_mrr", "Materials Resource Registration"),
    ("ingest_cleanup", "Post-processing cleanup")
)

# Status codes indicating some form of not-failure,
# defined as "the step is over, and processing is continuing"
SUCCESS_CODES = [
    "S",
    "M",
    "L",
    "R",
    "N"
]


def authenticate_token(token, groups, require_all=False):
    """Authenticate a token.
    Arguments:
        token (str): The token to authenticate with.
        groups (str or list of str): The Globus Group UUIDs to require the user belong to.
                The special value "public" is also allowed to always pass this check.
        require_all (bool): When True, the user must be in all groups to succeed the
                group check.
                When False, the user must be in at least one group to succeed.
                Default False.

    Returns:
        dict: Token and user info.
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
    # Finally, verify user is in appropriate group(s)
    if isinstance(groups, str):
        groups = [groups]

    # Groups setup
    groups_auth = deepcopy(CONFIG["GLOBUS_CREDS"])
    groups_auth["services"] = ["groups"]
    try:
        nexus = mdf_toolbox.confidential_login(**groups_auth)["groups"]
    except Exception as e:
        logger.error("NexusClient creation error: {}".format(repr(e)))
        return {
            "success": False,
            "error": "Unable to connect to Globus Groups",
            "error_code": 500
        }

    # Globus Groups does not take UUIDs, only usernames, but Globus Auth uses UUIDs
    # for identity-aware applications. Therefore, for Connect to be identity-aware,
    # we must convert the UUIDs into usernames.
    # However, the GlobusID "username" is not the email-like address, just the prefix.
    user_usernames = set([iden["username"].replace("@globusid.org", "")
                          for iden in auth_client.get_identities(
                                                    ids=auth_res["identities_set"])["identities"]])
    auth_succeeded = False
    missing_groups = []  # Used for require_all compliance
    group_roles = []
    for grp in groups:
        # public always succeeds
        if grp.lower() == "public":
            group_roles.append("member")
            auth_succeeded = True
        else:
            # Translate convert and admin groups
            if grp.lower() == "extract" or grp.lower() == "convert":
                grp = CONFIG["EXTRACT_GROUP_ID"]
            elif grp.lower() == "admin":
                grp = CONFIG["ADMIN_GROUP_ID"]
            # Group membership checks - each identity with each group
            for user_identifier in user_usernames:
                try:
                    member_info = nexus.get_group_membership(grp, user_identifier)
                    assert member_info["status"] == "active"
                    group_roles.append(member_info["role"])
                # Not in group or not active
                except (globus_sdk.GlobusAPIError, AssertionError):
                    # Log failed groups
                    missing_groups.append(grp)
                # Error getting membership
                except Exception as e:
                    logger.error("NexusClient fetch error: {}".format(repr(e)))
                    return {
                        "success": False,
                        "error": "Unable to connect to Globus Groups",
                        "error_code": 500
                    }
                else:
                    auth_succeeded = True
    # If must be in all groups, fail out if any groups missing
    if require_all and missing_groups:
        logger.debug("Auth rejected: require_all set, user '{}' not in '{}'"
                     .format(user_usernames, missing_groups))
        return {
            "success": False,
            "error": "You cannot access this service or organization",
            "error_code": 403
        }
    if not auth_succeeded:
        logger.debug("Auth rejected: User '{}' not in any group: '{}'"
                     .format(user_usernames, groups))
        return {
            "success": False,
            "error": "You cannot access this service or organization",
            "error_code": 403
        }

    # Admin membership check (allowed to fail)
    is_admin = False
    for user_identifier in user_usernames:
        try:
            admin_info = nexus.get_group_membership(CONFIG["ADMIN_GROUP_ID"], user_identifier)
            assert admin_info["status"] == "active"
        # Username is not active admin, which is fine
        except (globus_sdk.GlobusAPIError, AssertionError):
            pass
        # Error getting membership
        except Exception as e:
            logger.error("NexusClient admin fetch error: {}".format(repr(e)))
            return {
                "success": False,
                "error": "Unable to connect to Globus Groups",
                "error_code": 500
            }
        # Successful check, is admin
        else:
            is_admin = True

    return {
        "success": True,
        "token_info": auth_res,
        "user_id": auth_res["sub"],
        "username": user_identifier,
        "name": auth_res["name"] or "Not given",
        "email": auth_res["email"] or "Not given",
        "identities_set": auth_res["identities_set"],
        "group_roles": group_roles,
        "is_admin": is_admin
    }


def make_source_id(title, author, test=False, index=None, sanitize_only=False):
    """Make a source name out of a title."""
    if index is None:
        index = (CONFIG["INGEST_TEST_INDEX"] if test else CONFIG["INGEST_INDEX"])
    # Stopwords to delete from the source_name
    # Not using NTLK to avoid an entire package dependency for one minor feature,
    # and the NLTK stopwords are unlikely to be in a dataset title ("your", "that'll", etc.)
    delete_words = [
        "a",
        "an",
        "and",
        "as",
        "data",
        "dataset",
        "for",
        "from",
        "in",
        "of",
        "or",
        "study",
        "test",  # Clears test flag from new source_id
        "that",
        "the",
        "this",
        "to",
        "very",
        "with"
    ]
    # Remove any existing version number from title
    title = split_source_id(title)["source_name"]

    # Tokenize title and author
    # Valid token separators are space and underscore
    # Discard empty tokens
    title_tokens = [t for t in title.strip().replace("_", " ").split() if t]
    author_tokens = [t for t in author.strip().replace("_", " ").split() if t]

    # Clean title tokens
    title_clean = []
    for token in title_tokens:
        # Clean token is lowercase and alphanumeric
        # TODO: After Py3.7 upgrade, use .isascii()
        clean_token = "".join([char for char in token.lower() if char.isalnum()])
        # and char.isascii()])
        if clean_token and clean_token not in delete_words:
            title_clean.append(clean_token)

    # Clean author tokens, merge into one word
    author_word = ""
    for token in author_tokens:
        clean_token = "".join([char for char in token.lower() if char.isalnum()])
        # and char.isascii()])
        author_word += clean_token

    # Remove author_word from title, if exists (e.g. from previous make_source_id())
    while author_word in title_clean and not sanitize_only:
        title_clean.remove(author_word)

    # Select words from title for source_name
    # Use up to the first two words + last word
    if len(title_clean) >= 1:
        word1 = title_clean[0]
    else:
        # Must have at least one word
        raise ValueError("Title '{}' invalid: Must have at least one word that is not "
                         "the author name (the following words do not count: '{}')"
                         .format(title, delete_words))
    if len(title_clean) >= 2:
        word2 = title_clean[1]
    else:
        word2 = ""
    if len(title_clean) >= 3:
        word3 = title_clean[-1]
    else:
        word3 = ""

    # Assemble source_name
    # Strip trailing underscores from missing words
    if sanitize_only:
        source_name = "_".join(title_clean).strip("_")
    else:
        source_name = "{}_{}_{}_{}".format(author_word, word1, word2, word3).strip("_")

    # Add test flag if necessary
    if test:
        source_name = "_test_" + source_name

    # Determine version number to add
    # Get last Search version
    search_creds = mdf_toolbox.dict_merge(CONFIG["GLOBUS_CREDS"], {"services": ["search"]})
    search_client = mdf_toolbox.confidential_login(**search_creds)["search"]
    old_q = {
        "q": "mdf.source_name:{} AND mdf.resource_type:dataset".format(source_name),
        "advanced": True,
        "limit": 2,  # Should only ever be one, if two are returned there's a problem
        "offset": 0
    }
    old_search = mdf_toolbox.gmeta_pop(search_client.post_search(
                                            mdf_toolbox.translate_index(index), old_q))
    if len(old_search) == 0:
        search_version = 1
    elif len(old_search) == 1:
        search_version = old_search[0]["mdf"]["version"] + 1
    else:
        logger.error("{}: {} dataset entries found in Search: {}"
                     .format(source_name, len(old_search), old_search))
        raise ValueError("Dataset entry in Search has error")

    # Get old submission information
    scan_res = scan_table(table_name="status", fields=["source_id", "user_id"],
                          filters=[("source_id", "^", source_name)])
    if not scan_res["success"]:
        logger.error("Unable to scan status database for '{}': '{}'"
                     .format(source_name, scan_res["error"]))
        raise ValueError("Dataset status has error")
    user_ids = set([sub["user_id"] for sub in scan_res["results"]])
    # Get most recent previous source_id and info
    if scan_res["results"]:
        old_source_id = max([sub["source_id"] for sub in scan_res["results"]])
    else:
        old_source_id = ""
    old_source_info = split_source_id(old_source_id)
    old_search_version = old_source_info["search_version"]
    old_sub_version = old_source_info["submission_version"]
    # If new Search version > old Search version, sub version should reset
    if search_version > old_search_version:
        sub_version = 1
    # If they're the same, sub version should increment
    elif search_version == old_search_version:
        sub_version = old_sub_version + 1
    # Old > new is an error
    else:
        logger.error("Old Search version '{}' > new '{}': {}"
                     .format(old_search_version, search_version, source_name))
        raise ValueError("Dataset entry in Search has error")

    source_id = "{}_v{}.{}".format(source_name, search_version, sub_version)

    return {
        "source_id": source_id,
        "source_name": source_name,
        "search_version": search_version,
        "submission_version": sub_version,
        "user_id_list": user_ids
    }


def split_source_id(source_id):
    """Retrieve the source_name and version information from a source_id.
    Not complex logic, but easier to have in one location.
    Standard form: {source_name}_v{search_version}.{submission_version}

    Arguments:
    source_id (str): The source_id to split. If this is not a valid-form source_id,
                     the entire string will be assumed to be the source_name and source_id
                     and the versions will be 0.

    Returns:
    dict:
        success (bool): True if the versions were extracted, False otherwise.
        source_name (str): The base source_name.
        source_id (str): The assembled source_id.
        search_version (int): The Search version from the source_id.
        submission_version (int): The Connect version from the source_id.
    """
    # Check if source_id is valid
    if not re.search("_v[0-9]+\\.[0-9]+$", source_id):
        return {
            "success": False,
            "source_name": source_id,
            "source_id": source_id,
            "search_version": 0,
            "submission_version": 0
        }

    source_name, versions = source_id.rsplit("_v", 1)
    v_info = versions.split(".", 1)
    search_version, submission_version = v_info

    return {
        "success": True,
        "source_name": source_name,
        "source_id": "{}_v{}.{}".format(source_name, search_version, submission_version),
        "search_version": int(search_version),
        "submission_version": int(submission_version)
    }


def clean_start():
    """Reset the Connect environment to a clean state, as best as possible.
    """
    logger.debug("Cleaning Connect state")
    # Auth to get Transfer client
    transfer_creds = mdf_toolbox.dict_merge(CONFIG["GLOBUS_CREDS"], {"services": ["transfer"]})
    transfer_client = mdf_toolbox.confidential_login(**transfer_creds)["transfer"]
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


def fetch_org_rules(org_names, user_rules=None):
    """Fetch organization rules and metadata.

    Arguments:
        org_names (str or list of str): Org name or alias to fetch rules for.
        user_rules (dict): User-supplied rules to add, if desired. Default None.

    Returns:
        tuple: (list: All org canonical_names, dict: All appropriate rules)
    """
    # Normalize name: Remove special characters (including whitespace) and capitalization
    # Function for convenience, but not generalizable/useful for other cases
    def normalize_name(name): return "".join([c for c in name.lower() if c.isalnum()])

    # Fetch list of organizations
    with open(os.path.join(CONFIG["AUX_DATA_PATH"], "organizations.json")) as f:
        organizations = json.load(f)

    # Cache list of all organization aliases to match against
    # Turn into tuple (normalized_aliases, org_rules) for convenience
    all_clean_orgs = []
    for org in organizations:
        aliases = [normalize_name(alias) for alias in (org.get("aliases", [])
                                                       + [org["canonical_name"]])]
        all_clean_orgs.append((aliases, org))

    if isinstance(org_names, list):
        orgs_to_fetch = org_names
    else:
        orgs_to_fetch = [org_names]
    rules = {}
    all_names = []
    # Fetch org rules and parent rules
    while len(orgs_to_fetch) > 0:
        # Process sub 0 always, so orgs processed in order
        # New org matches on canonical_name or any alias
        fetch_org = orgs_to_fetch.pop(0)
        new_org_data = [org for aliases, org in all_clean_orgs
                        if normalize_name(fetch_org) in aliases]
        if len(new_org_data) < 1:
            raise ValueError("Organization '{}' not registered in MDF Connect (from '{}')"
                             .format(fetch_org, org_names))
        elif len(new_org_data) > 1:
            raise ValueError("Multiple organizations found with name '{}' (from '{}')"
                             .format(fetch_org, org_names))
        new_org_data = deepcopy(new_org_data[0])

        # Check that org rules not already fetched
        if new_org_data["canonical_name"] in all_names:
            continue
        else:
            all_names.append(new_org_data["canonical_name"])

        # Add all (unprocessed) parents to fetch list
        orgs_to_fetch.extend([parent for parent in new_org_data.get("parent_organizations", [])
                              if parent not in all_names])

        # Merge new rules with old
        # Strip out unneeded info
        new_org_data.pop("canonical_name", None)
        new_org_data.pop("aliases", None)
        new_org_data.pop("description", None)
        new_org_data.pop("homepage", None)
        new_org_data.pop("parent_organizations", None)
        # Save correct curation state
        if rules.get("curation", False) or new_org_data.get("curation", False):
            curation = True
        else:
            curation = False
        # Merge new rules into old rules
        rules = mdf_toolbox.dict_merge(rules, new_org_data, append_lists=True)
        # Ensure curation set if needed
        if curation:
            rules["curation"] = curation

    # Merge in user-set rules (with lower priority than any org-set rules)
    if user_rules:
        rules = mdf_toolbox.dict_merge(rules, user_rules)
        # If user set curation, set curation
        # Otherwise user preference is overridden by org preference
        if user_rules.get("curation", False):
            rules["curation"] = True

    return (all_names, rules)


def download_data(transfer_client, source_loc, local_ep, local_path,
                  admin_client=None, user_id=None):
    """Download data from a remote host to the configured machine.
    (Many sources to one destination)

    Arguments:
    transfer_client (TransferClient): An authenticated TransferClient with access to the data.
                                      Technically unnecessary for non-Globus data locations.
    source_loc (list of str): The location(s) of the data.
    local_ep (str): The local machine's endpoint ID.
    local_path (str): The path to the local storage location.
    admin_client (TransferClient): An authenticated TransferClient with Access Manager
                                   permissions on the local endpoint/GDrive.
                                   Optional if permission changes are not needed.
    user_id (str): The ID of the identity authenticated to the transfer_client.
                   Used for permission changes. Optional if permission changes are not needed.

    Returns:
    dict: success (bool): True on success, False on failure.
    """
    # admin_client and user_id must both be supplied if one is supplied
    # Effectively if (admin_client XOR user_id)
    if ((admin_client is not None or user_id is not None)
            and not (admin_client is not None and user_id is not None)):
        raise ValueError("admin_client and user_id must both be supplied if one is supplied")
    filename = None
    # If the local_path is a file and not a directory, use the directory
    if local_path[-1] != "/":
        # Save the filename for later
        filename = os.path.basename(local_path)
        local_path = os.path.dirname(local_path) + "/"

    os.makedirs(local_path, exist_ok=True)
    if not isinstance(source_loc, list):
        source_loc = [source_loc]

    # Download data locally
    for raw_loc in source_loc:
        location = normalize_globus_uri(raw_loc)
        loc_info = urllib.parse.urlparse(location)
        # Globus Transfer
        if loc_info.scheme == "globus":
            # Use admin_client for GDrive Transfers
            # User doesn't need permissions on MDF GDrive, we have those
            # For all other cases use user's TC
            tc = admin_client if (loc_info.netloc == CONFIG["GDRIVE_EP"]
                                  and admin_client is not None) else transfer_client
            if filename:
                transfer_path = os.path.join(local_path, filename)
            else:
                transfer_path = local_path
            # Check that data not already in place
            if (loc_info.netloc != local_ep
                    and loc_info.path != transfer_path):
                try:
                    if admin_client is not None:
                        # Edit ACL to allow pull
                        acl_rule = {
                            "DATA_TYPE": "access",
                            "principal_type": "identity",
                            "principal": user_id,
                            "path": local_path,
                            "permissions": "rw"
                        }
                        try:
                            acl_res = admin_client.add_endpoint_acl_rule(local_ep, acl_rule).data
                        except Exception as e:
                            logger.error("ACL rule creation exception for '{}': {}"
                                         .format(acl_rule, repr(e)))
                            raise ValueError("Internal permissions error.")
                        if not acl_res.get("code") == "Created":
                            logger.error("Unable to create ACL rule '{}': {}"
                                         .format(acl_rule, acl_res))
                            raise ValueError("Internal permissions error.")
                    else:
                        acl_res = None

                    # Transfer locally
                    transfer = mdf_toolbox.custom_transfer(
                                    tc, loc_info.netloc, local_ep,
                                    [(loc_info.path, transfer_path)],
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
                        logger.error("Transfer failed: {}".format(event))
                        raise ValueError(event)
                finally:
                    if acl_res is not None:
                        try:
                            acl_del = admin_client.delete_endpoint_acl_rule(
                                                        local_ep, acl_res["access_id"])
                        except Exception as e:
                            logger.critical("ACL rule deletion exception for '{}': {}"
                                            .format(acl_res, repr(e)))
                            raise ValueError("Internal permissions error.")
                        if not acl_del.get("code") == "Deleted":
                            logger.critical("Unable to delete ACL rule '{}': {}"
                                            .format(acl_res, acl_del))
                            raise ValueError("Internal permissions error.")
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


def backup_data(transfer_client, storage_loc, backup_locs, acl=None):
    """Back up data to remote endpoints.
    (One source to many destinations)

    Note:
        An endpoint of "False" will disable the backup for that location, or all
        backups if the storage endpoint is "False". When disabled in this way,
        the results will return a success and the event will be logged.

    Arguments:
    transfer_client (TransferClient): An authenticated TransferClient with access to the data.
    storage_loc (str): A globus:// uri to the current data location.
    backup_locs (list of str): The backup locations.
    acl (list of str): The ACL to set on the backup location. Default None, to not set ACL.

    Warning: ACL setting not supported for non-directory Transfers. Globus Transfer cannot
            set ACLs on individual files, only on directories.

    Returns:
    dict: [backup_loc] (dict)
            success (bool): True on a successful backup to this backup location,
                    False otherwise.
            error (str): The error encountered. May be empty, if no errors were encountered.
                    Must have some value if success is False.

    Example return value (transfer to EP abc123):
    {
        "abc123": {
            "success": True,
            "error": "Unable to set ACL on endpoint 'abc123'"
        }
    }
    """
    if isinstance(backup_locs, str):
        backup_locs = [backup_locs]
    if isinstance(acl, str):
        acl = [acl]
    # "public" permission allows any other identities anyway
    if acl is not None and "public" in acl:
        acl = ["public"]
    results = {}
    norm_store = normalize_globus_uri(storage_loc)
    storage_info = urllib.parse.urlparse(norm_store)

    # Storage must be Globus endpoint
    if not storage_info.scheme == "globus":
        error = ("Storage location '{}' (from '{}') is not a Globus Endpoint and cannot be "
                 "directly published from or backed up from".format(norm_store, storage_loc))
        return {
            "all_locations": {
                "success": False,
                "error": error
            }
        }
    # No backups if storage EP is False
    elif storage_info.netloc == "False":
        logger.warning("All backups skipped from storage: '{}'".format(norm_store))
        for backup in backup_locs:
            results[backup] = {
                "success": True,
                "error": "All backups skipped from storage: '{}'".format(norm_store)
            }
        return results

    for backup in backup_locs:
        error = ""
        norm_backup = normalize_globus_uri(backup)
        backup_info = urllib.parse.urlparse(norm_backup)
        # No backup if location EP is False
        if backup_info.netloc == "False":
            logger.warning("Backup location skipped: '{}'".format(norm_backup))
            results[backup] = {
                "success": True,
                "error": "Backup location skipped: '{}'".format(norm_backup)
            }
            continue
        # Set backup ACL if requested
        # Warn in log if impossible to set backup
        # TODO: Better handle file-level ACL rejection
        if acl is not None and not backup_info.path.endswith("/"):
            logger.warning("Backup path {} is a file; cannot set ACL {}"
                           .format(backup_info.path, acl))
        elif acl is not None:
            acl_res = []
            for identity in acl:
                # Set ACL appropriately for request
                if identity == "public":
                    acl_rules = [{
                        "DATA_TYPE": "access",
                        "principal_type": "anonymous",
                        "principal": "",
                        "path": backup_info.path,
                        "permissions": "r"
                    }]
                else:
                    # If URN provided, we only need one rule
                    if identity.startswith("urn:"):
                        if identity.startswith("urn:globus:auth:identity:"):
                            principal = "identity"
                        elif identity.startswith("urn:globus:groups:id:"):
                            principal = "group"
                        else:
                            # TODO: How to handle unknown URN?
                            principal = "identity"
                        acl_rules = [{
                            "DATA_TYPE": "access",
                            "principal_type": principal,
                            "principal": identity.split(":")[-1],
                            "path": backup_info.path,
                            "permissions": "r"
                        }]
                    else:
                        # TODO: Is it possible to determine a bare Globus UUID's type?
                        #       Generalize and add to Toolbox if so.
                        #       Assume for now it isn't possible; add both Group and Identity
                        acl_rules = [{
                            "DATA_TYPE": "access",
                            "principal_type": "identity",
                            "principal": identity,
                            "path": backup_info.path,
                            "permissions": "r"
                        }, {
                            "DATA_TYPE": "access",
                            "principal_type": "group",
                            "principal": identity,
                            "path": backup_info.path,
                            "permissions": "r"
                        }]
                for rule in acl_rules:
                    try:
                        res = transfer_client.add_endpoint_acl_rule(backup_info.netloc, rule).data
                        acl_res.append(res)
                    except Exception as e:
                        # Only stores last error, all errors here should be about the same
                        error = ("Unable to set ACL on endpoint '{}': {}"
                                 .format(backup_info.netloc, str(e)))
                    else:
                        if not res.get("code") == "Created":
                            error = ("Unable to set ACL on endpoint '{}': {}"
                                     .format(backup_info.netloc, res.get("code")))
        else:
            acl_res = None

        transfer = mdf_toolbox.custom_transfer(
                        transfer_client, storage_info.netloc, backup_info.netloc,
                        [(storage_info.path, backup_info.path)],
                        interval=CONFIG["TRANSFER_PING_INTERVAL"],
                        inactivity_time=CONFIG["TRANSFER_DEADLINE"], notify=False)
        for event in transfer:
            if not event["success"]:
                logger.debug(event)

        if not event["success"]:
            # Remove ACL, if set, because transfer failed
            if acl_res is not None:
                for acl_set in acl_res:
                    try:
                        acl_del = transfer_client.delete_endpoint_acl_rule(
                                                    backup_info.netloc, acl_set["access_id"])
                        if not acl_del.get("code") == "Deleted":
                            raise ValueError("ACL rule not deleted: '{}'"
                                             .format(acl_del.get("code")))
                    # Deletion failure not showstopper here
                    # Worst-case, invalid path has ACL set on destination EP
                    except Exception:
                        # If ACL creation failed, deletion failure is expected
                        # But if there was not an error, notify user about deletion error
                        if not error:
                            error = "Failed to delete ACL after failed Transfer"
            # If previous non-fatal error occurred, add after primary error message
            # Ex. "Code X: Permission denied, additionally failed to delete ACL"
            if error:
                error = "{}: {}\n{}".format(event.get("code", "No code found"),
                                            event.get("description", "No description found"),
                                            error)
            else:
                error = "{}: {}\n{}".format(event.get("code", "No code found"),
                                            event.get("description", "No description found"))

        results[backup] = {
            "success": event["success"],
            "error": error
        }

    return results


def normalize_globus_uri(location):
    """Normalize a Globus Web App link or Google Drive URI into a globus:// URI.
    For Google Drive URIs, the file(s) must be shared with
    materialsdatafacility@gmail.com.
    If the URI is not a Globus Web App link or Google Drive URI,
    it is returned unchanged.

    Arguments:
        location (str): One URI to normalize.

    Returns:
        str: The normalized URI, or the original URI if no normalization was possible.
    """
    loc_info = urllib.parse.urlparse(location)
    # Globus Web App link into globus:// form
    if any([re.search(pattern, location) for pattern in CONFIG["GLOBUS_LINK_FORMS"]]):
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
        new_location = "globus://{}{}".format(ep_id, path)

    # Google Drive protocol into globus:// form
    elif loc_info.scheme in ["gdrive", "google", "googledrive"]:
        # Correct form is "google:///path/file.dat"
        # (three slashes - two for scheme end, one for path start)
        # But if a user uses two slashes, the netloc will incorrectly be the top dir
        # (netloc="path", path="/file.dat")
        # Otherwise netloc is nothing (which is correct)
        if loc_info.netloc:
            gpath = "/" + loc_info.netloc + loc_info.path
        else:
            gpath = loc_info.path
        # Don't use os.path.join because gpath starts with /
        # GDRIVE_ROOT does not end in / to make compatible
        new_location = "globus://{}{}{}".format(CONFIG["GDRIVE_EP"], CONFIG["GDRIVE_ROOT"], gpath)

    # Default - do nothing
    else:
        new_location = location

    return new_location


def make_globus_app_link(globus_uri):
    globus_uri_info = urllib.parse.urlparse(normalize_globus_uri(globus_uri))
    globus_link = CONFIG["TRANSFER_WEB_APP_LINK"] \
        .format(globus_uri_info.netloc, urllib.parse.quote(globus_uri_info.path))
    return globus_link


def lookup_http_host(globus_uri):
    globus_uri_info = urllib.parse.urlparse(normalize_globus_uri(str(globus_uri)))
    return CONFIG["GLOBUS_HTTP_HOSTS"].get(globus_uri_info.netloc or globus_uri_info.path, None)


def get_dc_creds(test):
    if test:
        return CONFIG["DATACITE_CREDS"]["TEST"]
    else:
        return CONFIG["DATACITE_CREDS"]["NONTEST"]


def make_dc_doi(test):
    creds = get_dc_creds(test)
    doi_unique = False
    while not doi_unique:
        # Create new DOI by appending random characters to prefix
        new_doi = creds["DC_PREFIX"]
        for i in range(CONFIG["NUM_DOI_SECTIONS"]):
            new_doi += "".join(random.choices(string.ascii_lowercase + string.digits,
                                              k=CONFIG["NUM_DOI_CHARS"]))
            new_doi += "-"
        new_doi = new_doi.strip("-")

        # Check that new_doi is unique, not used previously
        # NOTE: Technically there is a non-zero chance that two identical IDs are generated
        #       before either submit to DataCite.
        #       However, the probability is low enough that we do not mitigate this
        #       condition. Should it occur, the later submission will fail.
        doi_fetch = requests.get(creds["DC_URL"]+new_doi)
        if doi_fetch.status_code == 404:
            doi_unique = True
    return new_doi


def translate_dc_schema(dc_md, doi=None, url=None):
    """Translate Datacite Schema to Datacite DOI Schema (slightly different)."""
    doi_data = deepcopy(dc_md)

    # url
    if url:
        doi_data["url"] = url

    # identifiers
    if doi_data.get("identifier"):
        doi_data["doi"] = doi_data["identifier"]["identifier"]
        doi_data["identifiers"] = [doi_data.pop("identifier")]
    elif doi:
        doi_data["doi"] = doi
        doi_data["identifiers"] = [{
            "identifier": doi,
            "identifierType": "DOI"
        }]

    # creators
    if doi_data.get("creators"):
        new_creators = []
        for creator in doi_data["creators"]:
            if creator.get("creatorName"):
                creator["name"] = creator.pop("creatorName")
            if creator.get("affiliations"):
                creator["affiliation"] = creator.pop("affiliations")
            new_creators.append(creator)
        doi_data["creators"] = new_creators

    # contributors
    if doi_data.get("contributors"):
        new_contributors = []
        for contributor in doi_data["contributors"]:
            if contributor.get("contributorName"):
                contributor["name"] = contributor.pop("contributorName")
            if contributor.get("affiliations"):
                contributor["affiliation"] = contributor.pop("affiliations")
            new_contributors.append(contributor)
        doi_data["contributors"] = new_contributors

    # types
    if doi_data.get("resourceType"):
        doi_data["types"] = doi_data.pop("resourceType")

    # alternateIdentifiers (does not exist)
    if doi_data.get("alternateIdentifiers"):
        doi_data.pop("alternateIdentifiers")

    doi_data["event"] = "publish"
    doi_md = {
        "data": {
            "type": "dois",
            "attributes": doi_data
        }
    }

    return doi_md


def datacite_mint_doi(dc_md, test, url=None, doi=None):
    if not doi and not dc_md.get("identifier") and not dc_md.get("identifiers"):
        doi = make_dc_doi(test)

    doi_md = translate_dc_schema(dc_md, doi=doi, url=url)
    creds = get_dc_creds(test)
    res = requests.post(creds["DC_URL"], auth=(creds["DC_USERNAME"], creds["DC_PASSWORD"]),
                        json=doi_md)
    try:
        res_json = res.json()
    except json.JSONDecodeError:
        return {
            "success": False,
            "error": "DOI minting failed",
            "details": res.content
        }

    if res.status_code >= 300:
        return {
            "success": False,
            "error": "; ".join([err["title"] for err in res_json["errors"]])
        }
    else:
        return {
            "success": True,
            # "datacite_full": res_json,
            # "dataset": doi_md,
            "datacite": res_json["data"]
        }


def datacite_update_doi(doi, updates, test, url=None):
    update_md = translate_dc_schema(updates, doi=doi, url=url)
    creds = get_dc_creds(test)
    res = requests.put(creds["DC_URL"]+doi, auth=(creds["DC_USERNAME"], creds["DC_PASSWORD"]),
                       json=update_md)
    try:
        res_json = res.json()
    except json.JSONDecodeError:
        return {
            "success": False,
            "error": "DOI update failed",
            "details": res.content
        }

    if res.status_code >= 300:
        return {
            "success": False,
            "error": "; ".join([err["title"] for err in res_json["errors"]])
        }
    else:
        return {
            "success": True,
            "datacite": res_json["data"]
        }


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
    stat_res = read_table("status", source_id)
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
            while read_table("status", source_id)["status"]["active"]:
                os.kill(current_status["pid"], 0)  # Triggers ProcessLookupError on failure
                logger.info("Waiting for submission {} (PID {}) to cancel".format(
                                                                            source_id,
                                                                            current_status["pid"]))
                time.sleep(CONFIG["CANCEL_WAIT_TIME"])
        except ProcessLookupError:
            # Process is dead
            complete_submission(source_id)

    # Change status code to reflect cancellation
    old_status_code = read_table("status", source_id)["status"]["code"]
    new_status_code = old_status_code.replace("z", "X").replace("W", "X") \
                                     .replace("T", "X").replace("P", "X")
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
    if not read_table("status", source_id).get("status", {}).get("active", False):
        return {
            "success": False,
            "error": "Submission not in progress"
        }
    logger.debug("{}: Starting cleanup".format(source_id))
    # Remove dirs containing processed data, if requested
    if cleanup:
        cleanup_paths = [
            os.path.join(CONFIG["LOCAL_PATH"], source_id) + "/",
            os.path.join(CONFIG["SERVICE_DATA"], source_id) + "/",
            os.path.join(CONFIG["CURATION_DATA"], source_id) + ".json"
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
    # Delete curation entry if exists
    delete_from_table("curation", source_id)
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


def validate_status(status, new_status=False):
    """Validate a submission status.

    Arguments:
    status (dict): The status to validate.
    new_status (bool): Is this status a new status?

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
        if new_status:
            # Nothing started or finished
            assert code == "z" * len(code)
    except AssertionError:
        return {
            "success": False,
            "error": ("Invalid status code '{}' for {} status"
                      .format(code, "new" if new_status else "old"))
        }
    else:
        return {
            "success": True
        }


def read_table(table_name, source_id):
    tbl_res = get_dmo_table(table_name)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]

    entry = table.get_item(Key={"source_id": source_id}, ConsistentRead=True).get("Item")
    if not entry:
        return {
            "success": False,
            "error": "ID {} not found in {} database".format(source_id, table_name)
            }
    return {
        "success": True,
        "status": entry
        }


def scan_table(table_name, fields=None, filters=None):
    """Scan the status or curation databases..

    Arguments:
    table_name (str): The Dynamo table to scan.
    fields (list of str): The fields from the results to return.
                          Default None, to return all fields.
    filters (list of tuples): The filters to apply. Format: (field, operator, value)
                              For an entry to be returned, all filters must match.
                              Default None, to return all entries.
                           field: The status field to filter on.
                           operator: The relation of field to value. Valid operators:
                                     ^: Begins with
                                     *: Contains
                                     ==: Equal to (or field does not exist, if value is None)
                                     !=: Not equal to (or field exists, if value is None)
                                     >: Greater than
                                     >=: Greater than or equal to
                                     <: Less than
                                     <=: Less than or equal to
                                     []: Between, inclusive (requires a list of two values)
                                     in: Is one of the values (requires a list of values)
                                         This operator effectively allows OR-ing '=='
                           value: The value of the field.

    Returns:
    dict: The results of the scan.
        success (bool): True on success, False otherwise.
        results (list of dict): The status entries returned.
        error (str): If success is False, the error that occurred.
    """
    # Get Dynamo status table
    tbl_res = get_dmo_table(table_name)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]

    # Translate fields
    if isinstance(fields, str) or fields is None:
        proj_exp = fields
    elif isinstance(fields, list):
        proj_exp = ",".join(fields)
    else:
        return {
            "success": False,
            "error": "Invalid fields type {}: '{}'".format(type(fields), fields)
        }

    # Translate filters
    # 0 = field
    # 1 = operator
    # 2 = value
    if isinstance(filters, tuple):
        filters = [filters]
    if filters is None or (isinstance(filters, list) and len(filters) == 0):
        filter_exps = None
    elif isinstance(filters, list):
        filter_exps = []
        for fil in filters:
            # Begins with
            if fil[1] == "^":
                filter_exps.append(Attr(fil[0]).begins_with(fil[2]))
            # Contains
            elif fil[1] == "*":
                filter_exps.append(Attr(fil[0]).contains(fil[2]))
            # Equal to (or field does not exist, if value is None)
            elif fil[1] == "==":
                if fil[2] is None:
                    filter_exps.append(Attr(fil[0]).not_exists())
                else:
                    filter_exps.append(Attr(fil[0]).eq(fil[2]))
            # Not equal to (or field exists, if value is None)
            elif fil[1] == "!=":
                if fil[2] is None:
                    filter_exps.append(Attr(fil[0]).exists())
                else:
                    filter_exps.append(Attr(fil[0]).ne(fil[2]))
            # Greater than
            elif fil[1] == ">":
                filter_exps.append(Attr(fil[0]).gt(fil[2]))
            # Greater than or equal to
            elif fil[1] == ">=":
                filter_exps.append(Attr(fil[0]).gte(fil[2]))
            # Less than
            elif fil[1] == "<":
                filter_exps.append(Attr(fil[0]).lt(fil[2]))
            # Less than or equal to
            elif fil[1] == "<=":
                filter_exps.append(Attr(fil[0]).lte(fil[2]))
            # Between, inclusive (requires a list of two values)
            elif fil[1] == "[]":
                if not isinstance(fil[2], list) or len(fil[2]) != 2:
                    return {
                        "success": False,
                        "error": "Invalid between ('[]') operator values: '{}'".format(fil[2])
                    }
                filter_exps.append(Attr(fil[0]).between(fil[2][0], fil[2][1]))
            # Is one of the values (requires a list of values)
            elif fil[1] == "in":
                if not isinstance(fil[2], list):
                    return {
                        "success": False,
                        "error": "Invalid 'in' operator values: '{}'".format(fil[2])
                    }
                filter_exps.append(Attr(fil[0]).is_in(fil[2]))
            else:
                return {
                    "success": False,
                    "error": "Invalid filter operator '{}'".format(fil[1])
                }
    else:
        return {
            "success": False,
            "error": "Invalid filters type {}: '{}'".format(type(filters), filters)
        }

    # Make scan arguments
    scan_args = {
        "ConsistentRead": True
    }
    if proj_exp is not None:
        scan_args["ProjectionExpression"] = proj_exp
    if filter_exps is not None:
        # Create valid FilterExpression
        # Each Attr must be combined with &
        filter_expression = filter_exps[0]
        for i in range(1, len(filter_exps)):
            filter_expression = filter_expression & filter_exps[i]
        scan_args["FilterExpression"] = filter_expression

    # Make scan call, paging through if too many entries are scanned
    result_entries = []
    while True:
        scan_res = table.scan(**scan_args)
        # Check for success
        if scan_res["ResponseMetadata"]["HTTPStatusCode"] >= 300:
            return {
                "success": False,
                "error": ("HTTP code {} returned: {}"
                          .format(scan_res["ResponseMetadata"]["HTTPStatusCode"],
                                  scan_res["ResponseMetadata"]))
            }
        # Add results to list
        result_entries.extend(scan_res["Items"])
        # Check for completeness
        # If LastEvaluatedKey exists, need to page through more results
        if scan_res.get("LastEvaluatedKey", None) is not None:
            scan_args["ExclusiveStartKey"] = scan_res["LastEvaluatedKey"]
        # Otherwise, all results retrieved
        else:
            break

    return {
        "success": True,
        "results": result_entries
    }


def create_status(status):
    tbl_res = get_dmo_table("status")
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]

    # Add defaults
    status["messages"] = ["No message available"] * len(STATUS_STEPS)
    status["active"] = True
    status["cancelled"] = False
    status["pid"] = os.getpid()
    status["extensions"] = []
    status["hibernating"] = False
    status["code"] = "z" * len(STATUS_STEPS)

    status_valid = validate_status(status, new_status=True)
    if not status_valid["success"]:
        return status_valid

    # Check that status does not already exist
    if read_table("status", status["source_id"])["success"]:
        return {
            "success": False,
            "error": "ID {} already exists in database".format(status["source_id"])
            }
    try:
        table.put_item(Item=status, ConditionExpression=Attr("source_id").not_exists())
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
    # Clean text and link (if present)
    if text:
        # This could be done with a complex regex and replace, but .replace() is simpler
        # and more robust - r'\\\\' only catches multiples of two backslashes,
        # while r'\\' catches nothing, according to basic testing.
        # So replacing all the reasonable escape sequences with spaces, deleting all backslashes,
        # and condensing the spaces is sufficient.
        # Removing newlines is okay in this particular case (simple status messages).
        text = text.replace("\\n", " ").replace("\\t", " ").replace("\\r", " ").replace("\\", "")
        while "  " in text:
            text = text.replace("  ", " ")
    if link:
        link = urllib.parse.quote(link, safe="/:")

    # Get status table
    tbl_res = get_dmo_table("status")
    if not tbl_res["success"]:
        if except_on_fail:
            raise ValueError(tbl_res["error"])
        return tbl_res
    table = tbl_res["table"]
    # Get old status
    old_status = read_table("status", source_id)
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
        # TODO: Since deprecating /ingest, is the following still true?
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
    tbl_res = get_dmo_table("status")
    if not tbl_res["success"]:
        if except_on_fail:
            raise ValueError(tbl_res["error"])
        return tbl_res
    table = tbl_res["table"]
    # Get old status
    old_status = read_table("status", source_id)
    if not old_status["success"]:
        if except_on_fail:
            raise ValueError(old_status["error"])
        return old_status
    status = old_status["status"]

    # Overwrite old status
    status = mdf_toolbox.dict_merge(modifications, status)

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
    # code: str, based on char position
    # messages: list of str, in order generated
    # errors: list of str, in order of failures
    # title: str,
    # submitter: str,
    # submission_time: str
    # }
    full_code = list(status["code"])
    messages = status["messages"]
    steps = [st[1] for st in STATUS_STEPS]

    usr_msg = ("Status of {}submission {} ({})\n"
               "Submitted by {} at {}\n\n").format("TEST " if status["test"] else "",
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


def create_curation_task(task):
    tbl_res = get_dmo_table("curation")
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]

    # Check that task does not already exist
    if read_table("curation", task["source_id"])["success"]:
        return {
            "success": False,
            "error": "ID {} already exists in database".format(task["source_id"])
        }
    try:
        table.put_item(Item=task, ConditionExpression=Attr("source_id").not_exists())
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
        }
    else:
        logger.info("Curation task for {}: Created".format(task["source_id"]))
        return {
            "success": True,
            "curation_task": task
        }


def delete_from_table(table_name, source_id):
    tbl_res = get_dmo_table(table_name)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]

    # Check that entry exists
    if not read_table(table_name, source_id)["success"]:
        return {
            "success": False,
            "error": "ID {} does not exist in database".format(source_id)
        }
    try:
        table.delete_item(Key={"source_id": source_id})
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
        }

    # Verify entry deleted
    if read_table(table_name, source_id)["success"]:
        return {
            "success": False,
            "error": "Entry not deleted from database"
        }

    return {
        "success": True
    }


def initialize_dmo_table(table_name, client=DMO_CLIENT):
    try:
        table_key = DMO_TABLES[table_name]
    except KeyError:
        return {
            "success": False,
            "error": "Invalid table '{}'".format(table_name)
        }
    schema = deepcopy(DMO_SCHEMA)
    schema["TableName"] = table_key

    tbl_res = get_dmo_table(table_name, client)
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

    tbl_res2 = get_dmo_table(table_name, client)
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


def get_dmo_table(table_name, client=DMO_CLIENT):
    try:
        table_key = DMO_TABLES[table_name]
    except KeyError:
        return {
            "success": False,
            "error": "Invalid table '{}'".format(table_name)
        }
    try:
        table = client.Table(table_key)
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
