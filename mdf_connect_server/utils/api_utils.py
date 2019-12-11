from copy import deepcopy
import json
import logging
import os
import re
import urllib

import boto3
from boto3.dynamodb.conditions import Attr
import globus_sdk
import jsonschema
import mdf_toolbox

from mdf_connect_server import CONFIG
# TODO (XTH): Remove old imports (will be deprecated, also causes F401)
from mdf_connect_server.utils import (create_status, submit_to_queue,  # noqa: F401
                                      translate_status, update_status, validate_status)


logger = logging.getLogger(__name__)

# DynamoDB setup
DMO_CLIENT = boto3.resource('dynamodb',
                            aws_access_key_id=CONFIG["AWS_KEY"],
                            aws_secret_access_key=CONFIG["AWS_SECRET"],
                            region_name="us-east-1")
DMO_TABLES = {
    # TODO (XTH): Sub log table, delete status and curation tables
    # "sub_log": CONFIG["SUB_LOG_TABLE"]
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
            if grp.lower() == "convert":
                grp = CONFIG["CONVERT_GROUP_ID"]
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
    # TODO (XTH)
    raise NotImplementedError


def complete_submission(source_id, cleanup=CONFIG["DEFAULT_CLEANUP"]):
    # TODO (XTH): Modify for API-only
    raise NotImplementedError
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
                    # NOTE: local_admin_delete not in api_utils
                    clean_res = None  # local_admin_delete(cleanup)
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
    """


def create_sub_log(sub_log):
    """Create a submission log entry in the submission database."""
    # TODO (XTH)
    raise NotImplementedError


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
    # Transform into tuple (normalized_aliases, org_rules) for convenience
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


def make_globus_app_link(globus_uri):
    globus_uri_info = urllib.parse.urlparse(normalize_globus_uri(globus_uri))
    globus_link = CONFIG["TRANSFER_WEB_APP_LINK"] \
        .format(globus_uri_info.netloc, urllib.parse.quote(globus_uri_info.path))
    return globus_link


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


def modify_sub_entry(source_id, modifications):
    """Modify a submission log entry."""
    # TODO: XTH
    raise NotImplementedError


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


def translate_automate_status(status):
    """Translate Automate's status message into an MDF status message."""
    # TODO (XTH)
    raise NotImplementedError


def validate_sub_log(entry):
    """Validate a submission log entry.

    Arguments:
        entry (dict): The entry to validate.

    Returns:
        dict:
            success (bool): True if the status is valid, False otherwise.
            error (str): When the status is not valid, the reason why.
            details (str): Optional verbose details about an error.
    """
    # TODO (XTH): Sub log schema
    raise NotImplementedError
    # Load schema
    with open(os.path.join(CONFIG["SCHEMA_PATH"], "internal_sub_log.json")) as schema_file:
        schema = json.load(schema_file)
    resolver = jsonschema.RefResolver(base_uri="file://{}/".format(CONFIG["SCHEMA_PATH"]),
                                      referrer=schema)
    # Validate entry
    try:
        jsonschema.validate(entry, schema, resolver=resolver)
    except jsonschema.ValidationError as e:
        return {
            "success": False,
            "error": "Invalid submission log entry: {}".format(str(e).split("\n")[0]),
            "details": str(e)
        }
    else:
        return {
            "success": True,
            "error": None,
            "details": None
        }
