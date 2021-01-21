import json
import os
from copy import deepcopy

import globus_sdk
import mdf_toolbox
import boto3
import re
import logging
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger(__name__)

CONFIG = {
    "INGEST_URL": "https://dev-api.materialsdatafacility.org/ingest",
    "INGEST_INDEX": "mdf-dev",
    "INGEST_TEST_INDEX": "mdf-dev",
    "DYNAMO_STATUS_TABLE": "dev-status-alpha-2",
    "DYNAMO_CURATION_TABLE": "dev-curation-alpha-1"
}

DMO_CLIENT = boto3.resource('dynamodb', region_name="us-east-1")
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
                        "error": "Invalid between ('[]') operator values: '{}'".format(
                            fil[2])
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


def get_secret():
    secret_name = "Globus"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()

    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    return eval(get_secret_value_response['SecretString'])


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
    globus_secrets = get_secret()
    search_client = mdf_toolbox.confidential_login(services=['search'],
                                                   client_id=globus_secrets[
                                                       'API_CLIENT_ID'],
                                                   client_secret=globus_secrets[
                                                       'API_CLIENT_SECRET'])['search']

    old_q = {
        "q": "mdf.source_name:{} AND mdf.resource_type:dataset".format(source_name),
        "advanced": True,
        "limit": 2,  # Should only ever be one, if two are returned there's a problem
        "offset": 0
    }
    old_search = mdf_toolbox.gmeta_pop(search_client.post_search(
        mdf_toolbox.translate_index(index), old_q))

    print("Search for the old ", old_search)
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

    print("Scan res", scan_res)
    user_ids = set([sub["user_id"] for sub in scan_res["results"]])
    # Get most recent previous source_id and info
    old_search_version = 0
    old_sub_version = 0
    for old_sid in scan_res["results"]:
        old_sid_info = split_source_id(old_sid["source_id"])
        # If found more recent Search version, save both Search and sub versions
        # (sub version resets on new Search version)
        if old_sid_info["search_version"] > old_search_version:
            old_search_version = old_sid_info["search_version"]
            old_sub_version = old_sid_info["submission_version"]
        # If found more recent sub version, just save sub version
        # Search version must be the same, though
        elif (old_sid_info["search_version"] == old_search_version
              and old_sid_info["submission_version"] > old_sub_version):
            old_sub_version = old_sid_info["submission_version"]

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
        globus_secrets = get_secret()
        auth_client = globus_sdk.ConfidentialAppAuthClient(globus_secrets['API_CLIENT_ID'],
                                                           globus_secrets['API_CLIENT_SECRET'])
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

    try:
        nexus = mdf_toolbox.confidential_login(services=['groups'],
                                                       client_id=globus_secrets[
                                                           'API_CLIENT_ID'],
                                                       client_secret=globus_secrets[
                                                           'API_CLIENT_SECRET'])['groups']

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
    schema_path = "./schemas/connect_aux_data"
    with open(os.path.join(schema_path, "organizations.json")) as organization_file:
        organizations = json.load(organization_file)

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
