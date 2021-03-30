import json
import logging
import os
import urllib
from copy import deepcopy
from datetime import datetime

import jsonschema
import mdf_toolbox

import utils
from dynamo_manager import DynamoManager
from automate_manager import AutomateManager
from organization import Organization
from source_id_manager import SourceIDManager
from utils import get_secret

logger = logging.getLogger(__name__)


class ClientException(Exception):
    pass


CONFIG = {
    "ADMIN_GROUP_ID": "5fc63928-3752-11e8-9c6f-0e00fd09bf20",
    "EXTRACT_GROUP_ID": "cc192dca-3751-11e8-90c1-0a7c735d220a",
    "API_SCOPE": "https://auth.globus.org/scopes/c17f27bb-f200-486a-b785-2a25e82af505/connect",
    "API_SCOPE_ID": "mdf_dataset_submission",
    "BACKUP_EP": False,
    "BACKUP_PATH": "/mdf_connect/dev/data/",
    "DEFAULT_DOI_TEST": True,
    "DEFAULT_CITRINATION_PUBLIC": False,
    "DEFAULT_MRR_TEST": True,
    # Regexes for detecting Globus Web App links
    "GLOBUS_LINK_FORMS": [
        "^https:\/\/www\.globus\.org\/app\/transfer",
        # noqa: W605 (invalid escape char '\/')
        "^https:\/\/app\.globus\.org\/file-manager",  # noqa: W605
        "^https:\/\/app\.globus\.org\/transfer",  # noqa: W605
        "^https:\/\/.*globus.*(?=.*origin_id)(?=.*origin_path)",  # noqa: W605
        "^https:\/\/.*globus.*(?=.*destination_id)(?=.*destination_path)"  # noqa: W605
    ],

    # Using Prod-P GDrive EP because having two GDrive EPs on one account seems to fail
    "GDRIVE_EP": "f00dfd6c-edf4-4c8b-a4b1-be6ad92a4fbb",
    "GDRIVE_ROOT": "/Shared With Me",

    "TRANSFER_WEB_APP_LINK": "https://app.globus.org/file-manager?origin_id={}&origin_path={}",
    "INGEST_URL": "https://dev-api.materialsdatafacility.org/ingest",
    "INGEST_INDEX": "mdf-dev",
    "INGEST_TEST_INDEX": "mdf-dev",
    "DYNAMO_STATUS_TABLE": "dev-status-alpha-2",
    "DYNAMO_CURATION_TABLE": "dev-curation-alpha-1"
}


def validate_submission_schema(metadata):
    schema_path = "./schemas/schemas"
    with open(os.path.join(schema_path, "connect_submission.json")) as schema_file:
        schema = json.load(schema_file)
        resolver = jsonschema.RefResolver(base_uri="file://{}/{}/".format(os.getcwd(),
                                                                          schema_path),
                                          referrer=schema)
        try:
            jsonschema.validate(metadata, schema, resolver=resolver)
            return None
        except jsonschema.ValidationError as e:
            return {
                'statusCode': 400,
                'body': json.dumps(
                    {
                        "success": False,
                        "error": "Invalid submission: " + str(e).split("\n")[0]
                    })
            }


def lambda_handler(event, context):
    print(json.dumps(event))
    name = event['requestContext']['authorizer']['name']
    identities = eval(event['requestContext']['authorizer']['identities'])
    user_id = event['requestContext']['authorizer']['user_id']
    user_email = event['requestContext']['authorizer']['principalId']

    depends = event['requestContext']['authorizer']['globus_dependent_token'].replace(
        'null', 'None')
    globus_dependent_token = eval(depends)
    print("name ", name, "identities", identities)
    print("globus_dependent_token ", globus_dependent_token)
    access_token = event['headers']['Authorization']

    dynamo_manager = DynamoManager(CONFIG)
    sourceid_manager = SourceIDManager(dynamo_manager, CONFIG)

    try:
        metadata = json.loads(event['body'], )
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": "Submission must be valid JSON"
                })
        }

    md_copy = deepcopy(metadata)

    if not metadata:
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": "POST data empty or not JSON"
                })
        }

    # NaN, Infinity, and -Infinity cause issues in Search, and have no use in MDF
    try:
        json.dumps(metadata, allow_nan=False)
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": "Submission may not contain NaN or Infinity"
                })
        }

    # If this is an incremental update, fetch the original submission
    if metadata.get("incremental_update"):
        # source_name and title cannot be updated
        metadata.get("mdf", {}).pop("source_name", None)
        metadata.get("dc", {}).pop("titles", None)

        # update must be True
        if not metadata.get("update"):
            raise ClientException(
                "{\"failure\":true, \"errorMessage\": \"You must be updating a submission (set update=True) when incrementally updating\"")
        # @TODO
        # Lookup previous submission in Dynamo

    # Validate input JSON
    # resourceType is always going to be Dataset, don't require from user
    if not metadata.get("dc") or not isinstance(metadata["dc"], dict):
        metadata["dc"] = {}
    if not metadata["dc"].get("resourceType"):
        try:
            metadata["dc"]["resourceType"] = {
                "resourceTypeGeneral": "Dataset",
                "resourceType": "Dataset"
            }
        except Exception:
            pass

    # Move tags to dc.subjects
    if metadata.get("tags"):
        tags = metadata.pop("tags", [])
        if not isinstance(tags, list):
            tags = [tags]
        if not metadata["dc"].get("subjects"):
            metadata["dc"]["subjects"] = []
        for tag in tags:
            metadata["dc"]["subjects"].append({
                "subject": tag
            })

    validate_err = validate_submission_schema(metadata)
    if validate_err:
        print("---->", validate_err)
        return validate_err

    # Pull out configuration fields from metadata into sub_conf, set defaults where appropriate
    sub_conf = {
        "data_sources": metadata.pop("data_sources"),
        "data_destinations": metadata.pop("data_destinations", []),
        "curation": metadata.pop("curation", False),
        "test": metadata.pop("test", False),
        "update": metadata.pop("update", False),
        "acl": metadata.get("mdf", {}).get("acl", ["public"]),
        "dataset_acl": metadata.pop("dataset_acl", []),
        "storage_acl": metadata.get("mdf", {}).get("acl", ["public"]),
        "index": metadata.pop("index", {}),
        "services": metadata.pop("services", {}),
        "extraction_config": metadata.pop("extraction_config", {}),
        "no_extract": metadata.pop("no_extract", False),  # Pass-through flag
        "submitter": name
    }

    sub_title = metadata["dc"]["titles"][0]["title"]

    try:
        # author_name is first author familyName, first author creatorName,
        # or submitter
        author_name = metadata["dc"]["creators"][0].get(
            "familyName", metadata["dc"]["creators"][0].get("creatorName", name))
        existing_source_name = metadata.get("mdf", {}).get("source_name", None)
        is_test = sub_conf["test"]
        source_id_info = sourceid_manager.make_source_id(
            existing_source_name or sub_title, author_name,
            is_test=is_test,
            index=(CONFIG["INGEST_TEST_INDEX"] if is_test else CONFIG["INGEST_INDEX"]),
            sanitize_only=bool(existing_source_name))
        print("SourceID Info ", source_id_info)
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": str(e)
                })
        }

    source_id = source_id_info["source_id"]
    source_name = source_id_info["source_name"]
    if (len(source_id_info["user_id_list"]) > 0
            and not any([uid in source_id_info["user_id_list"] for uid in identities])):
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": ("Your source_name or title has been submitted previously "
                              "by another user. Please change your source_name to "
                              "correct this error.")
                })
        }

    # Verify update flag is correct
    # update == False but version > 1
    if not sub_conf["update"] and (source_id_info["search_version"] > 1
                                   or source_id_info["submission_version"] > 1):
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": (
                        "This dataset has already been submitted, but this submission is not "
                        "marked as an update.\nIf you are updating a previously submitted "
                        "dataset, please resubmit with 'update=True'.\nIf you are submitting "
                        "a new dataset, please change the source_name.")
                })
        }

    # update == True but version == 1
    elif sub_conf["update"] and (source_id_info["search_version"] == 1
                                 and source_id_info["submission_version"] == 1):
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": (
                        "This dataset has not already been submitted, but this submission is "
                        "marked as an update.\nIf you are updating a previously submitted "
                        "dataset, please verify that your source_name is correct.\nIf you "
                        "are submitting a new dataset, please resubmit with 'update=False'.")
                })
        }

    print("Source ID", source_id_info)

    # Set appropriate metadata
    if not metadata.get("mdf"):
        metadata["mdf"] = {}
    metadata["mdf"]["source_id"] = source_id
    metadata["mdf"]["source_name"] = source_name
    metadata["mdf"]["version"] = source_id_info["search_version"]

    # Fetch custom block descriptors, cast values to str, turn _description => _desc
    new_custom = {}
    for key, val in metadata.pop("custom", {}).items():
        if key.endswith("_description"):
            new_custom[key[:-len("ription")]] = str(val)
        else:
            new_custom[key] = str(val)
    for key, val in metadata.pop("custom_desc", {}).items():
        if key.endswith("_desc"):
            new_custom[key] = str(val)
        elif key.endswith("_description"):
            new_custom[key[:-len("ription")]] = str(val)
        else:
            new_custom[key + "_desc"] = str(val)
    if new_custom:
        metadata["custom"] = new_custom

    # Get organization rules to apply
    if metadata["mdf"].get("organizations"):
        try:
            metadata["mdf"]["organizations"], sub_conf = \
                sourceid_manager.fetch_org_rules(metadata["mdf"]["organizations"],
                                                 sub_conf)
        except ValueError as e:
            logger.info(
                "Invalid organizations: {}".format(metadata["mdf"]["organizations"]))
            return {
                'statusCode': 400,
                'body': json.dumps(
                    {
                        "success": False,
                        "error": str(e)
                    })
            }

        # Pull out DC fields from org metadata
        # rightsList (license)
        if sub_conf.get("rightsList"):
            if not metadata["dc"].get("rightsList"):
                metadata["dc"]["rightsList"] = []
            metadata["dc"]["rightsList"] += sub_conf.pop("rightsList")
        # fundingReferences
        if sub_conf.get("fundingReferences"):
            if not metadata["dc"].get("fundingReferences"):
                metadata["dc"]["fundingReferences"] = []
            metadata["dc"]["fundingReferences"] += sub_conf.pop("fundingReferences")

    # Check that user is in appropriate org group(s), if applicable
    if sub_conf.get("permission_groups"):
        for group_uuid in sub_conf["permission_groups"]:
            try:
                group_res = sourceid_manager.authenticate_token(access_token, group_uuid)
            except Exception as e:
                logger.error("Authentication failure: {}".format(repr(e)))
                return {
                    'statusCode': 500,
                    'body': json.dumps(
                        {
                            "success": False,
                            "error": "Authentication failed"
                        })
                }

            if not group_res["success"]:
                error_code = group_res.pop("error_code")
                return {
                    'statusCode': error_code,
                    'body': json.dumps(group_res)
                }

    # If ACL includes "public", no other entries needed
    if "public" in sub_conf["acl"]:
        sub_conf["acl"] = ["public"]
    # Otherwise, make sure Connect admins have permission, also deduplicate
    else:
        sub_conf["acl"].append(CONFIG["ADMIN_GROUP_ID"])
        sub_conf["acl"] = list(set(sub_conf["acl"]))
    # Set correct ACL in metadata
    if "public" in sub_conf["dataset_acl"] or "public" in sub_conf["acl"]:
        sub_conf["dataset_acl"] = ["public"]
    else:
        sub_conf["dataset_acl"] = list(set(sub_conf["dataset_acl"] + sub_conf["acl"]))

    metadata["mdf"]["acl"] = sub_conf["dataset_acl"]

    # Set defaults for services if parameters not set or test flag overrides
    # Test defaults
    if sub_conf["test"]:
        # MDF Search
        sub_conf["services"]["mdf_search"] = {
            "index": CONFIG["INGEST_TEST_INDEX"]
        }
        # MDF Publish
        if sub_conf["services"].get("mdf_publish") is True:
            sub_conf["services"]["mdf_publish"] = {
                "publication_location": ("globus://{}{}/"
                                         .format(CONFIG["BACKUP_EP"],
                                                 os.path.join(CONFIG["BACKUP_PATH"],
                                                              source_id)))
            }
        if sub_conf["services"].get("mdf_publish"):
            sub_conf["services"]["mdf_publish"]["doi_test"] = True
        # Citrine
        if sub_conf["services"].get("citrine"):
            sub_conf["services"]["citrine"] = {
                "public": False
            }
        # MRR
        if sub_conf["services"].get("mrr"):
            sub_conf["services"]["mrr"] = {
                "test": True
            }
    # Non-test defaults
    else:
        # MDF Publish
        if sub_conf["services"].get("mdf_publish") is True:
            sub_conf["services"]["mdf_publish"] = {
                "publication_location": ("globus://{}{}/"
                                         .format(CONFIG["BACKUP_EP"],
                                                 os.path.join(CONFIG["BACKUP_PATH"],
                                                              source_id)))
            }
        if sub_conf["services"].get("mdf_publish"):
            sub_conf["services"]["mdf_publish"]["doi_test"] = CONFIG["DEFAULT_DOI_TEST"]
        # Citrine
        if sub_conf["services"].get("citrine") is True:
            sub_conf["services"]["citrine"] = {
                "public": CONFIG["DEFAULT_CITRINATION_PUBLIC"]
            }
        # MRR
        if sub_conf["services"].get("mrr") is True:
            sub_conf["services"]["mrr"] = {
                "test": CONFIG["DEFAULT_MRR_TEST"]
            }

    # Must be Publishing if not extracting
    if sub_conf["no_extract"] and not sub_conf["services"].get("mdf_publish"):
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": "You must specify 'services.mdf_publish' if using the 'no_extract' flag",
                    "details": (
                        "Datasets that are marked for 'pass-through' functionality "
                        "(with the 'no_extract' flag) MUST be published (by using "
                        "the 'mdf_publish' service in the 'services' block.")
                })
        }

    # If Publishing, canonical data location is Publish location
    elif sub_conf["services"].get("mdf_publish"):
        sub_conf["canon_destination"] = utils.normalize_globus_uri(
            sub_conf["services"]["mdf_publish"]
            ["publication_location"], CONFIG)
        # Transfer into source_id dir
        sub_conf["canon_destination"] = os.path.join(sub_conf["canon_destination"],
                                                     source_id + "/")
    # Otherwise (not Publishing), canon destination is backup
    else:
        sub_conf["canon_destination"] = ("globus://{}{}/"
                                         .format(CONFIG["BACKUP_EP"],
                                                 os.path.join(CONFIG["BACKUP_PATH"],
                                                              source_id)))
    # Remove canon dest from data_destinations (canon dest transferred to separately)
    if sub_conf["canon_destination"] in sub_conf["data_destinations"]:
        sub_conf["data_destinations"].remove(sub_conf["canon_destination"])
    # Transfer into source_id dir
    final_dests = []
    for dest in sub_conf["data_destinations"]:
        norm_dest = utils.normalize_globus_uri(dest, CONFIG)
        final_dests.append(os.path.join(norm_dest, source_id + "/"))
    sub_conf["data_destinations"] = final_dests

    # Add canon dest to metadata
    metadata["data"] = {
        "endpoint_path": sub_conf["canon_destination"],
        "link": utils.make_globus_app_link(sub_conf["canon_destination"], CONFIG)
    }
    if metadata.get("external_uri"):
        metadata["data"]["external_uri"] = metadata.pop("external_uri")

    # Determine storage_acl to set on canon destination
    # Default is the base acl, but if dataset and dest are already public, set None
    # If not backing up dataset, storage_acl should be default (also doesn't matter)
    if CONFIG["BACKUP_EP"]:
        try:
            # This is the only part of submission intake where we need a Transfer client
            mdf_tc = mdf_toolbox.confidential_login(services="transfer",
                                                    **CONFIG["GLOBUS_CREDS"])["transfer"]
            # Get EP + path from canon dest
            canon_loc = urllib.parse.urlparse(sub_conf["canon_destination"])
            # Get full list of ACLs (there is no search-by-path)
            acl_list = mdf_tc.endpoint_acl_list(canon_loc.netloc)
            # Get list of paths to match
            head = canon_loc.path
            path_list = []
            # If head ends with slash, slash will be removed by dirname
            # Otherwise, whole leaf dir removed by dirname - should save beforehand
            if not head.endswith("/"):
                path_list.append(head)
            while head and head != "/":
                head = os.path.dirname(head)
                # ACL paths listed with trailing slash
                # Don't add trailing slash to root path
                path_list.append((head + '/') if head != '/' else head)
        except Exception as e:
            if e.code == "PermissionDenied":
                return {
                    'statusCode': 500,
                    'body': json.dumps(
                        {
                            "success": False,
                            "error": (
                                "MDF Connect (UUID '{}') does not have the Access Manager role on "
                                "primary data destination endpoint '{}'.  This role is required "
                                "so that MDF Connect can set ACLs on the data. Please contact "
                                "the MDF team or the owner of the endpoint to resolve this "
                                "error.").format(CONFIG["API_CLIENT_ID"],
                                                 canon_loc.netloc)
                        })
                }
            else:
                logger.error("Public ACL check exception: {}".format(e))
                return {
                    'statusCode': 500,
                    'body': json.dumps(
                        {
                            "success": False,
                            "error": repr(e)
                        })
                }

        # Check if any dir in canon_dest path is public
        public_principals = ["anonymous", "all_authenticated_users"]
        public_type = False
        public_dir = None
        for rule in acl_list.data["DATA"]:
            if rule["path"] in path_list and rule["principal_type"] in public_principals:
                # Log public access dir and stop searching
                public_type = rule["principal_type"]
                public_dir = rule["path"]
                break

        # If the dir is public and dataset is public, do not set a storage_acl
        if public_type and "public" in sub_conf["acl"]:
            sub_conf["storage_acl"] = None
        # If the dir is public and the dataset is not public, error
        elif public_type and "public" not in sub_conf["acl"]:
            return {
                'statusCode': 400,
                'body': json.dumps(
                    {
                        "success": False,
                        "error": (
                            "Your submission has a non-public base ACL ({}), but the primary "
                            "storage location for your data is public (path '{}' on endpoint "
                            "'{}' is set to {} access)").format(sub_conf["acl"],
                                                                public_dir,
                                                                canon_loc.netloc,
                                                                public_type)
                    })
            }

        # If the dir is not public, set the storage_acl to the base acl
        else:
            sub_conf["storage_acl"] = sub_conf["acl"]

    status_info = {
        "source_id": source_id,
        "submission_time": datetime.utcnow().isoformat("T") + "Z",
        "submitter": name,
        "title": sub_title,
        "user_id": user_id,
        "user_email": user_email,
        "acl": sub_conf["acl"],
        "test": sub_conf["test"],
        "original_submission": json.dumps(md_copy)
    }

    print("status ", status_info)

    try:
        status_res = dynamo_manager.create_status(status_info)
    except Exception as e:
        logger.error("Status creation exception: {}".format(e))
        return {
            'statusCode': 500,
            'body': json.dumps(
                {
                    "success": False,
                    "error": repr(e)
                })
        }

    if not status_res["success"]:
        logger.error("Status creation error: {}".format(status_res["error"]))
        return {
            'statusCode': 500,
            'body': json.dumps(status_res)
        }

    automate_manager = AutomateManager(get_secret())
    organization = Organization.from_schema_repo(
        metadata["mdf"].get("organizations", "MDF Open"))
    print("######", organization)
    print("Depends ", globus_dependent_token)
    print("Token", globus_dependent_token['ce2aca7c-6de8-4b57-b0a0-dcca83a232ab'])
    automate_manager.submit(metadata, organization, globus_dependent_token[
        'ce2aca7c-6de8-4b57-b0a0-dcca83a232ab'], user_id)

    return {
        'statusCode': 202,
        'body': json.dumps(
            {
                "success": True,
                'source_id': source_id
            })
    }
