import json
import logging
import os
import traceback
import uuid
from copy import deepcopy
from datetime import datetime

import jsonschema

from automate_manager import AutomateManager
from dynamo_manager import DynamoManager
from organization import Organization
from source_id_manager import SourceIDManager
from utils import get_secret

logger = logging.getLogger(__name__)


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
    user_email = event['requestContext']['authorizer']['user_email']

    depends = event['requestContext']['authorizer']['globus_dependent_token'].replace(
        'null', 'None')
    globus_dependent_token = eval(depends)
    print("name ", name, "identities", identities)
    print("globus_dependent_token ", globus_dependent_token)

    run_as_scope = os.environ["RUN_AS_SCOPE"]
    monitor_by_group = os.environ['MONITOR_BY_GROUP']
    search_index_uuid = os.environ['SEARCH_INDEX_UUID']
    test_search_index_uuid = os.environ['TEST_SEARCH_INDEX_UUID']

    access_token = event['headers']['authorization']

    dynamo_manager = DynamoManager()
    sourceid_manager = SourceIDManager()

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

    print("+++Metadata+++", metadata)

    org_cannonical_name = metadata.get("mdf", {}).get("organization", "MDF Open")
    # MDF Connect Client needs to only allow one organization. Til then, we just
    # take the first one
    if type(org_cannonical_name) == list:
        org_cannonical_name = org_cannonical_name[0]

    organization = Organization.from_schema_repo(org_cannonical_name)
    print("######", organization)

    # Validate input JSON
    # resourceType is always going to be Dataset, don't require from user
    # i.e. default to Dataset
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

    # Move tags to dc.subjects - this is to simplify the specification of tags UX feature
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

    # Pull out configuration fields from metadata into submission_conf, set defaults where appropriate
    submission_conf = {
        "data_sources": metadata.pop("data_sources"),
        "data_destinations": metadata.pop("data_destinations", []),
        "test": metadata.pop("test", False),
        "update": metadata.pop("update", False),
        "acl": metadata.get("mdf", {}).get("acl", ["public"]),
        "dataset_acl": metadata.pop("dataset_acl", []),
        "storage_acl": metadata.get("mdf", {}).get("acl", ["public"]),
        "index": metadata.pop("index", {}),
        "services": metadata.pop("services", {}),
        "extraction_config": metadata.pop("extraction_config", {}),
        "no_extract": metadata.pop("no_extract", False),  # Pass-through flag
        "submitter": name,
        "update_metadata_only": metadata.pop("update_metadata_only", False),
    }

    submission_title = metadata["dc"]["titles"][0]["title"]

    try:
        # author_name is first author familyName, first author creatorName,
        # or submitter
        author_name = metadata["dc"]["creators"][0].get(
            "familyName", metadata["dc"]["creators"][0].get("creatorName", name))

        #
        existing_source_name = metadata.get("mdf", {}).get("source_name", None)

        is_test = submission_conf["test"]

        # Distinguish test sources from prod. Tack -test on the end of the provided
        # name
        if is_test and existing_source_name:
            existing_source_name += "-test"

        print("++++++++existing_source+++++++", existing_source_name)

        if not existing_source_name:
            source_name = str(uuid.uuid4())
            existing_record = None
            version = None
            previous_versions = []
        else:
            existing_record = dynamo_manager.get_current_version(existing_source_name)
            source_name = existing_source_name
            version = existing_record['version'] if existing_record else None
            previous_versions = existing_record['previous_versions'] + [
                f"{source_name}-{version}"] if existing_record else []
    except Exception as e:
        traceback.print_exc()
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": str(e)
                })
        }

    if existing_record and not any(
            [uid == existing_record['user_id'] for uid in identities]):
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": "Only the submitting user is allowed to update this record"
                })
        }

    # Verify update flag is correct
    # update == False but version > 1
    if existing_record and not submission_conf["update"]:
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": (
                        "This dataset has already been submitted, but this submission is "
                        "not marked as an update.")
                })
        }
    # update == True but version == 1
    elif submission_conf["update"] and not existing_record:
        return {
            'statusCode': 400,
            'body': json.dumps(
                {
                    "success": False,
                    "error": (
                        "This dataset has not already been submitted, but this "
                        "submission is marked as an update.\nIf you are updating a "
                        "previously submitted dataset, please verify that your "
                        "source_name is correct.\nIf you are submitting a new dataset, "
                        "please resubmit with 'update=False'.")
                })
        }

    # Set appropriate metadata
    if not metadata.get("mdf"):
        metadata["mdf"] = {}
    version = DynamoManager.increment_record_version(version)
    metadata["mdf"]["source_id"] = source_name
    metadata["mdf"]["versioned_source_id"] = f"{source_name}-{version}"
    metadata["mdf"]["source_name"] = source_name
    metadata["mdf"]["version"] = version
    metadata["mdf"]["domains"] = organization.domains
    metadata["mdf"]["resource_type"] = "dataset"  # Force the resource type to make this findable in the portal
    metadata["mdf"]["ingest_date"] = datetime.utcnow().isoformat("T") + "Z"

    # Fetch custom block descriptors, cast values to str, turn _description => _desc
    # @BenB edited
    # new_custom = {}
    # for key, val in metadata.pop("custom", {}).items():
    #     if key.endswith("_description"):
    #         new_custom[key[:-len("ription")]] = str(val)
    #     else:
    #         new_custom[key] = str(val)
    # for key, val in metadata.pop("custom_desc", {}).items():
    #     if key.endswith("_desc"):
    #         new_custom[key] = str(val)
    #     elif key.endswith("_description"):
    #         new_custom[key[:-len("ription")]] = str(val)
    #     else: 
    #         new_custom[key + "_desc"] = str(val)
    # if new_custom:
    #     metadata["custom"] = new_custom

    ### Move this to the start of the operation
    # @Ben Or make this its own function that checks auth status against a group ID
    # Check that user is in appropriate org group(s), if applicable
    if submission_conf.get("permission_groups"):
        for group_uuid in submission_conf["permission_groups"]:
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

    status_info = {
        "source_id": source_name,
        "version": metadata["mdf"]["version"],
        "previous_versions": previous_versions,
        "submission_time": datetime.utcnow().isoformat("T") + "Z",
        "submitter": name,
        "title": submission_title,
        "user_id": user_id,
        "user_email": user_email,
        "acl": submission_conf["acl"],
        "test": submission_conf["test"],
        "original_submission": json.dumps(md_copy),
        "update_metadata_only": submission_conf["update_metadata_only"],
    }

    print("status info", status_info)

    automate_manager = AutomateManager(get_secret(secret_name=os.environ['MDF_SECRETS_NAME'],
                                                  region_name=os.environ['MDF_AWS_REGION']), is_test)
    automate_manager.authenticate()

    try:
        # Passes to submit with magic UUID that allows mdf admins to monitor flows in progress
        action_id = automate_manager.submit(mdf_rec=metadata, organization=organization,
                                            submitting_user_token=globus_dependent_token[
                                                run_as_scope],
                                            submitting_user_id=user_id,
                                            submitting_user_email=user_email,
                                            monitor_by_id=[
                                                'urn:globus:auth:identity:' + user_id,
                                                monitor_by_group],
                                            search_index_uuid=search_index_uuid if not is_test else test_search_index_uuid,
                                            data_sources=submission_conf['data_sources'],
                                            is_test=is_test,
                                            update_metadata_only=submission_conf[
                                                'update_metadata_only'],
                                            )
        status_info['action_id'] = action_id

    except Exception as e:
        logger.error("Globus Automate Flow Submission exception: {}".format(e))
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps(
                {
                    "success": False,
                    "error": repr(e)
                })
        }

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

    return {
        'statusCode': 202,
        'body': json.dumps(
            {
                "success": True,
                'source_id': source_name,
                'version': status_info['version']
            })
    }
