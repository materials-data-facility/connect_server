import json
import os
import jsonschema
import source_id_manager
import logging

logger = logging.getLogger(__name__)


class ClientException(Exception):
    pass


CONFIG = {
    "ADMIN_GROUP_ID": "5fc63928-3752-11e8-9c6f-0e00fd09bf20"
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
    print(event)
    name = event['requestContext']['authorizer']['name']
    identities = event['requestContext']['authorizer']['identities']
    print("name ", name, "identities", identities)
    access_token = event['headers']['Authorization']

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
        source_id_info = source_id_manager.make_source_id(
            existing_source_name or sub_title, author_name,
            test=sub_conf["test"],
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
                source_id_manager.fetch_org_rules(metadata["mdf"]["organizations"], sub_conf)
        except ValueError as e:
            logger.info("Invalid organizations: {}".format(metadata["mdf"]["organizations"]))
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
                group_res = source_id_manager.authenticate_token(access_token, group_uuid)
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

    return {
        'statusCode': 202,
        'body': json.dumps(
            {
                'source_id': source_id,
            })
    }
