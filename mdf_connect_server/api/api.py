from copy import deepcopy
from datetime import datetime
import json
import logging
import os
from tempfile import NamedTemporaryFile
import urllib

from flask import Flask, jsonify, redirect, request
from globus_nexus_client import NexusClient
import globus_sdk
import jsonschema
import mdf_toolbox

from mdf_connect_server import CONFIG
from mdf_connect_server import utils


app = Flask(__name__)
app.config.from_mapping(**CONFIG)
app.url_map.strict_slashes = False

# Set up root logger
logger = logging.getLogger("mdf_connect_server")
logger.setLevel(CONFIG["LOG_LEVEL"])
logger.propagate = False
# Set up formatters
logfile_formatter = logging.Formatter("[{asctime}] [{levelname}] {name}: {message}",
                                      style='{',
                                      datefmt="%Y-%m-%d %H:%M:%S")
# Set up handlers
logfile_handler = logging.FileHandler(CONFIG["API_LOG_FILE"], mode='a')
logfile_handler.setFormatter(logfile_formatter)

logger.addHandler(logfile_handler)

logger.info("\n\n==========Connect API started==========\n")


'''
@app.before_request
def disable_connect():
    """For use when Connect is up but unable to process submissions."""
    # expected_back = "by Tuesday, September 10th"
    expected_back = "soon"
    return (jsonify({
        "success": False,
        "error": ("MDF Connect is currently unavailable due to backend maintenance. "
                  "We expect service to be restored {}."
                  "We apologize for the inconvenience.").format(expected_back)
    }), 503)
'''


# Redirect root requests and GETs to the web form
@app.route('/', methods=["GET", "POST"])
@app.route('/submit', methods=["GET"])
@app.route('/convert', methods=["GET"])
@app.route('/extract', methods=["GET"])
@app.route('/ingest', methods=["GET"])
def root_call():
    return redirect(CONFIG["FORM_URL"], code=302)


@app.route('/submit', methods=["POST"])
@app.route('/convert', methods=["POST"])
@app.route('/extract', methods=["POST"])
def accept_submission():
    """Accept the JSON metadata and begin the extraction process."""
    logger.debug("Started new submission")
    access_token = request.headers.get("Authorization")
    try:
        auth_res = utils.authenticate_token(access_token, "extract")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
        }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)

    user_id = auth_res["user_id"]
    name = auth_res["name"]
    email = auth_res["email"]
    identities = auth_res["identities_set"]

    metadata = request.get_json(force=True, silent=True)
    md_copy = deepcopy(metadata)
    if not metadata:
        return (jsonify({
            "success": False,
            "error": "POST data empty or not JSON"
        }), 400)
    # NaN, Infinity, and -Infinity cause issues in Search, and have no use in MDF
    try:
        json.dumps(metadata, allow_nan=False)
    except ValueError as e:
        return (jsonify({
            "success": False,
            "error": "{}, submission must be valid JSON".format(str(e))
        }), 400)
    except json.JSONDecodeError as e:
        return (jsonify({
            "success": False,
            "error": "{}, submission must be valid JSON".format(repr(e))
        }), 400)

    # If this is an incremental update, fetch the original submission
    if metadata.get("incremental_update"):
        # source_name and title cannot be updated
        metadata.get("mdf", {}).pop("source_name", None)
        metadata.get("dc", {}).pop("titles", None)
        # update must be True
        if not metadata.get("update"):
            return (jsonify({
                "success": False,
                "error": ("You must be updating a submission (set update=True) "
                          "when incrementally updating.")
            }), 400)
        # Fetch and merge original submission
        prev_sub = utils.read_table("status", metadata["incremental_update"])
        if not prev_sub["success"]:
            return (jsonify({
                "success": False,
                "error": ("Submission '{}' not found, or not available"
                          .format(metadata["incremental_update"]))
            }), 404)
        prev_sub = json.loads(prev_sub["status"]["original_submission"])
        new_sub = mdf_toolbox.dict_merge(metadata, prev_sub)
        # TODO: Are there any other validity checks necessary here?
        metadata = new_sub
        md_copy = deepcopy(metadata)

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

    with open(os.path.join(CONFIG["SCHEMA_PATH"], "connect_submission.json")) as schema_file:
        schema = json.load(schema_file)
    resolver = jsonschema.RefResolver(base_uri="file://{}/".format(CONFIG["SCHEMA_PATH"]),
                                      referrer=schema)
    try:
        jsonschema.validate(metadata, schema, resolver=resolver)
    except jsonschema.ValidationError as e:
        return (jsonify({
            "success": False,
            "error": "Invalid submission: " + str(e).split("\n")[0],
            "details": str(e)
        }), 400)

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

    # Create source_name
    sub_title = metadata["dc"]["titles"][0]["title"]
    try:
        # author_name is first author familyName, first author creatorName,
        # or submitter
        author_name = metadata["dc"]["creators"][0].get(
                            "familyName", metadata["dc"]["creators"][0].get("creatorName", name))
        existing_source_name = metadata.get("mdf", {}).get("source_name", None)
        source_id_info = utils.make_source_id(existing_source_name or sub_title,
                                              author_name, test=sub_conf["test"],
                                              sanitize_only=bool(existing_source_name))
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": str(e)
        }), 400)
    source_id = source_id_info["source_id"]
    source_name = source_id_info["source_name"]
    if (len(source_id_info["user_id_list"]) > 0
            and not any([uid in source_id_info["user_id_list"] for uid in identities])):
        return (jsonify({
            "success": False,
            "error": ("Your source_name or title has been submitted previously "
                      "by another user. Please change your source_name to "
                      "correct this error.")
            }), 400)
    # Verify update flag is correct
    # update == False but version > 1
    if not sub_conf["update"] and (source_id_info["search_version"] > 1
                                   or source_id_info["submission_version"] > 1):
        return (jsonify({
            "success": False,
            "error": ("This dataset has already been submitted, but this submission is not "
                      "marked as an update.\nIf you are updating a previously submitted "
                      "dataset, please resubmit with 'update=True'.\nIf you are submitting "
                      "a new dataset, please change the source_name.")
            }), 400)
    # update == True but version == 1
    elif sub_conf["update"] and (source_id_info["search_version"] == 1
                                 and source_id_info["submission_version"] == 1):
        return (jsonify({
            "success": False,
            "error": ("This dataset has not already been submitted, but this submission is "
                      "marked as an update.\nIf you are updating a previously submitted "
                      "dataset, please verify that your source_name is correct.\nIf you "
                      "are submitting a new dataset, please resubmit with 'update=False'.")
            }), 400)

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
            new_custom[key+"_desc"] = str(val)
    if new_custom:
        metadata["custom"] = new_custom

    # Get organization rules to apply
    if metadata["mdf"].get("organizations"):
        try:
            metadata["mdf"]["organizations"], sub_conf = \
                utils.fetch_org_rules(metadata["mdf"]["organizations"], sub_conf)
        except ValueError as e:
            logger.info("Invalid organizations: {}".format(metadata["mdf"]["organizations"]))
            return (jsonify({
                "success": False,
                "error": str(e)
            }), 400)
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
                group_res = utils.authenticate_token(access_token, group_uuid)
            except Exception as e:
                logger.error("Authentication failure: {}".format(repr(e)))
                return (jsonify({
                    "success": False,
                    "error": "Authentication failed"
                }), 500)
            if not group_res["success"]:
                error_code = group_res.pop("error_code")
                return (jsonify(group_res), error_code)

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
                                                 os.path.join(CONFIG["BACKUP_PATH"], source_id)))
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
                                                 os.path.join(CONFIG["BACKUP_PATH"], source_id)))
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
        return (jsonify({
            "success": False,
            "error": "You must specify 'services.mdf_publish' if using the 'no_extract' flag",
            "details": ("Datasets that are marked for 'pass-through' functionality "
                        "(with the 'no_extract' flag) MUST be published (by using "
                        "the 'mdf_publish' service in the 'services' block.")
        }), 400)
    # If Publishing, canonical data location is Publish location
    elif sub_conf["services"].get("mdf_publish"):
        sub_conf["canon_destination"] = utils.normalize_globus_uri(
                                                    sub_conf["services"]["mdf_publish"]
                                                            ["publication_location"])
        # Transfer into source_id dir
        sub_conf["canon_destination"] = os.path.join(sub_conf["canon_destination"],
                                                     source_id + "/")
    # Otherwise (not Publishing), canon destination is backup
    else:
        sub_conf["canon_destination"] = ("globus://{}{}/"
                                         .format(CONFIG["BACKUP_EP"],
                                                 os.path.join(CONFIG["BACKUP_PATH"], source_id)))
    # Remove canon dest from data_destinations (canon dest transferred to separately)
    if sub_conf["canon_destination"] in sub_conf["data_destinations"]:
        sub_conf["data_destinations"].remove(sub_conf["canon_destination"])
    # Transfer into source_id dir
    final_dests = []
    for dest in sub_conf["data_destinations"]:
        norm_dest = utils.normalize_globus_uri(dest)
        final_dests.append(os.path.join(norm_dest, source_id + "/"))
    sub_conf["data_destinations"] = final_dests

    # Add canon dest to metadata
    metadata["data"] = {
        "endpoint_path": sub_conf["canon_destination"],
        "link": utils.make_globus_app_link(sub_conf["canon_destination"])
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
                return (jsonify({
                    "success": False,
                    "error": ("MDF Connect (UUID '{}') does not have the Access Manager role on "
                              "primary data destination endpoint '{}'.  This role is required "
                              "so that MDF Connect can set ACLs on the data. Please contact "
                              "the MDF team or the owner of the endpoint to resolve this "
                              "error.").format(CONFIG["API_CLIENT_ID"], canon_loc.netloc)
                }), 500)
            else:
                logger.error("Public ACL check exception: {}".format(e))
                return (jsonify({
                    "success": False,
                    "error": repr(e)
                }), 500)
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
            return (jsonify({
                "success": False,
                "error": ("Your submission has a non-public base ACL ({}), but the primary "
                          "storage location for your data is public (path '{}' on endpoint "
                          "'{}' is set to {} access)").format(sub_conf["acl"], public_dir,
                                                              canon_loc.netloc, public_type)
            }), 400)
        # If the dir is not public, set the storage_acl to the base acl
        else:
            sub_conf["storage_acl"] = sub_conf["acl"]

    status_info = {
        "source_id": source_id,
        "submission_time": datetime.utcnow().isoformat("T") + "Z",
        "submitter": name,
        "title": sub_title,
        "user_id": user_id,
        "user_email": email,
        "acl": sub_conf["acl"],
        "test": sub_conf["test"],
        "original_submission": json.dumps(md_copy)
        }
    try:
        status_res = utils.create_status(status_info)
    except Exception as e:
        logger.error("Status creation exception: {}".format(e))
        return (jsonify({
            "success": False,
            "error": repr(e)
            }), 500)
    if not status_res["success"]:
        logger.error("Status creation error: {}".format(status_res["error"]))
        return (jsonify(status_res), 500)

    try:
        submission_args = {
            "metadata": metadata,
            "sub_conf": sub_conf,
            "source_id": source_id,
            "access_token": access_token,
            "user_id": user_id
        }
        sub_res = utils.submit_to_queue(submission_args)
        if not sub_res["success"]:
            logger.error("Submission to SQS error: {}".format(sub_res["error"]))
            return (jsonify(sub_res), 500)
    except Exception as e:
        logger.error("Submission to SQS exception: {}".format(e))
        return (jsonify({
            "success": False,
            "error": repr(e)
            }), 500)

    logger.info("Extract submission '{}' accepted".format(source_id))
    return (jsonify({
        "success": True,
        "source_id": source_id
        }), 202)


@app.route("/ingest", methods=["POST"])
def reject_ingest():
    """Deprecate the /ingest route."""
    return (jsonify({
        "success": False,
        "error": "/ingest has been deprecated. Use /submit for all Connect submissions."
    }), 410)


@app.route("/update/<source_id>", methods=["POST"])
def metadata_update(source_id):
    """Update the dataset entry without uploading the data or creating a new version."""
    # User auth
    try:
        auth_res = utils.authenticate_token(request.headers.get("Authorization"), "extract")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
        }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)

    update_metadata = request.get_json(force=True, silent=True)
    if not update_metadata:
        return (jsonify({
            "success": False,
            "error": "POST data empty or not JSON"
        }), 400)
    # NaN, Infinity, and -Infinity cause issues in Search, and have no use in MDF
    try:
        json.dumps(update_metadata, allow_nan=False)
    except ValueError as e:
        return (jsonify({
            "success": False,
            "error": "{}, submission must be valid JSON".format(str(e))
        }), 400)
    except json.JSONDecodeError as e:
        return (jsonify({
            "success": False,
            "error": "{}, submission must be valid JSON".format(repr(e))
        }), 400)

    # Certain mdf block fields cannot be updated (source_name, etc.)
    if any([banned_field in update_metadata.get("mdf", {}).keys()
            for banned_field in CONFIG["NO_UPDATE_FIELDS_MDF"]]):
        return (jsonify({
            "success": False,
            "error": ("The following fields in the 'mdf' block may not be updated:\n{}"
                      .format(CONFIG["NO_UPDATE_FIELDS_MDF"]))
        }), 400)

    # update_metadata munging - tags -> dc.subjects and external_uri -> data.external_uri
    if update_metadata.get("tags"):
        tags = update_metadata.pop("tags", [])
        if not isinstance(tags, list):
            tags = [tags]
        if not update_metadata.get("dc"):
            update_metadata["dc"] = {}
        if not update_metadata["dc"].get("subjects"):
            update_metadata["dc"]["subjects"] = []
        for tag in tags:
            update_metadata["dc"]["subjects"].append({
                "subject": tag
            })
    if update_metadata.get("external_uri"):
        if not update_metadata.get("data"):
            update_metadata["data"] = {}
        update_metadata["data"]["external_uri"] = update_metadata.pop("external_uri")

    # Get old submission info on source_id
    source_name_info = utils.split_source_id(source_id)
    try:
        scan_res = utils.scan_table(table_name="status", fields=["source_id", "user_id"],
                                    filters=[("source_id", "^", source_name_info["source_name"])])
    except Exception as e:
        logger.error("Unable to scan status database for '{}': '{}'"
                     .format(source_name_info["source_name"], repr(e)))
        return (jsonify({
            "success": False,
            "error": ("The MDF status database is experiencing technical difficulties. "
                      "Please try again later, or notify the MDF team of this error.")
        }), 500)
    if not scan_res["success"]:
        logger.error("Unable to scan status database for '{}': '{}'"
                     .format(source_name_info["source_name"], scan_res["error"]))
        return (jsonify({
            "success": False,
            "error": ("The MDF status database is experiencing technical difficulties. "
                      "Please try again later, or notify the MDF team of this error.")
        }), 500)

    # Check permissions - user must be in user_id list
    # This list will be empty if no results were returned (so 404 and 403 are the same)
    user_ids = set([sub["user_id"] for sub in scan_res["results"]])
    if not any([uid in user_ids for uid in auth_res["identities_set"]]):
        return (jsonify({
            "success": False,
            "error": "Submission {} not found, or not available".format(source_id)
            }), 404)
    # source_id submitted must be most recent version
    # This is to stop accidental writes, if a subsequent version updated the dataset
    current_source_id = max([sub["source_id"] for sub in scan_res["results"]])
    if current_source_id != source_id:
        return (jsonify({
            "success": False,
            "error": ("'{}' is not the current version of the dataset. The current "
                      "version is '{}'. Please verify that the current version "
                      "needs these updates.".format(source_id, current_source_id))
        }), 400)
    # Old submission must be completed, successfully
    try:
        status = utils.read_table("status", source_id)["status"]
    except Exception as e:
        logger.error("{} found in scan but not by direct read of DB: {}"
                     .format(source_id, repr(e)))
        return (jsonify({
            "success": False,
            "error": ("The MDF status database is experiencing technical difficulties. "
                      "Please try again later, or notify the MDF team of this error.")
        }), 500)
    if status["code"][-1] != "S":
        return (jsonify({
            "success": False,
            "error": ("The original submission for '{}' has not completed successfully. "
                      "Only successfully completed submissions can be updated.".format(source_id))
        }), 400)

    # Fetch old Search entry
    index = mdf_toolbox.translate_index(CONFIG["INGEST_INDEX"]
                                        if not status["test"] else CONFIG["INGEST_TEST_INDEX"])
    search_creds = mdf_toolbox.dict_merge(CONFIG["GLOBUS_CREDS"], {"services": ["search_ingest"]})
    search_client = mdf_toolbox.confidential_login(**search_creds)["search_ingest"]
    old_entry = search_client.get_entry(index, source_id)["content"][0]

    # Pull out ACL from updates or original submission
    original_submission = json.loads(status["original_submission"])
    # Order of precedence:
    #   dataset_acl from update
    #   base acl from update
    #   mdf block acl field from update
    #   dataset_acl from original submission
    #   base acl from original submission
    #   acl from original sub_conf (status)
    dataset_acl = (update_metadata.pop("dataset_acl", None) or update_metadata.pop("acl", None)
                   or update_metadata.get("mdf", {}).pop("acl", None)
                   or original_submission.get("dataset_acl") or original_submission.get("acl")
                   or status["acl"])
    # ACL should always be present, but handle missing just in case
    if not dataset_acl:
        return (jsonify({
            "success": False,
            "error": "No ACL found for this dataset. Please submit an ACL or dataset ACL."
        }), 400)

    # Merge updates and validate
    new_entry = mdf_toolbox.dict_merge(update_metadata, old_entry)
    if not new_entry.get("mdf"):
        new_entry["mdf"] = {}
    new_entry["mdf"]["acl"] = dataset_acl
    with open(os.path.join(CONFIG["SCHEMA_PATH"], "dataset.json")) as schema_file:
        schema = json.load(schema_file)
    resolver = jsonschema.RefResolver(base_uri="file://{}/".format(CONFIG["SCHEMA_PATH"]),
                                      referrer=schema)
    try:
        jsonschema.validate(new_entry, schema, resolver=resolver)
    except jsonschema.ValidationError as e:
        return (jsonify({
            "success": False,
            "error": "Invalid dataset entry: " + str(e).split("\n")[0],
            "details": str(e)
        }), 400)

    # Push updates to Search
    # Use existing tooling because of Search retry error handling
    with NamedTemporaryFile("w+") as tfile:
        json.dump(new_entry, tfile)
        tfile.seek(0)
        # Will not work on Windows - tempfile must be opened twice
        ingest_res = utils.search_ingest(tfile.name, index, delete_existing=False)
    if not ingest_res["success"]:
        return (jsonify({
            "success": False,
            "error": ("Errors ingesting to Search: {}\nDetails: {}"
                      .format(ingest_res.get("errors", []),
                              ingest_res.get("details", "No details")))
        }), 500)

    # Update Datacite, if necessary
    if new_entry.get("services", {}).get("mdf_publish"):
        try:
            doi_res = utils.datacite_update_doi(new_entry["dc"]["identifier"]["identifier"],
                                                updates=new_entry["dc"],
                                                test=status["test"] or CONFIG["DEFAULT_DOI_TEST"],
                                                url=new_entry["services"]["mdf_publish"])
        except Exception as e:
            logger.error("DOI update for {} failed: {}".format(source_id, repr(e)))
            return (jsonify({
                "success": False,
                "error": ("Unable to update DataCite metadata: '{}'\nHowever, MDF Search was "
                          "successfully updated.".format(str(e)))
            }), 502)
        if not doi_res["success"]:
            logger.error("DOI update for {} failed: {}".format(source_id, doi_res["error"]))
            return (jsonify({
                "success": False,
                "error": ("Unable to update DataCite metadata: '{}'\nHowever, MDF Search was "
                          "successfully updated.".format(doi_res["error"]))
            }), 502)

    # Log update
    # TODO: Migrate to modify_log_entry
    mod_res = utils.modify_status_entry(source_id,
                                        {"updates": status["updates"] + [update_metadata]})
    if not mod_res["success"]:
        # Log error internally, don't send user failure - update was successful, log was not
        # Critical error because status DB is not current anymore
        logger.critical("Status entry for update on {} not updated: {}"
                        .format(source_id, mod_res["error"]))

    return (jsonify({
        "success": True,
        "source_id": source_id,
        "new_dataset_entry": new_entry
    }), 200)


@app.route("/status/<source_id>", methods=["GET"])
def get_status(source_id):
    """Fetch and return status information"""
    # User auth
    try:
        auth_res = utils.authenticate_token(request.headers.get("Authorization"), "extract")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)

    raw_status = utils.read_table("status", source_id)
    # Failure message if status not fetched or user not allowed to view
    # Only the submitter, ACL users, and admins can view

    # If actually not found
    if (not raw_status["success"]
        # or dataset not public
        or ("public" not in raw_status["status"]["acl"]
            # and user was not submitter
            and raw_status["status"]["user_id"] not in auth_res["identities_set"]
            # and user is not in ACL
            and not any([uid in raw_status["status"]["acl"] for uid in auth_res["identities_set"]])
            # and user is not admin
            and not auth_res["is_admin"])):
        # Summary:
        # if (NOT found)
        #    OR (NOT public AND user != submitter AND user not in acl_list AND user is not admin)
        return (jsonify({
            "success": False,
            "error": "Submission {} not found, or not available".format(source_id)
            }), 404)
    else:
        return (jsonify({
            "success": True,
            "status": utils.translate_status(raw_status["status"])
            }), 200)


@app.route("/submissions", methods=["GET"])
@app.route("/submissions/<user_id>", methods=["GET"])
def get_user_submissions(user_id=None):
    """Get all submission statuses by a user."""
    # User auth
    try:
        auth_res = utils.authenticate_token(request.headers.get("Authorization"), "extract")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)

    # Users can request only their own submissions (by user_id or by default)
    # Admins can request any user's submissions
    if not (auth_res["is_admin"] or user_id is None or user_id in auth_res["identities_set"]):
        return (jsonify({
            "success": False,
            "error": "You are not authorized to view that submission's status"
            }), 403)

    # Create scan filter
    # Admins can request a special function instead of a user ID
    if auth_res["is_admin"] and user_id == "all":
        filters = None
    elif auth_res["is_admin"] and user_id == "active":
        filters = [("active", "==", True)]
    elif user_id is None:
        filters = [("user_id", "in", auth_res["identities_set"])]
    else:
        filters = [("user_id", "==", user_id)]

    scan_res = utils.scan_table(table_name="status", filters=filters)
    if not scan_res["success"]:
        return (jsonify(scan_res), 500)

    return (jsonify({
        "success": True,
        "submissions": [utils.translate_status(sub) for sub in scan_res["results"]]
        }), 200)


@app.route("/curation", methods=["GET"])
@app.route("/curation/<user_id>", methods=["GET"])
def get_curator_tasks(user_id=None):
    """Get all available curation tasks for a user."""
    access_token = request.headers.get("Authorization").replace("Bearer ", "")
    # User auth
    try:
        auth_res = utils.authenticate_token(access_token, "extract")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)

    # Users can request only their own curation tasks (by user_id or by default)
    # Admins can request any curator's tasks
    if not (auth_res["is_admin"] or user_id is None or user_id in auth_res["identities_set"]):
        return (jsonify({
            "success": False,
            "error": "You cannot view another curator's tasks"
            }), 403)

    # Use the user's credentials to fetch groups they're in
    # We can't actually use the user_id because we can't get that user's groups (for admins)
    try:
        mdf_conf_client = globus_sdk.ConfidentialAppAuthClient(CONFIG["API_CLIENT_ID"],
                                                               CONFIG["API_CLIENT_SECRET"])
        dependent_grant = mdf_conf_client.oauth2_get_dependent_tokens(access_token)
        # Get specifically Groups' access token
        user_groups_token = None
        for grant in dependent_grant.data:
            if grant["resource_server"] == "nexus.api.globus.org":
                user_groups_token = grant["access_token"]
        if not user_groups_token:
            raise ValueError("No user Groups token present")
        user_groups_authorizer = globus_sdk.AccessTokenAuthorizer(user_groups_token)
        user_groups_client = NexusClient(authorizer=user_groups_authorizer)
    except Exception as e:
        logger.error("Group authentication error: {}".format(repr(e)))
        return (jsonify({
            "success": False,
            "error": "Group authentication failure",
            }), 500)
    # Get all user's groups where user can curate (manager role or above)
    user_groups_raw = user_groups_client.list_groups(my_roles="manager,admin",
                                                     for_all_identities=True,
                                                     fields="id,name,my_status,my_role")
    user_groups_ids = []
    for group in user_groups_raw:
        # Only get active memberships
        if group["my_status"] == "active":
            user_groups_ids.append(group["id"])

    # Scan with no filter
    # There is no OR in Dynamo scanning, so we would have to scan the curation table once
    # per group the user is in to match all available curation tasks,
    # leading to unnacceptable latency. Instead, we're doing the filtering manually.
    #
    # A different solution would be caching the curator groups and scanning only for tasks
    # curated by groups the user is in, but it would be too difficult to update
    # that cache when a new curation task  is created (in Processing).
    #
    # Additionally, post-scan filtering allows curation by specific users instead of groups
    # (not used at the moment but potentially useful)
    scan_res = utils.scan_table(table_name="curation")
    if not scan_res["success"]:
        return (jsonify(scan_res), 500)

    # Filter results manually
    available_tasks = []
    for task in scan_res["results"]:
        # Check for membership in any allowed group, or specific user ID
        # or if admin selected "all"
        if any([(uuid in user_groups_ids
                 or uuid in auth_res["identities_set"]
                 or (auth_res["is_admin"] and user_id == "all"))
                for uuid in task["allowed_curators"]]):

            # Load JSON elements
            task["dataset"] = json.loads(task["dataset"])
            task["sample_records"] = json.loads(task["sample_records"])
            available_tasks.append(task)

    return (jsonify({
        "success": True,
        "curation_tasks": available_tasks
    }), 200)


@app.route("/curate/<source_id>", methods=["GET", "POST"])
def curate_task(source_id):
    """Interact with a curation task.
    GET requests get the task information.
    POST requests can accept or reject a task.
    """
    # Authentication stage
    # User auth
    access_token = request.headers.get("Authorization").replace("Bearer ", "")
    try:
        auth_res = utils.authenticate_token(access_token, "extract")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)

    user_id = auth_res["user_id"]
    name = auth_res["name"]

    # Fetch task from database
    task_res = utils.read_table("curation", source_id)
    task = task_res.get("status", {})

    # Check permissions, starting with short-circuits
    # Fail if task not found
    if not task_res["success"]:
        user_allowed = False
    # Succeed if task public, user ID is in allowed list, or user is admin
    elif ("public" in task["allowed_curators"]
          or user_id in task["allowed_curators"]
          or auth_res["is_admin"]):
        user_allowed = True
    # Otherwise, check group permissions
    else:
        try:
            group_res = utils.authenticate_token(access_token, task["allowed_curators"],
                                                 require_all=False)
            # User must be in group, and manager or higher
            if group_res["success"] and ("manager" in group_res["group_roles"]
                                         or "admin" in group_res["group_roles"]):
                user_allowed = True
            else:
                user_allowed = False
        except Exception as e:
            logger.error("Authentication failure: {}".format(e))
            return (jsonify({
                "success": False,
                "error": "Authentication failed"
                }), 500)

    # If user not allowed, fail
    if not user_allowed:
        return (jsonify({
            "success": False,
            "error": "Curation task for {} not found, or not available".format(source_id)
        }), 404)

    # Load JSON
    task["dataset"] = json.loads(task["dataset"])
    task["sample_records"] = json.loads(task["sample_records"])

    # Request handling stage
    # Handle GET requests (return info)
    if request.method == "GET":
        return (jsonify({
            "success": True,
            "curation_task": task
        }), 200)
    # Handle POST requests (accept or reject)
    elif request.method == "POST":
        # Get json data
        command = request.get_json(force=True, silent=True)
        if not command:
            return (jsonify({
                "success": False,
                "error": "POST data empty or not JSON"
            }), 400)
        elif not command.get("action"):
            return (jsonify({
                "success": False,
                "error": "You must specify an 'action' to curate"
            }), 400)
        elif not command.get("reason"):
            return (jsonify({
                "success": False,
                "error": "You must specify a 'reason' for action '{}'".format(command["action"])
            }), 400)

        action = command["action"].strip().lower()
        # Accept or reject
        if action in ["accept", "reject"]:
            try:
                # Format action - first capital letter, past tense
                # Ex. "accept" => "Accepted"
                formatted_action = action[0].upper() + action[1:] + "ed"
                curation_message = "{} by {}: {}".format(formatted_action, name, command["reason"])
                submission_args = {
                    "metadata": {},  # Not used after extract step
                    "sub_conf": {
                        # Only field needed to skip to curation resume
                        # Previous sub_conf loaded after resume
                        "curation": curation_message
                    },
                    "source_id": source_id,
                    "access_token": access_token,
                    "user_id": user_id
                }
                sub_res = utils.submit_to_queue(submission_args)
                if not sub_res["success"]:
                    logger.error("Submission to SQS error: {}".format(sub_res["error"]))
                    return (jsonify(sub_res), 500)
            except Exception as e:
                logger.error("Submission to SQS exception: {}".format(e))
                return (jsonify({
                    "success": False,
                    "error": str(e)
                }), 500)
            logger.info("Curation task for '{}' completed: {}".format(source_id, curation_message))
            return (jsonify({
                "success": True,
                "message": "Submission {}ed with reason: {}".format(action, command["reason"])
            }), 200)
        # Bad action
        else:
            return (jsonify({
                "success": False,
                "error": ("Action '{}' invalid. Acceptable actions are 'accept' and 'reject'"
                          .format(command["action"]))
            }), 400)
    # Can't happen
    else:
        return (jsonify({
            "success": False,
            "error": "Bad request method: '{}'".format(request.method)
        }), 405)


@app.route("/schemas", methods=["GET"])
@app.route("/schemas/<schema_type>", methods=["GET"])
def get_schema(schema_type=None):
    """Return schema of selected type.
    Valid types:
        - Named files in schema directory
        - "list" or None, which returns a list of available schemas
        - "all", which returns data on every schema in MDF
    """
    if schema_type is None:
        schema_type = "list"
    schema_type = schema_type.strip().lower()

    # Get list of all schema names
    if schema_type == "list":
        try:
            schema_list = [name.replace(".json", "") for name in os.listdir(CONFIG["SCHEMA_PATH"])
                           if name.endswith(".json")]
        except Exception as e:
            logger.error("While fetching schema list: {}".format(repr(e)))
            return (jsonify({
                "success": False,
                "error": "Unable to fetch list of MDF schemas"
            }), 500)
        else:
            return (jsonify({
                "success": True,
                "schema_list": schema_list
            }), 200)

    # Get all schemas in MDF
    elif schema_type == "all":
        try:
            schema_list = [name.replace(".json", "") for name in os.listdir(CONFIG["SCHEMA_PATH"])
                           if name.endswith(".json")]
        except Exception as e:
            logger.error("While fetching schema list: {}".format(repr(e)))
            return (jsonify({
                "success": False,
                "error": "Unable to fetch list of MDF schemas"
            }), 500)
        try:
            all_schemas = {}
            for schema_name in schema_list:
                with open(os.path.join(CONFIG["SCHEMA_PATH"],
                                       "{}.json".format(schema_name))) as schema_file:
                    raw_schema = json.load(schema_file)
                all_schemas[schema_name] = mdf_toolbox.expand_jsonschema(raw_schema,
                                                                         CONFIG["SCHEMA_PATH"])
        except Exception as e:
            logger.error("While fetching all schemas: {}".format(repr(e)))
            return (jsonify({
                "success": False,
                "error": "Unable to fetch content of all MDF schemas"
            }), 500)
        else:
            return (jsonify({
                "success": True,
                "all_schemas": all_schemas
            }), 200)

    # Get single named schema
    else:
        # Sanitize schema_type into filename-appropriate format
        schema_name = schema_type.replace(".json", "").replace(" ", "_")
        try:
            with open(os.path.join(CONFIG["SCHEMA_PATH"],
                                   "{}.json".format(schema_name))) as schema_file:
                raw_schema = json.load(schema_file)
            schema = mdf_toolbox.expand_jsonschema(raw_schema, CONFIG["SCHEMA_PATH"])
        except FileNotFoundError:
            return (jsonify({
                "success": False,
                "error": "Schema '{}' (from '{}') not found".format(schema_name, schema_type)
            }), 404)
        except Exception as e:
            logger.error("While fetching schema '{}' (from '{}'): {}"
                         .format(schema_name, schema_type, repr(e)))
            return (jsonify({
                "success": False,
                "error": "Unable to fetch schema '{}' (from '{}')".format(schema_name, schema_type)
            }), 500)
        else:
            return (jsonify({
                "success": True,
                "schema": schema
            }), 200)


@app.route("/organizations", methods=["GET"])
@app.route("/organizations/<organization>", methods=["GET"])
def get_organization(organization=None):
    """Return selected organization information.
    Valid argument values:
        - Named orgs (canonical or alias)
        - "list" or None, which returns a list of orgs
        - "all", which returns data on every org in MDF
    """
    # Normalize name: Remove special characters (including whitespace) and capitalization
    # Function for convenience, but not generalizable/useful for other cases
    def normalize_name(name): return "".join([c for c in name.lower() if c.isalnum()])

    if organization is None:
        organization = "list"
    org_type = normalize_name(organization)

    # Read org file
    try:
        with open(os.path.join(CONFIG["AUX_DATA_PATH"], "organizations.json")) as f:
            organizations = json.load(f)
    except Exception as e:
        logger.error("Unable to read organization list: {}".format(repr(e)))
        return (jsonify({
            "success": False,
            "error": "Unable to access organizations at this time."
        }), 500)

    # Get list of all orgs
    if org_type == "list":
        org_list = [org["canonical_name"] for org in organizations]
        return (jsonify({
            "success": True,
            "organization_list": org_list
        }), 200)
    # Get all orgs
    elif org_type == "all":
        return (jsonify({
            "success": True,
            "all_organizations": organizations
        }), 200)
    # Get specific org
    else:
        specific_org = []
        for org in organizations:
            aliases = [normalize_name(alias) for alias in (org.get("aliases", [])
                                                           + [org["canonical_name"]])]
            if org_type in aliases:
                specific_org.append(org)
        # None found
        if len(specific_org) < 1:
            return (jsonify({
                "success": False,
                "error": "Organization '{}' (from '{}') not found".format(org_type, organization)
            }), 404)
        elif len(specific_org) == 1:
            return (jsonify({
                "success": True,
                "organization": specific_org[0]
            }), 200)
        # Must be exactly one specific org, else is error
        # Should never happen
        elif len(specific_org) > 1:
            logger.critical("More than one org matches '{}':\n{}".format(org_type, specific_org))
            return (jsonify({
                "success": True,
                "error": ("Multiple organizations match '{}' (from '{}'). Both are included. "
                          "Please notify MDF about this error.".format(org_type, organization)),
                "organization": specific_org
            }), 200)
