from datetime import datetime
import json
import logging
import os

from flask import Flask, jsonify, redirect, request
import jsonschema

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


# Redirect root requests and GETs to the web form
@app.route('/', methods=["GET", "POST"])
@app.route('/submit', methods=["GET"])
@app.route('/convert', methods=["GET"])
@app.route('/ingest', methods=["GET"])
def root_call():
    return redirect(CONFIG["FORM_URL"], code=302)


@app.route('/submit', methods=["POST"])
@app.route('/convert', methods=["POST"])
def accept_submission():
    """Accept the JSON metadata and begin the conversion process."""
    logger.debug("Started new submission")
    access_token = request.headers.get("Authorization")
    try:
        auth_res = utils.authenticate_token(access_token, auth_level="convert")
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
    # username = auth_res["username"]
    name = auth_res["name"]
    email = auth_res["email"]
    identities = auth_res["identities_set"]

    metadata = request.get_json(force=True, silent=True)
    md_copy = request.get_json(force=True, silent=True)
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
        "test": metadata.pop("test", False) or CONFIG["DEFAULT_TEST_FLAG"],
        "update": metadata.pop("update", False),
        "acl": metadata.get("mdf", {}).get("acl", ["public"]),
        "index": metadata.pop("index", {}),
        "services": metadata.pop("services", {}),
        "conversion_config": metadata.pop("conversion_config", {}),
        "no_convert": metadata.pop("no_convert", False)  # Pass-through flag
    }

    # Create source_name
    sub_title = metadata["dc"]["titles"][0]["title"]
    try:
        source_id_info = utils.make_source_id(
                                metadata.get("mdf", {}).get("source_name") or sub_title,
                                test=sub_conf["test"])
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": repr(e)
        }), 500)
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

    # Get organization rules to apply
    if metadata["mdf"].get("organizations"):
        metadata["mdf"]["organizations"], sub_conf = \
            utils.fetch_org_rules(metadata["mdf"]["organizations"], sub_conf)
    # Check that user is in appropriate org group(s), if applicable
    # Also collect managers' UUID for ACL
    if sub_conf.get("permission_groups"):
        managers = set()
        for group_uuid in sub_conf["permission_groups"]:
            try:
                auth_res = utils.authenticate_token(access_token, auth_level=group_uuid)
            except Exception as e:
                logger.error("Authentication failure: {}".format(e))
                return (jsonify({
                    "success": False,
                    "error": "Authentication failed"
                }), 500)
            if not auth_res["success"]:
                error_code = auth_res.pop("error_code")
                return (jsonify(auth_res), error_code)
            try:
                manager_list = utils.fetch_whitelist(group_uuid, "manager")
                managers.update(manager_list)
            except Exception as e:
                logger.error("Whitelist fetch failure: {}".format(e))
                return (jsonify({
                    "success": False,
                    "error": "Group authentication failed"
                }), 500)
        sub_conf["acl"].extend(managers)

    # If ACL includes "public", no other entries needed
    if "public" in sub_conf["acl"]:
        sub_conf["acl"] = ["public"]
    # Set correct ACL in metadata
    metadata["mdf"]["acl"] = sub_conf["acl"]

    if sub_conf["test"]:
        sub_conf["services"]["mdf_search"] = {
            "index": CONFIG["INGEST_TEST_INDEX"]
        }
        if sub_conf["services"].get("citrine"):
            sub_conf["services"]["citrine"] = {
                "public": False
            }
        if sub_conf["services"].get("mrr"):
            sub_conf["services"]["mrr"] = {
                "test": True
            }
    else:
        # Put in defaults
        if sub_conf["services"].get("mdf_publish") is True:
            sub_conf["services"]["mdf_publish"] = {
                "publication_location": ("globus://{}{}/"
                                         .format(CONFIG["BACKUP_EP"],
                                                 os.path.join(CONFIG["BACKUP_PATH"], source_id)))
            }
        if sub_conf["services"].get("citrine") is True:
            sub_conf["services"]["citrine"] = {
                "public": CONFIG["DEFAULT_CITRINATION_PUBLIC"]
            }
        if sub_conf["services"].get("mrr") is True:
            sub_conf["services"]["mrr"] = {
                "test": CONFIG["DEFAULT_MRR_TEST"]
            }

    # Must be Publishing if not converting
    if sub_conf["no_convert"] and not sub_conf["services"].get("mdf_publish"):
        return (jsonify({
            "success": False,
            "error": "You must specify 'services.mdf_publish' if using the 'no_convert' flag",
            "details": ("Datasets that are marked for 'pass-through' functionality "
                        "(with the 'no_convert' flag) MUST be published (by using "
                        "the 'mdf_publish' service in the 'services' block.")
        }), 400)
    # If Publishing, canonical data location is Publish location
    elif sub_conf["services"].get("mdf_publish"):
        sub_conf["canon_destination"] = sub_conf["services"]["mdf_publish"]["publication_location"]
    # Otherwise (not Publishing), canon destination is Petrel
    else:
        sub_conf["canon_destination"] = ("globus://{}{}/"
                                         .format(CONFIG["BACKUP_EP"],
                                                 os.path.join(CONFIG["BACKUP_PATH"], source_id)))
    # Remove canon dest from data_destinations (canon dest transferred to separately)
    if sub_conf["canon_destination"] in sub_conf["data_destinations"]:
        sub_conf["data_destinations"].remove(sub_conf["canon_destination"])

    # Add canon dest to metadata
    metadata["data"] = {
        "endpoint_path": sub_conf["canon_destination"],
        "link": utils.make_globus_app_link(sub_conf["canon_destination"])
    }

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

    logger.info("Convert submission '{}' accepted".format(source_id))
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


# DEPRECATED
def accept_ingest():
    """Accept the JSON feedstock file and begin the ingestion process."""
    logger.debug("Started new ingest task")
    access_token = request.headers.get("Authorization")
    try:
        auth_res = utils.authenticate_token(access_token, auth_level="ingest")
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
    # username = auth_res["username"]
    name = auth_res["name"]
    email = auth_res["email"]
    identities = auth_res["identities_set"]

    metadata = request.get_json(force=True, silent=True)
    md_copy = request.get_json(force=True, silent=True)
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
            "error": "{}, Submission must be valid JSON".format(str(e))
            }), 400)
    except json.JSONDecodeError as e:
        return (jsonify({
            "success": False,
            "error": "{}, Submission must be valid JSON".format(repr(e))
            }), 400)

    # Validate input JSON
    with open(os.path.join(CONFIG["SCHEMA_PATH"], "connect_ingest.json")) as schema_file:
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

    feed_location = metadata["feedstock_location"]
    services = metadata.get("services", {})
    data_loc = metadata.get("data", [])
    service_data = metadata.get("service_data", [])
    title = metadata.get("title", "Title not supplied")
    source_name = metadata.get("source_name", None)
    acl = metadata.get("acl", ["public"])
    test = metadata.get("test", False) or CONFIG["DEFAULT_TEST_FLAG"]

    if not source_name and not title:
        return (jsonify({
            "success": False,
            "error": "Either title or source_name is required"
            }), 400)
    # "new" source_id for new submissions
    try:
        new_source_info = utils.make_source_id(source_name or title, test=test)
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": repr(e)
        }), 500)
    new_source_id = new_source_info["source_id"]
    new_status_info = utils.read_status(new_source_id)
    # Get "old" source_id for current/previous submission
    scan_res = utils.scan_status(fields="source_id",
                                 filters=[("source_id", "^", new_source_info["source_name"]),
                                          ("source_id", "!=", new_source_id)])
    if not scan_res["success"]:
        return (jsonify({
            "success": False,
            "error": "Unable to scan status database: {}".format(scan_res["error"])
        }), 500)
    # max() works exactly the right way on strings for this case
    if scan_res["results"]:
        old_source_id = max([sub["source_id"] for sub in scan_res["results"]])
    else:
        old_source_id = ""

    # Submissions from Connect will have status entries, user submission will not
    if CONFIG["API_CLIENT_ID"] in identities:
        old_status_info = utils.read_status(old_source_id)
        if not old_status_info["success"]:
            logger.error(("Prior status '{}' not in status database: "
                          "{}").format(old_source_id, old_status_info["error"]))
            return (jsonify({
                "success": False,
                "error": "Prior submission '{}' not found in database".format(old_source_id)
                }), 500)
        old_status = old_status_info["status"]
        # Check if past submission is active
        if old_status["active"]:
            # Check old status validity
            status_valid = utils.validate_status(old_status, code_mode="handoff")
            if not status_valid["success"]:
                logger.error("Prior status from database invalid: {}".format(
                                                                        status_valid["error"]))
                return (jsonify(status_valid), 500)

            # Correct version is "old" version
            source_id = old_source_id
            stat_res = utils.update_status(source_id, "ingest_start", "P", except_on_fail=False)
            if not stat_res["success"]:
                logger.error("Status update failure: {}".format(stat_res["error"]))
                return (jsonify(stat_res), 500)

        # Past submission complete, try "new" version
        elif new_status_info["success"]:
            new_status = new_status_info["status"]
            # Check new status validity
            status_valid = utils.validate_status(new_status, code_mode="handoff")
            if not status_valid["success"]:
                logger.error("New status invalid: {}".format(status_valid["error"]))
                return (jsonify(status_valid), 500)

            # Correct version is "new" version
            source_id = new_source_id
            stat_res = utils.update_status(source_id, "ingest_start", "P", except_on_fail=False)
            if not stat_res["success"]:
                logger.error("Status update failure: {}".format(stat_res["error"]))
                return (jsonify(stat_res), 500)
        else:
            logger.error("Current status '{}' not in status database: {}".format(old_source_id,
                                                                                 old_status_info))
            return (jsonify({
                "success": False,
                "error": "Current submission '{}' not found in database".format(old_source_id)
                }), 500)

    # User-submitted, not from Connect
    # Will not have existing status
    else:
        # Verify user is allowed to submit the source_name
        if (len(new_source_info["user_id_list"]) > 0
                and not any([uid in new_source_info["user_id_list"] for uid in identities])):
            return (jsonify({
                "success": False,
                "error": ("Your source_name or title has been submitted previously "
                          "by another user.")
                }), 400)
        # Create new status
        # Correct source_id is "new" always (previous user-submitted source_ids will be cancelled)
        source_id = new_source_id
        status_info = {
            "source_id": source_id,
            "submission_code": "I",
            "submission_time": datetime.utcnow().isoformat("T") + "Z",
            "submitter": name,
            "title": title,
            "acl": acl,
            "user_id": user_id,
            "user_email": email,
            "test": test,
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

    if test:
        services["mdf_search"] = {
            "index": CONFIG["INGEST_TEST_INDEX"]
        }
        if services.get("globus_publish"):
            services["globus_publish"] = {
                "collection_id": CONFIG["TEST_PUBLISH_COLLECTION"]
            }
        if services.get("citrine"):
            services["citrine"] = {
                "public": False
            }
        if services.get("mrr"):
            services["mrr"] = {
                "test": True
            }
    else:
        # Put in defaults
        if services.get("globus_publish") is True:
            services["globus_publish"] = {
                "collection_id": CONFIG["DEFAULT_PUBLISH_COLLECTION"]
            }
        if services.get("citrine") is True:
            services["citrine"] = {
                "public": CONFIG["DEFAULT_CITRINATION_PUBLIC"]
            }
        if services.get("mrr") is True:
            services["mrr"] = {
                "test": CONFIG["DEFAULT_MRR_TEST"]
            }

    try:
        submission_args = {
            "submission_type": "ingest",
            "feedstock_location": feed_location,
            "source_id": source_id,
            "services": services,
            "data_loc": data_loc,
            "service_loc": service_data,
            "access_token": access_token,
            "user_id": user_id
        }
        sub_res = utils.submit_to_queue(submission_args)
        if not sub_res["success"]:
            logger.error("Submission to SQS error: {}".format(sub_res["error"]))
            return (jsonify(sub_res), 500)
    except Exception as e:
        logger.error("Submission to SQS exception: {}".format(e))
        stat_res = utils.update_status(source_id, "ingest_start", "F", text=repr(e),
                                       except_on_fail=False)
        if not stat_res["success"]:
            return (jsonify(stat_res), 500)
        else:
            return (jsonify({
                "success": False,
                "error": repr(e)
                }), 500)

    logger.info("Ingest submission '{}' accepted".format(source_id))
    return (jsonify({
        "success": True,
        "source_id": source_id
        }), 202)


@app.route("/status/<source_id>", methods=["GET"])
def get_status(source_id):
    """Fetch and return status information"""
    # User auth
    try:
        auth_res = utils.authenticate_token(request.headers.get("Authorization"),
                                            auth_level="convert")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)
    # Admin auth (allowed to fail)
    try:
        admin_res = utils.authenticate_token(request.headers.get("Authorization"),
                                             auth_level="admin")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)

    raw_status = utils.read_status(source_id)
    # Failure message if status not fetched or user not allowed to view
    # Only the submitter, ACL users, and admins can view

    # If actually not found
    if (not raw_status["success"]
        # or dataset not public
        or (raw_status["status"]["acl"] != ["public"]
            # and user was not submitter
            and raw_status["status"]["user_id"] not in auth_res["identities_set"]
            # and user is not in ACL
            and not any([uid in raw_status["status"]["acl"] for uid in auth_res["identities_set"]])
            # and user is not admin
            and not admin_res["success"])):
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
        auth_res = utils.authenticate_token(request.headers.get("Authorization"),
                                            auth_level="convert")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)
    # Admin auth (allowed to fail)
    try:
        admin_res = utils.authenticate_token(request.headers.get("Authorization"),
                                             auth_level="admin")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)

    # Users can request only their own submissions (by user_id or by default)
    # Admins can request any user's submissions
    if not (admin_res["success"] or user_id is None or user_id in auth_res["identities_set"]):
        return (jsonify({
            "success": False,
            "error": "You are not authorized to view that submission's status"
            }), 403)

    # Create scan filter
    # Admins can request a special function instead of a user ID
    if admin_res["success"] and user_id == "all":
        filters = None
    elif admin_res["success"] and user_id == "active":
        filters = [("active", "==", True)]
    elif user_id is None:
        filters = [("user_id", "in", auth_res["identities_set"])]
    else:
        filters = [("user_id", "==", user_id)]
    scan_res = utils.scan_status(filters=filters)

    # Error message if no submissions
    if len(scan_res["results"]) == 0:
        return (jsonify({
            "success": False,
            "error": "No submissions available"
            }), 404)

    return (jsonify({
        "success": True,
        "submissions": [utils.translate_status(sub) for sub in scan_res["results"]]
        }), 200)
