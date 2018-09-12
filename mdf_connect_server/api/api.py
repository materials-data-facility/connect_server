from datetime import datetime
import json
import logging
import os

from flask import Flask, jsonify, redirect, request
import jsonschema

from mdf_connect_server import CONFIG
from mdf_connect_server.utils import (authenticate_token, create_status, make_source_id,
                                      read_status, submit_to_queue, translate_status, 
                                      update_status, validate_status)

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
@app.route('/convert', methods=["GET"])
@app.route('/ingest', methods=["GET"])
def root_call():
    return redirect(CONFIG["FORM_URL"], code=302)


@app.route('/convert', methods=["POST"])
def accept_convert():
    """Accept the JSON metadata and begin the conversion process."""
    logger.debug("Started new convert task")
    access_token = request.headers.get("Authorization")
    try:
        auth_res = authenticate_token(access_token, auth_level="convert")
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
    if not metadata:
        return (jsonify({
            "success": False,
            "error": "POST data empty or not JSON"
            }), 400)

    # Validate input JSON
    # resourceType is always going to be Dataset, don't require from user
    if not metadata.get("dc", {}).get("resourceType"):
        try:
            metadata["dc"]["resourceType"] = {
                "resourceTypeGeneral": "Dataset",
                "resourceType": "Dataset"
            }
        except Exception:
            pass
    with open(os.path.join(CONFIG["SCHEMA_PATH"], "connect_convert.json")) as schema_file:
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

    # test = True if set in metadata or config
    test = metadata.pop("test", False) or CONFIG["DEFAULT_TEST_FLAG"]

    sub_title = metadata["dc"]["titles"][0]["title"]
    source_id_info = make_source_id(
                        metadata.get("mdf", {}).get("source_name") or sub_title, test=test)
    source_id = source_id_info["source_id"]
    source_name = source_id_info["source_name"]
    if (len(source_id_info["user_id_list"]) > 0
            and not any([uid in source_id_info["user_id_list"] for uid in identities])):
        return (jsonify({
            "success": False,
            "error": ("Your source_name or title has been submitted previously "
                      "by another user.")
            }), 400)
    if not metadata.get("mdf"):
        metadata["mdf"] = {}
    metadata["mdf"]["source_id"] = source_id
    metadata["mdf"]["source_name"] = source_name
    metadata["mdf"]["version"] = source_id_info["version"]
    if not metadata["mdf"].get("acl"):
        metadata["mdf"]["acl"] = ["public"]

    # If the user has set a non-test Publish collection, verify user is in correct group
    if not test and isinstance(metadata.get("services", {}).get("globus_publish"), dict):
        collection = str(metadata["services"]["globus_publish"].get("collection_id")
                         or metadata["services"]["globus_publish"].get("collection_name", ""))
        # Make sure collection is in PUBLISH_COLLECTIONS, and grab the info
        if collection not in CONFIG["PUBLISH_COLLECTIONS"].keys():
            collection = [col_val for col_val in CONFIG["PUBLISH_COLLECTIONS"].values()
                          if col_val["name"].strip().lower() == collection.strip().lower()]
            if len(collection) == 0:
                return (jsonify({
                    "success": False,
                    "error": ("Submission to Globus Publish collection '{}' "
                              "is not supported.").format(collection)
                    }), 400)
            elif len(collection) > 1:
                return (jsonify({
                    "success": False,
                    "error": "Globus Publish collection {} is not unique.".format(collection)
                    }), 400)
            else:
                collection = collection[0]
        else:
            collection = CONFIG["PUBLISH_COLLECTIONS"][collection]
        try:
            auth_res = authenticate_token(request.headers.get("Authorization"),
                                          auth_level=collection["group"])
        except Exception as e:
            logger.error("Group authentication failure: {}".format(e))
            return (jsonify({
                "success": False,
                "error": "Group authentication failed"
                }), 500)
        if not auth_res["success"]:
            error_code = auth_res.pop("error_code")
            return (jsonify(auth_res), error_code)

    status_info = {
        "source_id": source_id,
        "submission_code": "C",
        "submission_time": datetime.utcnow().isoformat("T") + "Z",
        "submitter": name,
        "title": sub_title,
        "user_id": user_id,
        "user_email": email,
        "acl": metadata["mdf"]["acl"],
        "test": test
        }
    try:
        status_res = create_status(status_info)
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
            "submission_type": "convert",
            "metadata": metadata,
            "source_id": source_id,
            "test": test,
            "access_token": access_token,
            "user_id": user_id
        }
        sub_res = submit_to_queue(submission_args)
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
def accept_ingest():
    """Accept the JSON feedstock file and begin the ingestion process."""
    logger.debug("Started new ingest task")
    access_token = request.headers.get("Authorization")
    try:
        auth_res = authenticate_token(access_token, auth_level="ingest")
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
    if not metadata:
        return (jsonify({
            "success": False,
            "error": "POST data empty or not JSON"
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
    new_source_info = make_source_id(source_name or title, test=test)
    new_source_id = new_source_info["source_id"]
    new_status_info = read_status(new_source_id)
    # "old" source_id for current/previous submission
    # Found by decrementing new version, to a minimum of 1
    old_source_id = "{}_v{}".format(new_source_info["source_name"],
                                    max(new_source_info["version"] - 1, 1))

    # Submissions from Connect will have status entries, user submission will not
    if CONFIG["API_CLIENT_ID"] in identities:
        old_status_info = read_status(old_source_id)
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
            status_valid = validate_status(old_status, code_mode="ingest")
            if not status_valid["success"]:
                logger.error("Prior status from database invalid: {}".format(
                                                                        status_valid["error"]))
                return (jsonify(status_valid), 500)

            # Correct version is "old" version
            source_id = old_source_id
            stat_res = update_status(source_id, "ingest_start", "P", except_on_fail=False)
            if not stat_res["success"]:
                logger.error("Status update failure: {}".format(stat_res["error"]))
                return (jsonify(stat_res), 500)

        # Past submission complete, try "new" version
        elif new_status_info["success"]:
            new_status = new_status_info["status"]
            # Check new status validity
            status_valid = validate_status(new_status, code_mode="ingest")
            if not status_valid["success"]:
                logger.error("New status invalid: {}".format(status_valid["error"]))
                return (jsonify(status_valid), 500)

            # Correct version is "new" version
            source_id = new_source_id
            stat_res = update_status(source_id, "ingest_start", "P", except_on_fail=False)
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
            "test": test
            }
        try:
            status_res = create_status(status_info)
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
        sub_res = submit_to_queue(submission_args)
        if not sub_res["success"]:
            logger.error("Submission to SQS error: {}".format(sub_res["error"]))
            return (jsonify(sub_res), 500)
    except Exception as e:
        logger.error("Submission to SQS exception: {}".format(e))
        stat_res = update_status(source_id, "ingest_start", "F", text=repr(e),
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
    try:
        auth_res = authenticate_token(request.headers.get("Authorization"), auth_level="convert")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)
    if not auth_res["success"]:
        error_code = auth_res.pop("error_code")
        return (jsonify(auth_res), error_code)

    uid_set = auth_res["identities_set"]
    raw_status = read_status(source_id)
    # Failure message if status not fetched or user not allowed to view
    # Only the submitter, ACL users, and admins can view
    try:
        admin_res = authenticate_token(request.headers.get("Authorization"), auth_level="admin")
    except Exception as e:
        logger.error("Authentication failure: {}".format(e))
        return (jsonify({
            "success": False,
            "error": "Authentication failed"
            }), 500)
    # If actually not found
    if (not raw_status["success"]
        # or dataset not public
        or (raw_status["status"]["acl"] != ["public"]
            # and user was not submitter
            and raw_status["status"]["user_id"] not in uid_set
            # and user is not in ACL
            and not any([uid in raw_status["status"]["acl"] for uid in uid_set])
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
        return (jsonify(translate_status(raw_status["status"])), 200)
