from datetime import datetime
import json
import logging
import os

from flask import Flask, jsonify, redirect, request
from globus_nexus_client import NexusClient
import globus_sdk
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
        auth_res = utils.authenticate_token(access_token, "convert")
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
        "test": metadata.pop("test", False),
        "update": metadata.pop("update", False),
        "acl": metadata.get("mdf", {}).get("acl", ["public"]),
        "index": metadata.pop("index", {}),
        "services": metadata.pop("services", {}),
        "conversion_config": metadata.pop("conversion_config", {}),
        "no_convert": metadata.pop("no_convert", False),  # Pass-through flag
        "submitter": name
    }

    # Create source_name
    sub_title = metadata["dc"]["titles"][0]["title"]
    try:
        # author_name is first author familyName, first author creatorName,
        # or submitter
        author_name = metadata["dc"]["creators"][0].get(
                            "familyName", metadata["dc"]["creators"][0].get("creatorName", name))
        source_id_info = utils.make_source_id(
                                metadata.get("mdf", {}).get("source_name") or sub_title,
                                author_name, test=sub_conf["test"])
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

    # Get organization rules to apply
    if metadata["mdf"].get("organizations"):
        metadata["mdf"]["organizations"], sub_conf = \
            utils.fetch_org_rules(metadata["mdf"]["organizations"], sub_conf)
    # Check that user is in appropriate org group(s), if applicable
    if sub_conf.get("permission_groups"):
        for group_uuid in sub_conf["permission_groups"]:
            try:
                group_res = utils.authenticate_token(access_token, group_uuid, require_all=True)
            except Exception as e:
                logger.error("Authentication failure: {}".format(repr(e)))
                return (jsonify({
                    "success": False,
                    "error": "Authentication failed"
                }), 500)
            if not group_res["success"]:
                error_code = group_res.pop("error_code")
                return (jsonify(group_res), error_code)
        # Also allow permission group members to see submission
        sub_conf["acl"].extend(sub_conf["permission_groups"])

    # If ACL includes "public", no other entries needed
    if "public" in sub_conf["acl"]:
        sub_conf["acl"] = ["public"]
    # Set correct ACL in metadata
    metadata["mdf"]["acl"] = sub_conf["acl"]

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
    # Otherwise (not Publishing), canon destination is backup (Petrel)
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


@app.route("/status/<source_id>", methods=["GET"])
def get_status(source_id):
    """Fetch and return status information"""
    # User auth
    try:
        auth_res = utils.authenticate_token(request.headers.get("Authorization"), "convert")
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
        or (raw_status["status"]["acl"] != ["public"]
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
        auth_res = utils.authenticate_token(request.headers.get("Authorization"), "convert")
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

    # TODO: Is it an error for there to be no submissions?
    # A bad user_id would cause that (error), but a new user would also get it (not an error)
    '''
    # Error message if no submissions
    if len(scan_res["results"]) == 0:
        return (jsonify({
            "success": False,
            "error": "No submissions available"
            }), 404)
    '''

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
        auth_res = utils.authenticate_token(access_token, "convert")
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
        auth_res = utils.authenticate_token(access_token, "convert")
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
                    "metadata": {},  # Not used after convert step
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
