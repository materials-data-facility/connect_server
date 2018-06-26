from datetime import datetime
import json
import logging
import os
import tempfile
from threading import Thread
import urllib

from bson import ObjectId
from flask import jsonify, request, redirect
import jsonschema
import mdf_toolbox
import requests
from werkzeug.utils import secure_filename

from mdf_connect import (app, convert, search_ingest, update_search_entry,
                         authenticate_token, make_source_id, download_and_backup,
                         globus_publish_data, citrine_upload, cancel_submission,
                         complete_submission, read_status, create_status,
                         update_status, modify_status_entry, translate_status)


# Set up root logger
logger = logging.getLogger("mdf_connect")
logger.setLevel(app.config["LOG_LEVEL"])
logger.propagate = False
# Set up formatters
logfile_formatter = logging.Formatter("[{asctime}] [{levelname}] {name}: {message}",
                                      style='{',
                                      datefmt="%Y-%m-%d %H:%M:%S")
# Set up handlers
logfile_handler = logging.FileHandler(app.config["LOG_FILE"], mode='a')
logfile_handler.setFormatter(logfile_formatter)

logger.addHandler(logfile_handler)

logger.info("\n\n==========Connect service started==========\n")


# Redirect root requests and GETs to the web form
@app.route('/', methods=["GET", "POST"])
@app.route('/convert', methods=["GET"])
@app.route('/ingest', methods=["GET"])
def root_call():
    return redirect(app.config["FORM_URL"], code=302)


@app.route('/convert', methods=["POST"])
def accept_convert():
    """Accept the JSON metadata and begin the conversion process."""
    try:
        auth_res = authenticate_token(request.headers.get("Authorization"), auth_level="convert")
    except Exception as e:
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
    schema_dir = os.path.join(os.path.dirname(__file__), "schemas")
    with open(os.path.join(schema_dir, "connect_convert.json")) as schema_file:
        schema = json.load(schema_file)
    resolver = jsonschema.RefResolver(base_uri="file://{}/".format(schema_dir),
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
    test = metadata.pop("test", False) or app.config["DEFAULT_TEST_FLAG"]

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
        if collection not in app.config["PUBLISH_COLLECTIONS"].keys():
            collection = [col_val for col_val in app.config["PUBLISH_COLLECTIONS"].values()
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
            collection = app.config["PUBLISH_COLLECTIONS"][collection]
        try:
            auth_res = authenticate_token(request.headers.get("Authorization"),
                                          auth_level=collection["group"])
        except Exception as e:
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
        return (jsonify({
            "success": False,
            "error": repr(e)
            }), 500)
    if not status_res["success"]:
        return (jsonify(status_res), 500)

    driver = Thread(target=convert_driver, name="driver_thread", args=(metadata,
                                                                       source_id,
                                                                       test))
    driver.start()
    return (jsonify({
        "success": True,
        "source_id": source_id
        }), 202)


def convert_driver(metadata, source_id, test):
    """The driver function for MOC.
    Modifies the status database as steps are completed.

    Arguments:
    metadata (dict): The JSON passed to /convert.
    source_id (str): The source name of this submission.
    """
    # Setup
    update_status(source_id, "convert_start", "P", except_on_fail=True)
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["transfer", "connect"]
        }
    try:
        clients = mdf_toolbox.confidential_login(creds)
        transfer_client = clients["transfer"]
        connect_authorizer = clients["connect"]
    except Exception as e:
        update_status(source_id, "convert_start", "F", text=repr(e), except_on_fail=True)
        return

    # Cancel the previous version(s)
    vers = metadata["mdf"]["version"]
    old_source_id = source_id
    while vers > 1:
        old_source_id = old_source_id.replace("_v"+str(vers), "_v"+str(vers-1))
        cancel_res = cancel_submission(old_source_id, wait=True)
        if not cancel_res["stopped"]:
            update_status(source_id, "convert_start", "F",
                          text=cancel_res.get("error",
                                              ("Unable to cancel previous "
                                               "submission '{}'").format(old_source_id)),
                          except_on_fail=True)
            return
        if cancel_res["success"]:
            logger.info("{}: Cancelled source_id {}".format(source_id, old_source_id))
        else:
            logger.debug("{}: Stopped source_id {}".format(source_id, old_source_id))
        vers -= 1

    update_status(source_id, "convert_start", "S", except_on_fail=True)

    # Download data locally, back up on MDF resources
    # NOTE: Cancellation point
    if read_status(source_id).get("status", {}).get("cancelled"):
        complete_submission(source_id)
        return
    update_status(source_id, "convert_download", "P", except_on_fail=True)
    local_path = os.path.join(app.config["LOCAL_PATH"], source_id) + "/"
    backup_path = os.path.join(app.config["BACKUP_PATH"], source_id) + "/"
    try:
        for dl_res in download_and_backup(transfer_client,
                                          metadata.pop("data", {}),
                                          app.config["LOCAL_EP"],
                                          local_path,
                                          app.config["BACKUP_EP"] if not test else None,
                                          backup_path if not test else None):
            if not dl_res["success"]:
                update_status(source_id, "convert_download", "T",
                                         text=dl_res["error"], except_on_fail=True)
    except Exception as e:
        update_status(source_id, "convert_download", "F", text=repr(e),
                                 except_on_fail=True)
        return
    if not dl_res["success"]:
        update_status(source_id, "convert_download", "F", text=dl_res["error"],
                                 except_on_fail=True)
        return
    else:
        update_status(source_id, "convert_download", "S", except_on_fail=True)
        logger.info("{}: Data downloaded, {} archives extracted".format(
                                                                    source_id,
                                                                    dl_res["num_extracted"]))

    # Handle service integration data directory
    service_data = os.path.join(app.config["SERVICE_DATA"], source_id) + "/"
    os.makedirs(service_data)

    # Pull out special fields in metadata (the rest is the dataset)
    services = metadata.pop("services", {})
    parse_params = metadata.pop("index", {})
    # metadata should have data location
    metadata["data"] = {
        "endpoint_path": "globus://{}{}".format(app.config["BACKUP_EP"], backup_path),
        "link": app.config["TRANSFER_WEB_APP_LINK"].format(app.config["BACKUP_EP"],
                                                           urllib.parse.quote(backup_path))
    }
    # Add file info data
    parse_params["file"] = {
        "globus_endpoint": app.config["BACKUP_EP"],
        "http_host": app.config["BACKUP_HOST"],
        "local_path": local_path,
        "host_path": backup_path
    }
    convert_params = {
        "dataset": metadata,
        "parsers": parse_params,
        "service_data": service_data
    }

    # NOTE: Cancellation point
    if read_status(source_id).get("status", {}).get("cancelled"):
        complete_submission(source_id)
        return

    # Convert data
    update_status(source_id, "converting", "P", except_on_fail=True)
    try:
        feedstock, num_groups = convert(local_path, convert_params)
    except Exception as e:
        update_status(source_id, "converting", "F", text=repr(e), except_on_fail=True)
        return
    else:
        # feedstock minus dataset entry is records
        num_parsed = len(feedstock) - 1
        # If nothing in feedstock, panic
        if num_parsed < 0:
            update_status(source_id, "converting", "F",
                                     text="Could not parse dataset entry", except_on_fail=True)
            return
        # If no records, warn user
        elif num_parsed == 0:
            update_status(source_id, "converting", "U",
                                     text=("No records were parsed out of {} groups"
                                           .format(num_groups)), except_on_fail=True)
        else:
            update_status(source_id, "converting", "M",
                                     text=("{} records parsed out of {} groups"
                                           .format(num_parsed, num_groups)), except_on_fail=True)
        logger.debug("{}: {} entries parsed".format(source_id, len(feedstock)))

    # NOTE: Cancellation point
    if read_status(source_id).get("status", {}).get("cancelled"):
        complete_submission(source_id)
        return

    # Pass dataset to /ingest
    update_status(source_id, "convert_ingest", "P", except_on_fail=True)
    try:
        with tempfile.TemporaryFile(mode="w+") as stock:
            for entry in feedstock:
                json.dump(entry, stock)
                stock.write("\n")
            stock.seek(0)
            ingest_args = {
                "source_id": source_id,
                "data": json.dumps(["globus://" + app.config["LOCAL_EP"] + local_path]),
                "services": json.dumps(services),
                "service_data": json.dumps(["globus://" + app.config["LOCAL_EP"] + service_data]),
                "test": json.dumps(test)
            }
            headers = {}
            connect_authorizer.set_authorization_header(headers)
            ingest_res = requests.post(app.config["INGEST_URL"],
                                       data=ingest_args,
                                       files={'file': stock},
                                       headers=headers,
                                       # TODO: Verify after getting real cert
                                       verify=False)
    except Exception as e:
        update_status(source_id, "convert_ingest", "F", text=repr(e),
                                 except_on_fail=True)
        return
    else:
        if ingest_res.json().get("success"):
            update_status(source_id, "convert_ingest", "S", except_on_fail=True)
        else:
            update_status(source_id, "convert_ingest", "F",
                                     text=str(ingest_res.json()), except_on_fail=True)
            return

    return {
        "success": True,
        "source_id": source_id
        }


@app.route("/ingest", methods=["POST"])
def accept_ingest():
    """Accept the JSON feedstock file and begin the ingestion process."""
    try:
        auth_res = authenticate_token(request.headers.get("Authorization"), auth_level="ingest")
    except Exception as e:
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

    # Check that file exists and is valid
    try:
        feedstock = request.files["file"]
    except KeyError:
        return (jsonify({
            "success": False,
            "error": "No feedstock file uploaded"
            }), 400)
    # Get parameters
    try:
        # requests.form is an ImmutableMultiDict
        # flat=False returns all keys as lists
        params = request.form.to_dict(flat=False)
        services = json.loads(params.get("services", ["{}"])[0])
        data_loc = json.loads(params.get("data", ["{}"])[0])
        service_data = json.loads(params.get("service_data", ["{}"])[0])
        source_id = params.get("source_id", [None])[0]
        test = json.loads(params.get("test", ["false"])[0])
    except KeyError as e:
        return (jsonify({
            "success": False,
            "error": "Parameters missing: " + repr(e)
            }), 400)
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": "Invalid ingest JSON: " + repr(e)
            }), 400)

    # Mint or update status ID
    if source_id:
        # TODO: Verify source_id ownership
        stat_res = update_status(source_id, "ingest_start", "P", except_on_fail=False)
        if not stat_res["success"]:
            return (jsonify(stat_res), 400)
    else:
        # TODO: Fetch real source_id/title instead of minting ObjectId
        title = "ingested_{}".format(str(ObjectId()))
        source_id_info = make_source_id(title)
        source_id = source_id_info["source_id"]
        status_info = {
            "source_id": source_id,
            "submission_code": "I",
            "submission_time": datetime.utcnow().isoformat("T") + "Z",
            "submitter": name,
            "title": title,
            "user_id": user_id,
            "user_email": email,
            "test": test
            }
        try:
            # TODO: Better metadata validation
            status_res = create_status(status_info)
        except Exception as e:
            return (jsonify({
                "success": False,
                "error": repr(e)
                }), 500)
        if not status_res["success"]:
            return (jsonify(status_res), 500)

    if test:
        services["mdf_search"] = {
            "index": app.config["INGEST_TEST_INDEX"]
        }
        if services.get("globus_publish"):
            services["globus_publish"] = {
                "collection_id": app.config["TEST_PUBLISH_COLLECTION"]
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
                "collection_id": app.config["DEFAULT_PUBLISH_COLLECTION"]
            }
        if services.get("citrine") is True:
            services["citrine"] = {
                "public": app.config["DEFAULT_CITRINATION_PUBLIC"]
            }
        if services.get("mrr") is True:
            services["mrr"] = {
                "test": app.config["DEFAULT_MRR_TEST"]
            }

    # Save file
    try:
        feed_path = os.path.join(app.config["FEEDSTOCK_PATH"],
                                 secure_filename(feedstock.filename))
        feedstock.save(feed_path)
        ingester = Thread(target=ingest_driver, name="ingester_thread", args=(feed_path,
                                                                              source_id,
                                                                              services,
                                                                              data_loc,
                                                                              service_data))
    except Exception as e:
        stat_res = update_status(source_id, "ingest_start", "F", text=repr(e),
                                 except_on_fail=False)
        if not stat_res["success"]:
            return (jsonify(stat_res), 500)
        else:
            return (jsonify({
                "success": False,
                "error": repr(e)
                }), 400)
    ingester.start()
    return (jsonify({
        "success": True,
        "source_id": source_id
        }), 202)


def ingest_driver(base_feed_path, source_id, services, data_loc, service_loc):
    """Finalize and ingest feedstock."""
    # Will need client to ingest data
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["search_ingest", "publish", "transfer"]
        }
    try:
        clients = mdf_toolbox.confidential_login(creds)
        publish_client = clients["publish"]
        transfer_client = clients["transfer"]

        final_feed_path = os.path.join(app.config["FEEDSTOCK_PATH"], source_id + "_final.json")
    except Exception as e:
        update_status(source_id, "ingest_start", "F", text=repr(e), except_on_fail=True)
        return

    # Cancel the previous version(s)
    try:
        vers = int(source_id.rsplit("_v", 1)[1])
    except Exception as e:
        update_status(source_id, "ingest_start", "F", text="Invalid source_id: " + source_id,
                      except_on_fail=True)
    old_source_id = source_id
    while vers > 1:
        old_source_id = old_source_id.replace("_v"+str(vers), "_v"+str(vers-1))
        cancel_res = cancel_submission(old_source_id, wait=True)
        if not cancel_res["stopped"]:
            update_status(source_id, "convert_start", "F",
                          text=cancel_res.get("error",
                                              ("Unable to cancel previous "
                                               "submission '{}'").format(old_source_id)),
                          except_on_fail=True)
            return
        if cancel_res["success"]:
            logger.info("{}: Cancelled source_id {}".format(source_id, old_source_id))
        else:
            logger.debug("{}: Stopped source_id {}".format(source_id, old_source_id))
        vers -= 1

    update_status(source_id, "ingest_start", "S", except_on_fail=True)

    # NOTE: Cancellation point
    if read_status(source_id).get("status", {}).get("cancelled"):
        complete_submission(source_id)
        return

    # If the data should be local, make sure it is
    # Currently only Publish needs the data
    if services.get("globus_publish"):
        if not data_loc:
            update_status(source_id, "ingest_download", "F",
                                     text=("Globus Publish integration was selected, "
                                           "but the data location was not provided."),
                                     except_on_fail=True)
            update_status(source_id, "ingest_publish", "F",
                                     text="Unable to publish data without location.",
                                     except_on_fail=True)
            return
        else:
            # If all locations are Globus, don't need to download locally
            if all([loc.startswith("globus://") for loc in data_loc]):
                update_status(source_id, "ingest_download", "N", except_on_fail=True)
                data_ep = None
                data_path = None
            else:
                update_status(source_id, "ingest_download", "P", except_on_fail=True)
                # Will not transfer anything if already in place
                data_ep = app.config["LOCAL_EP"]
                data_path = os.path.join(app.config["LOCAL_PATH"], source_id) + "/"
                try:
                    for dl_res in download_and_backup(transfer_client,
                                                      data_loc,
                                                      data_ep,
                                                      data_path):
                        if not dl_res["success"]:
                            update_status(source_id, "ingest_download", "T",
                                                     text=dl_res["error"], except_on_fail=True)
                except Exception as e:
                    update_status(source_id, "ingest_download", "F", text=repr(e),
                                             except_on_fail=True)
                    return
                if not dl_res["success"]:
                    update_status(source_id, "ingest_download", "F",
                                             text=dl_res["error"], except_on_fail=True)
                    return
                else:
                    update_status(source_id, "ingest_download", "S",
                                             except_on_fail=True)
                    logger.debug("{}: Ingest data downloaded".format(source_id))
    else:
        update_status(source_id, "ingest_download", "N", except_on_fail=True)

    # Same for integrated service data
    if services.get("citrine"):
        if not service_loc:
            update_status(source_id, "ingest_integration", "F",
                                     text=("Citrine integration was selected, but the"
                                           "integration data location was not provided."),
                                     except_on_fail=True)
            update_status(source_id, "ingest_citrine", "F",
                                     text="Unable to upload PIFs without location.",
                                     except_on_fail=True)
            return
        else:
            update_status(source_id, "ingest_integration", "P", except_on_fail=True)
            # Will not transfer anything if already in place
            service_data = os.path.join(app.config["SERVICE_DATA"], source_id) + "/"
            try:
                for dl_res in download_and_backup(transfer_client,
                                                  service_loc,
                                                  app.config["LOCAL_EP"],
                                                  service_data):
                    if not dl_res["success"]:
                        update_status(source_id, "ingest_integration", "T",
                                                 text=dl_res["error"], except_on_fail=True)
            except Exception as e:
                update_status(source_id, "ingest_integration", "F", text=repr(e),
                                         except_on_fail=True)
                return
            if not dl_res["success"]:
                update_status(source_id, "ingest_integration", "F",
                                         text=dl_res["error"], except_on_fail=True)
                return
            else:
                update_status(source_id, "ingest_integration", "S", except_on_fail=True)
                logger.debug("{}: Integration data downloaded".format(source_id))
    else:
        update_status(source_id, "ingest_integration", "N", except_on_fail=True)

    # Integrations
    service_res = {}

    # NOTE: Cancellation point
    if read_status(source_id).get("status", {}).get("cancelled"):
        complete_submission(source_id)
        return

    # MDF Search (mandatory)
    update_status(source_id, "ingest_search", "P", except_on_fail=True)
    search_config = services.get("mdf_search", {})
    try:
        search_res = search_ingest(
                        creds, base_feed_path,
                        index=search_config.get("index", app.config["INGEST_INDEX"]),
                        batch_size=app.config["SEARCH_BATCH_SIZE"],
                        feedstock_save=final_feed_path)
    except Exception as e:
        update_status(source_id, "ingest_search", "F", text=repr(e),
                                 except_on_fail=True)
        return
    else:
        # Handle errors
        if len(search_res["errors"]) > 0:
            update_status(source_id, "ingest_search", "F",
                          text=("{} batches of records failed to ingest ({} records total)"
                                ".").format(len(search_res["errors"]),
                                            (len(search_res["errors"])
                                             * app.config["SEARCH_BATCH_SIZE"]),
                                            search_res["errors"]),
                          except_on_fail=True)
            return

        # Other services use the dataset information
        with open(final_feed_path) as f:
            dataset = json.loads(f.readline())
        # Back up feedstock
        backup_feed_path = os.path.join(app.config["BACKUP_FEEDSTOCK"],
                                        source_id + "_final.json")
        try:
            transfer = mdf_toolbox.custom_transfer(
                            transfer_client, app.config["LOCAL_EP"], app.config["BACKUP_EP"],
                            [(final_feed_path, backup_feed_path)],
                            interval=app.config["TRANSFER_PING_INTERVAL"],
                            inactivity_time=app.config["TRANSFER_DEADLINE"])
            for event in transfer:
                if not event["success"]:
                    logger.debug(event)
            if not event["success"]:
                raise ValueError(event["code"]+": "+event["description"])
        except Exception as e:
            update_status(source_id, "ingest_search", "R",
                                     text="Feedstock backup failed: {}".format(str(e)),
                                     except_on_fail=True)
        else:
            update_status(source_id, "ingest_search", "S", except_on_fail=True)
            os.remove(final_feed_path)
        service_res["mdf_search"] = "This dataset was ingested to MDF Search."

    # Globus Publish
    if services.get("globus_publish"):
        update_status(source_id, "ingest_publish", "P", except_on_fail=True)
        # collection should be in id or name
        collection = (services["globus_publish"].get("collection_id")
                      or services["globus_publish"].get("collection_name")
                      or app.config["DEFAULT_PUBLISH_COLLECTION"])
        try:
            fin_res = globus_publish_data(publish_client, transfer_client,
                                          dataset, collection,
                                          data_ep, data_path, data_loc)
        except Exception as e:
            update_status(source_id, "ingest_publish", "R", text=repr(e),
                                     except_on_fail=True)
        else:
            stat_link = app.config["PUBLISH_LINK"].format(fin_res["id"])
            update_status(source_id, "ingest_publish", "L",
                                     text=fin_res["dc.description.provenance"], link=stat_link,
                                     except_on_fail=True)
            service_res["globus_publish"] = stat_link
    else:
        update_status(source_id, "ingest_publish", "N", except_on_fail=True)

    # Citrine
    if services.get("citrine"):
        update_status(source_id, "ingest_citrine", "P", except_on_fail=True)

        # Check if this is a new version
        version = dataset.get("mdf", {}).get("version", 1)
        old_citrine_id = None
        # Get base (no version) source_id by removing _v#
        base_source_id = source_id.rsplit("_v"+str(version), 1)[0]
        # Find the last version uploaded to Citrine, if there was one
        while version > 1 and not old_citrine_id:
            # Get the old source name by adding the old version
            version -= 1
            old_source_id = base_source_id + "_v" + str(version)
            # Get the old version's citrine_id
            old_status = read_status(old_source_id)
            if not old_status["success"]:
                raise ValueError(str(old_status))
            old_citrine_id = old_status["status"].get("citrine_id", None)

        try:
            cit_path = os.path.join(service_data, "citrine")
            cit_res = citrine_upload(cit_path,
                                     app.config["CITRINATION_API_KEY"],
                                     dataset,
                                     old_citrine_id,
                                     public=services["citrine"].get("public", True))
        except Exception as e:
            update_status(source_id, "ingest_citrine", "R", text=repr(e),
                                     except_on_fail=True)
        else:
            if not cit_res["success"]:
                if cit_res.get("error"):
                    text = cit_res["error"]
                elif cit_res.get("failure_count"):
                    text = "All {} PIFs failed to upload".format(cit_res["failure_count"])
                elif cit_res.get("failure_count") == 0:
                    text = "No PIFs were generated"
                else:
                    text = "An error prevented PIF uploading"
                update_status(source_id, "ingest_citrine", "R", text=text,
                                         except_on_fail=True)
            else:
                text = "{}/{} PIFs uploaded successfully".format(cit_res["success_count"],
                                                                 cit_res["success_count"]
                                                                 + cit_res["failure_count"])
                link = app.config["CITRINATION_LINK"].format(cit_ds_id=cit_res["cit_ds_id"])
                update_status(source_id, "ingest_citrine", "L", text=text, link=link,
                                         except_on_fail=True)
                stat_res_2 = modify_status_entry(source_id,
                                                 {"citrine_id": cit_res["cit_ds_id"]})
                if not stat_res_2["success"]:
                    raise ValueError(str(stat_res_2))
                service_res["citrine"] = link
    else:
        update_status(source_id, "ingest_citrine", "N", except_on_fail=True)

    # MRR
    if services.get("mrr"):
        update_status(source_id, "ingest_mrr", "P", except_on_fail=True)
        try:
            if isinstance(services["mrr"], dict) and services["mrr"].get("test"):
                mrr_title = "TEST_" + dataset["dc"]["titles"][0]["title"]
            else:
                mrr_title = dataset["dc"]["titles"][0]["title"]
            mrr_entry = {
                "title": dataset["dc"]["titles"][0]["title"],
                "schema": app.config["MRR_SCHEMA"],
                "content": app.config["MRR_TEMPLATE"].format(
                                title=mrr_title,
                                publisher=dataset["dc"]["publisher"],
                                contributors="".join(
                                    [app.config["MRR_CONTRIBUTOR"].format(
                                        name=author.get("givenName", "") + " "
                                             + author.get("familyName", ""),
                                        affiliation=author.get("affiliation", ""))
                                     for author in dataset["dc"]["creators"]]),
                                contact_name=dataset["dc"]["creators"][0]["creatorName"],
                                description=dataset["dc"].get("description", ""),
                                subject="")
            }
        except Exception as e:
            update_status(source_id, "ingest_mrr", "R",
                                     text="Unable to create MRR metadata:"+repr(e),
                                     except_on_fail=True)
        else:
            try:
                mrr_res = requests.post(app.config["MRR_URL"],
                                        auth=(app.config["MRR_USERNAME"],
                                              app.config["MRR_PASSWORD"]),
                                        data=mrr_entry).json()
            except Exception as e:
                update_status(source_id, "ingest_mrr", "F",
                                         text="Unable to submit MRR entry:"+repr(e),
                                         except_on_fail=True)
            else:
                if mrr_res.get("_id"):
                    update_status(source_id, "ingest_mrr", "S", except_on_fail=True)
                    service_res["mrr"] = "This dataset was registered with the MRR."
                else:
                    update_status(source_id, "ingest_mrr", "R",
                                             text=mrr_res.get("message", "Unknown failure"),
                                             except_on_fail=True)
    else:
        update_status(source_id, "ingest_mrr", "N", except_on_fail=True)

    # Dataset update, start cleanup
    update_status(source_id, "ingest_cleanup", "P", except_on_fail=True)

    dataset["services"] = service_res
    ds_update = update_search_entry(creds,
                                    index=search_config.get("index", app.config["INGEST_INDEX"]),
                                    updated_entry=dataset, overwrite=False)
    if not ds_update["success"]:
        update_status(source_id, "ingest_cleanup", "F",
                                 text=ds_update.get("error", "Unable to update dataset"),
                                 except_on_fail=True)
        return

    # Cleanup
    try:
        fin_res = complete_submission(source_id, cleanup=True)
    except Exception as e:
        update_status(source_id, "ingest_cleanup", "F", text=repr(e), except_on_fail=True)
        return
    if not fin_res["success"]:
        update_status(source_id, "ingest_cleanup", "F", text=fin_res["error"], except_on_fail=True)
        return
    update_status(source_id, "ingest_cleanup", "S", except_on_fail=True)

    logger.debug("{}: Ingest complete".format(source_id))
    return {
        "success": True,
        "source_id": source_id
        }


@app.route("/status/<source_id>", methods=["GET"])
def get_status(source_id):
    """Fetch and return status information"""
    try:
        auth_res = authenticate_token(request.headers.get("Authorization"), auth_level="convert")
    except Exception as e:
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
