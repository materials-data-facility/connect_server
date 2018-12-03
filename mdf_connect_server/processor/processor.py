import json
import logging
from multiprocessing import Process
import os
from time import sleep
import urllib

import globus_sdk
import mdf_toolbox
import requests

from mdf_connect_server import CONFIG, utils
from mdf_connect_server.processor import convert, search_ingest, update_search_entry


# Set up root logger
logger = logging.getLogger("mdf_connect_server")
logger.setLevel(CONFIG["LOG_LEVEL"])
logger.propagate = False
# Set up formatters
logfile_formatter = logging.Formatter("[{asctime}] [{levelname}] {name}: {message}",
                                      style='{',
                                      datefmt="%Y-%m-%d %H:%M:%S")
# Set up handlers
logfile_handler = logging.FileHandler(CONFIG["PROCESS_LOG_FILE"], mode='a')
logfile_handler.setFormatter(logfile_formatter)

logger.addHandler(logfile_handler)


def processor():
    logger.info("\n\n==========Connect Process started==========\n")
    # Write out Processor PID
    with open("pid.log", 'w') as pf:
        pf.write(os.getpid())
    utils.clean_start()
    active_processes = []
    try:
        while True:
            try:
                submissions = utils.retrieve_from_queue(wait_time=CONFIG["PROCESSOR_WAIT_TIME"])
                if not submissions["success"]:
                    logger.debug("Submissions not retrieved: {}".format(submissions["error"]))
                if len(submissions["entries"]):
                    logger.debug("{} submissions retrieved".format(len(submissions["entries"])))
                    for sub in submissions["entries"]:
                        if sub["submission_type"] == "convert":
                            driver = Process(target=convert_driver, kwargs=sub,
                                             name="C"+sub["source_id"])
                        elif sub["submission_type"] == "ingest":
                            driver = Process(target=ingest_driver, kwargs=sub,
                                             name="I"+sub["source_id"])
                        else:
                            raise ValueError(("Submission type '{}' "
                                              "invalid").format(sub["submission_type"]))
                        driver.start()
                        active_processes.append(driver)
                    utils.delete_from_queue(submissions["delete_info"])
                    logger.info("{} submissions started".format(len(submissions["entries"])))
            except Exception as e:
                logger.error("Processor error: {}".format(e))
            try:
                for dead_proc in [proc for proc in active_processes if not proc.is_alive()]:
                    # Convert processes should not be cancelled if they finished
                    # 'converted' is sentinel value for convert finished
                    logger.info("Dead: {} ({})"
                                .format(
                                    dead_proc.name,
                                    utils.read_status(dead_proc.name[1:])["status"]["converted"]))
                    if (dead_proc.name[0] == "C"
                            and utils.read_status(dead_proc.name[1:])["status"]["converted"]):
                        active_processes.remove(dead_proc)
                        logger.debug("{}: Logged dead, not cancelled".format(dead_proc.name))
                    else:
                        cancel_res = utils.cancel_submission(dead_proc.name[1:])
                        if cancel_res["stopped"]:
                            active_processes.remove(dead_proc)
                            logger.debug("{}: Dead and cancelled/cleaned up"
                                         .format(dead_proc.name))
                        else:
                            logger.info(("Unable to cancel process for {}: "
                                         "{}").format(
                                                dead_proc.name,
                                                cancel_res.get("error", "No error provided")))
            except Exception as e:
                logger.error("Error life-checking processes: {}".format(e))
            sleep(CONFIG["PROCESSOR_SLEEP_TIME"])
    # SIGINT turns into KeyboardInterrupt, should exit gracefully
    except KeyboardInterrupt:
        logger.info("Shutting down Connect")
        for proc in active_processes:
            cancel_res = utils.cancel_submission(proc.name[1:])
            if cancel_res["stopped"]:
                logger.debug("{}: Shutdown".format(proc.name))
            else:
                logger.info("Unable to shut down process for {}: {}"
                            .format(proc.name, cancel_res.get("error", "No error provided")))
        logger.info("Connect gracefully shut down")


def convert_driver(submission_type, metadata, source_id, test, access_token, user_id):
    """The driver function for MOC.
    Modifies the status database as steps are completed.

    Arguments:
    submission_type (str): "convert" (used for error-checking).
    metadata (dict): The JSON passed to /convert.
    source_id (str): The source name of this submission.
    test (bool): If the submission is a test submission.
    access_token (str): The Globus Auth access token for the submitting user.
    user_id (str): The Globus ID of the submitting user.
    """
    # TODO: Better check?
    assert submission_type == "convert"
    # Setup
    utils.update_status(source_id, "convert_start", "P", except_on_fail=True)
    utils.modify_status_entry(source_id, {"pid": os.getpid()}, except_on_fail=True)
    try:
        # Connect auth
        mdf_conf_client = globus_sdk.ConfidentialAppAuthClient(CONFIG["API_CLIENT_ID"],
                                                               CONFIG["API_CLIENT_SECRET"])
        mdf_transfer_authorizer = globus_sdk.ClientCredentialsAuthorizer(
                                                mdf_conf_client, scopes=CONFIG["TRANSFER_SCOPE"])
        mdf_transfer_client = globus_sdk.TransferClient(authorizer=mdf_transfer_authorizer)

        # User auth
        access_token = access_token.replace("Bearer ", "")
        dependent_grant = mdf_conf_client.oauth2_get_dependent_tokens(access_token)
        user_transfer_authorizer = globus_sdk.AccessTokenAuthorizer(
                                                dependent_grant.data[0]["access_token"])
        user_transfer_client = globus_sdk.TransferClient(authorizer=user_transfer_authorizer)
    except Exception as e:
        utils.update_status(source_id, "convert_start", "F", text=repr(e), except_on_fail=True)
        utils.complete_submission(source_id)
        return

    # Cancel the previous version(s)
    source_info = utils.split_source_id(source_id)
    scan_res = utils.scan_status(fields="source_id",
                                 filters=[("source_id", "^", source_info["source_name"]),
                                          ("source_id", "<", source_id)])
    if not scan_res["success"]:
        utils.update_status(source_id, "convert_start", "F", text=scan_res["error"],
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return
    for old_source in scan_res["results"]:
        old_source_id = old_source["source_id"]
        cancel_res = utils.cancel_submission(old_source_id, wait=True)
        if not cancel_res["stopped"]:
            utils.update_status(source_id, "convert_start", "F",
                                text=cancel_res.get("error",
                                                    ("Unable to cancel previous "
                                                     "submission '{}'").format(old_source_id)),
                                except_on_fail=True)
            utils.complete_submission(source_id)
            return
        if cancel_res["success"]:
            logger.info("{}: Cancelled source_id {}".format(source_id, old_source_id))
        else:
            logger.debug("{}: Stopped source_id {}".format(source_id, old_source_id))

    utils.update_status(source_id, "convert_start", "S", except_on_fail=True)

    # Download data locally, back up on MDF resources
    # NOTE: Cancellation point
    if utils.read_status(source_id).get("status", {}).get("cancelled"):
        logger.debug("{}: Cancel signal acknowledged".format(source_id))
        utils.complete_submission(source_id)
        return
    utils.update_status(source_id, "convert_download", "P", except_on_fail=True)
    local_path = os.path.join(CONFIG["LOCAL_PATH"], source_id) + "/"
    backup_path = os.path.join(CONFIG["BACKUP_PATH"], source_id) + "/"
    try:
        # Download from user
        for dl_res in utils.download_data(user_transfer_client, metadata.pop("data", []),
                                          CONFIG["LOCAL_EP"], local_path,
                                          admin_client=mdf_transfer_client, user_id=user_id):
            if not dl_res["success"]:
                msg = "During data download: " + dl_res["error"]
                utils.update_status(source_id, "convert_download", "T", text=msg,
                                    except_on_fail=True)
        if not dl_res["success"]:
            raise ValueError(dl_res["error"])

        # Backup to MDF
        if not test:
            backup_res = utils.backup_data(mdf_transfer_client, CONFIG["LOCAL_EP"], local_path,
                                           CONFIG["BACKUP_EP"], backup_path)
            if not backup_res["success"]:
                raise ValueError(backup_res["error"])

    except Exception as e:
        utils.update_status(source_id, "convert_download", "F", text=repr(e), except_on_fail=True)
        utils.complete_submission(source_id)
        return

    utils.update_status(source_id, "convert_download", "M",
                        text=("{} files will be processed "
                              "({} archives extracted)").format(dl_res["total_files"],
                                                                dl_res["num_extracted"]),
                        except_on_fail=True)

    # Handle service integration data directory
    service_data = os.path.join(CONFIG["SERVICE_DATA"], source_id) + "/"
    os.makedirs(service_data)

    # Pull out special fields in metadata (the rest is the dataset)
    services = metadata.pop("services", {})
    parse_params = metadata.pop("index", {})
    # metadata should have data location
    metadata["data"] = {
        "endpoint_path": "globus://{}{}".format(CONFIG["BACKUP_EP"], backup_path),
        "link": CONFIG["TRANSFER_WEB_APP_LINK"].format(CONFIG["BACKUP_EP"],
                                                       urllib.parse.quote(backup_path))
    }
    # Add file info data
    parse_params["file"] = {
        "globus_endpoint": CONFIG["BACKUP_EP"],
        "http_host": CONFIG["BACKUP_HOST"],
        "local_path": local_path,
        "host_path": backup_path
    }
    convert_params = {
        "dataset": metadata,
        "parsers": parse_params,
        "service_data": service_data
    }

    # NOTE: Cancellation point
    if utils.read_status(source_id).get("status", {}).get("cancelled"):
        logger.debug("{}: Cancel signal acknowledged".format(source_id))
        utils.complete_submission(source_id)
        return

    # Convert data
    utils.update_status(source_id, "converting", "P", except_on_fail=True)
    try:
        feedstock, num_groups, extensions = convert(local_path, convert_params)
    except Exception as e:
        utils.update_status(source_id, "converting", "F", text=repr(e), except_on_fail=True)
        utils.complete_submission(source_id)
        return
    else:
        utils.modify_status_entry(source_id, {"extensions": extensions})
        # feedstock minus dataset entry is records
        num_parsed = len(feedstock) - 1
        # If nothing in feedstock, panic
        if num_parsed < 0:
            utils.update_status(source_id, "converting", "F",
                                text="Could not parse dataset entry", except_on_fail=True)
            utils.complete_submission(source_id)
            return
        # If no records, warn user
        elif num_parsed == 0:
            utils.update_status(source_id, "converting", "U",
                                text=("No records were parsed out of {} groups"
                                      .format(num_groups)), except_on_fail=True)
        else:
            utils.update_status(source_id, "converting", "M",
                                text=("{} records parsed out of {} groups"
                                      .format(num_parsed, num_groups)), except_on_fail=True)
        logger.debug("{}: {} entries parsed".format(source_id, len(feedstock)))

    # NOTE: Cancellation point
    if utils.read_status(source_id).get("status", {}).get("cancelled"):
        logger.debug("{}: Cancel signal acknowledged".format(source_id))
        utils.complete_submission(source_id)
        return

    # Pass dataset to /ingest
    utils.update_status(source_id, "convert_ingest", "P", except_on_fail=True)
    try:
        # Write out feedstock
        feed_path = os.path.join(CONFIG["FEEDSTOCK_PATH"], source_id + "_raw.json")
        with open(feed_path, 'w') as stock:
            for entry in feedstock:
                json.dump(entry, stock)
                stock.write("\n")
        ingest_args = {
            "feedstock_location": "globus://{}{}".format(CONFIG["LOCAL_EP"], feed_path),
            "source_name": source_id,
            "data": ["globus://{}{}".format(CONFIG["LOCAL_EP"], local_path)],
            "services": services,
            "service_data": ["globus://{}{}".format(CONFIG["LOCAL_EP"], service_data)],
            "test": test
        }
        headers = {}
        tokens = mdf_conf_client.oauth2_client_credentials_tokens(
                                    requested_scopes=CONFIG["API_SCOPE"])
        connect_authorizer = globus_sdk.AccessTokenAuthorizer(
                                            tokens.by_resource_server
                                            ["mdf_dataset_submission"]["access_token"])
        connect_authorizer.set_authorization_header(headers)
        ingest_res = requests.post(CONFIG["INGEST_URL"],
                                   json=ingest_args,
                                   headers=headers)
    except Exception as e:
        utils.update_status(source_id, "convert_ingest", "F", text=repr(e),
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return
    else:
        if ingest_res.status_code < 300 and ingest_res.json().get("success"):
            utils.update_status(source_id, "convert_ingest", "S", except_on_fail=True)
        else:
            utils.update_status(source_id, "convert_ingest", "F",
                                text=str(ingest_res.content), except_on_fail=True)
            utils.complete_submission(source_id)
            return

    # Set sentinel for convert finished
    utils.modify_status_entry(source_id, {"converted": True}, except_on_fail=True)

    return {
        "success": True,
        "source_id": source_id
        }


def ingest_driver(submission_type, feedstock_location, source_id, services, data_loc,
                  service_loc, access_token, user_id):
    """Finalize and ingest feedstock.

    Arguments:
    submission_type (str): "ingest" (used for error-checking).
    feedstock_location (str or list of str): The location(s) of the MDF-format feedstock.
    source_id (str): The source name of this submission.
    services (dict): The optional services and configurations requested.
    data_loc (str or list of str): The location of the data.
    service_loc (str or list of str): The location of service integration data.
    access_token (str): The Globus Auth access token for the submitting user.
    user_id (str): The Globus ID of the submitting user.
    """
    # TODO: Better check?
    assert submission_type == "ingest"
    utils.modify_status_entry(source_id, {"pid": os.getpid()}, except_on_fail=True)
    utils.update_status(source_id, "ingest_start", "P", except_on_fail=True)
    # Will need client to ingest data
    try:
        clients = mdf_toolbox.confidential_login(
                        mdf_toolbox.dict_merge(
                            CONFIG["GLOBUS_CREDS"],
                            {"services": ["search_ingest", "publish", "transfer"]}))
        publish_client = clients["publish"]
        mdf_transfer_client = clients["transfer"]

        base_feed_path = os.path.join(CONFIG["FEEDSTOCK_PATH"], source_id + "_raw.json")
        final_feed_path = os.path.join(CONFIG["FEEDSTOCK_PATH"], source_id + "_final.json")

        access_token = access_token.replace("Bearer ", "")
        conf_client = globus_sdk.ConfidentialAppAuthClient(CONFIG["API_CLIENT_ID"],
                                                           CONFIG["API_CLIENT_SECRET"])
        dependent_grant = conf_client.oauth2_get_dependent_tokens(access_token)
        user_transfer_authorizer = globus_sdk.AccessTokenAuthorizer(
                                                dependent_grant.data[0]["access_token"])
        user_transfer_client = globus_sdk.TransferClient(authorizer=user_transfer_authorizer)
    except Exception as e:
        utils.update_status(source_id, "ingest_start", "F", text=repr(e), except_on_fail=True)
        utils.complete_submission(source_id)
        return

    # Cancel the previous version(s)
    source_info = utils.split_source_id(source_id)
    if not source_info["success"]:
        utils.update_status(source_id, "ingest_start", "F",
                            text="Invalid source_id: " + source_id, except_on_fail=True)
    scan_res = utils.scan_status(fields="source_id",
                                 filters=[("source_id", "^", source_info["source_name"]),
                                          ("source_id", "!=", source_id)])
    if not scan_res["success"]:
        utils.update_status(source_id, "ingest_start", "F", text=scan_res["error"],
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return
    for old_source in scan_res["results"]:
        old_source_id = old_source["source_id"]
        cancel_res = utils.cancel_submission(old_source_id, wait=True)
        if not cancel_res["stopped"]:
            utils.update_status(source_id, "ingest_start", "F",
                                text=cancel_res.get("error",
                                                    ("Unable to cancel previous "
                                                     "submission '{}'").format(old_source_id)),
                                except_on_fail=True)
            utils.complete_submission(source_id)
            return
        if cancel_res["success"]:
            logger.info("{}: Cancelled source_id {}".format(source_id, old_source_id))
        else:
            logger.debug("{}: Stopped source_id {}".format(source_id, old_source_id))

    utils.update_status(source_id, "ingest_start", "S", except_on_fail=True)
    utils.modify_status_entry(source_id, {"active": True}, except_on_fail=True)

    # NOTE: Cancellation point
    if utils.read_status(source_id).get("status", {}).get("cancelled"):
        logger.debug("{}: Cancel signal acknowledged".format(source_id))
        utils.complete_submission(source_id)
        return

    utils.update_status(source_id, "ingest_download", "P", except_on_fail=True)
    try:
        for dl_res in utils.download_data(user_transfer_client, feedstock_location,
                                          CONFIG["LOCAL_EP"], base_feed_path,
                                          admin_client=mdf_transfer_client, user_id=user_id):
            if not dl_res["success"]:
                utils.update_status(source_id, "ingest_download", "T",
                                    text=dl_res["error"], except_on_fail=True)
    except Exception as e:
        utils.update_status(source_id, "ingest_download", "F", text=repr(e),
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return
    if not dl_res["success"]:
        utils.update_status(source_id, "ingest_download", "F", text=dl_res["error"],
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return
    else:
        logger.info("{}: Feedstock downloaded".format(source_id))

    # If the data should be local, make sure it is
    # Currently only Publish needs the data
    if services.get("globus_publish"):
        if not data_loc:
            utils.update_status(source_id, "ingest_download", "F",
                                text=("Globus Publish integration was selected, "
                                      "but the data location was not provided."),
                                except_on_fail=True)
            utils.update_status(source_id, "ingest_publish", "F",
                                text="Unable to publish data without location.",
                                except_on_fail=True)
            utils.complete_submission(source_id)
            return
        else:
            # If all locations are Globus, don't need to download locally
            if all([loc.startswith("globus://") for loc in data_loc]):
                utils.update_status(source_id, "ingest_download", "N", except_on_fail=True)
                data_ep = None
                data_path = None
            else:
                utils.update_status(source_id, "ingest_download", "P", except_on_fail=True)
                # Will not transfer anything if already in place
                data_ep = CONFIG["LOCAL_EP"]
                data_path = os.path.join(CONFIG["LOCAL_PATH"], source_id) + "/"
                try:
                    for dl_res in utils.download_data(user_transfer_client, data_loc,
                                                      data_ep, data_path,
                                                      admin_client=mdf_transfer_client,
                                                      user_id=user_id):
                        if not dl_res["success"]:
                            utils.update_status(source_id, "ingest_download", "T",
                                                text=dl_res["error"], except_on_fail=True)
                except Exception as e:
                    utils.update_status(source_id, "ingest_download", "F", text=repr(e),
                                        except_on_fail=True)
                    utils.complete_submission(source_id)
                    return
                if not dl_res["success"]:
                    utils.update_status(source_id, "ingest_download", "F",
                                        text=dl_res["error"], except_on_fail=True)
                    utils.complete_submission(source_id)
                    return
                else:
                    utils.update_status(source_id, "ingest_download", "S",
                                        except_on_fail=True)
                    logger.debug("{}: Ingest data downloaded".format(source_id))
    else:
        utils.update_status(source_id, "ingest_download", "S", except_on_fail=True)

    # Same for integrated service data
    if services.get("citrine"):
        if not service_loc:
            utils.update_status(source_id, "ingest_integration", "F",
                                text=("Citrine integration was selected, but the"
                                      "integration data location was not provided."),
                                except_on_fail=True)
            utils.update_status(source_id, "ingest_citrine", "F",
                                text="Unable to upload PIFs without location.",
                                except_on_fail=True)
            utils.complete_submission(source_id)
            return
        else:
            utils.update_status(source_id, "ingest_integration", "P", except_on_fail=True)
            # Will not transfer anything if already in place
            service_data = os.path.join(CONFIG["SERVICE_DATA"], source_id) + "/"
            try:
                for dl_res in utils.download_data(user_transfer_client, service_loc,
                                                  CONFIG["LOCAL_EP"], service_data,
                                                  admin_client=mdf_transfer_client,
                                                  user_id=user_id):
                    if not dl_res["success"]:
                        utils.update_status(source_id, "ingest_integration", "T",
                                            text=dl_res["error"], except_on_fail=True)
            except Exception as e:
                utils.update_status(source_id, "ingest_integration", "F", text=repr(e),
                                    except_on_fail=True)
                utils.complete_submission(source_id)
                return
            if not dl_res["success"]:
                utils.update_status(source_id, "ingest_integration", "F",
                                    text=dl_res["error"], except_on_fail=True)
                utils.complete_submission(source_id)
                return
            else:
                utils.update_status(source_id, "ingest_integration", "S", except_on_fail=True)
                logger.debug("{}: Integration data downloaded".format(source_id))
    else:
        utils.update_status(source_id, "ingest_integration", "N", except_on_fail=True)

    # Integrations
    service_res = {}

    # NOTE: Cancellation point
    if utils.read_status(source_id).get("status", {}).get("cancelled"):
        logger.debug("{}: Cancel signal acknowledged".format(source_id))
        utils.complete_submission(source_id)
        return

    # MDF Search (mandatory)
    utils.update_status(source_id, "ingest_search", "P", except_on_fail=True)
    search_config = services.get("mdf_search", {})
    try:
        search_res = search_ingest(
                        base_feed_path, index=search_config.get("index", CONFIG["INGEST_INDEX"]),
                        batch_size=CONFIG["SEARCH_BATCH_SIZE"], feedstock_save=final_feed_path)
    except Exception as e:
        utils.update_status(source_id, "ingest_search", "F", text=repr(e),
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return
    else:
        # Handle errors
        if len(search_res["errors"]) > 0:
            utils.update_status(source_id, "ingest_search", "F",
                                text=("{} batches of records failed to ingest ({} records total)"
                                      ".").format(len(search_res["errors"]),
                                                  (len(search_res["errors"])
                                                   * CONFIG["SEARCH_BATCH_SIZE"]),
                                                  search_res["errors"]),
                                except_on_fail=True)
            utils.complete_submission(source_id)
            return

        # Other services use the dataset information
        with open(final_feed_path) as f:
            dataset = json.loads(f.readline())
        # Back up feedstock
        backup_feed_path = os.path.join(CONFIG["BACKUP_FEEDSTOCK"],
                                        source_id + "_final.json")
        try:
            transfer = mdf_toolbox.custom_transfer(
                            mdf_transfer_client, CONFIG["LOCAL_EP"], CONFIG["BACKUP_EP"],
                            [(final_feed_path, backup_feed_path)],
                            interval=CONFIG["TRANSFER_PING_INTERVAL"],
                            inactivity_time=CONFIG["TRANSFER_DEADLINE"],
                            notify=False)
            for event in transfer:
                if not event["success"]:
                    logger.debug(event)
            if not event["success"]:
                raise ValueError(event.get("code", "No code")
                                 + ": " + event.get("description", "No description"))
        except Exception as e:
            utils.update_status(source_id, "ingest_search", "R",
                                text="Feedstock backup failed: {}".format(str(e)),
                                except_on_fail=True)
        else:
            utils.update_status(source_id, "ingest_search", "S", except_on_fail=True)
            os.remove(final_feed_path)
        service_res["mdf_search"] = "This dataset was ingested to MDF Search."

    # Globus Publish
    if services.get("globus_publish"):
        utils.update_status(source_id, "ingest_publish", "P", except_on_fail=True)
        # collection should be in id or name
        collection = (services["globus_publish"].get("collection_id")
                      or services["globus_publish"].get("collection_name")
                      or CONFIG["DEFAULT_PUBLISH_COLLECTION"])
        try:
            fin_res = utils.globus_publish_data(publish_client, mdf_transfer_client,
                                                dataset, collection,
                                                data_ep, data_path, data_loc)
        except Exception as e:
            utils.update_status(source_id, "ingest_publish", "R", text=repr(e),
                                except_on_fail=True)
        else:
            stat_link = CONFIG["PUBLISH_LINK"].format(fin_res["id"])
            utils.update_status(source_id, "ingest_publish", "L",
                                text=fin_res["dc.description.provenance"], link=stat_link,
                                except_on_fail=True)
            service_res["globus_publish"] = stat_link
    else:
        utils.update_status(source_id, "ingest_publish", "N", except_on_fail=True)

    # Citrine
    if services.get("citrine"):
        utils.update_status(source_id, "ingest_citrine", "P", except_on_fail=True)

        # Get old Citrine dataset version, if exists
        scan_res = utils.scan_status(fields=["source_id", "citrine_id"],
                                     filters=[("source_name", "==", source_info["source_name"]),
                                              ("citrine_id", "!=", None)])
        if not scan_res["success"]:
            logger.error("Status scan failed: {}".format(scan_res["error"]))
        old_cit_subs = scan_res.get("results", [])
        if len(old_cit_subs) == 0:
            old_citrine_id = None
        elif len(old_cit_subs) == 1:
            old_citrine_id = old_cit_subs[0]["citrine_id"]
        else:
            old_citrine_id = max([sub["citrine_id"] for sub in old_cit_subs])

        try:
            # Check for PIFs to ingest
            cit_path = os.path.join(service_data, "citrine")
            if len(os.listdir(cit_path)) > 0:
                cit_res = utils.citrine_upload(cit_path, CONFIG["CITRINATION_API_KEY"], dataset,
                                               old_citrine_id,
                                               public=services["citrine"].get("public", True))
            else:
                cit_res = {
                    "success": False,
                    "error": "No PIFs were generated from this dataset",
                    "success_count": 0,
                    "failure_count": 0
                }
        except Exception as e:
            utils.update_status(source_id, "ingest_citrine", "R", text=repr(e),
                                except_on_fail=True)
        else:
            if not cit_res["success"]:
                if cit_res.get("error"):
                    text = cit_res["error"]
                elif cit_res.get("failure_count"):
                    text = "All {} PIFs failed to upload".format(cit_res["failure_count"])
                elif cit_res.get("failure_count") == 0:
                    text = "No PIFs were found"
                    logger.warning("{}: PIFs not found!".format(source_id))
                else:
                    text = "An error prevented PIF uploading"
                utils.update_status(source_id, "ingest_citrine", "R", text=text,
                                    except_on_fail=True)
            else:
                text = "{}/{} PIFs uploaded successfully".format(cit_res["success_count"],
                                                                 cit_res["success_count"]
                                                                 + cit_res["failure_count"])
                link = CONFIG["CITRINATION_LINK"].format(cit_ds_id=cit_res["cit_ds_id"])
                utils.update_status(source_id, "ingest_citrine", "L", text=text, link=link,
                                    except_on_fail=True)
                stat_res_2 = utils.modify_status_entry(source_id,
                                                       {"citrine_id": cit_res["cit_ds_id"]})
                if not stat_res_2["success"]:
                    raise ValueError(str(stat_res_2))
                service_res["citrine"] = link
    else:
        utils.update_status(source_id, "ingest_citrine", "N", except_on_fail=True)

    # MRR
    if services.get("mrr"):
        utils.update_status(source_id, "ingest_mrr", "P", except_on_fail=True)
        try:
            if isinstance(services["mrr"], dict) and services["mrr"].get("test"):
                mrr_title = "TEST_" + dataset["dc"]["titles"][0]["title"]
            else:
                mrr_title = dataset["dc"]["titles"][0]["title"]
            mrr_entry = {
                "title": dataset["dc"]["titles"][0]["title"],
                "schema": CONFIG["MRR_SCHEMA"],
                "content": CONFIG["MRR_TEMPLATE"].format(
                                title=mrr_title,
                                publisher=dataset["dc"]["publisher"],
                                contributors="".join(
                                    [CONFIG["MRR_CONTRIBUTOR"].format(
                                        name=(author.get("givenName", "") + " "
                                              + author.get("familyName", "")),
                                        affiliation=author.get("affiliation", ""))
                                     for author in dataset["dc"]["creators"]]),
                                contact_name=dataset["dc"]["creators"][0]["creatorName"],
                                description=dataset["dc"].get("description", ""),
                                subject="")
            }
        except Exception as e:
            utils.update_status(source_id, "ingest_mrr", "R",
                                text="Unable to create MRR metadata:"+repr(e),
                                except_on_fail=True)
        else:
            try:
                mrr_res_raw = requests.post(CONFIG["MRR_URL"],
                                            auth=(CONFIG["MRR_USERNAME"],
                                                  CONFIG["MRR_PASSWORD"]),
                                            data=mrr_entry)
                try:
                    mrr_res = mrr_res_raw.json()
                except json.JSONDecodeError:
                    raise ValueError("Invalid MRR response: {}".format(mrr_res_raw.content))
            except Exception as e:
                utils.update_status(source_id, "ingest_mrr", "F",
                                    text="Unable to submit MRR entry:"+repr(e),
                                    except_on_fail=True)
            else:
                if mrr_res.get("_id"):
                    utils.update_status(source_id, "ingest_mrr", "S", except_on_fail=True)
                    service_res["mrr"] = "This dataset was registered with the MRR."
                else:
                    utils.update_status(source_id, "ingest_mrr", "R",
                                        text=mrr_res.get("message", "Unknown failure"),
                                        except_on_fail=True)
    else:
        utils.update_status(source_id, "ingest_mrr", "N", except_on_fail=True)

    # Dataset update, start cleanup
    utils.update_status(source_id, "ingest_cleanup", "P", except_on_fail=True)

    dataset["services"] = service_res
    ds_update = update_search_entry(index=search_config.get("index", CONFIG["INGEST_INDEX"]),
                                    updated_entry=dataset, overwrite=False)
    if not ds_update["success"]:
        utils.update_status(source_id, "ingest_cleanup", "F",
                            text=ds_update.get("error", "Unable to update dataset"),
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return

    # Cleanup
    try:
        fin_res = utils.complete_submission(source_id, cleanup=True)
    except Exception as e:
        utils.update_status(source_id, "ingest_cleanup", "F", text=repr(e), except_on_fail=True)
        return
    if not fin_res["success"]:
        utils.update_status(source_id, "ingest_cleanup", "F", text=fin_res["error"],
                            except_on_fail=True)
        return
    utils.update_status(source_id, "ingest_cleanup", "S", except_on_fail=True)

    logger.debug("{}: Ingest complete".format(source_id))
    return {
        "success": True,
        "source_id": source_id
        }
