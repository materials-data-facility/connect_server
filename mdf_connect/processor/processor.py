import json
import logging
from multiprocessing import Process
import os
from time import sleep
import urllib

import globus_sdk
import mdf_toolbox
import requests

from mdf_connect import CONFIG
from mdf_connect.processor import convert, search_ingest, update_search_entry
from mdf_connect.utils import (cancel_submission, citrine_upload, complete_submission,
                               delete_from_queue, download_and_backup, globus_publish_data,
                               modify_status_entry, read_status, retrieve_from_queue,
                               update_status)


# Set up root logger
logger = logging.getLogger("mdf_connect")
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

logger.info("\n\n==========Connect Process started==========\n")


def processor():
    active_processes = []
    inactive_processes = []
    while True:
        try:
            submissions = retrieve_from_queue(wait_time=CONFIG["PROCESSOR_WAIT_TIME"])
            if not submissions["success"]:
                logger.debug("Submissions not retrieved: {}".format(submissions["error"]))
                continue
            if len(submissions["entries"]):
                logger.debug("{} submissions retrieved".format(len(submissions["entries"])))
                for sub in submissions["entries"]:
                    if sub["submission_type"] == "convert":
                        driver = Process(target=convert_driver, kwargs=sub)
                    elif sub["submission_type"] == "ingest":
                        driver = Process(target=ingest_driver, kwargs=sub)
                    driver.start()
                    active_processes.append(driver)
                delete_from_queue(submissions["delete_info"])
                logger.info("{} submissions started".format(len(submissions["entries"])))
        except Exception as e:
            logger.error("Processor error: {}".format(e))
        try:
            for dead_proc in [proc for proc in active_processes if not proc.is_alive()]:
                inactive_processes.append(dead_proc)
                active_processes.remove(dead_proc)
        except Exception as e:
            logger.error("Error life-checking processes: {}".format(e))
        # TODO: Check status DB if inactive processes are recorded dead
        sleep(CONFIG["PROCESSOR_SLEEP_TIME"])


def convert_driver(submission_type, metadata, source_id, test, access_token, user_id):
    """The driver function for MOC.
    Modifies the status database as steps are completed.

    Arguments:
    metadata (dict): The JSON passed to /convert.
    source_id (str): The source name of this submission.
    """
    # TODO: Better check?
    assert submission_type == "convert"
    # Setup
    update_status(source_id, "convert_start", "P", except_on_fail=True)
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": CONFIG["API_CLIENT_ID"],
        "client_secret": CONFIG["API_CLIENT_SECRET"],
        "services": ["transfer", "connect"]
        }
    try:
        access_token = access_token.replace("Bearer ", "")
        conf_client = globus_sdk.ConfidentialAppAuthClient(creds["client_id"],
                                                           creds["client_secret"])
        tokens = conf_client.oauth2_client_credentials_tokens(
                                requested_scopes=("https://auth.globus.org/scopes/"
                                                  "c17f27bb-f200-486a-b785-2a25e82af505/connect"
                                                  " urn:globus:auth:scope:"
                                                  "transfer.api.globus.org:all"))
        mdf_transfer_authorizer = globus_sdk.AccessTokenAuthorizer(
                                                tokens.by_resource_server
                                                ["transfer.api.globus.org"]["access_token"])
        mdf_transfer_client = globus_sdk.TransferClient(authorizer=mdf_transfer_authorizer)

        connect_authorizer = globus_sdk.AccessTokenAuthorizer(
                                            tokens.by_resource_server
                                            ["mdf_dataset_submission"]["access_token"])

        dependent_grant = conf_client.oauth2_get_dependent_tokens(access_token)
        user_transfer_authorizer = globus_sdk.AccessTokenAuthorizer(
                                                dependent_grant.data[0]["access_token"])
        user_transfer_client = globus_sdk.TransferClient(authorizer=user_transfer_authorizer)
    except Exception as e:
        update_status(source_id, "convert_start", "F", text=repr(e), except_on_fail=True)
        complete_submission(source_id)
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
            complete_submission(source_id)
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
    local_path = os.path.join(CONFIG["LOCAL_PATH"], source_id) + "/"
    backup_path = os.path.join(CONFIG["BACKUP_PATH"], source_id) + "/"
    try:
        # Edit ACL to allow pull
        acl_rule = {
            "DATA_TYPE": "access",
            "principal_type": "identity",
            "principal": user_id,
            "path": local_path,
            "permissions": "rw"
        }
        acl_res = mdf_transfer_client.add_endpoint_acl_rule(CONFIG["LOCAL_EP"], acl_rule).data
        if not acl_res.get("code") == "Created":
            logger.error("{}: Unable to create ACL rule: '{}'".format(source_id, acl_res))
            raise ValueError("Internal permissions error.")
        # Download from user
        for dl_res in download_and_backup(user_transfer_client,
                                          metadata.pop("data", {}),
                                          CONFIG["LOCAL_EP"],
                                          local_path):
            if not dl_res["success"]:
                msg = "During data download: " + dl_res["error"]
                update_status(source_id, "convert_download", "T", text=msg, except_on_fail=True)
        if not dl_res["success"]:
            raise ValueError(dl_res["error"])
        acl_del = mdf_transfer_client.delete_endpoint_acl_rule(CONFIG["LOCAL_EP"],
                                                               acl_res["access_id"])
        if not acl_del.get("code") == "Deleted":
            logger.critical("{}: Unable to delete ACL rule: '{}'".format(source_id, acl_del))
            raise ValueError("Internal permissions error.")

        # Backup to MDF
        if not test:
            for dl_res in download_and_backup(mdf_transfer_client,
                                              "globus://{}{}".format(CONFIG["LOCAL_EP"],
                                                                     local_path),
                                              CONFIG["BACKUP_EP"],
                                              backup_path):
                if not dl_res["success"]:
                    msg = "During data backup: " + dl_res["error"]
                    update_status(source_id, "convert_download", "T", text=msg,
                                  except_on_fail=True)
            if not dl_res["success"]:
                raise ValueError(dl_res["error"])

    except Exception as e:
        update_status(source_id, "convert_download", "F", text=repr(e),
                                 except_on_fail=True)
        complete_submission(source_id)
        return

    update_status(source_id, "convert_download", "S", except_on_fail=True)
    logger.info("{}: Data downloaded, {} archives extracted".format(
                                                                source_id,
                                                                dl_res["num_extracted"]))

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
    if read_status(source_id).get("status", {}).get("cancelled"):
        complete_submission(source_id)
        return

    # Convert data
    update_status(source_id, "converting", "P", except_on_fail=True)
    try:
        feedstock, num_groups = convert(local_path, convert_params)
    except Exception as e:
        update_status(source_id, "converting", "F", text=repr(e), except_on_fail=True)
        complete_submission(source_id)
        return
    else:
        # feedstock minus dataset entry is records
        num_parsed = len(feedstock) - 1
        # If nothing in feedstock, panic
        if num_parsed < 0:
            update_status(source_id, "converting", "F",
                                     text="Could not parse dataset entry", except_on_fail=True)
            complete_submission(source_id)
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
        connect_authorizer.set_authorization_header(headers)
        ingest_res = requests.post(CONFIG["INGEST_URL"],
                                   json=ingest_args,
                                   headers=headers)
    except Exception as e:
        update_status(source_id, "convert_ingest", "F", text=repr(e),
                                 except_on_fail=True)
        complete_submission(source_id)
        return
    else:
        if ingest_res.status_code < 300 and ingest_res.json().get("success"):
            update_status(source_id, "convert_ingest", "S", except_on_fail=True)
        else:
            update_status(source_id, "convert_ingest", "F",
                                     text=str(ingest_res.content), except_on_fail=True)
            complete_submission(source_id)
            return

    return {
        "success": True,
        "source_id": source_id
        }


def ingest_driver(submission_type, feedstock_location, source_id, services, data_loc,
                  service_loc, access_token, user_id):
    """Finalize and ingest feedstock."""
    # TODO: Better check?
    assert submission_type == "ingest"
    # Will need client to ingest data
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": CONFIG["API_CLIENT_ID"],
        "client_secret": CONFIG["API_CLIENT_SECRET"],
        "services": ["search_ingest", "publish", "transfer"]
        }
    try:
        clients = mdf_toolbox.confidential_login(creds)
        publish_client = clients["publish"]
        mdf_transfer_client = clients["transfer"]

        base_feed_path = os.path.join(CONFIG["FEEDSTOCK_PATH"], source_id + "_raw.json")
        final_feed_path = os.path.join(CONFIG["FEEDSTOCK_PATH"], source_id + "_final.json")

        access_token = access_token.replace("Bearer ", "")
        conf_client = globus_sdk.ConfidentialAppAuthClient(creds["client_id"],
                                                           creds["client_secret"])
        dependent_grant = conf_client.oauth2_get_dependent_tokens(access_token)
        user_transfer_authorizer = globus_sdk.AccessTokenAuthorizer(
                                                dependent_grant.data[0]["access_token"])
        user_transfer_client = globus_sdk.TransferClient(authorizer=user_transfer_authorizer)
    except Exception as e:
        update_status(source_id, "ingest_start", "F", text=repr(e), except_on_fail=True)
        complete_submission(source_id)
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
            update_status(source_id, "ingest_start", "F",
                          text=cancel_res.get("error",
                                              ("Unable to cancel previous "
                                               "submission '{}'").format(old_source_id)),
                          except_on_fail=True)
            complete_submission(source_id)
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

    update_status(source_id, "ingest_download", "P", except_on_fail=True)
    try:
        # Edit ACL to allow pull
        acl_rule = {
            "DATA_TYPE": "access",
            "principal_type": "identity",
            "principal": user_id,
            "path": os.path.dirname(base_feed_path),
            "permissions": "rw"
        }
        acl_res = mdf_transfer_client.add_endpoint_acl_rule(CONFIG["LOCAL_EP"], acl_rule).data
        if not acl_res.get("code") == "Created":
            logger.error("{}: Unable to create ACL rule: '{}'".format(source_id, acl_res))
            raise ValueError("Internal permissions error.")
        for dl_res in download_and_backup(user_transfer_client,
                                          feedstock_location,
                                          CONFIG["LOCAL_EP"],
                                          base_feed_path):
            if not dl_res["success"]:
                update_status(source_id, "ingest_download", "T",
                                         text=dl_res["error"], except_on_fail=True)
        acl_del = mdf_transfer_client.delete_endpoint_acl_rule(CONFIG["LOCAL_EP"],
                                                               acl_res["access_id"])
        if not acl_del.get("code") == "Deleted":
            logger.critical("{}: Unable to delete ACL rule: '{}'".format(source_id, acl_del))
            raise ValueError("Internal permissions error.")
    except Exception as e:
        update_status(source_id, "ingest_download", "F", text=repr(e),
                                 except_on_fail=True)
        complete_submission(source_id)
        return
    if not dl_res["success"]:
        update_status(source_id, "ingest_download", "F", text=dl_res["error"],
                                 except_on_fail=True)
        complete_submission(source_id)
        return
    else:
        logger.info("{}: Feedstock downloaded".format(source_id))

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
            complete_submission(source_id)
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
                data_ep = CONFIG["LOCAL_EP"]
                data_path = os.path.join(CONFIG["LOCAL_PATH"], source_id) + "/"
                try:
                    # Edit ACL to allow pull
                    acl_rule = {
                        "DATA_TYPE": "access",
                        "principal_type": "identity",
                        "principal": user_id,
                        "path": data_path,
                        "permissions": "rw"
                    }
                    acl_res = mdf_transfer_client.add_endpoint_acl_rule(data_ep, acl_rule).data
                    if not acl_res.get("code") == "Created":
                        logger.error("{}: Unable to create ACL rule: '{}'".format(source_id,
                                                                                  acl_res))
                        raise ValueError("Internal permissions error.")
                    for dl_res in download_and_backup(user_transfer_client,
                                                      data_loc,
                                                      data_ep,
                                                      data_path):
                        if not dl_res["success"]:
                            update_status(source_id, "ingest_download", "T",
                                                     text=dl_res["error"], except_on_fail=True)
                    acl_del = mdf_transfer_client.delete_endpoint_acl_rule(CONFIG["LOCAL_EP"],
                                                                           acl_res["access_id"])
                    if not acl_del.get("code") == "Deleted":
                        logger.critical("{}: Unable to delete ACL rule: '{}'".format(source_id,
                                                                                     acl_del))
                        raise ValueError("Internal permissions error.")
                except Exception as e:
                    update_status(source_id, "ingest_download", "F", text=repr(e),
                                             except_on_fail=True)
                    complete_submission(source_id)
                    return
                if not dl_res["success"]:
                    update_status(source_id, "ingest_download", "F",
                                             text=dl_res["error"], except_on_fail=True)
                    complete_submission(source_id)
                    return
                else:
                    update_status(source_id, "ingest_download", "S",
                                             except_on_fail=True)
                    logger.debug("{}: Ingest data downloaded".format(source_id))
    else:
        update_status(source_id, "ingest_download", "S", except_on_fail=True)

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
            complete_submission(source_id)
            return
        else:
            update_status(source_id, "ingest_integration", "P", except_on_fail=True)
            # Will not transfer anything if already in place
            service_data = os.path.join(CONFIG["SERVICE_DATA"], source_id) + "/"
            try:
                # Edit ACL to allow pull
                acl_rule = {
                    "DATA_TYPE": "access",
                    "principal_type": "identity",
                    "principal": user_id,
                    "path": service_data,
                    "permissions": "rw"
                }
                acl_res = mdf_transfer_client.add_endpoint_acl_rule(CONFIG["LOCAL_EP"],
                                                                    acl_rule).data
                if not acl_res.get("code") == "Created":
                    logger.error("{}: Unable to create ACL rule: '{}'".format(source_id, acl_res))
                    raise ValueError("Internal permissions error.")
                for dl_res in download_and_backup(user_transfer_client,
                                                  service_loc,
                                                  CONFIG["LOCAL_EP"],
                                                  service_data):
                    if not dl_res["success"]:
                        update_status(source_id, "ingest_integration", "T",
                                                 text=dl_res["error"], except_on_fail=True)
                acl_del = mdf_transfer_client.delete_endpoint_acl_rule(CONFIG["LOCAL_EP"],
                                                                       acl_res["access_id"])
                if not acl_del.get("code") == "Deleted":
                    logger.critical("{}: Unable to delete ACL rule: '{}'".format(source_id,
                                                                                 acl_del))
                    raise ValueError("Internal permissions error.")
            except Exception as e:
                update_status(source_id, "ingest_integration", "F", text=repr(e),
                                         except_on_fail=True)
                complete_submission(source_id)
                return
            if not dl_res["success"]:
                update_status(source_id, "ingest_integration", "F",
                                         text=dl_res["error"], except_on_fail=True)
                complete_submission(source_id)
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
                        index=search_config.get("index", CONFIG["INGEST_INDEX"]),
                        batch_size=CONFIG["SEARCH_BATCH_SIZE"],
                        feedstock_save=final_feed_path)
    except Exception as e:
        update_status(source_id, "ingest_search", "F", text=repr(e),
                                 except_on_fail=True)
        complete_submission(source_id)
        return
    else:
        # Handle errors
        if len(search_res["errors"]) > 0:
            update_status(source_id, "ingest_search", "F",
                          text=("{} batches of records failed to ingest ({} records total)"
                                ".").format(len(search_res["errors"]),
                                            (len(search_res["errors"])
                                             * CONFIG["SEARCH_BATCH_SIZE"]),
                                            search_res["errors"]),
                          except_on_fail=True)
            complete_submission(source_id)
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
                            inactivity_time=CONFIG["TRANSFER_DEADLINE"])
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
                      or CONFIG["DEFAULT_PUBLISH_COLLECTION"])
        try:
            fin_res = globus_publish_data(publish_client, mdf_transfer_client,
                                          dataset, collection,
                                          data_ep, data_path, data_loc)
        except Exception as e:
            update_status(source_id, "ingest_publish", "R", text=repr(e),
                                     except_on_fail=True)
        else:
            stat_link = CONFIG["PUBLISH_LINK"].format(fin_res["id"])
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
                                     CONFIG["CITRINATION_API_KEY"],
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
                link = CONFIG["CITRINATION_LINK"].format(cit_ds_id=cit_res["cit_ds_id"])
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
            update_status(source_id, "ingest_mrr", "R",
                                     text="Unable to create MRR metadata:"+repr(e),
                                     except_on_fail=True)
        else:
            try:
                mrr_res = requests.post(CONFIG["MRR_URL"],
                                        auth=(CONFIG["MRR_USERNAME"],
                                              CONFIG["MRR_PASSWORD"]),
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
                                    index=search_config.get("index", CONFIG["INGEST_INDEX"]),
                                    updated_entry=dataset, overwrite=False)
    if not ds_update["success"]:
        update_status(source_id, "ingest_cleanup", "F",
                                 text=ds_update.get("error", "Unable to update dataset"),
                                 except_on_fail=True)
        complete_submission(source_id)
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
