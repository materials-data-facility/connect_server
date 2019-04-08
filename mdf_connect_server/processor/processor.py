import json
import logging
from multiprocessing import Process
import os
import signal
from time import sleep

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


# Class to catch signals for graceful shutdown
class SignalHandler:
    caught_signal = None

    def __init__(self, signal_catch_list=None):
        if signal_catch_list is None:
            signal_catch_list = [
                signal.SIGINT,
                signal.SIGTERM
            ]
        elif not isinstance(signal_catch_list, list):
            signal_catch_list = [signal_catch_list]
        for s in signal_catch_list:
            signal.signal(s, self.catch_signal)

    def catch_signal(self, signum, frame):
        if self.caught_signal is None:
            self.caught_signal = set([signum])
        else:
            self.caught_signal.add(signum)


def processor():
    logger.info("\n\n==========Connect Process started==========\n")
    # Write out Processor PID
    with open("pid.log", 'w') as pf:
        pf.write(str(os.getpid()))
    utils.clean_start()
    active_processes = []
    sig_handle = SignalHandler()
    while sig_handle.caught_signal is None:
        try:
            submissions = utils.retrieve_from_queue(wait_time=CONFIG["PROCESSOR_WAIT_TIME"])
            if not submissions["success"]:
                logger.debug("Submissions not retrieved: {}".format(submissions["error"]))
            if len(submissions["entries"]):
                logger.debug("{} submissions retrieved".format(len(submissions["entries"])))
                for sub in submissions["entries"]:
                    driver = Process(target=submission_driver, kwargs=sub, name=sub["source_id"])
                    driver.start()
                    active_processes.append(driver)
                utils.delete_from_queue(submissions["delete_info"])
                logger.info("{} submissions started".format(len(submissions["entries"])))
        except Exception as e:
            logger.error("Processor error: {}".format(e))
        try:
            for dead_proc in [proc for proc in active_processes if not proc.is_alive()]:
                # In-curation processes should not be cancelled
                # 'curating' is sentinel value for curation in progress
                dead_status = utils.read_status(dead_proc.name)
                if not dead_status["success"]:
                    logger.error("Unable to read status for '{}': {}".format(dead_proc.name,
                                                                             dead_status))
                    continue
                logger.info("Dead: {} ({})".format(dead_proc.name,
                                                   dead_status["status"]["curation"]))
                if dead_status["status"]["curation"] is True:
                    active_processes.remove(dead_proc)
                    logger.debug("{}: Dead but curating, not cancelled".format(dead_proc.name))
                else:
                    cancel_res = utils.cancel_submission(dead_proc.name)
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
            logger.error("Error life-checking processes: {}".format(repr(e)))
        sleep(CONFIG["PROCESSOR_SLEEP_TIME"])

    # After processing finished, shut down gracefully
    logger.info("Shutting down Connect")
    for proc in active_processes:
        cancel_res = utils.cancel_submission(proc.name)
        if cancel_res["stopped"]:
            logger.debug("{}: Shutdown".format(proc.name))
        else:
            logger.info("Unable to shut down process for {}: {}"
                        .format(proc.name, cancel_res.get("error", "No error provided")))
    logger.info("Connect gracefully shut down")
    return


def submission_driver(metadata, sub_conf, source_id, access_token, user_id):
    """The driver function for MOC.
    Modifies the status database as steps are completed.

    Arguments:
    metadata (dict): The JSON passed to /submit.
    sub_conf (dict): Submission configuration information.
    source_id (str): The source name of this submission.
    access_token (str): The Globus Auth access token for the submitting user.
    user_id (str): The Globus ID of the submitting user.
    """
    # Setup
    utils.update_status(source_id, "sub_start", "P", except_on_fail=True)
    utils.modify_status_entry(source_id, {"pid": os.getpid()}, except_on_fail=True)
    try:
        # Connect auth
        # CAAC required for user auth later
        mdf_conf_client = globus_sdk.ConfidentialAppAuthClient(CONFIG["API_CLIENT_ID"],
                                                               CONFIG["API_CLIENT_SECRET"])
        mdf_creds = mdf_toolbox.dict_merge(CONFIG["GLOBUS_CREDS"],
                                           {"services": ["publish", "transfer"]})
        mdf_clients = mdf_toolbox.confidential_login(mdf_creds)
        mdf_transfer_client = mdf_clients["transfer"]
        globus_publish_client = mdf_clients["publish"]

        # User auth
        access_token = access_token.replace("Bearer ", "")
        dependent_grant = mdf_conf_client.oauth2_get_dependent_tokens(access_token)
        user_transfer_authorizer = globus_sdk.AccessTokenAuthorizer(
                                                dependent_grant.data[0]["access_token"])
        user_transfer_client = globus_sdk.TransferClient(authorizer=user_transfer_authorizer)
    except Exception as e:
        utils.update_status(source_id, "sub_start", "F", text=repr(e), except_on_fail=True)
        utils.complete_submission(source_id)
        return

    # Cancel the previous version(s)
    source_info = utils.split_source_id(source_id)
    scan_res = utils.scan_status(fields="source_id",
                                 filters=[("source_id", "^", source_info["source_name"]),
                                          ("source_id", "<", source_id)])
    if not scan_res["success"]:
        utils.update_status(source_id, "sub_start", "F", text=scan_res["error"],
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return
    for old_source in scan_res["results"]:
        old_source_id = old_source["source_id"]
        cancel_res = utils.cancel_submission(old_source_id, wait=True)
        if not cancel_res["stopped"]:
            utils.update_status(source_id, "sub_start", "F",
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

    utils.update_status(source_id, "sub_start", "S", except_on_fail=True)
    # NOTE: Cancellation point
    if utils.read_status(source_id).get("status", {}).get("cancelled"):
        logger.debug("{}: Cancel signal acknowledged".format(source_id))
        utils.complete_submission(source_id)
        return

    # Curation skip point
    if type(sub_conf["curation"]) is not str:
        local_path = os.path.join(CONFIG["LOCAL_PATH"], source_id) + "/"
        feedstock_file = os.path.join(CONFIG["FEEDSTOCK_PATH"], source_id + ".json")
        # If we're converting, download data locally, then set canon source to local
        # This allows non-Globus sources (because to download to Connect's EP)
        if not sub_conf["no_convert"]:
            utils.update_status(source_id, "data_download", "P", except_on_fail=True)
            try:
                # Download from user
                for dl_res in utils.download_data(user_transfer_client, sub_conf["data_sources"],
                                                  CONFIG["LOCAL_EP"], local_path,
                                                  admin_client=mdf_transfer_client,
                                                  user_id=user_id):
                    if not dl_res["success"]:
                        msg = "During data download: " + dl_res["error"]
                        utils.update_status(source_id, "data_download", "T", text=msg,
                                            except_on_fail=True)
                if not dl_res["success"]:
                    raise ValueError(dl_res["error"])

            except Exception as e:
                utils.update_status(source_id, "data_download", "F", text=repr(e),
                                    except_on_fail=True)
                utils.complete_submission(source_id)
                return

            utils.update_status(source_id, "data_download", "M",
                                text=("{} files will be converted ({} archives extracted)"
                                      .format(dl_res["total_files"], dl_res["num_extracted"])),
                                except_on_fail=True)
            canon_data_sources = ["globus://{}{}".format(CONFIG["LOCAL_EP"], local_path)]

        # If we're not converting, set canon source to only source
        # Also create local dir with no data to "convert" for dataset entry
        else:
            utils.update_status(source_id, "data_download", "N", except_on_fail=True)
            os.makedirs(local_path)
            canon_data_sources = sub_conf["data_sources"]

        # Move data from canon source(s) to canon dest (if different)
        utils.update_status(source_id, "data_transfer", "P", except_on_fail=True)
        for data_source in canon_data_sources:
            if data_source != sub_conf["canon_destination"]:
                logger.debug("Data transfer: '{}' to '{}'".format(data_source,
                                                                  sub_conf["canon_destination"]))
                backup_res = utils.backup_data(mdf_transfer_client, data_source,
                                               sub_conf["canon_destination"])
                if not backup_res[sub_conf["canon_destination"]]:
                    err_text = ("Transfer from '{}' failed: {}"
                                .format(data_source, backup_res[sub_conf["canon_destination"]]))
                    utils.update_status(source_id, "data_transfer", "F", text=err_text,
                                        except_on_fail=True)
                    return
        utils.update_status(source_id, "data_transfer", "S", except_on_fail=True)

        # Handle service integration data directory
        service_data = os.path.join(CONFIG["SERVICE_DATA"], source_id) + "/"
        os.makedirs(service_data)

        # Add file info data
        sub_conf["index"]["file"] = {
            "globus_host": sub_conf["canon_destination"],
            "http_host": CONFIG["BACKUP_HOST"],  # TODO: Correct
            "local_path": local_path,
        }
        convert_params = {
            "dataset": metadata,
            "parsers": sub_conf["index"],
            "service_data": service_data,
            "feedstock_file": feedstock_file,
            "group_config": mdf_toolbox.dict_merge(sub_conf["conversion_config"],
                                                   CONFIG["GROUPING_RULES"]),
            "num_transformers": CONFIG["NUM_TRANSFORMERS"],
            "validation_info": {
                "project_blocks": sub_conf.get("project_blocks", []),
                "required_fields": sub_conf.get("required_fields", [])
            }
        }

        # NOTE: Cancellation point
        if utils.read_status(source_id).get("status", {}).get("cancelled"):
            logger.debug("{}: Cancel signal acknowledged".format(source_id))
            utils.complete_submission(source_id)
            return

        # Convert data
        utils.update_status(source_id, "converting", "P", except_on_fail=True)
        try:
            convert_res = convert(local_path, convert_params)
            if not convert_res["success"]:
                utils.update_status(source_id, "converting", "F", text=convert_res["error"],
                                    except_on_fail=True)
                return
            dataset = convert_res["dataset"]
            num_records = convert_res["num_records"]
            num_groups = convert_res["num_groups"]
            extensions = convert_res["extensions"]
        except Exception as e:
            utils.update_status(source_id, "converting", "F", text=repr(e), except_on_fail=True)
            utils.complete_submission(source_id)
            return
        else:
            utils.modify_status_entry(source_id, {"extensions": extensions})
            # If nothing in dataset, panic
            if not dataset:
                utils.update_status(source_id, "converting", "F",
                                    text="Could not parse dataset entry", except_on_fail=True)
                utils.complete_submission(source_id)
                return
            # If no records, warn user
            elif num_records < 1:
                utils.update_status(source_id, "converting", "U",
                                    text=("No records were parsed out of {} groups"
                                          .format(num_groups)), except_on_fail=True)
            else:
                utils.update_status(source_id, "converting", "M",
                                    text=("{} records parsed out of {} groups"
                                          .format(num_records, num_groups)), except_on_fail=True)
            logger.debug("{}: {} entries parsed".format(source_id, num_records+1))

        # NOTE: Cancellation point
        if utils.read_status(source_id).get("status", {}).get("cancelled"):
            logger.debug("{}: Cancel signal acknowledged".format(source_id))
            utils.complete_submission(source_id)
            return

        ###################
        #  Curation step  #
        ###################
        # Trigger curation if required
        if sub_conf.get("curation"):
            utils.update_status(source_id, "curation", "P", except_on_fail=True)
            # TODO: Real curation flow
            # Likely with state save to file, reload and skip reconverting
            logger.info("CURATION TRIGGERED FOR {}".format(source_id))
            utils.modify_status_entry(source_id, {"curation": True}, except_on_fail=True)

            # Pretending curation succeeded, state restored
            utils.modify_status_entry(source_id, {"curation": "Admin fiat"}, except_on_fail=True)
            utils.update_status(source_id, "curation", "M", text="Accepted by admin fiat",
                                except_on_fail=True)
        else:
            utils.update_status(source_id, "curation", "N", except_on_fail=True)
    # Returning from successful curation
    else:
        # TODO
        print("Successful curation is not implemented")

    # Integrations
    service_res = {}

    # NOTE: Cancellation point
    if utils.read_status(source_id).get("status", {}).get("cancelled"):
        logger.debug("{}: Cancel signal acknowledged".format(source_id))
        utils.complete_submission(source_id)
        return

    # MDF Search (mandatory)
    utils.update_status(source_id, "ingest_search", "P", except_on_fail=True)
    search_config = sub_conf["services"].get("mdf_search", {})
    try:
        search_args = {
            "feedstock_file": feedstock_file,
            "source_id": source_id,
            "index": search_config.get("index", CONFIG["INGEST_INDEX"]),
            "batch_size": CONFIG["SEARCH_BATCH_SIZE"]
        }
        search_res = search_ingest(**search_args)
        if not search_res["success"]:
            utils.update_status(source_id, "ingest_search", "F",
                                text="; ".join(search_res["errors"]), except_on_fail=True)
            return
    except Exception as e:
        utils.update_status(source_id, "ingest_search", "F", text=repr(e),
                            except_on_fail=True)
        utils.complete_submission(source_id)
        return
    else:
        # Handle errors
        if len(search_res["errors"]) > 0:
            utils.update_status(source_id, "ingest_search", "F",
                                text=("{} batches of records failed to ingest (up to {} records "
                                      "total)").format(len(search_res["errors"]),
                                                       (len(search_res["errors"])
                                                        * CONFIG["SEARCH_BATCH_SIZE"]),
                                                       search_res["errors"]),
                                except_on_fail=True)
            utils.complete_submission(source_id)
            return

        # Back up feedstock
        source_feed_loc = "globus://{}{}".format(CONFIG["LOCAL_EP"], feedstock_file)
        backup_feed_loc = "globus://{}{}".format(CONFIG["BACKUP_EP"],
                                                 os.path.join(CONFIG["BACKUP_FEEDSTOCK"],
                                                              source_id + "_final.json"))
        feed_backup_res = utils.backup_data(mdf_transfer_client, source_feed_loc,
                                            backup_feed_loc)
        if feed_backup_res[backup_feed_loc] is not True:
            utils.update_status(source_id, "ingest_search", "R",
                                text=("Feedstock backup failed: {}"
                                      .format(feed_backup_res[backup_feed_loc])),
                                except_on_fail=True)
        else:
            utils.update_status(source_id, "ingest_search", "S", except_on_fail=True)
            os.remove(feedstock_file)
        service_res["mdf_search"] = "This dataset was ingested to MDF Search."

    # Move files to data_destinations
    if sub_conf.get("data_destinations"):
        utils.update_status(source_id, "ingest_backup", "P", except_on_fail=True)
        backup_res = utils.backup_data(mdf_transfer_client,
                                       storage_loc=sub_conf["canon_destination"],
                                       backup_locs=sub_conf["data_destinations"])
        if not all([val is True for val in backup_res.values()]):
            err_msg = "; ".join(["'{}' failed: {}".format(k, v) for k, v in backup_res.items()
                                 if v is not True])
            utils.update_status(source_id, "ingest_backup", "F", text=err_msg, except_on_fail=True)
        else:
            utils.update_status(source_id, "ingest_backup", "S", except_on_fail=True)
    else:
        utils.update_status(source_id, "ingest_backup", "N", except_on_fail=True)

    # Globus Publish
    # TODO: MDF Publish migration
    if sub_conf["services"].get("mdf_publish"):
        utils.update_status(source_id, "ingest_publish", "F",
                            text="MDF Publish not yet available", except_on_fail=True)
    if sub_conf["services"].get("globus_publish"):
        utils.update_status(source_id, "ingest_publish", "P", except_on_fail=True)
        # collection should be in id or name
        collection = (sub_conf["services"]["globus_publish"].get("collection_id")
                      or sub_conf["services"]["globus_publish"].get("collection_name")
                      or CONFIG["DEFAULT_PUBLISH_COLLECTION"])
        try:
            fin_res = utils.globus_publish_data(globus_publish_client, mdf_transfer_client,
                                                dataset, collection, CONFIG["LOCAL_EP"],
                                                os.path.join(CONFIG["LOCAL_PATH"], source_id) + "/")
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
    if sub_conf["services"].get("citrine"):
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
                cit_res = utils.citrine_upload(
                                cit_path, CONFIG["CITRINATION_API_KEY"], dataset, old_citrine_id,
                                public=sub_conf["services"]["citrine"].get("public", True))
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
    if sub_conf["services"].get("mrr"):
        utils.update_status(source_id, "ingest_mrr", "P", except_on_fail=True)
        try:
            if (isinstance(sub_conf["services"]["mrr"], dict)
                    and sub_conf["services"]["mrr"].get("test")):
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
