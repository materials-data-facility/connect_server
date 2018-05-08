from ctypes import c_bool
import json
import logging
import multiprocessing
import os
from queue import Empty

from globus_sdk import GlobusAPIError
import mdf_toolbox

from mdf_connect import Validator


NUM_SUBMITTERS = 5

logger = logging.getLogger(__name__)


def search_ingest(ingest_creds, feedstock, index, batch_size,
                  num_submitters=NUM_SUBMITTERS, feedstock_save=None):
    """Ingests feedstock from file.

    Arguments:
    ingest_creds (dict): The credentials with which to ingest.
    feedstock (str): The path to feedstock to ingest.
    index (str): The Search index to ingest into.
    batch_size (int): Max size of a single ingest operation. -1 for unlimited. Default 100.
    num_submitters (int): The number of submission processes to create. Default NUM_SUBMITTERS.
    feedstock_save (str): Path to file for saving final feedstock. Default None, to save nothing.

    Returns:
    dict: success (bool): True on success.
          errors (list): The errors encountered.
    """
    ingest_creds["services"] = ["search_ingest"]
    ingest_client = mdf_toolbox.confidential_login(ingest_creds)["search_ingest"]
    index = mdf_toolbox.translate_index(index)

    # Validate feedstock
    with open(feedstock) as stock:
        val = Validator()
        dataset_entry = json.loads(next(stock))
        ds_res = val.start_dataset(dataset_entry)
        if not ds_res.get("success"):
            raise ValueError("Feedstock '{}' invalid: {}".format(feedstock, str(ds_res)))

        # Delete previous version of this dataset in Search
        version = dataset_entry["mdf"]["version"]
        source_name = dataset_entry["mdf"]["source_name"]
        old_source_name = source_name
        # Find previous version with entries
        while version > 1:
            old_source_name = old_source_name.replace("_v"+str(version), "_v"+str(version-1))
            del_q = {
                "q": "mdf.source_name:" + old_source_name,
                "advanced": True
                }
            del_res = ingest_client.delete_by_query(index, del_q)
            if del_res["num_subjects_deleted"]:
                logger.info(("{}: {} Search entries cleared from "
                             "{}").format(source_name, del_res["num_subjects_deleted"],
                                          old_source_name))
            version -= 1

        for rc in stock:
            record = json.loads(rc)
            rc_res = val.add_record(record)
            if not rc_res.get("success"):
                raise ValueError("Feedstock '{}' invalid: {}".format(feedstock, str(rc_res)))

    # Set up multiprocessing
    ingest_queue = multiprocessing.Queue()
    error_queue = multiprocessing.Queue()
    input_done = multiprocessing.Value(c_bool, False)

    # Create submitters
    submitters = [multiprocessing.Process(target=submit_ingests,
                                          args=(ingest_queue, error_queue, ingest_creds,
                                                index, input_done, source_name))
                  for i in range(NUM_SUBMITTERS)]
    # Create queue populator
    populator = multiprocessing.Process(target=populate_queue,
                                        args=(ingest_queue, val, batch_size,
                                              (feedstock_save or os.devnull), source_name))
    logger.debug("{}: Search ingestion starting".format(source_name))
    # Start processes
    populator.start()
    [s.start() for s in submitters]

    # Start pulling off any errors
    # Stop when populator is finished
    errors = []
    while populator.exitcode is None:
        try:
            errors.append(json.loads(error_queue.get(timeout=5)))
        except Empty:
            pass
    # Populator is finished, signal submitters
    input_done.value = True

    # Continue fetching errors until first Empty
    try:
        while True:
            errors.append(json.loads(error_queue.get(timeout=5)))
    except Empty:
        pass

    # Wait for submitters to finish
    [s.join() for s in submitters]
    logger.debug("{}: Submitters joined".format(source_name))

    # Fetch remaining errors, if any
    try:
        while True:
            errors.append(json.loads(error_queue.get(timeout=1)))
    except Empty:
        pass

    logger.debug("{}: Search ingestion finished with {} errors".format(source_name, len(errors)))
    return {
        "success": True,
        "errors": errors
    }


def populate_queue(ingest_queue, validator, batch_size, feedstock_save, source_name):
    # Populate ingest queue and save results if requested
    with open(feedstock_save, 'w') as save_loc:
        batch = []
        for entry in validator.get_finished_dataset():
            # Save entry
            json.dump(entry, save_loc)
            save_loc.write("\n")
            # Add gmeta-formatted entry to batch
            batch.append(mdf_toolbox.format_gmeta(entry))

            # If batch is appropriate size
            if batch_size > 0 and len(batch) >= batch_size:
                # Format batch into gmeta and put in queue
                full_ingest = mdf_toolbox.format_gmeta(batch)
                ingest_queue.put(json.dumps(full_ingest))
                batch.clear()

        # Ingest partial batch if needed
        if batch:
            full_ingest = mdf_toolbox.format_gmeta(batch)
            ingest_queue.put(json.dumps(full_ingest))
            batch.clear()
    logger.debug("{}: Input queue populated".format(source_name))
    return


def submit_ingests(ingest_queue, error_queue, ingest_creds, index, input_done, source_name):
    """Submit entry ingests to Globus Search."""
    ingest_client = mdf_toolbox.confidential_login(ingest_creds)["search_ingest"]
    while True:
        # Try getting an ingest from the queue
        try:
            ingestable = json.loads(ingest_queue.get(timeout=5))
        # There are no ingests in the queue
        except Empty:
            # If all ingests have been put in the queue (and thus processed), break
            if input_done.value:
                break
            # Otherwise, more ingests are coming, try again
            else:
                continue
        # Ingest, with error handling
        try:
            # Allow one retry
            try:
                res = ingest_client.ingest(index, ingestable)
            except Exception as e:
                logger.error("{}: Retrying Search error: {}".format(source_name, repr(e)))
                res = ingest_client.ingest(index, ingestable)
            if not res["success"]:
                raise ValueError("Ingest failed: " + str(res))
            elif res["num_documents_ingested"] <= 0:
                raise ValueError("No documents ingested: " + str(res))
            else:
                logger.debug("{}: Search batch ingested".format(source_name))
        except GlobusAPIError as e:
            logger.error("{}: Search Globus API Error: {}".format(source_name, e.raw_json))
            # logger.debug('Stack trace:', exc_info=True)
            # logger.debug("Full ingestable:\n{}\n".format(ingestable))
            err = {
                "exception_type": str(type(e)),
                "details": e.raw_json
            }
            error_queue.put(json.dumps(err))
        except Exception as e:
            logger.error("{}: Generic Search error: {}".format(source_name, repr(e)))
            # logger.debug('Stack trace:', exc_info=True)
            # logger.debug("Full ingestable:\n{}\n".format(ingestable))
            err = {
                "exception_type": str(type(e)),
                "details": str(e)
            }
            error_queue.put(json.dumps(err))
    return
