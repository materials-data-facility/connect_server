from ctypes import c_bool
import json
import multiprocessing
import os
from queue import Empty

from globus_sdk import GlobusAPIError
import mdf_toolbox

from mdf_connect import Validator


NUM_SUBMITTERS = 5


def search_ingest(ingest_client, feedstock, index, batch_size=100,
                  num_submitters=NUM_SUBMITTERS, feedstock_save=None):
    """Ingests feedstock from file.

    Arguments:
    ingest_client (globus_sdk.SearchClient): An authenticated client.
    feedstock (str): The path to feedstock to ingest.
    index (str): The Search index to ingest into.
    batch_size (int): Max size of a single ingest operation. -1 for unlimited. Default 100.
    num_submitters (int): The number of submission processes to create. Default NUM_SUBMITTERS.
    feedstock_save (str): Path to file for saving final feedstock. Default None, to save nothing.

    Returns:
    dict: success (bool): True on success.
          errors (list): The errors encountered.
    """
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
        old_source_name = dataset_entry["mdf"]["source_name"]
        # Find previous version with entries
        while version > 1:
            old_source_name = old_source_name.replace("_v"+str(version), "_v"+str(version-1))
            del_q = {
                "q": "mdf.source_name:" + old_source_name,
                "advanced": True
                }
            del_res = ingest_client.delete_by_query(index, del_q)
            if del_res["num_subjects_deleted"]:
                print("DEBUG:", del_res["num_subjects_deleted"],
                      "Search entries cleared from", old_source_name)
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
                                          args=(ingest_queue, error_queue, ingest_client,
                                                index, input_done))
                  for i in range(NUM_SUBMITTERS)]
    # Create queue populator
    populator = multiprocessing.Process(target=populate_queue,
                                        args=(ingest_queue, val, batch_size,
                                              (feedstock_save or os.devnull)))
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

    # Fetch remaining errors, if any
    try:
        while True:
            errors.append(json.loads(error_queue.get(timeout=1)))
    except Empty:
        pass

    return {
        "success": True,
        "errors": errors
    }


def populate_queue(ingest_queue, validator, batch_size, feedstock_save=os.devnull):
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
    return


def submit_ingests(ingest_queue, error_queue, ingest_client, index, input_done):
    """Submit entry ingests to Globus Search."""
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
            res = ingest_client.ingest(index, ingestable)
            if not res["success"]:
                raise ValueError("Ingest failed: " + str(res))
            elif res["num_documents_ingested"] <= 0:
                raise ValueError("No documents ingested: " + str(res))
        except GlobusAPIError as e:
            print("\nA Globus API Error has occurred. Details:\n", e.raw_json, "\n")
            err = {
                "ingest_batch": ingestable,
                "exception_type": str(type(e)),
                "details": e.raw_json
            }
            error_queue.put(json.dumps(err))
        except Exception as e:
            print("Search error:", str(e))
            err = {
                "ingest_batch": ingestable,
                "exception_type": str(type(e)),
                "details": str(e)
            }
            error_queue.put(json.dumps(err))
    return
