from ctypes import c_bool
import json
import multiprocessing
import os
from queue import Empty

from globus_sdk import GlobusAPIError
import mdf_toolbox

from mdf_connect import Validator


NUM_SUBMITTERS = 5


def search_ingest(ingest_client, feedstocks, index, batch_size=100,
                  num_submitters=NUM_SUBMITTERS, feedstock_save=None):
    """Ingests feedstock from file.

    Arguments:
    ingest_client (globus_sdk.SearchClient): An authenticated client.
    feedstock (str or list of str): The path(s) to feedstock to ingest.
    index (str): The Search index to ingest into.
    batch_size (int): Max size of a single ingest operation. -1 for unlimited. Default 100.
    num_submitters (int): The number of submission processes to create. Default NUM_SUBMITTERS.
    feedstock_save (str): Path to file for saving final feedstock. Default None, to save nothing.
    """
    if type(feedstocks) is str:
        feedstocks = [feedstocks]
    index = mdf_toolbox.translate_index(index)

    # Validate feedstock
    all_validators = []
    for feed_path in feedstocks:
        with open(feed_path) as stock:
            val = Validator()
            ds_res = val.start_dataset(json.loads(next(stock)))
            if not ds_res.get("success"):
                raise ValueError("Feedstock '{}' invalid: {}".format(feed_path, str(ds_res)))

            for rc in stock:
                record = json.loads(rc)
                rc_res = val.add_record(record)
                if not rc_res.get("success"):
                    raise ValueError("Feedstock '{}' invalid: {}".format(feed_path, str(rc_res)))
        all_validators.append(val)

    # Set up multiprocessing
    ingest_queue = multiprocessing.Queue()
    input_done = multiprocessing.Value(c_bool, False)

    # Create submitters
    submitters = [multiprocessing.Process(target=submit_ingests,
                                          args=(ingest_queue, ingest_client, index, input_done))
                  for i in range(NUM_SUBMITTERS)]
    [s.start() for s in submitters]

    # Populate ingest queue and save results if requested
    with open(feedstock_save or os.devnull, 'w') as save_loc:
        batch = []
        # For each entry in each dataset
        for val in all_validators:
            for entry in val.get_finished_dataset():
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
        input_done.value = True

    # Wait for submitters to finish
    [s.join() for s in submitters]

    return {"success": True}


def submit_ingests(ingest_queue, ingest_client, index, input_done):
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
            raise
    return
