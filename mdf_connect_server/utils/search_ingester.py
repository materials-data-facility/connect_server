from ctypes import c_bool
import json
import logging
import multiprocessing
from queue import Empty
from tempfile import NamedTemporaryFile
from time import sleep

from globus_sdk import GlobusAPIError
import mdf_toolbox

from mdf_connect_server import CONFIG
from .utils import split_source_id


logger = logging.getLogger(__name__)


def search_ingest(feedstock_file, index, delete_existing, source_id=None, batch_size=100,
                  num_submitters=CONFIG["NUM_SUBMITTERS"]):
    """Ingests feedstock from file.

    Arguments:
        feedstock_file (str): The feedstock file to ingest.
        index (str): The Search index to ingest into.
        delete_existing (bool): If True, will delete existing Search entries with the
                given source_name before ingesting the new entries.
                If False, will only ingest the new entries, overwriting identical subjects.
        source_id (str): The source_id of the feedstock.
                Default None. Required if delete_existing is True.
        batch_size (int): Max size of a single ingest operation. -1 for unlimited.
                Default 100.
        num_submitters (int): The number of submission processes to create. Default NUM_SUBMITTERS.

    Returns:
        dict:
            success (bool): True on success.
            errors (list): The errors encountered.
            details (str): If success is False, details about the major error, if available.
    """
    ingest_client = mdf_toolbox.confidential_login(
                        mdf_toolbox.dict_merge(CONFIG["GLOBUS_CREDS"],
                                               {"services": ["search_ingest"]}))["search_ingest"]
    index = mdf_toolbox.translate_index(index)

    if delete_existing:
        if not source_id:
            return {
                "success": False,
                "error": "source_id is required if delete_existing is True"
            }
        # Delete existing versions of this dataset in Search
        source_info = split_source_id(source_id)
        del_q = {
            "q": "mdf.source_name:{}".format(source_info["source_name"]),
            "advanced": True
        }
        # Try deleting from Search until success or try limit reached
        # Necessary because Search will 5xx but possibly succeed on large deletions
        i = 0
        while True:
            try:
                del_res = ingest_client.delete_by_query(index, del_q)
                break
            except GlobusAPIError as e:
                if i < CONFIG["SEARCH_RETRIES"]:
                    logger.warning("{}: Retrying Search delete error: {}"
                                   .format(source_id, repr(e)))
                    i += 1
                else:
                    raise
        if del_res["num_subjects_deleted"]:
            logger.info(("{}: {} Search entries cleared from "
                         "{}").format(source_id, del_res["num_subjects_deleted"],
                                      source_info["source_name"]))
    else:
        logger.debug("{}: Existing Search entries not deleted.".format(source_id))

    # Set up multiprocessing
    ingest_queue = multiprocessing.Queue()
    error_queue = multiprocessing.Queue()
    input_done = multiprocessing.Value(c_bool, False)

    # Create submitters
    submitters = [multiprocessing.Process(target=submit_ingests,
                                          args=(ingest_queue, error_queue,
                                                index, input_done, source_id))
                  for i in range(num_submitters)]
    # Create queue populator
    populator = multiprocessing.Process(target=populate_queue,
                                        args=(ingest_queue, feedstock_file, batch_size, source_id))
    logger.debug("{}: Search ingestion starting".format(source_id))
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
    logger.debug("{}: Submitters joined".format(source_id))

    # Fetch remaining errors, if any
    try:
        while True:
            errors.append(json.loads(error_queue.get(timeout=1)))
    except Empty:
        pass

    logger.debug("{}: Search ingestion finished with {} errors".format(source_id, len(errors)))
    return {
        "success": True,
        "errors": errors
    }


def populate_queue(ingest_queue, feedstock_file, batch_size, source_id):
    # Populate ingest queue
    batch = []
    with open(feedstock_file) as feed_in:
        for str_entry in feed_in:
            entry = json.loads(str_entry)
            # Add gmeta-formatted entry to batch
            acl = entry["mdf"].pop("acl")
            # Identifier is source_id for datasets, source_id + scroll_id for records
            if entry["mdf"]["resource_type"] == "dataset":
                iden = entry["mdf"]["source_id"]
            else:
                iden = entry["mdf"]["source_id"] + "." + str(entry["mdf"]["scroll_id"])
            batch.append(mdf_toolbox.format_gmeta(entry, acl=acl, identifier=iden))

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
    logger.debug("{}: Input queue populated".format(source_id))
    return


def submit_ingests(ingest_queue, error_queue, index, input_done, source_id):
    """Submit entry ingests to Globus Search."""
    ingest_client = mdf_toolbox.confidential_login(
                        mdf_toolbox.dict_merge(CONFIG["GLOBUS_CREDS"],
                                               {"services": ["search_ingest"]}))["search_ingest"]
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
            # Allow retries
            i = 0
            while True:
                try:
                    ingest_res = ingest_client.ingest(index, ingestable)
                    if not ingest_res["acknowledged"]:
                        raise ValueError("Ingest not acknowledged by Search")
                    task_id = ingest_res["task_id"]
                    task_status = "PENDING"  # Assume task starts as pending
                    # While task is not complete, check status
                    while task_status != "SUCCESS" and task_status != "FAILURE":
                        sleep(CONFIG["SEARCH_PING_TIME"])
                        task_res = ingest_client.get_task(task_id)
                        task_status = task_res["state"]
                    break
                except (GlobusAPIError, ValueError) as e:
                    if i < CONFIG["SEARCH_RETRIES"]:
                        logger.warning("{}: Retrying Search ingest error: {}"
                                       .format(source_id, repr(e)))
                        i += 1
                    else:
                        raise
            if task_status == "FAILURE":
                raise ValueError("Ingest failed: " + str(task_res))
            elif task_status == "SUCCESS":
                logger.debug("{}: Search batch ingested: {}"
                             .format(source_id, task_res["message"]))
            else:
                raise ValueError("Invalid state '{}' from {}".format(task_status, task_res))
        except GlobusAPIError as e:
            logger.error("{}: Search ingest error: {}".format(source_id, e.raw_json))
            # logger.debug('Stack trace:', exc_info=True)
            # logger.debug("Full ingestable:\n{}\n".format(ingestable))
            err = {
                "exception_type": str(type(e)),
                "details": e.raw_json
            }
            error_queue.put(json.dumps(err))
        except Exception as e:
            logger.error("{}: Generic ingest error: {}".format(source_id, repr(e)))
            # logger.debug('Stack trace:', exc_info=True)
            # logger.debug("Full ingestable:\n{}\n".format(ingestable))
            err = {
                "exception_type": str(type(e)),
                "details": str(e)
            }
            error_queue.put(json.dumps(err))
    return


def update_search_entries(index, entries, acl=None, overwrite=False):
    """Update entries in Search.

    Note:
        source_id, source_name, and scroll_id must not be updated.

    Arguments:
        index (str): The Search index to ingest into.
        entries (list of dict): The updated versions of the entries.
        acl (list of strings): The list of Globus UUIDs allowed to access these entries.
                Default None, if the acls are in the updated entries.
                It is an error if no ACL is supplied in the arguments or the entries.
        overwrite (bool): If True, will overwrite old entries (fields not present in
                the entries will be lost).
                If False, will merge the new entries with the old entries.
                The entries must exist in Search.
                Default False.

    Returns:
        dict: The results.
            success (bool): True on operation success, False on failure.
            error (str): If success is False, the error that occurred.
            entries (list of dict): The ingested entries.

    Note:
        If overwrite is False, the subjects being updated must exist in Search.
        It is an error if the subjects are not found.
        The subjects are not required to exist if overwrite is True.
    """
    index = mdf_toolbox.translate_index(index)
    if isinstance(entries, dict):
        entries = [entries]
    if isinstance(acl, str):
        acl = [acl]
    # ACL must be provided in args or entries
    if not all([entry["mdf"].get("acl") for entry in entries]):
        if acl is None:
            return {
                "success": False,
                "error": "ACL missing from at least one entry, and not provided in arguments."
            }
        else:
            # Add ACL to entries missing ACL
            for entry in entries:
                if not entry["mdf"].get("acl"):
                    entry["mdf"]["acl"] = acl
    # If not overwriting, merge with existing entries
    if not overwrite:
        new_entries = []
        search_client = mdf_toolbox.confidential_login(
                            mdf_toolbox.dict_merge(CONFIG["GLOBUS_CREDS"],
                                                   {"services": ["search"]}))["search"]
        for entry in entries:
            try:
                # Identifier is source_id for datasets, source_id + scroll_id for records
                if entry["mdf"]["resource_type"] == "dataset":
                    iden = entry["mdf"]["source_id"]
                else:
                    iden = entry["mdf"]["source_id"] + "." + str(entry["mdf"]["scroll_id"])
            except KeyError as e:
                return {
                    "success": False,
                    "error": "Unable to derive subject from entry without key " + str(e)
                }
            try:
                # Will raise SearchAPIError (404) if not found
                old_entry = search_client.get_entry(index, iden)["content"][0]
            except Exception as e:
                return {
                    "success": False,
                    "error": "Error fetching existing entry '{}': {}".format(iden, repr(e))
                }
            # Merge and add entry to new_entries
            new_entries.append(mdf_toolbox.dict_merge(entry, old_entry))
        entries = new_entries
    # Send to ingest
    # Write to tempfile
    with NamedTemporaryFile("w+") as tfile:
        for entry in entries:
            json.dump(entry, tfile)
            tfile.write("\n")
        tfile.seek(0)
        # Call search_ingest with tempfile
        # Will not work on Windows - tempfile must be opened twice
        ingest_res = search_ingest(tfile.name, index, delete_existing=False)
    if not ingest_res["success"]:
        return {
            "success": False,
            "error": "Errors: {}\nDetails: {}".format(ingest_res.get("errors", []),
                                                      ingest_res.get("details", "No details"))
        }
    else:
        return {
            "success": True,
            "entries": entries
        }


def update_search_subjects(index, subjects, convert_func, acl, overwrite=False):
    """Update entries by subject in Search.
    The entries must all exist in Search.

    Note:
        source_id, source_name, and scroll_id must not be updated.

    Arguments:
        index (str): The Search index to ingest into.
        subjects (list of str): The subjects to update.
        convert_func (function): The function to use to update the entries.
                This function must take one argument, the existing Search entry.
                It must return one dictionary, the updated entry.
        acl (list of strings): The list of Globus UUIDs allowed to access these entries.
                Default None, if the convert_func adds the ACL to the entries.
                It is an error if no ACL is supplied in the arguments or the entries.
        overwrite (bool): If True, will overwrite old entries (fields not present in
                the entries will be lost).
                If False, will merge the new entries with the old entries.
                Default False.

    Returns:
        dict: The results.
            success (bool): True on operation success, False on failure.
            error (str): If success is False, the error that occurred.
            entries (list of dict): The ingested entries.
    """
    index = mdf_toolbox.translate_index(index)
    if isinstance(subjects, str):
        subjects = [subjects]
    if isinstance(acl, str):
        acl = [acl]

    # Fetch the existing entries and run the convert_func on them
    new_entries = []
    search_client = mdf_toolbox.confidential_login(
                        mdf_toolbox.dict_merge(CONFIG["GLOBUS_CREDS"],
                                               {"services": ["search"]}))["search"]
    for subject in subjects:
        try:
            # Will raise SearchAPIError (404) if not found
            old_entry = search_client.get_entry(index, subject)["content"][0]
        except Exception as e:
            return {
                "success": False,
                "error": "Error fetching existing entry '{}': {}".format(subject, repr(e))
            }
        try:
            new_entries.append(convert_func(old_entry))
        except Exception as e:
            return {
                "success": False,
                "error": "Error converting entry '{}': {}".format(subject, repr(e))
            }
    # Now that we have the updated entries, pass to update_search_entries
    return update_search_entries(index, new_entries, acl=acl, overwrite=overwrite)
