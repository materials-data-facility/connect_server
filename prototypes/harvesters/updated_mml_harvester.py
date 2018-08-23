import json
import os
import shutil

import requests


def harvest(out_dir, index=None, existing_dir=0, verbose=False):
    """Collects available data from NIST's MML and saves to the given directory

    Arguments:
        out_dir (str): The path to the directory (which will be created) for the data files.
        index (str): Path to the JSON list of already-processed MML entries.
                     Default out_dir + harvester_index.json
        existing_dir (int):
           -1: Remove out_dir if it exists
            0: Error if out_dir exists (Default)
            1: Overwrite files in out_dir if there are path collisions
        verbose (bool): Print status messages?
                        Default False.
    """
    # Existing dir check, make nonexistant dirs
    if os.path.exists(out_dir):
        if existing_dir == 0:
            exit("Directory '" + out_dir + "' exists")
        elif not os.path.isdir(out_dir):
            exit("Error: '" + out_dir + "' is not a directory")
        elif existing_dir == -1: 
            rmtree(out_dir)
            os.mkdir(out_dir)
    else:
        os.mkdir(out_dir)
    # Handle index file
    if index is None:
        index = os.path.join(out_dir, "harvester_index.json")
        processed_ids = {
            "success": [],
            "failed": [],
            "errors": []
        }
    else:
        with open(index) as f:
            processed_ids = json.load(f)

    # Get list of all collections
    collection_res = requests.get("https://materialsdata.nist.gov/dspace/rest/collections")
    if not collection_res.status_code == 200:
        raise ValueError("Error {} fetching collection list".format(collection_res.status_code))

    for collection in collection_res.json():
        # Don't re-process collections
        if collection["id"] in (processed_ids["success"] + processed_ids["failure"]):
            continue
        item_res = requests.get("https://materialsdata.nist.gov/dspace/rest/collections/"
                                "{}/items".format(collection["id"]))
        if not item_res.status_code == 200:
            print("Error {} fetching item list for collection "
                  "{}".format(collection_res.status_code, collection["id"]))
        for item in item_res.json():



