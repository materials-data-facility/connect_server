from datetime import date
from hashlib import sha512
import json
import os
import re
import shutil
import tempfile
from threading import Thread
import zipfile

from bson import ObjectId
from citrination_client import CitrinationClient
from flask import jsonify, request
import magic
from mdf_toolbox import toolbox
from mdf_refinery import convert, ingest
from pif_ingestor.manager import IngesterManager
from pypif.pif import dump as pif_dump
from pypif_sdk.util import citrination as cit_utils
from pypif_sdk.interop.mdf import _to_user_defined as pif_to_feedstock
from pypif_sdk.interop.datacite import add_datacite as add_dc

import requests
from werkzeug.utils import secure_filename

from services import app

KEY_FILES = {
    "dft": {
        "exact": [],
        "extension": [],
        "regex": ["OUTCAR.*"]
    }
}
PUBLISH_COLLECTION = 35


@app.route('/convert', methods=["POST"])
def accept_convert():
    """Accept the JSON metadata and begin the conversion process."""
    metadata = request.get_json(force=True, silent=True)
    if not metadata:
        return jsonify({
            "success": False,
            "error": "POST data empty or not JSON"
            })
    status_id = str(ObjectId())
    # TODO: Register status ID
    print("DEBUG: Status ID created")
    driver = Thread(target=moc_driver, name="driver_thread", args=(metadata, status_id))
    driver.start()
    return jsonify({
        "success": True,
        "status_id": status_id
        })


def moc_driver(moc_params, status_id):
    """Pull, back up, and convert metadata."""
    # Setup
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["transfer"]
        }
    transfer_client = toolbox.confidential_login(creds)["transfer"]

    # Download data locally, back up on MDF resources
    dl_res = download_and_backup(transfer_client,
                                 moc_params.pop("data", {}),
                                 status_id)
    if dl_res["success"]:
        local_path = dl_res["local_path"]
        backup_path = dl_res["backup_path"]
    else:
        raise IOError("No data downloaded")
    # TODO: Update status - data downloaded
    print("DEBUG: Data downloaded")

    services = moc_params.pop("services", [])

    # Convert data
    feedstock = convert(local_path, moc_params)

    # Pass dataset to /ingest
    with tempfile.TemporaryFile(mode="w+") as stock:
        for entry in feedstock:
            json.dump(entry, stock)
            stock.write("\n")
        stock.seek(0)
        ingest_res = requests.post(app.config["INGEST_URL"],
                                   data={"status_id": status_id,
                                         "data": app.config["LOCAL_EP"] + local_path,
                                         "services": services},
                                   files={'file': stock})
    print("DEBUG: Ingest result:", ingest_res)
    if not ingest_res.json().get("success"):
        # TODO: Update status? Ingest failed
        raise ValueError("In convert - Ingest failed" + str(ingest_res.json()))

    # TODO: Update status - everything done
    return {
        "success": True,
        "status_id": status_id
        }


# OLD
def begin_convert(mdf_dataset, status_id):
    """Pull, back up, and convert metadata."""
    # Setup
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["transfer", "publish"]
        }
    clients = toolbox.confidential_login(creds)
    mdf_transfer_client = clients["transfer"]
    globus_publish_client = clients["publish"]

    # Download data locally, back up on MDF resources
    dl_res = download_and_backup(mdf_transfer_client,
                                 mdf_dataset.pop("data", {}),
                                 status_id)
    if dl_res["success"]:
        local_path = dl_res["local_path"]
        backup_path = dl_res["backup_path"]
    else:
        raise IOError("No data downloaded")
    # TODO: Update status - data downloaded
    print("DEBUG: Data downloaded")

    print("DEBUG: Conversions started")
    # Pop indexing args
    parse_params = mdf_dataset.pop("index", {})
    add_services = mdf_dataset.pop("services", [])

    # TODO: Stream data into files instead of holding feedstock in memory
    feedstock = [mdf_dataset]

    # tags = [sub["subject"] for sub in mdf_dataset.get("dc", {}).get("subjects", [])]
    # key_info = get_key_matches(tags or None)

    # List of all files, for bag
    all_files = []

    # Citrination setup
    cit_manager = IngesterManager()
    cit_client = CitrinationClient(app.config["CITRINATION_API_KEY"])
    # Get title and description
    try:
        cit_title = mdf_dataset["dc"]["titles"][0]["title"]
    except (KeyError, IndexError):
        cit_title = "Untitled"
    try:
        cit_desc = " ".join([desc["description"]
                            for desc in mdf_dataset["dc"]["descriptions"]])
        if not cit_desc:
            raise KeyError
    except (KeyError, IndexError):
        cit_desc = None
    cit_ds = cit_client.create_data_set(name=cit_title,
                                        description=cit_desc,
                                        share=0).json()
    cit_ds_id = cit_ds["id"]
    print("DEBUG: Citrine dataset ID:", cit_ds_id)

    for path, dirs, files in os.walk(os.path.abspath(local_path)):
        # Separate files into groups, process group as unit
        for group in group_files(files):
            # Get all file metadata
            group_file_md = [get_file_metadata(
                                file_path=os.path.join(path, filename),
                                backup_path=os.path.join(
                                                backup_path,
                                                path.replace(os.path.abspath(local_path), ""),
                                                filename))
                             for filename in group]
            all_files.extend(group_file_md)

            group_paths = [os.path.join(path, filename) for filename in group]

            # MDF parsing
            mdf_res = omniparser.omniparse(group_paths, parse_params)

            # Citrine parsing
            cit_pifs = cit_manager.run_extensions(group_paths,
                                                  include=None, exclude=[],
                                                  args={"quality_report": False})
            if not isinstance(cit_pifs, list):
                cit_pifs = [cit_pifs]
            cit_full = []
            if len(cit_pifs) > 0:
                cit_res = []
                # Add UIDs
                cit_pifs = cit_utils.set_uids(cit_pifs)
                for pif in cit_pifs:
                    # Get PIF URL
                    pif_land_page = {
                                        "mdf": {
                                            "landing_page": cit_utils.get_url(pif, cit_ds_id)
                                        }
                                    } if cit_ds_id else {}
                    # Make PIF into feedstock and save
                    cit_res.append(toolbox.dict_merge(pif_to_feedstock(pif), pif_land_page))
                    # Add DataCite metadata
                    pif = add_dc(pif, mdf_dataset.get("dc", {}))

                    cit_full.append(pif)
            else:  # No PIFs parsed
                # TODO: Send failed datatype to Citrine for logging
                # Pad cit_res to the same length as mdf_res for "merging"
                cit_res = [{} for i in range(len(mdf_res))]

            # If MDF parser failed to parse group, pad mdf_res to match PIF count
            if len(mdf_res) == 0:
                mdf_res = [{} for i in range(len(cit_res))]

            # If only one mdf record was parsed, merge all PIFs into that record
            if len(mdf_res) == 1:
                merged_cit = {}
                [toolbox.dict_merge(merged_cit, cr) for cr in cit_res]
                mdf_records = [toolbox.dict_merge(mdf_res[0], merged_cit)]
            # If the same number of MDF records and Citrine PIFs were parsed, merge in order
            elif len(mdf_res) == len(cit_res):
                mdf_records = [toolbox.dict_merge(r_mdf, r_cit)
                               for r_mdf, r_cit in zip(mdf_res, cit_res)]
            # Otherwise, keep the MDF records only
            else:
                print("DEBUG: Record mismatch:\nMDF parsed", len(mdf_res), "records",
                      "\nCitrine parsed", len(cit_res), "records"
                      "\nPIFs discarded")
                # TODO: Update status/log - Citrine records discarded
                mdf_records = mdf_res

            # Filter null records, save rest
            if not mdf_records:
                print("DEBUG: No MDF records in group:", group)
            [feedstock.append(toolbox.dict_merge(record, {"files": group_file_md}))
             for record in mdf_records if record]

            # Upload PIFs to Citrine
            for full_pif in cit_full:
                with tempfile.NamedTemporaryFile(mode="w+") as pif_file:
                    pif_dump(full_pif, pif_file)
                    pif_file.seek(0)
                    up_res = json.loads(cit_client.upload(cit_ds_id, pif_file.name))
                    if up_res["success"]:
                        print("DEBUG: Citrine upload success")
                    else:
                        print("DEBUG: Citrine upload failure, error", up_res.get("status"))

    # TODO: Update status - indexing success
    print("DEBUG: Indexing success")

    # Pass feedstock to /ingest
    with tempfile.TemporaryFile(mode="w+") as stock:
        for entry in feedstock:
            json.dump(entry, stock)
            stock.write("\n")
        stock.seek(0)
        ingest_res = requests.post(app.config["INGEST_URL"],
                                   json={"status_id": status_id},
                                   files={'file': stock})
    if not ingest_res.json().get("success"):
        # TODO: Update status? Ingest failed
        # TODO: Fail everything, delete Citrine dataset, etc.
        raise ValueError("In convert - Ingest failed" + str(ingest_res.json()))

    # Additional service integrations

    # Finalize Citrine dataset
    # TODO: Turn on public dataset ingest (share=1)
    if "citrine" in add_services:
        try:
            cit_client.update_data_set(cit_ds_id, share=0)
        except Exception as e:
            # TODO: Update status, notify Citrine - Citrine ds failure
            print("DEBUG: Citrination dataset not updated")

    # Globus Publish
    # TODO: Test after Publish API is fixed
    if "globus_publish" in add_services:
        try:
            fin_res = globus_publish_data(globus_publish_client,
                                          mdf_transfer_client,
                                          mdf_dataset,
                                          local_path)
        except Exception as e:
            # TODO: Update status - Publish failed
            print("Publish ERROR:", repr(e))
        else:
            # TODO: Update status - Publish success
            print("DEBUG: Publish success:", fin_res)

    # Remove local data
    shutil.rmtree(local_path)
    # TODO: Update status - everything done
    return {
        "success": True,
        "status_id": status_id
        }


def download_and_backup(mdf_transfer_client, data_loc, status_id):
    """Download remote data, backup"""
    local_path = os.path.join(app.config["LOCAL_PATH"], status_id) + "/"
    backup_path = os.path.join(app.config["BACKUP_PATH"], status_id) + "/"
    os.makedirs(local_path, exist_ok=True)  # TODO: exist not okay when status is real

    # Download data locally
    try:
        if data_loc.get("globus"):
            # Parse out EP and path
            # Right now, path assumed to be a directory
            user_ep, user_path = data_loc["globus"].split("/", 1)
            user_path = "/" + user_path + ("/" if not user_path.endswith("/") else "")
            # Transfer locally
            toolbox.quick_transfer(mdf_transfer_client, user_ep, app.config["LOCAL_EP"],
                                   [(user_path, local_path)], timeout=0)

        elif data_loc.get("zip"):
            # Download and unzip
            zip_path = os.path.join(local_path, status_id + ".zip")
            res = requests.get(data_loc["zip"])
            with open(zip_path, 'wb') as out:
                out.write(res.content)
            zipfile.ZipFile(zip_path).extractall()  # local_path)
            os.remove(zip_path)  # TODO: Should the .zip be removed?

        elif data_loc.get("files"):
            # TODO: Implement this
            raise NotImplementedError("Files not implemented yet")

        else:
            # Nothing to do
            raise IOError("Invalid data location: " + str(data_loc))

    except Exception as e:
        # TODO: Update status - download failure
        raise
    # TODO: Update status - download success
    print("DEBUG: Download success")

    # Backup data
    # TODO: Re-enable backup after testing
    print("DEBUG: WARNING: NO BACKUP")
    '''
    try:
        toolbox.quick_transfer(mdf_transfer_client,
                               app.config["LOCAL_EP"], app.config["BACKUP_EP"],
                               [(local_path, backup_path)], timeout=0)
    except Exception as e:
        # TODO: Update status - backup failed
        raise
    # TODO: Update status - backup success
    print("DEBUG: Backup success")
    '''

    return {
        "success": True,
        "local_path": local_path,
        "backup_path": backup_path
        }


def get_file_metadata(file_path, backup_path):
    with open(file_path, "rb") as f:
        md = {
            "globus": app.config["BACKUP_EP"] + backup_path,
            "data_type": magic.from_file(file_path),
            "mime_type": magic.from_file(file_path, mime=True),
            "url": app.config["BACKUP_HOST"] + backup_path,
            "length": os.path.getsize(file_path),
            "filename": os.path.basename(file_path),
            "sha512": sha512(f.read()).hexdigest()
        }
    return md


@app.route("/ingest", methods=["POST"])
def accept_ingest():
    """Accept the JSON feedstock file and begin the ingestion process."""
    # Check that file exists and is valid
    try:
        feedstock = request.files["file"]
    except KeyError:
        return jsonify({
            "success": False,
            "error": "No feedstock file uploaded"
            })
    # Get parameters
    try:
        params = request.form
        services = params.get("services", [])
        data_loc = params.get("data", None)
    except KeyError as e:
        return jsonify({
            "success": False,
            "error": "Parameters missing: " + repr(e)
            })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": "Invalid ingest JSON: " + repr(e)
            })
    
    # Mint/update status ID
    if not params.get("status_id"):
        status_id = str(ObjectId())
        # TODO: Register status ID
        print("DEBUG: New status ID created")
    else:
        # TODO: Check that status exists (must not be set by user)
        # TODO: Update status - ingest request recieved
        status_id = params.get("status_id")
        print("DEBUG: Current status ID read")
    # Save file
    feed_path = os.path.join(app.config["FEEDSTOCK_PATH"], secure_filename(feedstock.filename))
    feedstock.save(feed_path)
    ingester = Thread(target=begin_ingest, name="ingester_thread", args=(feed_path,
                                                                         status_id,
                                                                         services,
                                                                         data_loc))
    ingester.start()
    return jsonify({
        "success": True,
        "status_id": status_id
        })


def begin_ingest(base_feed_path, status_id, services, data_loc):
    """Finalize and ingest feedstock."""
    # Will need client to ingest data
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["search_ingest", "publish", "transfer"]
        }
    clients = toolbox.confidential_login(creds)
    search_client = clients["search_ingest"]
    publish_client = clients["publish"]
    transfer_client = clients["transfer"]

    final_feed_path = os.path.join(app.config["FEEDSTOCK_PATH"], status_id + "_final.json")

    # If the data should be local, make sure it is
    if data_loc:
        # Will not transfer anything if already in place
        dl_res = download_and_backup(transfer_client,
                                     data_loc,
                                     status_id)
        if dl_res["success"]:
            local_path = dl_res["local_path"]
            backup_path = dl_res["backup_path"]
        else:
            raise IOError("No data downloaded")
        # TODO: Update status - data downloaded
        print("DEBUG: Data downloaded")
    # If the data aren't local, but need to be, error
    elif "globus_publish" in services:
        raise ValueError("Unable to Publish data without location")

    # Globus Search (mandatory)
    try:
        ingest(search_client, base_feed_path, index=app.config["INGEST_INDEX"],
                        feedstock_save=final_feed_path)
    except Exception as e:
        # TODO: Update status - ingest failed
        raise Exception("Search error:" + str({
            "success": False,
            "error": repr(e)
            }))

    # Globus Publish
    # TODO: Test after Publish API is fixed
    if "globus_publish" in services:
        # Get DC metadata
        with open(final_feed_path) as f:
            dataset = json.loads(f.readline())
        try:
            fin_res = globus_publish_data(publish_client, transfer_client,
                                          dataset, local_path)
        except Exception as e:
            # TODO: Update status - Publish failed
            print("Publish ERROR:", repr(e))
        else:
            # TODO: Update status - Publish success
            print("DEBUG: Publish success:", fin_res)

    # Remove local data
    shutil.rmtree(local_path)

    # TODO: Update status - ingest successful, processing complete
    print("DEBUG: Ingest success, processing complete")
    return {
        "success": True,
        "status_id": status_id
        }


def globus_publish_data(publish_client, transfer_client, metadata, local_path):
    # Submit metadata
    try:
        pub_md = get_publish_metadata(metadata.get("dc", {}))
        md_result = publish_client.push_metadata(pub_md["collection_id"], pub_md)
        pub_endpoint = md_result['globus.shared_endpoint.name']
        pub_path = os.path.join(md_result['globus.shared_endpoint.path'], "data") + "/"
        submission_id = md_result["id"]
    except Exception as e:
        # TODO: Raise exception - not Published due to bad metadata
        print("DEBUG: Publish push failed")
        raise
    # Transfer data
    try:
        toolbox.quick_transfer(transfer_client, app.config["LOCAL_EP"],
                               pub_endpoint, [(local_path, pub_path)], timeout=0)
    except Exception as e:
        # TODO: Raise exception - not Published due to failed Transfer
        raise
    # Complete submission
    try:
        fin_res = publish_client.complete_submission(submission_id)
    except Exception as e:
        # TODO: Raise exception - not Published due to Publish error
        raise
    return fin_res


def get_publish_metadata(metadata):
    dc_metadata = metadata.get("dc", {})
    # TODO: Find full Publish schema for translation
    # Required fields
    pub_metadata = {
        "dc.title": ", ".join([title.get("title", "")
                               for title in dc_metadata.get("titles", [])]),
        "dc.date.issued": str(date.today().year),
        "dc.publisher": "Materials Data Facility",
        "dc.contributor.author": [author.get("creatorName", "")
                                  for author in dc_metadata.get("creators", [])],
        "collection_id": PUBLISH_COLLECTION,
        "accept_license": True
    }
    return pub_metadata


@app.route("/status", methods=["GET", "POST"])
def status():
    return jsonify({"success": False, "message": "Not implemented yet, try again later"})
