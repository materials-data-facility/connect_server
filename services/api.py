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
from mdf_refinery import convert, search_ingest
from pif_ingestor.manager import IngesterManager
from pypif.pif import dump as pif_dump
from pypif_sdk.util import citrination as cit_utils
from pypif_sdk.interop.mdf import _to_user_defined as pif_to_feedstock
from pypif_sdk.interop.datacite import add_datacite as add_dc

import requests
from werkzeug.utils import secure_filename

from services import app


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


def moc_driver(metadata, status_id):
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
    local_path = os.path.join(app.config["LOCAL_PATH"], status_id) + "/"
    dl_res = download_and_backup(transfer_client,
                                 metadata.pop("data", {}),
                                 app.config["LOCAL_EP"],
                                 local_path)
    if not dl_res["success"]:
        raise IOError("No data downloaded")
    # TODO: Update status - data downloaded
    print("DEBUG: Data downloaded")

    # Handle service integration data directory
    service_data = os.path.join(app.config["SERVICE_DATA"], status_id) + "/"
    os.makedirs(service_data)

    # Pull out special fields in metadata (the rest is the dataset)
    services = metadata.pop("services", [])
    parse_params = metadata.pop("index", {})
    convert_params = {
        "dataset": metadata,
        "parsers": parse_params,
        "service_data": service_data
        }

    # Convert data
    feedstock = convert(local_path, convert_params)
    print("DEBUG: Feedstock contains", len(feedstock), "entries")

    # Pass dataset to /ingest
    with tempfile.TemporaryFile(mode="w+") as stock:
        for entry in feedstock:
            json.dump(entry, stock)
            stock.write("\n")
        stock.seek(0)
        ingest_args = {
            "status_id": status_id,
            "data": json.dumps({
                "globus": app.config["LOCAL_EP"] + local_path
                }),
            "services": services,
            "service_data": json.dumps({
                "globus": app.config["LOCAL_EP"] + service_data
                })
        }
        ingest_res = requests.post(app.config["INGEST_URL"],
                                   data=ingest_args,
                                   files={'file': stock})

    print("DEBUG: Ingest result:", ingest_res.json())
    if not ingest_res.json().get("success"):
        # TODO: Update status? Ingest failed
        raise ValueError("In convert - Ingest failed" + str(ingest_res.json()))

    # TODO: Update status - everything done
    return {
        "success": True,
        "status_id": status_id
        }


def download_and_backup(mdf_transfer_client, data_loc, local_ep, local_path):
    """Download remote data, backup"""
    os.makedirs(local_path, exist_ok=True)
    # Download data locally
    try:
        if data_loc.get("globus"):
            # Check that data not already in place
            if data_loc.get("globus") != local_ep + local_path:
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
        raise
    print("DEBUG: Download success")


    return {
        "success": True,
        "local_path": local_path
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
        data_loc = json.loads(params.get("data", "{}"))
        service_data = json.loads(params.get("service_data", "{}"))
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
    ingester = Thread(target=moc_ingester, name="ingester_thread", args=(feed_path,
                                                                         status_id,
                                                                         services,
                                                                         data_loc,
                                                                         service_data))
    ingester.start()
    return jsonify({
        "success": True,
        "status_id": status_id
        })


def moc_ingester(base_feed_path, status_id, services, data_loc, service_loc):
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
        local_path = os.path.join(app.config["LOCAL_PATH"], status_id) + "/"
        dl_res = download_and_backup(transfer_client,
                                     data_loc,
                                     app.config["LOCAL_EP"],
                                     local_path)
        if not dl_res["success"]:
            raise IOError("No data downloaded")
        # TODO: Update status - data downloaded
        print("DEBUG: Data downloaded")
    # If the data aren't local, but need to be, error
    elif "globus_publish" in services:
        raise ValueError("Unable to Publish data without location")

    # Same for integrated service data
    if service_loc:
        service_data = os.path.join(app.config["SERVICE_DATA"], status_id) + "/"
        dl_res = download_and_backup(transfer_client,
                                     service_loc,
                                     app.config["LOCAL_EP"],
                                     service_data)
        if not dl_res["success"]:
            raise IOError("No data downloaded")

    # Globus Search (mandatory)
    try:
        search_ingest(search_client, base_feed_path, index=app.config["INGEST_INDEX"],
                        feedstock_save=final_feed_path)
    except Exception as e:
        # TODO: Update status - ingest failed
        raise Exception("Search error:" + str({
            "success": False,
            "error": repr(e)
            }))

    # Other services use the dataset information
    if services:
        with open(final_feed_path) as f:
            dataset = json.loads(f.readline())

    # Globus Publish
    # TODO: Test after Publish API is fixed
    if "globus_publish" in services:
        # Get DC metadata
        try:
            fin_res = globus_publish_data(publish_client, transfer_client,
                                          dataset, local_path)
        except Exception as e:
            # TODO: Update status - Publish failed
            print("Publish ERROR:", repr(e))
        else:
            # TODO: Update status - Publish success
            print("DEBUG: Publish success:", fin_res)

    # Citrine
    if "citrine" in services:
        try:
            cit_res = citrine_upload(os.path.join(service_data, "citrine"),
                           app.config["CITRINATION_API_KEY"],
                           dataset)
            if not cit_res["success"]:
                raise ValueError("No data uploaded to Citrine: " + str(cit_res))
        except Exception as e:
            # TODO: Update status, Citrine upload failed
            print("Citrine upload failed:", repr(e))

    # TODO: Update status - ingest successful, processing complete
    print("DEBUG: Ingest success, processing complete")
    return {
        "success": True,
        "status_id": status_id
        }


def globus_publish_data(publish_client, transfer_client, metadata, local_path):
    # Submit metadata
    try:
        pub_md = get_publish_metadata(metadata)
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
        "collection_id": app.config["DEFAULT_PUBLISH_COLLECTION"],
        "accept_license": True
    }
    return pub_metadata


def citrine_upload(citrine_data, api_key, mdf_dataset):
    cit_client = CitrinationClient(api_key)
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

    cit_ds_id = cit_client.create_data_set(name=cit_title,
                                        description=cit_desc,
                                        share=0).json()["id"]
    if not cit_ds_id:
        raise ValueError("Dataset name present in Citrine")

    for _, _, files in os.walk(citrine_data):
        for pif in files:
            up_res = json.loads(cit_client.upload(cit_ds_id, pif_file.name))
            if not up_res["success"]:
                # TODO: Handle errors
                print("DEBUG: Citrine upload failure:", up_res.get("status"))
    # TODO: Set share to 1 to enable public uploads
    cit_client.update_data_set(cit_ds_id, share=0)

    return {
        "success": True
        }


@app.route("/status", methods=["GET", "POST"])
def status():
    return jsonify({"success": False, "message": "Not implemented yet, try again later"})
