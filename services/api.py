import json
import os
import shutil
from threading import Thread
import zipfile

from bson import ObjectId
from flask import jsonify, request
from mdf_toolbox import toolbox
from mdf_refinery import ingester, omniconverter, validator
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
    metadata["mdf_status_id"] = status_id
    converter = Thread(target=begin_convert, name="converter_thread", args=(metadata, status_id))
    converter.start()
    return jsonify({
        "success": True,
        "status_id": status_id
        })


def begin_convert(metadata, status_id):
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

    status_id = metadata["mdf_status_id"]

    # Download data locally, back up on MDF resources
    dl_res = download_and_backup(mdf_transfer_client, metadata)
    if dl_res["success"]:
        local_path = dl_res["local_path"]
    else:
        raise IOError("No data downloaded")
    #TODO: Update status - data downloaded
    print("Data downloaded")

    #TODO: Update status - MDF conversion started
    print("MDF conversion started")
    feedstock_path = os.path.join(app.config["FEEDSTOCK_PATH"], status_id + "_basic.json")
    try:
        feedstock_results = omniconverter.omniconvert(local_path,
                                metadata, feedstock_path=feedstock_path)
    except Exception as e:
        #TODO: Update status - indexing failed
        raise
    num_records = feedstock_results["records_processed"]
    num_rec_failed = feedstock_results["num_failures"]
    #TODO: Update status - indexing success, give numbers success/fail
    print("DEBUG: Indexing success\nSuccess:", num_records, "\nFail:", num_rec_failed)

    # Attempt Citrine conversion flow
    records = []
    try:
        # Create new Citrine dataset
        #TODO
        citrine_ds_id = 0

        # Trigger conversion
        pifs, ignored = generate_pifs(local_path, includes=[], excludes=[])

        # Process PIFs
        pifs, pif_urls = get_uuids(pifs, citrine_ds_id)

        # Get MDF records
        records = pif_to_feedstock(pifs)

        # Enrich PIFs
        pifs = enrich_pifs(pifs, REPLACE_PATH_HOST, metadata)
    except Exception as e:
        #TODO: Update status - Citrine parsing failed
        records = []


        

    # Pass feedstock to /ingest
    with open(feedstock_path) as stock:
        requests.post(app.config["INGEST_URL"], data={"status_id":status_id}, files={'file': stock})


    # Pass data to additional integrations

    # Globus Publish
    #TODO: Enable after Publish API is fixed
    if False: #metadata.get("globus_publish"):
        # Submit metadata
        try:
            pub_md = metadata["globus_publish"]
            md_result = globus_publish_client.push_metadata(pub_md["collection"], pub_md)
            pub_ep = md_result['globus.shared_endpoint.name']
            pub_path = os.path.join(md_result['globus.shared_endpoint.path'], "data") + "/"
            submission_id = md_result["id"]
        except Exception as e:
            #TODO: Update status - not Published due to bad metadata
            raise
        # Transfer data
        try:
            toolbox.quick_transfer(mdf_transfer_client, app.config["LOCAL_EP"], pub_endpoint, [(local_path, pub_path)], timeout=0)
        except Exception as e:
            #TODO: Update status - not Published due to failed Transfer
            raise
        # Complete submission
        try:
            fin_res = globus_publish_client.complete_submission(submission_id)
        except Exception as e:
            #TODO: Update status - not Published due to Publish error
            raise
        #TODO: Update status - Publish success
        print("DEBUG: Publish success")


    # Remove local data
    shutil.rmtree(local_path)
    # TODO: Log backup_tid and user_tid with status DB
    return {
        "success": True,
        "status_id": status_id
        }


def download_and_backup(mdf_transfer_client, metadata):
    """Download remote data, backup"""
    status_id = metadata["mdf_status_id"]
    local_success = False
    backup_tid = None
    user_tid = None
    local_path = os.path.join(app.config["LOCAL_PATH"], status_id) + "/"
    backup_path = os.path.join(app.config["BACKUP_PATH"], status_id) + "/"
    os.makedirs(local_path, exist_ok=True) #TODO: exist not okay when status is real

    # Download data locally
    if metadata.get("zip"):
        # Download and unzip
        zip_path = os.path.join(local_path, metadata["mdf_status_id"] + ".zip")
        res = requests.get(metadata["zip"])
        with open(zip_path, 'wb') as out:
            out.write(res.content)
        zipfile.ZipFile(zip_path).extractall()#local_path)
        os.remove(zip_path) #TODO: Should the .zip be removed?
        local_success = True

    elif metadata.get("globus"):
        # Parse out EP and path
        # Right now, path assumed to be a directory
        user_ep, user_path = metadata["globus"].split("/", 1)
        user_path = "/" + user_path + ("/" if not user_path.endswith("/") else "")
        # Transfer locally
        user_tid = toolbox.quick_transfer(mdf_transfer_client, user_ep, app.config["LOCAL_EP"], [(user_path, local_path)], timeout=0)
        local_success = True

    elif metadata.get("files"):
        # TODO: Implement this
        pass

    else:
        # Nothing to do
        pass

    #TODO: Update status - download success/failure
    if not local_success:
        raise IOError("No data downloaded")
    print("DEBUG: Download success")

    # Backup data
    backup_tid = toolbox.quick_transfer(mdf_transfer_client, app.config["LOCAL_EP"], app.config["BACKUP_EP"], [(local_path, backup_path)], timeout=0)
    #TODO: Update status - backup success
    print("DEBUG: Backup success")

    return {
        "success": True,
        "local_path": local_path
        }


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
    if not feedstock.filename.endswith(".json"):
        return jsonify({
            "success": False,
            "error": "Feedstock file must be JSON"
            })
    # Mint/update status ID
    if not request.form.get("mdf_status_id"):
        status_id = str(ObjectId())
        #TODO: Register status ID
        print("DEBUG: New status ID created")
    else:
        #TODO: Check that status exists (must not be set by user)
        #TODO: Update status - ingest request recieved
        status_id = request.form.get("mdf_status_id")
        print("DEBUG: Current status ID read")
    # Save file
    feed_path = os.path.join(app.config["FEEDSTOCK_PATH"], secure_filename(feedstock.filename))
    feedstock.save(feed_path)
    ingester = Thread(target=begin_ingest, name="ingester_thread", args=(feed_path, status_id))
    ingester.start()
    return jsonify({
        "success": True,
        "status_id": status_id
        })


def begin_ingest(base_feed_path, status_id):
    """Finalize and ingest feedstock."""
    # Will need client to ingest data
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["search_ingest"],
        "index": app.config["INGEST_INDEX"]
        }
    search_client = toolbox.confidential_login(creds)["search_ingest"]
    final_feed_path = os.path.join(app.config["FEEDSTOCK_PATH"], status_id + "_final.json")

    # Finalize feedstock
    with open(base_feed_path, 'r') as base_stock, open(final_feed_path, 'w') as final_stock:
        # Finalize dataset entry
        #TODO: Remove after Validator is finished
        json.dump(json.loads(base_stock.readline()), final_stock)
#        dataset_result = validator.validate_dataset(json.loads(base_stock.readline()),
#                                                     finalize=True)
#        if not dataset_result["success"]:
            # TODO: Update status - dataset validation failed
#            return jsonify(dataset_result)
#        json.dump(dataset_result["valid"], final_stock)
        final_stock.write("\n")

        # Finalize records
        for rc in base_stock:
            record = json.loads(rc)
            #TODO: Remove after Validator finished
            json.dump(record, final_stock)
#            record_result = validator.validate_record(record, finalize=True)
#            if not record_result["success"]:
                #TODO: Update status - record validation failed
#                return jsonify(record_result)
#            json.dump(record_result["valid"], final_stock)
            final_stock.write("\n")
    #TODO: Update status - validation passed
    print("DEBUG: Validation 'passed'")

    # Ingest finalized feedstock
    try:
        ingester.ingest(search_client, final_feed_path)
    except Exception as e:
        #TODO: Update status - ingest failed
        return jsonify({
            "success": False,
            "error": repr(e)
            })
    #TODO: Update status - ingest successful, processing complete
    print("DEBUG: Ingest success, processing complete")
    return {
        "success": True,
        "status_id": status_id
        }


@app.route("/status", methods=["GET", "POST"])
def status():
    return jsonify({"success": False, "message": "Not implemented yet, try again later"})

