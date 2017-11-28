from hashlib import sha512
import json
import os
import re
import shutil
import tempfile
from threading import Thread
import zipfile

from bson import ObjectId
from flask import jsonify, request
import magic
from mdf_toolbox import toolbox
from mdf_refinery import ingester, omniparser, validator
import requests
from werkzeug.utils import secure_filename

from services import app

KEY_FILES = {
    "dft": {
        "exact": ["outcar"],
        "extension": [],
        "regex": []
    }
}


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
        "services": ["transfer"]  # , "publish"]
        }
    clients = toolbox.confidential_login(creds)
    mdf_transfer_client = clients["transfer"]
#    globus_publish_client = clients["publish"]

    status_id = metadata["mdf_status_id"]

    # Download data locally, back up on MDF resources
    dl_res = download_and_backup(mdf_transfer_client, metadata)
    if dl_res["success"]:
        local_path = dl_res["local_path"]
        backup_path = dl_res["backup_path"]
    else:
        raise IOError("No data downloaded")
    # TODO: Update status - data downloaded
    print("DEBUG: Data downloaded")

    print("DEBUG: Conversions started")
    # TODO: Parse out dataset entry
    mdf_dataset = metadata

    # TODO: Stream data into files instead of holding feedstock in memory
    feedstock = [mdf_dataset]

    # TODO: Parse tags
    tags = []
    key_info = get_key_matches(tags or None)

    # List of all files, for bag
    all_files = []

    # TODO: Create Citrine dataset
    citrine_ds_id = 0

    for path, dirs, files in os.walk(local_path):
        # Determine if dir or file is single entity
        # Dir is record
        if count_key_files(files, key_info) == 1:
            dir_file_md = []
            mdf_record = {}
            # Process all files into one record
            for filename in files:
                # Get file metadata
                file_md = get_file_metadata(file_path=os.path.join(path, filename),
                                            backup_path=os.path.join(backup_path, path, filename))
                # Save file metadata
                all_files.append(file_md)
                dir_file_md.append(file_md)
                with open(os.path.join(path, filename)) as data_file:
                    # MDF parsing
                    mdf_res = omniparser.omniparse(data_file)
                    data_file.seek(0)

                    mdf_record = toolbox.dict_merge(mdf_record, mdf_res)

            '''
            # Citrine parsing
            cit_pifs, = generate_pifs(path),
                                     includes=[], excludes=[])
            cit_pifs, = get_uuids(cit_pifs, citrine_ds_id)
            # Get MDF feedstock from PIFs
            cit_res = pif_to_feedstock(cit_pifs)
            # TODO: enrich links, dc md
            cit_pifs = enrich_pifs(cit_pifs, links, dc_metadata)
            '''
            cit_res = {}

            # Merge results
            mdf_record = toolbox.dict_merge(mdf_record, cit_res)

            # If data was parsed, save record
            if mdf_record:
                mdf_record = toolbox.dict_merge(mdf_record,
                                                {"files": dir_file_md})
                feedstock.append(mdf_record)
                # TODO: Upload PIF

        # File is record
        else:
            for filename in files:
                # Get file metadata
                file_md = get_file_metadata(file_path=os.path.join(path, filename),
                                            backup_path=os.path.join(backup_path, path, filename))
                # Save file metadata
                all_files.append(file_md)
                with open(os.path.join(path, filename)) as data_file:
                    # MDF parsing
                    mdf_res = omniparser.omniparse(data_file)
                    data_file.seek(0)

                    '''
                    # Citrine parsing
                    cit_pifs, = generate_pifs(os.path.join(path, filename),
                                             includes=[], excludes=[])
                    cit_pifs, = get_uuids(cit_pifs, citrine_ds_id)
                    # Get MDF feedstock from PIFs
                    cit_res = pif_to_feedstock(cit_pifs)
                    # TODO: enrich links, dc md
                    cit_pifs = enrich_pifs(cit_pifs, links, dc_metadata)
                    '''
                    cit_res = {}

                    # Merge results
                    mdf_record = toolbox.dict_merge(mdf_res, cit_res)

                    # If data was parsed, save record
                    if mdf_record:
                        mdf_record = toolbox.dict_merge(mdf_record,
                                                        {"files": [file_md]})
                        feedstock.append(mdf_record)
                        # TODO: Upload PIF

    # TODO: Update status - indexing success
    print("DEBUG: Indexing success")

    # Pass feedstock to /ingest
    with tempfile.TemporaryFile(mode="w+") as stock:
        for entry in feedstock:
            json.dump(entry, stock)
            stock.write("\n")
        stock.seek(0)
        ingest_res = requests.post(app.config["INGEST_URL"],
                                   data={"status_id": status_id},
                                   files={'file': stock})
    if not ingest_res.json().get("success"):
        # TODO: Update status? Ingest failed
        # TODO: Fail everything, delete Citrine dataset, etc.
        raise ValueError("In convert - Ingest failed" + str(ingest_res.json()))

    # Pass data to additional integrations

    # Globus Publish
    # TODO: Enable after Publish API is fixed
    #       And after datapublication is a service in globus_sdk
    if False:  # metadata.get("globus_publish"):
        # Submit metadata
        try:
            pub_md = metadata["globus_publish"]
            md_result = globus_publish_client.push_metadata(pub_md["collection"], pub_md)
            pub_endpoint = md_result['globus.shared_endpoint.name']
            pub_path = os.path.join(md_result['globus.shared_endpoint.path'], "data") + "/"
            submission_id = md_result["id"]
        except Exception as e:
            # TODO: Update status - not Published due to bad metadata
            raise
        # Transfer data
        try:
            toolbox.quick_transfer(mdf_transfer_client, app.config["LOCAL_EP"],
                                   pub_endpoint, [(local_path, pub_path)], timeout=0)
        except Exception as e:
            # TODO: Update status - not Published due to failed Transfer
            raise
        # Complete submission
        try:
            fin_res = globus_publish_client.complete_submission(submission_id)
        except Exception as e:
            # TODO: Update status - not Published due to Publish error
            raise
        # TODO: Update status - Publish success
        print("DEBUG: Publish success")

    # Remove local data
    shutil.rmtree(local_path)
    return {
        "success": True,
        "status_id": status_id
        }


def download_and_backup(mdf_transfer_client, metadata):
    """Download remote data, backup"""
    status_id = metadata["mdf_status_id"]
    local_success = False
    local_path = os.path.join(app.config["LOCAL_PATH"], status_id) + "/"
    backup_path = os.path.join(app.config["BACKUP_PATH"], status_id) + "/"
    os.makedirs(local_path, exist_ok=True)  # TODO: exist not okay when status is real

    # Download data locally
    if metadata.get("zip"):
        # Download and unzip
        zip_path = os.path.join(local_path, metadata["mdf_status_id"] + ".zip")
        res = requests.get(metadata["zip"])
        with open(zip_path, 'wb') as out:
            out.write(res.content)
        zipfile.ZipFile(zip_path).extractall()  # local_path)
        os.remove(zip_path)  # TODO: Should the .zip be removed?
        local_success = True

    elif metadata.get("globus"):
        # Parse out EP and path
        # Right now, path assumed to be a directory
        user_ep, user_path = metadata["globus"].split("/", 1)
        user_path = "/" + user_path + ("/" if not user_path.endswith("/") else "")
        # Transfer locally
        toolbox.quick_transfer(mdf_transfer_client, user_ep, app.config["LOCAL_EP"],
                               [(user_path, local_path)], timeout=0)
        local_success = True

    elif metadata.get("files"):
        # TODO: Implement this
        pass

    else:
        # Nothing to do
        pass

    # TODO: Update status - download success/failure
    if not local_success:
        raise IOError("No data downloaded")
    print("DEBUG: Download success")

    # Backup data
    toolbox.quick_transfer(mdf_transfer_client,
                           app.config["LOCAL_EP"], app.config["BACKUP_EP"],
                           [(local_path, backup_path)], timeout=0)
    # TODO: Update status - backup success
    print("DEBUG: Backup success")

    return {
        "success": True,
        "local_path": local_path,
        "backup_path": backup_path
        }


def get_key_matches(tags=None):
    exa = []
    ext = []
    rex = []
    for tag, val in KEY_FILES.items():
        if not tags or tag in tags:
            for key in val.get("exact", []):
                exa.append(key.lower())
            for key in val.get("extension", []):
                ext.append(key.lower())
            for key in val.get("regex", []):
                rex.append(re.compile(key))
    return {
        "exact_keys": exa,
        "extension_keys": ext,
        "regex_keys": rex
    }


def count_key_files(files, key_info):
    return len([f for f in files
                if (f.lower() in key_info["exact_keys"]
                    or any([f.lower().endswith(ext) for ext in key_info["extension_keys"]])
                    or any([rx.match(f) for rx in key_info["regex_keys"]]))])


def get_file_metadata(file_path, backup_path):
    with open(file_path, "rb") as f:
        md = {
            "globus_endpoint": app.config["BACKUP_EP"] + backup_path,
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
    # Mint/update status ID
    if not request.form.get("mdf_status_id"):
        status_id = str(ObjectId())
        # TODO: Register status ID
        print("DEBUG: New status ID created")
    else:
        # TODO: Check that status exists (must not be set by user)
        # TODO: Update status - ingest request recieved
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
        # TODO: Remove after Validator is finished
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
            # TODO: Remove after Validator finished
            json.dump(record, final_stock)
#            record_result = validator.validate_record(record, finalize=True)
#            if not record_result["success"]:
                # TODO: Update status - record validation failed
#                return jsonify(record_result)
#            json.dump(record_result["valid"], final_stock)
            final_stock.write("\n")
    # TODO: Update status - validation passed
    print("DEBUG: Validation 'passed'")

    # Ingest finalized feedstock
    try:
        ingester.ingest(search_client, final_feed_path)
    except Exception as e:
        # TODO: Update status - ingest failed
        return jsonify({
            "success": False,
            "error": repr(e)
            })
    # TODO: Update status - ingest successful, processing complete
    print("DEBUG: Ingest success, processing complete")
    return {
        "success": True,
        "status_id": status_id
        }


@app.route("/status", methods=["GET", "POST"])
def status():
    return jsonify({"success": False, "message": "Not implemented yet, try again later"})
