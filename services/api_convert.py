import os
import shutil
import json
import zipfile
from threading import Thread

import requests
from bson import ObjectId
from flask import Flask, request

from mdf_forge import toolbox
from mdf_refinery.omniconverter import omniconvert

app = Flask(__name__)
app.config.from_pyfile("api.conf")


@app.route('/convert', methods=["POST"])
def accept_convert():
    """Accept the JSON metadata and begin the conversion process."""
    metadata = request.get_json(force=True, silent=True)
    if not metadata:
        return {
            "success": False,
            "error": "POST data empty or not JSON"
            }
    status_id = str(ObjectId())
    # TODO: Register status ID
    metadata["mdf_status_id"] = status_id
    converter = Thread(target=begin_convert, name="converter_thread", args=(metadata,))
    converter.start()
    return {
        "success": True,
        "status_id": status_id
        }


def begin_convert(metadata):
    """Pull, back up, and convert metadata."""
    # Will need transfer client for backups
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["transfer"]
        }
    mdf_transfer_client = toolbox.confidential_login(creds)["transfer"]

    local_success = False
    backup_tid = None
    user_tid = None
    local_path = os.path.join(app.config["LOCAL_PATH"], metadata["mdf_status_id"]) + "/"
    backup_path = os.path.join(app.config["BACKUP_PATH"], metadata["mdf_status_id"]) + "/"
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

    # Backup data
    backup_tid = toolbox.quick_transfer(mdf_transfer_client, app.config["LOCAL_EP"], app.config["BACKUP_EP"], [(local_path, backup_path)], timeout=0)
    #TODO: Update status - backup success

    # Trigger omniconverter
    try:
        feedstock_results = omniconvert(local_path, metadata, feedstock_path=None)
    except Exception as e:
        #TODO: Update status - indexing failed
        raise
    num_records = feedstock_results["records_processed"]
    num_rec_failed = feedstock_results["num_failures"]
    feedstock = feedstock_results["feedstock"]
    #TODO: Update status - indexing success, give numbers success/fail

    # Pass feedstock to /ingest
#    requests.post(app.config["INGEST_URL"], data=feedstock)

    # Remove local data
    # TODO: Remove data after transfer success
#    shutil.rmtree(local_path)
    # TODO: Log backup_tid and user_tid with status DB
    return json.dumps({
        "success": True,
        #TODO: Remove dev result
        "feedstock": feedstock
        })

