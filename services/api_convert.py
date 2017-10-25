import os
import shutil
import json
import zipfile
from threading import Thread

from bson import ObjectId
from flask import Flask, request

from mdf_forge import toolbox

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

    success = False
    backup_tid = None
    user_tid = None
    local_path = os.path.join(app.config["LOCAL_PATH"], metadata["mdf_status_id"]) + "/"
    os.mkdir(local_path)

    if metadata.get("zip"):
        zip_path = os.path.join(local_path, metadata["mdf_status_id"] + ".zip")
        res = requests.get(metadata["zip"])
        with open(zip_path, 'wb') as out:
            out.write(res.content)
        zipfile.ZipFile(zip_path).extractall(local_path)
        os.remove(zip_path)
        # Do stuff with data

        backup_tid = toolbox.quick_transfer(mdf_transfer_client, app.config["LOCAL_EP"], app.config["BACKUP_EP"], [(local_path, app.config["BACKUP_PATH"])], timeout=-1)
        success = True
    elif metadata.get("globus"):
        user_ep, user_path = metadata["globus"].split("/", 1)
        user_path = "/" + user_path
        if user_path.endswith("/"):
            remote_path = app.config["BACKUP_PATH"]
        else:
            local_path = os.path.join(local_path, metadata["mdf_status_id"] + ".file")
            remote_path = os.path.join(app.config["BACKUP_PATH"], metadata["mdf_status_id"] + ".file")
        user_tid = toolbox.quick_transfer(mdf_transfer_client, user_ep, app.config["LOCAL_EP"], [(user_path, local_path)])
        # Do stuff with data
        backup_tid = toolbox.quick_transfer(mdf_transfer_client, app.config["LOCAL_EP"], app.config["BACKUP_EP"], [(local_path, remote_path)], timeout=-1)
        success = True
    else:
        # Nothing to do
        pass

    # Remove local data
    # TODO: Remove data after transfer success
#    shutil.rmtree(local_path)
    # TODO: Log backup_tid and user_tid with status DB
    return json.dumps({
        "success": success,
        "transfer_id_mdf": backup_tid,
        "transfer_id_user": user_tid
        })


