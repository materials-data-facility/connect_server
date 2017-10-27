import os
import json
from threading import Thread

from bson import ObjectId
from flask import Flask, request

from mdf_forge import toolbox
from mdf_refinery import validator, ingester

app = Flask(__name__)
app.config.from_pyfile("api.conf")


@app.route("/ingest", methods=["POST"])
def accept_ingest():
    """Accept the JSON feedstock file and begin the ingestion process."""
    # Check that file exists and is valid
    feedstock = request.files.get("file")
    if not feedstock:
        return {
            "success": False,
            "error": "No feedstock file uploaded"
            }
    elif not feedstock.filename.endswith(".json"):
        return {
            "success": False,
            "error": "Feedstock file must be JSON"
            }
    # Mint/update status ID
    if not request.form.get("mdf_status_id"):
        status_id = str(ObjectId())
        #TODO: Register status ID
    else:
        #TODO: Check that status exists (must not be set by user)
        #TODO: Update status - ingest request recieved
        status_id = request.form.get("mdf_status_id")
    # Save file
    feed_path = os.path.join(app.config["FEEDSTOCK_PATH"], secure_filename(feedstock.filename))
    feedstock.save(feed_path)
    ingester = Thread(target=begin_ingest, name="ingester_thread", args=(feed_path, status_id))
    ingester.start()
    return {
        "success": True,
        "status_id": status_id
        }


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
    with open(base_feed_path) as base_stock, open(final_feed_path, 'w') as final_stock:
        # Finalize dataset entry
        dataset_result = validator.validate_dataset(json.loads(base_stock.readline()), finalize=True)
        if not dataset_result["success"]:
            # TODO: Update status - dataset validation failed
            return dataset_result
        json.dump(dataset_result["valid"], final_stock)
        final_stock.write("\n")

        # Finalize records
        for rc in base_stock:
            record = json.loads(rc)
            record_result = validator.validate_record(record, finalize=True)
            if not record_result["success"]:
                #TODO: Update status - record validation failed
                return record_result
            json.dump(record_result["valid"], final_stock)
            final_stock.write("\n")
    #TODO: Update status - validation passed

    # Ingest finalized feedstock
    try:
        ingester.ingest(search_client, final_feed_path)
    except Exception as e:
        #TODO: Update status - ingest failed
        return {
            "success": False,
            "error": repr(e)
            }
    #TODO: Update status - ingest successful, processing complete
    return {
        "success": True,
        "status_id": status_id
        }

