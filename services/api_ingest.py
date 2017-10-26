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
    """Accept the JSON feedstock and begin the ingestion process."""
    feedstock = request.get_json(force=True, silent=True)
    if not feedstock:
        return {
            "success": False,
            "error": "POST data empty or not JSON"
            }
    # Separate dataset from records
    dataset = feedstock[0]
    records = feedstock[1:]
    # Mint/update status ID
    if not dataset.get("mdf_status_id"):
        status_id = str(ObjectId())
        dataset["mdf_status_id"] = status_id
        #TODO: Register status ID
    else:
        #TODO: Update status - ingest request recieved
        status_id = dataset["mdf_status_id"]
    ingester = Thread(target=begin_ingest, name="ingester_thread", args=(dataset,records))
    ingester.start()
    return {
        "success": True,
        "status_id": status_id
        }


def begin_ingest(dataset, records):
    """Finalize and ingest feedstock."""
    # Will need client to ingest data
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["search_ingest"]
        }
    search_client = toolbox.confidential_login(creds)["search_client"]

    # Validate feedstock
    dataset_result = validator.validate_dataset(dataset)
    if 


