from datetime import datetime, date
import json
import os
import tempfile
from threading import Thread
import zipfile

import boto3
from bson import ObjectId
from citrination_client import CitrinationClient
from flask import jsonify, request
import globus_sdk
from mdf_toolbox import toolbox
import requests
from werkzeug.utils import secure_filename

from mdf_refinery import convert, search_ingest
from moc import app

# Frequency of status messages printed to console
# Level 0: No messages
# Level 1: Messages only when the status DB is updated
# Level 2: Messages when something happens in a driver
# Level 3 (in progress): Messages whenever anything happens in this module
DEBUG_LEVEL = 1

# DynamoDB setup
DMO_CLIENT = boto3.resource('dynamodb',
                            aws_access_key_id=app.config["DYNAMO_KEY"],
                            aws_secret_access_key=app.config["DYNAMO_SECRET"],
                            region_name="us-east-1")
DMO_TABLE = app.config["DYNAMO_TABLE"]
DMO_SCHEMA = {
    "TableName": DMO_TABLE,
    "AttributeDefinitions": [{
        "AttributeName": "status_id",
        "AttributeType": "S"
    }],
    "KeySchema": [{
        "AttributeName": "status_id",
        "KeyType": "HASH"
    }],
    "ProvisionedThroughput": {
        "ReadCapacityUnits": 20,
        "WriteCapacityUnits": 20
    }
}
STATUS_STEPS = (
    ("convert_start", "Conversion initialization"),
    ("convert_download", "Conversion data download"),
    ("converting", "Data conversion"),
    ("convert_ingest", "Ingestion preparation"),
    ("ingest_start", "Ingestion initialization"),
    ("ingest_download", "Ingestion data download"),
    ("ingest_integration", "Integration data download"),
    ("ingest_search", "Globus Search ingestion"),
    ("ingest_publish", "Globus Publish publication"),
    ("ingest_citrine", "Citrine upload")
)
# This is the start of ingest steps in STATUS_STEPS
# In other words, the ingest steps are STATUS_STEPS[INGEST_MARK:]
# and the convert steps are STATUS_STEPS[:INGEST_MARK]
INGEST_MARK = 4


@app.route('/convert', methods=["POST"])
def accept_convert():
    """Accept the JSON metadata and begin the conversion process."""
    auth_head = request.headers.get("Authorization")
    if not auth_head:
        return (jsonify({
            "success": False,
            "error": "Not Authenticated"
            }), 401)
    try:
        auth_head = auth_head.replace("Bearer ", "")
        auth_client = globus_sdk.ConfidentialAppAuthClient(app.config["API_CLIENT_ID"],
                                                           app.config["API_CLIENT_SECRET"])
        auth_res = auth_client.oauth2_token_introspect(auth_head, include="identities_set")
    except Exception as e:
        return (jsonify({
            "success": False,
            # TODO: Check that the exception doesn't leak info
            "error": "Unacceptable auth: " + repr(e)
            }), 400)
    if not auth_res:
        return (jsonify({
            "success": False,
            "error": "Token could not be validated"
            }), 401)
    # Check that token is active
    if not auth_res["active"]:
        return (jsonify({
            "success": False,
            "error": "Token expired"
            }), 403)
    # Check correct scope and audience
    if (app.config["API_SCOPE"] not in auth_res["scope"]
        or app.config["API_SCOPE_ID"] not in auth_res["aud"]):

        return (jsonify({
            "success": False,
            "error": "Not authorized to MOC scope"
            }), 401)
    # Finally, verify that user ID is in whitelist
    # Can be any identity the user has (MOC is identity-aware)
    if not any([uid in app.config["CONVERT_WHITELIST"] for uid in auth_res["identities_set"]]):
        # TODO: Proper logging
        print("DEBUG: User not in whitelist:", auth_res["username"])
        return (jsonify({
            "success": False,
            "error": "You cannot access this service (yet)"
            }), 403)
    user_id = auth_res["sub"]
    # username = auth_res["username"]
    name = auth_res["name"] or "Not given"
    email = auth_res["email"] or "Not given"

    metadata = request.get_json(force=True, silent=True)
    if not metadata:
        return (jsonify({
            "success": False,
            "error": "POST data empty or not JSON"
            }), 400)
    try:
        sub_title = metadata["dc"]["titles"][0]["title"]
    except (KeyError, ValueError):
        return (jsonify({
            "success": False,
            "error": "No title supplied"
            }), 400)
    status_id = str(ObjectId())
    status_info = {
        "status_id": status_id,
        "submission_code": "C",
        "submission_time": datetime.utcnow().isoformat("T") + "Z",
        "submitter": name,
        "title": sub_title,
        "user_id": user_id,
        "user_email": email
        }
    try:
        # TODO: Better metadata validation
        status_res = create_status(status_info)
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": repr(e)
            }), 500)
    if not status_res["success"]:
        return (jsonify(status_res), 500)

    driver = Thread(target=moc_driver, name="driver_thread", args=(metadata, status_id))
    driver.start()
    return (jsonify({
        "success": True,
        "status_id": status_id
        }), 202)


def moc_driver(metadata, status_id):
    """The driver function for MOC.
    Modifies the status database as steps are completed.

    Arguments:
    metadata (dict): The JSON passed to /convert.
    status_id (str): The ID of this submission.

    Returns:
    dict: success (bool): True on success, False on failure.
    """
    # Setup
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["transfer", "moc"]
        }
    try:
        clients = toolbox.confidential_login(creds)
        transfer_client = clients["transfer"]
        moc_authorizer = clients["moc"]
    except Exception as e:
        stat_res = update_status(status_id, "convert_start", "F", text=repr(e))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        else:
            return
    stat_res = update_status(status_id, "convert_start", "S")
    if not stat_res["success"]:
        raise ValueError(str(stat_res))

    # Download data locally, back up on MDF resources
    stat_res = update_status(status_id, "convert_download", "P")
    if not stat_res["success"]:
        raise ValueError(str(stat_res))
    local_path = os.path.join(app.config["LOCAL_PATH"], status_id) + "/"
    backup_path = os.path.join(app.config["BACKUP_PATH"], status_id) + "/"
    try:
        dl_res = download_and_backup(transfer_client,
                                     metadata.pop("data", {}),
                                     app.config["LOCAL_EP"],
                                     local_path)
    except Exception as e:
        stat_res = update_status(status_id, "convert_download", "F", text=repr(e))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        else:
            return
    if not dl_res["success"]:
        stat_res = update_status(status_id, "convert_download", "F", text=str(dl_res))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        else:
            return
    else:
        stat_res = update_status(status_id, "convert_download", "S")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        if DEBUG_LEVEL >= 2:
            print("{}: Data downloaded".format(status_id))

    # Handle service integration data directory
    service_data = os.path.join(app.config["SERVICE_DATA"], status_id) + "/"
    os.makedirs(service_data)

    # Pull out special fields in metadata (the rest is the dataset)
    services = metadata.pop("services", [])
    parse_params = metadata.pop("index", {})
    # Add file info data
    parse_params["file"] = {
        "globus_endpoint": app.config["BACKUP_EP"],
        "http_host": app.config["BACKUP_HOST"],
        "local_path": local_path,
        "host_path": backup_path
    }
    convert_params = {
        "dataset": metadata,
        "parsers": parse_params,
        "service_data": service_data
    }

    # Convert data
    stat_res = update_status(status_id, "converting", "P")
    if not stat_res["success"]:
        raise ValueError(str(stat_res))
    try:
        feedstock = convert(local_path, convert_params)
    except Exception as e:
        stat_res = update_status(status_id, "converting", "F", text=repr(e))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        else:
            return
    else:
        stat_res = update_status(status_id, "converting", "M",
                                 text="{} entries parsed".format(len(feedstock)))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        if DEBUG_LEVEL >= 2:
            print("{}: {} entries parsed".format(status_id, len(feedstock)))

    # Pass dataset to /ingest
    stat_res = update_status(status_id, "convert_ingest", "P")
    if not stat_res["success"]:
        raise ValueError(str(stat_res))
    try:
        with tempfile.TemporaryFile(mode="w+") as stock:
            for entry in feedstock:
                json.dump(entry, stock)
                stock.write("\n")
            stock.seek(0)
            ingest_args = {
                "status_id": status_id,
                "data": json.dumps(["globus://" + app.config["LOCAL_EP"] + local_path]),
                "services": services,
                "service_data": json.dumps(["globus://" + app.config["LOCAL_EP"] + service_data])
            }
            headers = {}
            moc_authorizer.set_authorization_header(headers)
            ingest_res = requests.post(app.config["INGEST_URL"],
                                       data=ingest_args,
                                       files={'file': stock},
                                       headers=headers,
                                       # TODO: Verify after getting real cert
                                       verify=False)
    except Exception as e:
        stat_res = update_status(status_id, "convert_ingest", "F", text=repr(e))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        else:
            return
    else:
        if ingest_res.json().get("success"):
            stat_res = update_status(status_id, "convert_ingest", "S")
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
        else:
            stat_res = update_status(status_id, "convert_ingest", "F", text=str(ingest_res.json()))
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
            else:
                return

    return {
        "success": True,
        "status_id": status_id
        }


def download_and_backup(mdf_transfer_client, data_loc, local_ep, local_path):
    """Download data from a remote host to the configured machine.

    Arguments:
    mdf_transfer_client (TransferClient): An authenticated TransferClient.
    data_loc (list of str): The location(s) of the data.
    local_ep (str): The local machine's endpoint ID.
    local_path (str): The path ot the local storage location.

    Returns:
    dict: success (bool): True on success, False on failure.
    """
    os.makedirs(local_path, exist_ok=True)
    if not isinstance(data_loc, list):
        raise TypeError("Data locations must be in a list")
    # Download data locally
    for location in data_loc:
        try:
            protocol, data_info = location.split("://", 1)
        except ValueError:
            raise ValueError("Data location must be in the form [protocol]://[data_location]")
        if protocol == "globus":
            # Check that data not already in place
            if data_info != local_ep + local_path:
                # Parse out EP and path
                # Right now, path assumed to be a directory
                try:
                    user_ep, user_path = data_info.split("/", 1)
                except ValueError:
                    raise ValueError(("Globus link must be in the form "
                                      "'[endpoint_id]/path/to/data_directory/"))
                user_path = "/" + user_path + ("/" if not user_path.endswith("/") else "")
                # Transfer locally
                toolbox.quick_transfer(mdf_transfer_client, user_ep, app.config["LOCAL_EP"],
                                       [(user_path, local_path)], timeout=0)

        elif protocol == "http" or protocol == "https":
            # Determine file type
            if location.endswith(".zip"):
                # Download and unzip
                zip_path = os.path.join(local_path, "archive.zip")
                res = requests.get(location)
                with open(zip_path, 'wb') as out:
                    out.write(res.content)
                zipfile.ZipFile(zip_path).extractall(local_path)
                os.remove(zip_path)

        else:
            # Nothing to do
            raise IOError("Invalid data location: " + str(location))

    print("DEBUG: Download success")

    return {
        "success": True
        }


@app.route("/ingest", methods=["POST"])
def accept_ingest():
    """Accept the JSON feedstock file and begin the ingestion process."""
    auth_head = request.headers.get("Authorization")
    if not auth_head:
        return (jsonify({
            "success": False,
            "error": "Not Authenticated"
            }), 401)
    try:
        auth_head = auth_head.replace("Bearer ", "")
        auth_client = globus_sdk.ConfidentialAppAuthClient(app.config["API_CLIENT_ID"],
                                                           app.config["API_CLIENT_SECRET"])
        auth_res = auth_client.oauth2_token_introspect(auth_head, include="identities_set")
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": "Unacceptable auth: " + repr(e)
            }), 400)
    if not auth_res:
        return (jsonify({
            "success": False,
            "error": "Token could not be validated"
            }), 401)
    # Check that token is active
    if not auth_res["active"]:
        return (jsonify({
            "success": False,
            "error": "Token expired"
            }), 403)
    # Check correct scope and audience
    if (app.config["API_SCOPE"] not in auth_res["scope"]
        or app.config["API_SCOPE_ID"] not in auth_res["aud"]):

        return (jsonify({
            "success": False,
            "error": "Not authorized to MOC scope"
            }), 401)
    # Finally, verify that user ID is in whitelist
    # Can be any identity the user has (MOC is identity-aware)
    if not any([uid in app.config["INGEST_WHITELIST"] for uid in auth_res["identities_set"]]):
        # TODO: Proper logging
        print("DEBUG: User not in whitelist:", auth_res["username"])
        return (jsonify({
            "success": False,
            "error": "You cannot access this service (yet)"
            }), 403)
    user_id = auth_res["sub"]
    # username = auth_res["username"]
    name = auth_res["name"] or "Not given"
    email = auth_res["email"] or "Not given"
    # Check that file exists and is valid
    try:
        feedstock = request.files["file"]
    except KeyError:
        return (jsonify({
            "success": False,
            "error": "No feedstock file uploaded"
            }), 400)
    # Get parameters
    try:
        # requests.form is an ImmutableMultiDict
        # flat=False returns all keys as lists
        params = request.form.to_dict(flat=False)
        services = params.get("services", [])
        data_loc = json.loads(params.get("data", ["{}"])[0])
        service_data = json.loads(params.get("service_data", ["{}"])[0])
        status_id = params.get("status_id", [None])[0]
    except KeyError as e:
        return (jsonify({
            "success": False,
            "error": "Parameters missing: " + repr(e)
            }), 400)
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": "Invalid ingest JSON: " + repr(e)
            }), 400)

    # Mint or update status ID
    if status_id:
        stat_res = update_status(status_id, "ingest_start", "P")
        if not stat_res["success"]:
            return (jsonify(stat_res), 400)
    else:
        status_id = str(ObjectId())
        status_info = {
            "status_id": status_id,
            "submission_code": "I",
            "submission_time": datetime.utcnow().isoformat("T") + "Z",
            "submitter": name,
            # TODO: Get title?
            "title": "[Title skipped for Ingest]",
            "user_id": user_id,
            "user_email": email
            }
        try:
            # TODO: Better metadata validation
            status_res = create_status(status_info)
        except Exception as e:
            return (jsonify({
                "success": False,
                "error": repr(e)
                }), 500)
        if not status_res["success"]:
            return (jsonify(status_res), 500)

    # Save file
    try:
        feed_path = os.path.join(app.config["FEEDSTOCK_PATH"], secure_filename(feedstock.filename))
        feedstock.save(feed_path)
        ingester = Thread(target=moc_ingester, name="ingester_thread", args=(feed_path,
                                                                             status_id,
                                                                             services,
                                                                             data_loc,
                                                                             service_data))
    except Exception as e:
        stat_res = update_status(status_id, "ingest_start", "F", text=repr(e))
        if not stat_res["success"]:
            return (jsonify(stat_res), 500)
        else:
            return (jsonify({
                "success": False,
                "error": repr(e)
                }), 400)
    ingester.start()
    return (jsonify({
        "success": True,
        "status_id": status_id
        }), 202)


def moc_ingester(base_feed_path, status_id, services, data_loc, service_loc):
    """Finalize and ingest feedstock."""
    # Will need client to ingest data
    creds = {
        "app_name": "MDF Open Connect",
        "client_id": app.config["API_CLIENT_ID"],
        "client_secret": app.config["API_CLIENT_SECRET"],
        "services": ["search_ingest", "publish", "transfer"]
        }
    try:
        clients = toolbox.confidential_login(creds)
        search_client = clients["search_ingest"]
        publish_client = clients["publish"]
        transfer_client = clients["transfer"]

        final_feed_path = os.path.join(app.config["FEEDSTOCK_PATH"], status_id + "_final.json")
    except Exception as e:
        stat_res = update_status(status_id, "ingest_start", "F", text=repr(e))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        else:
            return

    stat_res = update_status(status_id, "ingest_start", "S")
    if not stat_res["success"]:
        raise ValueError(str(stat_res))

    # If the data should be local, make sure it is
    if data_loc:
        stat_res = update_status(status_id, "ingest_download", "P")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        # Will not transfer anything if already in place
        local_path = os.path.join(app.config["LOCAL_PATH"], status_id) + "/"
        try:
            dl_res = download_and_backup(transfer_client,
                                         data_loc,
                                         app.config["LOCAL_EP"],
                                         local_path)
        except Exception as e:
            stat_res = update_status(status_id, "ingest_download", "F", text=repr(e))
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
            else:
                return
        if not dl_res["success"]:
            stat_res = update_status(status_id, "ingest_download", "F", text=str(dl_res))
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
            else:
                return
        else:
            stat_res = update_status(status_id, "ingest_download", "S")
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
            if DEBUG_LEVEL >= 2:
                print("{}: Ingest data downloaded".format(status_id))
    # If the data aren't local, but need to be, error
    elif "globus_publish" in services:
        stat_res = update_status(status_id, "ingest_download", "F",
                                 text=("Globus Publish integration was selected, "
                                       "but the data location was not provided."))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        stat_res = update_status(status_id, "ingest_publish", "F",
                                 text="Unable to publish data without location.")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        return
    else:
        stat_res = update_status(status_id, "ingest_download", "N")

    # Same for integrated service data
    if service_loc:
        stat_res = update_status(status_id, "ingest_integration", "P")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        # Will not transfer anything if already in place
        service_data = os.path.join(app.config["SERVICE_DATA"], status_id) + "/"
        try:
            dl_res = download_and_backup(transfer_client,
                                         service_loc,
                                         app.config["LOCAL_EP"],
                                         service_data)
        except Exception as e:
            stat_res = update_status(status_id, "ingest_integration", "F", text=repr(e))
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
            else:
                return
        if not dl_res["success"]:
            stat_res = update_status(status_id, "ingest_integration", "F", text=str(dl_res))
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
            else:
                return
        else:
            stat_res = update_status(status_id, "ingest_integration", "S")
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
            if DEBUG_LEVEL >= 2:
                print("{}: Integration data downloaded".format(status_id))
    # If the data aren't local, but need to be, error
    elif "citrine" in services:
        stat_res = update_status(status_id, "ingest_integration", "F",
                                 text=("Citrine integration was selected, but the"
                                       "integration data location was not provided."))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        stat_res = update_status(status_id, "ingest_citrine", "F",
                                 text="Unable to upload PIFs without location.")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        return
    else:
        stat_res = update_status(status_id, "ingest_integration", "N")

    # Integrations

    # Globus Search (mandatory)
    stat_res = update_status(status_id, "ingest_search", "P")
    if not stat_res["success"]:
        raise ValueError(str(stat_res))
    try:
        search_ingest(search_client, base_feed_path, index=app.config["INGEST_INDEX"],
                      feedstock_save=final_feed_path)
    except Exception as e:
        stat_res = update_status(status_id, "ingest_search", "F", text=repr(e))
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        else:
            return
    else:
        stat_res = update_status(status_id, "ingest_search", "S")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        # Other services use the dataset information
        if services:
            with open(final_feed_path) as f:
                dataset = json.loads(f.readline())

    # Globus Publish
    if "globus_publish" in services:
        stat_res = update_status(status_id, "ingest_publish", "P")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        try:
            fin_res = globus_publish_data(publish_client, transfer_client,
                                          dataset, local_path)
        except Exception as e:
            stat_res = update_status(status_id, "ingest_publish", "R", text=repr(e))
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
        else:
            stat_res = update_status(status_id, "ingest_publish", "M", text=str(fin_res))
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
    else:
        stat_res = update_status(status_id, "ingest_publish", "N")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))

    # Citrine
    if "citrine" in services:
        stat_res = update_status(status_id, "ingest_citrine", "P")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))
        try:
            cit_res = citrine_upload(os.path.join(service_data, "citrine"),
                                     app.config["CITRINATION_API_KEY"],
                                     dataset)
        except Exception as e:
            stat_res = update_status(status_id, "ingest_citrine", "R", text=repr(e))
            if not stat_res["success"]:
                raise ValueError(str(stat_res))
        else:
            if not cit_res["success"]:
                stat_res = update_status(status_id, "ingest_citrine", "R", text=str(cit_res))
                if not stat_res["success"]:
                    raise ValueError(str(stat_res))
            else:
                stat_res = update_status(status_id, "ingest_citrine", "S")
                if not stat_res["success"]:
                    raise ValueError(str(stat_res))
    else:
        stat_res = update_status(status_id, "ingest_citrine", "N")
        if not stat_res["success"]:
            raise ValueError(str(stat_res))

    if DEBUG_LEVEL >= 2:
        print("{}: Ingest complete".format(status_id))
    return {
        "success": True,
        "status_id": status_id
        }


def globus_publish_data(publish_client, transfer_client, metadata, local_path):
    # Submit metadata
    pub_md = get_publish_metadata(metadata)
    md_result = publish_client.push_metadata(pub_md.pop("collection_id"), pub_md)
    pub_endpoint = md_result['globus.shared_endpoint.name']
    pub_path = os.path.join(md_result['globus.shared_endpoint.path'], "data") + "/"
    submission_id = md_result["id"]
    # Transfer data
    toolbox.quick_transfer(transfer_client, app.config["LOCAL_EP"],
                           pub_endpoint, [(local_path, pub_path)], timeout=0)
    # Complete submission
    fin_res = publish_client.complete_submission(submission_id)

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
            up_res = json.loads(cit_client.upload(cit_ds_id, pif))
            if not up_res["success"]:
                # TODO: Handle errors
                print("DEBUG: Citrine upload failure:", up_res.get("status"))
    # TODO: Set share to 1 to enable public uploads
    cit_client.update_data_set(cit_ds_id, share=0)

    return {
        "success": True
        }


@app.route("/status/<status_id>", methods=["GET"])
def get_status(status_id):
    """Fetch and return status information"""
    auth_head = request.headers.get("Authorization")
    if not auth_head:
        return (jsonify({
            "success": False,
            "error": "Not Authenticated"
            }), 401)
    try:
        auth_head = auth_head.replace("Bearer ", "")
        auth_client = globus_sdk.ConfidentialAppAuthClient(app.config["API_CLIENT_ID"],
                                                           app.config["API_CLIENT_SECRET"])
        auth_res = auth_client.oauth2_token_introspect(auth_head, include="identities_set")
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": "Unacceptable auth: " + repr(e)
            }), 400)
    if not auth_res:
        return (jsonify({
            "success": False,
            "error": "Token could not be validated"
            }), 401)
    # Check that token is active
    if not auth_res["active"]:
        return (jsonify({
            "success": False,
            "error": "Token expired"
            }), 403)
    # Check correct scope and audience
    if (app.config["API_SCOPE"] not in auth_res["scope"]
        or app.config["API_SCOPE_ID"] not in auth_res["aud"]):

        return (jsonify({
            "success": False,
            "error": "Not authorized to MOC scope"
            }), 401)
    uid_set = auth_res["identities_set"]

    raw_status = read_status(status_id)
    # Failure message if status not fetched or user not allowed to view
    # Only the user that submitted the dataset and admins can view
    if not raw_status["success"] or not (raw_status["status"]["user_id"] in uid_set
                                         or any([uid in app.config["ADMIN_WHITELIST"]
                                                 for uid in uid_set])):
        return (jsonify({
            "success": False,
            "error": "Submission {} not found, or not available".format(status_id)
            }), 404)
    else:
        return (jsonify(translate_status(raw_status["status"])), 200)


@app.route("/status/<status_id>/raw", methods=["GET"])
def get_raw_status(status_id):
    """Fetch and return user-inappropriate status info"""
    auth_head = request.headers.get("Authorization")
    if not auth_head:
        return (jsonify({
            "success": False,
            "error": "Not Authenticated"
            }), 401)
    try:
        auth_head = auth_head.replace("Bearer ", "")
        auth_client = globus_sdk.ConfidentialAppAuthClient(app.config["API_CLIENT_ID"],
                                                           app.config["API_CLIENT_SECRET"])
        auth_res = auth_client.oauth2_token_introspect(auth_head, include="identities_set")
    except Exception as e:
        return (jsonify({
            "success": False,
            "error": "Unacceptable auth: " + repr(e)
            }), 400)
    if not auth_res:
        return (jsonify({
            "success": False,
            "error": "Token could not be validated"
            }), 401)
    # Check that token is active
    if not auth_res["active"]:
        return (jsonify({
            "success": False,
            "error": "Token expired"
            }), 403)
    # Check correct scope and audience
    if (app.config["API_SCOPE"] not in auth_res["scope"]
        or app.config["API_SCOPE_ID"] not in auth_res["aud"]):

        return (jsonify({
            "success": False,
            "error": "Not authorized to MOC scope"
            }), 401)
    uid_set = auth_res["identities_set"]

    raw_status = read_status(status_id)
    # Failure message if status not fetched or user not allowed to view
    # Only the user that submitted the dataset and admins can view
    if not raw_status["success"] or not (raw_status["status"]["user_id"] in uid_set
                                         or any([uid in app.config["ADMIN_WHITELIST"]
                                                 for uid in uid_set])):
        return (jsonify({
            "success": False,
            "error": "Submission {} not found, or not available".format(status_id)
            }), 404)
    else:
        return (jsonify(raw_status), 200)


def read_status(status_id):
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]

    status_res = table.get_item(Key={"status_id": status_id}, ConsistentRead=True).get("Item")
    if not status_res:
        return {
            "success": False,
            "error": "ID {} not found in status database".format(status_id)
            }
    else:
        return {
            "success": True,
            "status": status_res
            }


def create_status(status):
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]
    # TODO: Validate status better (JSONSchema?)
    if not status.get("status_id"):
        return {
            "success": False,
            "error": "status_id missing"
            }
    elif not status.get("submission_code"):
        return {
            "success": False,
            "error": "submission_code missing"
            }
    elif not status.get("title"):
        return {
            "success": False,
            "error": "title missing"
            }
    elif not status.get("submitter"):
        return {
            "success": False,
            "error": "submitter missing"
            }
    elif not status.get("submission_time"):
        return {
            "success": False,
            "error": "submission_time missing"
            }

    # Create defaults
    status["messages"] = []
    status["errors"] = []
    status["code"] = "WWWWWWWWWW"

    # Check that status does not already exist
    if read_status(status["status_id"])["success"]:
        return {
            "success": False,
            "error": "ID {} already exists in database".format(status["status_id"])
            }
    try:
        table.put_item(Item=status)
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
            }
    else:
        if DEBUG_LEVEL >= 1:
            print("STATUS {}: Created".format(status["status_id"]))
        return {
            "success": True,
            "status": status
            }


def update_status(status_id, step, code, text=None):
    tbl_res = get_dmo_table(DMO_CLIENT, DMO_TABLE)
    if not tbl_res["success"]:
        return tbl_res
    table = tbl_res["table"]
    # TODO: Validate status
    # Get old status
    old_status = read_status(status_id)
    if not old_status["success"]:
        return old_status
    status = old_status["status"]
    # Update code
    try:
        step_index = int(step) - 1
    except ValueError:
        step_index = None
        # Why yes, this would be easier if STATUS_STEPS was a dict
        # But we need slicing for translate_status
        # Or else we're duplicating the info and making maintenance hard
        # And I'd rather one dumb hack than several painful, error-prone changes
        for i, s in enumerate(STATUS_STEPS):
            if step == s[0]:
                step_index = i
                break
    code_list = list(status["code"])
    code_list[step_index] = code
    # If needed, update messages or errors and cancel tasks
    if code == 'M':
        status["messages"].append(text or "No message available")
    elif code == 'F':
        status["errors"].append(text or "An error occurred and we're trying to fix it")
        # Cancel subsequent tasks
        code_list = code_list[:step_index+1] + ["X"]*len(code_list[step_index+1:])
    elif code == 'R':
        status["errors"].append(text or "An error occurred but we're recovering")
    status["code"] = "".join(code_list)

    try:
        # put_item will overwrite
        table.put_item(Item=status)
    except Exception as e:
        return {
            "success": False,
            "error": repr(e)
            }
    else:
        if DEBUG_LEVEL >= 1:
            print("STATUS {}: {}: {}, {}".format(status_id, step, code, text))
        return {
            "success": True,
            "status": status
            }


def translate_status(status):
    # {
    # status_id: str,
    # submission_code: "C" or "I"
    # code: str, based on char position
    # messages: list of str, in order generated
    # errors: list of str, in order of failures
    # title: str,
    # submitter: str,
    # submission_time: str
    # }
    full_code = list(status["code"])
    messages = status["messages"]
    errors = status["errors"]
    sub_type = status["submission_code"]
    # Submission type determines steps
    if sub_type == 'C':
        steps = [st[1] for st in STATUS_STEPS]
        subm = "convert"
    elif sub_type == 'I':
        steps = [st[1] for st in STATUS_STEPS[INGEST_MARK:]]
        subm = "ingest"
    else:
        steps = []
        subm = "unknown submission type '{}'".format(sub_type)

    usr_msg = ("Status of {} submission {} ({})\n"
               "Submitted by {} at {}\n\n").format(subm,
                                                   status["status_id"],
                                                   status["title"],
                                                   status["submitter"],
                                                   status["submission_time"])
    web_msg = []

    for code, step in zip(full_code, steps):
        if code == 'S':
            msg = "{} was successful.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "success",
                "text": msg
            })
        elif code == 'M':
            msg = "{} was successful: {}.".format(step, messages.pop(0))
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "success",
                "text": msg
            })
        elif code == 'F':
            msg = "{} failed: {}.".format(step, errors.pop(0))
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "failure",
                "text": msg
            })
        elif code == 'R':
            msg = "{} failed (processing will continue): {}.".format(step, errors.pop(0))
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "failure",
                "text": msg
            })
        elif code == 'N':
            msg = "{} was not requested or required.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "idle",
                "text": msg
            })
        elif code == 'P':
            msg = "{} is in progress.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "started",
                "text": msg
            })
        elif code == 'X':
            msg = "{} was cancelled.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "idle",
                "text": msg
            })
        elif code == 'W':
            msg = "{} has not started yet.".format(step)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "idle",
                "text": msg
            })
        else:
            msg = "{} code: {}".format(step, code)
            usr_msg += msg + "\n"
            web_msg.append({
                "signal": "warning",
                "text": msg
            })

    return {
        "status_id": status["status_id"],
        "status_message": usr_msg,
        "status_list": web_msg,
        "title": status["title"],
        "submitter": status["submitter"],
        "submission_time": status["submission_time"]
        }


def initialize_dmo_table(client=DMO_CLIENT, table_name=DMO_TABLE, schema=DMO_SCHEMA):
    tbl_res = get_dmo_table(client, table_name)
    # Table should not be active already
    if tbl_res["success"]:
        return {
            "success": False,
            "error": "Table already created"
            }
    # If misc/other exception, cannot create table
    elif tbl_res["error"] != "Table does not exist or is not active":
        return tbl_res

    try:
        new_table = client.create_table(**schema)
        new_table.wait_until_exists()
    except client.meta.client.exceptions.ResourceInUseException:
        return {
            "success": False,
            "error": "Table concurrently created"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
            }

    tbl_res2 = get_dmo_table(client, table_name)
    if not tbl_res2["success"]:
        return {
            "success": False,
            "error": "Unable to create table: {}".format(tbl_res2["error"])
            }
    else:
        return {
            "success": True,
            "table": tbl_res2["table"]
            }


def get_dmo_table(client=DMO_CLIENT, table_name=DMO_TABLE):
    try:
        table = client.Table(table_name)
        dmo_status = table.table_status
        if dmo_status != "ACTIVE":
            raise ValueError("Table not active")
    except (ValueError, client.meta.client.exceptions.ResourceNotFoundException):
        return {
            "success": False,
            "error": "Table does not exist or is not active"
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
            }
    else:
        return {
            "success": True,
            "table": table
            }
