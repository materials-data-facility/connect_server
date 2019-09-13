import os


DEFAULT = {
    "LOCAL_PATH": os.path.expanduser("~/data/"),
    "FEEDSTOCK_PATH": os.path.expanduser("~/feedstock/"),
    "SERVICE_DATA": os.path.expanduser("~/integrations/"),
    "CURATION_DATA": os.path.expanduser("~/curation/"),

    "SCHEMA_PATH": os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas", "schemas")),
    "AUX_DATA_PATH": os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas",
                                                  "connect_aux_data")),

    "PROCESSOR_WAIT_TIME": 20,  # Seconds
    "PROCESSOR_SLEEP_TIME": 40,  # Seconds

    "NUM_TRANSFORMERS": 10,
    "NUM_SUBMITTERS": 5,
    "TRANSFORMER_ERROR_FILE": "parser_errors.log",

    "CANCEL_WAIT_TIME": 60,  # Seconds

    "TRANSFER_PING_INTERVAL": 60,  # Seconds
    "TRANSFER_WEB_APP_LINK": "https://app.globus.org/file-manager?origin_id={}&origin_path={}",

    "TRANSFER_CANCEL_MSG": ("Your recent MDF Connect submission was cancelled due to a service"
                            " restart. Please resubmit your dataset. We apologize for the"
                            " inconvenience."),

    "NUM_CURATION_RECORDS": 3,

    "SCHEMA_NULLS": ["url"],  # Just url from files

    "SEARCH_BATCH_SIZE": 100,
    "SEARCH_RETRIES": 3,
    "SEARCH_PING_TIME": 2,  # Seconds

    "DATASET_LANDING_PAGE": "https://petreldata.net/mdf/detail/{}",
    "RECORD_LANDING_PAGE": "https://petreldata.net/mdf/detail/{}.{}",

    "CITRINATION_LINK": "https://citrination.com/datasets/{cit_ds_id}/",

    "MRR_URL": "https://mrr.materialsdatafacility.org/rest/curate",

    "MRR_SCHEMA": "5a79c146be2d440472d045d4",

    "API_CLIENT_ID": "c17f27bb-f200-486a-b785-2a25e82af505",
    "API_SCOPE": "https://auth.globus.org/scopes/c17f27bb-f200-486a-b785-2a25e82af505/connect",
    "API_SCOPE_ID": "mdf_dataset_submission",
    "TRANSFER_SCOPE": "urn:globus:auth:scope:transfer.api.globus.org:all",

    "GDRIVE_ROOT": "/Shared With Me",

    "ADMIN_GROUP_ID": "5fc63928-3752-11e8-9c6f-0e00fd09bf20",
    "CONVERT_GROUP_ID": "cc192dca-3751-11e8-90c1-0a7c735d220a"
}
with open(os.path.join(DEFAULT["SCHEMA_PATH"], "mrr_template.xml")) as f:
    DEFAULT["MRR_TEMPLATE"] = f.read()
with open(os.path.join(DEFAULT["SCHEMA_PATH"], "mrr_contributor.xml")) as f:
    DEFAULT["MRR_CONTRIBUTOR"] = f.read()
