import os


DEFAULT = {
    "LOCAL_PATH": os.path.expanduser("~/data/"),
    "FEEDSTOCK_PATH": os.path.expanduser("~/feedstock/"),
    "SERVICE_DATA": os.path.expanduser("~/integrations/"),
    "CURATION_DATA": os.path.expanduser("~/curation/"),

    "SCHEMA_PATH": os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas", "schemas")),
    "AUX_DATA_PATH": os.path.abspath(os.path.join(os.path.dirname(__file__), "schemas",
                                                  "connect_aux_data")),

    # Minimum time (in days) to keep test submissions
    "TEST_TTL": 30,

    "PROCESSOR_WAIT_TIME": 20,  # Seconds
    "PROCESSOR_SLEEP_TIME": 40,  # Seconds

    "NUM_EXTRACTORS": 10,
    "NUM_SUBMITTERS": 5,
    "EXTRACTOR_ERROR_FILE": "extractor_errors.log",

    "CANCEL_WAIT_TIME": 60,  # Seconds

    "TRANSFER_PING_INTERVAL": 20,  # Seconds
    "TRANSFER_WEB_APP_LINK": "https://app.globus.org/file-manager?origin_id={}&origin_path={}",

    "TRANSFER_CANCEL_MSG": ("Your recent MDF Connect submission was cancelled due to a service"
                            " restart. Please resubmit your dataset. We apologize for the"
                            " inconvenience."),

    "NUM_CURATION_RECORDS": 3,

    "SCHEMA_NULLS": ["url"],  # Just url from files

    "SEARCH_BATCH_SIZE": 100,
    "SEARCH_RETRIES": 3,
    "SEARCH_PING_TIME": 2,  # Seconds

    # Fields in the mdf block that cannot be updated with /update
    "NO_UPDATE_FIELDS_MDF": ["source_id", "source_name", "scroll_id", "version"],

    "DATASET_LANDING_PAGE": "https://petreldata.net/mdf/detail/{}",
    "RECORD_LANDING_PAGE": "https://petreldata.net/mdf/detail/{}.{}",

    "CITRINATION_LINK": "https://citrination.com/datasets/{cit_ds_id}/",

    "MRR_URL": "https://mrr.materialsdatafacility.org/rest/data/",
    "MRR_SCHEMA": "5df1452da623810013116d89",
    "MRR_LINK": "https://mrr.materialsdatafacility.org/data?id={}",

    "API_CLIENT_ID": "c17f27bb-f200-486a-b785-2a25e82af505",
    "API_SCOPE": "https://auth.globus.org/scopes/c17f27bb-f200-486a-b785-2a25e82af505/connect",
    "API_SCOPE_ID": "mdf_dataset_submission",
    "TRANSFER_SCOPE": "urn:globus:auth:scope:transfer.api.globus.org:all",

    # Regexes for detecting Globus Web App links
    "GLOBUS_LINK_FORMS": [
        "^https:\/\/www\.globus\.org\/app\/transfer",  # noqa: W605 (invalid escape char '\/')
        "^https:\/\/app\.globus\.org\/file-manager",  # noqa: W605
        "^https:\/\/app\.globus\.org\/transfer",  # noqa: W605
        "^https:\/\/.*globus.*(?=.*origin_id)(?=.*origin_path)",  # noqa: W605
        "^https:\/\/.*globus.*(?=.*destination_id)(?=.*destination_path)"  # noqa: W605
    ],

    # Using Prod-P GDrive EP because having two GDrive EPs on one account seems to fail
    "GDRIVE_EP": "f00dfd6c-edf4-4c8b-a4b1-be6ad92a4fbb",
    "GDRIVE_ROOT": "/Shared With Me",

    "ADMIN_GROUP_ID": "5fc63928-3752-11e8-9c6f-0e00fd09bf20",
    "EXTRACT_GROUP_ID": "cc192dca-3751-11e8-90c1-0a7c735d220a"
}
with open(os.path.join(DEFAULT["SCHEMA_PATH"], "mrr_template.xml")) as f:
    DEFAULT["MRR_TEMPLATE"] = f.read()
with open(os.path.join(DEFAULT["SCHEMA_PATH"], "mrr_contributor.xml")) as f:
    DEFAULT["MRR_CONTRIBUTOR"] = f.read()
