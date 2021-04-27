import json
from dynamo_manager import DynamoManager
from automate_manager import AutomateManager
from utils import get_secret

CONFIG = {
    "ADMIN_GROUP_ID": "5fc63928-3752-11e8-9c6f-0e00fd09bf20",
    "EXTRACT_GROUP_ID": "cc192dca-3751-11e8-90c1-0a7c735d220a",
    "API_SCOPE": "https://auth.globus.org/scopes/c17f27bb-f200-486a-b785-2a25e82af505/connect",
    "API_SCOPE_ID": "mdf_dataset_submission",
    "BACKUP_EP": False,
    "BACKUP_PATH": "/mdf_connect/dev/data/",
    "DEFAULT_DOI_TEST": True,
    "DEFAULT_CITRINATION_PUBLIC": False,
    "DEFAULT_MRR_TEST": True,
    # Regexes for detecting Globus Web App links
    "GLOBUS_LINK_FORMS": [
        "^https:\/\/www\.globus\.org\/app\/transfer",
        # noqa: W605 (invalid escape char '\/')
        "^https:\/\/app\.globus\.org\/file-manager",  # noqa: W605
        "^https:\/\/app\.globus\.org\/transfer",  # noqa: W605
        "^https:\/\/.*globus.*(?=.*origin_id)(?=.*origin_path)",  # noqa: W605
        "^https:\/\/.*globus.*(?=.*destination_id)(?=.*destination_path)"  # noqa: W605
    ],

    # Using Prod-P GDrive EP because having two GDrive EPs on one account seems to fail
    "GDRIVE_EP": "f00dfd6c-edf4-4c8b-a4b1-be6ad92a4fbb",
    "GDRIVE_ROOT": "/Shared With Me",

    "TRANSFER_WEB_APP_LINK": "https://app.globus.org/file-manager?origin_id={}&origin_path={}",
    "INGEST_URL": "https://dev-api.materialsdatafacility.org/ingest",
    "INGEST_INDEX": "mdf-dev",
    "INGEST_TEST_INDEX": "mdf-dev",
    "DYNAMO_STATUS_TABLE": "dev-status-alpha-2",
    "DYNAMO_CURATION_TABLE": "dev-curation-alpha-1"
}


def lambda_handler(event, context):
    dynamo_manager = DynamoManager()
    automate_manager = AutomateManager(get_secret())

    print(event)
    source_id = event['pathParameters']['source_id']
    status_rec = dynamo_manager.for_source_id(source_id)
    print(status_rec)

    print(automate_manager.get_log(status_rec['action_id']))

    return {
        'statusCode': 200,
        'body': json.dumps(automate_manager.get_status(status_rec['action_id']))
    }
