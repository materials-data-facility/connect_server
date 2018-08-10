DEV = {
    "SERVER_NAME": "dev-api.materialsdatafacility.org",

    "API_LOG_FILE": "deva.log",
    "PROCESS_LOG_FILE": "devp.log",
    "LOG_LEVEL": "DEBUG",

    "FORM_URL": "https://connect.materialsdatafacility.org/",

    "DEFAULT_TEST_FLAG": True,

    "INGEST_URL": "https://dev-api.materialsdatafacility.org/ingest",
    "INGEST_INDEX": "mdf-test",
    "INGEST_TEST_INDEX": "mdf-test",

    "LOCAL_EP": "",

    "BACKUP_EP": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
    "BACKUP_PATH": "/MDF/mdf_connect/dev/data/",
    "BACKUP_HOST": "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org",
    "BACKUP_FEEDSTOCK": "/MDF/mdf_connect/dev/feedstock/",

    "DEFAULT_CLEANUP": True,

    "DEFAULT_PUBLISH_COLLECTION": 35,
    "TEST_PUBLISH_COLLECTION": 35,

    "DEFAULT_CITRINATION_PUBLIC": False,

    "DEFAULT_MRR_TEST": True,

    "SQS_QUEUE": "mdfc_dev1.fifo",
    "SQS_GROUP_ID": "mdf_connect_dev",

    "DYNAMO_TABLE": "dev-status-3"
}
