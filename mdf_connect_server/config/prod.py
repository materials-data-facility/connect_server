PROD = {
    "SERVER_NAME": "api.materialsdatafacility.org",

    "API_LOG_FILE": "proda.log",
    "PROCESS_LOG_FILE": "prodp.log",
    "LOG_LEVEL": "INFO",

    "FORM_URL": "https://connect.materialsdatafacility.org/",

    "DEFAULT_TEST_FLAG": False,

    "INGEST_URL": "https://api.materialsdatafacility.org/ingest",
    "INGEST_INDEX": "mdf",
    "INGEST_TEST_INDEX": "mdf-test",

    "LOCAL_EP": "99d4ec57-863f-413e-a0b9-f4a5032989ab",

    "BACKUP_EP": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
    "BACKUP_PATH": "/MDF/mdf_connect/prod/data/",
    "BACKUP_HOST": "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org",
    "BACKUP_FEEDSTOCK": "/MDF/mdf_connect/prod/feedstock/",

    "GDRIVE_EP": "e338b362-32d1-4f9d-a1ec-f2723fff1e0c",

    "DEFAULT_CLEANUP": True,

    "DEFAULT_PUBLISH_COLLECTION": 21,
    "TEST_PUBLISH_COLLECTION": 35,

    "DEFAULT_CITRINATION_PUBLIC": True,

    "DEFAULT_MRR_TEST": False,

    "SQS_QUEUE": "mdfc_prod.fifo",
    "SQS_GROUP_ID": "mdf_connect_prod",

    "DYNAMO_TABLE": "prod-status-2"
}
