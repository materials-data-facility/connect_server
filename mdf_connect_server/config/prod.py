PROD = {
    "SERVER_NAME": "api.materialsdatafacility.org",

    "API_LOG_FILE": "proda.log",
    "PROCESS_LOG_FILE": "prodp.log",
    "LOG_LEVEL": "INFO",

    "FORM_URL": "https://connect.materialsdatafacility.org/",

    "TRANSFER_DEADLINE": 24 * 60 * 60,  # 1 day, in seconds

    "INGEST_URL": "https://api.materialsdatafacility.org/ingest",
    "INGEST_INDEX": "mdf",
    "INGEST_TEST_INDEX": "mdf-test",

    "LOCAL_EP": "693e4df6-9274-4fff-ad2d-53661a1df1f1",

    "BACKUP_EP": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
    "BACKUP_PATH": "/MDF/mdf_connect/prod/data/",
    "BACKUP_HOST": "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org",
    "BACKUP_FEEDSTOCK": "/MDF/mdf_connect/prod/feedstock/",

    "GDRIVE_EP": "728a8a88-605b-4605-8afd-396f087eb3fc",

    "DEFAULT_CLEANUP": True,
    "FINAL_CLEANUP": True,

    "DEFAULT_DOI_TEST": False,
    "NUM_DOI_CHARS": 4,  # Characters per section
    "NUM_DOI_SECTIONS": 2,

    "DEFAULT_PUBLISH_COLLECTION": 21,
    "TEST_PUBLISH_COLLECTION": 35,

    "DEFAULT_CITRINATION_PUBLIC": True,

    "DEFAULT_MRR_TEST": False,

    "SQS_QUEUE": "mdfc_prod.fifo",
    "SQS_GROUP_ID": "mdf_connect_prod",

    "DYNAMO_STATUS_TABLE": "prod-status-alpha-1",
    "DYNAMO_CURATION_TABLE": "prod-curation-alpha-1"
}
