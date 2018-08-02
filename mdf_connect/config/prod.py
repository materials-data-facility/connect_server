PROD = {
    "SERVER_NAME": "api.materialsdatafacility.org",

    "API_LOG_FILE": "proda.log",
    "PROCESS_LOG_FILE": "prodp.log",
    # "LOG_LEVEL": "INFO",
    "LOG_LEVEL": "DEBUG",

    "FORM_URL": "https://connect.materialsdatafacility.org/",

    # "DEFAULT_TEST_FLAG": False,
    "DEFAULT_TEST_FLAG": True,

    "INGEST_URL": "https://api.materialsdatafacility.org/ingest",
    #"INGEST_INDEX": "mdf",
    "INGEST_INDEX": "mdf-test",
    "INGEST_TEST_INDEX": "mdf-test",

    #"LOCAL_EP": "0f1c3918-749d-11e8-93ba-0a6d4e044368",

    "BACKUP_EP": "e38ee745-6d04-11e5-ba46-22000b92c6ec",
    "BACKUP_PATH": "/MDF/mdf_connect/prod/data/",
    "BACKUP_HOST": "https://e38ee745-6d04-11e5-ba46-22000b92c6ec.e.globus.org",
    "BACKUP_FEEDSTOCK": "/MDF/mdf_connect/prod/feedstock/",

    "DEFAULT_CLEANUP": True,

    #"DEFAULT_PUBLISH_COLLECTION": 21,
    "DEFAULT_PUBLISH_COLLECTION": 35,
    "TEST_PUBLISH_COLLECTION": 35,

    #"DEFAULT_CITRINATION_PUBLIC": True,
    "DEFAULT_CITRINATION_PUBLIC": False,

    #"DEFAULT_MRR_TEST": False,
    "DEFAULT_MRR_TEST": True,

    "SQS_QUEUE": "mdfc_prod.fifo",
    "SQS_GROUP_ID": "mdf_connect_prod",

    "DYNAMO_TABLE": "prod-status-2"
}
