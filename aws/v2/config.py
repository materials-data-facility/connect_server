import os


AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DYNAMO_SUBMISSIONS_TABLE = os.environ.get("DYNAMO_SUBMISSIONS_TABLE", "mdf-connect-v2-submissions")
DYNAMO_STREAMS_TABLE = os.environ.get("DYNAMO_STREAMS_TABLE", "mdf-connect-v2-streams")
DYNAMO_ENDPOINT_URL = os.environ.get("DYNAMO_ENDPOINT_URL")

SEARCH_INDEX_UUID = os.environ.get("SEARCH_INDEX_UUID")
TEST_SEARCH_INDEX_UUID = os.environ.get("TEST_SEARCH_INDEX_UUID")


GSI_USER_INDEX = os.environ.get("GSI_USER_INDEX", "user-submissions")
GSI_ORG_INDEX = os.environ.get("GSI_ORG_INDEX", "org-submissions")
GSI_LEGACY_INDEX = os.environ.get("GSI_LEGACY_INDEX", "legacy-source-id-index")

DEFAULT_ORGANIZATION = os.environ.get("DEFAULT_ORGANIZATION", "MDF Open")


def configure_logging() -> None:
    """Process-wide logging policy, shared by the API and the async worker.

    Root stays at INFO so third-party libraries never dump request bodies
    (botocore at DEBUG logs full DynamoDB/SES payloads). LOG_LEVEL applies only
    to our own ``v2`` namespace, and the chattiest HTTP/AWS libraries are pinned
    to WARNING. Idempotent: safe to call from every entrypoint.
    """
    import logging
    import os

    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("v2").setLevel(getattr(logging, level_name, logging.INFO))
    for noisy in ("botocore", "boto3", "urllib3", "httpx", "httpcore", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
