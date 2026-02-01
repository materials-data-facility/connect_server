import os


AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
DYNAMO_SUBMISSIONS_TABLE = os.environ.get("DYNAMO_SUBMISSIONS_TABLE", "mdf-connect-v2-submissions")
DYNAMO_STREAMS_TABLE = os.environ.get("DYNAMO_STREAMS_TABLE", "mdf-connect-v2-streams")
DYNAMO_ENDPOINT_URL = os.environ.get("DYNAMO_ENDPOINT_URL")

SEARCH_INDEX_UUID = os.environ.get("SEARCH_INDEX_UUID")
TEST_SEARCH_INDEX_UUID = os.environ.get("TEST_SEARCH_INDEX_UUID")

FLOW_ID = os.environ.get("FLOW_ID")
FLOW_SCOPE = os.environ.get("FLOW_SCOPE")
FLOW_LABEL_PREFIX = os.environ.get("FLOW_LABEL_PREFIX", "MDF Submission")

GSI_USER_INDEX = os.environ.get("GSI_USER_INDEX", "user-submissions")
GSI_ORG_INDEX = os.environ.get("GSI_ORG_INDEX", "org-submissions")

DEFAULT_ORGANIZATION = os.environ.get("DEFAULT_ORGANIZATION", "MDF Open")
