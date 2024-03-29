import json
import os

from dynamo_manager import DynamoManager
from automate_manager import AutomateManager
from utils import get_secret


def lambda_handler(event, context):
    dynamo_manager = DynamoManager()
    automate_manager = AutomateManager(get_secret(secret_name=os.environ['MDF_SECRETS_NAME'],
                                                  region_name=os.environ['MDF_AWS_REGION']))
    automate_manager.authenticate()

    print(event)
    source_id = event['pathParameters']['source_id']

    version = event['queryStringParameters'].get('version', None) if event[
        "queryStringParameters"] else None

    if version:
        status_rec = dynamo_manager.read_status_record(source_id, version)
    else:
        status_rec = dynamo_manager.get_current_version(source_id)

    print(status_rec)

    result ={
        "original_submission": json.loads(status_rec['original_submission']),
        "flow_status":automate_manager.get_status(status_rec['action_id'])
    }

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
