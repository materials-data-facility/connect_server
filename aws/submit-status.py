import json
from dynamo_manager import DynamoManager
from automate_manager import AutomateManager
from utils import get_secret


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
