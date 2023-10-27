import json
from dynamo_manager import DynamoManager
from automate_manager import AutomateManager
from utils import get_secret

status_codes = {
    "SUCCEEDED": "S",
    "ACTIVE": "P",
    "FAILED": "F"
}

def format_status_record(status:dict, automate_manager:AutomateManager) -> dict:
    usr_msg = ("Status of {}submission {} ({})\n"
               "Submitted by {} at {}\n\n").format("TEST " if status["test"] else "",
                                                   status["source_id"],
                                                   status["title"],
                                                   status["submitter"],
                                                   status["submission_time"])

    automate_status = automate_manager.get_status(status['action_id'])


    return {
        "source_id": status["source_id"],
        "status_message": usr_msg,
        "status_list": "need more status data",
        "status_code": status_codes[automate_status['status']],
        "title": status["title"],
        "submitter": status["submitter"],
        "submission_time": status["submission_time"],
        "description": automate_status['details']['description'],
        "test": status["test"],
        "active": automate_status['status'] == "ACTIVE",
        "original_submission": json.loads(status["original_submission"])
    }

def lambda_handler(event, context):
    user_id = event['requestContext']['authorizer']['user_id']

    if 'filters' in event['body']:
        provided_filters = json.loads(event['body'])['filters']
    else:
        provided_filters = []

    dynamo_manager = DynamoManager()

    automate_manager = AutomateManager(get_secret())
    automate_manager.authenticate()

    if event["pathParameters"] and  "user_id" in event['pathParameters']:
        requested_user_id = event['pathParameters']['user_id']
    else:
        requested_user_id = user_id

    filters = [("user_id", "==", requested_user_id)]
    filters.extend(provided_filters)
    print(f"Final filters = {filters}")
    scan_res = dynamo_manager.scan_table("status", filters=filters)
    response = [format_status_record(status, automate_manager) for status in scan_res['results']]

    return {
        'statusCode' : 200,
        'headers': {"content-type": "application/json"},
        'body': json.dumps({
            "submissions": response
        })
    }
