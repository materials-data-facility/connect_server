import json

smtp_send_credentials = []
with open(".mdfsecrets", 'r') as f:
    secrets = json.load(f)
    smtp_send_credentials = [{
        # "credential_method": "",
        "credential_type": "smtp",
        "credential_value": {
            "hostname": secrets['smtp_hostname'],
            "username": secrets['smtp_user'],
            "password": secrets['smtp_pass']
        }
    }]

mdf_flow = {
    "StartAt": "StartSubmission",
    "States": {
        "StartSubmission": {
            "Type": "Pass",
            "Next": "UserPermissions"
        },
        "UserPermissions": {
            "Type": "Pass",
            "Parameters": {},
            "ResultPath": "$.UserPermissionResult",
            "Next": "UserTransfer"
        },
        "UserTransfer": {
            "Type": "Action",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "action_inputs.$": "$.user_transfer_inputs"
            },
            "ResultPath": "$.UserTransferResult",
            "WaitTime": 86400,
            "Next": "UndoUserPermissions"
        },
        "UndoUserPermissions": {
            "Type": "Pass",
            "Parameters": {},
            "ResultPath": "$.UndoUserPermissionResult",
            "Next": "CheckUserTransfer"
        },
        "CheckUserTransfer": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.UserTransferResult.status",
                    "StringEquals": "SUCCEEDED",
                    "Next": "ChooseNotifyUserEnd"  # Was Xtract
                }
            ],
            "Default": "FailUserTransfer"
        },
        "FailUserTransfer": {
            "Type": "ExpressionEval",
            "Parameters": {
                "title": "MDF Submission Failed",
                "message.=": "'Your MDF submission ' + `$.source_id` + ' failed to transfer to MDF:\n' + `$.UserTransferResult.details`"
            },
            "ResultPath": "$.FinalState",
            "Next": "ChooseNotifyUserEnd"
        },
        "ChooseNotifyUserEnd": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.curation_input",
                    "BooleanEquals": False,
                    "Next": "EndSubmission"
                }
            ],
            "Default": "NotifyUserEnd"
        },
        "NotifyUserEnd": {
            "Type": "Action",
            "ActionUrl": "https://actions.globus.org/notification/notify",
            "ExceptionOnActionFailure": True,
            "Parameters": {
                "body_template.$": "$.FinalState.message",
                "destination.$": "$.curation_input.author_email",
                "send_credentials": smtp_send_credentials,
                "__Private_Parameters": [
                    "send_credentials"
                ],
                "subject.$": "$.FinalState.title"
            },
            "ResultPath": "$.NotifyUserResult",
            "WaitTime": 86400,
            "Next": "EndSubmission"
        },
        "EndSubmission": {
            "Type": "Pass",
            "End": True
        }
    }
}