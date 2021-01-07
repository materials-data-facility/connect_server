import json

import action_providers
from mdf_connect_server.automate.globus_automate_flow import GlobusAutomateFlowDef


def flow_def(smtp_send_credentials, sender_email, flow_permissions,
             transfer_loop_subflow):
    return GlobusAutomateFlowDef(
        title="Transfer Loop Flow",
        description="Perform multiple Globus Transfers",
        visible_to=flow_permissions,
        runnable_by=flow_permissions,
        administered_by=flow_permissions,
        flow_definition={
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
                    "ActionUrl": transfer_loop_subflow.url,
                    "ActionScope": transfer_loop_subflow.flow_scope,
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
                    "ActionUrl": action_providers.notify,
                    "ExceptionOnActionFailure": True,
                    "Parameters": {
                        "body_template.$": "$.FinalState.message",
                        "destination.$": "$.curation_input.author_email",
                        "send_credentials": smtp_send_credentials,
                        "__Private_Parameters": [
                            "send_credentials"
                        ],
                        "sender": sender_email,
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
        })
